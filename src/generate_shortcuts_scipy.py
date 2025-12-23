
import logging
from pathlib import Path
import time
import pandas as pd
import numpy as np
import duckdb
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import shortest_path

import config
import utilities as utils

import logging_config as log_conf

# Setup logging
logger = logging.getLogger(__name__)

def process_partition_scipy(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Process a single partition using Scipy's shortest_path.
    EXACTLY matches Spark version for identical via_edge output.
    """
    if len(pdf) == 0:
        return pd.DataFrame(columns=['from_edge', 'to_edge', 'via_edge', 'cost'])
    
    # Map nodes to indices
    nodes = pd.concat([pdf['from_edge'], pdf['to_edge']]).unique()
    n_nodes = len(nodes)
    node_to_idx = pd.Series(data=np.arange(n_nodes), index=nodes)
    
    # Deduplicate and keep minimum cost
    pdf_dedup = pdf.loc[
        pdf.groupby(['from_edge', 'to_edge'])['cost'].idxmin()
    ]
    
    src_indices = pdf_dedup['from_edge'].map(node_to_idx).values
    dst_indices = pdf_dedup['to_edge'].map(node_to_idx).values
    costs = pdf_dedup['cost'].values
    
    # Store via_edges for direct paths (SAME AS SPARK: +1 offset)
    via_values = pdf_dedup['via_edge'].values + 1
    via_matrix = csr_matrix((via_values, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
    
    # Build graph matrix
    graph = csr_matrix((costs, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
    
    # Compute shortest paths (SAME AS SPARK: method='auto')
    dist_matrix, predecessors = shortest_path(
        csgraph=graph,
        method='auto',
        directed=True,
        return_predecessors=True
    )
    
    # Convert back to DataFrame (SAME AS SPARK: chunked processing)
    results = []
    chunk_size = 2000
    
    for start_row in range(0, n_nodes, chunk_size):
        end_row = min(start_row + chunk_size, n_nodes)
        sub_dist = dist_matrix[start_row:end_row, :]
        sub_pred = predecessors[start_row:end_row, :]
        
        valid_mask = (sub_dist != np.inf)
        rows, cols = np.where(valid_mask)
        global_rows = rows + start_row
        
        # Filter self-loops
        non_loop_mask = (global_rows != cols)
        rows = rows[non_loop_mask]
        cols = cols[non_loop_mask]
        global_rows = global_rows[non_loop_mask]
        
        if len(rows) == 0:
            continue
        
        chunk_costs = sub_dist[rows, cols]
        chunk_preds = sub_pred[rows, cols]
        chunk_src = nodes[global_rows]
        chunk_dst = nodes[cols]
        
        # Determine via_edge (SAME AS SPARK: vectorized)
        original_vias = np.array(via_matrix[global_rows, cols]).flatten() - 1
        is_direct = (chunk_preds == global_rows)
        
        final_vias = np.where(
            is_direct,
            original_vias,
            nodes[chunk_preds]
        )
        
        chunk_df = pd.DataFrame({
            'from_edge': chunk_src,
            'to_edge': chunk_dst,
            'via_edge': final_vias,
            'cost': chunk_costs
        })
        results.append(chunk_df)
    
    if not results:
        return pd.DataFrame(columns=['from_edge', 'to_edge', 'via_edge', 'cost'])
    
    return pd.concat(results, ignore_index=True)



def run_scipy_algorithm(con: duckdb.DuckDBPyConnection):
    """
    Execute Scipy algorithm on 'shortcuts' table grouped by 'current_cell'.
    Updates 'shortcuts_next' with results.
    """
    logger.info("Fetching active shortcuts...")
    # Get all data into Pandas
    df = con.sql("SELECT * FROM shortcuts WHERE current_cell IS NOT NULL").df()
    
    if df.empty:
        logger.info("No active shortcuts to process.")
        con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")
        return

    logger.info(f"Processing {len(df)} shortcuts across {df['current_cell'].nunique()} partitions")
    
    # Group by cell and apply processing
    results = []
    for cell, group in df.groupby('current_cell'):
        processed_group = process_partition_scipy(group)
        if not processed_group.empty:
            processed_group['current_cell'] = cell
            results.append(processed_group)
            
    if results:
        final_df = pd.concat(results)
        # Load back to DuckDB
        con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
    else:
        con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")

def main():
    log_conf.setup_logging("generate_shortcuts_scipy")
    log_conf.log_section(logger, "SHORTCUTS GENERATION - DUCKDB + SCIPY VERSION")
    
    config_info = {
        "edges_file": str(config.EDGES_FILE),
        "graph_file": str(config.GRAPH_FILE),
        "output_file": str(config.SHORTCUTS_OUTPUT_FILE),
        "district": config.DISTRICT_NAME
    }
    log_conf.log_dict(logger, config_info, "Configuration")
    
    # Define unique database path if persistence is enabled
    db_path = ":memory:"
    if config.DUCKDB_PERSIST_DIR:
        db_path = str(Path(config.DUCKDB_PERSIST_DIR) / "scipy_working.db")
        logger.info(f"Using file-backed DuckDB: {db_path}")

    con = utils.initialize_duckdb(db_path)
    
    # 1. Load Data
    logger.info("Loading edge data...")
    utils.read_edges(con, str(config.EDGES_FILE))
    edges_count = con.sql("SELECT COUNT(*) FROM edges").fetchone()[0]
    logger.info(f"✓ Loaded {edges_count} edges")

    logger.info("Computing edge costs...")
    utils.create_edges_cost_table(con, str(config.EDGES_FILE))
    logger.info("✓ Edge costs computed")
    
    logger.info("Creating initial shortcuts table...")
    utils.initial_shortcuts_table(con, str(config.GRAPH_FILE))
    shortcuts_count = con.sql("SELECT COUNT(*) FROM shortcuts").fetchone()[0]
    logger.info(f"✓ Created {shortcuts_count} initial shortcuts")
    
    resolution_results = []

    # 2. Forward Pass
    log_conf.log_section(logger, "PHASE 1: FORWARD PASS (15 → -1)")
    # Forward Pass: 15 down to -1 (inclusive)
    for res in range(15, -2, -1):
        logger.info(f"\n--- Resolution {res} ---")
        utils.assign_cell_forward(con, res)
        # assign logic creates shortcuts_next (candidates) from shortcuts
        # But wait, logic is: shortcuts -> assign -> active shortcuts -> extend -> merge
        
        # Correct flow replication:
        # 1. Assign cells to current shortcuts -> result is shortcuts labeled with cell
        # 2. Filter for active
        # 3. Extend
        # 4. Merge
        
        # My utilities.assign_cell_forward creates 'shortcuts_next' which contains VALID assignments.
        # But we need to use this for processing, THEN merge.
        
        # Let's fix the flow to match hybrid:
        
        # A. Assign Cells
        utils.assign_cell_forward(con, res) 
        # Output: shortcuts_next (table with current_cell set)
        
        # B. Run Algorithm on shortcuts_next
        # We rename shortcuts_next to 'shortcuts_active' for clarity?
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # C. Run Scipy on shortcuts_active
        # 'run_scipy_algorithm' expects 'shortcuts' table with current_cell? 
        # No, let's make it accept table name or just use shortcuts_active
        
        # C. Run Scipy on shortcuts_active
        con.execute("DROP TABLE IF EXISTS shortcuts_processing")
        con.execute("CREATE TEMPORARY TABLE shortcuts_processing AS SELECT * FROM shortcuts_active WHERE current_cell IS NOT NULL")
        
        df = con.sql("SELECT * FROM shortcuts_processing").df()
        
        logger.info(f"✓ {len(df)} active shortcuts at resolution {res}")
        
        results = []
        if not df.empty:
            logger.info(f"Processing across {df['current_cell'].nunique()} partitions using Scipy...")
            for cell, group in df.groupby('current_cell'):
                processed = process_partition_scipy(group)
                if not processed.empty:
                    processed['current_cell'] = cell
                    results.append(processed)
        
        new_count = 0
        if results:
            final_df = pd.concat(results)
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
            new_count = len(final_df)
            logger.info(f"✓ Generated {new_count} shortcuts")
        else:
            logger.info("No active shortcuts, skipping...")
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")
            
        resolution_results.append({
            "phase": "forward",
            "resolution": res,
            "active": len(df),
            "generated": new_count
        })

        # E. Merge
        logger.info(f"Merging {new_count} new shortcuts...")
        utils.merge_shortcuts(con)
        utils.checkpoint(con)
        con.execute("DROP TABLE shortcuts_active")

    # ================================================================
    # PHASE 2: BACKWARD PASS (0 → 15)
    # ================================================================
    logger.info("\n=== PHASE 2: BACKWARD PASS (0 → 15) ===")
    
    for res in range(0, 16):
        logger.info(f"\n--- Backward Resolution {res} ---")
        
        # A. Assign Cells (backward uses same logic, or could use assign_cell_backward)
        utils.assign_cell_backward(con, res) 
        
        # B. Rename for processing
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # C. Prepare for Scipy
        con.execute("DROP TABLE IF EXISTS shortcuts_processing")
        con.execute("CREATE TEMPORARY TABLE shortcuts_processing AS SELECT * FROM shortcuts_active WHERE current_cell IS NOT NULL")
        
        df = con.sql("SELECT * FROM shortcuts_processing").df()

        # D. Execute Scipy
        logger.info(f"✓ {len(df)} active shortcuts at resolution {res}")
        
        results = []
        if not df.empty:
            logger.info(f"Processing across {df['current_cell'].nunique()} partitions using Scipy...")
            for cell, group in df.groupby('current_cell'):
                if cell is None:
                    continue
                processed = process_partition_scipy(group)
                if not processed.empty:
                    processed['current_cell'] = cell
                    results.append(processed)
        
        new_count = 0
        if results:
            final_df = pd.concat(results)
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
            new_count = len(final_df)
            logger.info(f"✓ Generated {new_count} shortcuts")
        else:
            logger.info("No active shortcuts, skipping...")
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts WHERE 1=0")
            
        resolution_results.append({
            "phase": "backward",
            "resolution": res,
            "active": len(df),
            "generated": new_count
        })

        # E. Merge
        logger.info(f"Merging {new_count} new shortcuts...")
        utils.merge_shortcuts(con)
        utils.checkpoint(con)
        con.execute("DROP TABLE shortcuts_active")

    # 3. Finalize
    log_conf.log_section(logger, "SAVING OUTPUT")
    
    final_count = con.sql("SELECT COUNT(*) FROM shortcuts").fetchone()[0]
    logger.info(f"Final shortcuts count: {final_count}")

    logger.info("Adding final info (cell, inside)...")
    utils.add_final_info(con)
    
    output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_duckdb_scipy")
    logger.info(f"Saving to {output_path}")
    utils.save_output(con, output_path)

    # 4. Summary
    log_conf.log_section(logger, "SUMMARY")
    for r in resolution_results:
        logger.info(f"  {r['phase']:8s} res={r['resolution']:2d}: {r['active']} active → {r['generated']} generated")
    logger.info(f"\n✓ Total shortcuts: {final_count}")
    
    log_conf.log_section(logger, "COMPLETED")
    logger.info("Done.")

if __name__ == "__main__":
    main()
