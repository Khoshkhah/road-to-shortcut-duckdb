
import logging
from pathlib import Path
import pandas as pd
import duckdb
import config
import utilities as utils

# Import algorithms from sibling modules
import generate_shortcuts_scipy as generate_shortcuts_scipy
import generate_shortcuts_pure as generate_shortcuts_pure

import logging_config as log_conf

# Setup logging
logger = log_conf.setup_logging(__name__)

def main():
    log_conf.setup_logging("generate_shortcuts_hybrid")
    log_conf.log_section(logger, "SHORTCUTS GENERATION - DUCKDB HYBRID VERSION")
    
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
        db_path = str(Path(config.DUCKDB_PERSIST_DIR) / "hybrid_working.db")
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
    
    # 2. Iteration Logic
    # Forward Pass: 15 -> 0
    # Backward Pass: 0 -> 15 (Simulated by logic, but actually we just continue processing)
    
    # In hybrid Spark: 
    # Forward resolutions: 15 down to 0
    # Backward resolutions: 0 up to 15 (if needed)
    
    # Scipy: resolutions -1 to 11 (fine resolutions, faster)
    # Pure:  resolutions 12 to 15 (coarse resolutions, better for large partitions)
    scipy_resolutions = set(range(-1, 10))  # -1 to 9
    pure_resolutions = set(range(10, 16))   # 10 to 15
    logger.info(f"Scipy resolutions: {sorted(scipy_resolutions)}")
    logger.info(f"Pure resolutions: {sorted(pure_resolutions)}")
    
    resolution_results = []
    
    log_conf.log_section(logger, "PHASE 1: FORWARD PASS (15 → -1)")
    for res in range(15, -2, -1):  # 15, 14, ..., 0, -1
        logger.info(f"\nForward: Resolution {res}")
        
        # A. Assign Cells
        logger.info(f"Assigning cells for resolution {res}...")
        utils.assign_cell_forward(con, res)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"✓ {active_count} active shortcuts at resolution {res}")
        
        new_count = 0
        if active_count > 0:
            if res in scipy_resolutions:
                logger.info(">>> Using Scipy Algorithm")
                con.execute("DROP TABLE IF EXISTS shortcuts_processing")
                con.execute("CREATE TEMPORARY TABLE shortcuts_processing AS SELECT * FROM shortcuts_active")
                
                df_active = con.sql("SELECT * FROM shortcuts_processing").df()
                results = []
                if not df_active.empty:
                    logger.info(f"Processing across {df_active['current_cell'].nunique()} partitions using Scipy...")
                    for cell, group in df_active.groupby('current_cell'):
                        processed = generate_shortcuts_scipy.process_partition_scipy(group)
                        if not processed.empty:
                            processed['current_cell'] = cell
                            results.append(processed)
                
                if results:
                    final_df = pd.concat(results)
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
                    new_count = len(final_df)
                else:
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
                con.execute("DROP TABLE shortcuts_processing")
            else:
                logger.info(">>> Using Pure DuckDB Algorithm")
                generate_shortcuts_pure.compute_shortest_paths_pure_duckdb(con)
                new_count = con.sql("SELECT COUNT(*) FROM shortcuts_next").fetchone()[0]
            
            logger.info(f"✓ Generated {new_count} shortcuts")
        else:
            logger.info("No active shortcuts, skipping...")
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
            
        resolution_results.append({
            "phase": "forward",
            "resolution": res,
            "active": active_count,
            "generated": new_count
        })

        # C. Merge
        logger.info(f"Merging {new_count} new shortcuts...")
        utils.merge_shortcuts(con)
        utils.checkpoint(con)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    # Log forward pass summary and DEDUPLICATE
    forward_count = con.sql("SELECT COUNT(*) FROM shortcuts").fetchone()[0]
    logger.info(f"=== FORWARD PASS COMPLETE: {forward_count} total shortcuts (before dedup) ===")
    
    # Deduplicate by keeping MIN cost for each (from_edge, to_edge) pair
    con.execute("""
        CREATE OR REPLACE TABLE shortcuts AS
        SELECT from_edge, to_edge, MIN(cost) as cost, FIRST(via_edge) as via_edge
        FROM shortcuts
        GROUP BY from_edge, to_edge
    """)
    forward_dedup_count = con.sql("SELECT COUNT(*) FROM shortcuts").fetchone()[0]
    logger.info(f"=== FORWARD PASS DEDUPLICATED: {forward_dedup_count} shortcuts (after dedup) ===")

    log_conf.log_section(logger, "PHASE 2: BACKWARD PASS (0 → 15)")
    for res in range(0, 16):  # 0, 1, ..., 15
        logger.info(f"\nBackward: Resolution {res}")
        
        # A. Assign Cells (backward)
        logger.info(f"Assigning cells for resolution {res}...")
        utils.assign_cell_backward(con, res)
        
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"✓ {active_count} active shortcuts at resolution {res}")
        
        new_count = 0
        if active_count > 0:
            if res in scipy_resolutions:
                logger.info(">>> Using Scipy Algorithm")
                con.execute("DROP TABLE IF EXISTS shortcuts_processing")
                con.execute("CREATE TEMPORARY TABLE shortcuts_processing AS SELECT * FROM shortcuts_active")
                
                df_active = con.sql("SELECT * FROM shortcuts_processing").df()
                results = []
                if not df_active.empty:
                    logger.info(f"Processing across {df_active['current_cell'].nunique()} partitions using Scipy...")
                    for cell, group in df_active.groupby('current_cell'):
                        processed = generate_shortcuts_scipy.process_partition_scipy(group)
                        if not processed.empty:
                            processed['current_cell'] = cell
                            results.append(processed)
                if results:
                    final_df = pd.concat(results)
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM final_df")
                    new_count = len(final_df)
                else:
                    con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
                con.execute("DROP TABLE shortcuts_processing")
            else:
                logger.info(">>> Using Pure DuckDB Algorithm")
                generate_shortcuts_pure.compute_shortest_paths_pure_duckdb(con)
                new_count = con.sql("SELECT COUNT(*) FROM shortcuts_next").fetchone()[0]
            
            logger.info(f"✓ Generated {new_count} shortcuts")
        else:
            logger.info("No active shortcuts, skipping...")
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
             
        resolution_results.append({
            "phase": "backward",
            "resolution": res,
            "active": active_count,
            "generated": new_count
        })

        logger.info(f"Merging {new_count} new shortcuts...")
        utils.merge_shortcuts(con)
        utils.checkpoint(con)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    # 3. Finalize
    log_conf.log_section(logger, "SAVING OUTPUT")
    
    final_count = con.sql("SELECT COUNT(*) FROM shortcuts").fetchone()[0]
    logger.info(f"Final shortcuts count: {final_count}")

    logger.info("Adding final info (cell, inside)...")
    utils.add_final_info(con)
    
    output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_duckdb_hybrid")
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
