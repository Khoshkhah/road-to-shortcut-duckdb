"""
Parallel Shortcut Generation using DuckDB with multiprocessing.

This is an optimized version of processor.py that uses
parallel processing for Phase 1 and Phase 4 where chunks are independent.
"""
import logging
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import duckdb
import h3
import pandas as pd

import utilities as utils
from sp_methods.pure import compute_shortest_paths_pure_duckdb
from sp_methods.scipy import process_partition_scipy

import logging_config as log_conf
import config

logger = logging.getLogger(__name__)

PARTITION_RES = 7
SP_METHOD = "SCIPY"
MAX_WORKERS = min(4, cpu_count())  # Limit parallelism to avoid memory issues


def process_chunk_phase1(args):
    """
    Worker function for Phase 1 parallel processing.
    Uses in-memory DuckDB. Receives data as DataFrames.
    Returns: (chunk_id, active_shortcuts_df, deactivated_shortcuts_df, active_count, timing_info)
    """
    chunk_id, edges_df, shortcuts_df, partition_res, sp_method, hybrid_res = args
    
    # Each worker uses in-memory DuckDB
    con = duckdb.connect(":memory:")
    
    # Register H3 UDFs
    con.create_function("h3_lca", utils._find_lca_impl, ["BIGINT", "BIGINT"], "BIGINT")
    con.create_function("h3_resolution", utils._find_resolution_impl, ["BIGINT"], "INTEGER")
    con.create_function("h3_parent", utils._get_parent_cell_impl, ["BIGINT", "INTEGER"], "BIGINT")
    
    timing_info = []  # List of (res, method, time)
    
    try:
        # Load edges into worker's in-memory DB
        con.execute("CREATE TABLE edges AS SELECT * FROM edges_df")
        
        if len(shortcuts_df) == 0:
            return (chunk_id, None, None, 0, [])
        
        # Create table for processing
        con.execute("CREATE TABLE shortcuts AS SELECT * FROM shortcuts_df")
        
        # Create table to collect deactivated shortcuts
        con.execute("""
            CREATE TABLE deactivated (
                from_edge BIGINT, to_edge BIGINT, cost DOUBLE, via_edge BIGINT,
                lca_res INTEGER, inner_cell BIGINT, outer_cell BIGINT, 
                inner_res TINYINT, outer_res TINYINT, current_cell BIGINT
            )
        """)
        
        # Iterative Forward Pass (15 -> partition_res)
        for res in range(15, partition_res - 1, -1):
            res_start = time.time()
            
            _assign_cell_to_shortcuts_worker(con, res, "shortcuts")
            
            # Collect deactivated shortcuts (NULL current_cell) before processing
            con.execute("""
                INSERT INTO deactivated
                SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell 
                FROM shortcuts WHERE current_cell IS NULL
            """)
            
            # Keep only active shortcuts
            con.execute("CREATE OR REPLACE TABLE shortcuts_active AS SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell FROM shortcuts WHERE current_cell IS NOT NULL")
            con.execute("DROP TABLE shortcuts")
            con.execute("ALTER TABLE shortcuts_active RENAME TO shortcuts")
            
            active_count = con.execute("SELECT count(*) FROM shortcuts").fetchone()[0]
            
            if active_count == 0:
                break
            
            # Determine method for this resolution
            if sp_method == "HYBRID":
                method = "PURE" if res >= hybrid_res else "SCIPY"
            else:
                method = sp_method
            
            # Run SP on active shortcuts
            _run_shortest_paths_worker(con, "shortcuts", method=method)
            
            res_time = time.time() - res_start
            timing_info.append((res, method, res_time))
        
        # Get final results
        active_df = con.execute("SELECT * FROM shortcuts").df()
        deactivated_df = con.execute("SELECT * FROM deactivated").df()
        
        return (chunk_id, active_df, deactivated_df, len(active_df), timing_info)
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return (chunk_id, None, None, 0, [])
    finally:
        con.close()



def process_chunk_phase4(args):
    """
    Worker function for Phase 4 parallel processing.
    Uses in-memory DuckDB. Receives data as DataFrames.
    Returns: (cell_id, shortcuts_df, count, timing_info)
    """
    cell_id, edges_df, cell_df, partition_res, sp_method, hybrid_res = args
    
    # Each worker uses in-memory DuckDB
    con = duckdb.connect(":memory:")
    
    # Register H3 UDFs
    con.create_function("h3_lca", utils._find_lca_impl, ["BIGINT", "BIGINT"], "BIGINT")
    con.create_function("h3_resolution", utils._find_resolution_impl, ["BIGINT"], "INTEGER")
    con.create_function("h3_parent", utils._get_parent_cell_impl, ["BIGINT", "INTEGER"], "BIGINT")
    
    timing_info = []  # List of (res, method, time)
    
    try:
        # Load edges into worker's in-memory DB
        con.execute("CREATE TABLE edges AS SELECT * FROM edges_df")
        
        # Load cell data
        con.execute("CREATE TABLE cell_data AS SELECT * FROM cell_df")
        
        initial_count = len(cell_df)
        
        # Iterative backward loop: partition_res+1 -> 15
        for res in range(partition_res + 1, 16):
            _assign_cell_to_shortcuts_worker(con, res, "cell_data")
            
            # Separate active from inactive
            con.execute("CREATE OR REPLACE TABLE cell_active AS SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell FROM cell_data WHERE current_cell IS NOT NULL")
            con.execute("CREATE OR REPLACE TABLE cell_inactive AS SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell FROM cell_data WHERE current_cell IS NULL")
            
            active_count = con.execute("SELECT count(*) FROM cell_active").fetchone()[0]
            
            # Determine method for this resolution
            if sp_method == "HYBRID":
                method = "PURE" if res >= hybrid_res else "SCIPY"
            else:
                method = sp_method
            
            # Run SP on active shortcuts
            if active_count > 0:
                res_start = time.time()
                _run_shortest_paths_worker(con, "cell_active", method=method)
                timing_info.append((res, method, time.time() - res_start))
            
            # Merge back
            con.execute("CREATE OR REPLACE TABLE cell_data AS SELECT * FROM cell_active UNION ALL SELECT * FROM cell_inactive")
            
            remaining = con.execute("SELECT count(*) FROM cell_data").fetchone()[0]
            if remaining == 0:
                break
        
        # Get final result
        result_df = con.execute("SELECT * FROM cell_data").df()
        final_count = len(result_df)
        
        return (cell_id, result_df, final_count, timing_info)
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return (cell_id, None, 0, [])
    finally:
        con.close()


def _assign_cell_to_shortcuts_worker(con, res: int, input_table: str):
    """Worker version of assign_cell_to_shortcuts."""
    con.execute(f"DROP TABLE IF EXISTS {input_table}_tmp")
    
    if res == -1:
        con.execute(f"""
            CREATE TABLE {input_table}_tmp AS
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, 0::BIGINT AS current_cell
            FROM {input_table}
        """)
    else:
        con.execute(f"""
            CREATE TABLE {input_table}_tmp AS
            WITH with_parents AS (
                SELECT 
                    from_edge, to_edge, cost, via_edge, lca_res, 
                    inner_cell, outer_cell, inner_res, outer_res,
                    CASE WHEN inner_res >= {res} 
                         THEN h3_parent(inner_cell::BIGINT, {res}) 
                         ELSE NULL END AS inner_parent,
                    CASE WHEN outer_res >= {res} 
                         THEN h3_parent(outer_cell::BIGINT, {res}) 
                         ELSE NULL END AS outer_parent
                FROM {input_table}
            )
            -- 1. Inner Parent (Active)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, inner_parent AS current_cell
            FROM with_parents 
            WHERE lca_res <= {res} AND inner_parent IS NOT NULL
            
            UNION
            
            -- 2. Outer Parent (Active)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, outer_parent AS current_cell
            FROM with_parents 
            WHERE lca_res <= {res} AND outer_parent IS NOT NULL
            
            UNION ALL
            
            -- 3. Inactive or Not Yet Active
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, NULL AS current_cell
            FROM with_parents 
            WHERE lca_res > {res} 
               OR (inner_parent IS NULL AND outer_parent IS NULL)
        """)
    
    con.execute(f"DROP TABLE {input_table}")
    con.execute(f"ALTER TABLE {input_table}_tmp RENAME TO {input_table}")


def _process_cell_forward_worker(con, table_name: str):
    """Worker version of process_cell_forward."""
    # Separate active and deactivated
    con.execute(f"""
        CREATE OR REPLACE TABLE shortcuts_to_process AS
        SELECT * FROM {table_name} WHERE current_cell IS NOT NULL
    """)
    
    active_count = con.execute("SELECT count(*) FROM shortcuts_to_process").fetchone()[0]
    
    if active_count > 0:
        # Run SP on active shortcuts
        _run_shortest_paths_worker(con, "shortcuts_to_process")
        
        # Replace original table
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"ALTER TABLE shortcuts_to_process RENAME TO {table_name}")
        new_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    else:
        con.execute(f"DELETE FROM {table_name}")
        new_count = 0
    
    con.execute("DROP TABLE IF EXISTS shortcuts_to_process")
    return active_count, new_count


def _run_shortest_paths_worker(con, input_table: str, method: str = "SCIPY"):
    """Worker version of run_shortest_paths. Supports SCIPY and PURE methods."""
    con.execute("DROP TABLE IF EXISTS sp_input")
    con.execute(f"""
        CREATE TEMPORARY TABLE sp_input AS 
        SELECT from_edge, to_edge, cost, via_edge, current_cell::BIGINT as current_cell 
        FROM {input_table}
    """)
    
    if method == "PURE":
        # Use pure DuckDB approach
        from sp_methods.pure import compute_shortest_paths_pure_duckdb
        compute_shortest_paths_pure_duckdb(con, quiet=True, input_table="sp_input")
        con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM sp_input")
    else:
        # SCIPY method
        df = con.execute("SELECT * FROM sp_input").df()
        results = []
        for cell, group in df.groupby('current_cell'):
            processed = process_partition_scipy(group)
            if not processed.empty:
                processed['current_cell'] = cell
                results.append(processed)
        
        if results:
            final_df = pd.concat(results, ignore_index=True)
            # Deduplicate
            idx = final_df.groupby(['from_edge', 'to_edge'])['cost'].idxmin()
            final_df = final_df.loc[idx]
            con.execute("""
                CREATE OR REPLACE TABLE shortcuts_next AS 
                SELECT from_edge, to_edge, cost, via_edge, current_cell FROM final_df
            """)
        else:
            con.execute(f"CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM sp_input WHERE 1=0")
    
    # Re-enrich
    table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'shortcuts_next'").fetchone()[0] > 0
    if table_exists:
        con.execute("""
            CREATE OR REPLACE TABLE shortcuts_next_enriched AS
            SELECT 
                s.from_edge, s.to_edge, s.cost, s.via_edge,
                GREATEST(e1.lca_res, e2.lca_res) as lca_res,
                h3_lca(e1.to_cell, e2.from_cell)::BIGINT as inner_cell,
                h3_lca(e1.from_cell, e2.to_cell)::BIGINT as outer_cell,
                h3_resolution(h3_lca(e1.to_cell, e2.from_cell))::TINYINT as inner_res,
                h3_resolution(h3_lca(e1.from_cell, e2.to_cell))::TINYINT as outer_res,
                s.current_cell
            FROM shortcuts_next s
            LEFT JOIN edges e1 ON s.from_edge = e1.id
            LEFT JOIN edges e2 ON s.to_edge = e2.id
        """)
        con.execute("DROP TABLE shortcuts_next")
        con.execute("ALTER TABLE shortcuts_next_enriched RENAME TO shortcuts_next")
    else:
        # Fallback empty table
        con.execute(f"""
            CREATE OR REPLACE TABLE shortcuts_next AS 
            SELECT from_edge, to_edge, cost, via_edge, 
                   0 as lca_res, 0::BIGINT as inner_cell, 0::BIGINT as outer_cell, 
                   0::TINYINT as inner_res, 0::TINYINT as outer_res, 0::BIGINT as current_cell
            FROM {input_table} WHERE 1=0
        """)
    
    con.execute(f"DROP TABLE IF EXISTS {input_table}")
    con.execute(f"ALTER TABLE shortcuts_next RENAME TO {input_table}")
    con.execute("DROP TABLE IF EXISTS sp_input")


class ParallelShortcutProcessor:
    """Parallel version of ShortcutProcessor with multiprocessing for Phase 1 and 4."""
    
    def __init__(self, db_path: str, forward_deactivated_table: str, backward_deactivated_table: str, 
                 partition_res: int = PARTITION_RES, elementary_table: str = "elementary_table",
                 sp_method: str = None, hybrid_res: int = 10):
        self.db_path = db_path
        self.con = utils.initialize_duckdb(db_path)
        self.forward_deactivated_table = forward_deactivated_table
        self.backward_deactivated_table = backward_deactivated_table
        self.partition_res = partition_res
        self.elementary_table = elementary_table
        self.sp_method = sp_method or SP_METHOD
        self.hybrid_res = hybrid_res
        self.current_cells = []
        
        # Ensure tables exist
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.forward_deactivated_table} (
                from_edge BIGINT, to_edge BIGINT, cost DOUBLE, via_edge BIGINT,
                lca_res INTEGER, inner_cell BIGINT, outer_cell BIGINT, 
                inner_res TINYINT, outer_res TINYINT, current_cell BIGINT
            )
        """)
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.backward_deactivated_table} (
                from_edge BIGINT, to_edge BIGINT, cost DOUBLE, via_edge BIGINT,
                lca_res INTEGER, inner_cell BIGINT, outer_cell BIGINT, 
                inner_res TINYINT, outer_res TINYINT, current_cell BIGINT
            )
        """)

    def load_shared_data(self, edges_file: str, graph_file: str):
        """Loads edges and initial shortcuts into the database."""
        if self.con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'edges'").fetchone()[0] > 0:
            logger.info("Shared data already loaded, skipping.")
            return
            
        logger.info("Loading shared data...")
        utils.read_edges(self.con, edges_file)
        utils.create_edges_cost_table(self.con, edges_file)
        utils.initial_shortcuts_table(self.con, graph_file)
        
        logger.info("Pre-calculating H3 metadata...")
        self.con.execute(f"""
            CREATE TABLE {self.elementary_table} AS
            SELECT 
                s.from_edge, s.to_edge, s.cost, s.via_edge,
                GREATEST(e1.lca_res, e2.lca_res) AS lca_res,
                h3_lca(e1.to_cell::BIGINT, e2.from_cell::BIGINT)::BIGINT AS inner_cell,
                h3_lca(e1.from_cell::BIGINT, e2.to_cell::BIGINT)::BIGINT AS outer_cell,
                h3_resolution(h3_lca(e1.to_cell::BIGINT, e2.from_cell::BIGINT))::TINYINT AS inner_res,
                h3_resolution(h3_lca(e1.from_cell::BIGINT, e2.to_cell::BIGINT))::TINYINT AS outer_res,
                CAST(NULL AS BIGINT) AS current_cell
            FROM shortcuts s
            LEFT JOIN edges e1 ON s.from_edge = e1.id
            LEFT JOIN edges e2 ON s.to_edge = e2.id
        """)
        self.con.execute("DROP TABLE shortcuts")

    def process_forward_phase1_parallel(self):
        """
        PARALLEL Phase 1: Process chunks concurrently using multiprocessing.
        Workers use in-memory DBs with data passed as DataFrames.
        """
        log_conf.log_section(logger, f"PHASE 1: PARALLEL FORWARD (15 -> {self.partition_res})")
        
        # Identify chunks
        self.con.execute(f"""
            CREATE OR REPLACE TABLE chunks AS
            SELECT DISTINCT h3_parent(c, {self.partition_res}) as cell_id
            FROM (
                SELECT inner_cell as c FROM {self.elementary_table} WHERE inner_cell IS NOT NULL AND h3_resolution(inner_cell) >= {self.partition_res}
                UNION ALL
                SELECT outer_cell as c FROM {self.elementary_table} WHERE outer_cell IS NOT NULL AND h3_resolution(outer_cell) >= {self.partition_res}
            )
            WHERE c != 0
        """)
        chunk_ids = [r[0] for r in self.con.execute("SELECT cell_id FROM chunks").fetchall()]
        logger.info(f"  Processing {len(chunk_ids)} chunks in parallel (max {MAX_WORKERS} workers)...")
        
        # Load edges once - shared by all workers
        edges_df = self.con.execute("SELECT * FROM edges").df()
        
        # Prepare args for workers - each gets edges + chunk-specific shortcuts
        args_list = []
        for chunk_id in chunk_ids:
            shortcuts_df = self.con.execute(f"""
                SELECT * FROM {self.elementary_table}
                WHERE h3_parent(inner_cell, {self.partition_res}) = {chunk_id}
                   OR h3_parent(outer_cell, {self.partition_res}) = {chunk_id}
            """).df()
            args_list.append((chunk_id, edges_df, shortcuts_df, self.partition_res, self.sp_method, self.hybrid_res))
        
        res_partition_cells = []
        total_deactivated = 0
        all_timing_info = []  # Collect timing from all workers
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_chunk_phase1, args): args[0] for args in args_list}
            
            for i, future in enumerate(as_completed(futures), 1):
                chunk_id, active_df, deactivated_df, count, timing_info = future.result()
                all_timing_info.extend(timing_info)
                
                # Save active shortcuts to cell table
                if active_df is not None and len(active_df) > 0:
                    self.con.execute(f"CREATE OR REPLACE TABLE cell_{chunk_id} AS SELECT * FROM active_df")
                    res_partition_cells.append(chunk_id)
                
                # Save deactivated shortcuts to forward_deactivated_table
                if deactivated_df is not None and len(deactivated_df) > 0:
                    self.con.execute(f"""
                        INSERT INTO {self.forward_deactivated_table}
                        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell
                        FROM deactivated_df
                    """)
                    total_deactivated += len(deactivated_df)
                
                logger.info(f"  [{i}/{len(chunk_ids)}] Chunk {chunk_id} complete. {count} active, {len(deactivated_df) if deactivated_df is not None else 0} deactivated")
        
        # Log PURE vs SCIPY timing summary
        if all_timing_info:
            pure_times = [t for res, method, t in all_timing_info if method == "PURE"]
            scipy_times = [t for res, method, t in all_timing_info if method == "SCIPY"]
            if pure_times:
                logger.info(f"  PURE timing: {len(pure_times)} calls, total {sum(pure_times):.1f}s, avg {sum(pure_times)/len(pure_times):.2f}s")
            if scipy_times:
                logger.info(f"  SCIPY timing: {len(scipy_times)} calls, total {sum(scipy_times):.1f}s, avg {sum(scipy_times)/len(scipy_times):.2f}s")
        
        logger.info(f"  Total deactivated from Phase 1: {total_deactivated}")
        self.current_cells = res_partition_cells
        return res_partition_cells

    def process_backward_phase4_parallel(self):
        """
        PARALLEL Phase 4: Process cells concurrently using multiprocessing.
        Workers use in-memory DBs with data passed as DataFrames.
        """
        log_conf.log_section(logger, f"PHASE 4: PARALLEL BACKWARD ({self.partition_res+1} -> 15)")
        
        cell_ids = self.current_cells
        total_shortcuts = sum(
            self.con.execute(f"SELECT count(*) FROM cell_{cell_id}").fetchone()[0] 
            for cell_id in cell_ids
        )
        logger.info(f"  Starting with {len(cell_ids)} cells ({total_shortcuts} shortcuts), max {MAX_WORKERS} workers...")
        
        # Load edges once - shared by all workers
        edges_df = self.con.execute("SELECT * FROM edges").df()
        
        # Prepare args - each worker gets edges + cell data
        args_list = []
        for cell_id in cell_ids:
            cell_df = self.con.execute(f"SELECT * FROM cell_{cell_id}").df()
            if len(cell_df) > 0:
                args_list.append((cell_id, edges_df, cell_df, self.partition_res, self.sp_method, self.hybrid_res))
        
        total_deactivated = self.con.execute(f"SELECT count(*) FROM {self.backward_deactivated_table}").fetchone()[0]
        
        all_timing_info = []  # Collect timing from all workers
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_chunk_phase4, args): args[0] for args in args_list}
            
            for i, future in enumerate(as_completed(futures), 1):
                cell_id, result_df, count, timing_info = future.result()
                all_timing_info.extend(timing_info)
                
                if result_df is not None and len(result_df) > 0:
                    # Insert into backward_deactivated
                    self.con.execute(f"""
                        INSERT INTO {self.backward_deactivated_table}
                        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell
                        FROM result_df
                    """)
                    total_deactivated += count
                
                # Drop cell table
                self.con.execute(f"DROP TABLE IF EXISTS cell_{cell_id}")
                logger.info(f"  [{i}/{len(cell_ids)}] Cell {cell_id}: {count} shortcuts, total: {total_deactivated}")
        
        # Log PURE vs SCIPY timing summary
        if all_timing_info:
            pure_times = [t for res, method, t in all_timing_info if method == "PURE"]
            scipy_times = [t for res, method, t in all_timing_info if method == "SCIPY"]
            if pure_times:
                logger.info(f"  PURE timing: {len(pure_times)} calls, total {sum(pure_times):.1f}s, avg {sum(pure_times)/len(pure_times):.2f}s")
            if scipy_times:
                logger.info(f"  SCIPY timing: {len(scipy_times)} calls, total {sum(scipy_times):.1f}s, avg {sum(scipy_times)/len(scipy_times):.2f}s")
        
        return total_deactivated

    def checkpoint(self):
        self.con.execute("CHECKPOINT")

    def vacuum(self):
        self.con.execute("VACUUM")

    def close(self):
        self.con.close()


# Import Phase 2 and 3 from original (they use sequential processing)
from processor import ShortcutProcessor as OriginalProcessor


def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    return f"{minutes}m {remaining_seconds}s"


def main():
    log_conf.setup_logging("parallel_shortcuts")
    log_conf.log_section(logger, f"PARALLEL SHORTCUTS GENERATION (WORKERS={MAX_WORKERS})")
    
    total_start = time.time()
    edges_file = str(config.EDGES_FILE)
    graph_file = str(config.GRAPH_FILE)
    
    db_path = ":memory:"
    if config.DUCKDB_PERSIST_DIR:
        db_path = str(Path(config.DUCKDB_PERSIST_DIR) / "parallel_db.db")
    
    # Use parallel processor for Phase 1 and 4
    processor = ParallelShortcutProcessor(
        db_path, "forward_deactivated_shortcuts", "backward_deactivated_shortcuts", 
        partition_res=PARTITION_RES
    )
    
    # Load data
    processor.load_shared_data(edges_file, graph_file)
    
    # PHASE 1: Parallel
    log_conf.log_section(logger, f"PHASE 1: PARALLEL FORWARD 15 -> {processor.partition_res}")
    phase1_start = time.time()
    res_partition_cells = processor.process_forward_phase1_parallel()
    logger.info(f"Phase 1 complete ({format_time(time.time() - phase1_start)}). Created {len(res_partition_cells)} cell tables.")
    processor.checkpoint()
    
    # PHASE 2: Sequential (use original processor for this)
    # We need to transfer state to original processor
    original = OriginalProcessor(
        db_path, "forward_deactivated_shortcuts", "backward_deactivated_shortcuts",
        partition_res=PARTITION_RES
    )
    original.current_cells = processor.current_cells
    
    log_conf.log_section(logger, f"PHASE 2: HIERARCHICAL CONSOLIDATION")
    phase2_start = time.time()
    original.process_forward_phase2_consolidation()
    logger.info(f"Phase 2 complete ({format_time(time.time() - phase2_start)}).")
    
    # PHASE 3: Sequential
    log_conf.log_section(logger, "PHASE 3: BACKWARD CONSOLIDATION")
    phase3_start = time.time()
    original.process_backward_phase3_consolidation()
    logger.info(f"Phase 3 complete ({format_time(time.time() - phase3_start)}).")
    
    # Transfer state back
    processor.current_cells = original.current_cells
    
    # PHASE 4: Parallel
    log_conf.log_section(logger, "PHASE 4: PARALLEL BACKWARD CHUNKED")
    phase4_start = time.time()
    processor.process_backward_phase4_parallel()
    logger.info(f"Phase 4 complete ({format_time(time.time() - phase4_start)}).")
    processor.checkpoint()
    processor.vacuum()
    
    # Finalize
    processor.con.execute(f"""
        CREATE OR REPLACE TABLE shortcuts AS
        SELECT from_edge, to_edge, MIN(cost) as cost, arg_min(via_edge, cost) as via_edge
        FROM {processor.backward_deactivated_table}
        GROUP BY from_edge, to_edge
    """)
    final_count = processor.con.execute("SELECT count(*) FROM shortcuts").fetchone()[0]
    logger.info(f"Final Count (after dedup): {final_count}")
    logger.info(f"Total time: {format_time(time.time() - total_start)}")
    
    processor.close()


if __name__ == "__main__":
    main()
