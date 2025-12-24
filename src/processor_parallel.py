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
            
            # Expand from current_cell_in/out to current_cell
            con.execute("""
                CREATE OR REPLACE TABLE shortcuts_expanded AS
                -- Inner cell (always include if not null)
                SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                       inner_res, outer_res, current_cell_in AS current_cell
                FROM shortcuts
                WHERE current_cell_in IS NOT NULL
                UNION
                -- Outer cell (only if different from inner)
                SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                       inner_res, outer_res, current_cell_out AS current_cell
                FROM shortcuts
                WHERE current_cell_out IS NOT NULL 
                  AND (current_cell_in IS NULL OR current_cell_out != current_cell_in)
                UNION ALL
                -- Inactive (both NULL)
                SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                       inner_res, outer_res, NULL AS current_cell
                FROM shortcuts
                WHERE current_cell_in IS NULL AND current_cell_out IS NULL
            """)
            
            # Collect deactivated shortcuts (NULL current_cell)
            con.execute("""
                INSERT INTO deactivated
                SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell 
                FROM shortcuts_expanded WHERE current_cell IS NULL
            """)
            
            # Keep only active shortcuts
            con.execute("CREATE OR REPLACE TABLE shortcuts AS SELECT * FROM shortcuts_expanded WHERE current_cell IS NOT NULL")
            con.execute("DROP TABLE IF EXISTS shortcuts_expanded")
            
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
            
            # Determine method for this resolution
            if sp_method == "HYBRID":
                method = "PURE" if res >= hybrid_res else "SCIPY"
            else:
                method = sp_method
            
            # Run SP using _process_cell_backward_worker
            res_start = time.time()
            active_count, total_count = _process_cell_backward_worker(con, "cell_data", method=method)
            if active_count > 0:
                timing_info.append((res, method, time.time() - res_start))
            
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
    """
    Worker version of assign_cell_to_shortcuts.
    Adds current_cell_in and current_cell_out columns instead of creating UNION.
    """
    con.execute(f"DROP TABLE IF EXISTS {input_table}_tmp")
    
    if res == -1:
        # Global level: all shortcuts belong to cell 0
        con.execute(f"""
            CREATE TABLE {input_table}_tmp AS
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, 
                   0::BIGINT AS current_cell_in, 
                   0::BIGINT AS current_cell_out
            FROM {input_table}
        """)
    else:
        # Compute parent cells for inner and outer, store as separate columns
        con.execute(f"""
            CREATE TABLE {input_table}_tmp AS
            SELECT 
                from_edge, to_edge, cost, via_edge, lca_res, 
                inner_cell, outer_cell, inner_res, outer_res,
                CASE WHEN lca_res <= {res} AND inner_res >= {res} 
                     THEN h3_parent(inner_cell::BIGINT, {res}) 
                     ELSE NULL END AS current_cell_in,
                CASE WHEN lca_res <= {res} AND outer_res >= {res} 
                     THEN h3_parent(outer_cell::BIGINT, {res}) 
                     ELSE NULL END AS current_cell_out
            FROM {input_table}
        """)
    
    con.execute(f"DROP TABLE {input_table}")
    con.execute(f"ALTER TABLE {input_table}_tmp RENAME TO {input_table}")


def _process_cell_forward_worker(con, table_name: str):
    """
    Worker version of process_cell_forward.
    Expands current_cell_in/out to single current_cell column first.
    """
    # Step 1: Expand from current_cell_in/out to current_cell
    con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
    con.execute(f"""
        CREATE TABLE {table_name}_expanded AS
        -- Inner cell (always include if not null)
        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
               inner_res, outer_res, current_cell_in AS current_cell
        FROM {table_name}
        WHERE current_cell_in IS NOT NULL
        UNION
        -- Outer cell (only if different from inner)
        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
               inner_res, outer_res, current_cell_out AS current_cell
        FROM {table_name}
        WHERE current_cell_out IS NOT NULL 
          AND (current_cell_in IS NULL OR current_cell_out != current_cell_in)
        UNION ALL
        -- Inactive (both NULL)
        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
               inner_res, outer_res, NULL AS current_cell
        FROM {table_name}
        WHERE current_cell_in IS NULL AND current_cell_out IS NULL
    """)
    
    # Step 2: Separate active and deactivated
    con.execute(f"""
        CREATE OR REPLACE TABLE shortcuts_to_process AS
        SELECT * FROM {table_name}_expanded WHERE current_cell IS NOT NULL
    """)
    
    active_count = con.execute("SELECT count(*) FROM shortcuts_to_process").fetchone()[0]
    
    if active_count > 0:
        # Run SP on active shortcuts
        _run_shortest_paths_worker(con, "shortcuts_to_process")
        
        # Replace original table
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
        con.execute(f"ALTER TABLE shortcuts_to_process RENAME TO {table_name}")
        new_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    else:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM shortcuts_to_process WHERE 1=0")
        new_count = 0
    
    con.execute("DROP TABLE IF EXISTS shortcuts_to_process")
    return active_count, new_count


def _process_cell_backward_worker(con, table_name: str, method: str = "SCIPY"):
    """
    Worker version of process_cell_backward.
    Expands current_cell_in/out to single current_cell, then splits active/inactive, 
    runs SP only on active, merges back.
    """
    # Step 1: Expand from current_cell_in/out to current_cell
    con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
    con.execute(f"""
        CREATE TABLE {table_name}_expanded AS
        -- Inner cell (always include if not null)
        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
               inner_res, outer_res, current_cell_in AS current_cell
        FROM {table_name}
        WHERE current_cell_in IS NOT NULL
        UNION
        -- Outer cell (only if different from inner)
        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
               inner_res, outer_res, current_cell_out AS current_cell
        FROM {table_name}
        WHERE current_cell_out IS NOT NULL 
          AND (current_cell_in IS NULL OR current_cell_out != current_cell_in)
        UNION ALL
        -- Inactive (both NULL)
        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
               inner_res, outer_res, NULL AS current_cell
        FROM {table_name}
        WHERE current_cell_in IS NULL AND current_cell_out IS NULL
    """)
    
    # Step 2: Split active vs inactive
    con.execute(f"""
        CREATE OR REPLACE TABLE shortcuts_active AS
        SELECT * FROM {table_name}_expanded WHERE current_cell IS NOT NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE shortcuts_inactive AS
        SELECT * FROM {table_name}_expanded WHERE current_cell IS NULL
    """)
    
    active_count = con.execute("SELECT count(*) FROM shortcuts_active").fetchone()[0]
    
    if active_count > 0:
        _run_shortest_paths_worker(con, "shortcuts_active", method=method)
    
    # Merge active + inactive back into original table
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
    con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM shortcuts_active
        UNION ALL
        SELECT * FROM shortcuts_inactive
    """)
    
    total_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    
    # Cleanup
    con.execute("DROP TABLE IF EXISTS shortcuts_active")
    con.execute("DROP TABLE IF EXISTS shortcuts_inactive")
    
    return active_count, total_count


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

    def get_sp_method_for_resolution(self, res: int, is_forward: bool) -> str:
        """
        Determine which SP method to use based on resolution and phase direction.
        
        For HYBRID mode:
          - res >= hybrid_res: PURE
          - res < hybrid_res: SCIPY
            
        For PURE or SCIPY modes, always return that method.
        """
        if self.sp_method == "HYBRID":
            if res >= self.hybrid_res:
                return "PURE"
            else:
                return "SCIPY"
        else:
            return self.sp_method

    def clear_backward_deactivated_shortcuts(self):
        self.con.execute(f"DELETE FROM {self.backward_deactivated_table}")
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.backward_deactivated_table} (
                from_edge BIGINT, to_edge BIGINT, cost DOUBLE, via_edge BIGINT,
                lca_res INTEGER, inner_cell BIGINT, outer_cell BIGINT, inner_res TINYINT, outer_res TINYINT, current_cell BIGINT
            )
        """)

    def assign_cell_to_shortcuts(self, res: int, phase: int = 1, direction: str = "forward", input_table: str = "shortcuts", single_assignment: bool = False):
        """
        Assigns each shortcut to H3 cell(s) at resolution res.
        Instead of creating UNION (row duplication), adds current_cell_in and current_cell_out columns.
        Row expansion is deferred to process_cell_forward/backward.
        """
        self.con.execute(f"DROP TABLE IF EXISTS {input_table}_tmp")
        
        if res == -1:
            # Global level: all shortcuts belong to cell 0
            self.con.execute(f"""
                CREATE TABLE {input_table}_tmp AS
                SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell,
                       inner_res, outer_res, 
                       0::BIGINT AS current_cell_in, 
                       0::BIGINT AS current_cell_out
                FROM {input_table}
            """)
        else:
            # Compute parent cells for inner and outer, store as separate columns
            self.con.execute(f"""
                CREATE TABLE {input_table}_tmp AS
                SELECT 
                    from_edge, to_edge, cost, via_edge, lca_res, 
                    inner_cell, outer_cell, inner_res, outer_res,
                    CASE WHEN lca_res <= {res} AND inner_res >= {res} 
                         THEN h3_parent(inner_cell::BIGINT, {res}) 
                         ELSE NULL END AS current_cell_in,
                    CASE WHEN lca_res <= {res} AND outer_res >= {res} 
                         THEN h3_parent(outer_cell::BIGINT, {res}) 
                         ELSE NULL END AS current_cell_out
                FROM {input_table}
            """)
        
        self.con.execute(f"DROP TABLE {input_table}")
        self.con.execute(f"ALTER TABLE {input_table}_tmp RENAME TO {input_table}")

    def h3_get_children(self, cell_id: int, res: int) -> list[int]:
        """Helper to get children, handling the global parent 0."""
        if cell_id == 0:
            return [int(h, 16) for h in h3.get_res0_cells()]
        return [int(h, 16) for h in h3.cell_to_children(h3.int_to_str(cell_id), res)]

    def partition_to_children(self, child_res: int, child_list: list[int], input_table: str = "shortcuts"):
        """Partition shortcuts to child cells using hash join."""
        self.con.execute(f"ALTER TABLE {input_table} DROP COLUMN IF EXISTS current_cell")    
        
        if not child_list:
            self.con.execute(f"ALTER TABLE {input_table} ADD COLUMN IF NOT EXISTS current_cell BIGINT DEFAULT NULL")
            return

        self.con.execute("DROP TABLE IF EXISTS _child_cells")
        self.con.execute("CREATE TEMP TABLE _child_cells (cell_id BIGINT PRIMARY KEY)")
        self.con.execute("INSERT INTO _child_cells VALUES " + ",".join(f"({c})" for c in child_list))

        self.con.execute(f"DROP TABLE IF EXISTS {input_table}_tmp")
        self.con.execute(f"""
            CREATE TABLE {input_table}_tmp AS
            WITH with_parents AS (
                SELECT from_edge, to_edge, cost, via_edge, lca_res, 
                    inner_cell, outer_cell, inner_res, outer_res,
                    h3_parent(inner_cell::BIGINT, {child_res}) AS inner_parent,
                    h3_parent(outer_cell::BIGINT, {child_res}) AS outer_parent
                FROM {input_table}
            ),
            inner_matches AS (
                SELECT p.*, p.inner_parent AS current_cell
                FROM with_parents p
                INNER JOIN _child_cells c ON p.inner_parent = c.cell_id
            ),
            outer_matches AS (
                SELECT p.*, p.outer_parent AS current_cell
                FROM with_parents p
                INNER JOIN _child_cells c ON p.outer_parent = c.cell_id
                WHERE p.inner_parent IS DISTINCT FROM p.outer_parent
            ),
            no_matches AS (
                SELECT p.*, NULL::BIGINT AS current_cell
                FROM with_parents p
                LEFT JOIN _child_cells c1 ON p.inner_parent = c1.cell_id
                LEFT JOIN _child_cells c2 ON p.outer_parent = c2.cell_id
                WHERE c1.cell_id IS NULL AND c2.cell_id IS NULL
            )
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell FROM inner_matches
            UNION ALL
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell FROM outer_matches
            UNION ALL
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell FROM no_matches
        """)

        self.con.execute(f"DROP TABLE {input_table}")
        self.con.execute(f"ALTER TABLE {input_table}_tmp RENAME TO {input_table}")
        self.con.execute("DROP TABLE IF EXISTS _child_cells")

    def process_cell_forward(self, table_name: str, method: str = SP_METHOD):
        """
        Processes shortcuts for forward pass with deactivation.
        Expands current_cell_in/out to single current_cell column first.
        """
        # Step 1: Expand from current_cell_in/out to current_cell
        self.con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
        self.con.execute(f"""
            CREATE TABLE {table_name}_expanded AS
            -- Inner cell (always include if not null)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, current_cell_in AS current_cell
            FROM {table_name}
            WHERE current_cell_in IS NOT NULL
            UNION
            -- Outer cell (only if different from inner)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, current_cell_out AS current_cell
            FROM {table_name}
            WHERE current_cell_out IS NOT NULL 
              AND (current_cell_in IS NULL OR current_cell_out != current_cell_in)
            UNION ALL
            -- Inactive (both NULL)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, NULL AS current_cell
            FROM {table_name}
            WHERE current_cell_in IS NULL AND current_cell_out IS NULL
        """)
        
        # Step 2: Separate active and deactivated
        self.con.execute(f"DROP TABLE IF EXISTS shortcuts_to_process")
        self.con.execute(f"CREATE TEMPORARY TABLE shortcuts_to_process AS SELECT * FROM {table_name}_expanded WHERE current_cell IS NOT NULL")
        
        self.con.execute(f"""
            INSERT INTO {self.forward_deactivated_table}
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell
            FROM {table_name}_expanded WHERE current_cell IS NULL
        """)
        deactivated_count = self.con.sql(f"SELECT count(*) FROM {table_name}_expanded WHERE current_cell IS NULL").fetchone()[0]
        
        active_count = self.con.sql("SELECT count(*) FROM shortcuts_to_process").fetchone()[0]
        
        new_count = 0
        if active_count > 0:
            self.run_shortest_paths(method=method, quiet=True, input_table="shortcuts_to_process")
            self.con.execute(f"DROP TABLE {table_name}")
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
            self.con.execute(f"ALTER TABLE shortcuts_to_process RENAME TO {table_name}")
            new_count = self.con.sql(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        else:
            self.con.execute(f"DROP TABLE {table_name}")
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
            self.con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM shortcuts_to_process WHERE 1=0")
            new_count = 0
            
        self.con.execute("DROP TABLE IF EXISTS shortcuts_to_process")
        self.con.execute("DROP TABLE IF EXISTS shortcuts_next")
        
        return active_count, new_count, deactivated_count

    def process_cell_backward(self, table_name: str, method: str = SP_METHOD):
        """
        Processes shortcuts for backward pass:
        1. Expand from current_cell_in/out to current_cell
        2. Split into active (current_cell IS NOT NULL) and inactive (current_cell IS NULL)
        3. Run SP only on active shortcuts
        4. Merge back with inactive shortcuts
        """
        # Step 1: Expand from current_cell_in/out to current_cell
        self.con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
        self.con.execute(f"""
            CREATE TABLE {table_name}_expanded AS
            -- Inner cell (always include if not null)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, current_cell_in AS current_cell
            FROM {table_name}
            WHERE current_cell_in IS NOT NULL
            UNION
            -- Outer cell (only if different from inner)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, current_cell_out AS current_cell
            FROM {table_name}
            WHERE current_cell_out IS NOT NULL 
              AND (current_cell_in IS NULL OR current_cell_out != current_cell_in)
            UNION ALL
            -- Inactive (both NULL)
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, 
                   inner_res, outer_res, NULL AS current_cell
            FROM {table_name}
            WHERE current_cell_in IS NULL AND current_cell_out IS NULL
        """)
        
        # Step 2: Split active vs inactive
        self.con.execute(f"""
            CREATE OR REPLACE TABLE shortcuts_active AS
            SELECT * FROM {table_name}_expanded WHERE current_cell IS NOT NULL
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE shortcuts_inactive AS
            SELECT * FROM {table_name}_expanded WHERE current_cell IS NULL
        """)
        
        active_count = self.con.sql("SELECT count(*) FROM shortcuts_active").fetchone()[0]
        
        new_count = 0
        if active_count > 0:
            # Run SP only on active shortcuts
            self.run_shortest_paths(method=method, quiet=True, input_table="shortcuts_active")
            new_count = self.con.sql("SELECT count(*) FROM shortcuts_active").fetchone()[0]
        
        # Merge active + inactive back into original table
        self.con.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.con.execute(f"DROP TABLE IF EXISTS {table_name}_expanded")
        self.con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM shortcuts_active
            UNION ALL
            SELECT * FROM shortcuts_inactive
        """)
        
        total_count = self.con.sql(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        
        # Cleanup
        self.con.execute("DROP TABLE IF EXISTS shortcuts_active")
        self.con.execute("DROP TABLE IF EXISTS shortcuts_inactive")
            
        return active_count, total_count, 0


    def run_shortest_paths(self, method: str = None, quiet: bool = True, input_table: str = "shortcuts"):
        """Run shortest paths on input table with slim and enrich optimization."""
        if method is None:
            method = SP_METHOD
        
        self.con.execute("DROP TABLE IF EXISTS sp_input")
        self.con.execute(f"CREATE TEMPORARY TABLE sp_input AS SELECT from_edge, to_edge, cost, via_edge, current_cell::BIGINT as current_cell FROM {input_table}")
        
        if method == "PURE":
            compute_shortest_paths_pure_duckdb(self.con, quiet=quiet, input_table="sp_input")
            self.con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM sp_input")
        elif method == "SCIPY":
            df = self.con.sql("SELECT * FROM sp_input").df()
            results = []
            for cell, group in df.groupby('current_cell'):
                processed = process_partition_scipy(group)
                if not processed.empty:
                    processed['current_cell'] = cell
                    results.append(processed)
            
            if results:
                final_df = pd.concat(results, ignore_index=True)
                idx = final_df.groupby(['from_edge', 'to_edge'])['cost'].idxmin()
                final_df = final_df.loc[idx]
                self.con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT from_edge, to_edge, cost, via_edge, current_cell FROM final_df")
            else:
                self.con.execute(f"CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM sp_input WHERE 1=0")

        table_exists = self.con.sql("SELECT count(*) FROM information_schema.tables WHERE table_name = 'shortcuts_next'").fetchone()[0] > 0
        if table_exists:
            self.con.execute("""
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
            self.con.execute("DROP TABLE shortcuts_next")
            self.con.execute("ALTER TABLE shortcuts_next_enriched RENAME TO shortcuts_next")
        else:
            self.con.execute(f"""
                CREATE OR REPLACE TABLE shortcuts_next AS 
                SELECT from_edge, to_edge, cost, via_edge, 
                       0 as lca_res, 0::BIGINT as inner_cell, 0::BIGINT as outer_cell, 
                       0::TINYINT as inner_res, 0::TINYINT as outer_res, 0::BIGINT as current_cell
                FROM {input_table} WHERE 1=0
            """)

        self.con.execute(f"DROP TABLE IF EXISTS {input_table}")
        self.con.execute(f"ALTER TABLE shortcuts_next RENAME TO {input_table}")
        self.con.execute("DROP TABLE IF EXISTS sp_input")

    def process_forward_phase2_consolidation(self):
        """Phase 2: Hierarchical Consolidation (Forward Pass)"""
        log_conf.log_section(logger, f"PHASE 2: HIERARCHICAL CONSOLIDATION ({self.partition_res-1} -> 0)")
        logger.info(f"  Starting Phase 2 with {len(self.current_cells)} cell tables.")

        for target_res in range(self.partition_res - 1, -2, -1):
            res_start = time.time()
            
            parent_to_children = {}
            for cell_id in self.current_cells:
                parent_id = self.con.execute(f"SELECT h3_parent({cell_id}, {target_res})").fetchone()[0] if target_res >= 0 else 0
                if parent_id not in parent_to_children:
                    parent_to_children[parent_id] = []
                parent_to_children[parent_id].append(cell_id)
            
            logger.info(f"  Resolution {target_res}: {len(self.current_cells)} cells -> {len(parent_to_children)} parent cells.")
            
            new_cells = []
            for parent_id, children in parent_to_children.items():
                cell_start = time.time()
                
                valid_children = [child for child in children 
                    if self.con.sql(f"SELECT count(*) FROM information_schema.tables WHERE table_name = 'cell_{child}'").fetchone()[0] > 0]
                
                if not valid_children:
                    continue

                merge_sql = " UNION ALL ".join([f"SELECT * FROM cell_{child}" for child in valid_children])
                self.con.execute(f"""
                    CREATE OR REPLACE TABLE cell_{parent_id}_tmp AS
                    SELECT from_edge, to_edge, MIN(cost) as cost, arg_min(via_edge, cost) as via_edge,
                        FIRST(lca_res) as lca_res, FIRST(inner_cell) as inner_cell, FIRST(outer_cell) as outer_cell,
                        FIRST(inner_res) as inner_res, FIRST(outer_res) as outer_res, current_cell
                    FROM ({merge_sql})
                    GROUP BY from_edge, to_edge, current_cell
                """)
                
                for child in valid_children:
                    if child != parent_id:
                        self.con.execute(f"DROP TABLE IF EXISTS cell_{child}")
                
                self.con.execute(f"DROP TABLE IF EXISTS cell_{parent_id}")
                self.con.execute(f"ALTER TABLE cell_{parent_id}_tmp RENAME TO cell_{parent_id}")
                
                merged_count = self.con.sql(f"SELECT count(*) FROM cell_{parent_id}").fetchone()[0]
                self.assign_cell_to_shortcuts(target_res, input_table=f"cell_{parent_id}")
                active, news, decs = self.process_cell_forward(f"cell_{parent_id}")
                new_cells.append(parent_id)
                
                logger.info(f"    Parent {parent_id}: {len(valid_children)} children, {merged_count} merged -> {active} active -> {news} pool, {decs} deactivated ({format_time(time.time() - cell_start)})")
            
            self.con.execute("DROP TABLE IF EXISTS shortcuts_active")
            self.con.execute("DROP TABLE IF EXISTS shortcuts_next")
            self.checkpoint()
            
            self.current_cells = list(set(new_cells))
            logger.info(f"  Res {target_res} complete in {format_time(time.time() - res_start)}. Active cells: {len(self.current_cells)}, Deactivated: {self.con.sql(f'SELECT count(*) FROM {self.forward_deactivated_table}').fetchone()[0]}")

        remaining_active = 0
        for cell_id in self.current_cells:
            count = self.con.sql(f"SELECT count(*) FROM cell_{cell_id}").fetchone()[0]
            remaining_active += count
            self.con.execute(f"INSERT INTO {self.forward_deactivated_table} SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell FROM cell_{cell_id}")
            self.con.execute(f"DROP TABLE cell_{cell_id}")
        
        total_forward = self.con.sql(f"SELECT count(*) FROM {self.forward_deactivated_table}").fetchone()[0]
        
        self.con.execute(f"""
            CREATE OR REPLACE TABLE {self.forward_deactivated_table}_dedup AS
            SELECT from_edge, to_edge, MIN(cost) as cost, arg_min(via_edge, cost) as via_edge,
                FIRST(lca_res) as lca_res, FIRST(inner_cell) as inner_cell, FIRST(outer_cell) as outer_cell, 
                FIRST(inner_res) as inner_res, FIRST(outer_res) as outer_res, FIRST(current_cell) as current_cell
            FROM {self.forward_deactivated_table}
            GROUP BY from_edge, to_edge
        """)
        self.con.execute(f"DROP TABLE {self.forward_deactivated_table}")
        self.con.execute(f"ALTER TABLE {self.forward_deactivated_table}_dedup RENAME TO {self.forward_deactivated_table}")
        dedup_count = self.con.sql(f"SELECT count(*) FROM {self.forward_deactivated_table}").fetchone()[0]
        
        logger.info("--------------------------------------------------")
        logger.info(f"  Remaining active at Res -1: {remaining_active}")
        logger.info(f"  Total deactivated (before dedup): {total_forward}")  
        logger.info(f"  Deduplicated forward results: {dedup_count}")
        
        return dedup_count

    def process_backward_phase3_consolidation(self):
        """Phase 3: Backward Consolidation (0 -> partition_res)"""
        log_conf.log_section(logger, f"PHASE 3: BACKWARD CONSOLIDATION (0 -> {self.partition_res})")
        if not self.current_cells:
            logger.warning("No cells to process in Phase 3.")
            return

        self.con.execute(f"""
            CREATE OR REPLACE TABLE shortcuts AS
            SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res
            FROM {self.forward_deactivated_table}
        """)
        forward_count = self.con.sql("SELECT count(*) FROM shortcuts").fetchone()[0]
        logger.info(f"Starting backward consolidation with {forward_count} shortcuts from forward pass.")

        self.con.execute("DROP TABLE IF EXISTS cell_0")
        self.con.execute("ALTER TABLE shortcuts RENAME TO cell_0")
        self.current_cells = [0]
        logger.info(f"  Starting with {forward_count} shortcuts in global cell_0.")

        for target_res in range(-1, self.partition_res):
            res_start = time.time()
            total_deactivated = 0
            child_res = target_res + 1
            list_children_cells = []
            
            for parent_cell in self.current_cells:
                if self.con.sql(f"SELECT count(*) FROM information_schema.tables WHERE table_name = 'cell_{parent_cell}'").fetchone()[0] == 0:
                    continue

                active_children = set()
                children_for_parent = self.h3_get_children(parent_cell, child_res)
                t_partition = time.time()
                self.partition_to_children(child_res, children_for_parent, input_table=f"cell_{parent_cell}")
                t_partition = time.time() - t_partition
                
                null_count = self.con.execute(f"SELECT COUNT(*) FROM cell_{parent_cell} WHERE current_cell IS NULL").fetchone()[0]
                if null_count > 0:
                    self.con.execute(f"""
                        INSERT INTO {self.backward_deactivated_table}
                        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell
                        FROM cell_{parent_cell} WHERE current_cell IS NULL
                    """)
                    total_deactivated += null_count
                
                self.con.execute(f"""
                    CREATE OR REPLACE TABLE current_splits AS
                    SELECT DISTINCT current_cell FROM cell_{parent_cell} WHERE current_cell IS NOT NULL
                """)
                child_ids = [r[0] for r in self.con.execute("SELECT current_cell FROM current_splits").fetchall()]
                
                for child_id in child_ids:
                    self.con.execute(f"""
                        CREATE OR REPLACE TABLE cell_{child_id} AS
                        SELECT from_edge, to_edge, cost, via_edge, lca_res, inner_cell, outer_cell, inner_res, outer_res, current_cell
                        FROM cell_{parent_cell} WHERE current_cell = {child_id}
                    """)
                    active_children.add(child_id)
                
                self.con.execute(f"DROP TABLE IF EXISTS cell_{parent_cell}")
              
                for child_id in active_children:
                    child_start = time.time()
                    child_count = self.con.sql(f"SELECT count(*) FROM cell_{child_id}").fetchone()[0]
                    if child_count == 0:
                        self.con.execute(f"DROP TABLE IF EXISTS cell_{child_id}")
                        continue
                    
                    t_assign = time.time()
                    self.assign_cell_to_shortcuts(child_res, input_table=f"cell_{child_id}")
                    t_assign = time.time() - t_assign
                    
                    # Use process_cell_backward to split active/inactive, run SP, merge back
                    t_sp = time.time()
                    method = self.get_sp_method_for_resolution(child_res, is_forward=False)
                    active_count, news, _ = self.process_cell_backward(f"cell_{child_id}", method=method)
                    t_sp = time.time() - t_sp
                    
                    logger.info(f"      Cell {child_id}: {child_count} -> {news} [assign={t_assign:.2f}s, partition={t_partition:.2f}s, SP={t_sp:.2f}s]")
                    
                self.con.execute("DROP TABLE IF EXISTS current_splits")
                self.con.execute("DROP TABLE IF EXISTS shortcuts")
                self.con.execute("DROP TABLE IF EXISTS shortcuts_next")
                self.con.execute("DROP TABLE IF EXISTS shortcuts_active")
                self.con.execute("DROP TABLE IF EXISTS children_list")
                self.checkpoint()
                
                list_children_cells += list(active_children)
            self.current_cells = list_children_cells
            
            self.vacuum()
            logger.info(f"  Res {target_res} -> {child_res} complete in {format_time(time.time() - res_start)}. Active cells: {len(list_children_cells)}, Deactivated so far: {self.con.sql(f'SELECT count(*) FROM {self.backward_deactivated_table}').fetchone()[0]}")

        remaining_active = sum(
            self.con.sql(f"SELECT count(*) FROM cell_{cell_id}").fetchone()[0]
            for cell_id in self.current_cells
        )
        total_backward = self.con.sql(f"SELECT count(*) FROM {self.backward_deactivated_table}").fetchone()[0]
        logger.info("--------------------------------------------------")
        logger.info(f"  Summary: {len(self.current_cells)} cells ({remaining_active} shortcuts) remain for Phase 4. Deactivated: {total_backward}")
        
        return total_backward

    def finalize_and_save(self, output_path: str):
        """Deduplicates backward pass results and saves output."""
        log_conf.log_section(logger, "FINALIZING")
        
        self.con.execute(f"""
            CREATE OR REPLACE TABLE shortcuts_final AS
            SELECT from_edge, to_edge, MIN(cost) as cost, arg_min(via_edge, cost) as via_edge
            FROM {self.backward_deactivated_table}
            GROUP BY from_edge, to_edge
        """)
        
        final_count = self.con.sql("SELECT COUNT(*) FROM shortcuts_final").fetchone()[0]
        logger.info(f"Final Count (after dedup): {final_count}")
        
        self.con.execute("DROP TABLE IF EXISTS shortcuts")
        self.con.execute("ALTER TABLE shortcuts_final RENAME TO shortcuts")

        utils.add_final_info(self.con)
        utils.save_output(self.con, output_path)
        self.close()
        return final_count



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
    
    # Use parallel processor for all phases
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
    
    # PHASE 2: Sequential (using ParallelShortcutProcessor methods)
    log_conf.log_section(logger, f"PHASE 2: HIERARCHICAL CONSOLIDATION")
    phase2_start = time.time()
    processor.process_forward_phase2_consolidation()
    logger.info(f"Phase 2 complete ({format_time(time.time() - phase2_start)}).")
    
    # PHASE 3: Sequential (using ParallelShortcutProcessor methods)
    log_conf.log_section(logger, "PHASE 3: BACKWARD CONSOLIDATION")
    phase3_start = time.time()
    processor.process_backward_phase3_consolidation()
    logger.info(f"Phase 3 complete ({format_time(time.time() - phase3_start)}).")
    
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
