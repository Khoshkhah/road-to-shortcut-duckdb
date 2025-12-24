#!/usr/bin/env python3
"""
Main entry point for Shortcut Generation.

Usage:
    python main.py burnaby          # Run with config/burnaby.yaml
    python main.py somerset         # Run with config/somerset.yaml
    python main.py --config custom  # Run with config/custom.yaml
    python main.py --list           # List available configs
"""

import sys
import os
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config_loader import load_config, CONFIG_DIR


def list_configs():
    """List available configuration profiles."""
    print("Available configuration profiles:")
    for f in CONFIG_DIR.glob("*.yaml"):
        name = f.stem
        print(f"  - {name}")


def run_algorithm(cfg):
    """Run the appropriate algorithm based on config."""
    algo_name = cfg.algorithm.name.lower()
    workers = cfg.parallel.workers
    
    print(f"Running algorithm: {algo_name}")
    print(f"  District: {cfg.input.district}")
    print(f"  Edges: {cfg.input.edges_file}")
    print(f"  Graph: {cfg.input.graph_file}")
    print(f"  Output: {cfg.output.directory}/{cfg.output.shortcuts_file}")
    print(f"  Workers: {workers} ({'parallel' if workers > 1 else 'single-threaded'})")
    print(f"  SP Method: {cfg.algorithm.sp_method}")
    print(f"  Partition Res: {cfg.algorithm.partition_res}")
    print()
    
    # Set environment variables for the algorithms to use
    os.environ["SP_METHOD"] = cfg.algorithm.sp_method
    os.environ["DUCKDB_MEMORY_LIMIT"] = cfg.duckdb.memory_limit
    os.environ["DUCKDB_PERSIST_DIR"] = str(cfg.output.persist_dir)
    
    # Import and run the appropriate algorithm
    if algo_name == "partitioned":
        if workers > 1:
            # Parallel execution
            run_partitioned_parallel(cfg)
        else:
            run_partitioned_single(cfg)
    
    elif algo_name in ["hybrid", "scipy", "pure"]:
        print(f"Note: '{algo_name}' is deprecated. Use 'partitioned' with sp_method setting.")
        print(f"  Example: sp_method: '{algo_name.upper()}'")
        sys.exit(1)
    
    else:
        print(f"Unknown algorithm: {algo_name}")
        print("Use 'partitioned' with workers setting:")
        print("  workers: 1    -> single-threaded")
        print("  workers: N    -> parallel with N workers")
        sys.exit(1)


def format_time(seconds: float) -> str:
    """Format seconds into human-readable time string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}m{secs}s"


def run_partitioned_single(cfg):
    """Run partitioned algorithm in single-threaded mode."""
    import logging
    import logging_config as log_conf
    import utilities as utils
    from processor import ShortcutProcessor
    
    logger = logging.getLogger(__name__)
    log_conf.setup_logging(f"single_{cfg.input.district}", level=cfg.logging.level, verbose=cfg.logging.verbose)
    
    # Log config info at start
    log_conf.log_section(logger, "CONFIGURATION")
    logger.info(f"District: {cfg.input.district}")
    logger.info(f"Edges: {cfg.input.edges_file}")
    logger.info(f"Graph: {cfg.input.graph_file}")
    logger.info(f"Output: {cfg.output.directory}/{cfg.output.shortcuts_file}")
    logger.info(f"SP Method: {cfg.algorithm.sp_method}")
    if cfg.algorithm.sp_method == "HYBRID":
        logger.info(f"Hybrid Res: {cfg.algorithm.hybrid_res} (PURE for res >= {cfg.algorithm.hybrid_res}, SCIPY for res < {cfg.algorithm.hybrid_res})")
    logger.info(f"Partition Res: {cfg.algorithm.partition_res}")
    logger.info(f"Workers: {cfg.parallel.workers}")
    logger.info(f"DuckDB Memory: {cfg.duckdb.memory_limit}")
    
    # Setup paths
    persist_dir = Path(cfg.output.persist_dir)
    persist_dir.mkdir(parents=True, exist_ok=True)
    db_path = str(persist_dir / f"{cfg.input.district}_partitioned.db")
    
    # Delete existing DB if fresh_start is enabled
    if cfg.duckdb.fresh_start:
        import glob
        for f in glob.glob(f"{db_path}*"):
            Path(f).unlink()
            logger.info(f"Deleted: {f}")
    
    output_dir = Path(cfg.output.directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize DuckDB and load data
    print(f"Database: {db_path}")
    
    # Create processor with config parameters
    processor = ShortcutProcessor(
        db_path=db_path,
        forward_deactivated_table="deactivated_shortcuts",
        backward_deactivated_table="backward_deactivated",
        partition_res=cfg.algorithm.partition_res,
        elementary_table="elementary_shortcuts",
        sp_method=cfg.algorithm.sp_method,
        hybrid_res=cfg.algorithm.hybrid_res
    )
    
    # Load shared data (edges and graph)
    processor.load_shared_data(cfg.input.edges_file, cfg.input.graph_file)
    
    # Run the phases
    processor.process_forward_phase1()
    processor.process_forward_phase2_consolidation()
    processor.process_backward_phase3_consolidation()
    processor.process_backward_phase4_chunked()
    
    # Finalize and save output
    output_file = str(output_dir / cfg.output.shortcuts_file)
    processor.finalize_and_save(output_file)
    print(f"Saved shortcuts to: {output_file}")


def run_partitioned_parallel(cfg):
    """Run partitioned algorithm in parallel mode."""
    import time
    import logging
    import logging_config as log_conf
    from processor_parallel import ParallelShortcutProcessor, MAX_WORKERS
    from processor import ShortcutProcessor
    
    logger = logging.getLogger(__name__)
    log_conf.setup_logging(f"parallel_{cfg.input.district}", level=cfg.logging.level, verbose=cfg.logging.verbose)
    
    # Log config info at start
    log_conf.log_section(logger, "CONFIGURATION")
    logger.info(f"District: {cfg.input.district}")
    logger.info(f"Edges: {cfg.input.edges_file}")
    logger.info(f"Graph: {cfg.input.graph_file}")
    logger.info(f"Output: {cfg.output.directory}/{cfg.output.shortcuts_file}")
    logger.info(f"SP Method: {cfg.algorithm.sp_method}")
    if cfg.algorithm.sp_method == "HYBRID":
        logger.info(f"Hybrid Res: {cfg.algorithm.hybrid_res} (PURE for res >= {cfg.algorithm.hybrid_res}, SCIPY for res < {cfg.algorithm.hybrid_res})")
    logger.info(f"Partition Res: {cfg.algorithm.partition_res}")
    logger.info(f"Workers: {cfg.parallel.workers}")
    logger.info(f"DuckDB Memory: {cfg.duckdb.memory_limit}")
    
    # Setup paths
    persist_dir = Path(cfg.output.persist_dir)
    persist_dir.mkdir(parents=True, exist_ok=True)
    db_path = str(persist_dir / f"{cfg.input.district}_parallel.db")
    
    # Delete existing DB if fresh_start is enabled
    if cfg.duckdb.fresh_start:
        import glob
        for f in glob.glob(f"{db_path}*"):
            Path(f).unlink()
            logger.info(f"Deleted: {f}")
    
    output_dir = Path(cfg.output.directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Database: {db_path}")
    print(f"Workers: {cfg.parallel.workers}")
    
    # Override MAX_WORKERS from config
    import processor_parallel as parallel_module
    parallel_module.MAX_WORKERS = cfg.parallel.workers
    
    total_start = time.time()
    
    # Create parallel processor
    processor = ParallelShortcutProcessor(
        db_path=db_path,
        forward_deactivated_table="forward_deactivated",
        backward_deactivated_table="backward_deactivated",
        partition_res=cfg.algorithm.partition_res,
        elementary_table="elementary_shortcuts",
        sp_method=cfg.algorithm.sp_method,
        hybrid_res=cfg.algorithm.hybrid_res
    )
    
    # Load shared data
    processor.load_shared_data(cfg.input.edges_file, cfg.input.graph_file)
    
    # Check if we can resume from Phase 3 (forward_deactivated already populated)
    try:
        forward_count = processor.con.execute("SELECT count(*) FROM forward_deactivated").fetchone()[0]
    except:
        forward_count = 0
    
    if forward_count > 0:
        logger.info(f"Resuming: forward_deactivated has {forward_count:,} rows. Skipping Phase 1 & 2.")
        # Clear backward_deactivated for fresh Phase 3
        processor.con.execute("DELETE FROM backward_deactivated")
        res_partition_cells = []  # Not needed for Phase 3
    else:
        # Phase 1: Parallel (processor logs its own header)
        phase1_start = time.time()
        res_partition_cells = processor.process_forward_phase1_parallel()
        logger.info(f"Phase 1 complete ({format_time(time.time() - phase1_start)}). Created {len(res_partition_cells)} cell tables.")
        processor.checkpoint()
        
        # Phase 2 & 3: Sequential (use original processor)
        original = ShortcutProcessor(
            db_path=db_path,
            forward_deactivated_table="forward_deactivated",
            backward_deactivated_table="backward_deactivated",
            partition_res=cfg.algorithm.partition_res,
            elementary_table="elementary_shortcuts",
            sp_method=cfg.algorithm.sp_method,
            hybrid_res=cfg.algorithm.hybrid_res
        )
        original.current_cells = processor.current_cells
        
        # Phase 2: Sequential (processor logs its own header)
        phase2_start = time.time()
        original.process_forward_phase2_consolidation()
        logger.info(f"Phase 2 complete ({format_time(time.time() - phase2_start)}).")
    
    # Phase 3: Always runs (creates original processor if resuming)
    if forward_count > 0:
        original = ShortcutProcessor(
            db_path=db_path,
            forward_deactivated_table="forward_deactivated",
            backward_deactivated_table="backward_deactivated",
            partition_res=cfg.algorithm.partition_res,
            elementary_table="elementary_shortcuts",
            sp_method=cfg.algorithm.sp_method,
            hybrid_res=cfg.algorithm.hybrid_res
        )
        # Phase 3 needs current_cells to be non-empty to start
        original.current_cells = [0]  # Global cell - Phase 3 will populate from forward_deactivated
    
    # Phase 3: Sequential (processor logs its own header)
    phase3_start = time.time()
    original.process_backward_phase3_consolidation()
    logger.info(f"Phase 3 complete ({format_time(time.time() - phase3_start)}).")
    
    # Transfer state back
    processor.current_cells = original.current_cells
    
    # Phase 4: Parallel (processor logs its own header)
    phase4_start = time.time()
    processor.process_backward_phase4_parallel()
    logger.info(f"Phase 4 complete ({format_time(time.time() - phase4_start)}).")
    
    # Finalize
    log_conf.log_section(logger, "FINALIZING")
    processor.con.execute("""
        CREATE OR REPLACE TABLE shortcuts AS
        SELECT from_edge, to_edge, MIN(cost) as cost, arg_min(via_edge, cost) as via_edge
        FROM backward_deactivated
        GROUP BY from_edge, to_edge
    """)
    final_count = processor.con.execute("SELECT count(*) FROM shortcuts").fetchone()[0]
    logger.info(f"Final Count: {final_count}")
    logger.info(f"Total time: {format_time(time.time() - total_start)}")
    
    # Save output
    output_file = str(output_dir / cfg.output.shortcuts_file)
    processor.con.execute(f"""
        COPY shortcuts TO '{output_file}' (FORMAT PARQUET)
    """)
    print(f"Saved shortcuts to: {output_file}")
    
    processor.close()


def normalize_profile(profile: str) -> str:
    """Normalize profile input to just the profile name."""
    # Handle full paths like "config/somerset.yaml"
    if "/" in profile or "\\" in profile:
        profile = Path(profile).stem
    # Remove .yaml extension if present
    if profile.endswith(".yaml") or profile.endswith(".yml"):
        profile = Path(profile).stem
    return profile


def main():
    parser = argparse.ArgumentParser(
        description="Shortcut Generation with Config-based Settings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "profile",
        nargs="?",
        default="default",
        help="Config profile name (e.g., 'burnaby', 'somerset')"
    )
    parser.add_argument(
        "--config", "-c",
        help="Alternative way to specify config profile"
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available config profiles"
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_configs()
        return
    
    # Determine which config to use
    profile = args.config if args.config else args.profile
    profile = normalize_profile(profile)
    
    print(f"Loading config: {profile}")
    try:
        cfg = load_config(profile)
    except Exception as e:
        print(f"Error loading config '{profile}': {e}")
        print("\nAvailable configs:")
        list_configs()
        sys.exit(1)
    
    # Run the algorithm
    run_algorithm(cfg)


if __name__ == "__main__":
    main()
