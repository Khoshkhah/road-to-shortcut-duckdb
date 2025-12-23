# Configuration Guide

## Overview

All configuration is centralized in `src/config.py`. This guide explains each setting.

## DuckDB Settings

```python
# Memory limit for DuckDB operations
DUCKDB_MEMORY_LIMIT = "16GB"

# Directory for persistent database files
DUCKDB_PERSIST_DIR = "persist/"
```

### Memory Considerations

- **Small datasets (<10K edges)**: 4GB is sufficient
- **Medium datasets (10K-50K edges)**: 8-16GB recommended
- **Large datasets (>50K edges)**: Use partitioned algorithm

## Input File Paths

```python
# District to process (change this for different datasets)
DISTRICT_NAME = "Burnaby"

# Edge data with H3 indices
EDGES_FILE = f"/path/to/{DISTRICT_NAME}_edges_with_h3.csv"

# Edge graph (connectivity)
GRAPH_FILE = f"/path/to/{DISTRICT_NAME}_edge_graph.csv"
```

### Required Input Format

**edges_with_h3.csv**:
```csv
id,from_cell,to_cell,lca_res,cost,...
1,644733694722389744,644733694722358043,12,1.5,...
```

**edge_graph.csv**:
```csv
from_edge,to_edge
1,2
1,3
2,4
```

## Output Settings

```python
# Output directory
OUTPUT_DIR = "output/"

# Output file (without extension - will be saved as parquet)
SHORTCUTS_OUTPUT_FILE = "output/Burnaby_shortcuts"
```

## Resolution Parameters

```python
# H3 resolution range (typically 0-15)
MIN_H3_RESOLUTION = 0
MAX_H3_RESOLUTION = 15
```

## Algorithm-Specific Settings

### Hybrid Algorithm

Edit `generate_shortcuts_hybrid.py` to adjust resolution thresholds:

```python
# Resolutions using Scipy (larger cells, faster)
SCIPY_RESOLUTIONS = list(range(-1, 10))

# Resolutions using Pure DuckDB (smaller cells, simpler)
PURE_RESOLUTIONS = list(range(10, 16))
```

### Partitioned Algorithm

Edit `generate_shortcuts_partitioned.py`:

```python
# Resolution for partitioning (typically 6-8)
PARTITION_RES = 7
```

Lower values = fewer partitions, more memory per partition
Higher values = more partitions, less memory per partition

## Environment Variables

Override config via environment:

```bash
export DUCKDB_MEMORY_LIMIT="32GB"
export DUCKDB_PERSIST_DIR="/tmp/duckdb_persist"
python generate_shortcuts_hybrid.py
```
