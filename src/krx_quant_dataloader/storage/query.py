"""
Storage query layer for Parquet database

What this module does:
- Generic Parquet table queries with Hive partition optimization
- Data-type-neutral: works for snapshots, adj_factors, liquidity_ranks, and future tables
- Returns Pandas DataFrame (user-friendly default)
- Optimizations: partition pruning, row-group pruning, column pruning

Interaction:
- Called by Layer 2 Services (UniverseService, QueryEngine)
- Uses PyArrow internally for zero-copy performance
- Missing partitions (holidays) handled gracefully
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc


# Universe name to rank threshold mapping
_UNIVERSE_RANK_MAP = {
    'univ100': 100,
    'univ200': 200,
    'univ500': 500,
    'univ1000': 1000,
    'univ2000': 2000,
}


def query_parquet_table(
    db_path: str | Path,
    table_name: str,
    *,
    start_date: str,
    end_date: str,
    symbols: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Generic Parquet table query with Hive partition optimization.
    
    This is a data-type-neutral operation that reads any Hive-partitioned
    table (snapshots, adj_factors, liquidity_ranks, or future tables like
    daily_pbr, daily_per, etc.).
    
    Parameters
    ----------
    db_path : str | Path
        Root path to data directory (e.g., './data/').
    table_name : str
        Table name (subdirectory): 'snapshots', 'adj_factors', 'liquidity_ranks'.
    start_date : str
        Start date (YYYYMMDD format, inclusive).
    end_date : str
        End date (YYYYMMDD format, inclusive).
    symbols : Optional[List[str]]
        Stock codes to filter (e.g., ['005930', '000660']).
        If None, returns all symbols.
        Filter uses row-group pruning (leverages sorted writes by ISU_SRT_CD).
    fields : Optional[List[str]]
        Column names to retrieve (column pruning).
        If None, returns all columns.
        Note: TRD_DD is always included (injected from partition names).
    
    Returns
    -------
    pd.DataFrame
        Query results with TRD_DD as column.
        Empty DataFrame if no data found (holidays, missing date range).
    
    Optimizations Applied
    ---------------------
    1. Partition pruning: Only read TRD_DD=[start_date, end_date] partitions
    2. Column pruning: Only read requested fields (reduces I/O)
    3. Row-group pruning: Filter by ISU_SRT_CD (leverages sorted writes)
    
    Missing Partitions
    ------------------
    - Holidays/non-trading days have no partitions → silently skipped
    - Returns available data without error
    - Caller responsible for handling gaps (e.g., forward-fill)
    
    Performance (SSD)
    -----------------
    - 100 stocks × 252 days: ~100-500ms
    - 500 stocks × 252 days: ~200-800ms
    - Full market (3000 × 252): ~1-3 seconds
    
    Example
    -------
    >>> # Query snapshot data (market prices)
    >>> df = query_parquet_table(
    ...     db_path='./data/',
    ...     table_name='snapshots',
    ...     start_date='20240101',
    ...     end_date='20241231',
    ...     symbols=['005930', '000660'],
    ...     fields=['TRD_DD', 'ISU_SRT_CD', 'TDD_CLSPRC', 'ACC_TRDVAL']
    ... )
    """
    db_path = Path(db_path)
    table_path = db_path / table_name
    
    # Discover available partitions in date range
    partition_names = _discover_partitions(table_path, start_date, end_date)
    
    if not partition_names:
        # No partitions found (all holidays or missing data)
        # Return empty DataFrame with expected columns
        if fields:
            return pd.DataFrame(columns=fields)
        else:
            return pd.DataFrame()
    
    # Build symbol filter if provided
    filters = None
    if symbols:
        filters = _build_symbol_filter(symbols)
    
    # Read partitions with PyArrow (zero-copy, optimized)
    table = _read_partitions_pyarrow(
        table_path=table_path,
        partition_names=partition_names,
        columns=fields,
        filters=filters,
    )
    
    # Convert to Pandas DataFrame
    df = table.to_pandas()
    
    # Ensure TRD_DD is string type (consistent with partition names)
    if 'TRD_DD' in df.columns:
        df['TRD_DD'] = df['TRD_DD'].astype(str)
    
    return df


def load_universe_symbols(
    db_path: str | Path,
    universe_name: str,
    *,
    start_date: str,
    end_date: str,
) -> Dict[str, List[str]]:
    """
    Load pre-computed universe symbol lists per date.
    
    This function reads the liquidity_ranks table and returns per-date
    symbol lists for a specified universe (univ100, univ500, etc.).
    
    Parameters
    ----------
    db_path : str | Path
        Root path to Parquet database.
    universe_name : str
        Pre-computed universe name:
        - 'univ100': Top 100 liquid stocks per date
        - 'univ200': Top 200 liquid stocks per date
        - 'univ500': Top 500 liquid stocks per date
        - 'univ1000': Top 1000 liquid stocks per date
        - 'univ2000': Top 2000 liquid stocks per date
    start_date : str
        Start date (YYYYMMDD).
    end_date : str
        End date (YYYYMMDD).
    
    Returns
    -------
    Dict[str, List[str]]
        Mapping of {date: [symbols]} with per-date symbol lists.
        Symbols are ordered by liquidity rank (rank 1 = most liquid).
    
    Survivorship Bias-Free
    ----------------------
    - Universe membership changes daily (stocks can enter/exit)
    - Returns actual liquid stocks on each historical date
    - Includes delisted stocks if they were liquid on that date
    
    Example
    -------
    >>> universe_map = load_universe_symbols(
    ...     db_path='./data/',
    ...     universe_name='univ100',
    ...     start_date='20240101',
    ...     end_date='20240105'
    ... )
    >>> universe_map
    {
        '20240101': ['005930', '000660', ...],  # Top 100 on 2024-01-01
        '20240102': ['005930', '000660', ...],  # Top 100 on 2024-01-02 (may differ!)
        '20240103': ['005930', '035720', ...],  # Top 100 on 2024-01-03
        ...
    }
    
    Note: Used internally by Layer 2 UniverseService for universe='univ100' queries.
    """
    # Parse universe name to rank threshold
    max_rank = _parse_universe_rank(universe_name)
    
    # Query liquidity_ranks table with rank filter
    df = query_parquet_table(
        db_path=db_path,
        table_name='liquidity_ranks',
        start_date=start_date,
        end_date=end_date,
        symbols=None,  # Don't filter by symbols
        fields=['TRD_DD', 'ISU_SRT_CD', 'xs_liquidity_rank'],
    )
    
    if df.empty:
        return {}
    
    # Filter by rank threshold
    df = df[df['xs_liquidity_rank'] <= max_rank]
    
    # Sort by rank within each date
    df = df.sort_values(['TRD_DD', 'xs_liquidity_rank'])
    
    # Group by date and extract symbol lists
    universe_map: Dict[str, List[str]] = {}
    for date, group in df.groupby('TRD_DD'):
        universe_map[str(date)] = group['ISU_SRT_CD'].tolist()
    
    return universe_map


# ============================================================================
# Internal Helpers
# ============================================================================

def _discover_partitions(
    table_path: Path,
    start_date: str,
    end_date: str,
) -> List[str]:
    """
    Discover available partition names in date range.
    
    Parameters
    ----------
    table_path : Path
        Path to table directory (e.g., db/snapshots/).
    start_date : str
        Start date (YYYYMMDD).
    end_date : str
        End date (YYYYMMDD).
    
    Returns
    -------
    List[str]
        Available partition names (e.g., ['TRD_DD=20240101', ...]).
        Missing dates (holidays) are silently skipped.
    
    Example
    -------
    start='20240101', end='20240105'
    Available: [TRD_DD=20240102, TRD_DD=20240103, TRD_DD=20240105]
    → 01/04 missing (holidays) → silently skip
    """
    if not table_path.exists():
        return []
    
    # Find all TRD_DD=* directories (single-level partitioning)
    all_partitions = [d.name for d in table_path.iterdir() if d.is_dir() and d.name.startswith('TRD_DD=')]
    
    # Filter by date range
    filtered_partitions = []
    for partition_name in all_partitions:
        # Extract date from "TRD_DD=20240101"
        date_str = partition_name.split('=')[1]
        
        # Check if in range
        if start_date <= date_str <= end_date:
            filtered_partitions.append(partition_name)
    
    # Sort by date
    filtered_partitions.sort()
    
    return filtered_partitions


def _read_partitions_pyarrow(
    table_path: Path,
    partition_names: List[str],
    *,
    columns: Optional[List[str]] = None,
    filters: Optional[pc.Expression] = None,
) -> pa.Table:
    """
    Read multiple Hive partitions with PyArrow (internal, zero-copy).
    
    Parameters
    ----------
    table_path : Path
        Path to table directory (e.g., db/snapshots/).
    partition_names : List[str]
        Partition names to read (e.g., ['TRD_DD=20240101', 'TRD_DD=20240102']).
    columns : Optional[List[str]]
        Columns to read (column pruning).
        If columns includes 'TRD_DD', it will be injected from partition names (not read from data).
    filters : Optional[pc.Expression]
        Row filters (e.g., ISU_SRT_CD.isin(['005930', '000660'])).
    
    Returns
    -------
    pa.Table
        Concatenated table from all partitions with TRD_DD injected.
    
    Note: Internal function, not exposed to users. Returns PyArrow for speed.
    """
    tables = []
    
    # Determine which columns to read from files (exclude TRD_DD if requested)
    file_columns = columns
    if columns and 'TRD_DD' in columns:
        file_columns = [col for col in columns if col != 'TRD_DD']
        if not file_columns:
            file_columns = None  # Read all columns
    
    for partition_name in partition_names:
        partition_path = table_path / partition_name / "data.parquet"
        
        if not partition_path.exists():
            # Partition directory exists but no data file (should not happen in normal operation)
            continue
        
        # Read partition with filters and column selection
        table = pq.read_table(
            partition_path,
            columns=file_columns,
            filters=filters,
        )
        
        # Inject TRD_DD column from partition name
        table = _inject_trd_dd_column(table, partition_name)
        
        tables.append(table)
    
    if not tables:
        # No data found
        if columns:
            # Return empty table with requested schema
            schema = pa.schema([(col, pa.string()) for col in columns])
            return pa.table({col: [] for col in columns}, schema=schema)
        else:
            return pa.table({})
    
    # Concatenate all tables
    combined_table = pa.concat_tables(tables)
    
    # If specific columns requested, select and reorder them
    if columns:
        # Ensure all requested columns exist
        available_cols = combined_table.column_names
        final_cols = [col for col in columns if col in available_cols]
        combined_table = combined_table.select(final_cols)
    
    return combined_table


def _inject_trd_dd_column(
    table: pa.Table,
    partition_name: str,
) -> pa.Table:
    """
    Inject TRD_DD column from partition name.
    
    Parameters
    ----------
    table : pa.Table
        PyArrow table (may or may not have TRD_DD).
    partition_name : str
        Partition name (e.g., 'TRD_DD=20240101').
    
    Returns
    -------
    pa.Table
        Table with TRD_DD column added (or replaced if already exists).
    
    Rationale
    ---------
    - TRD_DD is partition key (directory structure, not in data files)
    - Must be injected for downstream processing (joins, sorting, grouping)
    
    Example
    -------
    Partition name: 'TRD_DD=20240101'
    → Extracts '20240101' and adds as string column to table
    """
    # Extract date from "TRD_DD=20240101"
    date_str = partition_name.split('=')[1]
    
    # Create TRD_DD column (string array with same length as table)
    trd_dd_array = pa.array([date_str] * len(table), type=pa.string())
    
    # Remove TRD_DD if it already exists (avoid duplicates)
    if 'TRD_DD' in table.column_names:
        table = table.drop(['TRD_DD'])
    
    # Add column to table (prepend for consistency - first column)
    # Use PyArrow Table constructor to specify column order
    new_schema = pa.schema([('TRD_DD', pa.string())] + [(name, field.type) for name, field in zip(table.column_names, table.schema)])
    new_columns = [trd_dd_array] + [table.column(name) for name in table.column_names]
    
    table = pa.table(dict(zip([field.name for field in new_schema], new_columns)), schema=new_schema)
    
    return table


def _build_symbol_filter(
    symbols: List[str]
) -> pc.Expression:
    """
    Build PyArrow filter expression for symbol filtering.
    
    Parameters
    ----------
    symbols : List[str]
        Stock codes to filter.
    
    Returns
    -------
    pc.Expression
        Filter expression: pc.field('ISU_SRT_CD').isin(symbols)
    
    Row-Group Pruning
    -----------------
    - Writer sorts data by ISU_SRT_CD
    - Row groups have statistics (min/max ISU_SRT_CD)
    - PyArrow skips row groups where symbols don't overlap
    - Result: 50-90% I/O reduction for symbol-specific queries
    
    Example
    -------
    >>> symbols = ['005930', '000660']
    >>> filter_expr = _build_symbol_filter(symbols)
    >>> # PyArrow will only read row groups containing these symbols
    """
    return pc.field('ISU_SRT_CD').isin(symbols)


def _parse_universe_rank(universe_name: str) -> int:
    """
    Parse universe name to rank threshold.
    
    Parameters
    ----------
    universe_name : str
        Universe name (e.g., 'univ100', 'univ500').
    
    Returns
    -------
    int
        Rank threshold (e.g., 100 for 'univ100').
    
    Raises
    ------
    ValueError
        If universe name is unknown.
    
    Example
    -------
    >>> _parse_universe_rank('univ100')
    100
    >>> _parse_universe_rank('univ500')
    500
    """
    if universe_name not in _UNIVERSE_RANK_MAP:
        raise ValueError(
            f"Unknown universe: {universe_name}. "
            f"Available: {list(_UNIVERSE_RANK_MAP.keys())}"
        )
    return _UNIVERSE_RANK_MAP[universe_name]


__all__ = [
    "query_parquet_table",
    "load_universe_symbols",
]

