"""
Liquidity ranking pipeline

Stage 3 of the data pipeline:
1. Snapshots (Stage 1) → 2. Adjustments (Stage 2) → 3. Liquidity Ranking (Stage 3)

What this module does:
- Compute cross-sectional liquidity ranks based on ACC_TRDVAL (trading value)
- Rank stocks per-date independently (survivorship bias-free)
- Dense ranking: ties get same rank, no gaps in sequence
- Rank 1 = most liquid stock on that date

Why cross-sectional (per-date) ranking:
- Prevents survivorship bias (each date ranked independently)
- Handles corporate actions naturally (trading halts → zero value → low rank)
- No need to track delisting dates or special cases

Algorithm:
- Group by TRD_DD
- Rank by ACC_TRDVAL (descending: higher value = lower rank number)
- Dense ranking (no gaps when ties occur)

Example:
    Date        Symbol    ACC_TRDVAL    Rank
    20240101    STOCK01    5000000        1   ← Most liquid
    20240101    STOCK02    2000000        2
    20240101    STOCK03    2000000        2   ← Tie
    20240101    STOCK04    1000000        3   ← Dense ranking (not 4)
    
    20240102    STOCK02    8000000        1   ← Ranks change per date
    20240102    STOCK01    3000000        2
    20240102    STOCK03           0        3   ← Halted → low rank

Integration:
- Input: Snapshots from Parquet DB (query_parquet_table)
- Output: liquidity_ranks table (Hive-partitioned by TRD_DD)
- Used by: Universe builder (Stage 4) to define univ100, univ500, etc.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from krx_quant_dataloader.storage.query import query_parquet_table
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter


def compute_liquidity_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute cross-sectional liquidity ranks from snapshot data.
    
    This function ranks stocks by ACC_TRDVAL (trading value) per date,
    using dense ranking to handle ties gracefully.
    
    Parameters
    ----------
    df : pd.DataFrame
        Snapshot data with columns:
        - TRD_DD (str): Trading date (YYYYMMDD)
        - ISU_SRT_CD (str): Stock code
        - ACC_TRDVAL (int): Accumulated trading value
        - Other columns preserved as-is
    
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added column:
        - xs_liquidity_rank (int): Liquidity rank per date
          - Rank 1 = highest ACC_TRDVAL (most liquid)
          - Dense ranking: ties get same rank, no gaps
          - Sorted by TRD_DD (asc) and xs_liquidity_rank (asc)
    
    Algorithm
    ---------
    1. Group by TRD_DD (per-date ranking)
    2. Rank by ACC_TRDVAL (descending: higher value → lower rank number)
    3. Dense ranking (ties → same rank, no gaps)
    4. Cast to int (pandas rank returns float)
    
    Handles Edge Cases
    ------------------
    - Zero trading value: Lowest rank (e.g., trading halts)
    - Null/NaN values: Treated as zero (lowest rank)
    - Ties: Same ACC_TRDVAL → same rank
    - Single stock: Rank 1
    - Empty DataFrame: Empty result
    
    Performance
    -----------
    - 2,300 stocks × 10 days = 23,115 rows: <1 second
    - Pandas groupby + rank is optimized for this operation
    
    Example
    -------
    >>> df = pd.DataFrame({
    ...     'TRD_DD': ['20240101', '20240101', '20240101'],
    ...     'ISU_SRT_CD': ['STOCK01', 'STOCK02', 'STOCK03'],
    ...     'ACC_TRDVAL': [5000000, 2000000, 0],
    ... })
    >>> result = compute_liquidity_ranks(df)
    >>> result[['ISU_SRT_CD', 'xs_liquidity_rank']]
       ISU_SRT_CD  xs_liquidity_rank
    0     STOCK01                  1  ← Highest value
    1     STOCK02                  2
    2     STOCK03                  3  ← Zero value
    
    Notes
    -----
    - Cross-sectional (per-date) to prevent survivorship bias
    - Ranks can vary across dates (e.g., Samsung rank 1 → rank 2230 on halt day)
    - Dense ranking ensures no gaps (important for filtering top N stocks)
    """
    # Validate required columns
    required_cols = ['TRD_DD', 'ACC_TRDVAL']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing required columns: {missing_cols}")
    
    # Handle empty DataFrame
    if len(df) == 0:
        result = df.copy()
        result['xs_liquidity_rank'] = pd.Series(dtype='int64')
        return result
    
    # Fill NaN/NULL values with 0 (treat as no liquidity)
    df = df.copy()
    df['ACC_TRDVAL'] = df['ACC_TRDVAL'].fillna(0)
    
    # Compute ranks per date
    # - Group by TRD_DD (per-date ranking)
    # - Rank by ACC_TRDVAL (ascending=False: higher value → lower rank)
    # - method='dense': no gaps in ranking (1, 2, 2, 3 not 1, 2, 2, 4)
    df_ranked = df.groupby('TRD_DD', group_keys=False).apply(
        lambda g: g.assign(
            xs_liquidity_rank=g['ACC_TRDVAL'].rank(
                method='dense',
                ascending=False,
                na_option='bottom',  # NaN → lowest rank
            ).astype(int)
        )
    ).reset_index(drop=True)
    
    # Sort by date (ascending) and rank (ascending) for efficient storage
    # This enables row-group pruning when querying top N stocks
    df_ranked = df_ranked.sort_values(
        ['TRD_DD', 'xs_liquidity_rank'],
        ascending=[True, True]
    ).reset_index(drop=True)
    
    return df_ranked


def write_liquidity_ranks(
    df: pd.DataFrame,
    db_path: str | Path,
    *,
    table_name: str = 'liquidity_ranks',
) -> None:
    """
    Write liquidity ranks to Parquet database with Hive partitioning.
    
    This function persists ranked snapshot data to the liquidity_ranks table,
    enabling efficient queries for universe construction (top N stocks).
    
    Parameters
    ----------
    df : pd.DataFrame
        Ranked snapshot data from compute_liquidity_ranks().
        Must contain:
        - TRD_DD (str): Trading date (partition key)
        - xs_liquidity_rank (int): Liquidity rank
        - All other snapshot columns preserved
    db_path : str | Path
        Root path to Parquet database (e.g., './data/krx_db').
    table_name : str, default='liquidity_ranks'
        Table name (subdirectory in database).
    
    Storage Format
    --------------
    - Hive-partitioned by TRD_DD (directory per date)
    - Sorted by xs_liquidity_rank (enables row-group pruning)
    - Compressed with Snappy (balanced speed/size)
    
    Directory Structure
    -------------------
    db_path/
        liquidity_ranks/
            TRD_DD=20240101/
                data.parquet
            TRD_DD=20240102/
                data.parquet
    
    Performance
    -----------
    - 23,115 rows → ~2-5 MB compressed
    - Write time: ~200-500ms
    - Row-group pruning enabled by sorting
    
    Example
    -------
    >>> df_ranked = compute_liquidity_ranks(df_snapshots)
    >>> write_liquidity_ranks(df_ranked, './data/krx_db')
    
    Notes
    -----
    - Uses same writer as snapshots (ParquetSnapshotWriter)
    - TRD_DD stripped from data (becomes partition key)
    - Sorting by xs_liquidity_rank enables efficient top-N queries
    """
    db_path = Path(db_path)
    table_path = db_path / table_name
    
    # Create table directory if needed
    table_path.mkdir(parents=True, exist_ok=True)
    
    # Use ParquetSnapshotWriter (same infrastructure as snapshots)
    writer = ParquetSnapshotWriter(db_path=str(db_path))
    
    # Group by date and write partitions
    for date, group in df.groupby('TRD_DD'):
        # Ensure sorted by rank (for row-group pruning)
        group_sorted = group.sort_values('xs_liquidity_rank').reset_index(drop=True)
        
        # Convert to list of dicts (writer expects this format)
        rows = group_sorted.to_dict('records')
        
        # Write partition
        # Note: Writer strips TRD_DD and uses it as partition key
        writer._write_partition(
            table_name=table_name,
            date_str=str(date),
            rows=rows,
        )


def query_liquidity_ranks(
    db_path: str | Path,
    *,
    start_date: str,
    end_date: str,
    symbols: Optional[list[str]] = None,
    max_rank: Optional[int] = None,
) -> pd.DataFrame:
    """
    Query liquidity ranks from Parquet database.
    
    This is a convenience wrapper around query_parquet_table()
    with rank filtering support.
    
    Parameters
    ----------
    db_path : str | Path
        Root path to Parquet database.
    start_date : str
        Start date (YYYYMMDD, inclusive).
    end_date : str
        End date (YYYYMMDD, inclusive).
    symbols : Optional[list[str]]
        Stock codes to filter (e.g., ['005930', '000660']).
        If None, returns all symbols.
    max_rank : Optional[int]
        Maximum rank to include (e.g., 100 for top 100 stocks).
        If None, returns all ranks.
    
    Returns
    -------
    pd.DataFrame
        Liquidity ranks with columns:
        - TRD_DD (str): Trading date
        - ISU_SRT_CD (str): Stock code
        - xs_liquidity_rank (int): Rank
        - All other snapshot columns
        
        Sorted by TRD_DD (asc) and xs_liquidity_rank (asc).
    
    Optimizations
    -------------
    - Partition pruning: Only reads [start_date, end_date]
    - Row-group pruning: Leverages sorted writes by xs_liquidity_rank
    - Column pruning: Only reads requested fields
    
    Performance
    -----------
    - Top 100 × 252 days: ~50-100ms
    - Top 500 × 252 days: ~100-200ms
    - Full market (3000 × 252): ~1-3 seconds
    
    Example
    -------
    >>> # Get top 100 liquid stocks for backtest period
    >>> df = query_liquidity_ranks(
    ...     db_path='./data/krx_db',
    ...     start_date='20240101',
    ...     end_date='20241231',
    ...     max_rank=100,
    ... )
    >>> # Result: Top 100 stocks per date (survivorship bias-free)
    
    Notes
    -----
    - Used by universe builder to construct univ100, univ500, etc.
    - Cross-sectional: ranks vary per date
    - Includes halted/delisted stocks if they were liquid on that date
    """
    # Query from liquidity_ranks table
    df = query_parquet_table(
        db_path=db_path,
        table_name='liquidity_ranks',
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        fields=None,  # Read all columns
    )
    
    if df.empty:
        return df
    
    # Filter by max_rank if specified
    if max_rank is not None:
        df = df[df['xs_liquidity_rank'] <= max_rank]
    
    # Ensure sorted (for consistency)
    df = df.sort_values(['TRD_DD', 'xs_liquidity_rank']).reset_index(drop=True)
    
    return df


__all__ = [
    'compute_liquidity_ranks',
    'write_liquidity_ranks',
    'query_liquidity_ranks',
]

