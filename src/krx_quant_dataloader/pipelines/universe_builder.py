"""
Universe builder pipeline

What this module does:
- Constructs pre-computed universe membership from liquidity ranks
- Filters stocks by rank thresholds (univ100 = top 100, univ500 = top 500, etc.)
- Enables fast universe-based queries without on-the-fly ranking
- Preserves per-date independence (survivorship-bias-free)

Pipeline B: Snapshots → Liquidity Ranks → Universes

Functions:
- build_universes: Core logic to construct universe membership DataFrame
- build_universes_and_persist: Persist universes to Parquet database

Design:
- Cross-sectional: Rankings are independent per date
- Subset relationships: univ100 ⊂ univ200 ⊂ univ500 ⊂ univ1000
- One stock can appear in multiple universes (if rank ≤ threshold for each)
- Hive-partitioned by TRD_DD for efficient date-range queries
"""

from __future__ import annotations

from typing import Dict
import pandas as pd

from ..storage.protocols import SnapshotWriter


def build_universes(
    ranks_df: pd.DataFrame,
    universe_tiers: Dict[str, int],
) -> pd.DataFrame:
    """
    Construct universe membership rows from liquidity ranks.
    
    For each universe tier (e.g., univ100 = rank ≤ 100), filters stocks
    by rank threshold and creates membership rows. Stocks can appear in
    multiple universes (e.g., rank 50 stock is in univ100, univ500, univ1000).
    
    Parameters
    ----------
    ranks_df : pd.DataFrame
        Liquidity ranks DataFrame with columns:
        - TRD_DD: Trade date (YYYYMMDD)
        - ISU_SRT_CD: Stock symbol (6-digit code)
        - xs_liquidity_rank: Cross-sectional liquidity rank (1 = most liquid)
        - ACC_TRDVAL: Accumulated trading value (for reference)
    universe_tiers : Dict[str, int]
        Universe definitions as {name: rank_threshold}.
        Example: {'univ100': 100, 'univ500': 500, 'univ1000': 1000}
        
    Returns
    -------
    pd.DataFrame
        Universe membership DataFrame with columns:
        - TRD_DD: Trade date
        - ISU_SRT_CD: Stock symbol
        - universe_name: Universe identifier ('univ100', 'univ500', etc.)
        - xs_liquidity_rank: Rank at time of construction
        
        Sorted by TRD_DD (ascending), then ISU_SRT_CD (ascending)
        for efficient Hive-partitioned storage.
    
    Raises
    ------
    KeyError
        If required columns are missing from ranks_df.
    
    Notes
    -----
    - Per-date independence: Universe membership is determined per date
    - Subset relationships: Smaller universes are subsets of larger ones
    - If fewer stocks exist than threshold, includes all available stocks
    
    Examples
    --------
    >>> ranks = pd.DataFrame({
    ...     'TRD_DD': ['20240101'] * 3,
    ...     'ISU_SRT_CD': ['STOCK01', 'STOCK02', 'STOCK03'],
    ...     'xs_liquidity_rank': [1, 2, 3],
    ...     'ACC_TRDVAL': [1000000, 900000, 800000]
    ... })
    >>> tiers = {'univ2': 2}
    >>> result = build_universes(ranks, tiers)
    >>> len(result)
    2
    >>> set(result['ISU_SRT_CD'])
    {'STOCK01', 'STOCK02'}
    """
    # Validate required columns
    required_columns = ['TRD_DD', 'ISU_SRT_CD', 'xs_liquidity_rank']
    missing = [col for col in required_columns if col not in ranks_df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")
    
    # Handle empty inputs
    if ranks_df.empty or not universe_tiers:
        return pd.DataFrame(columns=['TRD_DD', 'ISU_SRT_CD', 'universe_name', 'xs_liquidity_rank'])
    
    # Build universe membership rows
    universe_rows = []
    
    for universe_name, rank_threshold in universe_tiers.items():
        # Filter stocks by rank threshold
        universe_stocks = ranks_df[ranks_df['xs_liquidity_rank'] <= rank_threshold].copy()
        
        # Add universe_name column
        universe_stocks['universe_name'] = universe_name
        
        # Select only required columns
        universe_stocks = universe_stocks[['TRD_DD', 'ISU_SRT_CD', 'universe_name', 'xs_liquidity_rank']]
        
        universe_rows.append(universe_stocks)
    
    # Combine all universe memberships
    if universe_rows:
        result = pd.concat(universe_rows, ignore_index=True)
        
        # Sort by date (ascending) and symbol (ascending) for efficient storage
        result = result.sort_values(
            ['TRD_DD', 'ISU_SRT_CD'],
            ascending=[True, True]
        ).reset_index(drop=True)
        
        return result
    else:
        return pd.DataFrame(columns=['TRD_DD', 'ISU_SRT_CD', 'universe_name', 'xs_liquidity_rank'])


def build_universes_and_persist(
    ranks_df: pd.DataFrame,
    universe_tiers: Dict[str, int],
    writer: SnapshotWriter,
) -> int:
    """
    Construct universe membership and persist to Parquet database.
    
    Combines build_universes() with persistence via ParquetSnapshotWriter.
    Data is Hive-partitioned by TRD_DD for efficient date-range queries.
    
    Parameters
    ----------
    ranks_df : pd.DataFrame
        Liquidity ranks DataFrame (see build_universes for schema).
    universe_tiers : Dict[str, int]
        Universe definitions as {name: rank_threshold}.
    writer : SnapshotWriter
        Writer instance (e.g., ParquetSnapshotWriter) for persistence.
        Must implement write_universes(rows, date) method.
        
    Returns
    -------
    int
        Total number of universe membership rows persisted.
    
    Notes
    -----
    - Writes are idempotent: re-running with same data overwrites partitions
    - Data is partitioned by TRD_DD (one partition per date)
    - Per-date writes enable resume-safe operation
    
    Examples
    --------
    >>> from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
    >>> writer = ParquetSnapshotWriter(root_path='data/krx_db')
    >>> ranks = load_liquidity_ranks(...)
    >>> tiers = {'univ100': 100, 'univ500': 500}
    >>> count = build_universes_and_persist(ranks, tiers, writer)
    >>> print(f"Persisted {count} universe membership rows")
    """
    # Build universe membership
    universes_df = build_universes(ranks_df, universe_tiers)
    
    if universes_df.empty:
        return 0
    
    # Persist per date (Hive partitioning)
    total_written = 0
    
    for date in sorted(universes_df['TRD_DD'].unique()):
        # Get rows for this date
        date_rows = universes_df[universes_df['TRD_DD'] == date]
        
        # Convert to list of dicts for writer
        rows = date_rows.to_dict('records')
        
        # Write to database
        writer.write_universes(rows, date=date)
        
        total_written += len(rows)
    
    return total_written


__all__ = [
    'build_universes',
    'build_universes_and_persist',
]

