"""
Storage schemas

What this module does:
- Defines PyArrow schemas for Parquet tables (snapshots, adj_factors, liquidity_ranks).
- Ensures consistent data types across writes and reads.
- Column ordering optimized for row-group pruning (filter keys early).

Interaction:
- Used by ParquetSnapshotWriter for writing data.
- Used by storage/query.py for reading and filtering data.
"""

from __future__ import annotations

import pyarrow as pa


# Snapshots table: Market-wide daily data
# Note: TRD_DD is partition key (not in data files, only in directory structure)
SNAPSHOTS_SCHEMA = pa.schema([
    # Partition key (implicit in directory structure, not in data)
    # Format: db/snapshots/TRD_DD=20240820/data.parquet
    
    # Primary filter key (sorted writes for row-group pruning)
    ('ISU_SRT_CD', pa.string()),        # Security ID (6-digit code)
    
    # Descriptive fields
    ('ISU_ABBRV', pa.string()),         # Security abbreviation (Korean name)
    ('MKT_NM', pa.string()),            # Market name (KOSPI/KOSDAQ/KONEX)
    
    # Core price fields (coerced to int64 by preprocessing)
    ('BAS_PRC', pa.int64()),            # Base price (previous adjusted close)
    ('TDD_CLSPRC', pa.int64()),         # Today's close price
    ('CMPPREVDD_PRC', pa.int64()),      # Change from previous day
    
    # Trading volume/value (for liquidity ranking)
    ('ACC_TRDVOL', pa.int64()),         # Accumulated trade volume
    ('ACC_TRDVAL', pa.int64()),         # Accumulated trade value (거래대금)
    
    # Additional fields (passthrough as strings)
    ('FLUC_RT', pa.string()),           # Fluctuation rate
    ('FLUC_TP', pa.string()),           # Fluctuation type (1: 상한, 2: 상승, ...)
    ('MKT_ID', pa.string()),            # Market ID (STK, KSQ, KNX)
])


# Adjustment factors table: Per-symbol corporate action adjustments
ADJ_FACTORS_SCHEMA = pa.schema([
    # Primary filter key (sorted by symbol for row-group pruning)
    ('ISU_SRT_CD', pa.string()),        # Security ID
    
    # Adjustment factor: BAS_PRC_t / TDD_CLSPRC_{t-1}
    ('adj_factor', pa.float64()),       # Adjustment factor (1.0 = no adjustment)
])


# Liquidity ranks table: Cross-sectional liquidity ranking per date
LIQUIDITY_RANKS_SCHEMA = pa.schema([
    # Primary filter keys
    ('ISU_SRT_CD', pa.string()),        # Security ID
    ('xs_liquidity_rank', pa.int32()),  # Cross-sectional rank (1 = most liquid)
    
    # Reference field
    ('ACC_TRDVAL', pa.int64()),         # Trading value (for verification)
])


__all__ = [
    "SNAPSHOTS_SCHEMA",
    "ADJ_FACTORS_SCHEMA",
    "LIQUIDITY_RANKS_SCHEMA",
]

