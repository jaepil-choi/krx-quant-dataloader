"""
Storage schemas

What this module does:
- Defines PyArrow schemas for Parquet tables.
- Ensures consistent data types across writes and reads.
- Column ordering optimized for row-group pruning (filter keys early).

Schemas:
- PRICEVOLUME_SCHEMA: Single unified table with progressive enrichment
- UNIVERSES_SCHEMA: Pre-computed universe membership (persistent, optional optimization)
- CUMULATIVE_ADJUSTMENTS_SCHEMA: Range-dependent multipliers (ephemeral cache)

Interaction:
- Used by storage writers for writing data.
- Used by storage/query.py for reading and filtering data.
"""

from __future__ import annotations

import pyarrow as pa


# =============================================================================
# PriceVolume Schema: Single Unified Table
# =============================================================================

# PriceVolume table: Single unified table with progressive enrichment
# Partition key: TRD_DD (YYYYMMDD) - single-level partitioning
# Format: data/pricevolume/TRD_DD=20240820/data.parquet
#
# Contains ALL markets (KOSPI/KOSDAQ/KONEX) in single file per date
# Use MKT_ID column to filter by market after reading
#
# Schema evolves through 3 stages:
# - Stage 1: Raw fields only (NO enriched columns yet)
# - Stage 2: Raw fields + adj_factor column added by AdjustmentEnricher
# - Stage 3: Raw fields + adj_factor + liquidity_rank columns (COMPLETE)

# Stage 1: Raw fields only (used by PriceVolumeWriter.write_initial)
PRICEVOLUME_RAW_SCHEMA = pa.schema([
    # Primary filter key (sorted writes for row-group pruning)
    ('ISU_SRT_CD', pa.string()),        # Security ID (6-digit code)
    
    # Descriptive fields (from KRX API)
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

# Stages 2-3: Full schema with enriched columns (used by enrichers)
PRICEVOLUME_SCHEMA = pa.schema([
    # Raw fields (same as PRICEVOLUME_RAW_SCHEMA)
    ('ISU_SRT_CD', pa.string()),
    ('ISU_ABBRV', pa.string()),
    ('MKT_NM', pa.string()),
    ('BAS_PRC', pa.int64()),
    ('TDD_CLSPRC', pa.int64()),
    ('CMPPREVDD_PRC', pa.int64()),
    ('ACC_TRDVOL', pa.int64()),
    ('ACC_TRDVAL', pa.int64()),
    ('FLUC_RT', pa.string()),
    ('FLUC_TP', pa.string()),
    ('MKT_ID', pa.string()),
    
    # Enriched fields (added progressively by enrichers)
    ('adj_factor', pa.float64()),       # Stage 2: Adjustment factor (BAS_PRC_t / TDD_CLSPRC_{t-1})
    ('liquidity_rank', pa.int32()),     # Stage 3: Cross-sectional liquidity rank (1 = most liquid)
])


# Cumulative adjustments table: Ephemeral cache for range-dependent multipliers
# NOTE: TRD_DD is partition key (not in data columns, implicit from directory structure)
CUMULATIVE_ADJUSTMENTS_SCHEMA = pa.schema([
    # Primary filter key (sorted writes for row-group pruning)
    ('ISU_SRT_CD', pa.string()),          # Security ID
    
    # Cumulative multiplier (product of future adjustment factors)
    # CRITICAL: Minimum 1e-6 precision required for accurate split handling
    ('cum_adj_multiplier', pa.float64()),  # Cumulative adjustment multiplier
])


# Universes table: Pre-computed universe membership per date
# NOTE: TRD_DD is partition key (not in data columns, implicit from directory structure)
# Purpose: Fast universe filtering (survivorship-bias-free, per-date membership)
# Design: Boolean columns for efficient filtering (no string comparisons needed)
#         Subset relationships are explicit: univ100=1 implies univ200=1, univ500=1, univ1000=1
UNIVERSES_SCHEMA = pa.schema([
    # Primary filter key (sorted writes for row-group pruning)
    ('ISU_SRT_CD', pa.string()),          # Security ID
    
    # Universe membership flags (boolean for efficient filtering)
    ('univ100', pa.int8()),               # 1 if in top 100, 0 otherwise
    ('univ200', pa.int8()),               # 1 if in top 200, 0 otherwise
    ('univ500', pa.int8()),               # 1 if in top 500, 0 otherwise
    ('univ1000', pa.int8()),              # 1 if in top 1000, 0 otherwise
    
    # Reference field (for verification and debugging)
    ('liquidity_rank', pa.int32()),       # Rank at time of universe construction (from pricevolume table)
])


# =============================================================================
# Legacy Schemas (for backward compatibility with ParquetSnapshotWriter)
# =============================================================================

# DEPRECATED: Use PRICEVOLUME_SCHEMA instead
# Kept for backward compatibility with existing ParquetSnapshotWriter
SNAPSHOTS_SCHEMA = pa.schema([
    ('ISU_SRT_CD', pa.string()),
    ('ISU_ABBRV', pa.string()),
    ('MKT_NM', pa.string()),
    ('BAS_PRC', pa.int64()),
    ('TDD_CLSPRC', pa.int64()),
    ('CMPPREVDD_PRC', pa.int64()),
    ('ACC_TRDVOL', pa.int64()),
    ('ACC_TRDVAL', pa.int64()),
    ('FLUC_RT', pa.string()),
    ('FLUC_TP', pa.string()),
    ('MKT_ID', pa.string()),
])

# DEPRECATED: adj_factor is now a column in PRICEVOLUME_SCHEMA
ADJ_FACTORS_SCHEMA = pa.schema([
    ('ISU_SRT_CD', pa.string()),
    ('adj_factor', pa.float64()),
])

# DEPRECATED: liquidity_rank is now a column in PRICEVOLUME_SCHEMA
LIQUIDITY_RANKS_SCHEMA = pa.schema([
    ('ISU_SRT_CD', pa.string()),
    ('xs_liquidity_rank', pa.int32()),
    ('ACC_TRDVAL', pa.int64()),
])


__all__ = [
    "PRICEVOLUME_RAW_SCHEMA",  # NEW: Stage 1 raw fields only
    "PRICEVOLUME_SCHEMA",       # Full schema with enriched fields
    "UNIVERSES_SCHEMA",
    "CUMULATIVE_ADJUSTMENTS_SCHEMA",
    # Legacy (deprecated, kept for ParquetSnapshotWriter)
    "SNAPSHOTS_SCHEMA",
    "ADJ_FACTORS_SCHEMA",
    "LIQUIDITY_RANKS_SCHEMA",
]

