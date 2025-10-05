"""
Storage layer - Progressive enrichment with atomic writes

Purpose:
- Define schemas for unified PriceVolume table with progressive enrichment
- Provide writers with atomic write patterns (staging → backup → move)
- Provide enrichers for Stage 2 (adj_factor) and Stage 3 (liquidity_rank)

Modules:
- schema.py: PyArrow schemas for PriceVolume, Universes, and cumulative adjustments
- writers.py: TempSnapshotWriter, PriceVolumeWriter with atomic patterns
- enrichers.py: AdjustmentEnricher, LiquidityRankEnricher for progressive enrichment
- protocols.py: SnapshotWriter protocol (legacy)
- query.py: Parquet query helpers

Design:
- Single persistent DB: data/pricevolume/ with single-level date partitioning
- Atomic writes via temp/staging/ and temp/backup/
- Progressive schema: raw → +adj_factor → +liquidity_rank
- Writers and enrichers injected via dependency injection
"""


