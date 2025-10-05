"""
Pipelines layer

What this package does:
- Resume-safe ingestion workflows that combine raw fetching (via RawClient),
  preprocessing (via transforms), and persistence (via injected SnapshotWriter).
- Post-hoc batch jobs (e.g., adjustment factor computation after ingestion).
- Pipeline orchestration for DataLoader initialization (3-stage pipeline).

Modules:
- orchestrator: coordinate 3-stage pipeline (snapshots → adjustments → universes).
  Class: PipelineOrchestrator (NEW).
- snapshots: per-day ingestion for MDCSTAT01602-like endpoints with writer injection.
  Functions: ingest_change_rates_day, ingest_change_rates_range, compute_and_persist_adj_factors.
- liquidity_ranking: compute cross-sectional liquidity ranks from snapshots.
  Functions: compute_liquidity_ranks, write_liquidity_ranks, query_liquidity_ranks.
- universe_builder: construct pre-computed universe membership from liquidity ranks.
  Functions: build_universes, build_universes_and_persist.

Design rules:
- No direct network code; all IO goes through RawClient.
- Storage side-effects explicit via injected writer (dependency injection).
- Per-day isolation: errors on one day do not halt subsequent days.
"""


