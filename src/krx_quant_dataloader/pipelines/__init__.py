"""
Pipelines layer

What this package does:
- Resume-safe ingestion workflows that combine raw fetching (via RawClient),
  preprocessing (via transforms), and persistence (via injected SnapshotWriter).
- Post-hoc batch jobs (e.g., adjustment factor computation after ingestion).

Modules:
- snapshots: per-day ingestion for MDCSTAT01602-like endpoints with writer injection.
  Functions: ingest_change_rates_day, ingest_change_rates_range, compute_and_persist_adj_factors.

Design rules:
- No direct network code; all IO goes through RawClient.
- Storage side-effects explicit via injected writer (dependency injection).
- Per-day isolation: errors on one day do not halt subsequent days.
"""


