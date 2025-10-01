"""
Storage layer (protocol-driven, opt-in)

Purpose:
- Define protocols for snapshot/factor persistence (dependency injection).
- Provide concrete writer implementations (CSV, SQLite) for pipelines.

Modules:
- protocols.py: SnapshotWriter protocol (ABC) for write_snapshot_rows, write_factor_rows, close.
- writers.py: CSVSnapshotWriter (UTF-8, no BOM, append-only), SQLiteSnapshotWriter (UPSERT on composite key).
- schema.py: Minimal DDL suggestions; actual schemas enforced by writers.

Design:
- Writers injected into pipelines via dependency injection.
- Not used implicitly by APIs; explicit user opt-in.
"""


