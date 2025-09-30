"""
Storage adapters (optional, opt-in)

Purpose:
- Provide optional sinks to persist rows to relational databases or files
  (CSV/Parquet). These are not used implicitly; users opt in explicitly.

Modules:
- schema.py: minimal table schemas (e.g., snapshot rows and adj_factors)
- writers.py: simple adapters for SQL/CSV/Parquet
"""


