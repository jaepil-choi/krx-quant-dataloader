"""
Storage writers (optional)

Contains opt-in adapters to persist rows produced by pipelines to external
stores (CSV, Parquet, SQL). Not used implicitly by apis.

Design:
- Keep writer functions small and explicit; accept rows (list[dict]) and a
  destination. Avoid tight coupling to any particular DB client.
"""


