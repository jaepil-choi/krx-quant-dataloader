"""
Daily snapshots pipelines (e.g., MDCSTAT01602)

What this module does:
- Provides a high-level function to iterate a date range [start, end] and, for
  each date D, call an endpoint with strtDd=endDd=D. It injects TRD_DD=D into
  each row to produce a labeled cross-sectional time series.
- Optionally composes with transforms.adjustment to compute per-day adj_factors.

Interaction:
- Consumed by apis/dataloader.py to deliver user-facing results.
- Uses RawClient under the hood; does not import transport/orchestration directly.

Implementation plan (future):
- fetch_change_rates_range(raw_client, start, end, market, adjusted_flag) -> List[rows]
- compute_and_attach_adj_factors(rows_by_day) -> List[rows_with_factor]
"""


