"""
Transforms layer

Pure, in-memory transformations over rows (no network IO):

Modules:
- preprocessing.py: Client-side labeling (TRD_DD injection), numeric coercion (comma strings → int).
  Functions: parse_int_krx, preprocess_change_rates_row(s).
- shaping.py: Structural pivots (long → wide); no type coercion or data cleaning.
  Functions: pivot_long_to_wide.
- adjustment.py: Per-symbol, date-ordered adjustment factors (SQL LAG semantics).
  Functions: compute_adj_factors_per_symbol, compute_adj_factors_grouped.
- validation.py: Optional lightweight schema checks (future).

Design:
- Preprocessing ≠ shaping ≠ adjustment (clear separation).
- All functions are pure; safe to unit-test in isolation.
"""


