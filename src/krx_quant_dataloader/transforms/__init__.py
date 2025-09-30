"""
Transforms layer

Pure, in-memory transformations over rows:
- adjustment: compute adj_factor from MDCSTAT01602 daily snapshots
- shaping: tidy/wide reshaping and column normalization
- validation: light schema checks on rows

No network IO; safe to unit-test in isolation.
"""


