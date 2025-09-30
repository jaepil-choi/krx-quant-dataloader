"""
Adjustment factor computation from daily snapshots (MDCSTAT01602)

What this module does:
- Given per-day snapshots of MDCSTAT01602 (전종목등락률) with fields such as
  BAS_PRC and TDD_CLSPRC and a client-injected TRD_DD, compute per-symbol
  adj_factor for the transition (t-1 → t):

    adj_factor_{t-1→t}(s) = BAS_PRC_t(s) / TDD_CLSPRC_{t-1}(s)

- Normally equals 1; deviations reflect corporate actions. This factor allows
  downstream consumers to compose adjusted series without rebuilding a full
  back-adjusted history at fetch time.

How it interacts with other modules:
- Called by pipelines/snapshots.py after fetching and labeling daily snapshots
  via RawClient and orchestration.
- Returns rows augmented with adj_factor or a separate mapping keyed by
  (TRD_DD, ISU_SRT_CD).

Implementation note:
- Keep computation pure and tolerant to missing neighbors (e.g., halts/new listings).
  Skip or flag pairs where prior day is missing. Avoid numeric coercions here;
  leave concrete parsing to a shaping utility.
"""


