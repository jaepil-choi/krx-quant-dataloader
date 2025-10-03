"""
Post-storage adjustment factor computation

TEMPORAL DEPENDENCY: After storage (multi-day history required)

What this module does:
- Given **multiple days** of snapshots from MDCSTAT01602 (전종목등락률), compute 
  per-symbol adjustment factors for corporate actions using LAG semantics:

    adj_factor_{t-1→t}(s) = BAS_PRC_t(s) / TDD_CLSPRC_{t-1}(s)

- Normally equals 1; deviations reflect corporate actions (splits, dividends, etc.).
- This factor allows downstream consumers to compose adjusted series without 
  rebuilding a full back-adjusted history at fetch time.

CRITICAL: This is a **post-hoc batch job**:
- **Cannot run per-snapshot** - requires previous trading day's close price
- **Must run AFTER ingestion completes** - needs full date range in storage
- Uses SQL-style LAG semantics: PARTITION BY ISU_SRT_CD ORDER BY TRD_DD

How it interacts with other modules:
- Called by pipelines/snapshots.py **AFTER** all daily snapshots are written to storage.
- Reads back complete snapshot history from Parquet DB.
- Returns factor rows to be persisted separately (adj_factors table).

Contrast with preprocessing.py:
- preprocessing.py: Per-snapshot, stateless (before write, no history needed)
- adjustment.py: Multi-day, stateful LAG semantics (after write, requires full history)

Implementation note:
- Keep computation pure and tolerant to missing neighbors (e.g., halts/new listings).
- First observation per symbol yields empty string (no previous close available).
- Avoid numeric coercions here; expects preprocessed input with int types.
"""


from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping


def compute_adj_factors_per_symbol(rows_sorted: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compute adjustment factors for a single symbol given chronologically sorted rows.

    Each row is expected to contain keys: TRD_DD, BAS_PRC (int|None), TDD_CLSPRC (int|None), ISU_SRT_CD.
    Returns one factor row per input row: {TRD_DD, ISU_SRT_CD, ADJ_FACTOR:str}
    ADJ_FACTOR is an empty string when previous close is missing.
    """
    factors: List[Dict[str, Any]] = []
    prev_close = None
    symbol = None
    for row in rows_sorted:
        symbol = row.get("ISU_SRT_CD", symbol)
        bas = row.get("BAS_PRC")
        factor_str = ""
        if prev_close is not None and prev_close != 0 and bas is not None:
            factor_str = str(Decimal(bas) / Decimal(prev_close))
        factors.append({
            "TRD_DD": row.get("TRD_DD"),
            "ISU_SRT_CD": symbol,
            "ADJ_FACTOR": factor_str,
        })
        if row.get("TDD_CLSPRC") is not None:
            prev_close = row["TDD_CLSPRC"]
    return factors


def compute_adj_factors_grouped(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compute adjustment factors by grouping rows per ISU_SRT_CD and ordering by TRD_DD.

    Equivalent to SQL window: LAG(TDD_CLSPRC) OVER (PARTITION BY ISU_SRT_CD ORDER BY TRD_DD)
    and ADJ_FACTOR = BAS_PRC / LAG_TDD_CLSPRC.
    """
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        s = row.get("ISU_SRT_CD")
        if s is None:
            # skip malformed rows
            continue
        by_symbol.setdefault(s, []).append(row)

    out: List[Dict[str, Any]] = []
    for s, group in by_symbol.items():
        group.sort(key=lambda r: r.get("TRD_DD"))
        out.extend(compute_adj_factors_per_symbol(group))
    # Sort output for stability: by TRD_DD then ISU_SRT_CD
    out.sort(key=lambda r: (r.get("TRD_DD"), r.get("ISU_SRT_CD")))
    return out


__all__ = [
    "compute_adj_factors_per_symbol",
    "compute_adj_factors_grouped",
]

