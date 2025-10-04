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


def compute_cumulative_adjustments(
    adj_factors: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Compute cumulative adjustment multipliers per-symbol (reverse chronological product).
    
    This is Stage 5 of the data flow: ephemeral cache computation.
    
    Algorithm (per symbol):
      1. Sort by date ascending
      2. Reverse iterate (future → past)
      3. Cumulative product of adj_factors
      4. Most recent date gets 1.0, historical dates get product of future events
    
    Example (Samsung 50:1 split on 2018-05-04):
      Date       | adj_factor | cum_adj_multiplier
      2018-04-25 | 1.0        | 0.02 (= 1.0 × 1.0 × ... × 0.02)
      2018-05-03 | 1.0        | 0.02 (= 1.0 × 0.02)
      2018-05-04 | 0.02       | 1.0  (= 1.0, most recent)
      2018-05-08 | 1.0        | 1.0  (no future events)
    
    Precision: Uses Decimal for computation (arbitrary precision), converts to
    float64 for storage (sufficient for 1e-6 minimum precision requirement).
    
    Parameters:
      adj_factors: List of {TRD_DD, ISU_SRT_CD, adj_factor} from adj_factors table
                   adj_factor can be float, string, or None
    
    Returns: List of {TRD_DD, ISU_SRT_CD, cum_adj_multiplier:float}
    """
    # Group by symbol
    symbol_groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in adj_factors:
        symbol = row.get('ISU_SRT_CD')
        if symbol is None:
            continue
        if symbol not in symbol_groups:
            symbol_groups[symbol] = []
        symbol_groups[symbol].append(row)
    
    cum_adj_rows: List[Dict[str, Any]] = []
    
    for symbol, factors in symbol_groups.items():
        # Sort by date ascending
        factors_sorted = sorted(factors, key=lambda x: x.get('TRD_DD', ''))
        
        # Compute cumulative product (reverse chronological)
        cum_multipliers: List[float] = []
        cum_product = Decimal('1.0')
        
        for factor_row in reversed(factors_sorted):
            # Parse adj_factor (handle various types)
            adj_factor_val = factor_row.get('adj_factor', 1.0)
            
            if adj_factor_val is None or adj_factor_val == '' or adj_factor_val == 'None':
                # Missing factor = assume 1.0 (no adjustment)
                adj_factor_val = 1.0
            
            # Convert to Decimal for high-precision computation
            try:
                adj_factor = Decimal(str(adj_factor_val))
            except (ValueError, TypeError):
                # Fallback to 1.0 if conversion fails
                adj_factor = Decimal('1.0')
            
            cum_product *= adj_factor
            cum_multipliers.insert(0, float(cum_product))
        
        # Create output rows
        for factor_row, cum_mult in zip(factors_sorted, cum_multipliers):
            cum_adj_rows.append({
                'TRD_DD': factor_row['TRD_DD'],
                'ISU_SRT_CD': symbol,
                'cum_adj_multiplier': cum_mult
            })
    
    return cum_adj_rows


__all__ = [
    "compute_adj_factors_per_symbol",
    "compute_adj_factors_grouped",
    "compute_cumulative_adjustments",
]

