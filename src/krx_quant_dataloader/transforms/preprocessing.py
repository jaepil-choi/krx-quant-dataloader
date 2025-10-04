"""
Pre-storage preprocessing utilities

TEMPORAL DEPENDENCY: Before storage (per-snapshot)

What this module does:
- Client-side labeling (e.g., TRD_DD injection for daily snapshots).
- Numeric type coercion (KRX comma-separated strings → int).
- **Stateless per-row transforms** - can be applied to a single day's snapshot.
- Does not perform pivots or structural transforms; see shaping.py for that.

Interaction:
- Used by pipelines to prepare raw endpoint responses BEFORE writing to storage.
- Applied day-by-day during ingestion (does not require multi-day history).
- Does not perform hidden transforms; operations are explicit and parameterized.

Contrast with adjustment.py:
- preprocessing.py: Per-snapshot, stateless (before write)
- adjustment.py: Multi-day, stateful LAG semantics (after write, requires full history)
"""


from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


def parse_int_krx(value: Any) -> Optional[int]:
    """Parse KRX numeric strings like "1,234" into int. Returns None on failure.

    This is intentionally tolerant; we avoid raising in preprocessing utilities.
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).replace(",", ""))
    except Exception:
        return None


def preprocess_change_rates_row(row: Dict[str, Any], *, trade_date: str) -> Dict[str, Any]:
    """Preprocess a single MDCSTAT01602-style row: label and coerce.

    - Injects TRD_DD
    - Coerces integer-like numeric strings for key fields
    - Leaves unknown fields as-is (pass-through)
    """
    shaped: Dict[str, Any] = dict(row)
    shaped["TRD_DD"] = trade_date
    # Core numeric fields we depend on for factor computation
    shaped["BAS_PRC"] = parse_int_krx(row.get("BAS_PRC"))
    shaped["TDD_CLSPRC"] = parse_int_krx(row.get("TDD_CLSPRC"))
    shaped["CMPPREVDD_PRC"] = parse_int_krx(row.get("CMPPREVDD_PRC"))
    # Common volume/value fields
    shaped["ACC_TRDVOL"] = parse_int_krx(row.get("ACC_TRDVOL"))
    shaped["ACC_TRDVAL"] = parse_int_krx(row.get("ACC_TRDVAL"))
    return shaped


def preprocess_change_rates_rows(
    rows: Iterable[Dict[str, Any]], *, trade_date: str
) -> List[Dict[str, Any]]:
    """Preprocess many MDCSTAT01602-style rows: label and coerce for each."""
    return [preprocess_change_rates_row(r, trade_date=trade_date) for r in rows]


__all__ = [
    "parse_int_krx",
    "preprocess_change_rates_row",
    "preprocess_change_rates_rows",
]

