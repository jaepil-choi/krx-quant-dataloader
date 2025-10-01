"""
Shaping utilities

What this module does:
- Structural transformations: pivot wide/long formats for tidy outputs.
- Does not perform type coercion or data cleaning; see preprocessing.py for that.

Interaction:
- Used by apis/dataloader.py to produce tidy outputs when requested (opt-in).
- Does not perform hidden transforms; operations are explicit and parameterized.
"""


from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


def pivot_long_to_wide(
    rows: Iterable[Dict[str, Any]],
    *,
    index_key: str,
    column_key: str,
    value_key: str,
) -> List[Dict[str, Any]]:
    """Pivot long-format rows to wide format.

    Parameters
    ----------
    rows : Iterable[Dict[str, Any]]
        Long-format rows.
    index_key : str
        Key to use as row index (e.g., 'TRD_DD').
    column_key : str
        Key whose values become column names (e.g., 'ISU_SRT_CD').
    value_key : str
        Key whose values fill the pivoted cells (e.g., 'TDD_CLSPRC').

    Returns
    -------
    List[Dict[str, Any]]
        Wide-format rows, sorted by index_key. Each row: {index_key: value, col1: v1, col2: v2, ...}.
        Missing values become None.

    Example
    -------
    Input:
      [{'date': '20250101', 'symbol': 'A', 'close': 100},
       {'date': '20250101', 'symbol': 'B', 'close': 200},
       {'date': '20250102', 'symbol': 'A', 'close': 105}]

    Output (index_key='date', column_key='symbol', value_key='close'):
      [{'date': '20250101', 'A': 100, 'B': 200},
       {'date': '20250102', 'A': 105, 'B': None}]
    """
    # Collect all unique columns
    columns = set()
    by_index: Dict[Any, Dict[Any, Any]] = {}

    for row in rows:
        idx_val = row.get(index_key)
        col_val = row.get(column_key)
        val = row.get(value_key)
        if idx_val is None or col_val is None:
            continue  # skip malformed rows
        columns.add(col_val)
        if idx_val not in by_index:
            by_index[idx_val] = {}
        by_index[idx_val][col_val] = val

    # Build wide rows
    wide_rows: List[Dict[str, Any]] = []
    for idx_val in sorted(by_index.keys()):
        row_dict: Dict[str, Any] = {index_key: idx_val}
        for col in sorted(columns):
            row_dict[col] = by_index[idx_val].get(col, None)
        wide_rows.append(row_dict)

    return wide_rows


__all__ = [
    "pivot_long_to_wide",
]
