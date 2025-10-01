"""
Storage protocols

What this module does:
- Defines abstract protocols for snapshot and factor persistence.
- Enables dependency injection in pipelines without coupling to concrete backends.

Interaction:
- Implemented by writers.py (CSV, SQLite, Parquet).
- Consumed by pipelines for resume-safe per-day ingestion.
"""


from __future__ import annotations

from typing import Any, Dict, List, Protocol


class SnapshotWriter(Protocol):
    """Protocol for writing daily snapshot and adjustment factor rows."""

    def write_snapshot_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Write snapshot rows to storage.

        Parameters
        ----------
        rows : List[Dict[str, Any]]
            Preprocessed snapshot rows with TRD_DD and coerced numeric fields.

        Returns
        -------
        int
            Count of rows written.
        """
        ...

    def write_factor_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Write adjustment factor rows to storage.

        Parameters
        ----------
        rows : List[Dict[str, Any]]
            Factor rows with keys: TRD_DD, ISU_SRT_CD, ADJ_FACTOR.

        Returns
        -------
        int
            Count of rows written.
        """
        ...

    def close(self) -> None:
        """Flush and close any open resources."""
        ...


__all__ = [
    "SnapshotWriter",
]

