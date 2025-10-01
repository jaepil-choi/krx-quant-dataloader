"""
Storage writers

What this module does:
- Concrete implementations of SnapshotWriter protocol (CSV, SQLite).
- Resume-safe, append-only writes with UTF-8 encoding and composite key enforcement.

Interaction:
- Instantiated by pipelines or user code and passed to ingestion functions.
- Abstracts backend details; pipelines remain decoupled from storage format.
"""


from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


class CSVSnapshotWriter:
    """CSV-based snapshot and factor writer (UTF-8, no BOM, append-only)."""

    def __init__(
        self,
        *,
        snapshot_path: Path | str,
        factor_path: Path | str,
        snapshot_fieldnames: Optional[List[str]] = None,
        factor_fieldnames: Optional[List[str]] = None,
    ):
        """Initialize CSV writer with paths and optional fieldnames.

        Parameters
        ----------
        snapshot_path : Path | str
            Path to snapshot CSV file.
        factor_path : Path | str
            Path to factor CSV file.
        snapshot_fieldnames : Optional[List[str]]
            Fieldnames for snapshot CSV. If None, inferred from first write.
        factor_fieldnames : Optional[List[str]]
            Fieldnames for factor CSV. If None, defaults to ['TRD_DD', 'ISU_SRT_CD', 'ADJ_FACTOR'].
        """
        self.snapshot_path = Path(snapshot_path)
        self.factor_path = Path(factor_path)
        self.snapshot_fieldnames = snapshot_fieldnames
        self.factor_fieldnames = factor_fieldnames or ["TRD_DD", "ISU_SRT_CD", "ADJ_FACTOR"]

        # Track if headers written
        self._snapshot_initialized = self.snapshot_path.exists()
        self._factor_initialized = self.factor_path.exists()

    def write_snapshot_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Append snapshot rows to CSV."""
        if not rows:
            return 0

        # Infer fieldnames from first row if not provided
        if self.snapshot_fieldnames is None:
            self.snapshot_fieldnames = list(rows[0].keys())

        mode = "a" if self._snapshot_initialized else "w"
        with open(self.snapshot_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.snapshot_fieldnames)
            if not self._snapshot_initialized:
                writer.writeheader()
                self._snapshot_initialized = True
            for row in rows:
                writer.writerow(row)
        return len(rows)

    def write_factor_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Append factor rows to CSV."""
        if not rows:
            return 0

        mode = "a" if self._factor_initialized else "w"
        with open(self.factor_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.factor_fieldnames)
            if not self._factor_initialized:
                writer.writeheader()
                self._factor_initialized = True
            for row in rows:
                writer.writerow(row)
        return len(rows)

    def close(self) -> None:
        """No-op for CSV; files are closed after each write."""
        pass


class SQLiteSnapshotWriter:
    """SQLite-based snapshot and factor writer (append-only, UPSERT on composite key)."""

    def __init__(
        self,
        *,
        db_path: Path | str,
        snapshot_table: str = "change_rates_snapshot",
        factor_table: str = "change_rates_adj_factor",
    ):
        """Initialize SQLite writer with DB path and table names.

        Parameters
        ----------
        db_path : Path | str
            Path to SQLite database file.
        snapshot_table : str
            Table name for snapshots.
        factor_table : str
            Table name for adjustment factors.
        """
        self.db_path = Path(db_path)
        self.snapshot_table = snapshot_table
        self.factor_table = factor_table
        self.conn = sqlite3.connect(str(self.db_path))
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create tables if they don't exist."""
        with self.conn:
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.snapshot_table} (
                    TRD_DD TEXT NOT NULL,
                    ISU_SRT_CD TEXT NOT NULL,
                    ISU_ABBRV TEXT,
                    BAS_PRC INTEGER,
                    TDD_CLSPRC INTEGER,
                    CMPPREVDD_PRC TEXT,
                    FLUC_RT TEXT,
                    ACC_TRDVOL INTEGER,
                    ACC_TRDVAL INTEGER,
                    FLUC_TP TEXT,
                    PRIMARY KEY (TRD_DD, ISU_SRT_CD)
                )
                """
            )
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.factor_table} (
                    TRD_DD TEXT NOT NULL,
                    ISU_SRT_CD TEXT NOT NULL,
                    ADJ_FACTOR TEXT,
                    PRIMARY KEY (TRD_DD, ISU_SRT_CD)
                )
                """
            )

    def write_snapshot_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Insert or replace snapshot rows."""
        if not rows:
            return 0

        # Extract fieldnames from first row
        fieldnames = list(rows[0].keys())
        placeholders = ", ".join(["?" for _ in fieldnames])
        cols = ", ".join(fieldnames)

        with self.conn:
            for row in rows:
                vals = [row.get(k) for k in fieldnames]
                self.conn.execute(
                    f"INSERT OR REPLACE INTO {self.snapshot_table} ({cols}) VALUES ({placeholders})",
                    vals,
                )
        return len(rows)

    def write_factor_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Insert or replace factor rows."""
        if not rows:
            return 0

        with self.conn:
            for row in rows:
                self.conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {self.factor_table} (TRD_DD, ISU_SRT_CD, ADJ_FACTOR)
                    VALUES (?, ?, ?)
                    """,
                    (row["TRD_DD"], row["ISU_SRT_CD"], row.get("ADJ_FACTOR", "")),
                )
        return len(rows)

    def close(self) -> None:
        """Commit and close the database connection."""
        self.conn.commit()
        self.conn.close()


__all__ = [
    "CSVSnapshotWriter",
    "SQLiteSnapshotWriter",
]
