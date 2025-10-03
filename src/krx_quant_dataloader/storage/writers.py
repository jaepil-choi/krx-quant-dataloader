"""
Storage writers

What this module does:
- Concrete implementations of SnapshotWriter protocol (CSV, SQLite, Parquet).
- Resume-safe, append-only writes with UTF-8 encoding and composite key enforcement.
- Parquet: Hive-partitioned by TRD_DD, sorted writes for row-group pruning.

Interaction:
- Instantiated by pipelines or user code and passed to ingestion functions.
- Abstracts backend details; pipelines remain decoupled from storage format.
"""


from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .schema import SNAPSHOTS_SCHEMA, ADJ_FACTORS_SCHEMA, LIQUIDITY_RANKS_SCHEMA


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


class ParquetSnapshotWriter:
    """Parquet-based snapshot and factor writer (Hive-partitioned by TRD_DD)."""

    def __init__(
        self,
        *,
        root_path: Path | str,
        snapshots_table: str = "snapshots",
        factors_table: str = "adj_factors",
        ranks_table: str = "liquidity_ranks",
    ):
        """Initialize Parquet writer with root path and table names.

        Directory structure:
            root_path/
                snapshots/
                    TRD_DD=20240820/data.parquet
                    TRD_DD=20240821/data.parquet
                adj_factors/
                    TRD_DD=20240820/data.parquet
                liquidity_ranks/
                    TRD_DD=20240820/data.parquet

        Parameters
        ----------
        root_path : Path | str
            Root directory for Parquet database.
        snapshots_table : str
            Subdirectory name for snapshots table.
        factors_table : str
            Subdirectory name for adjustment factors table.
        ranks_table : str
            Subdirectory name for liquidity ranks table.
        """
        self.root_path = Path(root_path)
        self.snapshots_path = self.root_path / snapshots_table
        self.factors_path = self.root_path / factors_table
        self.ranks_path = self.root_path / ranks_table

        # Create root directories
        self.snapshots_path.mkdir(parents=True, exist_ok=True)
        self.factors_path.mkdir(parents=True, exist_ok=True)
        self.ranks_path.mkdir(parents=True, exist_ok=True)

    def write_snapshot_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Write snapshot rows to Hive-partitioned Parquet.

        Rows must contain 'TRD_DD' field for partitioning.
        Data is sorted by ISU_SRT_CD for row-group pruning optimization.

        Parameters
        ----------
        rows : List[Dict[str, Any]]
            Preprocessed snapshot rows with TRD_DD and coerced numeric fields.

        Returns
        -------
        int
            Count of rows written.
        """
        if not rows:
            return 0

        # Sort by ISU_SRT_CD for row-group pruning
        rows_sorted = sorted(rows, key=lambda r: r.get('ISU_SRT_CD', ''))

        # Convert to PyArrow table
        # Note: Schema will filter out any fields not in SNAPSHOTS_SCHEMA
        try:
            table = pa.Table.from_pylist(rows_sorted, schema=SNAPSHOTS_SCHEMA)
        except Exception as e:
            # If schema doesn't match, infer schema and convert
            # This allows flexibility for additional fields in raw data
            table = pa.Table.from_pylist(rows_sorted)
            # Cast to target schema (drops extra fields)
            table = table.cast(SNAPSHOTS_SCHEMA, safe=False)

        # Write to Hive-partitioned dataset
        # TRD_DD must be in the data for partitioning (added by preprocessing)
        # Extract partition value from first row
        trade_date = rows[0].get('TRD_DD')
        if not trade_date:
            raise ValueError("Rows must contain 'TRD_DD' field for partitioning")

        # Write to specific partition
        partition_path = self.snapshots_path / f"TRD_DD={trade_date}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        pq.write_table(
            table,
            partition_path / "data.parquet",
            row_group_size=1000,  # ~1000 stocks per row group
            compression='zstd',
            compression_level=3,
        )

        return len(rows)

    def write_factor_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Write adjustment factor rows to Hive-partitioned Parquet.

        Rows must contain 'TRD_DD' field for partitioning.
        Data is sorted by ISU_SRT_CD for row-group pruning optimization.

        Parameters
        ----------
        rows : List[Dict[str, Any]]
            Factor rows with keys: TRD_DD, ISU_SRT_CD, ADJ_FACTOR or adj_factor.

        Returns
        -------
        int
            Count of rows written.
        """
        if not rows:
            return 0

        # Normalize field names: ADJ_FACTOR → adj_factor
        normalized_rows = []
        for row in rows:
            normalized = dict(row)
            if 'ADJ_FACTOR' in normalized and 'adj_factor' not in normalized:
                adj_val = normalized.pop('ADJ_FACTOR')
                # Convert to float, handle empty strings
                if adj_val == '' or adj_val is None:
                    normalized['adj_factor'] = None
                else:
                    try:
                        normalized['adj_factor'] = float(adj_val)
                    except (ValueError, TypeError):
                        normalized['adj_factor'] = None
            normalized_rows.append(normalized)

        # Group by TRD_DD and write per partition
        by_date: Dict[str, List[Dict[str, Any]]] = {}
        for row in normalized_rows:
            date = row.get('TRD_DD')
            if not date:
                continue  # skip rows without date
            by_date.setdefault(date, []).append(row)

        total_written = 0
        for trade_date, date_rows in by_date.items():
            # Sort by ISU_SRT_CD for row-group pruning
            date_rows_sorted = sorted(date_rows, key=lambda r: r.get('ISU_SRT_CD', ''))

            # Convert to PyArrow table
            try:
                table = pa.Table.from_pylist(date_rows_sorted, schema=ADJ_FACTORS_SCHEMA)
            except Exception:
                table = pa.Table.from_pylist(date_rows_sorted)
                table = table.cast(ADJ_FACTORS_SCHEMA, safe=False)

            # Write to specific partition
            partition_path = self.factors_path / f"TRD_DD={trade_date}"
            partition_path.mkdir(parents=True, exist_ok=True)
            
            pq.write_table(
                table,
                partition_path / "data.parquet",
                row_group_size=1000,
                compression='zstd',
                compression_level=3,
            )
            total_written += len(date_rows)

        return total_written

    def write_liquidity_ranks(self, rows: List[Dict[str, Any]]) -> int:
        """Write liquidity rank rows to Hive-partitioned Parquet.

        This method is separate from the SnapshotWriter protocol as it's used
        by the universe builder pipeline, not the snapshot ingestion pipeline.

        Rows must contain 'TRD_DD' field for partitioning.
        Data is sorted by xs_liquidity_rank for row-group pruning optimization.

        Parameters
        ----------
        rows : List[Dict[str, Any]]
            Rank rows with keys: TRD_DD, ISU_SRT_CD, xs_liquidity_rank, ACC_TRDVAL.

        Returns
        -------
        int
            Count of rows written.
        """
        if not rows:
            return 0

        # Group by TRD_DD and write per partition
        by_date: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            date = row.get('TRD_DD')
            if not date:
                continue
            by_date.setdefault(date, []).append(row)

        total_written = 0
        for trade_date, date_rows in by_date.items():
            # Sort by xs_liquidity_rank (so top-100 queries can prune row groups)
            date_rows_sorted = sorted(date_rows, key=lambda r: r.get('xs_liquidity_rank', float('inf')))

            # Convert to PyArrow table
            try:
                table = pa.Table.from_pylist(date_rows_sorted, schema=LIQUIDITY_RANKS_SCHEMA)
            except Exception:
                table = pa.Table.from_pylist(date_rows_sorted)
                table = table.cast(LIQUIDITY_RANKS_SCHEMA, safe=False)

            # Write to specific partition
            partition_path = self.ranks_path / f"TRD_DD={trade_date}"
            partition_path.mkdir(parents=True, exist_ok=True)
            
            pq.write_table(
                table,
                partition_path / "data.parquet",
                row_group_size=500,  # Smaller row groups for top-N queries
                compression='zstd',
                compression_level=3,
            )
            total_written += len(date_rows)

        return total_written

    def close(self) -> None:
        """No-op for Parquet; files are closed after each write."""
        pass


__all__ = [
    "CSVSnapshotWriter",
    "SQLiteSnapshotWriter",
    "ParquetSnapshotWriter",
]
