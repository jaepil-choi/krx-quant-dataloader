"""
Pipeline Orchestrator: Coordinates 3-stage data pipeline.

This module orchestrates the complete data pipeline for DataLoader initialization:
- Stage 1: Snapshots (check/fetch/ingest from KRX)
- Stage 2: Adjustments (compute factors + build ephemeral cache)
- Stage 3: Universes (compute liquidity ranks + build universe tables)

Design principles:
- Delegates to specialized pipeline modules (no inline implementation)
- Checks data existence before triggering expensive operations
- Prints progress for observability
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional


class PipelineOrchestrator:
    """
    Orchestrates 3-stage data pipeline for DataLoader initialization.
    
    Responsibilities:
    - Check if data exists for date range
    - Delegate ingestion to pipelines/snapshots.py
    - Delegate adjustment cache to transforms/adjustment.py
    - Delegate universe building to pipelines/liquidity_ranking.py + universe_builder.py
    
    Does NOT:
    - Implement ingestion logic (delegates to pipelines)
    - Implement ranking logic (delegates to pipelines)
    - Handle raw API calls directly (uses raw_client)
    
    Parameters
    ----------
    db_path : Path
        Path to persistent Parquet database
    temp_path : Path
        Path for ephemeral cache
    raw_client : Optional
        RawClient instance for fetching from KRX API (None if DB pre-built)
    """
    
    def __init__(
        self,
        db_path: Path,
        temp_path: Path,
        raw_client=None,
    ):
        self._db_path = db_path
        self._temp_path = temp_path
        self._raw_client = raw_client
    
    def ensure_data_ready(self, start_date: str, end_date: str) -> None:
        """
        Run 3-stage pipeline to ensure DB completeness.
        
        Parameters
        ----------
        start_date : str
            Start of date range (YYYYMMDD)
        end_date : str
            End of date range (YYYYMMDD)
        """
        print(f"[PipelineOrchestrator] Ensuring data ready for: {start_date} → {end_date}")
        
        # Stage 1: Ensure snapshots exist
        self._ensure_snapshots(start_date, end_date)
        
        # Stage 2: Build adjustment cache
        self._build_adjustment_cache(start_date, end_date)
        
        # Stage 3: Build universe tables
        self._build_universe_tables(start_date, end_date)
        
        print(f"[PipelineOrchestrator] Data ready")
    
    def _ensure_snapshots(self, start_date: str, end_date: str) -> None:
        """
        Stage 1: Ensure snapshots exist for date range.
        
        Checks if snapshots exist; if missing, ingests from KRX API.
        """
        from ..storage.query import query_parquet_table
        
        print(f"[Stage 1] Checking snapshots...")
        
        # Check if snapshots table exists
        snapshots_path = self._db_path / 'snapshots'
        if not snapshots_path.exists():
            print(f"  → Snapshots table missing, will auto-ingest")
            self._ingest_missing_dates(start_date, end_date)
            return
        
        # Check for missing dates (simplified for MVP)
        try:
            existing_df = query_parquet_table(
                self._db_path,
                'snapshots',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD']
            )
            existing_dates = set(existing_df['TRD_DD'].unique())
            print(f"  → Found {len(existing_dates)} dates in DB")
        except Exception as e:
            print(f"  → Error querying snapshots: {e}")
            existing_dates = set()
        
        # For MVP: Assume all dates exist if table exists
        # TODO: Implement missing date detection and selective ingestion
        print(f"  [OK] Snapshots exist")
    
    def _ingest_missing_dates(self, start_date: str, end_date: str) -> None:
        """
        Ingest missing dates from KRX API.
        
        Delegates to pipelines/snapshots.py for actual ingestion.
        """
        from ..pipelines.snapshots import ingest_change_rates_day
        from ..storage.writers import ParquetSnapshotWriter
        from ..storage.query import query_parquet_table
        
        if self._raw_client is None:
            raise ValueError(
                "Snapshots missing but no raw_client provided. "
                "Either pre-build DB or provide raw_client for auto-ingestion."
            )
        
        print(f"  → Auto-ingesting dates [{start_date}, {end_date}]...")
        print(f"  → Note: This may take time due to API rate limiting")
        
        writer = ParquetSnapshotWriter(root_path=self._db_path)
        
        # Generate date range properly using datetime
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        total_rows = 0
        date_count = 0
        
        # Ingest each date
        current_dt = start_dt
        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y%m%d')
            try:
                count = ingest_change_rates_day(
                    raw_client=self._raw_client,
                    date=date_str,
                    market='ALL',
                    adjusted_flag=1,
                    writer=writer
                )
                if count > 0:
                    total_rows += count
                    date_count += 1
                    print(f"    [{date_str}] {count} rows")
            except Exception as e:
                # Silently skip holidays/weekends
                pass
            
            # Increment by one day
            current_dt += timedelta(days=1)
        
        print(f"  [OK] Ingested {total_rows} rows across {date_count} dates")
        
        # Compute and persist adjustment factors after ingestion
        if total_rows > 0:
            print(f"  → Computing adjustment factors...")
            try:
                from ..pipelines.snapshots import compute_and_persist_adj_factors
                
                # Read back snapshots to compute factors
                snapshots_df = query_parquet_table(
                    self._db_path,
                    'snapshots',
                    start_date=start_date,
                    end_date=end_date,
                    fields=['TRD_DD', 'ISU_SRT_CD', 'BAS_PRC', 'TDD_CLSPRC']
                )
                
                factor_count = compute_and_persist_adj_factors(
                    snapshots_df.to_dict('records'),
                    writer
                )
                print(f"  [OK] Computed {factor_count} adjustment factors")
            except Exception as e:
                print(f"  → Warning: Failed to compute adjustment factors: {e}")
            
            # Compute and persist liquidity ranks
            print(f"  → Computing liquidity ranks...")
            try:
                from ..pipelines.liquidity_ranking import compute_liquidity_ranks, write_liquidity_ranks
                
                # Compute ranks from snapshots
                ranks_df = compute_liquidity_ranks(snapshots_df)
                
                # Persist ranks
                for date in sorted(ranks_df['TRD_DD'].unique()):
                    date_ranks = ranks_df[ranks_df['TRD_DD'] == date]
                    write_liquidity_ranks(date_ranks, self._db_path, date=date)
                
                print(f"  [OK] Computed {len(ranks_df)} liquidity ranks")
            except Exception as e:
                print(f"  → Warning: Failed to compute liquidity ranks: {e}")
    
    def _build_adjustment_cache(self, start_date: str, end_date: str) -> None:
        """
        Stage 2: Build ephemeral cumulative adjustment cache.
        
        Computes cumulative multipliers for the date range and stores in temp/.
        """
        from ..storage.query import query_parquet_table
        from ..transforms.adjustment import compute_cumulative_adjustments
        from ..storage.writers import ParquetSnapshotWriter
        
        print(f"[Stage 2] Building adjustment cache...")
        
        # Check if adj_factors exist
        adj_factors_path = self._db_path / 'adj_factors'
        if not adj_factors_path.exists():
            print(f"  → No adjustment factors found, skipping cache build")
            return
        
        # Query adjustment factors for date range
        try:
            adj_factors_df = query_parquet_table(
                self._db_path,
                'adj_factors',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
            )
            
            if adj_factors_df.empty:
                print(f"  → No adjustment factors in date range")
                return
            
            # Compute cumulative adjustments
            adj_rows = adj_factors_df.to_dict('records')
            cum_adj_rows = compute_cumulative_adjustments(adj_rows)
            
            # Write to ephemeral cache
            writer = ParquetSnapshotWriter(root_path=self._temp_path)
            for date in set(r['TRD_DD'] for r in cum_adj_rows):
                date_rows = [r for r in cum_adj_rows if r['TRD_DD'] == date]
                writer.write_cumulative_adjustments(date_rows, date=date)
            
            print(f"  [OK] Cached {len(cum_adj_rows)} cumulative adjustments")
        
        except Exception as e:
            print(f"  → Warning: Failed to build adjustment cache: {e}")
            print(f"  → Adjusted queries may fail")
    
    def _build_universe_tables(self, start_date: str, end_date: str) -> None:
        """
        Stage 3: Build pre-computed universe tables.
        
        Computes liquidity ranks and builds universe membership tables.
        """
        from ..storage.query import query_parquet_table
        
        print(f"[Stage 3] Building universe tables...")
        
        # Check if liquidity_ranks exist
        ranks_path = self._db_path / 'liquidity_ranks'
        if not ranks_path.exists():
            print(f"  → Liquidity ranks missing, computing...")
            self._compute_liquidity_ranks(start_date, end_date)
        else:
            print(f"  → Liquidity ranks exist")
        
        # Check if universes exist
        universes_path = self._db_path / 'universes'
        if not universes_path.exists():
            print(f"  → Universe tables missing, building...")
            self._build_universes(start_date, end_date)
        else:
            print(f"  → Universe tables exist")
        
        print(f"  [OK] Universe tables ready")
    
    def _compute_liquidity_ranks(self, start_date: str, end_date: str) -> None:
        """Compute liquidity ranks from snapshots."""
        from ..pipelines.liquidity_ranking import compute_liquidity_ranks, write_liquidity_ranks
        from ..storage.query import query_parquet_table
        from ..storage.writers import ParquetSnapshotWriter
        
        try:
            # Query snapshots
            snapshots_df = query_parquet_table(
                self._db_path,
                'snapshots',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', 'ACC_TRDVAL', 'ISU_ABBRV', 'TDD_CLSPRC']
            )
            
            # Compute ranks
            ranks_df = compute_liquidity_ranks(snapshots_df)
            
            # Persist
            writer = ParquetSnapshotWriter(root_path=self._db_path)
            for date in sorted(ranks_df['TRD_DD'].unique()):
                date_ranks = ranks_df[ranks_df['TRD_DD'] == date]
                write_liquidity_ranks(date_ranks, writer, date=date)
            
            print(f"    → Computed {len(ranks_df)} rank rows")
        
        except Exception as e:
            print(f"    → Warning: Failed to compute liquidity ranks: {e}")
    
    def _build_universes(self, start_date: str, end_date: str) -> None:
        """Build universe tables from liquidity ranks."""
        from ..pipelines.universe_builder import build_universes_and_persist
        from ..storage.query import query_parquet_table
        from ..storage.writers import ParquetSnapshotWriter
        
        try:
            # Query liquidity ranks
            ranks_df = query_parquet_table(
                self._db_path,
                'liquidity_ranks',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', 'xs_liquidity_rank']
            )
            
            print(f"    → Queried {len(ranks_df)} liquidity rank rows")
            
            if ranks_df.empty:
                print(f"    → Warning: No liquidity ranks found, skipping universe build")
                return
            
            # Build universes
            universe_tiers = {
                'univ100': 100,
                'univ200': 200,
                'univ500': 500,
                'univ1000': 1000,
            }
            writer = ParquetSnapshotWriter(root_path=self._db_path)
            count = build_universes_and_persist(ranks_df, universe_tiers, writer)
            
            print(f"    → Built {count} universe membership rows")
        
        except Exception as e:
            print(f"    → Warning: Failed to build universes: {e}")

