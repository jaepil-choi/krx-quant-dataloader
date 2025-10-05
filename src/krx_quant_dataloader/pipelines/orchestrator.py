"""
Pipeline Orchestrator: Coordinates 3-stage progressive enrichment pipeline.

This module orchestrates the complete data pipeline for DataLoader initialization:
- Stage 0-1: Download raw data → Write to pricevolume DB (raw fields only)
- Stage 2: Enrich with adjustment factors (read → calculate → rewrite partition)
- Stage 3: Enrich with liquidity ranks (read → calculate → rewrite partition)

Data Flow:
1. Download → temp/snapshots/date={date}/raw.parquet (ephemeral)
2. Move → pricevolume/date={date}/data.parquet (persistent, raw fields)
3. Enrich → pricevolume/date={date}/data.parquet (+ adj_factor column)
4. Enrich → pricevolume/date={date}/data.parquet (+ liquidity_rank column)

Design principles:
- Single persistent DB (pricevolume/) with progressive enrichment
- Atomic writes with staging/backup safety
- Single-level partitioning (date= only, all markets in one file)
- Delegates to specialized modules (TempSnapshotWriter, PriceVolumeWriter, enrichers)
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


class PipelineOrchestrator:
    """
    Orchestrates 3-stage progressive enrichment pipeline.
    
    Responsibilities:
    - Stage 0-1: Download raw data → Write to pricevolume DB
    - Stage 2: Enrich with adjustment factors
    - Stage 3: Enrich with liquidity ranks
    - Stage 4: Build universe tables (from enriched pricevolume data)
    
    Uses:
    - TempSnapshotWriter: Stage 0 (temp/snapshots/)
    - PriceVolumeWriter: Stage 1 (atomic write to pricevolume/)
    - AdjustmentEnricher: Stage 2 (read → enrich → rewrite)
    - LiquidityRankEnricher: Stage 3 (read → enrich → rewrite)
    
    Parameters
    ----------
    pricevolume_path : Path
        Path to persistent pricevolume DB (data/pricevolume/)
    temp_snapshots_path : Path
        Path for ephemeral raw downloads (data/temp/snapshots/)
    temp_staging_path : Path
        Path for atomic write staging (data/temp/staging/)
    temp_backup_path : Path
        Path for rollback safety (data/temp/backup/)
    raw_client : Optional
        RawClient instance for fetching from KRX API (None if DB pre-built)
    """
    
    def __init__(
        self,
        pricevolume_path: Path,
        temp_snapshots_path: Path,
        temp_staging_path: Path,
        temp_backup_path: Path,
        raw_client=None,
    ):
        self._pricevolume_path = pricevolume_path
        self._temp_snapshots_path = temp_snapshots_path
        self._temp_staging_path = temp_staging_path
        self._temp_backup_path = temp_backup_path
        self._raw_client = raw_client
    
    def ensure_data_ready(self, start_date: str, end_date: str) -> None:
        """
        Run 3-stage progressive enrichment pipeline.
        
        Parameters
        ----------
        start_date : str
            Start of date range (YYYYMMDD)
        end_date : str
            End of date range (YYYYMMDD)
        """
        print(f"[PipelineOrchestrator] Ensuring data ready for: {start_date} → {end_date}")
        
        # Stage 0-1: Download raw data → Write to pricevolume DB
        self._ensure_raw_data(start_date, end_date)
        
        # Stage 2: Enrich with adjustment factors
        self._enrich_with_adjustments(start_date, end_date)
        
        # Stage 3: Enrich with liquidity ranks
        self._enrich_with_liquidity_ranks(start_date, end_date)
        
        # Stage 4: Build universe tables (from enriched pricevolume)
        self._build_universe_tables(start_date, end_date)
        
        print(f"[PipelineOrchestrator] Data ready")
    
    def _ensure_raw_data(self, start_date: str, end_date: str) -> None:
        """
        Stage 0-1: Ensure raw data exists in pricevolume DB.
        
        Checks if pricevolume partitions exist; if missing, ingests from KRX API.
        """
        print(f"[Stage 0-1] Checking pricevolume DB...")
        
        # Check if pricevolume table exists
        if not self._pricevolume_path.exists():
            print(f"  → PriceVolume DB missing, will auto-ingest")
            self._ingest_missing_dates(start_date, end_date)
            return
        
        # Check for missing dates
        from datetime import datetime
        
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        missing_dates = []
        current_dt = start_dt
        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y%m%d')
            partition_path = self._pricevolume_path / f"TRD_DD={date_str}"
            if not partition_path.exists():
                missing_dates.append(date_str)
            current_dt += timedelta(days=1)
        
        if missing_dates:
            print(f"  → Found {len(missing_dates)} missing dates, auto-ingesting...")
            self._ingest_specific_dates(missing_dates)
        else:
            print(f"  [OK] All dates present in pricevolume DB")
    
    def _ingest_missing_dates(self, start_date: str, end_date: str) -> None:
        """
        Ingest date range from KRX API using new pipeline.
        
        Stage 0-1: Download → temp/snapshots/ → pricevolume/
        """
        if self._raw_client is None:
            raise ValueError(
                "PriceVolume DB missing but no raw_client provided. "
                "Either pre-build DB or provide raw_client for auto-ingestion."
            )
        
        # Generate date list
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        dates_to_ingest = []
        current_dt = start_dt
        while current_dt <= end_dt:
            dates_to_ingest.append(current_dt.strftime('%Y%m%d'))
            current_dt += timedelta(days=1)
        
        self._ingest_specific_dates(dates_to_ingest)
    
    def _ingest_specific_dates(self, dates: list[str]) -> None:
        """
        Ingest specific dates from KRX API.
        
        Stage 0: Download raw data to temp/snapshots/
        Stage 1: Atomically move to pricevolume/ (raw fields ONLY, no placeholders)
        """
        from ..pipelines.snapshots import preprocess_change_rates_rows
        from ..storage.writers import TempSnapshotWriter, PriceVolumeWriter
        
        print(f"  → Auto-ingesting {len(dates)} dates...")
        print(f"  → Note: This may take time due to API rate limiting")
        
        # Initialize writers
        temp_writer = TempSnapshotWriter(temp_snapshots_path=self._temp_snapshots_path)
        pv_writer = PriceVolumeWriter(
            pricevolume_path=self._pricevolume_path,
            temp_staging_path=self._temp_staging_path,
            temp_backup_path=self._temp_backup_path,
        )
        
        total_rows = 0
        date_count = 0
        
        # Ingest each date
        for date_str in dates:
            try:
                # Stage 0: Fetch raw data from API
                raw_rows = self._raw_client.call(
                    "stock.all_change_rates",
                    host_id="krx",
                    params={
                        "strtDd": date_str,
                        "endDd": date_str,
                        "mktId": "ALL",
                        "adjStkPrc": 2,  # adjusted prices
                    },
                )
                
                if not raw_rows:
                    continue  # Skip holidays/weekends
                
                # Preprocess rows (add TRD_DD, clean data)
                preprocessed_rows = preprocess_change_rates_rows(raw_rows, trade_date=date_str)
                
                # Stage 0: Write to temp/snapshots/
                temp_writer.write(preprocessed_rows, date=date_str)
                
                # Stage 1: Atomically move to pricevolume/ (raw fields only, NO placeholders)
                count = pv_writer.write_initial(
                    preprocessed_rows, 
                    date=date_str,
                    temp_snapshots_path=self._temp_snapshots_path
                )
                
                if count > 0:
                    total_rows += count
                    date_count += 1
                    print(f"    [{date_str}] {count} rows → pricevolume/")
            
            except Exception as e:
                print(f"    [{date_str}] Skipped (API error or holiday)")
                continue
        
        print(f"  [OK] Ingested {total_rows} rows across {date_count} dates")
    
    def _enrich_with_adjustments(self, start_date: str, end_date: str) -> None:
        """
        Stage 2: Enrich pricevolume partitions with adjustment factors.
        
        For each date: Read pricevolume → Calculate adj_factor → Rewrite partition
        """
        from ..storage.enrichers import AdjustmentEnricher
        from ..storage.writers import PriceVolumeWriter
        
        print(f"[Stage 2] Enriching with adjustment factors...")
        
        # Initialize writer and enricher
        pv_writer = PriceVolumeWriter(
            pricevolume_path=self._pricevolume_path,
            temp_staging_path=self._temp_staging_path,
            temp_backup_path=self._temp_backup_path,
        )
        enricher = AdjustmentEnricher(
            pricevolume_path=self._pricevolume_path,
            writer=pv_writer
        )
        
        # Generate date range
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        total_enriched = 0
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y%m%d')
            partition_path = self._pricevolume_path / f"TRD_DD={date_str}"
            
            if partition_path.exists():
                try:
                    count = enricher.enrich_partition(date=date_str)
                    total_enriched += count
                    print(f"    [{date_str}] Enriched {count} rows with adj_factor")
                except Exception as e:
                    print(f"    [{date_str}] Warning: Failed to enrich ({e})")
            
            current_dt += timedelta(days=1)
        
        print(f"  [OK] Enriched {total_enriched} total rows with adjustment factors")
        
        # Build cumulative adjustments cache (needed for adjusted price queries)
        self._build_cumulative_adjustments_cache(start_date, end_date)
    
    def _build_cumulative_adjustments_cache(self, start_date: str, end_date: str) -> None:
        """
        Build cumulative adjustments cache from pricevolume adj_factor column.
        
        This is needed for DataLoader._apply_adjustments() to work.
        Reads adj_factor from pricevolume, computes cumulative multipliers, 
        and writes to temp/cumulative_adjustments/.
        """
        from ..storage.query import query_parquet_table
        from ..transforms.adjustment import compute_cumulative_adjustments
        from ..storage.writers import ParquetSnapshotWriter
        
        print(f"  → Building cumulative adjustments cache...")
        
        try:
            # Query adj_factor from pricevolume
            adj_df = query_parquet_table(
                self._pricevolume_path.parent,
                'pricevolume',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
            )
            
            if adj_df.empty or adj_df['adj_factor'].isna().all():
                print(f"    → No adjustment factors found, skipping cache build")
                return
            
            # Convert to dict format expected by compute_cumulative_adjustments
            # It expects: [{'TRD_DD': ..., 'ISU_SRT_CD': ..., 'adj_factor': ...}, ...]
            # IMPORTANT: Include ALL rows (even NaN adj_factors) so first date gets cumulative multiplier
            adj_rows = []
            for _, row in adj_df.iterrows():
                adj_rows.append({
                    'TRD_DD': row['TRD_DD'],
                    'ISU_SRT_CD': row['ISU_SRT_CD'],
                    'adj_factor': float(row['adj_factor']) if pd.notna(row['adj_factor']) else None
                })
            
            if not adj_rows:
                print(f"    → No valid adjustment factors, skipping cache build")
                return
            
            # Compute cumulative adjustments
            cum_adj_rows = compute_cumulative_adjustments(adj_rows)
            
            # Write to temp cache using NEW date= partitioning
            import pyarrow as pa
            import pyarrow.parquet as pq
            from ..storage.schema import CUMULATIVE_ADJUSTMENTS_SCHEMA
            
            temp_cum_adj_path = self._pricevolume_path.parent / 'temp' / 'cumulative_adjustments'
            temp_cum_adj_path.mkdir(parents=True, exist_ok=True)
            
            # Group by date and write each partition
            dates_written = set()
            for date in set(r['TRD_DD'] for r in cum_adj_rows):
                date_rows = [r for r in cum_adj_rows if r['TRD_DD'] == date]
                
                # Convert to format expected by schema (ISU_SRT_CD, cum_adj_multiplier)
                schema_rows = [{
                    'ISU_SRT_CD': r['ISU_SRT_CD'],
                    'cum_adj_multiplier': float(r['cum_adj_multiplier'])
                } for r in date_rows]
                
                # Sort by ISU_SRT_CD for row-group pruning
                schema_rows.sort(key=lambda r: r['ISU_SRT_CD'])
                
                # Write partition with TRD_DD= naming (standard)
                partition_path = temp_cum_adj_path / f"TRD_DD={date}"
                partition_path.mkdir(parents=True, exist_ok=True)
                
                table = pa.Table.from_pylist(schema_rows, schema=CUMULATIVE_ADJUSTMENTS_SCHEMA)
                pq.write_table(
                    table,
                    partition_path / "data.parquet",
                    row_group_size=1000,
                    compression='zstd',
                    compression_level=3,
                )
                dates_written.add(date)
            
            print(f"    → Cached {len(cum_adj_rows)} cumulative adjustments across {len(dates_written)} dates")
        
        except Exception as e:
            print(f"    → Warning: Failed to build cumulative adjustments cache: {e}")
    
    def _enrich_with_liquidity_ranks(self, start_date: str, end_date: str) -> None:
        """
        Stage 3: Enrich pricevolume partitions with liquidity ranks.
        
        For each date: Read pricevolume → Calculate liquidity_rank → Rewrite partition
        """
        from ..storage.enrichers import LiquidityRankEnricher
        from ..storage.writers import PriceVolumeWriter
        
        print(f"[Stage 3] Enriching with liquidity ranks...")
        
        # Initialize writer and enricher
        pv_writer = PriceVolumeWriter(
            pricevolume_path=self._pricevolume_path,
            temp_staging_path=self._temp_staging_path,
            temp_backup_path=self._temp_backup_path,
        )
        enricher = LiquidityRankEnricher(
            pricevolume_path=self._pricevolume_path,
            writer=pv_writer
        )
        
        # Generate date range
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        total_enriched = 0
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y%m%d')
            partition_path = self._pricevolume_path / f"TRD_DD={date_str}"
            
            if partition_path.exists():
                try:
                    count = enricher.enrich_partition(date=date_str)
                    total_enriched += count
                    print(f"    [{date_str}] Enriched {count} rows with liquidity_rank")
                except Exception as e:
                    print(f"    [{date_str}] Warning: Failed to enrich ({e})")
            
            current_dt += timedelta(days=1)
        
        print(f"  [OK] Enriched {total_enriched} total rows with liquidity ranks")
    
    def _build_universe_tables(self, start_date: str, end_date: str) -> None:
        """
        Stage 4: Build pre-computed universe tables from enriched pricevolume.
        
        Reads liquidity_rank from pricevolume and builds universe membership tables.
        """
        from ..storage.query import query_parquet_table
        
        print(f"[Stage 4] Building universe tables...")
        
        # Check if universes exist
        universes_path = self._pricevolume_path.parent / 'universes'
        if not universes_path.exists():
            print(f"  → Universe tables missing, building...")
            self._build_universes(start_date, end_date)
        else:
            print(f"  → Universe tables exist")
        
        print(f"  [OK] Universe tables ready")
    
    def _build_universes(self, start_date: str, end_date: str) -> None:
        """Build universe tables from enriched pricevolume liquidity ranks."""
        from ..pipelines.universe_builder import build_universes_and_persist
        from ..storage.query import query_parquet_table
        from ..storage.writers import ParquetSnapshotWriter
        
        try:
            # Query liquidity ranks from pricevolume (now has liquidity_rank column)
            ranks_df = query_parquet_table(
                self._pricevolume_path.parent,
                'pricevolume',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', 'liquidity_rank']
            )
            
            print(f"    → Queried {len(ranks_df)} rows from pricevolume")
            
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
            writer = ParquetSnapshotWriter(root_path=self._pricevolume_path.parent)
            count = build_universes_and_persist(ranks_df, universe_tiers, writer)
            
            print(f"    → Built {count} universe membership rows")
        
        except Exception as e:
            print(f"    → Warning: Failed to build universes: {e}")

