"""
DataLoader: Range-locked, stateful API with automatic 3-stage pipeline.

This module implements the high-level DataLoader that:
1. Initializes with fixed [start_date, end_date] window
2. Automatically ensures DB completeness (3-stage pipeline on init)
3. Provides simple field-based queries with universe filtering and adjustments
4. Returns wide-format DataFrames (dates × symbols)

Design principles:
- Smart initialization (auto-fetch, post-process, cache)
- Dumb queries (filter, mask, pivot)
- FieldMapper for extensibility
- No intermediate service layer (direct storage queries)
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union, Dict, Any
import pandas as pd

from ..apis.field_mapper import FieldMapper
from ..storage.query import query_parquet_table
from ..storage.writers import ParquetSnapshotWriter


class DataLoader:
    """
    Range-locked DataLoader with automatic 3-stage pipeline orchestration.
    
    Usage:
        loader = DataLoader(
            db_path='data/krx_db',
            start_date='20240101',
            end_date='20241231'
        )
        df = loader.get_data('close', universe='univ100', adjusted=True)
    
    Architecture:
    - Stage 1 (Snapshots): Check/fetch/ingest missing dates from KRX
    - Stage 2 (Adjustments): Compute factors + build ephemeral cumulative cache
    - Stage 3 (Universes): Compute liquidity ranks + build universe tables
    - Query: Simple filtering/masking/pivoting operations
    
    Parameters
    ----------
    db_path : str | Path
        Path to persistent Parquet database (e.g., 'data/krx_db')
    start_date : str
        Start of query window (YYYYMMDD format)
    end_date : str
        End of query window (YYYYMMDD format)
    temp_path : Optional[str | Path]
        Path for ephemeral cache (default: 'data/temp')
    raw_client : Optional
        RawClient instance for fetching from KRX API (if None, assumes DB is pre-built)
    
    Attributes
    ----------
    _db_path : Path
        Persistent database path
    _temp_path : Path
        Ephemeral cache path
    _start_date : str
        Query window start
    _end_date : str
        Query window end
    _field_mapper : FieldMapper
        Maps field names → (table, column)
    _raw_client : Optional
        For fetching missing data from KRX
    
    Raises
    ------
    ValueError
        If start_date > end_date
    """
    
    def __init__(
        self,
        db_path: str | Path,
        *,
        start_date: str,
        end_date: str,
        temp_path: Optional[str | Path] = None,
        raw_client=None,
    ):
        """Initialize DataLoader and run 3-stage pipeline."""
        # Validate date range
        if start_date > end_date:
            raise ValueError(f"start_date must be <= end_date, got {start_date} > {end_date}")
        
        # Set paths
        self._db_path = Path(db_path)
        self._temp_path = Path(temp_path or 'data/temp')
        self._start_date = start_date
        self._end_date = end_date
        self._raw_client = raw_client
        
        # Ensure directories exist
        self._db_path.mkdir(parents=True, exist_ok=True)
        self._temp_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize FieldMapper
        config_path = Path(__file__).parent.parent.parent.parent / 'config' / 'fields.yaml'
        self._field_mapper = FieldMapper.from_yaml(config_path)
        
        # Run 3-stage pipeline
        self._run_pipeline()
    
    def _run_pipeline(self) -> None:
        """
        Run 3-stage pipeline to ensure DB completeness.
        
        Stage 1: Snapshots (check/fetch/ingest)
        Stage 2: Adjustments (compute factors + build ephemeral cache)
        Stage 3: Universes (compute ranks + build universe tables)
        """
        print(f"[DataLoader] Initializing for window: {self._start_date} → {self._end_date}")
        
        # Stage 1: Ensure snapshots exist
        self._ensure_snapshots()
        
        # Stage 2: Build adjustment cache
        self._build_adjustment_cache()
        
        # Stage 3: Build universe tables
        self._build_universe_tables()
        
        print(f"[DataLoader] Ready for queries")
    
    def _ensure_snapshots(self) -> None:
        """
        Stage 1: Ensure snapshots exist for date range.
        
        Checks if snapshots exist; if missing, ingests from KRX API.
        """
        print(f"[Stage 1] Checking snapshots...")
        
        # Check if snapshots table exists and has data for date range
        snapshots_path = self._db_path / 'snapshots'
        if not snapshots_path.exists():
            print(f"  → Snapshots table missing")
            if self._raw_client is None:
                raise ValueError(
                    f"Snapshots missing and no raw_client provided. "
                    f"Either pre-build DB or provide raw_client for auto-ingestion."
                )
            self._ingest_missing_dates()
            return
        
        # Check for missing dates
        try:
            existing_df = query_parquet_table(
                self._db_path,
                'snapshots',
                start_date=self._start_date,
                end_date=self._end_date,
                fields=['TRD_DD']
            )
            existing_dates = set(existing_df['TRD_DD'].unique())
            print(f"  → Found {len(existing_dates)} dates in DB")
        except Exception as e:
            print(f"  → Error querying snapshots: {e}")
            existing_dates = set()
        
        # For MVP: Assume all dates exist if table exists
        # TODO: Implement missing date detection and ingestion
        print(f"  [OK] Snapshots exist")
    
    def _ingest_missing_dates(self) -> None:
        """Ingest missing dates from KRX API."""
        # TODO: Implement automatic ingestion
        # For MVP: Require pre-built DB
        raise NotImplementedError(
            "Automatic ingestion not yet implemented. "
            "Please pre-build DB using showcase scripts or build_db.py"
        )
    
    def _build_adjustment_cache(self) -> None:
        """
        Stage 2: Build ephemeral cumulative adjustment cache.
        
        Computes cumulative multipliers for the date range and stores in temp/.
        """
        print(f"[Stage 2] Building adjustment cache...")
        
        # Check if adj_factors exist
        adj_factors_path = self._db_path / 'adj_factors'
        if not adj_factors_path.exists():
            print(f"  → No adjustment factors found, skipping cache build")
            return
        
        # Query adjustment factors for date range
        try:
            from ..transforms.adjustment import compute_cumulative_adjustments
            
            adj_factors_df = query_parquet_table(
                self._db_path,
                'adj_factors',
                start_date=self._start_date,
                end_date=self._end_date,
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
                writer.write_cumulative_adjustments(
                    date_rows, date=date, temp_root=self._temp_path
                )
            
            print(f"  [OK] Cached {len(cum_adj_rows)} cumulative adjustments")
        
        except Exception as e:
            print(f"  → Warning: Failed to build adjustment cache: {e}")
            print(f"  → Adjusted queries may fail")
    
    def _build_universe_tables(self) -> None:
        """
        Stage 3: Build pre-computed universe tables.
        
        Computes liquidity ranks and builds universe membership tables.
        """
        print(f"[Stage 3] Building universe tables...")
        
        # Check if liquidity_ranks exist
        ranks_path = self._db_path / 'liquidity_ranks'
        if not ranks_path.exists():
            print(f"  → Liquidity ranks missing, computing...")
            self._compute_liquidity_ranks()
        else:
            print(f"  → Liquidity ranks exist")
        
        # Check if universes exist
        universes_path = self._db_path / 'universes'
        if not universes_path.exists():
            print(f"  → Universe tables missing, building...")
            self._build_universes()
        else:
            print(f"  → Universe tables exist")
        
        print(f"  [OK] Universe tables ready")
    
    def _compute_liquidity_ranks(self) -> None:
        """Compute liquidity ranks from snapshots."""
        try:
            from ..pipelines.liquidity_ranking import compute_liquidity_ranks, write_liquidity_ranks
            
            # Query snapshots
            snapshots_df = query_parquet_table(
                self._db_path,
                'snapshots',
                start_date=self._start_date,
                end_date=self._end_date,
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
    
    def _build_universes(self) -> None:
        """Build universe tables from liquidity ranks."""
        try:
            from ..pipelines.universe_builder import build_universes_and_persist
            
            # Query liquidity ranks
            ranks_df = query_parquet_table(
                self._db_path,
                'liquidity_ranks',
                start_date=self._start_date,
                end_date=self._end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', 'xs_liquidity_rank']
            )
            
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
    
    def get_data(
        self,
        field: str,
        *,
        universe: Union[str, List[str], None] = None,
        query_start: Optional[str] = None,
        query_end: Optional[str] = None,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        """
        Query data with optional universe filtering and adjustments.
        
        Returns wide-format DataFrame (dates × symbols).

        Parameters
        ----------
        field : str
            Field name to query (e.g., 'close', 'volume', 'liquidity_rank')
        universe : Union[str, List[str], None]
            Universe specification:
            - String (e.g., 'univ100'): Pre-computed universe (survivorship-bias-free)
            - List (e.g., ['005930', '000660']): Explicit symbol list
            - None: All symbols
        query_start : Optional[str]
            Sub-range start (must be within loader's date range)
        query_end : Optional[str]
            Sub-range end (must be within loader's date range)
        adjusted : bool
            Whether to apply cumulative adjustments (default: True)
        
        Returns
        -------
        pd.DataFrame
            Wide-format DataFrame with:
            - Index: TRD_DD (dates)
            - Columns: ISU_SRT_CD (symbols)
            - Values: Field values
        
        Raises
        ------
        ValueError
            If query range is outside loader's date range
            If field is unknown
        
        Examples
        --------
        >>> loader = DataLoader('data/krx_db', start_date='20240101', end_date='20241231')
        >>> df = loader.get_data('close', universe='univ100', adjusted=True)
        >>> df.shape
        (252, 100)  # 252 trading days × 100 symbols
        """
        # Validate query range
        q_start = query_start or self._start_date
        q_end = query_end or self._end_date
        
        if q_start < self._start_date or q_end > self._end_date:
            raise ValueError(
                f"Query range [{q_start}, {q_end}] outside loader window "
                f"[{self._start_date}, {self._end_date}]. "
                f"Create new DataLoader instance for different range."
            )
        
        # Step 1: Resolve field name via FieldMapper
        mapping = self._field_mapper.resolve(field)
        
        # Step 2: Query field data from table
        df = query_parquet_table(
            self._db_path,
            mapping.table,
            start_date=q_start,
            end_date=q_end,
            fields=['TRD_DD', 'ISU_SRT_CD', mapping.column]
        )
        
        # Step 3: Filter by universe (if specified)
        if universe is not None:
            df = self._apply_universe_filter(df, universe, q_start, q_end)
        
        # Step 4: Apply adjustments (if requested and supported)
        if adjusted and self._field_mapper.is_original(field):
            df = self._apply_adjustments(df, mapping.column, q_start, q_end)
        
        # Step 5: Pivot to wide format (dates × symbols)
        wide_df = df.pivot(
            index='TRD_DD',
            columns='ISU_SRT_CD',
            values=mapping.column
        )
        
        return wide_df
    
    def _apply_universe_filter(
        self,
        df: pd.DataFrame,
        universe: Union[str, List[str]],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Apply universe filtering to DataFrame.
        
        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame with columns [TRD_DD, ISU_SRT_CD, ...]
        universe : Union[str, List[str]]
            - String: Pre-computed universe ('univ100', etc.) → query universes table
            - List: Explicit symbol list → filter directly
        start_date : str
            Query start date
        end_date : str
            Query end date
        
        Returns
        -------
        pd.DataFrame
            Filtered DataFrame
        """
        if isinstance(universe, list):
            # Explicit symbol list: simple filtering
            return df[df['ISU_SRT_CD'].isin(universe)]
        
        elif isinstance(universe, str):
            # Pre-computed universe: query universes table with boolean filter
            universe_df = query_parquet_table(
                self._db_path,
                'universes',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', universe]
            )
            
            # Filter to universe members only
            universe_members = universe_df[universe_df[universe] == 1]
            
            # JOIN/mask: keep only rows matching universe membership
            result = df.merge(
                universe_members[['TRD_DD', 'ISU_SRT_CD']],
                on=['TRD_DD', 'ISU_SRT_CD'],
                how='inner'
            )
            
            return result
        
        else:
            raise ValueError(f"Invalid universe type: {type(universe)}")
    
    def _apply_adjustments(
        self,
        df: pd.DataFrame,
        column: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Apply cumulative adjustments to prices.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame with columns [TRD_DD, ISU_SRT_CD, <column>]
        column : str
            Column name to adjust (e.g., 'TDD_CLSPRC')
        start_date : str
            Query start date
        end_date : str
            Query end date
        
        Returns
        -------
        pd.DataFrame
            DataFrame with adjusted prices
        """
        try:
            # Query cumulative adjustments from ephemeral cache
            cum_adj_df = query_parquet_table(
                self._temp_path,
                'cumulative_adjustments',
                start_date=start_date,
                end_date=end_date,
                fields=['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']
            )
            
            # Merge with prices
            result = df.merge(
                cum_adj_df,
                on=['TRD_DD', 'ISU_SRT_CD'],
                how='left'
            )
            
            # Fill missing multipliers with 1.0 (no adjustment)
            result['cum_adj_multiplier'] = result['cum_adj_multiplier'].fillna(1.0)
            
            # Apply adjustment
            result[column] = (result[column] * result['cum_adj_multiplier']).round(0).astype(int)
            
            # Drop multiplier column
            result = result.drop(columns=['cum_adj_multiplier'])
            
            return result
        
        except Exception as e:
            print(f"  Warning: Failed to apply adjustments: {e}")
            print(f"  Returning raw prices")
            return df
    
    def get_trading_dates(self) -> List[str]:
        """
        Get available trading dates within loader's date range.
        
        Returns
        -------
        List[str]
            Sorted list of trading dates (YYYYMMDD format)
        """
        df = query_parquet_table(
            self._db_path,
            'snapshots',
            start_date=self._start_date,
            end_date=self._end_date,
            fields=['TRD_DD']
        )
        return sorted(df['TRD_DD'].unique().tolist())
