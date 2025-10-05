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
        db_path: Optional[str | Path] = None,
        *,
        start_date: str,
        end_date: str,
        temp_path: Optional[str | Path] = None,
        settings_path: Optional[str | Path] = None,
        raw_client=None,
    ):
        """
        Initialize DataLoader and run 3-stage pipeline.
        
        Uses defaults from config/settings.yaml unless overridden.
        
        Parameters
        ----------
        db_path : Optional[str | Path]
            Path to persistent Parquet database.
            If None, uses default from config/settings.yaml (data/krx_db).
        start_date : str
            Start of query window (YYYYMMDD format).
        end_date : str
            End of query window (YYYYMMDD format).
        temp_path : Optional[str | Path]
            Path for ephemeral cache.
            If None, uses default from config/settings.yaml (data/temp).
        settings_path : Optional[str | Path]
            Path to settings.yaml for configuration.
            If None, uses config/settings.yaml in project root.
        raw_client : Optional
            Pre-built RawClient instance.
            If None, will create lazily if needed for auto-ingestion.
        
        Examples
        --------
        >>> # Simple usage (all defaults from settings.yaml):
        >>> loader = DataLoader(start_date='20240101', end_date='20241231')
        
        >>> # Custom paths:
        >>> loader = DataLoader(
        ...     db_path='/custom/db',
        ...     start_date='20240101',
        ...     end_date='20241231',
        ...     settings_path='my_config/settings.yaml'
        ... )
        """
        # Validate date range
        if start_date > end_date:
            raise ValueError(f"start_date must be <= end_date, got {start_date} > {end_date}")
        
        # Load configuration (used for defaults and paths)
        from ..config import ConfigFacade
        self._config = ConfigFacade.load(settings_path=settings_path)
        
        # Set paths (use provided OR defaults from config)
        self._db_path = Path(db_path) if db_path else self._config.default_db_path
        self._temp_path = Path(temp_path) if temp_path else self._config.default_temp_path
        self._start_date = start_date
        self._end_date = end_date
        
        # Store raw client (lazy initialization if needed)
        self._raw_client = raw_client
        
        # Ensure directories exist
        self._db_path.mkdir(parents=True, exist_ok=True)
        self._temp_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize FieldMapper (using path from config)
        self._field_mapper = FieldMapper.from_yaml(self._config.fields_yaml_path)
        
        # Run 3-stage pipeline via orchestrator
        print(f"[DataLoader] Initializing for window: {self._start_date} → {self._end_date}")
        orchestrator = self._create_orchestrator()
        orchestrator.ensure_data_ready(start_date, end_date)
        print(f"[DataLoader] Ready for queries")
    
    def _create_orchestrator(self):
        """
        Create PipelineOrchestrator for 3-stage pipeline.
        
        Lazy-initializes raw_client if needed for ingestion.
        """
        from ..pipelines.orchestrator import PipelineOrchestrator
        
        # Lazy initialize raw client if needed (will be None if DB pre-built)
        if self._raw_client is None:
            # Check if DB exists - if not, we'll need a client
            snapshots_path = self._db_path / 'snapshots'
            if not snapshots_path.exists():
                print(f"  [INFO] Initializing RawClient for auto-ingestion...")
                from ..factory import create_raw_client
                
                # Factory will use ConfigFacade to find endpoints.yaml
                self._raw_client = create_raw_client()
                print(f"  [OK] RawClient ready")
        
        return PipelineOrchestrator(
            db_path=self._db_path,
            temp_path=self._temp_path,
            raw_client=self._raw_client,
        )
    
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
