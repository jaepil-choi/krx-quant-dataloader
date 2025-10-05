"""
Storage enrichers

What this module does:
- Provides enrichment classes for progressive column enrichment in PriceVolume table.
- Stage 2: AdjustmentEnricher adds adj_factor column
- Stage 3: LiquidityRankEnricher adds liquidity_rank column

Pattern:
- Read partition from persistent DB
- Calculate enriched column(s)
- Atomic rewrite via PriceVolumeWriter

Interaction:
- Used by pipeline orchestrator to enrich partitions sequentially
- Delegates actual calculation to transforms/ modules
- Uses PriceVolumeWriter for atomic writes
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

from .writers import PriceVolumeWriter


class AdjustmentEnricher:
    """
    Stage 2 enricher: Calculate and add adj_factor column.
    
    Reads raw data from pricevolume/, calculates adjustment factors
    per-symbol using LAG semantics, and atomically rewrites with
    adj_factor column filled.
    """
    
    def __init__(
        self,
        *,
        pricevolume_path: Path | str,
        writer: PriceVolumeWriter,
    ):
        """
        Initialize adjustment enricher.
        
        Parameters
        ----------
        pricevolume_path : Path | str
            Path to persistent pricevolume DB (e.g., data/pricevolume/)
        writer : PriceVolumeWriter
            Writer instance for atomic rewrites
        """
        self.pricevolume_path = Path(pricevolume_path)
        self.writer = writer
    
    def enrich_partition(self, date: str) -> int:
        """
        Enrich a single partition with adj_factor column.
        
        Algorithm:
        1. Read partition from pricevolume/date=X/
        2. Calculate adj_factor per-symbol: BAS_PRC_t / TDD_CLSPRC_{t-1}
        3. Atomic rewrite with adj_factor filled (liquidity_rank still None)
        
        Parameters
        ----------
        date : str
            Trade date in YYYYMMDD format
        
        Returns
        -------
        int
            Number of rows enriched
        """
        # Read existing partition
        partition_path = self.pricevolume_path / f"TRD_DD={date}" / "data.parquet"
        if not partition_path.exists():
            raise FileNotFoundError(f"Partition not found: {partition_path}")
        
        df = pd.read_parquet(partition_path)
        
        # Calculate adj_factor using transform module
        # This requires looking at previous trading day's close price
        # For now, we'll implement a simplified version that requires
        # the full date range to be available
        from ..transforms.adjustment import compute_adj_factors_for_partition
        
        # Calculate adj_factor (will be None for first trading day per symbol)
        df_enriched = compute_adj_factors_for_partition(
            df, 
            pricevolume_path=self.pricevolume_path,
            current_date=date
        )
        
        # Convert to list of dicts for writer
        rows = df_enriched.to_dict('records')
        
        # Atomic rewrite
        count = self.writer.rewrite_enriched(rows, date=date)
        
        return count
    
    def enrich_date_range(
        self, 
        start_date: str, 
        end_date: str,
        *,
        trading_dates: List[str],
    ) -> Dict[str, int]:
        """
        Enrich multiple partitions in chronological order.
        
        This is important because adj_factor calculation requires
        looking at previous trading day's data.
        
        Parameters
        ----------
        start_date : str
            Start date in YYYYMMDD format
        end_date : str
            End date in YYYYMMDD format
        trading_dates : List[str]
            Sorted list of trading dates to process
        
        Returns
        -------
        Dict[str, int]
            Mapping of date → row count enriched
        """
        results = {}
        
        for date in trading_dates:
            if start_date <= date <= end_date:
                try:
                    count = self.enrich_partition(date)
                    results[date] = count
                except Exception as e:
                    print(f"Warning: Failed to enrich {date}: {e}")
                    results[date] = 0
        
        return results


class LiquidityRankEnricher:
    """
    Stage 3 enricher: Calculate and add liquidity_rank column.
    
    Reads enriched data (with adj_factor) from pricevolume/,
    calculates cross-sectional liquidity ranking per date,
    and atomically rewrites with liquidity_rank column filled.
    """
    
    def __init__(
        self,
        *,
        pricevolume_path: Path | str,
        writer: PriceVolumeWriter,
    ):
        """
        Initialize liquidity rank enricher.
        
        Parameters
        ----------
        pricevolume_path : Path | str
            Path to persistent pricevolume DB (e.g., data/pricevolume/)
        writer : PriceVolumeWriter
            Writer instance for atomic rewrites
        """
        self.pricevolume_path = Path(pricevolume_path)
        self.writer = writer
    
    def enrich_partition(self, date: str) -> int:
        """
        Enrich a single partition with liquidity_rank column.
        
        Algorithm:
        1. Read partition from pricevolume/date=X/
        2. Rank by ACC_TRDVAL descending (dense ranking)
        3. Atomic rewrite with liquidity_rank filled
        
        Parameters
        ----------
        date : str
            Trade date in YYYYMMDD format
        
        Returns
        -------
        int
            Number of rows enriched
        """
        # Read existing partition (should already have adj_factor filled)
        partition_path = self.pricevolume_path / f"TRD_DD={date}" / "data.parquet"
        if not partition_path.exists():
            raise FileNotFoundError(f"Partition not found: {partition_path}")
        
        df = pd.read_parquet(partition_path)
        
        # Calculate cross-sectional liquidity rank
        # Rank by ACC_TRDVAL descending: higher value = lower rank number
        # Rank 1 = most liquid stock on that date
        df['liquidity_rank'] = df['ACC_TRDVAL'].rank(
            method='dense',      # Dense ranking (no gaps: 1, 2, 3, ...)
            ascending=False      # Higher ACC_TRDVAL → lower rank number
        ).astype('Int32')        # Use nullable integer type
        
        # Convert to list of dicts for writer
        rows = df.to_dict('records')
        
        # Atomic rewrite (final version with both adj_factor and liquidity_rank)
        count = self.writer.rewrite_enriched(rows, date=date)
        
        return count
    
    def enrich_date_range(
        self, 
        start_date: str, 
        end_date: str,
        *,
        trading_dates: List[str],
    ) -> Dict[str, int]:
        """
        Enrich multiple partitions with liquidity ranks.
        
        Each partition is independent (cross-sectional ranking per date).
        
        Parameters
        ----------
        start_date : str
            Start date in YYYYMMDD format
        end_date : str
            End date in YYYYMMDD format
        trading_dates : List[str]
            Sorted list of trading dates to process
        
        Returns
        -------
        Dict[str, int]
            Mapping of date → row count enriched
        """
        results = {}
        
        for date in trading_dates:
            if start_date <= date <= end_date:
                try:
                    count = self.enrich_partition(date)
                    results[date] = count
                except Exception as e:
                    print(f"Warning: Failed to enrich {date}: {e}")
                    results[date] = 0
        
        return results


__all__ = [
    "AdjustmentEnricher",
    "LiquidityRankEnricher",
]

