"""
Unit tests for universe builder pipeline

Tests the build_universes() function that constructs universe membership
from liquidity ranks. Uses synthetic data.
"""

import pytest
import pandas as pd
from pathlib import Path


@pytest.mark.unit
class TestBuildUniversesLogic:
    """Test universe construction logic with synthetic data."""
    
    def test_single_date_single_universe(self):
        """Test building a single universe for one date."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        # Synthetic liquidity ranks
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK04', 'xs_liquidity_rank': 4, 'ACC_TRDVAL': 700000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK05', 'xs_liquidity_rank': 5, 'ACC_TRDVAL': 600000},
        ])
        
        # Build univ3 (top 3 stocks)
        universe_tiers = {'univ3': 3}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Should return 3 rows (top 3 stocks for univ3)
        assert len(result) == 3
        assert set(result['ISU_SRT_CD']) == {'STOCK01', 'STOCK02', 'STOCK03'}
        assert (result['universe_name'] == 'univ3').all()
        assert result['xs_liquidity_rank'].max() == 3
    
    def test_multiple_universes_subset_relationship(self):
        """Test that smaller universes are subsets of larger ones."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Create 10 stocks
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': f'STOCK{i:02d}', 'xs_liquidity_rank': i, 'ACC_TRDVAL': 1000000 - i*10000}
            for i in range(1, 11)
        ])
        
        # Build multiple tiers
        universe_tiers = {'univ3': 3, 'univ5': 5, 'univ10': 10}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Check subset relationships
        univ3 = set(result[result['universe_name'] == 'univ3']['ISU_SRT_CD'])
        univ5 = set(result[result['universe_name'] == 'univ5']['ISU_SRT_CD'])
        univ10 = set(result[result['universe_name'] == 'univ10']['ISU_SRT_CD'])
        
        assert univ3.issubset(univ5), "univ3 should be subset of univ5"
        assert univ5.issubset(univ10), "univ5 should be subset of univ10"
        assert len(univ3) == 3
        assert len(univ5) == 5
        assert len(univ10) == 10
    
    def test_multiple_dates_cross_sectional_independence(self):
        """Test universe construction preserves per-date independence."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Same stocks, different ranks per date
        ranks_df = pd.DataFrame([
            # Date 1: STOCK01 is #1
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
            # Date 2: STOCK03 is #1
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
        ])
        
        universe_tiers = {'univ2': 2}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Check per-date membership
        date1_stocks = set(result[result['TRD_DD'] == '20240101']['ISU_SRT_CD'])
        date2_stocks = set(result[result['TRD_DD'] == '20240102']['ISU_SRT_CD'])
        
        assert date1_stocks == {'STOCK01', 'STOCK02'}
        assert date2_stocks == {'STOCK03', 'STOCK01'}
        assert date1_stocks != date2_stocks, "Universe membership should vary per date"
    
    def test_stocks_appear_in_multiple_universes(self):
        """Test that top-ranked stocks appear in all appropriate universes."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
        ])
        
        universe_tiers = {'univ1': 1, 'univ2': 2, 'univ3': 3}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # STOCK01 (rank 1) should appear in all 3 universes
        stock01_universes = result[result['ISU_SRT_CD'] == 'STOCK01']['universe_name'].tolist()
        assert set(stock01_universes) == {'univ1', 'univ2', 'univ3'}
        
        # STOCK03 (rank 3) should only appear in univ3
        stock03_universes = result[result['ISU_SRT_CD'] == 'STOCK03']['universe_name'].tolist()
        assert set(stock03_universes) == {'univ3'}
    
    def test_empty_ranks_returns_empty_result(self):
        """Test that empty ranks DataFrame returns empty result."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame(columns=['TRD_DD', 'ISU_SRT_CD', 'xs_liquidity_rank', 'ACC_TRDVAL'])
        universe_tiers = {'univ100': 100}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        assert len(result) == 0
    
    def test_fewer_stocks_than_threshold(self):
        """Test universe construction when fewer stocks than threshold exist."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Only 3 stocks, but requesting univ100
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Should include all 3 stocks (even though threshold is 100)
        assert len(result) == 3
        assert set(result['ISU_SRT_CD']) == {'STOCK01', 'STOCK02', 'STOCK03'}


@pytest.mark.unit
class TestBuildUniversesOutputFormat:
    """Test output DataFrame format and schema."""
    
    def test_output_has_required_columns(self):
        """Test output DataFrame contains required columns."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ1': 1}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Required columns for UNIVERSES_SCHEMA
        assert 'TRD_DD' in result.columns
        assert 'ISU_SRT_CD' in result.columns
        assert 'universe_name' in result.columns
        assert 'xs_liquidity_rank' in result.columns
    
    def test_output_rank_dtype(self):
        """Test xs_liquidity_rank is integer type."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ1': 1}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        assert result['xs_liquidity_rank'].dtype in ['int32', 'int64']
    
    def test_output_sorted_by_date_and_symbol(self):
        """Test output is sorted by date and symbol for efficient storage."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
        ])
        
        universe_tiers = {'univ2': 2}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Check sorting
        dates = result['TRD_DD'].tolist()
        assert dates == sorted(dates), "Should be sorted by date"
        
        # Within each date, should be sorted by symbol
        for date in result['TRD_DD'].unique():
            date_data = result[result['TRD_DD'] == date]
            symbols = date_data['ISU_SRT_CD'].tolist()
            assert symbols == sorted(symbols), f"Symbols within {date} should be sorted"


@pytest.mark.unit
class TestBuildUniversesWithPersistence:
    """Test build_universes_and_persist() function with writer."""
    
    def test_writes_to_database(self, tmp_path):
        """Test that build_universes_and_persist() writes to database."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes_and_persist
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ2': 2}
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        count = build_universes_and_persist(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
            writer=writer,
        )
        
        # Should write 3 rows (2 stocks × 1 universe + 1 stock for date2)
        assert count == 3
        
        # Check files were created
        assert (tmp_path / 'universes' / 'TRD_DD=20240101').exists()
        assert (tmp_path / 'universes' / 'TRD_DD=20240102').exists()
    
    def test_per_date_partitioning(self, tmp_path):
        """Test that data is partitioned by date."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes_and_persist
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        from krx_quant_dataloader.storage.query import query_parquet_table
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ1': 1}
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        build_universes_and_persist(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
            writer=writer,
        )
        
        # Query back each date separately
        date1 = query_parquet_table(tmp_path, 'universes', start_date='20240101', end_date='20240101')
        date2 = query_parquet_table(tmp_path, 'universes', start_date='20240102', end_date='20240102')
        date3 = query_parquet_table(tmp_path, 'universes', start_date='20240103', end_date='20240103')
        
        assert len(date1) == 1
        assert len(date2) == 1
        assert len(date3) == 1
        assert date1['TRD_DD'].iloc[0] == '20240101'
        assert date2['TRD_DD'].iloc[0] == '20240102'
        assert date3['TRD_DD'].iloc[0] == '20240103'
    
    def test_idempotent_overwrites(self, tmp_path):
        """Test that re-running with same data overwrites (idempotent)."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes_and_persist
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        from krx_quant_dataloader.storage.query import query_parquet_table
        
        ranks_v1 = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        ranks_v2 = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ1': 1}
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        # First write
        build_universes_and_persist(ranks_v1, universe_tiers, writer)
        
        # Second write (should overwrite)
        build_universes_and_persist(ranks_v2, universe_tiers, writer)
        
        # Should have v2 data only
        result = query_parquet_table(tmp_path, 'universes', start_date='20240101', end_date='20240101')
        
        assert len(result) == 1
        assert result['ISU_SRT_CD'].iloc[0] == 'STOCK02'


@pytest.mark.unit
class TestBuildUniversesEdgeCases:
    """Test edge cases and error handling."""
    
    def test_missing_required_columns_raises(self):
        """Test that missing columns raise appropriate errors."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Missing xs_liquidity_rank column
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ1': 1}
        
        with pytest.raises(KeyError):
            build_universes(ranks_df, universe_tiers)
    
    def test_empty_universe_tiers(self):
        """Test that empty universe_tiers returns empty result."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {}
        
        result = build_universes(ranks_df, universe_tiers)
        
        assert len(result) == 0

