"""
Unit tests for universe builder pipeline

Tests the build_universes() function that constructs universe membership
from liquidity ranks using boolean columns. Uses synthetic data.
"""

import pytest
import pandas as pd
from pathlib import Path


@pytest.mark.unit
class TestBuildUniversesLogic:
    """Test universe construction logic with synthetic data."""
    
    def test_single_date_single_universe(self):
        """Test building with boolean columns for one date."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Synthetic liquidity ranks
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK04', 'xs_liquidity_rank': 4, 'ACC_TRDVAL': 700000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK05', 'xs_liquidity_rank': 5, 'ACC_TRDVAL': 600000},
        ])
        
        # Build with standard tier
        universe_tiers = {'univ100': 100}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Should return one row per stock (5 stocks)
        assert len(result) == 5
        
        # All 5 stocks should have univ100=1 (since threshold is 100 and we only have 5 stocks)
        assert (result['univ100'] == 1).all()
    
    def test_multiple_universes_subset_relationship(self):
        """Test that subset relationships are explicit in boolean columns."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Create 10 stocks
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': f'STOCK{i:02d}', 'xs_liquidity_rank': i, 'ACC_TRDVAL': 1000000 - i*10000}
            for i in range(1, 11)
        ])
        
        # Build standard tiers
        universe_tiers = {'univ100': 100, 'univ200': 200, 'univ500': 500, 'univ1000': 1000}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # All 10 stocks should be in univ100, univ200, univ500, univ1000 (since threshold > 10)
        assert len(result) == 10
        assert (result['univ100'] == 1).all()
        assert (result['univ200'] == 1).all()
        assert (result['univ500'] == 1).all()
        assert (result['univ1000'] == 1).all()
        
        # Test subset relationships explicitly
        # If univ100=1, then univ200, univ500, univ1000 must also be 1
        univ100_stocks = result[result['univ100'] == 1]
        assert (univ100_stocks['univ200'] == 1).all()
        assert (univ100_stocks['univ500'] == 1).all()
        assert (univ100_stocks['univ1000'] == 1).all()
    
    def test_multiple_dates_cross_sectional_independence(self):
        """Test universe construction preserves per-date independence."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Same stocks, different ranks per date
        ranks_df = pd.DataFrame([
            # Date 1: STOCK01 is #1, STOCK02 is #2
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
            # Date 2: STOCK03 is #1, STOCK01 is #2
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
        ])
        
        universe_tiers = {'univ100': 100, 'univ200': 200}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # All stocks should be included (one row per stock per date)
        assert len(result) == 6
        assert result['TRD_DD'].nunique() == 2
        
        # All stocks should have univ100=1 and univ200=1 (threshold > 3)
        assert (result['univ100'] == 1).all()
        assert (result['univ200'] == 1).all()
    
    def test_stocks_boolean_flags_correct(self):
        """Test that boolean flags are correctly set based on rank thresholds."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 50, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 150, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 300, 'ACC_TRDVAL': 800000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK04', 'xs_liquidity_rank': 600, 'ACC_TRDVAL': 700000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK05', 'xs_liquidity_rank': 1500, 'ACC_TRDVAL': 600000},
        ])
        
        universe_tiers = {'univ100': 100, 'univ200': 200, 'univ500': 500, 'univ1000': 1000}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # STOCK01 (rank 50): in all universes
        stock01 = result[result['ISU_SRT_CD'] == 'STOCK01'].iloc[0]
        assert stock01['univ100'] == 1
        assert stock01['univ200'] == 1
        assert stock01['univ500'] == 1
        assert stock01['univ1000'] == 1
        
        # STOCK02 (rank 150): not in univ100, but in univ200+
        stock02 = result[result['ISU_SRT_CD'] == 'STOCK02'].iloc[0]
        assert stock02['univ100'] == 0
        assert stock02['univ200'] == 1
        assert stock02['univ500'] == 1
        assert stock02['univ1000'] == 1
        
        # STOCK03 (rank 300): only in univ500+
        stock03 = result[result['ISU_SRT_CD'] == 'STOCK03'].iloc[0]
        assert stock03['univ100'] == 0
        assert stock03['univ200'] == 0
        assert stock03['univ500'] == 1
        assert stock03['univ1000'] == 1
        
        # STOCK04 (rank 600): only in univ1000
        stock04 = result[result['ISU_SRT_CD'] == 'STOCK04'].iloc[0]
        assert stock04['univ100'] == 0
        assert stock04['univ200'] == 0
        assert stock04['univ500'] == 0
        assert stock04['univ1000'] == 1
        
        # STOCK05 (rank 1500): not in any universe
        stock05 = result[result['ISU_SRT_CD'] == 'STOCK05'].iloc[0]
        assert stock05['univ100'] == 0
        assert stock05['univ200'] == 0
        assert stock05['univ500'] == 0
        assert stock05['univ1000'] == 0
    
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
        
        # Should include all 3 stocks (since 3 < 100)
        assert len(result) == 3
        assert (result['univ100'] == 1).all()


@pytest.mark.unit
class TestBuildUniversesOutputFormat:
    """Test output DataFrame format and structure."""
    
    def test_output_has_required_columns(self):
        """Test that output contains all required columns."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Check required columns
        assert 'TRD_DD' in result.columns
        assert 'ISU_SRT_CD' in result.columns
        assert 'univ100' in result.columns
        assert 'univ200' in result.columns
        assert 'univ500' in result.columns
        assert 'univ1000' in result.columns
        assert 'xs_liquidity_rank' in result.columns
    
    def test_output_rank_dtype(self):
        """Test that xs_liquidity_rank is integer type."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # xs_liquidity_rank should be int
        assert pd.api.types.is_integer_dtype(result['xs_liquidity_rank'])
        
        # Boolean flags should be int8 (0 or 1)
        assert pd.api.types.is_integer_dtype(result['univ100'])
        assert result['univ100'].isin([0, 1]).all()
    
    def test_output_sorted_by_date_and_symbol(self):
        """Test that output is sorted by TRD_DD and ISU_SRT_CD."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK03', 'xs_liquidity_rank': 3, 'ACC_TRDVAL': 800000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        # Check sorting
        expected_order = [
            ('20240101', 'STOCK01'),
            ('20240101', 'STOCK02'),
            ('20240102', 'STOCK03'),
        ]
        
        actual_order = list(zip(result['TRD_DD'], result['ISU_SRT_CD']))
        assert actual_order == expected_order


@pytest.mark.unit
class TestBuildUniversesWithPersistence:
    """Test persistence via build_universes_and_persist()."""
    
    def test_writes_to_database(self, tmp_path):
        """Test that build_universes_and_persist() writes to database."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes_and_persist
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK02', 'xs_liquidity_rank': 2, 'ACC_TRDVAL': 900000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        row_count = build_universes_and_persist(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
            writer=writer,
        )
        
        # Should write 2 rows
        assert row_count == 2
        
        # Check file exists
        partition_path = tmp_path / 'universes' / 'TRD_DD=20240101'
        assert partition_path.exists()
    
    def test_per_date_partitioning(self, tmp_path):
        """Test that universes are partitioned by TRD_DD."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes_and_persist
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        build_universes_and_persist(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
            writer=writer,
        )
        
        # Check both partitions exist
        assert (tmp_path / 'universes' / 'TRD_DD=20240101').exists()
        assert (tmp_path / 'universes' / 'TRD_DD=20240102').exists()
    
    def test_idempotent_overwrites(self, tmp_path):
        """Test that re-running with same data overwrites existing partitions."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes_and_persist
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        # Write once
        count1 = build_universes_and_persist(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
            writer=writer,
        )
        
        # Write again (should overwrite)
        count2 = build_universes_and_persist(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
            writer=writer,
        )
        
        assert count1 == count2 == 1


@pytest.mark.unit
class TestBuildUniversesEdgeCases:
    """Test edge cases and error handling."""
    
    def test_missing_required_columns_raises(self):
        """Test that missing required columns raises KeyError."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        # Missing xs_liquidity_rank
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {'univ100': 100}
        
        with pytest.raises(KeyError, match="Missing required columns"):
            build_universes(
                ranks_df=ranks_df,
                universe_tiers=universe_tiers,
            )
    
    def test_empty_universe_tiers(self):
        """Test that empty universe_tiers returns empty result."""
        from krx_quant_dataloader.pipelines.universe_builder import build_universes
        
        ranks_df = pd.DataFrame([
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK01', 'xs_liquidity_rank': 1, 'ACC_TRDVAL': 1000000},
        ])
        
        universe_tiers = {}
        
        result = build_universes(
            ranks_df=ranks_df,
            universe_tiers=universe_tiers,
        )
        
        assert len(result) == 0
