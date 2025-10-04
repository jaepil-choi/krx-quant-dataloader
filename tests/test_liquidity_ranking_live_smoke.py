"""
Live smoke tests for liquidity ranking pipeline

Tests end-to-end pipeline with real Samsung split test database:
1. Load raw snapshots from database
2. Compute liquidity ranks
3. Write to database
4. Read back and validate
5. Verify known stocks (Samsung, SK Hynix) in expected positions
"""

import pytest
import pandas as pd
from pathlib import Path

from krx_quant_dataloader.storage.query import query_parquet_table

# Placeholder imports for functions to be implemented
# from krx_quant_dataloader.pipelines.liquidity_ranking import (
#     compute_liquidity_ranks,
#     write_liquidity_ranks,
#     query_liquidity_ranks,
# )


# Test database from Samsung split experiment
TEST_DB_PATH = Path("data/krx_db_samsung_split_test")
TEST_START_DATE = "20180425"
TEST_END_DATE = "20180510"

# Known liquid stocks from experiment
SAMSUNG_CODE = "005930"
SK_HYNIX_CODE = "000660"
CELLTRION_CODE = "068270"


class TestLiquidityRankingPipeline:
    """Test end-to-end liquidity ranking pipeline with real data."""
    
    @pytest.fixture
    def raw_snapshots(self):
        """Load raw snapshots from test database."""
        if not TEST_DB_PATH.exists():
            pytest.skip(f"Test database not found: {TEST_DB_PATH}")
        
        df = query_parquet_table(
            db_path=str(TEST_DB_PATH),
            table_name='snapshots',
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
        )
        
        assert len(df) > 0, "No data in test database"
        return df
    
    def test_compute_ranks_basic(self, raw_snapshots):
        """Test compute_liquidity_ranks() returns DataFrame with rank column."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # # Should have rank column
        # assert 'xs_liquidity_rank' in result.columns
        
        # # Should preserve all rows
        # assert len(result) == len(raw_snapshots)
        
        # # Rank should be integer
        # assert result['xs_liquidity_rank'].dtype in ['int64', 'int32']
        pass
    
    def test_compute_ranks_samsung_rank_one(self, raw_snapshots):
        """Test Samsung (005930) ranks #1 on trading days."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # # Samsung should be rank 1 on most dates (except trading halt days)
        # samsung = result[result['ISU_SRT_CD'] == SAMSUNG_CODE]
        # trading_days = samsung[samsung['ACC_TRDVAL'] > 0]
        
        # # All trading days should be rank 1
        # for _, row in trading_days.iterrows():
        #     assert row['xs_liquidity_rank'] == 1, \
        #         f"Samsung rank {row['xs_liquidity_rank']} on {row['TRD_DD']} (expected 1)"
        pass
    
    def test_compute_ranks_samsung_halt_days(self, raw_snapshots):
        """Test Samsung ranks low on trading halt days (2018-04-30, 05-02, 05-03)."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # samsung = result[result['ISU_SRT_CD'] == SAMSUNG_CODE]
        # halt_days = ['20180430', '20180502', '20180503']
        
        # for halt_date in halt_days:
        #     halt_row = samsung[samsung['TRD_DD'] == halt_date]
        #     if len(halt_row) > 0:
        #         rank = halt_row['xs_liquidity_rank'].iloc[0]
        #         assert rank > 2000, \
        #             f"Samsung rank {rank} on halt day {halt_date} (expected >2000)"
        pass
    
    def test_compute_ranks_sk_hynix_top10(self, raw_snapshots):
        """Test SK Hynix (000660) consistently in top 10."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # sk_hynix = result[result['ISU_SRT_CD'] == SK_HYNIX_CODE]
        
        # # SK Hynix should be in top 10 on all dates
        # for _, row in sk_hynix.iterrows():
        #     assert row['xs_liquidity_rank'] <= 10, \
        #         f"SK Hynix rank {row['xs_liquidity_rank']} on {row['TRD_DD']} (expected ≤10)"
        pass
    
    def test_compute_ranks_celltrion_top10(self, raw_snapshots):
        """Test Celltrion (068270) consistently in top 10."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # celltrion = result[result['ISU_SRT_CD'] == CELLTRION_CODE]
        
        # # Celltrion should be in top 10 on all dates
        # for _, row in celltrion.iterrows():
        #     assert row['xs_liquidity_rank'] <= 10, \
        #         f"Celltrion rank {row['xs_liquidity_rank']} on {row['TRD_DD']} (expected ≤10)"
        pass
    
    def test_compute_ranks_all_dates_have_rank_one(self, raw_snapshots):
        """Test every date has at least one stock with rank 1."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # dates = result['TRD_DD'].unique()
        
        # for date in dates:
        #     date_data = result[result['TRD_DD'] == date]
        #     rank_ones = date_data[date_data['xs_liquidity_rank'] == 1]
        #     assert len(rank_ones) > 0, f"No rank 1 stock on {date}"
        pass
    
    def test_compute_ranks_dense_ranking_no_gaps(self, raw_snapshots):
        """Test dense ranking produces no gaps in rank sequences per date."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # dates = result['TRD_DD'].unique()
        
        # for date in dates:
        #     date_data = result[result['TRD_DD'] == date]
        #     ranks = sorted(date_data['xs_liquidity_rank'].unique())
        #     
        #     # Check no gaps (ranks should be consecutive: 1, 2, 3, ...)
        #     max_rank = max(ranks)
        #     expected_ranks = list(range(1, max_rank + 1))
        #     assert ranks == expected_ranks, \
        #         f"Gaps in ranking on {date}: {ranks} vs expected {expected_ranks}"
        pass
    
    def test_compute_ranks_cross_sectional_independence(self, raw_snapshots):
        """Test Samsung ranks vary across dates (cross-sectional independence)."""
        # result = compute_liquidity_ranks(raw_snapshots)
        
        # samsung = result[result['ISU_SRT_CD'] == SAMSUNG_CODE]
        # unique_ranks = samsung['xs_liquidity_rank'].nunique()
        
        # # Samsung should have at least 2 different ranks (trading vs halt days)
        # assert unique_ranks >= 2, \
        #     f"Samsung has only {unique_ranks} unique ranks (expected ≥2 for cross-sectional independence)"
        pass


class TestLiquidityRankingPersistence:
    """Test write and read-back of liquidity ranks to database."""
    
    @pytest.fixture
    def computed_ranks(self):
        """Compute liquidity ranks from test database."""
        if not TEST_DB_PATH.exists():
            pytest.skip(f"Test database not found: {TEST_DB_PATH}")
        
        df = query_parquet_table(
            db_path=str(TEST_DB_PATH),
            table_name='snapshots',
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
        )
        
        # result = compute_liquidity_ranks(df)
        # return result
        pytest.skip("compute_liquidity_ranks not implemented yet")
    
    def test_write_ranks_to_database(self, computed_ranks, tmp_path):
        """Test write_liquidity_ranks() persists to database."""
        # temp_db = tmp_path / "test_liquidity_ranks.db"
        
        # write_liquidity_ranks(computed_ranks, db_path=str(temp_db))
        
        # # Database should exist
        # assert temp_db.exists()
        
        # # Should have data
        # assert temp_db.stat().st_size > 0
        pass
    
    def test_read_ranks_from_database(self, computed_ranks, tmp_path):
        """Test query_liquidity_ranks() reads back correctly."""
        # temp_db = tmp_path / "test_liquidity_ranks.db"
        
        # # Write
        # write_liquidity_ranks(computed_ranks, db_path=str(temp_db))
        
        # # Read back
        # readback = query_liquidity_ranks(
        #     db_path=str(temp_db),
        #     start_date=TEST_START_DATE,
        #     end_date=TEST_END_DATE,
        # )
        
        # # Should match computed ranks
        # assert len(readback) == len(computed_ranks)
        
        # # Check Samsung ranks match
        # samsung_computed = computed_ranks[computed_ranks['ISU_SRT_CD'] == SAMSUNG_CODE]
        # samsung_readback = readback[readback['ISU_SRT_CD'] == SAMSUNG_CODE]
        
        # pd.testing.assert_frame_equal(
        #     samsung_computed.sort_values('TRD_DD').reset_index(drop=True),
        #     samsung_readback.sort_values('TRD_DD').reset_index(drop=True),
        # )
        pass
    
    def test_write_and_query_specific_date(self, computed_ranks, tmp_path):
        """Test querying single date returns only that date's ranks."""
        # temp_db = tmp_path / "test_liquidity_ranks.db"
        
        # # Write all dates
        # write_liquidity_ranks(computed_ranks, db_path=str(temp_db))
        
        # # Query single date
        # single_date = "20180427"
        # result = query_liquidity_ranks(
        #     db_path=str(temp_db),
        #     start_date=single_date,
        #     end_date=single_date,
        # )
        
        # # Should only have data for 20180427
        # assert result['TRD_DD'].nunique() == 1
        # assert result['TRD_DD'].iloc[0] == single_date
        
        # # Should match computed ranks for that date
        # expected = computed_ranks[computed_ranks['TRD_DD'] == single_date]
        # assert len(result) == len(expected)
        pass
    
    def test_write_preserves_all_columns(self, computed_ranks, tmp_path):
        """Test write preserves all columns (not just rank)."""
        # temp_db = tmp_path / "test_liquidity_ranks.db"
        
        # # Write
        # write_liquidity_ranks(computed_ranks, db_path=str(temp_db))
        
        # # Read back
        # readback = query_liquidity_ranks(
        #     db_path=str(temp_db),
        #     start_date=TEST_START_DATE,
        #     end_date=TEST_END_DATE,
        # )
        
        # # Should preserve ACC_TRDVAL, ISU_ABBRV, etc.
        # assert 'ACC_TRDVAL' in readback.columns
        # assert 'ISU_ABBRV' in readback.columns
        # assert 'ISU_SRT_CD' in readback.columns
        # assert 'xs_liquidity_rank' in readback.columns
        pass


class TestLiquidityRankingPerformance:
    """Test performance and scalability."""
    
    def test_compute_ranks_completes_in_reasonable_time(self):
        """Test ranking 23k rows completes in <5 seconds."""
        if not TEST_DB_PATH.exists():
            pytest.skip(f"Test database not found: {TEST_DB_PATH}")
        
        import time
        
        df = query_parquet_table(
            db_path=str(TEST_DB_PATH),
            table_name='snapshots',
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
        )
        
        # start = time.time()
        # result = compute_liquidity_ranks(df)
        # elapsed = time.time() - start
        
        # # Should complete in <5 seconds for 23k rows
        # assert elapsed < 5.0, f"Ranking took {elapsed:.2f}s (expected <5s)"
        
        # # Verify result is valid
        # assert len(result) == len(df)
        pass


class TestLiquidityRankingIntegration:
    """Test integration with upstream/downstream pipelines."""
    
    def test_ranks_sortable_for_row_group_pruning(self):
        """Test ranks are suitable for sorting (enables row-group pruning)."""
        if not TEST_DB_PATH.exists():
            pytest.skip(f"Test database not found: {TEST_DB_PATH}")
        
        df = query_parquet_table(
            db_path=str(TEST_DB_PATH),
            table_name='snapshots',
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
        )
        
        # result = compute_liquidity_ranks(df)
        
        # # Sort by rank
        # sorted_result = result.sort_values(['TRD_DD', 'xs_liquidity_rank'])
        
        # # Verify top 100 per date can be efficiently queried
        # for date in sorted_result['TRD_DD'].unique():
        #     top100 = sorted_result[
        #         (sorted_result['TRD_DD'] == date) & 
        #         (sorted_result['xs_liquidity_rank'] <= 100)
        #     ]
        #     assert len(top100) >= 100  # Should have at least 100 stocks
        pass
    
    def test_ranks_compatible_with_universe_builder(self):
        """Test rank format is compatible with universe builder (Stage 4)."""
        if not TEST_DB_PATH.exists():
            pytest.skip(f"Test database not found: {TEST_DB_PATH}")
        
        df = query_parquet_table(
            db_path=str(TEST_DB_PATH),
            table_name='snapshots',
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
        )
        
        # result = compute_liquidity_ranks(df)
        
        # # Should be able to filter top N stocks per date
        # N = 500
        # top_n = result[result['xs_liquidity_rank'] <= N]
        
        # # Every date should have N stocks (or close to N due to ties)
        # for date in result['TRD_DD'].unique():
        #     date_top_n = top_n[top_n['TRD_DD'] == date]
        #     assert len(date_top_n) >= N, \
        #         f"Date {date} has {len(date_top_n)} stocks in top {N} (expected ≥{N})"
        pass

