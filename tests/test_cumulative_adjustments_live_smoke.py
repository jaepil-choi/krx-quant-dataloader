"""
Live smoke tests for cumulative adjustments pipeline

Tests end-to-end flow with real Samsung Electronics split data (2018-05-04).
Requires KRX API access and validates:
1. Adjustment factor computation
2. Cumulative adjustment computation
3. Ephemeral cache write/read
4. Price continuity validation
"""

import pytest
from pathlib import Path
import shutil

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.storage.query import query_parquet_table
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
from krx_quant_dataloader.transforms.adjustment import compute_cumulative_adjustments


@pytest.fixture(scope="module")
def samsung_split_db(tmp_path_factory):
    """
    Fixture providing Samsung split test database.
    
    Uses existing data/krx_db_samsung_split_test if available,
    otherwise skips (user must run exp_samsung_stock_split_2018.py first).
    """
    db_path = Path("data/krx_db_samsung_split_test")
    
    if not db_path.exists():
        pytest.skip(
            "Samsung split test DB not found. "
            "Run: poetry run python experiments/exp_samsung_stock_split_2018.py"
        )
    
    return db_path


@pytest.fixture(scope="module")
def temp_cache_path(tmp_path_factory):
    """Fixture providing temporary cache directory for tests."""
    temp_path = tmp_path_factory.mktemp("cumulative_adjustments_cache")
    yield temp_path
    # Cleanup after tests
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.mark.live
@pytest.mark.slow
class TestCumulativeAdjustmentsLiveSmoke:
    """Live smoke tests with real Samsung split data."""
    
    def test_load_adjustment_factors(self, samsung_split_db):
        """Test loading adjustment factors from persistent DB."""
        df_factors = query_parquet_table(
            db_path=samsung_split_db,
            table_name='adj_factors',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        assert not df_factors.empty
        assert len(df_factors) == 10  # 10 trading days
        assert df_factors['ISU_SRT_CD'].unique()[0] == '005930'
        
        # Verify split date factor
        split_row = df_factors[df_factors['TRD_DD'] == '20180504']
        assert len(split_row) == 1
        assert split_row.iloc[0]['adj_factor'] == pytest.approx(0.02, abs=1e-6)
    
    def test_compute_cumulative_adjustments(self, samsung_split_db):
        """Test computing cumulative adjustments from loaded factors."""
        # Load factors
        df_factors = query_parquet_table(
            db_path=samsung_split_db,
            table_name='adj_factors',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        factors_list = df_factors.to_dict('records')
        
        # Compute cumulative adjustments
        cum_adj_rows = compute_cumulative_adjustments(factors_list)
        
        assert len(cum_adj_rows) == 10
        
        # Convert to dict for easy access
        cum_adj_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in cum_adj_rows}
        
        # Validate pre-split dates (should be 0.02)
        pre_split_dates = ['20180425', '20180426', '20180427', '20180430', '20180502', '20180503']
        for date in pre_split_dates:
            assert cum_adj_dict[date] == pytest.approx(0.02, abs=1e-6), \
                f"Date {date} should have cum_adj=0.02 (pre-split)"
        
        # Validate split day (should be 1.0, NOT 0.02!)
        assert cum_adj_dict['20180504'] == pytest.approx(1.0, abs=1e-6), \
            "Split day should have cum_adj=1.0 (close is post-split scale)"
        
        # Validate post-split dates (should be 1.0)
        post_split_dates = ['20180508', '20180509', '20180510']
        for date in post_split_dates:
            assert cum_adj_dict[date] == pytest.approx(1.0, abs=1e-6), \
                f"Date {date} should have cum_adj=1.0 (post-split)"
    
    def test_write_to_ephemeral_cache(self, samsung_split_db, temp_cache_path):
        """Test writing cumulative adjustments to ephemeral cache."""
        # Load and compute
        df_factors = query_parquet_table(
            db_path=samsung_split_db,
            table_name='adj_factors',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        cum_adj_rows = compute_cumulative_adjustments(df_factors.to_dict('records'))
        
        # Initialize ephemeral writer
        writer = ParquetSnapshotWriter(root_path=temp_cache_path)
        
        # Group by date and write
        cum_adj_by_date = {}
        for row in cum_adj_rows:
            date = row['TRD_DD']
            if date not in cum_adj_by_date:
                cum_adj_by_date[date] = []
            cum_adj_by_date[date].append(row)
        
        total_written = 0
        for date, date_rows in sorted(cum_adj_by_date.items()):
            written = writer.write_cumulative_adjustments(date_rows, date=date)
            total_written += written
        
        assert total_written == 10
        
        # Verify files exist
        cache_dir = temp_cache_path / 'cumulative_adjustments'
        assert cache_dir.exists()
        
        # Should have 10 partitions (one per trading day)
        partitions = list(cache_dir.glob('TRD_DD=*/'))
        assert len(partitions) == 10
    
    def test_read_from_ephemeral_cache(self, samsung_split_db, temp_cache_path):
        """Test reading back cumulative adjustments from ephemeral cache."""
        # Write first (from previous test, but ensure it's done)
        df_factors = query_parquet_table(
            db_path=samsung_split_db,
            table_name='adj_factors',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        cum_adj_rows = compute_cumulative_adjustments(df_factors.to_dict('records'))
        
        writer = ParquetSnapshotWriter(root_path=temp_cache_path)
        cum_adj_by_date = {}
        for row in cum_adj_rows:
            date = row['TRD_DD']
            cum_adj_by_date.setdefault(date, []).append(row)
        
        for date, date_rows in cum_adj_by_date.items():
            writer.write_cumulative_adjustments(date_rows, date=date)
        
        # Read back
        df_cached = query_parquet_table(
            db_path=temp_cache_path,
            table_name='cumulative_adjustments',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']
        )
        
        assert not df_cached.empty
        assert len(df_cached) == 10
        
        # Verify split day value
        split_row = df_cached[df_cached['TRD_DD'] == '20180504']
        assert split_row.iloc[0]['cum_adj_multiplier'] == pytest.approx(1.0, abs=1e-6)
    
    def test_computed_vs_cached_match(self, samsung_split_db, temp_cache_path):
        """Test that computed values match cached values exactly."""
        # Compute
        df_factors = query_parquet_table(
            db_path=samsung_split_db,
            table_name='adj_factors',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        cum_adj_rows = compute_cumulative_adjustments(df_factors.to_dict('records'))
        computed_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in cum_adj_rows}
        
        # Write and read
        writer = ParquetSnapshotWriter(root_path=temp_cache_path)
        cum_adj_by_date = {}
        for row in cum_adj_rows:
            date = row['TRD_DD']
            cum_adj_by_date.setdefault(date, []).append(row)
        
        for date, date_rows in cum_adj_by_date.items():
            writer.write_cumulative_adjustments(date_rows, date=date)
        
        df_cached = query_parquet_table(
            db_path=temp_cache_path,
            table_name='cumulative_adjustments',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']
        )
        
        cached_dict = {
            row['TRD_DD']: row['cum_adj_multiplier']
            for _, row in df_cached.iterrows()
        }
        
        # All values should match
        for date in computed_dict.keys():
            assert computed_dict[date] == pytest.approx(cached_dict[date], abs=1e-10), \
                f"Mismatch at {date}: computed={computed_dict[date]}, cached={cached_dict[date]}"
    
    def test_price_continuity_with_adjustments(self, samsung_split_db, temp_cache_path):
        """Test that adjusted prices are continuous across split date."""
        # Load raw prices
        df_prices = query_parquet_table(
            db_path=samsung_split_db,
            table_name='snapshots',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'TDD_CLSPRC']
        )
        
        # Compute and cache adjustments
        df_factors = query_parquet_table(
            db_path=samsung_split_db,
            table_name='adj_factors',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        cum_adj_rows = compute_cumulative_adjustments(df_factors.to_dict('records'))
        
        writer = ParquetSnapshotWriter(root_path=temp_cache_path)
        cum_adj_by_date = {}
        for row in cum_adj_rows:
            date = row['TRD_DD']
            cum_adj_by_date.setdefault(date, []).append(row)
        
        for date, date_rows in cum_adj_by_date.items():
            writer.write_cumulative_adjustments(date_rows, date=date)
        
        # Load cumulative adjustments
        df_cum_adj = query_parquet_table(
            db_path=temp_cache_path,
            table_name='cumulative_adjustments',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']
        )
        
        # Merge and apply adjustments
        df_merged = df_prices.merge(
            df_cum_adj[['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']],
            on=['TRD_DD', 'ISU_SRT_CD'],
            how='left'
        )
        
        df_merged['adjusted_close'] = (
            df_merged['TDD_CLSPRC'] * df_merged['cum_adj_multiplier']
        ).round(0).astype(int)
        
        # Check specific dates
        pre_split = df_merged[df_merged['TRD_DD'] == '20180503'].iloc[0]
        split_day = df_merged[df_merged['TRD_DD'] == '20180504'].iloc[0]
        
        assert pre_split['adjusted_close'] == 53_000  # 2,650,000 × 0.02
        assert split_day['adjusted_close'] == 51_900  # 51,900 × 1.0
        
        # Price change should be reasonable
        pct_change = abs(split_day['adjusted_close'] - pre_split['adjusted_close']) / pre_split['adjusted_close'] * 100
        
        assert pct_change < 10, \
            f"Discontinuity detected: {pct_change:.2f}% change across split date"
    
    def test_precision_maintained_float64(self, samsung_split_db, temp_cache_path):
        """Test that precision is maintained at 1e-6 minimum."""
        df_factors = query_parquet_table(
            db_path=samsung_split_db,
            table_name='adj_factors',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        cum_adj_rows = compute_cumulative_adjustments(df_factors.to_dict('records'))
        
        # Check data type
        assert all(isinstance(row['cum_adj_multiplier'], float) for row in cum_adj_rows)
        
        # Check 0.02 is precisely represented
        pre_split_row = next(row for row in cum_adj_rows if row['TRD_DD'] == '20180425')
        assert pre_split_row['cum_adj_multiplier'] == pytest.approx(0.02, abs=1e-6)
        
        # Verify no loss of precision in storage round-trip
        writer = ParquetSnapshotWriter(root_path=temp_cache_path)
        cum_adj_by_date = {}
        for row in cum_adj_rows:
            date = row['TRD_DD']
            cum_adj_by_date.setdefault(date, []).append(row)
        
        for date, date_rows in cum_adj_by_date.items():
            writer.write_cumulative_adjustments(date_rows, date=date)
        
        df_cached = query_parquet_table(
            db_path=temp_cache_path,
            table_name='cumulative_adjustments',
            start_date='20180425',
            end_date='20180510',
            symbols=['005930'],
            fields=['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']
        )
        
        # Verify precision after round-trip
        cached_row = df_cached[df_cached['TRD_DD'] == '20180425'].iloc[0]
        assert cached_row['cum_adj_multiplier'] == pytest.approx(0.02, abs=1e-6)

