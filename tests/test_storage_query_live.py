"""
Live smoke tests for storage query layer (storage/query.py)

These tests use the actual krx_db_test Parquet database built earlier.
They validate:
- Generic query_parquet_table() for all table types
- Partition pruning (date range filtering)
- Row-group pruning (symbol filtering)
- Column pruning (field selection)
- TRD_DD injection from partition names
- Missing partition handling (holidays)
- Pandas DataFrame returns
"""

import pytest
import pandas as pd
from pathlib import Path

from krx_quant_dataloader.storage.query import (
    query_parquet_table,
    load_universe_symbols,
)


@pytest.fixture
def krx_db_test_path() -> Path:
    """Path to test Parquet database (3 days: 20241101, 20241104, 20241105)."""
    return Path(__file__).parent.parent / "data" / "krx_db_test"


@pytest.mark.live
@pytest.mark.slow
def test_query_parquet_table_snapshots_all_data(krx_db_test_path):
    """Test querying snapshots table with full date range (no filters)."""
    print("\n" + "="*80)
    print("TEST: Query all snapshot data (3 trading days)")
    print("="*80)
    
    df = query_parquet_table(
        db_path=krx_db_test_path,
        table_name='snapshots',
        start_date='20241101',
        end_date='20241105',
        symbols=None,  # All symbols
        fields=None,   # All fields
    )
    
    # Validate return type
    assert isinstance(df, pd.DataFrame), "Should return Pandas DataFrame"
    
    # Validate TRD_DD injection
    assert 'TRD_DD' in df.columns, "TRD_DD should be injected from partition names"
    dates = sorted(df['TRD_DD'].unique())
    print(f"\nDates found: {dates}")
    
    # Should have 3 trading days (20241101, 20241104, 20241105)
    # Note: 20241102-20241103 are weekend (missing partitions)
    assert len(dates) == 3, f"Expected 3 trading days, got {len(dates)}"
    assert '20241101' in dates
    assert '20241104' in dates
    assert '20241105' in dates
    
    # Validate data
    assert len(df) > 0, "Should have data"
    assert 'ISU_SRT_CD' in df.columns
    assert 'TDD_CLSPRC' in df.columns
    
    print(f"\nTotal rows: {len(df):,}")
    print(f"Unique symbols: {df['ISU_SRT_CD'].nunique()}")
    print(f"\nSample data (first 5 rows):")
    print(df[['TRD_DD', 'ISU_SRT_CD', 'ISU_ABBRV', 'TDD_CLSPRC', 'ACC_TRDVAL']].head())
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")


@pytest.mark.live
@pytest.mark.slow
def test_query_parquet_table_snapshots_symbol_filter(krx_db_test_path):
    """Test row-group pruning with symbol filter."""
    print("\n" + "="*80)
    print("TEST: Query snapshots with symbol filter (Samsung, SK Hynix, Kakao)")
    print("="*80)
    
    symbols = ['005930', '000660', '035720']  # Samsung, SK Hynix, Kakao
    
    df = query_parquet_table(
        db_path=krx_db_test_path,
        table_name='snapshots',
        start_date='20241104',
        end_date='20241105',
        symbols=symbols,
        fields=None,
    )
    
    # Validate symbol filtering
    unique_symbols = df['ISU_SRT_CD'].unique().tolist()
    print(f"\nRequested symbols: {symbols}")
    print(f"Found symbols: {unique_symbols}")
    
    assert len(unique_symbols) <= len(symbols), "Should only return requested symbols"
    for sym in unique_symbols:
        assert sym in symbols, f"Unexpected symbol: {sym}"
    
    # Validate dates
    dates = sorted(df['TRD_DD'].unique())
    print(f"Dates: {dates}")
    assert len(dates) == 2, f"Expected 2 days, got {len(dates)}"
    
    # Show sample data per symbol
    print(f"\nSample data per symbol:")
    for sym in symbols:
        sym_data = df[df['ISU_SRT_CD'] == sym]
        if len(sym_data) > 0:
            print(f"\n{sym} ({sym_data['ISU_ABBRV'].iloc[0]}):")
            print(sym_data[['TRD_DD', 'TDD_CLSPRC', 'ACC_TRDVAL']].to_string())


@pytest.mark.live
@pytest.mark.slow
def test_query_parquet_table_snapshots_column_pruning(krx_db_test_path):
    """Test column pruning (only read needed fields)."""
    print("\n" + "="*80)
    print("TEST: Query snapshots with column pruning (only 3 fields)")
    print("="*80)
    
    fields = ['TRD_DD', 'ISU_SRT_CD', 'TDD_CLSPRC']
    
    df = query_parquet_table(
        db_path=krx_db_test_path,
        table_name='snapshots',
        start_date='20241105',
        end_date='20241105',
        symbols=None,
        fields=fields,
    )
    
    # Validate columns
    print(f"\nRequested fields: {fields}")
    print(f"DataFrame columns: {list(df.columns)}")
    
    # Should only have requested columns (plus TRD_DD if not in fields)
    expected_cols = set(fields)
    actual_cols = set(df.columns)
    
    assert expected_cols.issubset(actual_cols), "Should have all requested fields"
    assert len(df) > 0, "Should have data"
    
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Sample data (first 10 rows):")
    print(df.head(10))


@pytest.mark.live
@pytest.mark.slow
def test_query_parquet_table_adj_factors(krx_db_test_path):
    """Test querying sparse adj_factors table."""
    print("\n" + "="*80)
    print("TEST: Query adj_factors table (sparse)")
    print("="*80)
    
    df = query_parquet_table(
        db_path=krx_db_test_path,
        table_name='adj_factors',
        start_date='20241104',
        end_date='20241105',
        symbols=['005930', '000660'],
        fields=None,
    )
    
    # Validate return type
    assert isinstance(df, pd.DataFrame), "Should return Pandas DataFrame"
    
    # adj_factors is sparse - may have no rows if no corporate actions
    print(f"\nAdjustment factors found: {len(df)}")
    
    if len(df) > 0:
        assert 'TRD_DD' in df.columns
        assert 'ISU_SRT_CD' in df.columns
        assert 'adj_factor' in df.columns
        
        print(f"\nColumns: {list(df.columns)}")
        print(f"\nSample factors:")
        print(df[['TRD_DD', 'ISU_SRT_CD', 'adj_factor']].head(10))
        
        # Validate factor values
        print(f"\nFactor statistics:")
        print(df['adj_factor'].describe())
    else:
        print("No adjustment factors found (expected if no corporate actions in period)")


@pytest.mark.live
@pytest.mark.slow
def test_query_parquet_table_partition_pruning(krx_db_test_path):
    """Test partition pruning (date range filtering)."""
    print("\n" + "="*80)
    print("TEST: Partition pruning - single day vs. date range")
    print("="*80)
    
    # Query single day
    df_single = query_parquet_table(
        db_path=krx_db_test_path,
        table_name='snapshots',
        start_date='20241105',
        end_date='20241105',
        symbols=None,
        fields=['TRD_DD', 'ISU_SRT_CD'],
    )
    
    # Query date range
    df_range = query_parquet_table(
        db_path=krx_db_test_path,
        table_name='snapshots',
        start_date='20241104',
        end_date='20241105',
        symbols=None,
        fields=['TRD_DD', 'ISU_SRT_CD'],
    )
    
    dates_single = df_single['TRD_DD'].unique()
    dates_range = sorted(df_range['TRD_DD'].unique())
    
    print(f"\nSingle day query (20241105):")
    print(f"  Dates: {dates_single}")
    print(f"  Rows: {len(df_single):,}")
    
    print(f"\nDate range query (20241104-20241105):")
    print(f"  Dates: {dates_range}")
    print(f"  Rows: {len(df_range):,}")
    
    # Validate partition pruning worked
    assert len(dates_single) == 1
    assert dates_single[0] == '20241105'
    
    assert len(dates_range) == 2
    assert '20241104' in dates_range
    assert '20241105' in dates_range
    
    # Range should have more data than single day
    assert len(df_range) > len(df_single)


@pytest.mark.live
@pytest.mark.slow
def test_query_parquet_table_missing_partitions_holiday(krx_db_test_path):
    """Test that missing partitions (holidays) are handled gracefully."""
    print("\n" + "="*80)
    print("TEST: Missing partitions (weekend 20241102-20241103)")
    print("="*80)
    
    # Query range including weekend (missing partitions)
    df = query_parquet_table(
        db_path=krx_db_test_path,
        table_name='snapshots',
        start_date='20241101',
        end_date='20241105',
        symbols=['005930'],
        fields=['TRD_DD', 'ISU_SRT_CD', 'TDD_CLSPRC'],
    )
    
    dates = sorted(df['TRD_DD'].unique())
    print(f"\nRequested range: 20241101-20241105 (5 calendar days)")
    print(f"Found dates: {dates}")
    print(f"Missing dates: ['20241102', '20241103'] (weekend)")
    
    # Should only have trading days, silently skip holidays
    assert '20241102' not in dates, "Weekend should be skipped"
    assert '20241103' not in dates, "Weekend should be skipped"
    assert '20241101' in dates, "Friday should be present"
    assert '20241104' in dates, "Monday should be present"
    assert '20241105' in dates, "Tuesday should be present"
    
    print(f"\nRows per date:")
    for date in dates:
        count = len(df[df['TRD_DD'] == date])
        price = df[df['TRD_DD'] == date]['TDD_CLSPRC'].iloc[0]
        print(f"  {date}: {count} row(s), close = {price:,}")


@pytest.mark.live
@pytest.mark.slow
@pytest.mark.skip(reason="Requires universe_builder to run first (Phase 2)")
def test_load_universe_symbols_univ100(krx_db_test_path):
    """Test loading pre-computed universe (univ100)."""
    print("\n" + "="*80)
    print("TEST: Load universe symbols (univ100)")
    print("="*80)
    
    universe_map = load_universe_symbols(
        db_path=krx_db_test_path,
        universe_name='univ100',
        start_date='20241104',
        end_date='20241105',
    )
    
    # Validate return type
    assert isinstance(universe_map, dict), "Should return dict"
    
    # Validate structure
    print(f"\nUniverse map keys (dates): {sorted(universe_map.keys())}")
    
    for date, symbols in universe_map.items():
        assert isinstance(symbols, list), f"Symbols for {date} should be list"
        assert len(symbols) <= 100, f"univ100 should have max 100 symbols, got {len(symbols)}"
        
        print(f"\n{date}: {len(symbols)} symbols")
        print(f"  Top 10: {symbols[:10]}")
        print(f"  Last 10: {symbols[-10:]}")


if __name__ == "__main__":
    # Run live smoke tests with verbose output
    pytest.main([__file__, "-v", "-s", "-m", "live"])

