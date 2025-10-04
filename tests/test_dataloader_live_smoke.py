"""
Live smoke tests for DataLoader

Tests the DataLoader with real database and KRX API integration:
- Full pipeline: init → ingest → query
- Universe filtering with real universe tables
- Adjusted vs raw prices
- Wide-format output validation

Note: These tests use real I/O and may take several seconds.
Run with: pytest tests/test_dataloader_live_smoke.py -v -s
"""

import pytest
from pathlib import Path
import shutil
import pandas as pd

from krx_quant_dataloader.apis.dataloader import DataLoader


@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path for testing."""
    db_path = tmp_path / 'krx_db_dataloader_test'
    db_path.mkdir(parents=True, exist_ok=True)
    yield db_path
    # Cleanup after test
    if db_path.exists():
        shutil.rmtree(db_path)


@pytest.fixture
def test_temp_path(tmp_path):
    """Provide a temporary cache path for testing."""
    temp_path = tmp_path / 'temp'
    temp_path.mkdir(parents=True, exist_ok=True)
    yield temp_path
    # Cleanup after test
    if temp_path.exists():
        shutil.rmtree(temp_path)


@pytest.mark.live
@pytest.mark.slow
class TestDataLoaderLiveSmoke:
    """Live smoke tests for DataLoader end-to-end."""
    
    def test_init_and_query_single_field(self, test_db_path, test_temp_path):
        """
        Test full pipeline: initialize loader → query single field.
        
        Tests:
        - Initialization triggers 3-stage pipeline
        - Snapshots ingested from KRX (if missing)
        - Query returns wide-format DataFrame
        """
        print("\n" + "="*60)
        print("TEST: Initialize DataLoader and query 'close' field")
        print("="*60)
        
        # Initialize DataLoader (triggers 3-stage pipeline)
        print(f"\n[1] Initializing DataLoader...")
        print(f"    Date range: 2024-08-14 → 2024-08-16 (3 days)")
        print(f"    DB path: {test_db_path}")
        print(f"    Temp path: {test_temp_path}")
        
        loader = DataLoader(
            db_path=test_db_path,
            start_date='20240814',
            end_date='20240816',
            temp_path=test_temp_path
        )
        
        print(f"    [OK] Loader initialized")
        
        # Query close prices (no universe filter, no adjustment)
        print(f"\n[2] Querying 'close' field...")
        result = loader.get_data('close', universe=None, adjusted=False)
        
        print(f"    [OK] Query returned {len(result)} rows × {len(result.columns)} symbols")
        
        # Validate output format
        print(f"\n[3] Validating output format...")
        assert isinstance(result, pd.DataFrame), "Result should be DataFrame"
        assert len(result) > 0, "Result should not be empty"
        assert len(result.columns) > 0, "Result should have columns (symbols)"
        
        print(f"    [OK] Output format validated")
        
        # Display sample
        print(f"\n[4] Sample output (first 5 symbols):")
        print(result.iloc[:, :5].head())
        
        print("\n" + "="*60)
        print("TEST PASSED")
        print("="*60)
    
    def test_query_with_universe_filter(self, test_db_path, test_temp_path):
        """
        Test querying with pre-computed universe filter (univ100).
        
        Tests:
        - Universe table is queried
        - Result contains only universe members
        - Per-date filtering (survivorship bias-free)
        """
        print("\n" + "="*60)
        print("TEST: Query with universe='univ100' filter")
        print("="*60)
        
        # Initialize loader
        print(f"\n[1] Initializing DataLoader...")
        loader = DataLoader(
            db_path=test_db_path,
            start_date='20240814',
            end_date='20240816',
            temp_path=test_temp_path
        )
        print(f"    [OK] Loader initialized")
        
        # Query with universe filter
        print(f"\n[2] Querying 'close' with universe='univ100'...")
        result = loader.get_data('close', universe='univ100', adjusted=False)
        
        print(f"    [OK] Query returned {len(result)} rows × {len(result.columns)} symbols")
        
        # Validate universe filtering
        print(f"\n[3] Validating universe filtering...")
        assert len(result.columns) <= 100, f"univ100 should have ≤100 symbols, got {len(result.columns)}"
        
        print(f"    [OK] Universe filtering validated")
        
        # Display sample
        print(f"\n[4] Sample output (first 10 symbols):")
        print(result.iloc[:, :10].head())
        
        print("\n" + "="*60)
        print("TEST PASSED")
        print("="*60)
    
    def test_query_with_explicit_symbol_list(self, test_db_path, test_temp_path):
        """
        Test querying with explicit symbol list.
        
        Tests:
        - Explicit list filtering works
        - Result contains only specified symbols
        - All dates included for each symbol
        """
        print("\n" + "="*60)
        print("TEST: Query with explicit symbol list")
        print("="*60)
        
        # Initialize loader
        print(f"\n[1] Initializing DataLoader...")
        loader = DataLoader(
            db_path=test_db_path,
            start_date='20240814',
            end_date='20240816',
            temp_path=test_temp_path
        )
        print(f"    [OK] Loader initialized")
        
        # Query with explicit symbol list
        symbols = ['005930', '000660', '035720']  # Samsung, SK Hynix, Kakao
        print(f"\n[2] Querying 'close' for symbols: {symbols}...")
        result = loader.get_data('close', universe=symbols, adjusted=False)
        
        print(f"    [OK] Query returned {len(result)} rows × {len(result.columns)} symbols")
        
        # Validate symbol filtering
        print(f"\n[3] Validating symbol filtering...")
        assert set(result.columns) <= set(symbols), "Result should only contain specified symbols"
        
        print(f"    [OK] Symbol filtering validated")
        
        # Display result
        print(f"\n[4] Result:")
        print(result)
        
        print("\n" + "="*60)
        print("TEST PASSED")
        print("="*60)
    
    def test_adjusted_vs_raw_prices(self, test_db_path, test_temp_path):
        """
        Test adjusted vs raw prices.
        
        Tests:
        - adjusted=True applies cumulative adjustments
        - adjusted=False returns raw prices
        - Adjusted prices differ from raw (if corporate actions exist)
        """
        print("\n" + "="*60)
        print("TEST: Adjusted vs raw prices")
        print("="*60)
        
        # Initialize loader
        print(f"\n[1] Initializing DataLoader...")
        loader = DataLoader(
            db_path=test_db_path,
            start_date='20240814',
            end_date='20240816',
            temp_path=test_temp_path
        )
        print(f"    [OK] Loader initialized")
        
        # Query raw prices
        print(f"\n[2] Querying raw prices (adjusted=False)...")
        raw_prices = loader.get_data(
            'close',
            universe=['005930'],  # Samsung only
            adjusted=False
        )
        print(f"    [OK] Raw prices:")
        print(raw_prices)
        
        # Query adjusted prices
        print(f"\n[3] Querying adjusted prices (adjusted=True)...")
        adj_prices = loader.get_data(
            'close',
            universe=['005930'],
            adjusted=True
        )
        print(f"    [OK] Adjusted prices:")
        print(adj_prices)
        
        # Validate
        print(f"\n[4] Validating...")
        assert raw_prices.shape == adj_prices.shape, "Shapes should match"
        print(f"    [OK] Shapes match")
        
        # Note: Adjusted prices may equal raw if no corporate actions in window
        print(f"\n[5] Note: Adjusted prices may equal raw if no corporate actions in this window")
        
        print("\n" + "="*60)
        print("TEST PASSED")
        print("="*60)
    
    def test_sub_range_query(self, test_db_path, test_temp_path):
        """
        Test querying a sub-range within loader's date range.
        
        Tests:
        - Sub-range queries work correctly
        - Only requested dates are returned
        - No re-ingestion needed (fast)
        """
        print("\n" + "="*60)
        print("TEST: Sub-range query")
        print("="*60)
        
        # Initialize loader with wider range
        print(f"\n[1] Initializing DataLoader with range [20240814, 20240820]...")
        loader = DataLoader(
            db_path=test_db_path,
            start_date='20240814',
            end_date='20240820',
            temp_path=test_temp_path
        )
        print(f"    [OK] Loader initialized")
        
        # Query sub-range
        print(f"\n[2] Querying sub-range [20240814, 20240816]...")
        result = loader.get_data(
            'close',
            query_start='20240814',
            query_end='20240816',
            universe=['005930'],
            adjusted=False
        )
        
        print(f"    [OK] Query returned {len(result)} rows")
        
        # Validate date range
        print(f"\n[3] Validating date range...")
        assert len(result) <= 3, f"Should have ≤3 dates, got {len(result)}"
        
        print(f"    [OK] Date range validated")
        
        # Display result
        print(f"\n[4] Result:")
        print(result)
        
        print("\n" + "="*60)
        print("TEST PASSED")
        print("="*60)
    
    def test_query_out_of_range_raises_error(self, test_db_path, test_temp_path):
        """
        Test that querying outside loader's date range raises error.
        
        Tests:
        - Out-of-range query raises ValueError
        - Error message is clear and actionable
        """
        print("\n" + "="*60)
        print("TEST: Out-of-range query raises error")
        print("="*60)
        
        # Initialize loader
        print(f"\n[1] Initializing DataLoader with range [20240814, 20240816]...")
        loader = DataLoader(
            db_path=test_db_path,
            start_date='20240814',
            end_date='20240816',
            temp_path=test_temp_path
        )
        print(f"    [OK] Loader initialized")
        
        # Attempt out-of-range query
        print(f"\n[2] Attempting query outside range [20240820, 20240822]...")
        with pytest.raises(ValueError, match="Query range .* outside loader window"):
            loader.get_data(
                'close',
                query_start='20240820',
                query_end='20240822'
            )
        
        print(f"    [OK] ValueError raised as expected")
        
        print("\n" + "="*60)
        print("TEST PASSED")
        print("="*60)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])

