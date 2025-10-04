"""
Unit tests for cumulative adjustments computation

Tests the compute_cumulative_adjustments() function in transforms/adjustment.py
using synthetic data and real Samsung split data.
"""

import pytest
from decimal import Decimal

from krx_quant_dataloader.transforms.adjustment import compute_cumulative_adjustments


class TestCumulativeAdjustmentsLogic:
    """Test cumulative adjustments computation logic with synthetic data."""
    
    def test_single_symbol_no_splits(self):
        """Test single symbol with no corporate actions (all factors = 1.0)."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        
        assert len(result) == 3
        # All cumulative adjustments should be 1.0 (no future events)
        for row in result:
            assert row['cum_adj_multiplier'] == 1.0
            assert row['ISU_SRT_CD'] == 'TEST01'
    
    def test_single_symbol_one_split(self):
        """Test single symbol with one 2:1 stock split."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 0.5},  # 2:1 split
            {'TRD_DD': '20240104', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240105', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        result_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in result}
        
        # Pre-split dates should be adjusted by 0.5
        assert result_dict['20240101'] == pytest.approx(0.5)
        assert result_dict['20240102'] == pytest.approx(0.5)
        
        # Split day and after should be 1.0 (CRITICAL: split day excludes own factor)
        assert result_dict['20240103'] == pytest.approx(1.0)
        assert result_dict['20240104'] == pytest.approx(1.0)
        assert result_dict['20240105'] == pytest.approx(1.0)
    
    def test_samsung_50_to_1_split(self):
        """Test Samsung Electronics 50:1 stock split (2018-05-04)."""
        factors = [
            {'TRD_DD': '20180425', 'ISU_SRT_CD': '005930', 'adj_factor': None},  # First day
            {'TRD_DD': '20180426', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
            {'TRD_DD': '20180427', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
            {'TRD_DD': '20180430', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
            {'TRD_DD': '20180502', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
            {'TRD_DD': '20180503', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
            {'TRD_DD': '20180504', 'ISU_SRT_CD': '005930', 'adj_factor': 0.02},  # 50:1 split
            {'TRD_DD': '20180508', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
            {'TRD_DD': '20180509', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
            {'TRD_DD': '20180510', 'ISU_SRT_CD': '005930', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        result_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in result}
        
        # Pre-split dates should be adjusted by 0.02
        assert result_dict['20180425'] == pytest.approx(0.02, abs=1e-6)
        assert result_dict['20180426'] == pytest.approx(0.02, abs=1e-6)
        assert result_dict['20180427'] == pytest.approx(0.02, abs=1e-6)
        assert result_dict['20180430'] == pytest.approx(0.02, abs=1e-6)
        assert result_dict['20180502'] == pytest.approx(0.02, abs=1e-6)
        assert result_dict['20180503'] == pytest.approx(0.02, abs=1e-6)
        
        # Split day should have cum_adj = 1.0 (CRITICAL: excludes own factor)
        assert result_dict['20180504'] == pytest.approx(1.0, abs=1e-6)
        
        # Post-split dates should be 1.0
        assert result_dict['20180508'] == pytest.approx(1.0, abs=1e-6)
        assert result_dict['20180509'] == pytest.approx(1.0, abs=1e-6)
        assert result_dict['20180510'] == pytest.approx(1.0, abs=1e-6)
    
    def test_multiple_splits_same_symbol(self):
        """Test symbol with multiple corporate actions."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 0.5},   # 2:1 split
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240104', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 2.0},   # Reverse split
            {'TRD_DD': '20240105', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        result_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in result}
        
        # 20240101: future = [0.5, 1.0, 2.0, 1.0] → product = 1.0
        assert result_dict['20240101'] == pytest.approx(1.0)
        
        # 20240102: future = [1.0, 2.0, 1.0] → product = 2.0
        assert result_dict['20240102'] == pytest.approx(2.0)
        
        # 20240103: future = [2.0, 1.0] → product = 2.0
        assert result_dict['20240103'] == pytest.approx(2.0)
        
        # 20240104: future = [1.0] → product = 1.0
        assert result_dict['20240104'] == pytest.approx(1.0)
        
        # 20240105: future = [] → product = 1.0
        assert result_dict['20240105'] == pytest.approx(1.0)
    
    def test_multiple_symbols(self):
        """Test multiple symbols processed independently."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK_A', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK_A', 'adj_factor': 0.5},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'STOCK_B', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'STOCK_B', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        
        # Separate by symbol
        stock_a = {row['TRD_DD']: row['cum_adj_multiplier'] 
                   for row in result if row['ISU_SRT_CD'] == 'STOCK_A'}
        stock_b = {row['TRD_DD']: row['cum_adj_multiplier'] 
                   for row in result if row['ISU_SRT_CD'] == 'STOCK_B'}
        
        # STOCK_A has split
        assert stock_a['20240101'] == pytest.approx(0.5)
        assert stock_a['20240102'] == pytest.approx(1.0)
        
        # STOCK_B has no split
        assert stock_b['20240101'] == pytest.approx(1.0)
        assert stock_b['20240102'] == pytest.approx(1.0)
    
    def test_empty_input(self):
        """Test empty input returns empty result."""
        result = compute_cumulative_adjustments([])
        assert result == []
    
    def test_none_and_empty_string_factors(self):
        """Test handling of None and empty string adj_factors."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': None},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': ''},
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 'None'},
            {'TRD_DD': '20240104', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        
        # All should be treated as 1.0 (no adjustment)
        for row in result:
            assert row['cum_adj_multiplier'] == pytest.approx(1.0)
    
    def test_precision_float64(self):
        """Test precision is maintained with float64."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 0.333333333333},
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        result_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in result}
        
        # Check precision is maintained (at least 1e-6)
        assert isinstance(result_dict['20240101'], float)
        assert result_dict['20240101'] == pytest.approx(0.333333333333, abs=1e-6)
    
    def test_date_ordering(self):
        """Test that dates are correctly ordered even if input is unsorted."""
        factors = [
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 0.5},
            {'TRD_DD': '20240104', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        result_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in result}
        
        # Should be sorted internally and computed correctly
        assert result_dict['20240101'] == pytest.approx(0.5)
        assert result_dict['20240102'] == pytest.approx(1.0)
        assert result_dict['20240103'] == pytest.approx(1.0)
        assert result_dict['20240104'] == pytest.approx(1.0)
    
    def test_missing_symbol_handled(self):
        """Test that rows with missing ISU_SRT_CD are skipped."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': None, 'adj_factor': 1.0},  # Should skip
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        
        # Should have 2 results (skipped None symbol)
        assert len(result) == 2
        assert all(row['ISU_SRT_CD'] == 'TEST01' for row in result)


class TestCumulativeAdjustmentsPriceApplication:
    """Test applying cumulative adjustments to actual prices."""
    
    def test_samsung_split_price_continuity(self):
        """Test that Samsung split prices are continuous after adjustment."""
        # Raw prices
        prices = {
            '20180503': 2_650_000,  # Pre-split
            '20180504': 51_900,     # Split day (already post-split scale)
        }
        
        # Cumulative adjustments
        cum_adj = {
            '20180503': 0.02,
            '20180504': 1.0,
        }
        
        # Apply adjustments
        adjusted_prices = {
            date: int(round(price * cum_adj[date]))
            for date, price in prices.items()
        }
        
        # Check continuity
        pre_split_adj = adjusted_prices['20180503']  # 53,000
        split_day_adj = adjusted_prices['20180504']  # 51,900
        
        assert pre_split_adj == 53_000
        assert split_day_adj == 51_900
        
        # Price change should be reasonable (< 10%)
        pct_change = abs(split_day_adj - pre_split_adj) / pre_split_adj * 100
        assert pct_change < 10, f"Discontinuity: {pct_change:.2f}%"
    
    def test_price_adjustment_preserves_int(self):
        """Test that adjusted prices are properly rounded to integers (Korean Won)."""
        prices = [1_000_000, 2_500_000, 500_000]
        cum_adj = 0.02
        
        adjusted = [int(round(price * cum_adj)) for price in prices]
        
        assert adjusted == [20_000, 50_000, 10_000]
        assert all(isinstance(p, int) for p in adjusted)


class TestCumulativeAdjustmentsEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_extreme_split_ratio(self):
        """Test extreme split ratio (100:1)."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 0.01},  # 100:1
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 1.0},
        ]
        
        result = compute_cumulative_adjustments(factors)
        result_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in result}
        
        assert result_dict['20240101'] == pytest.approx(0.01, abs=1e-6)
        assert result_dict['20240102'] == pytest.approx(1.0, abs=1e-6)
        assert result_dict['20240103'] == pytest.approx(1.0, abs=1e-6)
    
    def test_single_date(self):
        """Test single date (no future factors)."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': 0.5},
        ]
        
        result = compute_cumulative_adjustments(factors)
        
        assert len(result) == 1
        # Single date has no future factors, so cum_adj = 1.0
        assert result[0]['cum_adj_multiplier'] == pytest.approx(1.0)
    
    def test_string_adj_factor(self):
        """Test adj_factor as string (from CSV/database)."""
        factors = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': 'TEST01', 'adj_factor': '1.0'},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': 'TEST01', 'adj_factor': '0.5'},
            {'TRD_DD': '20240103', 'ISU_SRT_CD': 'TEST01', 'adj_factor': '1.0'},
        ]
        
        result = compute_cumulative_adjustments(factors)
        result_dict = {row['TRD_DD']: row['cum_adj_multiplier'] for row in result}
        
        # Should handle string conversion
        assert result_dict['20240101'] == pytest.approx(0.5)
        assert result_dict['20240102'] == pytest.approx(1.0)

