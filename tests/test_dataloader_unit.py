"""
Unit tests for DataLoader

Tests the DataLoader class with mocked dependencies to verify:
- Initialization logic (3-stage pipeline)
- Query execution (field resolution, filtering, pivoting)
- Universe filtering (string vs list)
- Adjustment application
- Error handling

Note: These are pure unit tests with mocked storage/pipelines.
Live smoke tests are in test_dataloader_live_smoke.py.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from krx_quant_dataloader.apis.dataloader import DataLoader


@pytest.mark.unit
class TestDataLoaderInitialization:
    """Test DataLoader initialization and 3-stage pipeline."""
    
    @patch('krx_quant_dataloader.apis.dataloader.DataLoader._run_pipeline')
    @patch('krx_quant_dataloader.apis.dataloader.FieldMapper')
    def test_init_with_existing_data(self, mock_mapper, mock_pipeline):
        """Test initialization when all data already exists (no ingestion needed)."""
        # Mock pipeline to do nothing
        mock_pipeline.return_value = None
        
        loader = DataLoader(
            db_path='data/test_db',
            start_date='20240101',
            end_date='20240103'
        )
        
        # Verify paths set correctly
        assert loader._db_path == Path('data/test_db')
        assert loader._start_date == '20240101'
        assert loader._end_date == '20240103'
        
        # Verify pipeline was called
        mock_pipeline.assert_called_once()
        
        # Verify FieldMapper initialized
        mock_mapper.from_yaml.assert_called_once()
    
    @patch('krx_quant_dataloader.apis.dataloader.FieldMapper')
    def test_init_validates_date_range(self, mock_mapper):
        """Test that initialization validates start_date <= end_date."""
        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            DataLoader(
                db_path='data/test_db',
                start_date='20240103',
                end_date='20240101'  # Invalid: before start
            )
    
    @patch('krx_quant_dataloader.apis.dataloader.DataLoader._run_pipeline')
    @patch('krx_quant_dataloader.apis.dataloader.FieldMapper')
    def test_init_creates_temp_directory(self, mock_mapper, mock_pipeline):
        """Test that initialization creates temp directory for ephemeral cache."""
        mock_pipeline.return_value = None
        
        loader = DataLoader(
            db_path='data/test_db',
            start_date='20240101',
            end_date='20240101',
            temp_path='data/test_temp'
        )
        
        # Verify temp directory set
        assert loader._temp_path == Path('data/test_temp')


@pytest.mark.unit
class TestDataLoaderQuery:
    """Test DataLoader.get_data() query execution."""
    
    @pytest.fixture
    def mock_loader(self):
        """Create a DataLoader with mocked dependencies."""
        with patch('krx_quant_dataloader.apis.dataloader.DataLoader._run_pipeline'):
            with patch('krx_quant_dataloader.apis.dataloader.FieldMapper'):
                loader = DataLoader(
                    db_path='data/test_db',
                    start_date='20240101',
                    end_date='20240103'
                )
                return loader
    
    def test_get_data_resolves_field_name(self, mock_loader):
        """Test that get_data() resolves field name via FieldMapper."""
        # Mock FieldMapper.resolve()
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots',
            column='TDD_CLSPRC',
            is_original=True,
            description='Close price'
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101'],
                'ISU_SRT_CD': ['005930'],
                'TDD_CLSPRC': [70000],
            })
            
            result = mock_loader.get_data('close', universe=None, adjusted=False)
            
            # Verify field resolution
            mock_loader._field_mapper.resolve.assert_called_with('close')
    
    def test_get_data_queries_correct_table(self, mock_loader):
        """Test that get_data() queries the correct table based on field mapping."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader._field_mapper.resolve.return_value = FieldMapping(
            table='liquidity_ranks',
            column='xs_liquidity_rank',
            is_original=False,
            description='Liquidity rank'
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101'],
                'ISU_SRT_CD': ['005930'],
                'xs_liquidity_rank': [1],
            })
            
            result = mock_loader.get_data('liquidity_rank', universe=None, adjusted=False)
            
            # Verify correct table queried (access positional args, not kwargs)
            mock_query.assert_called()
            call_args = mock_query.call_args
            # query_parquet_table(db_path, table_name, ...)
            # First positional arg is db_path, second is table_name
            assert call_args[0][1] == 'liquidity_ranks'  # Second positional arg
    
    def test_get_data_validates_query_range(self, mock_loader):
        """Test that get_data() validates query range is within loader's date range."""
        with pytest.raises(ValueError, match="Query range .* outside loader window"):
            mock_loader.get_data(
                'close',
                query_start='20231201',  # Before loader's start_date
                query_end='20240102'
            )
        
        with pytest.raises(ValueError, match="Query range .* outside loader window"):
            mock_loader.get_data(
                'close',
                query_start='20240102',
                query_end='20240105'  # After loader's end_date
            )
    
    def test_get_data_unknown_field_raises_error(self, mock_loader):
        """Test that get_data() raises error for unknown field names."""
        mock_loader._field_mapper.resolve.side_effect = ValueError("Unknown field: 'nonexistent'")
        
        with pytest.raises(ValueError, match="Unknown field"):
            mock_loader.get_data('nonexistent')


@pytest.mark.unit
class TestDataLoaderUniverseFiltering:
    """Test universe filtering (string vs list)."""
    
    @pytest.fixture
    def mock_loader_with_data(self):
        """Create loader with mocked query returning sample data."""
        with patch('krx_quant_dataloader.apis.dataloader.DataLoader._run_pipeline'):
            with patch('krx_quant_dataloader.apis.dataloader.FieldMapper'):
                loader = DataLoader(
                    db_path='data/test_db',
                    start_date='20240101',
                    end_date='20240101'
                )
                return loader
    
    def test_universe_string_queries_universe_table(self, mock_loader_with_data):
        """Test that string universe (e.g., 'univ100') queries universes table."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader_with_data._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            # Mock TWO queries: first for snapshots, second for universes
            mock_query.side_effect = [
                # First call: query snapshots table for 'close' prices
                pd.DataFrame({
                    'TRD_DD': ['20240101'] * 3,
                    'ISU_SRT_CD': ['005930', '000660', '035720'],
                    'TDD_CLSPRC': [70000, 150000, 400000],
                }),
                # Second call: query universes table for 'univ100' members
                pd.DataFrame({
                    'TRD_DD': ['20240101'] * 2,
                    'ISU_SRT_CD': ['005930', '000660'],
                    'univ100': [1, 1],  # Boolean flags indicating membership
                })
            ]
            
            result = mock_loader_with_data.get_data('close', universe='univ100', adjusted=False)
            
            # Verify universes table was queried (second call)
            calls = mock_query.call_args_list
            assert len(calls) == 2, "Should query snapshots and universes tables"
            # Second call should be to universes table
            assert calls[1][0][1] == 'universes'  # Second positional arg of second call
    
    def test_universe_list_uses_fixed_symbols(self, mock_loader_with_data):
        """Test that list universe uses fixed symbol list for all dates."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader_with_data._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101'] * 3,
                'ISU_SRT_CD': ['005930', '000660', '035720'],
                'TDD_CLSPRC': [70000, 150000, 400000],
            })
            
            result = mock_loader_with_data.get_data(
                'close',
                universe=['005930', '000660'],  # Explicit list
                adjusted=False
            )
            
            # Verify only specified symbols in result
            assert set(result.columns) <= {'005930', '000660'}
    
    def test_universe_none_returns_all_symbols(self, mock_loader_with_data):
        """Test that universe=None returns all symbols."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader_with_data._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101'] * 3,
                'ISU_SRT_CD': ['005930', '000660', '035720'],
                'TDD_CLSPRC': [70000, 150000, 400000],
            })
            
            result = mock_loader_with_data.get_data('close', universe=None, adjusted=False)
            
            # Verify all symbols present
            assert len(result.columns) == 3


@pytest.mark.unit
class TestDataLoaderAdjustment:
    """Test adjustment application logic."""
    
    @pytest.fixture
    def mock_loader(self):
        with patch('krx_quant_dataloader.apis.dataloader.DataLoader._run_pipeline'):
            with patch('krx_quant_dataloader.apis.dataloader.FieldMapper'):
                return DataLoader('data/test_db', start_date='20240101', end_date='20240101')
    
    def test_adjusted_true_queries_cumulative_adjustments(self, mock_loader):
        """Test that adjusted=True queries cumulative adjustments from temp cache."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            # Mock snapshots and cumulative adjustments queries
            mock_query.side_effect = [
                # Snapshots
                pd.DataFrame({
                    'TRD_DD': ['20240101'],
                    'ISU_SRT_CD': ['005930'],
                    'TDD_CLSPRC': [70000],
                }),
                # Cumulative adjustments
                pd.DataFrame({
                    'TRD_DD': ['20240101'],
                    'ISU_SRT_CD': ['005930'],
                    'cum_adj_multiplier': [0.5],
                })
            ]
            
            result = mock_loader.get_data('close', adjusted=True)
            
            # Verify cumulative_adjustments table was queried
            calls = mock_query.call_args_list
            assert len(calls) >= 2  # At least snapshots + cumulative_adjustments
    
    def test_adjusted_false_skips_cumulative_adjustments(self, mock_loader):
        """Test that adjusted=False does NOT query cumulative adjustments."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101'],
                'ISU_SRT_CD': ['005930'],
                'TDD_CLSPRC': [70000],
            })
            
            result = mock_loader.get_data('close', adjusted=False)
            
            # Verify cumulative_adjustments NOT queried
            calls = mock_query.call_args_list
            # Extract table names from positional args (second arg is table_name)
            table_names = [call[0][1] for call in calls if len(call[0]) > 1]
            assert 'cumulative_adjustments' not in table_names


@pytest.mark.unit
class TestDataLoaderOutputFormat:
    """Test wide-format output (dates × symbols)."""
    
    @pytest.fixture
    def mock_loader(self):
        with patch('krx_quant_dataloader.apis.dataloader.DataLoader._run_pipeline'):
            with patch('krx_quant_dataloader.apis.dataloader.FieldMapper'):
                return DataLoader('data/test_db', start_date='20240101', end_date='20240102')
    
    def test_output_is_wide_format(self, mock_loader):
        """Test that get_data() returns wide-format DataFrame (dates × symbols)."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101', '20240101', '20240102', '20240102'],
                'ISU_SRT_CD': ['005930', '000660', '005930', '000660'],
                'TDD_CLSPRC': [70000, 150000, 71000, 151000],
            })
            
            result = mock_loader.get_data('close', adjusted=False)
            
            # Verify wide format structure
            assert isinstance(result, pd.DataFrame)
            assert result.index.name == 'TRD_DD' or 'TRD_DD' in str(result.index.name)
            assert len(result.columns) == 2  # Two symbols
            assert '005930' in result.columns
            assert '000660' in result.columns
    
    def test_output_index_is_dates(self, mock_loader):
        """Test that output index is dates (TRD_DD)."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101', '20240102'],
                'ISU_SRT_CD': ['005930', '005930'],
                'TDD_CLSPRC': [70000, 71000],
            })
            
            result = mock_loader.get_data('close', adjusted=False)
            
            # Verify dates in index
            assert len(result.index) == 2
            assert '20240101' in result.index or 20240101 in result.index
    
    def test_output_columns_are_symbols(self, mock_loader):
        """Test that output columns are symbols (ISU_SRT_CD)."""
        from krx_quant_dataloader.apis.field_mapper import FieldMapping
        mock_loader._field_mapper.resolve.return_value = FieldMapping(
            table='snapshots', column='TDD_CLSPRC', is_original=True, description=''
        )
        
        with patch('krx_quant_dataloader.apis.dataloader.query_parquet_table') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'TRD_DD': ['20240101', '20240101', '20240101'],
                'ISU_SRT_CD': ['005930', '000660', '035720'],
                'TDD_CLSPRC': [70000, 150000, 400000],
            })
            
            result = mock_loader.get_data('close', adjusted=False)
            
            # Verify symbols in columns
            assert len(result.columns) == 3
            assert set(result.columns) == {'005930', '000660', '035720'}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

