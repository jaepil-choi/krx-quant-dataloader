"""
Unit tests for universes schema and writer

Tests the UNIVERSES_SCHEMA and ParquetSnapshotWriter.write_universes() method.
Uses synthetic data and temporary directories.
"""

import pytest
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


@pytest.mark.unit
class TestUniversesSchema:
    """Test UNIVERSES_SCHEMA definition and validation."""
    
    def test_universes_schema_defined(self):
        """Test UNIVERSES_SCHEMA is exported from schema module."""
        from krx_quant_dataloader.storage.schema import UNIVERSES_SCHEMA
        
        assert UNIVERSES_SCHEMA is not None
        assert isinstance(UNIVERSES_SCHEMA, pa.Schema)
    
    def test_universes_schema_has_required_fields(self):
        """Test UNIVERSES_SCHEMA contains expected fields."""
        from krx_quant_dataloader.storage.schema import UNIVERSES_SCHEMA
        
        field_names = UNIVERSES_SCHEMA.names
        
        # Required fields
        assert 'ISU_SRT_CD' in field_names
        assert 'universe_name' in field_names
        assert 'xs_liquidity_rank' in field_names
    
    def test_universes_schema_field_types(self):
        """Test UNIVERSES_SCHEMA field types are correct."""
        from krx_quant_dataloader.storage.schema import UNIVERSES_SCHEMA
        
        # ISU_SRT_CD should be string
        assert UNIVERSES_SCHEMA.field('ISU_SRT_CD').type == pa.string()
        
        # universe_name should be string
        assert UNIVERSES_SCHEMA.field('universe_name').type == pa.string()
        
        # xs_liquidity_rank should be int32
        assert UNIVERSES_SCHEMA.field('xs_liquidity_rank').type == pa.int32()
    
    def test_universes_schema_accepts_valid_data(self):
        """Test UNIVERSES_SCHEMA validates correctly with valid data."""
        from krx_quant_dataloader.storage.schema import UNIVERSES_SCHEMA
        
        # Valid universe membership rows (no TRD_DD - it's partition key)
        valid_rows = [
            {'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
            {'ISU_SRT_CD': '000660', 'universe_name': 'univ100', 'xs_liquidity_rank': 2},
            {'ISU_SRT_CD': '068270', 'universe_name': 'univ500', 'xs_liquidity_rank': 150},
        ]
        
        # Should not raise
        table = pa.Table.from_pylist(valid_rows, schema=UNIVERSES_SCHEMA)
        
        assert len(table) == 3
        assert table.num_columns == 3


@pytest.mark.unit
class TestParquetWriterUniverses:
    """Test ParquetSnapshotWriter.write_universes() method."""
    
    def test_write_universes_creates_partition_directory(self, tmp_path):
        """Test write_universes() creates TRD_DD partition directory."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        rows = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '000660', 'universe_name': 'univ100', 'xs_liquidity_rank': 2},
        ]
        
        count = writer.write_universes(rows, date='20240101')
        
        assert count == 2
        
        # Check directory structure
        partition_dir = tmp_path / 'universes' / 'TRD_DD=20240101'
        assert partition_dir.exists()
        assert (partition_dir / 'data.parquet').exists()
    
    def test_write_universes_creates_hive_partition_structure(self, tmp_path):
        """Test write_universes() uses Hive partitioning (TRD_DD in directory path)."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        rows = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
            {'TRD_DD': '20240102', 'ISU_SRT_CD': '000660', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
        ]
        
        writer.write_universes([rows[0]], date='20240101')
        writer.write_universes([rows[1]], date='20240102')
        
        # Verify Hive partitioning: TRD_DD is in directory name
        partition_20240101 = tmp_path / 'universes' / 'TRD_DD=20240101'
        partition_20240102 = tmp_path / 'universes' / 'TRD_DD=20240102'
        
        assert partition_20240101.exists()
        assert partition_20240102.exists()
        assert (partition_20240101 / 'data.parquet').exists()
        assert (partition_20240102 / 'data.parquet').exists()
        
        # Read back using dataset API (properly handles partitioning)
        import pyarrow.dataset as ds
        dataset = ds.dataset(tmp_path / 'universes', format='parquet', partitioning='hive')
        table = dataset.to_table()
        
        # PyArrow injects partition columns when reading
        # This is expected behavior for Hive-partitioned datasets
        assert 'TRD_DD' in table.column_names
        assert len(table) == 2
    
    def test_write_universes_sorts_by_symbol(self, tmp_path):
        """Test write_universes() sorts rows by ISU_SRT_CD for row-group pruning."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        # Unsorted input
        rows = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '068270', 'universe_name': 'univ100', 'xs_liquidity_rank': 5},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '000660', 'universe_name': 'univ100', 'xs_liquidity_rank': 2},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
        ]
        
        writer.write_universes(rows, date='20240101')
        
        # Read back and verify sorted
        parquet_file = tmp_path / 'universes' / 'TRD_DD=20240101' / 'data.parquet'
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        
        # Should be sorted by ISU_SRT_CD
        assert df['ISU_SRT_CD'].iloc[0] == '000660'
        assert df['ISU_SRT_CD'].iloc[1] == '005930'
        assert df['ISU_SRT_CD'].iloc[2] == '068270'
    
    def test_write_universes_handles_empty_rows(self, tmp_path):
        """Test write_universes() handles empty input gracefully."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        count = writer.write_universes([], date='20240101')
        
        assert count == 0
        
        # Should not create partition directory for empty data
        partition_dir = tmp_path / 'universes' / 'TRD_DD=20240101'
        assert not partition_dir.exists()
    
    def test_write_universes_validates_date_mismatch(self, tmp_path):
        """Test write_universes() raises error if row date != expected date."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        rows = [
            {'TRD_DD': '20240102', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
        ]
        
        # Should raise ValueError due to date mismatch
        with pytest.raises(ValueError, match="Row date 20240102 != expected 20240101"):
            writer.write_universes(rows, date='20240101')
    
    def test_write_universes_handles_multiple_universes_per_stock(self, tmp_path):
        """Test write_universes() handles stocks in multiple universes."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        # Stock 005930 (rank 1) should be in univ100, univ500, univ1000
        rows = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ500', 'xs_liquidity_rank': 1},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ1000', 'xs_liquidity_rank': 1},
        ]
        
        count = writer.write_universes(rows, date='20240101')
        
        assert count == 3
        
        # Read back and verify
        parquet_file = tmp_path / 'universes' / 'TRD_DD=20240101' / 'data.parquet'
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        
        # All 3 rows for same stock
        assert len(df) == 3
        assert (df['ISU_SRT_CD'] == '005930').all()
        assert set(df['universe_name']) == {'univ100', 'univ500', 'univ1000'}
    
    def test_write_universes_preserves_rank_for_reference(self, tmp_path):
        """Test write_universes() preserves xs_liquidity_rank for reference."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        rows = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '000660', 'universe_name': 'univ100', 'xs_liquidity_rank': 2},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '068270', 'universe_name': 'univ500', 'xs_liquidity_rank': 150},
        ]
        
        writer.write_universes(rows, date='20240101')
        
        # Read back and verify ranks preserved
        parquet_file = tmp_path / 'universes' / 'TRD_DD=20240101' / 'data.parquet'
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        
        # Find each stock and check rank
        samsung = df[df['ISU_SRT_CD'] == '005930'].iloc[0]
        assert samsung['xs_liquidity_rank'] == 1
        
        sk_hynix = df[df['ISU_SRT_CD'] == '000660'].iloc[0]
        assert sk_hynix['xs_liquidity_rank'] == 2
        
        celltrion = df[df['ISU_SRT_CD'] == '068270'].iloc[0]
        assert celltrion['xs_liquidity_rank'] == 150
    
    def test_write_universes_idempotent_overwrites_partition(self, tmp_path):
        """Test write_universes() is idempotent (overwrites existing partition)."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        # First write
        rows_v1 = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
        ]
        writer.write_universes(rows_v1, date='20240101')
        
        # Second write (different data, same date)
        rows_v2 = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '000660', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '068270', 'universe_name': 'univ100', 'xs_liquidity_rank': 2},
        ]
        writer.write_universes(rows_v2, date='20240101')
        
        # Read back - should have v2 data only
        parquet_file = tmp_path / 'universes' / 'TRD_DD=20240101' / 'data.parquet'
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        
        assert len(df) == 2
        assert '005930' not in df['ISU_SRT_CD'].values
        assert '000660' in df['ISU_SRT_CD'].values
        assert '068270' in df['ISU_SRT_CD'].values


@pytest.mark.unit
class TestUniversesSchemaIntegration:
    """Integration tests for schema + writer."""
    
    def test_schema_writer_roundtrip(self, tmp_path):
        """Test full roundtrip: write with schema, read back, validate."""
        from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
        from krx_quant_dataloader.storage.schema import UNIVERSES_SCHEMA
        
        writer = ParquetSnapshotWriter(root_path=tmp_path)
        
        # Write with schema
        rows = [
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '005930', 'universe_name': 'univ100', 'xs_liquidity_rank': 1},
            {'TRD_DD': '20240101', 'ISU_SRT_CD': '000660', 'universe_name': 'univ500', 'xs_liquidity_rank': 50},
        ]
        
        writer.write_universes(rows, date='20240101')
        
        # Read back with schema validation
        parquet_file = tmp_path / 'universes' / 'TRD_DD=20240101' / 'data.parquet'
        table = pq.read_table(parquet_file, schema=UNIVERSES_SCHEMA)
        
        # Validate schema matches
        assert table.schema.equals(UNIVERSES_SCHEMA)
        
        # Validate data
        df = table.to_pandas()
        assert len(df) == 2
        assert df['ISU_SRT_CD'].dtype == 'object'  # string in pandas
        assert df['universe_name'].dtype == 'object'
        assert df['xs_liquidity_rank'].dtype == 'int32'

