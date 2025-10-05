"""
Unit tests for FieldMapper service

Tests field name resolution and adjustment support checks.
Uses config/fields.yaml configuration.
"""

import pytest
from pathlib import Path

from krx_quant_dataloader.apis.field_mapper import FieldMapper, FieldMapping


@pytest.mark.unit
class TestFieldMapperLoading:
    """Test FieldMapper loading from YAML configuration."""
    
    def test_loads_from_yaml(self):
        """Test that FieldMapper loads successfully from config."""
        mapper = FieldMapper.from_yaml('config/fields.yaml')
        
        assert mapper is not None
        assert len(mapper.list_fields()) > 0
    
    def test_fails_on_missing_config(self):
        """Test that loading fails gracefully if config missing."""
        with pytest.raises(FileNotFoundError, match="Field config not found"):
            FieldMapper.from_yaml('config/nonexistent.yaml')
    
    def test_validates_config_structure(self, tmp_path):
        """Test that invalid config raises ValueError."""
        # Empty config
        invalid_config = tmp_path / "invalid.yaml"
        invalid_config.write_text("version: '1.0'\n")
        
        with pytest.raises(ValueError, match="missing 'fields' section"):
            FieldMapper.from_yaml(invalid_config)
    
    def test_validates_field_definition(self, tmp_path):
        """Test that field definitions are validated."""
        # Missing required keys
        invalid_config = tmp_path / "invalid.yaml"
        invalid_config.write_text("""
fields:
  close:
    table: snapshots
    # Missing 'column' and 'supports_adjustment'
""")
        
        with pytest.raises(ValueError, match="missing required keys"):
            FieldMapper.from_yaml(invalid_config)


@pytest.mark.unit
class TestFieldResolution:
    """Test field name resolution to storage locations."""
    
    @pytest.fixture
    def mapper(self):
        """Load FieldMapper from config."""
        return FieldMapper.from_yaml('config/fields.yaml')
    
    def test_resolves_close_field(self, mapper):
        """Test resolution of 'close' field."""
        mapping = mapper.resolve('close')
        
        assert mapping.table == 'snapshots'
        assert mapping.column == 'TDD_CLSPRC'
        assert mapping.is_original is True
    
    def test_resolves_volume_field(self, mapper):
        """Test resolution of 'volume' field."""
        mapping = mapper.resolve('volume')
        
        assert mapping.table == 'snapshots'
        assert mapping.column == 'ACC_TRDVOL'
        assert mapping.is_original is True
    
    def test_resolves_liquidity_rank_field(self, mapper):
        """Test resolution of cross-table field (liquidity_ranks)."""
        mapping = mapper.resolve('liquidity_rank')
        
        assert mapping.table == 'liquidity_ranks'
        assert mapping.column == 'xs_liquidity_rank'
        assert mapping.is_original is False
    
    def test_resolves_adj_factor_field(self, mapper):
        """Test resolution of adjustment factor field."""
        mapping = mapper.resolve('adj_factor')
        
        assert mapping.table == 'adj_factors'
        assert mapping.column == 'adj_factor'
        assert mapping.is_original is False
    
    def test_raises_on_unknown_field(self, mapper):
        """Test that unknown field names raise ValueError with suggestions."""
        with pytest.raises(ValueError, match="Unknown field: 'nonexistent'"):
            mapper.resolve('nonexistent')
        
        # Error message should include available fields
        with pytest.raises(ValueError, match="Available fields"):
            mapper.resolve('invalid_field')
    
    def test_returns_field_mapping_namedtuple(self, mapper):
        """Test that resolve() returns FieldMapping NamedTuple."""
        mapping = mapper.resolve('close')
        
        assert isinstance(mapping, FieldMapping)
        assert hasattr(mapping, 'table')
        assert hasattr(mapping, 'column')
        assert hasattr(mapping, 'is_original')
        assert hasattr(mapping, 'description')


@pytest.mark.unit
class TestOriginalVsDerived:
    """Test is_original checks for different field types."""
    
    @pytest.fixture
    def mapper(self):
        """Load FieldMapper from config."""
        return FieldMapper.from_yaml('config/fields.yaml')
    
    def test_snapshot_fields_are_original(self, mapper):
        """Test that snapshot fields are marked as original."""
        snapshot_fields = ['close', 'base_price', 'change', 'volume', 'value', 'name']
        
        for field in snapshot_fields:
            assert mapper.is_original(field) is True, \
                f"Snapshot field '{field}' should be original (from KRX API)"
    
    def test_computed_fields_are_not_original(self, mapper):
        """Test that computed/derived fields are not marked as original."""
        derived_fields = ['liquidity_rank', 'adj_factor']
        
        for field in derived_fields:
            assert mapper.is_original(field) is False, \
                f"Derived field '{field}' should not be original (computed post-ingestion)"
    
    def test_raises_on_unknown_field_in_original_check(self, mapper):
        """Test that is_original() raises on unknown fields."""
        with pytest.raises(ValueError, match="Unknown field"):
            mapper.is_original('nonexistent_field')


@pytest.mark.unit
class TestFieldListing:
    """Test field listing utilities."""
    
    @pytest.fixture
    def mapper(self):
        """Load FieldMapper from config."""
        return FieldMapper.from_yaml('config/fields.yaml')
    
    def test_lists_all_fields(self, mapper):
        """Test that list_fields() returns all available fields."""
        fields = mapper.list_fields()
        
        assert isinstance(fields, list)
        assert len(fields) > 0
        assert 'close' in fields
        assert 'volume' in fields
        assert 'liquidity_rank' in fields
        
        # Should be sorted
        assert fields == sorted(fields)
    
    def test_lists_original_fields_only(self, mapper):
        """Test that list_original_fields() returns only KRX API fields."""
        original = mapper.list_original_fields()
        
        assert isinstance(original, list)
        assert 'close' in original
        assert 'volume' in original
        
        # Derived fields should NOT be in list
        assert 'liquidity_rank' not in original
        assert 'adj_factor' not in original
        
        # Should be sorted
        assert original == sorted(original)
    
    def test_lists_derived_fields_only(self, mapper):
        """Test that list_derived_fields() returns only computed fields."""
        derived = mapper.list_derived_fields()
        
        assert isinstance(derived, list)
        assert 'liquidity_rank' in derived
        assert 'adj_factor' in derived
        
        # Original fields should NOT be in list
        assert 'close' not in derived
        assert 'volume' not in derived
        
        # Should be sorted
        assert derived == sorted(derived)
    
    def test_original_and_derived_partition_all_fields(self, mapper):
        """Test that original + derived = all fields."""
        all_fields = set(mapper.list_fields())
        original = set(mapper.list_original_fields())
        derived = set(mapper.list_derived_fields())
        
        # No overlap
        assert original.isdisjoint(derived)
        
        # Union equals all
        assert original.union(derived) == all_fields


@pytest.mark.unit
class TestFieldMappingMetadata:
    """Test FieldMapping metadata (descriptions, etc.)."""
    
    @pytest.fixture
    def mapper(self):
        """Load FieldMapper from config."""
        return FieldMapper.from_yaml('config/fields.yaml')
    
    def test_mapping_includes_description(self, mapper):
        """Test that field mappings include descriptions."""
        mapping = mapper.resolve('close')
        
        assert mapping.description is not None
        assert len(mapping.description) > 0
        assert isinstance(mapping.description, str)
    
    def test_all_fields_have_descriptions(self, mapper):
        """Test that all fields have descriptions in config."""
        for field_name in mapper.list_fields():
            mapping = mapper.resolve(field_name)
            assert mapping.description is not None, \
                f"Field '{field_name}' missing description"


@pytest.mark.unit
class TestFieldMapperEdgeCases:
    """Test edge cases and error handling."""
    
    def test_case_sensitive_field_names(self):
        """Test that field names are case-sensitive."""
        mapper = FieldMapper.from_yaml('config/fields.yaml')
        
        # 'close' should work
        mapper.resolve('close')
        
        # 'Close' should fail
        with pytest.raises(ValueError):
            mapper.resolve('Close')
    
    def test_empty_field_name(self):
        """Test that empty field name raises ValueError."""
        mapper = FieldMapper.from_yaml('config/fields.yaml')
        
        with pytest.raises(ValueError):
            mapper.resolve('')
    
    def test_whitespace_in_field_name(self):
        """Test that whitespace in field name fails."""
        mapper = FieldMapper.from_yaml('config/fields.yaml')
        
        with pytest.raises(ValueError):
            mapper.resolve(' close ')

