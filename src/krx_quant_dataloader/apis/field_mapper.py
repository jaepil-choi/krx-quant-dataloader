"""
Field mapper service

What this module does:
- Maps user-facing field names to storage locations (table + column)
- Loads field definitions from config/fields.yaml
- Determines which fields support corporate action adjustments

Service Layer (Layer 2):
- Used by QueryEngine to resolve field queries
- Pure configuration + simple lookups (no I/O)
- Validates field names at query time

Design:
- Config-driven (fields.yaml) instead of hardcoded mappings
- NamedTuple for type-safe field metadata
- Simple dict lookup (O(1) performance)
"""

from __future__ import annotations

from typing import NamedTuple, Dict, Optional
from pathlib import Path
import yaml


class FieldMapping(NamedTuple):
    """
    Field mapping metadata.
    
    Attributes:
        table: Parquet table name ('snapshots', 'adj_factors', 'liquidity_ranks')
        column: Actual column name in Parquet
        is_original: True if field comes directly from KRX API, False if derived/computed
        description: Human-readable description (for documentation)
    """
    table: str
    column: str
    is_original: bool
    description: Optional[str] = None


class FieldMapper:
    """
    Map user-facing field names to storage locations.
    
    Loads field definitions from config/fields.yaml and provides
    fast lookup for field resolution and adjustment support checks.
    
    Examples:
        >>> mapper = FieldMapper.from_yaml('config/fields.yaml')
        >>> mapping = mapper.resolve('close')
        >>> mapping.table, mapping.column
        ('snapshots', 'TDD_CLSPRC')
        >>> mapper.supports_adjustment('close')
        True
        >>> mapper.supports_adjustment('volume')
        False
    """
    
    def __init__(self, mappings: Dict[str, FieldMapping]):
        """
        Initialize with pre-loaded field mappings.
        
        Parameters:
            mappings: Dict mapping field names to FieldMapping objects
        """
        self._mappings = mappings
    
    @classmethod
    def from_yaml(cls, config_path: str | Path) -> FieldMapper:
        """
        Load field mappings from YAML configuration.
        
        Parameters:
            config_path: Path to fields.yaml configuration file
            
        Returns:
            FieldMapper instance with loaded mappings
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config format is invalid
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Field config not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config or 'fields' not in config:
            raise ValueError(f"Invalid field config: missing 'fields' section in {config_path}")
        
        # Parse field definitions
        mappings = {}
        for field_name, field_def in config['fields'].items():
            if not isinstance(field_def, dict):
                raise ValueError(f"Invalid field definition for '{field_name}': expected dict")
            
            # Validate required keys
            required_keys = ['table', 'column', 'is_original']
            missing = [k for k in required_keys if k not in field_def]
            if missing:
                raise ValueError(f"Field '{field_name}' missing required keys: {missing}")
            
            # Create FieldMapping
            mappings[field_name] = FieldMapping(
                table=field_def['table'],
                column=field_def['column'],
                is_original=field_def['is_original'],
                description=field_def.get('description'),
            )
        
        return cls(mappings)
    
    def resolve(self, field_name: str) -> FieldMapping:
        """
        Resolve field name to storage location.
        
        Parameters:
            field_name: User-facing field name (e.g., 'close', 'volume')
            
        Returns:
            FieldMapping with table, column, and adjustment support info
            
        Raises:
            ValueError: If field name is unknown
            
        Examples:
            >>> mapper.resolve('close')
            FieldMapping(table='snapshots', column='TDD_CLSPRC', is_original=True, ...)
            >>> mapper.resolve('liquidity_rank')
            FieldMapping(table='liquidity_ranks', column='xs_liquidity_rank', is_original=False, ...)
        """
        if field_name not in self._mappings:
            available = sorted(self._mappings.keys())
            raise ValueError(
                f"Unknown field: '{field_name}'. "
                f"Available fields: {', '.join(available)}"
            )
        
        return self._mappings[field_name]
    
    def is_original(self, field_name: str) -> bool:
        """
        Check if field comes directly from KRX API (original) or is derived/computed.
        
        Original fields: close, volume, name, etc. (from snapshots table)
        Derived fields: liquidity_rank, adj_factor, etc. (computed post-ingestion)
        
        Parameters:
            field_name: User-facing field name
            
        Returns:
            True if field is original from KRX API, False if derived/computed
            
        Raises:
            ValueError: If field name is unknown
            
        Examples:
            >>> mapper.is_original('close')
            True
            >>> mapper.is_original('liquidity_rank')
            False
        """
        mapping = self.resolve(field_name)  # Raises if unknown
        return mapping.is_original
    
    def list_fields(self) -> list[str]:
        """
        List all available field names.
        
        Returns:
            Sorted list of field names
        """
        return sorted(self._mappings.keys())
    
    def list_original_fields(self) -> list[str]:
        """
        List fields that come directly from KRX API (not derived).
        
        Returns:
            Sorted list of original field names
        """
        return sorted([
            name for name, mapping in self._mappings.items()
            if mapping.is_original
        ])
    
    def list_derived_fields(self) -> list[str]:
        """
        List fields that are computed/derived (not from KRX API).
        
        Returns:
            Sorted list of derived field names
        """
        return sorted([
            name for name, mapping in self._mappings.items()
            if not mapping.is_original
        ])


__all__ = [
    'FieldMapping',
    'FieldMapper',
]

