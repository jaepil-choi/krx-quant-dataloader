"""
Factory functions for creating fully configured instances.

This module provides composition roots for building complex objects
from their dependencies (Config → Transport → Orchestrator → Client).

Design principles:
- Centralized composition (avoids duplicate setup code)
- Dependency injection (explicit configuration)
- Testability (easy to mock for unit tests)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional


def create_raw_client(settings_path: Optional[Path] = None):
    """
    Factory for creating fully configured RawClient.
    
    Builds the complete Layer 1 stack:
    Config → AdapterRegistry → Transport → Orchestrator → RawClient
    
    Parameters
    ----------
    settings_path : Optional[Path]
        Path to settings.yaml (default: config/settings.yaml in project root)
        ConfigFacade will use this to find endpoints.yaml
    
    Returns
    -------
    RawClient
        Fully configured raw client ready for API calls
    
    Examples
    --------
    >>> client = create_raw_client()
    >>> data = client.call('stock.all_change_rates', strtDd='20240101', endDd='20240101')
    
    >>> # With custom config
    >>> client = create_raw_client(settings_path=Path('my_config/settings.yaml'))
    """
    from .config import ConfigFacade
    from .adapter import AdapterRegistry
    from .transport import Transport
    from .orchestration import Orchestrator
    from .client import RawClient
    
    # Load config facade (handles finding endpoints.yaml via settings.yaml)
    config = ConfigFacade.load(settings_path=settings_path)
    
    # Build Layer 1 stack
    # AdapterRegistry loads from endpoints.yaml path provided by ConfigFacade
    registry = AdapterRegistry.load(config_path=config.endpoints_yaml_path)
    transport = Transport(config=config)
    orchestrator = Orchestrator(transport=transport)
    client = RawClient(registry=registry, orchestrator=orchestrator)
    
    return client

