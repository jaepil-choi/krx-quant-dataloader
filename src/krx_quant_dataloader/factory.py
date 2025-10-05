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


def create_raw_client(config_path: Optional[Path] = None):
    """
    Factory for creating fully configured RawClient.
    
    Builds the complete Layer 1 stack:
    Config → AdapterRegistry → Transport → Orchestrator → RawClient
    
    Parameters
    ----------
    config_path : Optional[Path]
        Path to config.yaml (default: config/config.yaml in project root)
    
    Returns
    -------
    RawClient
        Fully configured raw client ready for API calls
    
    Examples
    --------
    >>> client = create_raw_client()
    >>> data = client.call('stock.all_change_rates', strtDd='20240101', endDd='20240101')
    
    >>> # With custom config
    >>> client = create_raw_client(config_path=Path('my_config.yaml'))
    """
    from .config import ConfigFacade
    from .adapter import AdapterRegistry
    from .transport import Transport
    from .orchestration import Orchestrator
    from .client import RawClient
    
    # Default config path (project root / config / config.yaml)
    if config_path is None:
        # __file__ is in src/krx_quant_dataloader/factory.py
        # Go up 3 levels: factory.py -> krx_quant_dataloader -> src -> project_root
        project_root = Path(__file__).parent.parent.parent
        config_path = project_root / 'config' / 'config.yaml'
    
    # Build Layer 1 stack
    config = ConfigFacade.load(config_path=config_path)
    registry = AdapterRegistry.load(config_path=config_path)
    transport = Transport(config=config)
    orchestrator = Orchestrator(transport=transport)
    client = RawClient(registry=registry, orchestrator=orchestrator)
    
    return client

