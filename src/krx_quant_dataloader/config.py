from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

import yaml
from pydantic import BaseModel, ConfigDict, field_validator


class RetriesConfig(BaseModel):
    """Retry policy configuration."""

    model_config = ConfigDict(frozen=True)

    max_retries: int
    backoff_factor: float
    retry_statuses: List[int]


class RateLimitConfig(BaseModel):
    """Rate limit configuration (tokens per second style)."""

    model_config = ConfigDict(frozen=True)

    requests_per_second: float


class TransportConfig(BaseModel):
    """Transport settings used by the HTTP client."""

    model_config = ConfigDict(frozen=True)

    request_timeout_seconds: int
    connect_timeout_seconds: int
    retries: RetriesConfig
    rate_limit: RateLimitConfig
    http_version: str
    impersonate: Optional[str] = None
    verify_tls: bool = True

    @field_validator("http_version")
    @classmethod
    def validate_http_version(cls, v: str) -> str:
        allowed = {"1.1", "2"}
        if v not in allowed:
            raise ValueError(f"http_version must be one of {allowed}, got {v!r}")
        return v


class HostConfig(BaseModel):
    """Per-host configuration including base URL, headers, and transport."""

    model_config = ConfigDict(frozen=True)

    base_url: str
    default_path: str
    headers: Dict[str, str]
    transport: TransportConfig

    @field_validator("base_url")
    @classmethod
    def validate_https_base_url(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("base_url must use https scheme")
        return v


class ConfigFilesConfig(BaseModel):
    """Configuration file locations (from settings.yaml)."""

    model_config = ConfigDict(frozen=True)

    fields_yaml: str = "config/fields.yaml"
    endpoints_yaml: str = "config/endpoints.yaml"


class DataDirectoriesConfig(BaseModel):
    """Default data directory paths (from settings.yaml)."""

    model_config = ConfigDict(frozen=True)

    db_path: str = "data/krx_db"
    temp_path: str = "data/temp"
    data_root: str = "data/"


class SettingsConfig(BaseModel):
    """Main settings configuration (from config/settings.yaml)."""

    model_config = ConfigDict(frozen=True)

    version: int
    config: ConfigFilesConfig
    data: DataDirectoriesConfig


class EndpointsConfig(BaseModel):
    """Endpoints configuration (from config/endpoints.yaml)."""

    model_config = ConfigDict(frozen=True)

    version: int
    hosts: Dict[str, HostConfig]
    endpoints: Optional[Dict[str, Any]] = None


# Deprecated - kept for backward compatibility
class RootConfig(BaseModel):
    """Root configuration as loaded from YAML (DEPRECATED - use SettingsConfig or EndpointsConfig)."""

    model_config = ConfigDict(frozen=True)

    version: int
    hosts: Dict[str, HostConfig]
    # endpoints are not used by the current tests, but present in YAML
    endpoints: Optional[Dict[str, Any]] = None


class ConfigFacade:
    """
    Central configuration facade with defaults + overrides pattern.
    
    Loading hierarchy:
    1. Load config/settings.yaml (main settings)
    2. Use settings to find and load config/endpoints.yaml
    3. Provide unified interface for all configuration access
    
    Usage:
        # Simple (all defaults):
        config = ConfigFacade.load()
        
        # Custom settings file:
        config = ConfigFacade.load(settings_path='my_config/settings.yaml')
        
        # Access configuration:
        db_path = config.default_db_path
        fields_path = config.fields_yaml_path
    """

    def __init__(
        self,
        settings: SettingsConfig,
        hosts: Dict[str, HostConfig],
        endpoints: Dict[str, Any],
        project_root: Path,
    ):
        """
        Initialize ConfigFacade (use .load() class method instead).
        
        Parameters
        ----------
        settings : SettingsConfig
            Main settings from settings.yaml
        hosts : Dict[str, HostConfig]
            Host configurations from endpoints.yaml
        endpoints : Dict[str, Any]
            Endpoint definitions from endpoints.yaml
        project_root : Path
            Project root directory
        """
        self._settings = settings
        self._hosts = hosts
        self._endpoints = endpoints
        self._project_root = project_root

    @classmethod
    def load(
        cls,
        *,
        settings_path: Optional[str | Path] = None,
        project_root: Optional[Path] = None,
        env_prefix: Optional[str] = None,
    ) -> "ConfigFacade":
        """
        Load configuration from settings.yaml and referenced files.
        
        Steps:
        1. Determine project root (if not provided)
        2. Load config/settings.yaml
        3. Load config/endpoints.yaml (path from settings)
        4. Return unified facade
        
        Parameters
        ----------
        settings_path : Optional[str | Path]
            Path to settings.yaml. If None, uses config/settings.yaml in project root.
        project_root : Optional[Path]
            Project root directory. If None, auto-detected by finding config/ directory.
        env_prefix : Optional[str]
            Environment variable prefix for overrides (e.g., 'KQDL' for KQDL__DATA__DB_PATH).
        
        Returns
        -------
        ConfigFacade
            Unified configuration facade
        
        Examples
        --------
        >>> config = ConfigFacade.load()
        >>> config.default_db_path
        PosixPath('data/krx_db')
        
        >>> config = ConfigFacade.load(settings_path='custom/settings.yaml')
        """
        # Determine project root
        if project_root is None:
            project_root = cls._find_project_root()
        else:
            project_root = Path(project_root)
        
        # Load main settings
        if settings_path is None:
            settings_path = project_root / 'config' / 'settings.yaml'
        else:
            settings_path = Path(settings_path)
            if not settings_path.is_absolute():
                settings_path = project_root / settings_path
        
        with open(settings_path, 'r', encoding='utf-8') as f:
            settings_data = yaml.safe_load(f) or {}
        
        if env_prefix:
            _apply_env_overrides(settings_data, env_prefix=env_prefix, nested_delim="__")
        
        settings = SettingsConfig(**settings_data)
        
        # Load endpoints using path from settings
        endpoints_path = project_root / settings.config.endpoints_yaml
        with open(endpoints_path, 'r', encoding='utf-8') as f:
            endpoints_data = yaml.safe_load(f) or {}
        
        # Parse hosts and endpoints from endpoints.yaml
        hosts = {k: HostConfig(**v) for k, v in endpoints_data.get('hosts', {}).items()}
        endpoints = endpoints_data.get('endpoints', {})
        
        return cls(
            settings=settings,
            hosts=hosts,
            endpoints=endpoints,
            project_root=project_root,
        )

    @staticmethod
    def _find_project_root() -> Path:
        """
        Find project root by looking for config/ directory.
        
        Starts from this file's location and searches upward.
        
        Returns
        -------
        Path
            Project root directory
        
        Raises
        ------
        RuntimeError
            If project root cannot be found
        """
        # Start from this file's location (src/krx_quant_dataloader/)
        current = Path(__file__).parent
        
        # Go up until we find config/ directory (max 5 levels)
        for _ in range(5):
            if (current / 'config').exists():
                return current
            current = current.parent
        
        raise RuntimeError(
            "Could not find project root. "
            "Expected to find 'config/' directory within 5 levels up from config.py"
        )

    # Properties for easy access to paths
    @property
    def fields_yaml_path(self) -> Path:
        """Path to fields.yaml configuration file."""
        return self._project_root / self._settings.config.fields_yaml

    @property
    def endpoints_yaml_path(self) -> Path:
        """Path to endpoints.yaml configuration file."""
        return self._project_root / self._settings.config.endpoints_yaml

    @property
    def default_db_path(self) -> Path:
        """Default database directory path."""
        return self._project_root / self._settings.data.db_path

    @property
    def default_temp_path(self) -> Path:
        """Default temporary cache directory path."""
        return self._project_root / self._settings.data.temp_path

    @property
    def default_data_root(self) -> Path:
        """Default data root directory path."""
        return self._project_root / self._settings.data.data_root

    # Properties for endpoint configuration (backward compatibility)
    @property
    def hosts(self) -> Dict[str, HostConfig]:
        """Host configurations from endpoints.yaml."""
        return self._hosts

    @property
    def endpoints(self) -> Dict[str, Any]:
        """Endpoint definitions from endpoints.yaml."""
        return self._endpoints
    
    @property
    def project_root(self) -> Path:
        """Project root directory."""
        return self._project_root


def _apply_env_overrides(
    data: MutableMapping[str, Any], *, env_prefix: str, nested_delim: str
) -> None:
    """Apply environment variable overrides into the nested dict in-place.

    Mapping rule: {PREFIX}__KEY1__KEY2__...__KEYN=value → data[key1][key2]...[keyn] = value
    Keys are matched case-insensitively by converting env tokens to lower-case.
    """
    prefix = f"{env_prefix}{nested_delim}"
    for env_key, env_value in os.environ.items():
        if not env_key.startswith(prefix):
            continue
        path_tokens = env_key[len(prefix) :].split(nested_delim)
        if not path_tokens:
            continue
        tokens = [t.lower() for t in path_tokens]
        _set_deep_value(data, tokens, env_value)


def _set_deep_value(target: MutableMapping[str, Any], path: List[str], value: Any) -> None:
    """Set a value deep inside a nested dict, creating dict nodes as needed."""
    cur: MutableMapping[str, Any] = target
    for key in path[:-1]:
        if key not in cur or not isinstance(cur[key], MutableMapping):
            cur[key] = {}
        cur = cur[key]  # type: ignore[assignment]
    cur[path[-1]] = value


