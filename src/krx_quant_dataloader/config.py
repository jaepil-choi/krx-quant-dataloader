from __future__ import annotations

import os
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


class RootConfig(BaseModel):
    """Root configuration as loaded from YAML."""

    model_config = ConfigDict(frozen=True)

    version: int
    hosts: Dict[str, HostConfig]
    # endpoints are not used by the current tests, but present in YAML
    endpoints: Optional[Dict[str, Any]] = None


class ConfigFacade(BaseModel):
    """Immutable facade providing access to validated configuration.

    Use ConfigFacade.load(...) to construct from YAML with optional env overrides.
    """

    model_config = ConfigDict(frozen=True)

    hosts: Dict[str, HostConfig]
    # Expose endpoints mapping to accommodate schema evolution (params, client_policy, response)
    endpoints: Dict[str, Any] = {}

    @classmethod
    def load(cls, *, config_path: str, env_prefix: Optional[str] = None) -> "ConfigFacade":
        """Load configuration from YAML, apply env overrides, and validate.

        Parameters
        ----------
        config_path: str
            Path to YAML configuration file.
        env_prefix: Optional[str]
            If provided, environment variables starting with this prefix and
            using a double-underscore nested delimiter will override YAML values.

        Returns
        -------
        ConfigFacade
            An immutable facade over validated configuration models.
        """
        if not config_path:
            raise ValueError("config_path is required")

        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        if env_prefix:
            _apply_env_overrides(data, env_prefix=env_prefix, nested_delim="__")

        root = RootConfig(**data)
        return cls(hosts=root.hosts, endpoints=root.endpoints or {})


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


