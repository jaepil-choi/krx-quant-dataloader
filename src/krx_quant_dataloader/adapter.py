from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import yaml
from pydantic import BaseModel, ConfigDict, field_validator


class ChunkingSpec(BaseModel):
    """Chunking hints for date-ranged endpoints."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    days: Optional[int] = None
    gap_days: Optional[int] = None


class EndpointSpec(BaseModel):
    """Immutable value object describing an endpoint.

    This is a projection from YAML endpoint entries. Unknown fields are ignored.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    method: str
    path: str
    bld: str
    response_roots: list[str]
    order_by: Optional[str] = None
    date_params: Optional[Dict[str, str]] = None
    chunking: Optional[ChunkingSpec] = None

    @field_validator("method")
    @classmethod
    def normalize_method(cls, v: str) -> str:
        return v.upper()


class AdapterRegistry(BaseModel):
    """Registry of endpoint specifications loaded from YAML.

    Use `AdapterRegistry.load(config_path=...)` to construct.
    """

    model_config = ConfigDict(frozen=True)

    specs: Mapping[str, EndpointSpec]

    def get(self, endpoint_id: str) -> EndpointSpec:
        """Retrieve the immutable spec for an endpoint id."""
        try:
            return self.specs[endpoint_id]
        except KeyError as exc:
            raise KeyError(f"Unknown endpoint id: {endpoint_id}") from exc

    @classmethod
    def load(cls, *, config_path: str) -> "AdapterRegistry":
        """Load endpoint specs from a YAML file path.

        Only the `endpoints` section is consumed. Unknown keys per endpoint are ignored.
        Required keys per endpoint: method, path, bld, response.root_keys.
        Optional keys: response.order_by, date_params, chunking.{days,gap_days}.
        """
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        endpoints = data.get("endpoints")
        if not isinstance(endpoints, dict):
            raise ValueError("YAML must contain an 'endpoints' mapping")

        specs: Dict[str, EndpointSpec] = {}
        for endpoint_id, raw in endpoints.items():
            try:
                method = raw["method"]
                path = raw["path"]
                bld = raw["bld"]

                response = raw.get("response") or {}
                roots = response.get("root_keys")
                if not isinstance(roots, list) or not all(isinstance(x, str) for x in roots):
                    raise ValueError(
                        f"endpoint {endpoint_id}: response.root_keys must be a list of strings"
                    )
                order_by = response.get("order_by")

                date_params = raw.get("date_params")
                if date_params is not None and not (
                    isinstance(date_params, dict)
                    and "start" in date_params
                    and "end" in date_params
                ):
                    raise ValueError(
                        f"endpoint {endpoint_id}: date_params must be a mapping with 'start' and 'end'"
                    )

                chunking_raw = raw.get("chunking")
                chunking = None
                if chunking_raw is not None:
                    if not isinstance(chunking_raw, dict):
                        raise ValueError(
                            f"endpoint {endpoint_id}: chunking must be a mapping"
                        )
                    chunking = ChunkingSpec(**chunking_raw)

                spec = EndpointSpec(
                    method=method,
                    path=path,
                    bld=bld,
                    response_roots=list(roots),
                    order_by=order_by,
                    date_params=date_params,
                    chunking=chunking,
                )
                specs[endpoint_id] = spec
            except KeyError as exc:
                # Missing required fields
                raise ValueError(
                    f"endpoint {endpoint_id}: missing required field {exc.args[0]!r}"
                ) from exc

        return cls(specs=specs)


