from __future__ import annotations

from typing import Any, Dict, List

from .adapter import AdapterRegistry, EndpointSpec


class RawClient:
    def __init__(self, *, registry: AdapterRegistry, orchestrator):
        self._registry = registry
        self._orchestrator = orchestrator

    def _apply_defaults_and_validate(self, spec: EndpointSpec, params: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(params)
        for name, pspec in spec.params.items():
            if name not in out:
                if pspec.default is not None:
                    out[name] = pspec.default
                elif pspec.required:
                    raise ValueError(f"Missing required parameter: {name}")
        return out

    def call(self, endpoint_id: str, *, host_id: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        spec = self._registry.get(endpoint_id)
        norm_params = self._apply_defaults_and_validate(spec, params)
        rows = self._orchestrator.execute(spec=spec, host_id=host_id, params=norm_params)
        return rows


