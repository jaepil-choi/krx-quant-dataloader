import os
import pytest


@pytest.mark.integration
@pytest.mark.live
def test_live_daily_quotes_smoke(test_config_path: str):
    """Early live smoke: verify the real KRX path is reachable and returns rows.

    Scope: tiny single-day request. Uses requests directly to avoid blocking on
    Transport/Adapter/Orchestrator implementation, but honors YAML settings
    (base_url, default_path, headers, bld, and params) to validate schema.
    """
    import requests
    from krx_quant_dataloader.config import ConfigFacade
    from krx_quant_dataloader.adapter import AdapterRegistry

    cfg = ConfigFacade.load(config_path=test_config_path)
    krx = cfg.hosts["krx"]

    # Load endpoint spec from AdapterRegistry so smoke test validates registry schema too
    registry = AdapterRegistry.load(config_path=test_config_path)
    spec = registry.get("stock.daily_quotes")

    url = f"{krx.base_url}{spec.path}"
    headers = krx.headers
    data = {
        "bld": spec.bld,
        "trdDd": "20240105",
        "mktId": "ALL",
    }

    if spec.method.upper() == "POST":
        resp = requests.post(
            url,
            headers=headers,
            data=data,
            timeout=(
                krx.transport.connect_timeout_seconds,
                krx.transport.request_timeout_seconds,
            ),
        )
    else:
        resp = requests.get(
            url,
            headers=headers,
            params=data,
            timeout=(
                krx.transport.connect_timeout_seconds,
                krx.transport.request_timeout_seconds,
            ),
        )
    resp.raise_for_status()
    payload = resp.json()

    # Accept any of the known root keys from the spec, assert non-empty
    rows = None
    for key in spec.response_roots:
        if key in payload:
            rows = payload[key]
            break
    assert isinstance(rows, list) and len(rows) >= 1


