import pytest


@pytest.mark.integration
@pytest.mark.live
def test_transport_live_smoke_builds_and_calls_real_api(test_config_path: str):
    from krx_quant_dataloader.config import ConfigFacade
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.transport import Transport

    cfg = ConfigFacade.load(config_path=test_config_path)
    reg = AdapterRegistry.load(config_path=test_config_path)
    spec = reg.get("stock.daily_quotes")

    t = Transport(cfg)
    payload = t.send(
        method=spec.method,
        host_id="krx",
        path=spec.path,
        data={
            "bld": spec.bld,
            "trdDd": "20240105",
            "mktId": "ALL",
        },
    )

    # find rows using declared roots
    rows = None
    for key in spec.response_roots:
        if key in payload:
            rows = payload[key]
            break
    assert isinstance(rows, list) and len(rows) >= 1


