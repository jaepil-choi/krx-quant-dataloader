import pytest


@pytest.mark.integration
@pytest.mark.live
def test_raw_client_live_smoke_full_stack(test_settings_path: str, test_config_path: str):
    from krx_quant_dataloader.config import ConfigFacade
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.transport import Transport
    from krx_quant_dataloader.orchestration import Orchestrator
    from krx_quant_dataloader.client import RawClient

    cfg = ConfigFacade.load(settings_path=test_settings_path)
    reg = AdapterRegistry.load(config_path=test_config_path)
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    client = RawClient(registry=reg, orchestrator=orch)

    rows = client.call(
        "stock.daily_quotes",
        host_id="krx",
        params={"trdDd": "20240105", "mktId": "ALL"},
    )
    assert isinstance(rows, list) and len(rows) >= 1


