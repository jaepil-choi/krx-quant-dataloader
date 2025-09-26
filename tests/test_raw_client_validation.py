import pytest


class _FakeOrchestrator:
    def __init__(self, rows):
        self._rows = rows
        self.last_call = None

    def execute(self, *, spec, host_id, params):
        self.last_call = {"spec": spec, "host_id": host_id, "params": dict(params)}
        return list(self._rows)


@pytest.mark.unit
def test_unknown_endpoint_id_raises(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.client import RawClient  # type: ignore

    reg = AdapterRegistry.load(config_path=test_config_path)
    fake = _FakeOrchestrator(rows=[])
    client = RawClient(registry=reg, orchestrator=fake)

    with pytest.raises(KeyError):
        _ = client.call("stock.unknown", host_id="krx", params={})


@pytest.mark.unit
def test_missing_required_params_raise(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.client import RawClient  # type: ignore

    reg = AdapterRegistry.load(config_path=test_config_path)
    fake = _FakeOrchestrator(rows=[])
    client = RawClient(registry=reg, orchestrator=fake)

    # stock.daily_quotes requires trdDd and mktId
    with pytest.raises(ValueError):
        _ = client.call("stock.daily_quotes", host_id="krx", params={"mktId": "ALL"})


@pytest.mark.unit
def test_defaults_applied_where_declared(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.client import RawClient  # type: ignore

    reg = AdapterRegistry.load(config_path=test_config_path)
    fake = _FakeOrchestrator(rows=[])
    client = RawClient(registry=reg, orchestrator=fake)

    # stock.search_listed has defaults for mktsel, searchText, typeNo
    _ = client.call("stock.search_listed", host_id="krx", params={})
    sent = fake.last_call["params"]
    assert sent["mktsel"] == "ALL"
    assert sent["searchText"] == ""
    assert sent["typeNo"] == 0


@pytest.mark.unit
def test_orchestrator_error_propagates(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.client import RawClient  # type: ignore

    class _ErrOrch(_FakeOrchestrator):
        def execute(self, *, spec, host_id, params):  # type: ignore[override]
            raise RuntimeError("boom")

    reg = AdapterRegistry.load(config_path=test_config_path)
    fake = _ErrOrch(rows=[])
    client = RawClient(registry=reg, orchestrator=fake)

    with pytest.raises(RuntimeError):
        _ = client.call("stock.daily_quotes", host_id="krx", params={"trdDd": "20240105", "mktId": "ALL"})


