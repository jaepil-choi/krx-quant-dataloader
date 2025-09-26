import pytest


class _FakeTransport:
    def __init__(self, payloads):
        self._payloads = list(payloads)
        self.calls = []

    def send(self, *, method, host_id, path, headers=None, params=None, data=None):
        self.calls.append({
            "method": method,
            "host_id": host_id,
            "path": path,
            "headers": headers,
            "params": params,
            "data": data,
        })
        if not self._payloads:
            raise RuntimeError("No more fake payloads queued")
        return self._payloads.pop(0)


@pytest.mark.unit
def test_chunking_and_merge_ordering(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.orchestration import Orchestrator  # type: ignore

    reg = AdapterRegistry.load(config_path=test_config_path)
    spec = reg.get("stock.individual_history")

    # Two chunks: ensure final rows ordered by TRD_DD ascending as per spec.order_by
    fake = _FakeTransport([
        {"output": [{"TRD_DD": "2024-01-03"}]},
        {"output": [{"TRD_DD": "2024-01-01"}]},
    ])

    orch = Orchestrator(fake)
    rows = orch.execute(
        spec=spec,
        host_id="krx",
        params={
            "isuCd": "KR7005930003",
            "adjStkPrc": 1,
            "strtDd": "20220101",
            "endDd": "20241231",
        },
    )
    assert [r["TRD_DD"] for r in rows] == ["2024-01-01", "2024-01-03"]
    assert len(fake.calls) >= 2


@pytest.mark.unit
def test_extraction_mixed_root_keys(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.orchestration import Orchestrator  # type: ignore

    reg = AdapterRegistry.load(config_path=test_config_path)
    spec = reg.get("stock.individual_history")

    fake = _FakeTransport([
        {"output": [{"TRD_DD": "2023-01-01"}]},
        {"OutBlock_1": [{"TRD_DD": "2023-02-01"}]},
    ])
    orch = Orchestrator(fake)
    rows = orch.execute(
        spec=spec,
        host_id="krx",
        params={
            "isuCd": "KR7005930003",
            "adjStkPrc": 1,
            "strtDd": "20220101",
            "endDd": "20231231",
        },
    )
    assert [r["TRD_DD"] for r in rows] == ["2023-01-01", "2023-02-01"]


@pytest.mark.unit
def test_extraction_error_when_no_known_roots(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.orchestration import Orchestrator, ExtractionError  # type: ignore

    reg = AdapterRegistry.load(config_path=test_config_path)
    spec = reg.get("stock.daily_quotes")

    fake = _FakeTransport([
        {"unknown": []},
    ])
    orch = Orchestrator(fake)
    with pytest.raises(ExtractionError):
        _ = orch.execute(
            spec=spec,
            host_id="krx",
            params={"trdDd": "20240105", "mktId": "ALL"},
        )


@pytest.mark.unit
def test_no_chunk_for_single_date_endpoint(test_config_path: str):
    from krx_quant_dataloader.adapter import AdapterRegistry
    from krx_quant_dataloader.orchestration import Orchestrator  # type: ignore

    reg = AdapterRegistry.load(config_path=test_config_path)
    spec = reg.get("stock.daily_quotes")

    sample_rows = [{"ISU_SRT_CD": "005930", "TDD_CLSPRC": "70000"}]
    fake = _FakeTransport([
        {"OutBlock_1": sample_rows},
    ])
    orch = Orchestrator(fake)
    rows = orch.execute(
        spec=spec,
        host_id="krx",
        params={"trdDd": "20240105", "mktId": "ALL"},
    )
    assert rows == sample_rows
    assert len(fake.calls) == 1


