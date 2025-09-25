import copy
from pathlib import Path
import pytest
import yaml


def _load_registry(config_path: str):
    # Local import to avoid hard import errors during collection
    # until the adapter layer is implemented.
    from krx_quant_dataloader.adapter import AdapterRegistry  # type: ignore
    return AdapterRegistry.load(config_path=config_path)


@pytest.mark.unit
def test_loads_valid_endpoint_specs(test_config_path: str):
    registry = _load_registry(test_config_path)

    dq = registry.get("stock.daily_quotes")
    assert dq.method == "POST"
    assert dq.path == "/comm/bldAttendant/getJsonData.cmd"
    assert dq.bld == "dbms/MDC/STAT/standard/MDCSTAT01501"
    assert dq.response_roots == ["OutBlock_1", "output", "block1"]

    ih = registry.get("stock.individual_history")
    assert ih.method == "POST"
    assert ih.bld == "dbms/MDC/STAT/standard/MDCSTAT01701"
    assert ih.response_roots == ["output", "OutBlock_1", "block1"]


@pytest.mark.unit
def test_response_roots_order_preserved(test_config_path: str):
    registry = _load_registry(test_config_path)
    dq = registry.get("stock.daily_quotes")
    assert dq.response_roots == ["OutBlock_1", "output", "block1"]


@pytest.mark.unit
def test_date_params_and_chunking_exposed(test_config_path: str):
    registry = _load_registry(test_config_path)
    dq = registry.get("stock.daily_quotes")
    assert dq.date_params is None
    assert dq.chunking is None or (dq.chunking.days is None and dq.chunking.gap_days is None)

    ih = registry.get("stock.individual_history")
    assert ih.date_params == {"start": "strtDd", "end": "endDd"}
    assert ih.chunking.days == 730
    assert ih.chunking.gap_days == 1
    assert ih.order_by == "TRD_DD"


@pytest.mark.unit
def test_specs_are_immutable(test_config_path: str):
    registry = _load_registry(test_config_path)
    spec = registry.get("stock.daily_quotes")
    with pytest.raises(Exception):
        spec.method = "GET"  # type: ignore


@pytest.mark.unit
def test_malformed_spec_missing_bld_raises(tmp_path: Path, test_config_path: str):
    with open(test_config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    broken = copy.deepcopy(cfg)
    broken["endpoints"]["stock.daily_quotes"].pop("bld", None)

    p = tmp_path / "broken.yaml"
    with open(p, "w", encoding="utf-8") as f:
        yaml.safe_dump(broken, f, sort_keys=False, allow_unicode=True)

    with pytest.raises(Exception):
        _ = _load_registry(str(p))


@pytest.mark.unit
def test_unknown_keys_are_ignored(tmp_path: Path, test_config_path: str):
    with open(test_config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    modified = copy.deepcopy(cfg)
    modified["endpoints"]["stock.daily_quotes"]["x-extra"] = {"foo": 1}

    p = tmp_path / "extra.yaml"
    with open(p, "w", encoding="utf-8") as f:
        yaml.safe_dump(modified, f, sort_keys=False, allow_unicode=True)

    registry = _load_registry(str(p))
    spec = registry.get("stock.daily_quotes")
    assert spec.method == "POST"
    # No attribute for unknown keys should exist
    assert not hasattr(spec, "x-extra")


