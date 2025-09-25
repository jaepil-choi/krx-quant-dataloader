import pytest


def _load_facade(test_config_path: str, *, env_prefix: str | None = None):
    # Import inside helper to avoid hard import errors masking path issues
    from krx_quant_dataloader.config import ConfigFacade  # type: ignore
    if env_prefix is None:
        return ConfigFacade.load(config_path=test_config_path)
    return ConfigFacade.load(config_path=test_config_path, env_prefix=env_prefix)


def test_loads_defaults_from_yaml(test_config_path: str):
    cfg = _load_facade(test_config_path)
    assert "krx" in cfg.hosts
    krx = cfg.hosts["krx"]
    assert krx.base_url.startswith("https://")
    assert krx.default_path == "/comm/bldAttendant/getJsonData.cmd"

    # transport settings
    assert krx.transport.request_timeout_seconds == 15
    assert krx.transport.connect_timeout_seconds == 5

    # headers
    assert isinstance(krx.headers, dict)
    assert krx.headers["User-Agent"].startswith("Mozilla/")
    assert krx.headers["Referer"].startswith("https://")


def test_env_override_wins(monkeypatch: pytest.MonkeyPatch, test_config_path: str):
    # Pydantic-style nested env override using double underscore
    monkeypatch.setenv(
        "KQDL__HOSTS__KRX__TRANSPORT__REQUEST_TIMEOUT_SECONDS",
        "7",
    )
    cfg = _load_facade(test_config_path, env_prefix="KQDL")
    assert cfg.hosts["krx"].transport.request_timeout_seconds == 7


def test_https_enforced_raises(monkeypatch: pytest.MonkeyPatch, test_config_path: str):
    monkeypatch.setenv("KQDL__HOSTS__KRX__BASE_URL", "http://data.krx.co.kr")
    with pytest.raises(ValueError):
        _ = _load_facade(test_config_path, env_prefix="KQDL")


def test_retry_policy_loaded(test_config_path: str):
    cfg = _load_facade(test_config_path)
    retries = cfg.hosts["krx"].transport.retries
    assert retries.max_retries == 2
    assert retries.backoff_factor == 0.5
    assert set(retries.retry_statuses) == {502, 503, 504}


def test_immutability_of_settings(test_config_path: str):
    cfg = _load_facade(test_config_path)
    with pytest.raises(Exception):
        cfg.hosts["krx"].transport.request_timeout_seconds = 99  # type: ignore


def test_idempotent_load(test_config_path: str):
    a = _load_facade(test_config_path)
    b = _load_facade(test_config_path)
    assert a.hosts["krx"].base_url == b.hosts["krx"].base_url
    assert (
        a.hosts["krx"].transport.request_timeout_seconds
        == b.hosts["krx"].transport.request_timeout_seconds
    )


