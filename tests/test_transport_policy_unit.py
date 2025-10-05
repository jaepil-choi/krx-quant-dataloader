import pytest


class _FakeResp:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _FakeHTTP:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def request(self, method, url, *, headers=None, params=None, data=None, timeout=None):
        self.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "params": params,
            "data": data,
            "timeout": timeout,
        })
        if not self._responses:
            raise RuntimeError("No more fake responses queued")
        return self._responses.pop(0)


class _FakeRateLimiter:
    def __init__(self):
        self.acquire_calls = 0

    def acquire(self, host_id: str):
        self.acquire_calls += 1


@pytest.mark.unit
def test_retry_on_configured_statuses(test_settings_path: str):
    from krx_quant_dataloader.config import ConfigFacade
    from krx_quant_dataloader.transport import Transport  # type: ignore

    cfg = ConfigFacade.load(settings_path=test_settings_path)
    http = _FakeHTTP([
        _FakeResp(503, {"err": "backend busy"}),
        _FakeResp(200, {"ok": True}),
    ])
    rl = _FakeRateLimiter()

    t = Transport(cfg, http_client=http, rate_limiter=rl)
    out = t.send(
        method="POST",
        host_id="krx",
        path="/comm/bldAttendant/getJsonData.cmd",
        headers=None,
        params=None,
        data={"bld": "dbms/MDC/STAT/standard/MDCSTAT01501", "trdDd": "20240105", "mktId": "ALL"},
    )
    assert out == {"ok": True}
    assert len(http.calls) == 2  # one retry
    assert rl.acquire_calls == 2
    # timeout tuple propagated
    expected_timeout = (
        cfg.hosts["krx"].transport.connect_timeout_seconds,
        cfg.hosts["krx"].transport.request_timeout_seconds,
    )
    assert http.calls[0]["timeout"] == expected_timeout


@pytest.mark.unit
def test_no_retry_on_4xx_and_raises(test_settings_path: str):
    from krx_quant_dataloader.config import ConfigFacade
    from krx_quant_dataloader.transport import Transport, TransportError  # type: ignore

    cfg = ConfigFacade.load(settings_path=test_settings_path)
    http = _FakeHTTP([
        _FakeResp(404, {"error": "not found"}),
    ])
    rl = _FakeRateLimiter()

    t = Transport(cfg, http_client=http, rate_limiter=rl)
    with pytest.raises(TransportError):
        _ = t.send(
            method="POST",
            host_id="krx",
            path="/comm/bldAttendant/getJsonData.cmd",
            data={"bld": "dbms/MDC/STAT/standard/MDCSTAT01501"},
        )
    assert len(http.calls) == 1
    assert rl.acquire_calls == 1


@pytest.mark.unit
def test_get_uses_params_and_header_merge(test_settings_path: str):
    from krx_quant_dataloader.config import ConfigFacade
    from krx_quant_dataloader.transport import Transport  # type: ignore

    cfg = ConfigFacade.load(settings_path=test_settings_path)
    http = _FakeHTTP([
        _FakeResp(200, {"ok": True}),
    ])
    rl = _FakeRateLimiter()

    t = Transport(cfg, http_client=http, rate_limiter=rl)
    _ = t.send(
        method="GET",
        host_id="krx",
        path="/comm/bldAttendant/getJsonData.cmd",
        headers={"X-Extra": "1"},
        params={"foo": "bar"},
    )

    call = http.calls[0]
    # GET uses params, not data
    assert call["params"] == {"foo": "bar"}
    assert call["data"] is None
    # Headers include host defaults and merged extras
    assert call["headers"]["Referer"].startswith("https://")
    assert call["headers"]["User-Agent"].startswith("Mozilla/")
    assert call["headers"]["X-Extra"] == "1"


