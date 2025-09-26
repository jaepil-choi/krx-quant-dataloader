from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import requests


class TransportError(RuntimeError):
    def __init__(self, *, status_code: int, message: str | None = None):
        super().__init__(message or f"Transport error: HTTP {status_code}")
        self.status_code = status_code


class _NoopRateLimiter:
    def acquire(self, host_id: str) -> None:
        return None


class Transport:
    """HTTP transport with header merging, retries, and timeout propagation.

    The http_client must provide a `request(method, url, headers=..., params=..., data=..., timeout=...)` API
    compatible with `requests.Session`.
    """

    def __init__(
        self,
        config,
        *,
        http_client: Optional[Any] = None,
        rate_limiter: Optional[Any] = None,
    ) -> None:
        self._cfg = config
        self._http = http_client or requests.Session()
        self._rl = rate_limiter or _NoopRateLimiter()

    def send(
        self,
        *,
        method: str,
        host_id: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        host = self._cfg.hosts[host_id]
        url = f"{host.base_url}{path}"

        merged_headers: Dict[str, str] = {**(host.headers or {}), **(headers or {})}

        timeout: Tuple[int, int] = (
            host.transport.connect_timeout_seconds,
            host.transport.request_timeout_seconds,
        )

        retry_statuses = set(host.transport.retries.retry_statuses)
        max_retries = host.transport.retries.max_retries

        attempt = 0
        while True:
            self._rl.acquire(host_id)
            resp = self._http.request(
                method.upper(),
                url,
                headers=merged_headers,
                params=params if method.upper() == "GET" else None,
                data=data if method.upper() != "GET" else None,
                timeout=timeout,
            )

            status = getattr(resp, "status_code", 0)
            if 200 <= status < 300:
                # Assume JSON payload for KRX endpoints
                return resp.json()

            # Non-2xx
            if status in retry_statuses and attempt < max_retries:
                attempt += 1
                continue

            raise TransportError(status_code=status)


