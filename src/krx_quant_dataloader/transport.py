from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import requests

from .rate_limiter import TokenBucketRateLimiter


class TransportError(RuntimeError):
    def __init__(self, *, status_code: int, message: str | None = None):
        super().__init__(message or f"Transport error: HTTP {status_code}")
        self.status_code = status_code


class _NoopRateLimiter:
    """No-op rate limiter for testing/mocking."""
    def acquire(self, host_id: str) -> None:
        return None


class Transport:
    """HTTP transport with header merging, retries, rate limiting, and timeout propagation.

    The http_client must provide a `request(method, url, headers=..., params=..., data=..., timeout=...)` API
    compatible with `requests.Session`.
    
    Rate limiting is automatically enabled based on config.hosts[host_id].transport.rate_limit.requests_per_second.
    """

    def __init__(
        self,
        config,
        *,
        http_client: Optional[Any] = None,
        rate_limiter: Optional[Any] = None,
    ) -> None:
        """Initialize Transport with config-driven rate limiting.
        
        Parameters
        ----------
        config : ConfigFacade
            Configuration facade with host and transport settings.
        http_client : Optional[Any]
            HTTP client (must be requests.Session compatible). Default: requests.Session().
        rate_limiter : Optional[Any]
            Rate limiter instance. If None, creates TokenBucketRateLimiter from config.
            Pass _NoopRateLimiter() to disable rate limiting (testing only).
        """
        self._cfg = config
        self._http = http_client or requests.Session()
        
        # Create rate limiter from config if not provided
        if rate_limiter is None:
            # Extract rate limit from config (assuming single host 'krx' for now)
            # In future, could create per-host rate limiters
            if 'krx' in config.hosts:
                rps = config.hosts['krx'].transport.rate_limit.requests_per_second
                self._rl = TokenBucketRateLimiter(requests_per_second=rps)
            else:
                self._rl = _NoopRateLimiter()
        else:
            self._rl = rate_limiter

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


