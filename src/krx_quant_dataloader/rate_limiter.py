"""
Rate limiter for API requests

Implements token bucket algorithm for per-host rate limiting.
"""

from __future__ import annotations

import time
from threading import Lock
from typing import Dict


class TokenBucketRateLimiter:
    """Token bucket rate limiter per host.
    
    Implements a simple token bucket algorithm:
    - Each host has a bucket that fills at a constant rate (tokens/second)
    - Each request consumes 1 token
    - If no tokens available, blocks until one is available
    
    Thread-safe for concurrent use.
    """
    
    def __init__(self, requests_per_second: float):
        """Initialize rate limiter.
        
        Parameters
        ----------
        requests_per_second : float
            Maximum requests per second per host.
            Example: 1.0 = 1 request per second
                    0.5 = 1 request per 2 seconds
                    2.0 = 2 requests per second
        """
        if requests_per_second <= 0:
            raise ValueError(f"requests_per_second must be positive, got {requests_per_second}")
        
        self.rate = requests_per_second  # tokens per second
        self.interval = 1.0 / requests_per_second  # seconds per token
        
        # Per-host state
        self._buckets: Dict[str, float] = {}  # host_id -> last_request_time
        self._locks: Dict[str, Lock] = {}  # host_id -> lock
    
    def acquire(self, host_id: str) -> None:
        """Acquire a token for the given host, blocking if necessary.
        
        Parameters
        ----------
        host_id : str
            Host identifier (e.g., 'krx').
        """
        # Ensure lock exists for this host
        if host_id not in self._locks:
            self._locks[host_id] = Lock()
        
        with self._locks[host_id]:
            current_time = time.time()
            
            # Initialize bucket if first request
            if host_id not in self._buckets:
                self._buckets[host_id] = current_time
                return
            
            last_request = self._buckets[host_id]
            time_since_last = current_time - last_request
            
            # If enough time has passed, allow immediately
            if time_since_last >= self.interval:
                self._buckets[host_id] = current_time
                return
            
            # Otherwise, sleep until we can make the next request
            sleep_time = self.interval - time_since_last
            time.sleep(sleep_time)
            
            # Update bucket
            self._buckets[host_id] = time.time()


__all__ = [
    "TokenBucketRateLimiter",
]

