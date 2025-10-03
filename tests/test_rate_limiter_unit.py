"""
Unit tests for TokenBucketRateLimiter
"""

import time
import pytest

from krx_quant_dataloader.rate_limiter import TokenBucketRateLimiter


@pytest.mark.unit
def test_rate_limiter_basic():
    """Test that rate limiter enforces 1 request per second."""
    limiter = TokenBucketRateLimiter(requests_per_second=1.0)
    
    # First request should be immediate
    start = time.time()
    limiter.acquire('test_host')
    first_elapsed = time.time() - start
    assert first_elapsed < 0.1, "First request should be immediate"
    
    # Second request should be delayed by ~1 second
    start2 = time.time()
    limiter.acquire('test_host')
    second_elapsed = time.time() - start2
    assert 0.9 < second_elapsed < 1.2, f"Second request should wait ~1s, got {second_elapsed:.2f}s"


@pytest.mark.unit
def test_rate_limiter_multiple_hosts():
    """Test that rate limiting is per-host."""
    limiter = TokenBucketRateLimiter(requests_per_second=1.0)
    
    # Request to host A
    start = time.time()
    limiter.acquire('host_a')
    
    # Immediate request to host B should not be blocked
    limiter.acquire('host_b')
    elapsed = time.time() - start
    assert elapsed < 0.1, "Different hosts should not interfere"
    
    # Second request to host A should be blocked
    start2 = time.time()
    limiter.acquire('host_a')
    elapsed2 = time.time() - start2
    assert 0.9 < elapsed2 < 1.2, "Same host should be rate limited"


@pytest.mark.unit
def test_rate_limiter_fast_rate():
    """Test that higher rates work (2 requests per second = 0.5s interval)."""
    limiter = TokenBucketRateLimiter(requests_per_second=2.0)
    
    # First request immediate
    limiter.acquire('test')
    
    # Second request should wait ~0.5 seconds
    start = time.time()
    limiter.acquire('test')
    elapsed = time.time() - start
    assert 0.4 < elapsed < 0.7, f"Should wait ~0.5s, got {elapsed:.2f}s"


@pytest.mark.unit
def test_rate_limiter_slow_rate():
    """Test slow rate (0.5 requests per second = 2s interval)."""
    limiter = TokenBucketRateLimiter(requests_per_second=0.5)
    
    # First request immediate
    limiter.acquire('test')
    
    # Second request should wait ~2 seconds
    start = time.time()
    limiter.acquire('test')
    elapsed = time.time() - start
    assert 1.9 < elapsed < 2.2, f"Should wait ~2s, got {elapsed:.2f}s"


@pytest.mark.unit
def test_rate_limiter_invalid_rate():
    """Test that invalid rates raise ValueError."""
    with pytest.raises(ValueError, match="must be positive"):
        TokenBucketRateLimiter(requests_per_second=0)
    
    with pytest.raises(ValueError, match="must be positive"):
        TokenBucketRateLimiter(requests_per_second=-1)


@pytest.mark.unit
def test_rate_limiter_natural_spacing():
    """Test that naturally spaced requests don't get delayed."""
    limiter = TokenBucketRateLimiter(requests_per_second=1.0)
    
    # First request
    limiter.acquire('test')
    
    # Wait 1.5 seconds naturally
    time.sleep(1.5)
    
    # Next request should be immediate (no additional waiting)
    start = time.time()
    limiter.acquire('test')
    elapsed = time.time() - start
    assert elapsed < 0.1, "Naturally spaced requests should not be delayed"

