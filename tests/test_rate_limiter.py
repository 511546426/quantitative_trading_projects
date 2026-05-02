"""
Tests for data/common/rate_limiter.py

Covers: RateLimiter — token bucket acquire, try_acquire,
        refill behavior, timeout, edge cases
"""
from __future__ import annotations

import time

import pytest

from data.common.rate_limiter import RateLimiter


class TestRateLimiter:
    def test_initial_tokens(self):
        """Starts with capacity tokens available."""
        rl = RateLimiter(capacity=10, refill_rate=5.0)
        assert rl.available_tokens == pytest.approx(10.0, abs=0.1)

    def test_acquire_full_capacity(self):
        """Acquire all tokens at once succeeds."""
        rl = RateLimiter(capacity=5, refill_rate=10.0)
        assert rl.acquire(5) is True
        assert rl.available_tokens < 1.0

    def test_acquire_exceeds_capacity(self):
        """Acquire more than capacity requires waiting for refill."""
        rl = RateLimiter(capacity=3, refill_rate=10.0)
        assert rl.acquire(3) is True
        # Now 0 tokens, try get 2 more, should time out if we set short timeout
        assert rl.acquire(2, timeout=0.01) is False

    def test_try_acquire_success(self):
        """try_acquire succeeds when tokens are available."""
        rl = RateLimiter(capacity=5, refill_rate=10.0)
        assert rl.try_acquire(3) is True

    def test_try_acquire_failure(self):
        """try_acquire returns False when insufficient tokens."""
        rl = RateLimiter(capacity=2, refill_rate=10.0)
        assert rl.try_acquire(2) is True
        assert rl.try_acquire(1) is False  # no tokens left

    def test_refill_over_time(self):
        """Tokens refill over time at refill_rate."""
        rl = RateLimiter(capacity=10, refill_rate=100.0)
        rl.acquire(10)  # drain
        time.sleep(0.05)  # should refill ~5 tokens
        assert rl.available_tokens >= 4.0

    def test_refill_does_not_exceed_capacity(self):
        """Refill does not exceed capacity."""
        rl = RateLimiter(capacity=5, refill_rate=100.0)
        time.sleep(0.1)  # would refill 10 tokens but capped at 5
        assert rl.available_tokens == pytest.approx(5.0, abs=0.1)

    def test_acquire_timeout(self):
        """acquire returns False when timeout is reached."""
        rl = RateLimiter(capacity=1, refill_rate=1.0)
        rl.acquire(1)  # drain
        start = time.monotonic()
        result = rl.acquire(1, timeout=0.05)
        elapsed = time.monotonic() - start
        assert result is False
        assert elapsed < 0.5  # shouldn't wait much longer than timeout

    def test_acquire_no_timeout_eventually_succeeds(self):
        """Acquire with no timeout eventually gets tokens from refill."""
        rl = RateLimiter(capacity=1, refill_rate=100.0)
        rl.acquire(1)  # drain
        result = rl.acquire(1, timeout=1.0)
        assert result is True

    def test_available_tokens_after_acquire(self):
        """available_tokens decreases after acquire."""
        rl = RateLimiter(capacity=10, refill_rate=10.0)
        before = rl.available_tokens
        rl.acquire(3)
        after = rl.available_tokens
        assert after == pytest.approx(before - 3, abs=0.1)

    def test_concurrent_safety(self):
        """Multiple quick acquires should be thread-safe (smoke test)."""
        rl = RateLimiter(capacity=50, refill_rate=100.0)
        for _ in range(30):
            assert rl.acquire(1, timeout=0.1) is True

    def test_large_burst(self):
        """Burst of capacity sized acquire is allowed."""
        rl = RateLimiter(capacity=100, refill_rate=10.0)
        assert rl.acquire(100) is True
        assert rl.available_tokens < 0.5  # drained
