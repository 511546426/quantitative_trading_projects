"""
令牌桶速率限制器。

每个数据源一个 RateLimiter 实例，确保请求频率不超限。
支持阻塞等待和非阻塞检查两种模式。
"""
from __future__ import annotations

import threading
import time


class RateLimiter:
    """
    线程安全的令牌桶限速器。

    Parameters
    ----------
    capacity : int
        桶容量（允许的突发请求数）。
    refill_rate : float
        每秒补充的令牌数。
    """

    def __init__(self, capacity: int = 10, refill_rate: float = 3.0):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, n: int = 1, timeout: float | None = None) -> bool:
        """
        获取 *n* 个令牌，令牌不足时阻塞等待。

        Parameters
        ----------
        n : int
            需要的令牌数量。
        timeout : float, optional
            最长等待秒数。None 表示无限等待。

        Returns
        -------
        bool
            True 表示成功获取，False 表示超时。
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return True
                wait = (n - self._tokens) / self.refill_rate

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait = min(wait, remaining)

            time.sleep(wait)

    def try_acquire(self, n: int = 1) -> bool:
        """非阻塞尝试，令牌不足立即返回 False"""
        with self._lock:
            self._refill()
            if self._tokens >= n:
                self._tokens -= n
                return True
            return False

    @property
    def available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now
