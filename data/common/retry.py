"""
重试装饰器 — 指数退避 + 可配置的异常过滤。
"""
from __future__ import annotations

import functools
import logging
import time
from typing import Callable, Sequence, Type

from data.common.exceptions import (
    AuthError,
    DataFormatError,
    FetchError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS: tuple[Type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
    FetchError,
)

NON_RETRYABLE_EXCEPTIONS: tuple[Type[Exception], ...] = (
    AuthError,
    DataFormatError,
)


def retry(
    max_retries: int = 3,
    backoff: Sequence[float] | None = None,
    retryable: tuple[Type[Exception], ...] = RETRYABLE_EXCEPTIONS,
    non_retryable: tuple[Type[Exception], ...] = NON_RETRYABLE_EXCEPTIONS,
) -> Callable:
    """
    重试装饰器。

    Parameters
    ----------
    max_retries : int
        最大重试次数。
    backoff : list[float], optional
        每次重试的等待秒数。默认指数退避 [1, 2, 4, ...]。
    retryable : tuple
        可重试的异常类型。
    non_retryable : tuple
        不可重试的异常类型（优先级高于 retryable）。
    """
    if backoff is None:
        backoff = [2 ** i for i in range(max_retries)]

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except non_retryable as e:
                    logger.error(
                        "%s 不可重试异常 (attempt %d/%d): %s",
                        func.__name__, attempt + 1, max_retries + 1, e,
                    )
                    raise
                except retryable as e:
                    last_exc = e
                    if attempt >= max_retries:
                        break

                    wait = backoff[min(attempt, len(backoff) - 1)]
                    if isinstance(e, RateLimitError) and e.retry_after > 0:
                        wait = max(wait, e.retry_after)

                    logger.warning(
                        "%s 第 %d/%d 次重试，等待 %.1fs: %s",
                        func.__name__, attempt + 1, max_retries, wait, e,
                    )
                    time.sleep(wait)

            logger.error(
                "%s 重试 %d 次后仍然失败: %s",
                func.__name__, max_retries, last_exc,
            )
            raise last_exc  # type: ignore[misc]

        return wrapper
    return decorator
