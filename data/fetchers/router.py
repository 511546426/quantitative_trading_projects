"""
数据源路由器 — 自动降级与恢复。

正常模式:   TushareFetcher (主力)
降级模式:   BaoStockFetcher (备用)
补充数据:   AKShareFetcher (北向/龙虎榜)

降级触发: 连续 N 次失败
恢复检测: 每隔 M 秒探测主源可用性
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

import pandas as pd

from data.common.exceptions import FetchError, SourceUnavailableError
from data.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)


class FetcherRouter:
    """
    多数据源路由器，带自动降级与恢复。

    Parameters
    ----------
    primary : BaseFetcher
        主数据源。
    fallback : BaseFetcher, optional
        备用数据源。
    supplementary : BaseFetcher, optional
        补充数据源（用于获取主源不提供的数据）。
    max_failures : int
        触发降级所需的连续失败次数。
    recovery_interval : float
        降级后探测恢复的间隔秒数。
    """

    def __init__(
        self,
        primary: BaseFetcher,
        fallback: BaseFetcher | None = None,
        supplementary: BaseFetcher | None = None,
        max_failures: int = 3,
        recovery_interval: float = 300.0,
    ):
        self.primary = primary
        self.fallback = fallback
        self.supplementary = supplementary
        self._max_failures = max_failures
        self._recovery_interval = recovery_interval

        self._failure_count = 0
        self._degraded = False
        self._last_recovery_attempt = 0.0

    # ---- 代理方法（自动路由到可用数据源） ----

    def get_stock_list(self) -> pd.DataFrame:
        return self._route("get_stock_list")

    def get_trade_calendar(self, **kwargs) -> pd.DataFrame:
        return self._route("get_trade_calendar", **kwargs)

    def get_daily_bars(self, **kwargs) -> pd.DataFrame:
        return self._route("get_daily_bars", **kwargs)

    def get_adj_factor(self, **kwargs) -> pd.DataFrame:
        return self._route("get_adj_factor", **kwargs)

    def get_index_daily(self, **kwargs) -> pd.DataFrame:
        return self._route("get_index_daily", **kwargs)

    def get_financial_indicator(self, **kwargs) -> pd.DataFrame:
        return self._route("get_financial_indicator", **kwargs)

    def get_valuation(self, **kwargs) -> pd.DataFrame:
        return self._route("get_valuation", **kwargs)

    def get_dividend(self, **kwargs) -> pd.DataFrame:
        return self._route("get_dividend", **kwargs)

    # ---- 核心路由逻辑 ----

    def _route(self, method_name: str, **kwargs) -> pd.DataFrame:
        """尝试主源 → 触发降级 → 尝试备用源"""

        if self._degraded:
            self._try_recovery(method_name, kwargs)

        if not self._degraded:
            try:
                result = self._call(self.primary, method_name, **kwargs)
                self._on_success()
                return result
            except (FetchError, NotImplementedError) as e:
                self._on_failure(e)

        if self.fallback:
            try:
                logger.warning(
                    "降级到 %s.%s", self.fallback.source_name, method_name
                )
                result = self._call(self.fallback, method_name, **kwargs)
                return result
            except (FetchError, NotImplementedError) as e:
                logger.error(
                    "备用源 %s.%s 也失败: %s",
                    self.fallback.source_name, method_name, e,
                )

        raise SourceUnavailableError(
            f"所有数据源的 {method_name} 均不可用",
            source="router",
        )

    def _call(
        self, fetcher: BaseFetcher, method_name: str, **kwargs
    ) -> pd.DataFrame:
        fn: Callable = getattr(fetcher, method_name)
        return fn(**kwargs)

    # ---- 降级与恢复 ----

    def _on_success(self) -> None:
        self._failure_count = 0
        if self._degraded:
            logger.info("主数据源 %s 已恢复", self.primary.source_name)
            self._degraded = False

    def _on_failure(self, error: Exception) -> None:
        self._failure_count += 1
        logger.warning(
            "主数据源 %s 失败 (%d/%d): %s",
            self.primary.source_name,
            self._failure_count,
            self._max_failures,
            error,
        )
        if self._failure_count >= self._max_failures:
            self._degraded = True
            logger.error(
                "主数据源 %s 连续失败 %d 次，进入降级模式",
                self.primary.source_name,
                self._failure_count,
            )

    def _try_recovery(self, method_name: str, kwargs: dict) -> None:
        now = time.monotonic()
        if now - self._last_recovery_attempt < self._recovery_interval:
            return

        self._last_recovery_attempt = now
        logger.info("探测主数据源 %s 是否恢复...", self.primary.source_name)

        try:
            self._call(self.primary, method_name, **kwargs)
            self._on_success()
            logger.info("主数据源 %s 恢复正常!", self.primary.source_name)
        except Exception:
            logger.info("主数据源 %s 仍不可用", self.primary.source_name)

    # ---- 生命周期 ----

    def connect_all(self) -> None:
        for fetcher in [self.primary, self.fallback, self.supplementary]:
            if fetcher:
                try:
                    fetcher.connect()
                except Exception as e:
                    logger.warning("连接 %s 失败: %s", fetcher.source_name, e)

    def close_all(self) -> None:
        for fetcher in [self.primary, self.fallback, self.supplementary]:
            if fetcher:
                try:
                    fetcher.close()
                except Exception:
                    pass

    def __enter__(self):
        self.connect_all()
        return self

    def __exit__(self, *args):
        self.close_all()

    # ---- 状态查询 ----

    @property
    def is_degraded(self) -> bool:
        return self._degraded

    @property
    def active_source(self) -> str:
        if self._degraded and self.fallback:
            return self.fallback.source_name
        return self.primary.source_name

    def health_check(self) -> dict[str, bool]:
        result = {}
        for fetcher in [self.primary, self.fallback, self.supplementary]:
            if fetcher:
                try:
                    fetcher.get_stock_list()
                    result[fetcher.source_name] = True
                except Exception:
                    result[fetcher.source_name] = False
        return result
