"""
数据采集抽象基类。

设计约定:
    1. 所有方法返回 pd.DataFrame，列名使用统一命名规范
    2. 日期格式统一为 'YYYYMMDD' 字符串
    3. 股票代码格式统一为 '{code}.{exchange}'，如 '000001.SZ'
    4. 方法抛出 FetchError 而非底层异常
    5. 每个方法自带速率限制（由子类配置的 RateLimiter 处理）
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import pandas as pd

from data.common.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """数据采集抽象基类"""

    source_name: str = "base"

    def __init__(self, rate_limiter: RateLimiter | None = None):
        self._rate_limiter = rate_limiter or RateLimiter()
        self._connected = False

    # ================================================================
    # 基础信息
    # ================================================================

    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取全部 A 股股票列表。

        Returns
        -------
        DataFrame
            ts_code, name, industry, market, list_date, delist_date, is_st
        """
        ...

    @abstractmethod
    def get_trade_calendar(
        self, exchange: str = "SSE", start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        """
        获取交易日历。

        Returns
        -------
        DataFrame
            exchange, cal_date, is_open
        """
        ...

    # ================================================================
    # 行情数据
    # ================================================================

    @abstractmethod
    def get_daily_bars(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        获取日K线数据。

        两种查询模式:
        - 按股票: ts_code + start_date/end_date
        - 按日期: trade_date（全市场当日数据）

        Returns
        -------
        DataFrame
            ts_code, trade_date, open, high, low, close, volume, amount, pct_chg, turn
        """
        ...

    @abstractmethod
    def get_adj_factor(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        获取复权因子。

        Returns
        -------
        DataFrame
            ts_code, trade_date, adj_factor
        """
        ...

    @abstractmethod
    def get_index_daily(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        获取指数日线。

        Returns
        -------
        DataFrame
            ts_code, trade_date, open, high, low, close, volume, amount, pct_chg
        """
        ...

    # ================================================================
    # 基本面数据
    # ================================================================

    @abstractmethod
    def get_financial_indicator(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        获取财务指标。

        Returns
        -------
        DataFrame
            ts_code, ann_date, end_date, roe, roa, gross_margin,
            net_profit_yoy, revenue_yoy
        """
        ...

    @abstractmethod
    def get_valuation(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        获取每日估值指标。

        Returns
        -------
        DataFrame
            ts_code, trade_date, pe_ttm, pb, ps_ttm, total_mv, circ_mv
        """
        ...

    @abstractmethod
    def get_dividend(
        self, ts_code: str | None = None, ann_date: str | None = None
    ) -> pd.DataFrame:
        """
        获取分红送股数据。

        Returns
        -------
        DataFrame
            ts_code, ann_date, ex_date, div_proc, cash_div, share_div
        """
        ...

    # ================================================================
    # 生命周期
    # ================================================================

    @abstractmethod
    def connect(self) -> None:
        """初始化连接"""
        ...

    @abstractmethod
    def close(self) -> None:
        """释放资源"""
        ...

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    # ================================================================
    # 工具方法
    # ================================================================

    def _throttle(self) -> None:
        """调用前获取令牌，确保不超频"""
        self._rate_limiter.acquire()

    def _ensure_connected(self) -> None:
        if not self._connected:
            self.connect()
