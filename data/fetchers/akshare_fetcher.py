"""
AKShare 数据采集实现 — 补充数据源。

主要用于:
    - 北向资金
    - 龙虎榜
    - 融资融券
    - Tushare 降级时的日K线备用

限制:
    - 无官方限速，但频繁请求会被封 IP
    - 接口变化频繁，需注意版本适配
"""
from __future__ import annotations

import logging

import pandas as pd

from data.common.exceptions import (
    EmptyDataError,
    FetchConnectionError,
    FetchError,
)
from data.common.rate_limiter import RateLimiter
from data.common.retry import retry
from data.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)


class AKShareFetcher(BaseFetcher):
    """AKShare 数据采集器"""

    source_name = "akshare"

    def __init__(self, rate_limiter: RateLimiter | None = None):
        rl = rate_limiter or RateLimiter(capacity=2, refill_rate=1.0)
        super().__init__(rate_limiter=rl)

    def connect(self) -> None:
        try:
            import akshare  # noqa: F401
            self._connected = True
            logger.info("AKShare 就绪")
        except ImportError as e:
            raise FetchConnectionError(
                f"akshare 未安装: {e}", source=self.source_name
            ) from e

    def close(self) -> None:
        self._connected = False

    # ---- 基础信息 ----

    @retry(max_retries=2, backoff=[2, 5])
    def get_stock_list(self) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            import akshare as ak
            df = ak.stock_info_a_code_name()
            df = df.rename(columns={"code": "ts_code", "name": "name"})
            df["ts_code"] = df["ts_code"].apply(_to_ts_code)
            df["industry"] = ""
            df["market"] = df["ts_code"].apply(_infer_market)
            df["list_date"] = ""
            df["delist_date"] = None
            df["is_st"] = df["name"].str.contains("ST", na=False)
            df["is_delisted"] = False
            logger.info("AKShare 获取股票列表: %d 条", len(df))
            return df
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    @retry(max_retries=2, backoff=[2, 5])
    def get_trade_calendar(
        self, exchange: str = "SSE", start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        raise NotImplementedError("AKShare 不提供标准交易日历接口，请使用 Tushare")

    # ---- 行情数据 ----

    @retry(max_retries=2, backoff=[2, 5])
    def get_daily_bars(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            import akshare as ak

            if ts_code:
                symbol = ts_code.split(".")[0]
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date or "20100101",
                    end_date=end_date or "20991231",
                    adjust="",
                )
                df = self._normalize_hist(df, ts_code)
            elif trade_date:
                date_str = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
                df = ak.stock_zh_a_spot_em()
                df = self._normalize_spot(df, trade_date)
            else:
                raise FetchError(
                    "必须指定 ts_code 或 trade_date", source=self.source_name
                )

            if df.empty:
                raise EmptyDataError("日K线为空", source=self.source_name)

            logger.info("AKShare 获取日K线: %d 条", len(df))
            return df
        except (EmptyDataError, FetchError):
            raise
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    @retry(max_retries=2, backoff=[2, 5])
    def get_adj_factor(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("AKShare 不直接提供复权因子，请使用 Tushare")

    @retry(max_retries=2, backoff=[2, 5])
    def get_index_daily(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            import akshare as ak

            index_map = {
                "000300.SH": "000300",
                "000905.SH": "000905",
                "000001.SH": "000001",
            }
            symbol = index_map.get(ts_code, ts_code.split(".")[0])
            df = ak.stock_zh_index_daily(symbol=f"sh{symbol}")

            df = df.rename(columns={
                "date": "trade_date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            })
            df["ts_code"] = ts_code
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
            df["amount"] = 0.0
            df["pct_chg"] = df["close"].pct_change() * 100

            mask = (df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)
            df = df[mask].reset_index(drop=True)

            logger.info("AKShare 获取指数日线 %s: %d 条", ts_code, len(df))
            return df
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    # ---- 基本面（有限支持） ----

    def get_financial_indicator(
        self, ts_code: str, start_date: str | None = None, end_date: str | None = None
    ) -> pd.DataFrame:
        raise NotImplementedError("AKShare 财务指标接口不稳定，请使用 Tushare")

    def get_valuation(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("AKShare 估值接口不稳定，请使用 Tushare")

    def get_dividend(
        self, ts_code: str | None = None, ann_date: str | None = None
    ) -> pd.DataFrame:
        raise NotImplementedError("AKShare 分红接口不稳定，请使用 Tushare")

    # ---- 特有数据（Tushare 没有的） ----

    @retry(max_retries=2, backoff=[2, 5])
    def get_north_flow(self, trade_date: str) -> pd.DataFrame:
        """获取北向资金数据"""
        self._ensure_connected()
        self._throttle()
        try:
            import akshare as ak
            df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            df["trade_date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
            logger.info("AKShare 获取北向资金: %d 条", len(df))
            return df
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    # ---- 内部方法 ----

    @staticmethod
    def _normalize_hist(df: pd.DataFrame, ts_code: str) -> pd.DataFrame:
        col_map = {
            "日期": "trade_date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
            "涨跌幅": "pct_chg",
            "换手率": "turn",
        }
        df = df.rename(columns=col_map)
        df["ts_code"] = ts_code
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @staticmethod
    def _normalize_spot(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        col_map = {
            "代码": "ts_code",
            "今开": "open",
            "最高": "high",
            "最低": "low",
            "最新价": "close",
            "成交量": "volume",
            "成交额": "amount",
            "涨跌幅": "pct_chg",
            "换手率": "turn",
        }
        df = df.rename(columns=col_map)
        if "ts_code" in df.columns:
            df["ts_code"] = df["ts_code"].apply(_to_ts_code)
        df["trade_date"] = trade_date
        return df


def _to_ts_code(code: str) -> str:
    """转换纯数字代码为 ts_code 格式"""
    code = str(code).zfill(6)
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    elif code.startswith(("0", "2", "3")):
        return f"{code}.SZ"
    elif code.startswith("8"):
        return f"{code}.BJ"
    return f"{code}.SZ"


def _infer_market(ts_code: str) -> str:
    code = ts_code.split(".")[0]
    if code.startswith("688"):
        return "科创板"
    if code.startswith(("300", "301")):
        return "创业板"
    if code.startswith("8"):
        return "北交所"
    return "主板"
