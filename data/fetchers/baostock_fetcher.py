"""
BaoStock 数据采集实现 — 免费备用源。

特点:
    - 完全免费，无需 Token
    - 有状态连接（login/logout）
    - 数据更新略有延迟（T+1 晚更新完毕）
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


class BaoStockFetcher(BaseFetcher):
    """BaoStock 数据采集器"""

    source_name = "baostock"

    def __init__(self, rate_limiter: RateLimiter | None = None):
        rl = rate_limiter or RateLimiter(capacity=5, refill_rate=2.0)
        super().__init__(rate_limiter=rl)
        self._bs = None

    # ---- 生命周期 ----

    def connect(self) -> None:
        try:
            import baostock as bs
            self._bs = bs
            result = bs.login()
            if result.error_code != "0":
                raise FetchConnectionError(
                    f"BaoStock login 失败: {result.error_msg}",
                    source=self.source_name,
                )
            self._connected = True
            logger.info("BaoStock 连接成功")
        except FetchConnectionError:
            raise
        except Exception as e:
            raise FetchConnectionError(
                f"BaoStock 连接异常: {e}", source=self.source_name
            ) from e

    def close(self) -> None:
        if self._bs and self._connected:
            self._bs.logout()
        self._connected = False
        logger.info("BaoStock 连接关闭")

    # ---- 基础信息 ----

    @retry(max_retries=2, backoff=[1, 3])
    def get_stock_list(self) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            rs = self._bs.query_stock_basic()
            rows = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            df = pd.DataFrame(rows, columns=rs.fields)

            df = df.rename(columns={
                "code": "ts_code",
                "code_name": "name",
                "ipoDate": "list_date",
                "outDate": "delist_date",
                "industry": "industry",
            })
            df["ts_code"] = df["ts_code"].apply(_baostock_to_ts_code)
            df["list_date"] = df["list_date"].str.replace("-", "")
            df["delist_date"] = df["delist_date"].replace("", None)
            if "delist_date" in df.columns:
                df.loc[df["delist_date"].notna(), "delist_date"] = (
                    df.loc[df["delist_date"].notna(), "delist_date"]
                    .str.replace("-", "")
                )
            df["market"] = df["ts_code"].apply(_infer_market)
            df["is_st"] = df["name"].str.contains("ST", na=False)
            df["is_delisted"] = df["delist_date"].notna()

            logger.info("BaoStock 获取股票列表: %d 条", len(df))
            return df
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    def get_trade_calendar(
        self, exchange: str = "SSE", start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            s = _to_baostock_date(start_date) if start_date else ""
            e = _to_baostock_date(end_date) if end_date else ""
            rs = self._bs.query_trade_dates(start_date=s, end_date=e)
            rows = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            df = pd.DataFrame(rows, columns=rs.fields)

            df = df.rename(columns={
                "calendar_date": "cal_date",
                "is_trading_day": "is_open",
            })
            df["cal_date"] = df["cal_date"].str.replace("-", "")
            df["is_open"] = df["is_open"].astype(int)
            df["exchange"] = exchange
            logger.info("BaoStock 获取交易日历: %d 条", len(df))
            return df
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    # ---- 行情数据 ----

    @retry(max_retries=2, backoff=[1, 3])
    def get_daily_bars(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()

        if not ts_code:
            raise FetchError(
                "BaoStock 不支持按日期拉取全市场数据，请指定 ts_code",
                source=self.source_name,
            )

        try:
            bs_code = _ts_code_to_baostock(ts_code)
            s = _to_baostock_date(start_date) if start_date else ""
            e = _to_baostock_date(end_date) if end_date else ""

            rs = self._bs.query_history_k_data_plus(
                code=bs_code,
                fields="date,code,open,high,low,close,volume,amount,pctChg,turn",
                start_date=s,
                end_date=e,
                frequency="d",
                adjustflag="3",  # 不复权
            )

            rows = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())

            if not rows:
                raise EmptyDataError(
                    f"BaoStock 日K线为空 ({ts_code})", source=self.source_name
                )

            df = pd.DataFrame(rows, columns=rs.fields)
            df = self._normalize(df, ts_code)
            logger.info("BaoStock 获取日K线 %s: %d 条", ts_code, len(df))
            return df
        except (EmptyDataError, FetchError):
            raise
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    @retry(max_retries=2, backoff=[1, 3])
    def get_adj_factor(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """BaoStock 通过对比复权/不复权价格推算复权因子"""
        self._ensure_connected()
        self._throttle()

        if not ts_code:
            raise FetchError(
                "BaoStock get_adj_factor 需要指定 ts_code",
                source=self.source_name,
            )

        try:
            bs_code = _ts_code_to_baostock(ts_code)
            s = _to_baostock_date(start_date) if start_date else ""
            e = _to_baostock_date(end_date) if end_date else ""

            rs_raw = self._bs.query_history_k_data_plus(
                code=bs_code, fields="date,close",
                start_date=s, end_date=e, frequency="d", adjustflag="3",
            )
            rows_raw = []
            while rs_raw.error_code == "0" and rs_raw.next():
                rows_raw.append(rs_raw.get_row_data())

            self._throttle()
            rs_adj = self._bs.query_history_k_data_plus(
                code=bs_code, fields="date,close",
                start_date=s, end_date=e, frequency="d", adjustflag="1",
            )
            rows_adj = []
            while rs_adj.error_code == "0" and rs_adj.next():
                rows_adj.append(rs_adj.get_row_data())

            df_raw = pd.DataFrame(rows_raw, columns=["date", "close_raw"])
            df_adj = pd.DataFrame(rows_adj, columns=["date", "close_adj"])

            df = df_raw.merge(df_adj, on="date")
            df["close_raw"] = pd.to_numeric(df["close_raw"], errors="coerce")
            df["close_adj"] = pd.to_numeric(df["close_adj"], errors="coerce")
            df["adj_factor"] = df["close_adj"] / df["close_raw"]
            df["ts_code"] = ts_code
            df["trade_date"] = df["date"].str.replace("-", "")

            result = df[["ts_code", "trade_date", "adj_factor"]].copy()
            logger.info("BaoStock 获取复权因子 %s: %d 条", ts_code, len(result))
            return result
        except Exception as e:
            raise FetchError(str(e), source=self.source_name) from e

    def get_index_daily(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        raise NotImplementedError("BaoStock 指数日线未实现，请使用 Tushare")

    # ---- 基本面 ----

    def get_financial_indicator(
        self, ts_code: str, start_date: str | None = None, end_date: str | None = None
    ) -> pd.DataFrame:
        raise NotImplementedError("BaoStock 财务指标未实现，请使用 Tushare")

    def get_valuation(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("BaoStock 估值数据未实现，请使用 Tushare")

    def get_dividend(
        self, ts_code: str | None = None, ann_date: str | None = None
    ) -> pd.DataFrame:
        raise NotImplementedError("BaoStock 分红数据未实现，请使用 Tushare")

    # ---- 内部方法 ----

    @staticmethod
    def _normalize(df: pd.DataFrame, ts_code: str) -> pd.DataFrame:
        rename = {
            "date": "trade_date",
            "pctChg": "pct_chg",
        }
        df = df.rename(columns=rename)
        df["ts_code"] = ts_code
        df["trade_date"] = df["trade_date"].str.replace("-", "")

        numeric_cols = ["open", "high", "low", "close", "volume", "amount",
                        "pct_chg", "turn"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df


# ================================================================
# 代码格式转换
# ================================================================

def _baostock_to_ts_code(bs_code: str) -> str:
    """sh.600000 -> 600000.SH"""
    parts = bs_code.split(".")
    if len(parts) == 2:
        return f"{parts[1]}.{parts[0].upper()}"
    return bs_code


def _ts_code_to_baostock(ts_code: str) -> str:
    """600000.SH -> sh.600000"""
    parts = ts_code.split(".")
    if len(parts) == 2:
        return f"{parts[1].lower()}.{parts[0]}"
    return ts_code


def _to_baostock_date(date_str: str) -> str:
    """20260226 -> 2026-02-26"""
    d = date_str.replace("-", "")
    if len(d) == 8:
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return date_str


def _infer_market(ts_code: str) -> str:
    code = ts_code.split(".")[0]
    if code.startswith("688"):
        return "科创板"
    if code.startswith(("300", "301")):
        return "创业板"
    if code.startswith("8"):
        return "北交所"
    return "主板"
