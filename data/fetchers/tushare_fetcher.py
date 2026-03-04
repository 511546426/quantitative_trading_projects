"""
Tushare Pro 数据采集实现 — 主力数据源。

限制:
    - 免费账户每分钟 200 次请求
    - 部分接口需要 2000+ 积分
    - 单次返回上限 5000 条
"""
from __future__ import annotations

import logging

import pandas as pd

from data.common.exceptions import (
    AuthError,
    EmptyDataError,
    FetchConnectionError,
    FetchError,
    RateLimitError,
)
from data.common.rate_limiter import RateLimiter
from data.common.retry import retry
from data.fetchers.base import BaseFetcher

logger = logging.getLogger(__name__)


class TushareFetcher(BaseFetcher):
    """Tushare Pro 数据采集器"""

    source_name = "tushare"

    def __init__(self, token: str, rate_limiter: RateLimiter | None = None):
        rl = rate_limiter or RateLimiter(capacity=10, refill_rate=3.0)
        super().__init__(rate_limiter=rl)
        self._token = token
        self._api = None

    # ---- 生命周期 ----

    def connect(self) -> None:
        try:
            import tushare as ts
            ts.set_token(self._token)
            self._api = ts.pro_api()
            self._connected = True
            logger.info("Tushare Pro 连接成功")
        except Exception as e:
            raise AuthError(f"Tushare 连接失败: {e}", source=self.source_name) from e

    def close(self) -> None:
        self._api = None
        self._connected = False
        logger.info("Tushare Pro 连接关闭")

    # ---- 基础信息 ----

    @retry(max_retries=3, backoff=[1, 2, 4])
    def get_stock_list(self) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            fields = "ts_code,name,industry,market,list_date,delist_date"
            df_l = self._api.stock_basic(
                exchange="", list_status="L", fields=fields
            )
            df_d = self._api.stock_basic(
                exchange="", list_status="D", fields=fields
            )
            df = pd.concat([df_l, df_d], ignore_index=True)

            df["is_st"] = df["name"].str.contains("ST", na=False)
            df["is_delisted"] = df["delist_date"].notna()

            market_map = {"主板": "主板", "创业板": "创业板", "科创板": "科创板", "CDR": "CDR"}
            df["market"] = df["market"].map(market_map).fillna("其他")

            logger.info("获取股票列表: %d 条", len(df))
            return df
        except Exception as e:
            raise self._wrap_error(e) from e

    @retry(max_retries=3, backoff=[1, 2, 4])
    def get_trade_calendar(
        self, exchange: str = "SSE", start_date: str = "", end_date: str = ""
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            df = self._api.trade_cal(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                fields="exchange,cal_date,is_open",
            )
            logger.info("获取交易日历: %d 条", len(df))
            return df
        except Exception as e:
            raise self._wrap_error(e) from e

    # ---- 行情数据 ----

    @retry(max_retries=3, backoff=[1, 2, 4])
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
            kwargs: dict = {}
            if ts_code:
                kwargs["ts_code"] = ts_code
            if trade_date:
                kwargs["trade_date"] = trade_date
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date

            df = self._api.daily(**kwargs)
            if df is None or df.empty:
                raise EmptyDataError(
                    f"日K线数据为空 (params={kwargs})", source=self.source_name
                )

            df = self._rename_daily_columns(df)
            logger.info("获取日K线: %d 条", len(df))
            return df
        except EmptyDataError:
            raise
        except Exception as e:
            raise self._wrap_error(e) from e

    @retry(max_retries=3, backoff=[1, 2, 4])
    def get_adj_factor(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            kwargs: dict = {}
            if ts_code:
                kwargs["ts_code"] = ts_code
            if trade_date:
                kwargs["trade_date"] = trade_date
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date

            df = self._api.adj_factor(**kwargs)
            if df is None or df.empty:
                raise EmptyDataError(
                    f"复权因子为空 (params={kwargs})", source=self.source_name
                )

            logger.info("获取复权因子: %d 条", len(df))
            return df
        except EmptyDataError:
            raise
        except Exception as e:
            raise self._wrap_error(e) from e

    @retry(max_retries=3, backoff=[1, 2, 4])
    def get_index_daily(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            df = self._api.index_daily(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            if df is None or df.empty:
                raise EmptyDataError(
                    f"指数日线为空 ({ts_code})", source=self.source_name
                )
            df = self._rename_daily_columns(df)
            logger.info("获取指数日线 %s: %d 条", ts_code, len(df))
            return df
        except EmptyDataError:
            raise
        except Exception as e:
            raise self._wrap_error(e) from e

    # ---- 基本面数据 ----

    @retry(max_retries=3, backoff=[1, 2, 4])
    def get_financial_indicator(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            fields = (
                "ts_code,ann_date,end_date,roe,roa,"
                "grossprofit_margin,netprofit_yoy,or_yoy"
            )
            kwargs: dict = {"ts_code": ts_code, "fields": fields}
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date

            df = self._api.fina_indicator(**kwargs)
            if df is None or df.empty:
                raise EmptyDataError(
                    f"财务指标为空 ({ts_code})", source=self.source_name
                )

            rename = {
                "grossprofit_margin": "gross_margin",
                "netprofit_yoy": "net_profit_yoy",
                "or_yoy": "revenue_yoy",
            }
            df = df.rename(columns=rename)
            logger.info("获取财务指标 %s: %d 条", ts_code, len(df))
            return df
        except EmptyDataError:
            raise
        except Exception as e:
            raise self._wrap_error(e) from e

    @retry(max_retries=3, backoff=[1, 2, 4])
    def get_valuation(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            kwargs: dict = {
                "fields": "ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv"
            }
            if ts_code:
                kwargs["ts_code"] = ts_code
            if trade_date:
                kwargs["trade_date"] = trade_date
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date

            df = self._api.daily_basic(**kwargs)
            if df is None or df.empty:
                raise EmptyDataError(
                    f"估值数据为空 (params={kwargs})", source=self.source_name
                )

            logger.info("获取估值数据: %d 条", len(df))
            return df
        except EmptyDataError:
            raise
        except Exception as e:
            raise self._wrap_error(e) from e

    @retry(max_retries=3, backoff=[1, 2, 4])
    def get_dividend(
        self, ts_code: str | None = None, ann_date: str | None = None
    ) -> pd.DataFrame:
        self._ensure_connected()
        self._throttle()
        try:
            kwargs: dict = {
                "fields": "ts_code,ann_date,ex_date,div_proc,"
                          "stk_div,cash_div_tax"
            }
            if ts_code:
                kwargs["ts_code"] = ts_code
            if ann_date:
                kwargs["ann_date"] = ann_date

            df = self._api.dividend(**kwargs)
            if df is None or df.empty:
                raise EmptyDataError(
                    f"分红数据为空 (params={kwargs})", source=self.source_name
                )

            rename = {"cash_div_tax": "cash_div", "stk_div": "share_div"}
            df = df.rename(columns=rename)
            logger.info("获取分红数据: %d 条", len(df))
            return df
        except EmptyDataError:
            raise
        except Exception as e:
            raise self._wrap_error(e) from e

    # ---- 列名映射 ----

    @staticmethod
    def _rename_daily_columns(df: pd.DataFrame) -> pd.DataFrame:
        rename = {
            "vol": "volume",
            "turnover_rate": "turn",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        return df

    def _wrap_error(self, e: Exception) -> FetchError:
        msg = str(e)
        if "token" in msg.lower() or "权限" in msg:
            return AuthError(msg, source=self.source_name)
        if "频率" in msg or "limit" in msg.lower():
            return RateLimitError(msg, source=self.source_name, retry_after=60)
        if "timeout" in msg.lower() or "超时" in msg:
            return FetchConnectionError(msg, source=self.source_name)
        return FetchError(msg, source=self.source_name)
