"""
波动率类因子。

低波动率因子在学术和实务中均有稳健的超额收益 (Low Volatility Anomaly)。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategy.factors.base import BaseFactor, FactorMeta

__all__ = [
    "RealizedVolatility",
    "ATR_Factor",
    "DownsideVolatility",
    "VolatilityChange",
]


class RealizedVolatility(BaseFactor):
    """
    已实现波动率因子。

    factor = std(daily_return, N)
    低波动股票长期跑赢高波动股票 (方向=-1)。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"realized_vol_{n}d",
            category="volatility",
            description=f"已实现波动率({n}日)",
            window=n + 1,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        daily_ret = price_df.pct_change(fill_method=None)
        return daily_ret.rolling(self.n, min_periods=self.n).std()


class ATR_Factor(BaseFactor):
    """
    ATR (Average True Range) 因子。

    ATR 衡量绝对波动幅度，除以 close 得到相对 ATR。
    """

    def __init__(self, n: int = 14):
        self.n = n
        self.meta = FactorMeta(
            name=f"atr_{n}d",
            category="volatility",
            description=f"相对ATR({n}日)",
            window=n + 1,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        high_df = kwargs.get("high_df")
        low_df = kwargs.get("low_df")
        close_df = price_df

        if high_df is None or low_df is None:
            daily_ret = close_df.pct_change().abs()
            return daily_ret.rolling(self.n, min_periods=self.n).mean()

        prev_close = close_df.shift(1)
        tr1 = high_df - low_df
        tr2 = (high_df - prev_close).abs()
        tr3 = (low_df - prev_close).abs()
        true_range = pd.concat([tr1, tr2, tr3]).max(level=0) if False else tr1
        true_range = np.maximum(np.maximum(tr1, tr2), tr3)
        atr = true_range.rolling(self.n, min_periods=self.n).mean()
        return atr / close_df


class DownsideVolatility(BaseFactor):
    """
    下行波动率因子。

    只计算负收益的标准差，衡量下行风险。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"downside_vol_{n}d",
            category="volatility",
            description=f"下行波动率({n}日)",
            window=n + 1,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        daily_ret = price_df.pct_change()
        downside = daily_ret.clip(upper=0)
        return downside.rolling(self.n, min_periods=self.n).std()


class VolatilityChange(BaseFactor):
    """
    波动率变化因子。

    factor = vol_short / vol_long - 1
    波动率收敛可能预示突破。
    """

    def __init__(self, n_short: int = 5, n_long: int = 20):
        self.n_short = n_short
        self.n_long = n_long
        self.meta = FactorMeta(
            name=f"vol_change_{n_short}_{n_long}",
            category="volatility",
            description=f"波动率变化({n_short}/{n_long})",
            window=n_long + 1,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        daily_ret = price_df.pct_change()
        vol_short = daily_ret.rolling(self.n_short, min_periods=self.n_short).std()
        vol_long = daily_ret.rolling(self.n_long, min_periods=self.n_long).std()
        return vol_short / vol_long.replace(0, np.nan) - 1
