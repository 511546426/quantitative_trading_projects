"""
均值回归类因子。

均值回归假设: 价格偏离均衡后会回归，超跌反弹、超涨回落。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategy.factors.base import BaseFactor, FactorMeta

__all__ = [
    "RSI",
    "BollingerBandPosition",
    "Bias",
    "OverExtended",
]


class RSI(BaseFactor):
    """
    RSI (Relative Strength Index) 因子。

    RSI < 30 → 超卖信号 (做多);  RSI > 70 → 超买信号 (做空/回避)。
    作为因子使用: 1 - RSI/100，值越大越超卖(越看好)。
    """

    def __init__(self, n: int = 14):
        self.n = n
        self.meta = FactorMeta(
            name=f"rsi_{n}",
            category="mean_reversion",
            description=f"RSI({n})反转信号",
            window=n + 1,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        delta = price_df.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(span=self.n, adjust=False).mean()
        avg_loss = loss.ewm(span=self.n, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - 100 / (1 + rs)
        return rsi


class BollingerBandPosition(BaseFactor):
    """
    布林带位置因子。

    factor = (close - lower_band) / (upper_band - lower_band)
    值接近 0 → 在下轨附近(超卖); 值接近 1 → 在上轨附近(超买)。
    """

    def __init__(self, n: int = 20, n_std: float = 2.0):
        self.n = n
        self.n_std = n_std
        self.meta = FactorMeta(
            name=f"boll_pos_{n}",
            category="mean_reversion",
            description=f"布林带({n},{n_std})位置",
            window=n,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        ma = price_df.rolling(self.n, min_periods=self.n).mean()
        std = price_df.rolling(self.n, min_periods=self.n).std()
        upper = ma + self.n_std * std
        lower = ma - self.n_std * std
        width = (upper - lower).replace(0, np.nan)
        return (price_df - lower) / width


class Bias(BaseFactor):
    """
    乖离率因子 (BIAS)。

    factor = (close - MA_N) / MA_N
    正值代表偏高，负值代表偏低。做均值回归时，direction=-1。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"bias_{n}",
            category="mean_reversion",
            description=f"乖离率({n}日)",
            window=n,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        ma = price_df.rolling(self.n, min_periods=self.n).mean()
        return (price_df - ma) / ma


class OverExtended(BaseFactor):
    """
    超涨超跌因子。

    最近 N 天连续上涨/下跌天数。连续下跌天数多 → 超卖。
    factor = -(连涨天数 - 连跌天数)，越大越超卖。
    """

    def __init__(self, n: int = 10):
        self.n = n
        self.meta = FactorMeta(
            name=f"over_extended_{n}d",
            category="mean_reversion",
            description=f"过去{n}日涨跌天数差",
            window=n + 1,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        daily_sign = np.sign(price_df.pct_change())
        up_days = (daily_sign > 0).astype(float).rolling(self.n).sum()
        down_days = (daily_sign < 0).astype(float).rolling(self.n).sum()
        return -(up_days - down_days)
