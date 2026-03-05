"""
动量类因子。

动量 (Momentum) 因子基于 "强者恒强" 假设:
过去表现好的股票未来仍倾向表现好。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategy.factors.base import BaseFactor, FactorMeta

__all__ = [
    "ReturnN",
    "RelativeStrength",
    "PriceMA_Ratio",
    "Momentum12_1",
    "ExponentialMomentum",
]


class ReturnN(BaseFactor):
    """
    N 日收益率动量因子。

    factor = close_t / close_{t-N} - 1
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"return_{n}d",
            category="momentum",
            description=f"过去{n}日收益率",
            window=n,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return price_df / price_df.shift(self.n) - 1


class RelativeStrength(BaseFactor):
    """
    相对强弱因子。

    factor = stock_return_N / market_mean_return_N
    相对于市场平均的超额收益。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"relative_strength_{n}d",
            category="momentum",
            description=f"过去{n}日相对强弱",
            window=n,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        ret = price_df / price_df.shift(self.n) - 1
        market_ret = ret.mean(axis=1)
        return ret.sub(market_ret, axis=0)


class PriceMA_Ratio(BaseFactor):
    """
    价格/均线比值因子。

    factor = close / MA(close, N)
    > 1 表示价格在均线上方（多头排列）。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"price_ma{n}_ratio",
            category="momentum",
            description=f"价格/MA{n}比值",
            window=n,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        ma = price_df.rolling(self.n, min_periods=self.n).mean()
        return price_df / ma


class Momentum12_1(BaseFactor):
    """
    经典 12-1 动量因子 (Jegadeesh & Titman 1993)。

    factor = 过去12个月收益 - 最近1个月收益
    剔除短期反转效应，捕捉中期动量。
    """

    def __init__(self, long_period: int = 252, skip_period: int = 21):
        self.long_period = long_period
        self.skip_period = skip_period
        self.meta = FactorMeta(
            name="momentum_12_1",
            category="momentum",
            description="12个月动量(跳过最近1个月)",
            window=long_period,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        ret_12m = price_df / price_df.shift(self.long_period) - 1
        ret_1m = price_df / price_df.shift(self.skip_period) - 1
        return ret_12m - ret_1m


class ExponentialMomentum(BaseFactor):
    """
    指数加权动量因子。

    近期收益率权重更高，衰减半衰期为 halflife 天。
    """

    def __init__(self, n: int = 60, halflife: int = 20):
        self.n = n
        self.halflife = halflife
        self.meta = FactorMeta(
            name=f"exp_momentum_{n}d_hl{halflife}",
            category="momentum",
            description=f"指数加权动量({n}日/半衰期{halflife})",
            window=n,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        daily_ret = price_df.pct_change()
        weights = np.exp(-np.log(2) / self.halflife * np.arange(self.n)[::-1])
        weights = weights / weights.sum()

        def _ewm_sum(col: pd.Series) -> pd.Series:
            return col.rolling(self.n, min_periods=self.n).apply(
                lambda x: (x * weights).sum(), raw=True
            )

        return daily_ret.apply(_ewm_sum)
