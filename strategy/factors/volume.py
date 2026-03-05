"""
成交量类因子。

量价关系是技术分析的核心: "量在价先"。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategy.factors.base import BaseFactor, FactorMeta

__all__ = [
    "VolumeRatio",
    "OBV_Divergence",
    "AmountMA_Ratio",
    "TurnoverN",
    "VolumePrice_Corr",
]


class VolumeRatio(BaseFactor):
    """
    量比因子。

    factor = 近 n1 日平均成交量 / 近 n2 日平均成交量
    > 1 表示近期放量，< 1 表示缩量。
    """

    def __init__(self, n_short: int = 5, n_long: int = 20):
        self.n_short = n_short
        self.n_long = n_long
        self.meta = FactorMeta(
            name=f"volume_ratio_{n_short}_{n_long}",
            category="volume",
            description=f"量比({n_short}/{n_long}日)",
            window=n_long,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        volume_df = kwargs.get("volume_df", price_df)
        short_avg = volume_df.rolling(self.n_short, min_periods=self.n_short).mean()
        long_avg = volume_df.rolling(self.n_long, min_periods=self.n_long).mean()
        return short_avg / long_avg.replace(0, np.nan)


class OBV_Divergence(BaseFactor):
    """
    OBV 背离因子。

    OBV = 累积 (涨日成交量 - 跌日成交量)。
    当价格创新低而 OBV 没有创新低 → 底部背离 (看涨)。
    简化版: 计算 OBV 的 N 日变化率 vs 价格 N 日变化率的差。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"obv_divergence_{n}d",
            category="volume",
            description=f"OBV背离({n}日)",
            window=n + 1,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        volume_df = kwargs.get("volume_df", price_df)
        close_df = price_df

        sign = np.sign(close_df.pct_change())
        obv = (sign * volume_df).cumsum()

        obv_chg = obv / obv.shift(self.n) - 1
        price_chg = close_df / close_df.shift(self.n) - 1

        return obv_chg - price_chg


class AmountMA_Ratio(BaseFactor):
    """
    成交额/均量比。

    factor = 当日成交额 / 过去 N 日平均成交额
    突然放量可能是资金进入信号。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"amount_ma{n}_ratio",
            category="volume",
            description=f"成交额/MA{n}成交额",
            window=n,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        amount_df = kwargs.get("amount_df", price_df)
        ma = amount_df.rolling(self.n, min_periods=self.n).mean()
        return amount_df / ma.replace(0, np.nan)


class TurnoverN(BaseFactor):
    """
    N 日平均换手率因子。

    换手率高 → 活跃度高 / 分歧大; 换手率低 → 关注度低。
    低换手率因子在 A 股有一定超额。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"turnover_{n}d",
            category="volume",
            description=f"过去{n}日平均换手率",
            window=n,
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        turn_df = kwargs.get("turn_df", price_df)
        return turn_df.rolling(self.n, min_periods=self.n).mean()


class VolumePrice_Corr(BaseFactor):
    """
    量价相关性因子。

    过去 N 日成交量与收益率的相关系数。
    正相关 → 健康上涨; 负相关 → 放量下跌(不健康)。
    """

    def __init__(self, n: int = 20):
        self.n = n
        self.meta = FactorMeta(
            name=f"vol_price_corr_{n}d",
            category="volume",
            description=f"量价相关性({n}日)",
            window=n + 1,
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        volume_df = kwargs.get("volume_df", price_df)
        daily_ret = price_df.pct_change()

        result = pd.DataFrame(index=price_df.index, columns=price_df.columns, dtype=float)
        for col in price_df.columns:
            result[col] = daily_ret[col].rolling(self.n, min_periods=self.n).corr(
                volume_df[col]
            )
        return result
