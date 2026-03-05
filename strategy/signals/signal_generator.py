"""
信号生成器。

将一个或多个因子组合为交易信号矩阵。
支持: 单因子信号、多因子等权合成、IC 加权合成。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from strategy.factors.base import BaseFactor

logger = logging.getLogger(__name__)


@dataclass
class SignalConfig:
    """信号生成配置"""
    top_n: int = 20                # 选股数量
    bottom_n: int = 0              # 做空数量 (A 股通常为 0)
    neutralize_industry: bool = False
    neutralize_market_cap: bool = False


class SignalGenerator:
    """
    信号生成器。

    将因子值转换为标准化的交易信号矩阵:
    正值=做多信号, 0=无持仓, 负值=做空信号。
    """

    def __init__(self, config: SignalConfig | None = None):
        self.config = config or SignalConfig()

    def from_single_factor(
        self,
        factor: BaseFactor,
        price_df: pd.DataFrame,
        **kwargs,
    ) -> pd.DataFrame:
        """
        单因子生成信号。

        按因子方向选 top_n 股票, 信号值=因子截面排名。
        """
        factor_values = factor.compute(price_df, **kwargs)
        direction = factor.meta.direction
        if direction == -1:
            factor_values = -factor_values

        return self._top_n_signal(factor_values)

    def from_multi_factor(
        self,
        factors: list[BaseFactor],
        price_df: pd.DataFrame,
        weights: list[float] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        多因子合成信号。

        Parameters
        ----------
        factors : list[BaseFactor]
            因子列表。
        price_df : DataFrame
            pivot close 价格。
        weights : list[float], optional
            因子权重, 默认等权。
        """
        if weights is None:
            weights = [1.0 / len(factors)] * len(factors)

        combined = None
        for factor, w in zip(factors, weights):
            fv = factor.compute(price_df, **kwargs)
            if factor.meta.direction == -1:
                fv = -fv
            ranked = fv.rank(axis=1, pct=True)
            if combined is None:
                combined = ranked * w
            else:
                common_dates = combined.index.intersection(ranked.index)
                common_codes = combined.columns.intersection(ranked.columns)
                combined = combined.loc[common_dates, common_codes]
                combined = combined + ranked.loc[common_dates, common_codes] * w

        return self._top_n_signal(combined)

    def from_ic_weighted(
        self,
        factors: list[BaseFactor],
        price_df: pd.DataFrame,
        ic_window: int = 60,
        forward_n: int = 5,
        **kwargs,
    ) -> pd.DataFrame:
        """
        IC 加权合成信号。

        用过去 ic_window 天的滚动 IC 作为因子权重,
        IC 高的因子权重大。
        """
        from strategy.analysis.ic_analyzer import ICAnalyzer
        analyzer = ICAnalyzer()

        factor_values_list = []
        for factor in factors:
            fv = factor.compute(price_df, **kwargs)
            if factor.meta.direction == -1:
                fv = -fv
            factor_values_list.append(fv.rank(axis=1, pct=True))

        from scipy.stats import spearmanr
        fwd_ret = price_df.pct_change(forward_n).shift(-forward_n)

        combined = pd.DataFrame(0.0, index=price_df.index, columns=price_df.columns)
        for fv in factor_values_list:
            rolling_ic = pd.Series(0.0, index=price_df.index)
            common = fv.columns.intersection(fwd_ret.columns)

            for i in range(ic_window, len(price_df.index)):
                window_ics = []
                for j in range(max(0, i - ic_window), i):
                    dt = price_df.index[j]
                    f_row = fv.loc[dt, common].dropna()
                    r_row = fwd_ret.loc[dt, common].dropna()
                    overlap = f_row.index.intersection(r_row.index)
                    if len(overlap) >= 30:
                        corr, _ = spearmanr(f_row[overlap], r_row[overlap])
                        if not np.isnan(corr):
                            window_ics.append(corr)
                if window_ics:
                    rolling_ic.iloc[i] = np.mean(window_ics)

            ic_weight = rolling_ic.clip(lower=0)
            for dt in price_df.index:
                if ic_weight.loc[dt] > 0:
                    combined.loc[dt] += fv.loc[dt].fillna(0) * ic_weight.loc[dt]

        return self._top_n_signal(combined)

    def _top_n_signal(self, scored: pd.DataFrame) -> pd.DataFrame:
        """选出截面 top_n 生成信号矩阵"""
        signals = pd.DataFrame(0.0, index=scored.index, columns=scored.columns)
        for dt in scored.index:
            row = scored.loc[dt].dropna()
            if len(row) == 0:
                continue
            top = row.nlargest(self.config.top_n)
            signals.loc[dt, top.index] = top.rank(ascending=True)
        return signals
