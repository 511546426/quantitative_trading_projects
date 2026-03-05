"""
因子 IC (Information Coefficient) 分析框架。

IC = 因子值与未来 N 日收益率的 Spearman 截面相关系数。
ICIR = mean(IC) / std(IC)，衡量因子有效性的稳定性。

好的因子标准: |IC| > 0.03, ICIR > 0.5。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from strategy.factors.base import BaseFactor

logger = logging.getLogger(__name__)


@dataclass
class ICReport:
    """因子 IC 分析报告"""
    factor_name: str
    forward_days: int
    ic_series: pd.Series
    ic_by_year: pd.Series | None = None
    layered_returns: pd.DataFrame | None = None
    metadata: dict = field(default_factory=dict)

    @property
    def ic_mean(self) -> float:
        return self.ic_series.mean()

    @property
    def ic_std(self) -> float:
        return self.ic_series.std()

    @property
    def icir(self) -> float:
        if self.ic_std == 0:
            return 0.0
        return self.ic_mean / self.ic_std

    @property
    def ic_positive_ratio(self) -> float:
        """IC > 0 的天数比例"""
        return (self.ic_series > 0).mean()

    @property
    def ic_abs_mean(self) -> float:
        return self.ic_series.abs().mean()

    def summary(self) -> dict[str, Any]:
        return {
            "factor": self.factor_name,
            "forward_days": self.forward_days,
            "ic_mean": round(self.ic_mean, 4),
            "ic_std": round(self.ic_std, 4),
            "icir": round(self.icir, 4),
            "|ic|_mean": round(self.ic_abs_mean, 4),
            "ic_positive_ratio": round(self.ic_positive_ratio, 4),
            "n_periods": len(self.ic_series),
        }

    def __repr__(self) -> str:
        return (
            f"ICReport({self.factor_name}, "
            f"IC={self.ic_mean:.4f}, "
            f"ICIR={self.icir:.4f}, "
            f"IC>0={self.ic_positive_ratio:.1%})"
        )


class ICAnalyzer:
    """
    因子 IC 分析器。

    核心功能:
    1. 单因子 IC 时间序列
    2. ICIR 计算
    3. 分层回测 (十分组)
    4. 因子衰减测试
    """

    def __init__(self, min_stocks: int = 30):
        self.min_stocks = min_stocks

    def calc_ic(
        self,
        factor_df: pd.DataFrame,
        return_df: pd.DataFrame,
        forward_n: int = 5,
    ) -> ICReport:
        """
        计算因子 IC 时间序列。

        Parameters
        ----------
        factor_df : DataFrame
            pivot 格式因子值, index=trade_date, columns=ts_code。
        return_df : DataFrame
            pivot 格式收益率 (close), index=trade_date, columns=ts_code。
            内部会自动计算 forward_n 日收益率。
        forward_n : int
            因子预测未来 N 个交易日的收益率。

        Returns
        -------
        ICReport
        """
        fwd_return = return_df.pct_change(forward_n).shift(-forward_n)

        common_dates = factor_df.index.intersection(fwd_return.index)
        common_codes = factor_df.columns.intersection(fwd_return.columns)

        factor_aligned = factor_df.loc[common_dates, common_codes]
        return_aligned = fwd_return.loc[common_dates, common_codes]

        ic_values = []
        ic_dates = []

        for dt in common_dates:
            f_row = factor_aligned.loc[dt].dropna()
            r_row = return_aligned.loc[dt].dropna()
            common = f_row.index.intersection(r_row.index)

            if len(common) < self.min_stocks:
                continue

            corr, _ = spearmanr(f_row[common].values, r_row[common].values)
            if not np.isnan(corr):
                ic_values.append(corr)
                ic_dates.append(dt)

        ic_series = pd.Series(ic_values, index=ic_dates, name="IC")

        ic_by_year = None
        if len(ic_series) > 0:
            year_idx = pd.to_datetime(ic_series.index).year
            ic_by_year = ic_series.groupby(year_idx).agg(["mean", "std", "count"])
            ic_by_year.columns = ["ic_mean", "ic_std", "n_periods"]
            ic_by_year["icir"] = ic_by_year["ic_mean"] / ic_by_year["ic_std"]

        report = ICReport(
            factor_name=str(factor_df.columns.name or "unknown"),
            forward_days=forward_n,
            ic_series=ic_series,
            ic_by_year=ic_by_year,
        )
        logger.info("IC 分析: %s", report)
        return report

    def calc_factor_ic(
        self,
        factor: BaseFactor,
        price_df: pd.DataFrame,
        forward_n: int = 5,
        **kwargs,
    ) -> ICReport:
        """
        直接对 BaseFactor 实例做 IC 分析。

        Parameters
        ----------
        factor : BaseFactor
            因子实例。
        price_df : DataFrame
            pivot 格式 close 价格。
        forward_n : int
            预测天数。
        """
        factor_values = factor.compute(price_df, **kwargs)
        report = self.calc_ic(factor_values, price_df, forward_n)
        report.factor_name = factor.name
        return report

    def layered_backtest(
        self,
        factor_df: pd.DataFrame,
        return_df: pd.DataFrame,
        n_groups: int = 10,
        forward_n: int = 5,
    ) -> pd.DataFrame:
        """
        分层回测 (十分组)。

        将股票按因子值从小到大排为 n_groups 组，
        计算每组的平均 forward_n 日收益率，检验因子单调性。

        Parameters
        ----------
        factor_df : DataFrame
            pivot 格式因子值。
        return_df : DataFrame
            pivot 格式收益率 (close)。
        n_groups : int
            分组数 (默认 10)。
        forward_n : int
            持仓天数。

        Returns
        -------
        DataFrame
            index=date, columns=Group_1..Group_N, 各组平均收益。
        """
        fwd_return = return_df.pct_change(forward_n).shift(-forward_n)
        common_dates = factor_df.index.intersection(fwd_return.index)
        common_codes = factor_df.columns.intersection(fwd_return.columns)

        group_returns = {f"G{i+1}": [] for i in range(n_groups)}
        valid_dates = []

        for dt in common_dates:
            f_row = factor_df.loc[dt, common_codes].dropna()
            r_row = fwd_return.loc[dt, common_codes].dropna()
            common = f_row.index.intersection(r_row.index)

            if len(common) < n_groups * 5:
                continue

            f_sorted = f_row[common].sort_values()
            groups = np.array_split(f_sorted.index, n_groups)

            for i, grp in enumerate(groups):
                group_returns[f"G{i+1}"].append(r_row[grp].mean())

            valid_dates.append(dt)

        result = pd.DataFrame(group_returns, index=valid_dates)
        return result

    def factor_decay(
        self,
        factor_df: pd.DataFrame,
        return_df: pd.DataFrame,
        max_forward: int = 20,
        step: int = 1,
    ) -> pd.DataFrame:
        """
        因子衰减测试。

        计算不同持仓天数下的 IC，观察因子的预测力如何随时间衰减。

        Returns
        -------
        DataFrame
            index=forward_days, columns=[ic_mean, icir]
        """
        results = []
        for fwd in range(1, max_forward + 1, step):
            report = self.calc_ic(factor_df, return_df, forward_n=fwd)
            results.append({
                "forward_days": fwd,
                "ic_mean": report.ic_mean,
                "icir": report.icir,
                "ic_abs_mean": report.ic_abs_mean,
            })
        return pd.DataFrame(results).set_index("forward_days")


def batch_ic_analysis(
    factors: list[BaseFactor],
    price_df: pd.DataFrame,
    forward_n: int = 5,
    min_stocks: int = 30,
    **kwargs,
) -> pd.DataFrame:
    """
    批量因子 IC 分析。

    Returns
    -------
    DataFrame
        每行一个因子, 列为 ic_mean / icir / ic_positive_ratio 等。
    """
    analyzer = ICAnalyzer(min_stocks=min_stocks)
    summaries = []
    for factor in factors:
        try:
            report = analyzer.calc_factor_ic(factor, price_df, forward_n, **kwargs)
            summaries.append(report.summary())
        except Exception as e:
            logger.warning("因子 %s IC 计算失败: %s", factor.name, e)
            summaries.append({"factor": factor.name, "error": str(e)})
    return pd.DataFrame(summaries)
