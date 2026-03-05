"""
回测绩效指标。

提供全套策略评估指标，对齐 02_STRATEGY_LAYER.md 中定义的指标体系:
年化收益率、最大回撤、夏普比率、卡玛比率、换手率、胜率、盈亏比等。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


TRADING_DAYS_PER_YEAR = 252
RISK_FREE_RATE = 0.025  # 无风险利率 2.5%


@dataclass
class BacktestResult:
    """回测结果"""
    net_returns: pd.Series          # 每日净收益率
    gross_returns: pd.Series | None = None
    turnover: pd.Series | None = None
    positions: pd.DataFrame | None = None  # 每日持仓权重
    trades: list[dict] | None = None
    metadata: dict = field(default_factory=dict)

    @property
    def cumulative_returns(self) -> pd.Series:
        return (1 + self.net_returns).cumprod()

    @property
    def nav(self) -> pd.Series:
        """净值曲线"""
        return self.cumulative_returns

    def summary(self) -> dict[str, Any]:
        """计算完整的绩效指标摘要"""
        return calc_full_metrics(self.net_returns, self.turnover)


def calc_full_metrics(
    returns: pd.Series,
    turnover: pd.Series | None = None,
    risk_free_rate: float = RISK_FREE_RATE,
) -> dict[str, Any]:
    """
    计算全套绩效指标。

    Parameters
    ----------
    returns : Series
        每日收益率序列。
    turnover : Series, optional
        每日换手率序列。
    risk_free_rate : float
        年化无风险利率。

    Returns
    -------
    dict
        包含所有绩效指标的字典。
    """
    returns = returns.dropna()
    if len(returns) < 2:
        return {"error": "insufficient data"}

    n_days = len(returns)
    n_years = n_days / TRADING_DAYS_PER_YEAR

    total_return = (1 + returns).prod() - 1
    ann_return = (1 + total_return) ** (1 / n_years) - 1 if n_years > 0 else 0.0
    ann_vol = returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)

    daily_rf = (1 + risk_free_rate) ** (1 / TRADING_DAYS_PER_YEAR) - 1
    excess = returns - daily_rf
    sharpe = excess.mean() / excess.std() * np.sqrt(TRADING_DAYS_PER_YEAR) if excess.std() > 0 else 0.0

    dd = _calc_drawdown(returns)
    max_dd = dd["max_drawdown"]
    calmar = ann_return / abs(max_dd) if abs(max_dd) > 1e-10 else 0.0

    sortino = _calc_sortino(returns, risk_free_rate)

    win_rate, profit_loss_ratio = _calc_trade_stats(returns)

    result = {
        "total_return": round(total_return, 4),
        "annualized_return": round(ann_return, 4),
        "annualized_volatility": round(ann_vol, 4),
        "sharpe_ratio": round(sharpe, 4),
        "calmar_ratio": round(calmar, 4),
        "sortino_ratio": round(sortino, 4),
        "max_drawdown": round(max_dd, 4),
        "max_drawdown_duration_days": dd["max_dd_duration"],
        "win_rate": round(win_rate, 4),
        "profit_loss_ratio": round(profit_loss_ratio, 4),
        "n_trading_days": n_days,
        "n_years": round(n_years, 2),
    }

    if turnover is not None:
        turnover = turnover.dropna()
        ann_turnover = turnover.mean() * TRADING_DAYS_PER_YEAR
        result["annualized_turnover"] = round(ann_turnover, 4)
        total_cost = (turnover * 0.0015).sum()
        result["total_cost"] = round(total_cost, 4)

    return result


def _calc_drawdown(returns: pd.Series) -> dict:
    """计算回撤相关指标"""
    cum = (1 + returns).cumprod()
    running_max = cum.cummax()
    drawdown = cum / running_max - 1
    max_dd = drawdown.min()

    dd_end = drawdown.idxmin()
    dd_peak = cum[:dd_end].idxmax()
    dd_duration = len(cum[dd_peak:dd_end]) if dd_peak != dd_end else 0

    return {
        "max_drawdown": max_dd,
        "max_dd_peak": dd_peak,
        "max_dd_trough": dd_end,
        "max_dd_duration": dd_duration,
        "drawdown_series": drawdown,
    }


def _calc_sortino(returns: pd.Series, risk_free_rate: float) -> float:
    """计算 Sortino 比率 (只考虑下行波动)"""
    daily_rf = (1 + risk_free_rate) ** (1 / TRADING_DAYS_PER_YEAR) - 1
    excess = returns - daily_rf
    downside = excess[excess < 0]
    downside_std = downside.std()
    if downside_std == 0 or np.isnan(downside_std):
        return 0.0
    return excess.mean() / downside_std * np.sqrt(TRADING_DAYS_PER_YEAR)


def _calc_trade_stats(returns: pd.Series) -> tuple[float, float]:
    """
    计算胜率和盈亏比。

    基于日收益: 正收益天/总天数, 平均正收益/平均负收益。
    """
    positive = returns[returns > 0]
    negative = returns[returns < 0]

    win_rate = len(positive) / len(returns) if len(returns) > 0 else 0.0

    avg_win = positive.mean() if len(positive) > 0 else 0.0
    avg_loss = abs(negative.mean()) if len(negative) > 0 else 1e-10
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 1e-10 else 0.0

    return win_rate, profit_loss_ratio


def format_report(metrics: dict[str, Any]) -> str:
    """格式化绩效报告"""
    lines = [
        "=" * 55,
        "策略回测绩效报告",
        "=" * 55,
        f"  回测区间:       {metrics.get('n_years', 0):.1f} 年 ({metrics.get('n_trading_days', 0)} 交易日)",
        f"  总收益率:       {metrics.get('total_return', 0):.2%}",
        f"  年化收益率:     {metrics.get('annualized_return', 0):.2%}",
        f"  年化波动率:     {metrics.get('annualized_volatility', 0):.2%}",
        "-" * 55,
        f"  夏普比率:       {metrics.get('sharpe_ratio', 0):.3f}",
        f"  卡玛比率:       {metrics.get('calmar_ratio', 0):.3f}",
        f"  Sortino:        {metrics.get('sortino_ratio', 0):.3f}",
        "-" * 55,
        f"  最大回撤:       {metrics.get('max_drawdown', 0):.2%}",
        f"  最大回撤持续:   {metrics.get('max_drawdown_duration_days', 0)} 天",
        "-" * 55,
        f"  胜率 (日频):    {metrics.get('win_rate', 0):.2%}",
        f"  盈亏比:         {metrics.get('profit_loss_ratio', 0):.3f}",
    ]
    if "annualized_turnover" in metrics:
        lines.extend([
            "-" * 55,
            f"  年化换手率:     {metrics.get('annualized_turnover', 0):.1%}",
            f"  累计交易成本:   {metrics.get('total_cost', 0):.2%}",
        ])
    lines.append("=" * 55)
    return "\n".join(lines)
