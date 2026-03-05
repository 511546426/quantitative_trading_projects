"""
向量化回测引擎。

所有计算基于矩阵运算，速度比事件驱动快 100~1000 倍。
适合: 参数扫描、因子研究、策略快速验证。
代价: 无法精确模拟订单撮合，成本模型较粗糙。

核心思路:
    signals → weights → daily_returns → costs → net_returns
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

import numpy as np
import pandas as pd

from strategy.backtest.metrics import BacktestResult, TRADING_DAYS_PER_YEAR

logger = logging.getLogger(__name__)


class WeightScheme(Enum):
    """持仓权重分配方案"""
    EQUAL = "equal"         # 等权
    SIGNAL = "signal"       # 按信号强度加权
    RANK = "rank"           # 按截面排名加权
    RISK_PARITY = "risk_parity"  # 风险平价


@dataclass
class CostModel:
    """
    A 股交易成本模型。

    commission:  佣金 (双边, 默认万2.5)
    stamp_duty:  印花税 (仅卖出, 千分之一)
    slippage:    滑点 (默认万5)
    """
    commission_bps: float = 2.5
    stamp_duty_bps: float = 10.0
    slippage_bps: float = 5.0

    @property
    def buy_cost(self) -> float:
        """单边买入成本"""
        return (self.commission_bps + self.slippage_bps) / 10000

    @property
    def sell_cost(self) -> float:
        """单边卖出成本"""
        return (self.commission_bps + self.stamp_duty_bps + self.slippage_bps) / 10000

    @property
    def round_trip_bps(self) -> float:
        """双边总成本 (bps)"""
        return self.commission_bps * 2 + self.stamp_duty_bps + self.slippage_bps * 2


class VectorizedBacktester:
    """
    向量化回测引擎。

    Parameters
    ----------
    cost_model : CostModel
        交易成本模型。
    weight_scheme : WeightScheme
        权重分配方案。
    max_stocks : int
        最大持仓只数。
    rebalance_freq : int
        调仓频率 (交易日), 如 5=周频, 21=月频。
    """

    def __init__(
        self,
        cost_model: CostModel | None = None,
        weight_scheme: WeightScheme = WeightScheme.EQUAL,
        max_stocks: int = 20,
        rebalance_freq: int = 5,
    ):
        self.cost_model = cost_model or CostModel()
        self.weight_scheme = weight_scheme
        self.max_stocks = max_stocks
        self.rebalance_freq = rebalance_freq

    def run(
        self,
        signals: pd.DataFrame,
        prices: pd.DataFrame,
        benchmark: pd.Series | None = None,
    ) -> BacktestResult:
        """
        运行向量化回测。

        Parameters
        ----------
        signals : DataFrame
            pivot 格式信号矩阵, index=trade_date, columns=ts_code。
            正值=做多信号，值越大信号越强。NaN/0=不持仓。
        prices : DataFrame
            pivot 格式价格 (复权收盘价), 同 signals 维度。
        benchmark : Series, optional
            基准指数收益率，用于计算超额收益。

        Returns
        -------
        BacktestResult
        """
        signals, prices = self._align(signals, prices)
        daily_returns = prices.pct_change(fill_method=None)

        weights = self._signals_to_weights(signals)
        weights = self._apply_rebalance(weights)

        portfolio_return = (weights.shift(1) * daily_returns).sum(axis=1)

        turnover = weights.diff().abs().sum(axis=1)
        buy_turnover = weights.diff().clip(lower=0).sum(axis=1)
        sell_turnover = (-weights.diff().clip(upper=0)).sum(axis=1)
        costs = (
            buy_turnover * self.cost_model.buy_cost
            + sell_turnover * self.cost_model.sell_cost
        )

        net_return = portfolio_return - costs

        metadata = {
            "weight_scheme": self.weight_scheme.value,
            "max_stocks": self.max_stocks,
            "rebalance_freq": self.rebalance_freq,
            "cost_model": {
                "commission_bps": self.cost_model.commission_bps,
                "stamp_duty_bps": self.cost_model.stamp_duty_bps,
                "slippage_bps": self.cost_model.slippage_bps,
            },
        }

        if benchmark is not None:
            benchmark = benchmark.reindex(net_return.index).fillna(0)
            metadata["benchmark_total_return"] = float((1 + benchmark).prod() - 1)
            excess = net_return - benchmark
            metadata["excess_annualized_return"] = float(
                (1 + excess).prod() ** (TRADING_DAYS_PER_YEAR / len(excess)) - 1
            )

        result = BacktestResult(
            net_returns=net_return,
            gross_returns=portfolio_return,
            turnover=turnover,
            positions=weights,
            metadata=metadata,
        )

        logger.info(
            "向量化回测完成: %d 交易日, 年化换手 %.1f%%",
            len(net_return),
            turnover.mean() * TRADING_DAYS_PER_YEAR * 100,
        )
        return result

    def _align(
        self, signals: pd.DataFrame, prices: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """对齐 signals 和 prices 的索引"""
        common_dates = signals.index.intersection(prices.index)
        common_codes = signals.columns.intersection(prices.columns)
        return (
            signals.loc[common_dates, common_codes],
            prices.loc[common_dates, common_codes],
        )

    def _signals_to_weights(self, signals: pd.DataFrame) -> pd.DataFrame:
        """将信号矩阵转换为持仓权重矩阵"""
        if self.weight_scheme == WeightScheme.EQUAL:
            return self._equal_weight(signals)
        elif self.weight_scheme == WeightScheme.SIGNAL:
            return self._signal_weight(signals)
        elif self.weight_scheme == WeightScheme.RANK:
            return self._rank_weight(signals)
        else:
            return self._equal_weight(signals)

    def _equal_weight(self, signals: pd.DataFrame) -> pd.DataFrame:
        """等权分配: 选 top N 信号最强的股票"""
        weights = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
        for dt in signals.index:
            row = signals.loc[dt].dropna()
            row = row[row > 0].nlargest(self.max_stocks)
            if len(row) > 0:
                weights.loc[dt, row.index] = 1.0 / len(row)
        return weights

    def _signal_weight(self, signals: pd.DataFrame) -> pd.DataFrame:
        """信号强度加权: 按信号大小分配权重"""
        weights = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
        for dt in signals.index:
            row = signals.loc[dt].dropna()
            row = row[row > 0].nlargest(self.max_stocks)
            if len(row) > 0:
                total = row.sum()
                if total > 0:
                    weights.loc[dt, row.index] = row / total
        return weights

    def _rank_weight(self, signals: pd.DataFrame) -> pd.DataFrame:
        """排名加权: 按截面排名分配权重"""
        weights = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
        for dt in signals.index:
            row = signals.loc[dt].dropna()
            row = row[row > 0].nlargest(self.max_stocks)
            if len(row) > 0:
                ranks = row.rank(ascending=True)
                weights.loc[dt, row.index] = ranks / ranks.sum()
        return weights

    def _apply_rebalance(self, weights: pd.DataFrame) -> pd.DataFrame:
        """
        按调仓频率过滤权重变化。

        非调仓日沿用上一期权重，减少不必要的换手。
        """
        if self.rebalance_freq <= 1:
            return weights

        rebalanced = weights.copy()
        last_rebalance = None

        for i, dt in enumerate(weights.index):
            if i % self.rebalance_freq == 0:
                last_rebalance = dt
            elif last_rebalance is not None:
                rebalanced.loc[dt] = rebalanced.loc[last_rebalance]

        return rebalanced
