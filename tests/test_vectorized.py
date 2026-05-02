"""
Tests for strategy/backtest/vectorized.py

Covers: CostModel, VectorizedBacktester, WeightScheme, _align,
        _signals_to_weights, _apply_rebalance
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategy.backtest.vectorized import (
    CostModel,
    VectorizedBacktester,
    WeightScheme,
)


class TestCostModel:
    def test_buy_cost(self):
        """buy_cost = (commission + slippage) / 10000"""
        cm = CostModel(commission_bps=2.5, stamp_duty_bps=10.0, slippage_bps=5.0)
        expected = (2.5 + 5.0) / 10000
        assert cm.buy_cost == pytest.approx(expected)

    def test_sell_cost(self):
        """sell_cost = (commission + stamp_duty + slippage) / 10000"""
        cm = CostModel(commission_bps=2.5, stamp_duty_bps=10.0, slippage_bps=5.0)
        expected = (2.5 + 10.0 + 5.0) / 10000
        assert cm.sell_cost == pytest.approx(expected)

    def test_round_trip_bps(self):
        """round_trip_bps = 2×commission + stamp_duty + 2×slippage"""
        cm = CostModel(commission_bps=2.5, stamp_duty_bps=10.0, slippage_bps=5.0)
        expected = 2.5 * 2 + 10.0 + 5.0 * 2
        assert cm.round_trip_bps == expected

    def test_default_values(self):
        """Default cost model matches A-share typical values."""
        cm = CostModel()
        assert cm.commission_bps == 2.5
        assert cm.stamp_duty_bps == 10.0
        assert cm.slippage_bps == 5.0


class TestVectorizedBacktester:
    def test_align_common_dates_and_codes(self, price_panel, signal_panel):
        """_align filters to common dates and columns."""
        bt = VectorizedBacktester()
        sig, pri = bt._align(signal_panel, price_panel)
        assert sig.index.equals(pri.index)
        assert list(sig.columns) == list(pri.columns)
        assert len(sig.columns) <= len(signal_panel.columns)

    def test_align_mismatched_columns(self, price_panel, stock_universe):
        """_align drops codes present in one but not the other."""
        signals = signal_panel = pd.DataFrame(
            {stock_universe[0]: np.random.uniform(-1, 1, 100),
             stock_universe[1]: np.random.uniform(-1, 1, 100)},
            index=price_panel.index,
        )
        bt = VectorizedBacktester()
        sig, pri = bt._align(signals, price_panel)
        assert len(sig.columns) == 2  # only 2 common

    def test_equal_weight_allocation(self, signal_panel, price_panel, stock_universe):
        """EQUAL weight: top 2 stocks get 0.5 each on rebalance days."""
        bt = VectorizedBacktester(
            max_stocks=2, weight_scheme=WeightScheme.EQUAL, rebalance_freq=20
        )
        signals, prices = bt._align(signal_panel, price_panel)
        weights = bt._signals_to_weights(signals)
        # On first day, check weights sum to 1.0
        first_day = weights.iloc[0]
        assert first_day.sum() == pytest.approx(1.0, abs=1e-10)
        assert (first_day >= 0).all()  # no shorting
        n_positions = (first_day > 0).sum()
        assert n_positions <= 2

    def test_signal_weight_allocation(self, signal_panel, price_panel):
        """SIGNAL weight: weights proportional to signal strength."""
        bt = VectorizedBacktester(
            max_stocks=3, weight_scheme=WeightScheme.SIGNAL, rebalance_freq=20
        )
        signals, prices = bt._align(signal_panel, price_panel)
        weights = bt._signals_to_weights(signals)
        first_day = weights.iloc[0]
        assert first_day.sum() == pytest.approx(1.0, abs=1e-10)

    def test_rank_weight_allocation(self, signal_panel, price_panel):
        """RANK weight: linear rank-based weights."""
        bt = VectorizedBacktester(
            max_stocks=3, weight_scheme=WeightScheme.RANK, rebalance_freq=20
        )
        signals, prices = bt._align(signal_panel, price_panel)
        weights = bt._signals_to_weights(signals)
        first_day = weights.iloc[0]
        assert first_day.sum() == pytest.approx(1.0, abs=1e-10)

    def test_apply_rebalance_monthly(self, price_panel):
        """With rebalance_freq=20, weights stay constant between rebalance dates."""
        bt = VectorizedBacktester(rebalance_freq=20)
        # Create simple weights that change every day
        n = len(price_panel)
        weights = pd.DataFrame(
            np.random.rand(n, 3),
            index=price_panel.index,
            columns=list("ABC"),
        )
        weights = weights.div(weights.sum(axis=1), axis=0)  # normalize
        rebalanced = bt._apply_rebalance(weights)
        # Days 1-19 should equal day 0, day 20 should equal day 20, etc.
        assert rebalanced.iloc[1].equals(rebalanced.iloc[0])
        assert rebalanced.iloc[19].equals(rebalanced.iloc[0])
        if n > 20:
            assert not rebalanced.iloc[20].equals(rebalanced.iloc[0])

    def test_apply_rebalance_daily(self, price_panel):
        """rebalance_freq=1 → no change to weights."""
        bt = VectorizedBacktester(rebalance_freq=1)
        n = len(price_panel)
        weights = pd.DataFrame(
            np.ones((n, 3)) / 3, index=price_panel.index, columns=list("ABC")
        )
        rebalanced = bt._apply_rebalance(weights)
        pd.testing.assert_frame_equal(rebalanced, weights)

    def test_run_returns_backtest_result(self, signal_panel, price_panel):
        """run() returns a BacktestResult with valid metrics."""
        bt = VectorizedBacktester(
            max_stocks=3, weight_scheme=WeightScheme.EQUAL, rebalance_freq=20
        )
        result = bt.run(signal_panel, price_panel)
        assert result is not None
        assert len(result.net_returns) > 0
        assert result.net_returns.index.equals(
            signal_panel.index.intersection(price_panel.index)
        )

    def test_run_with_benchmark(self, signal_panel, price_panel):
        """run() with benchmark includes excess return metadata."""
        bt = VectorizedBacktester(
            max_stocks=3, weight_scheme=WeightScheme.EQUAL, rebalance_freq=20
        )
        benchmark = pd.Series(0.0005, index=price_panel.index)
        result = bt.run(signal_panel, price_panel, benchmark=benchmark)
        assert result.metadata is not None
        assert "excess_annualized_return" in result.metadata

    def test_turnover_non_negative(self, signal_panel, price_panel):
        """Turnover values are non-negative."""
        bt = VectorizedBacktester(
            max_stocks=3, weight_scheme=WeightScheme.EQUAL, rebalance_freq=20
        )
        result = bt.run(signal_panel, price_panel)
        assert (result.turnover.dropna() >= -1e-12).all()

    def test_cost_reduces_return(self, signal_panel, price_panel):
        """Net returns <= gross returns (costs are non-negative)."""
        bt = VectorizedBacktester(
            max_stocks=3, weight_scheme=WeightScheme.EQUAL, rebalance_freq=20
        )
        result = bt.run(signal_panel, price_panel)
        assert (result.net_returns <= result.gross_returns + 1e-12).all()

    def test_zero_signals(self, price_panel):
        """Zero signals → no positions → zero returns."""
        zero_sig = pd.DataFrame(0.0, index=price_panel.index, columns=price_panel.columns)
        bt = VectorizedBacktester()
        result = bt.run(zero_sig, price_panel)
        assert (result.net_returns.abs() < 1e-12).all()

    def test_negative_signals_excluded(self, price_panel, stock_universe):
        """Negative signals are filtered out by _equal_weight (row > 0 filter)."""
        sig = pd.DataFrame(-1.0, index=price_panel.index[:10], columns=stock_universe)
        bt = VectorizedBacktester(weight_scheme=WeightScheme.EQUAL, max_stocks=2)
        weights = bt._signals_to_weights(sig)
        assert (weights.iloc[0] == 0).all()

    def test_configurable_cost(self, signal_panel, price_panel):
        """Custom cost model is used in return calculation."""
        cm = CostModel(commission_bps=50.0, stamp_duty_bps=10.0, slippage_bps=50.0)
        bt_high = VectorizedBacktester(cost_model=cm, rebalance_freq=10)
        bt_low = VectorizedBacktester(rebalance_freq=10)
        result_high = bt_high.run(signal_panel, price_panel)
        result_low = bt_low.run(signal_panel, price_panel)
        # Higher costs → lower net returns
        cum_high = (1 + result_high.net_returns).cumprod().iloc[-1]
        cum_low = (1 + result_low.net_returns).cumprod().iloc[-1]
        assert cum_high <= cum_low + 1e-10
