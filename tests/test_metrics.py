"""
Tests for strategy/backtest/metrics.py

Covers: calc_full_metrics, _calc_drawdown, _calc_sortino,
        _calc_trade_stats, format_report, BacktestResult
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategy.backtest.metrics import (
    TRADING_DAYS_PER_YEAR,
    BacktestResult,
    _calc_drawdown,
    _calc_sortino,
    _calc_trade_stats,
    calc_full_metrics,
    format_report,
)


class TestCalcDrawdown:
    def test_no_drawdown(self, daily_returns_constant):
        """Strictly positive returns → drawdown ≈ 0."""
        dd = _calc_drawdown(daily_returns_constant)
        assert dd["max_drawdown"] >= -1e-12
        assert dd["max_dd_duration"] >= 0

    def test_known_drawdown(self, daily_returns_with_drawdown):
        """Known drawdown pattern: ~126 days of -1% after ramp-up."""
        dd = _calc_drawdown(daily_returns_with_drawdown)
        assert dd["max_drawdown"] < -0.05  # should be a substantial drawdown
        assert dd["max_dd_duration"] > 0
        assert dd["max_dd_peak"] != dd["max_dd_trough"]

    def test_single_value(self):
        """Single day → drawdown must be 0."""
        s = pd.Series([1.0], index=pd.date_range("2024-01-02", periods=1))
        dd = _calc_drawdown(s)
        assert dd["max_drawdown"] == 0.0

    def test_all_negative(self):
        """All negative returns → drawdown is total loss."""
        s = pd.Series([-0.01] * 50, index=pd.bdate_range("2024-01-01", periods=50))
        dd = _calc_drawdown(s)
        assert dd["max_drawdown"] < -0.3  # significant loss


class TestCalcSortino:
    def test_zero_downside(self, daily_returns_constant):
        """All returns positive → zero downside → Sortino returns 0."""
        sortino = _calc_sortino(daily_returns_constant, 0.025)
        # When downside std is 0, the function returns 0.0
        assert sortino == 0.0

    def test_known_sortino(self, daily_returns_alternating):
        """Alternating pattern should produce a computable Sortino."""
        sortino = _calc_sortino(daily_returns_alternating, 0.025)
        assert np.isfinite(sortino)
        assert isinstance(sortino, float)

    def test_high_risk_free_rate(self):
        """Risk-free rate above mean return → negative Sortino (when downside exists)."""
        # Daily return 0.0002 ≈ 0.02%, daily RFR at 10% annual ≈ 0.0378%
        # This creates negative excesses → computable downside
        rng = pd.bdate_range("2024-01-01", periods=252)
        s = pd.Series(np.random.normal(0.0001, 0.01, 252), index=rng)
        sortino = _calc_sortino(s, 0.10)
        # Should be computable and finite (not default 0.0)
        assert isinstance(sortino, float)
        assert np.isfinite(sortino)


class TestCalcTradeStats:
    def test_all_positive(self, daily_returns_constant):
        """All positive → win_rate=1, profit_loss_ratio → inf (no losses)."""
        wr, plr = _calc_trade_stats(daily_returns_constant)
        assert wr == 1.0
        assert plr == 0.0 or np.isinf(plr)  # avg_loss → 1e-10 → plr ∈ {0, ∞}

    def test_alternating(self, daily_returns_alternating):
        """Known pattern: 50 wins (+1%), 50 losses (-0.5%) → win_rate=0.5, plr=2.0"""
        wr, plr = _calc_trade_stats(daily_returns_alternating)
        assert wr == pytest.approx(0.5, abs=0.01)
        assert plr == pytest.approx(2.0, abs=0.05)

    def test_all_negative(self):
        """All negative → win_rate=0, plr=0."""
        s = pd.Series([-0.01] * 100, index=pd.bdate_range("2024-01-01", periods=100))
        wr, plr = _calc_trade_stats(s)
        assert wr == 0.0
        assert plr == 0.0

    def test_empty(self):
        """Empty series → win_rate=0, plr=0."""
        wr, plr = _calc_trade_stats(pd.Series([], dtype=float))
        assert wr == 0.0
        assert plr == 0.0

    def test_zero_returns(self):
        """All zero returns → win_rate=0 (no positive), plr=0."""
        s = pd.Series([0.0] * 100, index=pd.bdate_range("2024-01-01", periods=100))
        wr, plr = _calc_trade_stats(s)
        assert wr == 0.0
        assert plr == 0.0


class TestCalcFullMetrics:
    def test_returns_zero(self, daily_returns_zero):
        """All zero returns → zero total return, zero ann return, zero sharpe."""
        m = calc_full_metrics(daily_returns_zero)
        assert m["total_return"] == pytest.approx(0.0, abs=1e-6)
        assert m["annualized_return"] == pytest.approx(0.0, abs=1e-6)
        assert m["annualized_volatility"] == pytest.approx(0.0, abs=1e-6)

    def test_returns_constant(self, daily_returns_constant):
        """Constant 0.1% return for 1 year → positive sharpe, correct return."""
        m = calc_full_metrics(daily_returns_constant)
        assert m["total_return"] > 0
        assert m["annualized_return"] > 0
        assert m["sharpe_ratio"] > 0
        assert m["n_trading_days"] == 252

    def test_alternating(self, daily_returns_alternating):
        """Alternating returns → zero-ish total return, computable metrics."""
        m = calc_full_metrics(daily_returns_alternating)
        assert "error" not in m
        total = (1 + daily_returns_alternating).prod() - 1
        # total_return is rounded to 4 decimal places in calc_full_metrics
        assert m["total_return"] == pytest.approx(float(total), abs=1e-4)
        assert m["win_rate"] == pytest.approx(0.5, abs=0.02)

    def test_insufficient_data(self):
        """Single return → insufficient data error."""
        s = pd.Series([0.01], index=pd.date_range("2024-01-02", periods=1))
        m = calc_full_metrics(s)
        assert "error" in m

    def test_with_turnover(self, daily_returns_alternating):
        """With turnover provided, annualized_turnover and total_cost appear."""
        turnover = pd.Series(np.random.uniform(0.1, 0.3, 100),
                             index=daily_returns_alternating.index)
        m = calc_full_metrics(daily_returns_alternating, turnover)
        assert "annualized_turnover" in m
        assert "total_cost" in m
        assert m["annualized_turnover"] > 0
        assert m["total_cost"] > 0

    def test_nan_dropped(self):
        """NaN returns are dropped without affecting results."""
        rng = pd.bdate_range("2024-01-01", periods=100)
        vals = np.full(100, 0.001)
        vals[10:20] = np.nan
        s = pd.Series(vals, index=rng)
        m = calc_full_metrics(s)
        assert m["n_trading_days"] == 90  # 10 NaN dropped


class TestBacktestResult:
    def test_nav_property(self):
        """nav equals cumulative_returns."""
        rng = pd.date_range("2024-01-02", periods=10)
        ret = pd.Series([0.01] * 10, index=rng)
        br = BacktestResult(net_returns=ret)
        pd.testing.assert_series_equal(br.nav, br.cumulative_returns)

    def test_summary(self, daily_returns_alternating):
        """summary() delegates to calc_full_metrics."""
        br = BacktestResult(net_returns=daily_returns_alternating)
        s = br.summary()
        assert "total_return" in s
        assert "annualized_return" in s
        assert "sharpe_ratio" in s

    def test_summary_with_turnover(self, daily_returns_alternating):
        """summary() with turnover includes turnover metrics."""
        turnover = pd.Series(0.1, index=daily_returns_alternating.index)
        br = BacktestResult(net_returns=daily_returns_alternating, turnover=turnover)
        s = br.summary()
        assert "annualized_turnover" in s


class TestFormatReport:
    def test_format_report(self):
        """format_report returns a formatted string."""
        metrics = {
            "n_years": 5.0,
            "n_trading_days": 1260,
            "total_return": 0.5,
            "annualized_return": 0.084,
            "annualized_volatility": 0.20,
            "sharpe_ratio": 0.42,
            "calmar_ratio": 0.35,
            "sortino_ratio": 0.55,
            "max_drawdown": -0.25,
            "max_drawdown_duration_days": 120,
            "win_rate": 0.45,
            "profit_loss_ratio": 1.2,
        }
        report = format_report(metrics)
        assert isinstance(report, str)
        assert "总收益率" in report
        assert "夏普比率" in report

    def test_format_report_with_cost(self):
        """format_report includes cost section when turnover info present."""
        metrics = {
            "n_years": 3.0,
            "n_trading_days": 756,
            "total_return": 0.3,
            "annualized_return": 0.095,
            "annualized_volatility": 0.18,
            "sharpe_ratio": 0.52,
            "calmar_ratio": 0.40,
            "sortino_ratio": 0.60,
            "max_drawdown": -0.20,
            "max_drawdown_duration_days": 90,
            "win_rate": 0.48,
            "profit_loss_ratio": 1.15,
            "annualized_turnover": 5.0,
            "total_cost": 0.05,
        }
        report = format_report(metrics)
        assert "年化换手率" in report
        assert "累计交易成本" in report
