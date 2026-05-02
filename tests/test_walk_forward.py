"""
Tests for strategy/analysis/walk_forward.py

Covers: walk_forward_validate, WalkForwardReport
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategy.analysis.walk_forward import (
    WalkForwardReport,
    WalkForwardWindow,
    walk_forward_validate,
)


def _dummy_strategy_fn(
    top_n: int = 20,
    inertia: float = 0.2,
    date_start: str = "",
    date_end: str = "",
    **kwargs,
) -> dict:
    """Dummy strategy that returns metrics based on parameters."""
    _ = kwargs  # absorb extra kwargs
    if date_start == date_end:
        return {
            "error": "insufficient data",
            "annualized_return": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
        }
    years = int(date_end[:4]) - int(date_start[:4])
    # Simulate: larger top_n + moderate inertia = better performance
    base_return = 0.08 + (30 - top_n) * 0.002 + abs(inertia - 0.25) * 0.05
    base_sharpe = 0.5 + (30 - top_n) * 0.01 + abs(inertia - 0.25) * 0.1
    return {
        "annualized_return": base_return * (years / 5),
        "sharpe_ratio": base_sharpe * (years / 5),
        "max_drawdown": -0.15 - abs(inertia - 0.25) * 0.1,
    }


class TestWalkForwardReport:
    def test_empty_report(self):
        """Empty report returns zeros."""
        r = WalkForwardReport()
        assert r.avg_oos_sharpe == 0.0
        assert r.avg_oos_annual_return == 0.0
        assert r.sharpe_std == 0.0

    def test_single_window(self):
        """Single window report reflects that window's metrics."""
        w = WalkForwardWindow(
            train_start="2010", train_end="2014",
            test_start="2015", test_end="2015",
            best_params={"top_n": 30},
            in_sample_metrics={},
            out_of_sample_metrics={},
            oos_annualized_return=0.12,
            oos_max_drawdown=-0.15,
            oos_sharpe=0.8,
        )
        r = WalkForwardReport(windows=[w])
        assert r.avg_oos_sharpe == pytest.approx(0.8)
        assert r.avg_oos_annual_return == pytest.approx(0.12)

    def test_multi_window(self):
        """Multiple windows produce averaged metrics."""
        windows = [
            WalkForwardWindow(
                train_start=f"20{i}0", train_end=f"20{i}4",
                test_start=f"20{i}5", test_end=f"20{i}5",
                best_params={}, in_sample_metrics={},
                out_of_sample_metrics={},
                oos_annualized_return=0.10 * (i + 1),
                oos_max_drawdown=-0.10,
                oos_sharpe=0.5 + 0.1 * i,
            )
            for i in range(3)
        ]
        r = WalkForwardReport(windows=windows)
        assert r.avg_oos_sharpe == pytest.approx(0.6, abs=0.01)
        assert r.avg_oos_annual_return == pytest.approx(0.20, abs=0.01)
        assert r.sharpe_std > 0

    def test_summary(self):
        """summary() returns formatted string."""
        r = WalkForwardReport()
        s = r.summary()
        assert isinstance(s, str)
        assert "Walk-Forward" in s


class TestWalkForwardValidate:
    def test_requires_all_dates(self):
        """Missing all_dates raises ValueError."""
        with pytest.raises(ValueError, match="all_dates"):
            walk_forward_validate(
                strategy_fn=_dummy_strategy_fn,
                param_grid={"top_n": [20, 30]},
                train_years=2,
                test_years=1,
            )

    def test_insufficient_data(self):
        """Too few years raises ValueError."""
        dates = pd.date_range("2024-01-01", periods=252, freq="B")
        with pytest.raises(ValueError, match="不足"):
            walk_forward_validate(
                strategy_fn=_dummy_strategy_fn,
                param_grid={"top_n": [20]},
                train_years=5,
                test_years=1,
                all_dates=dates,
            )

    def test_empty_param_grid(self):
        """Empty param grid raises ValueError."""
        dates = pd.date_range("2020-01-01", periods=252 * 10, freq="B")
        with pytest.raises(ValueError, match="param_grid 为空"):
            walk_forward_validate(
                strategy_fn=_dummy_strategy_fn,
                param_grid={},
                train_years=3,
                test_years=1,
                all_dates=dates,
            )

    def test_basic_validation(self):
        """Basic 2-window validation returns expected structure."""
        dates = pd.date_range("2015-01-01", periods=252 * 10, freq="B")
        report = walk_forward_validate(
            strategy_fn=_dummy_strategy_fn,
            param_grid={"top_n": [20, 30], "inertia": [0.2, 0.3]},
            train_years=4,
            test_years=1,
            all_dates=dates,
            logger_prefix="TEST",
        )
        assert len(report.windows) > 0
        assert len(report.oos_sharpes) == len(report.windows)
        assert report.avg_oos_sharpe != 0.0

    def test_params_are_reported(self):
        """Best params are recorded for each window."""
        dates = pd.date_range("2015-01-01", periods=252 * 8, freq="B")
        report = walk_forward_validate(
            strategy_fn=_dummy_strategy_fn,
            param_grid={"top_n": [20, 30]},
            train_years=4,
            test_years=1,
            all_dates=dates,
        )
        for w in report.windows:
            assert "top_n" in w.best_params
            assert w.best_params["top_n"] in (20, 30)

    def test_lower_is_better(self):
        """When higher_is_better=False, lower score is selected."""
        def _fn_lower_better(threshold: float = 0.5, **kwargs):
            _ = kwargs
            return {
                "annualized_return": 0.1 - threshold * 0.05,
                "sharpe_ratio": 0.5 - threshold * 0.2,
                "max_drawdown": -0.1 - threshold * 0.2,
                "cost_ratio": threshold * 0.1,  # lower is better
            }

        dates = pd.date_range("2015-01-01", periods=252 * 8, freq="B")
        report = walk_forward_validate(
            strategy_fn=_fn_lower_better,
            param_grid={"threshold": [0.3, 0.7]},
            train_years=4,
            test_years=1,
            all_dates=dates,
            scoring_metric="cost_ratio",  # lower cost is better
            higher_is_better=False,
        )
        for w in report.windows:
            # lower cost (threshold=0.3 → 0.03) is better than 0.07
            assert w.best_params["threshold"] == 0.3
