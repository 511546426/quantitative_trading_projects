"""
Shared fixtures for all tests.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

# ─── Return series fixtures ───────────────────────────────────────────────


@pytest.fixture
def daily_returns_constant() -> pd.Series:
    """252 trading days of 0.1% daily return (no volatility)."""
    rng = pd.bdate_range("2024-01-01", periods=252, freq=pd.offsets.CustomBusinessDay(weekmask="1111100"))
    return pd.Series(0.001, index=rng)


@pytest.fixture
def daily_returns_zero() -> pd.Series:
    """252 trading days of 0% daily return."""
    rng = pd.bdate_range("2024-01-01", periods=252, freq=pd.offsets.CustomBusinessDay(weekmask="1111100"))
    return pd.Series(0.0, index=rng)


@pytest.fixture
def daily_returns_alternating() -> pd.Series:
    """Alternating +1% / -0.5% daily to create known win rate and profit/loss ratio."""
    rng = pd.bdate_range("2024-01-01", periods=100, freq=pd.offsets.CustomBusinessDay(weekmask="1111100"))
    vals = np.tile([0.01, -0.005], 50)
    return pd.Series(vals[:100], index=rng[:100])


@pytest.fixture
def daily_returns_with_drawdown() -> pd.Series:
    """Returns that create a known drawdown pattern: steady rise then sharp drop then recovery."""
    rng = pd.bdate_range("2024-01-01", periods=504, freq=pd.offsets.CustomBusinessDay(weekmask="1111100"))
    vals = np.zeros(504)
    vals[:252] = 0.002  # steady rise year 1
    vals[252:378] = -0.01  # 126-day drawdown
    vals[378:] = 0.003  # recovery
    return pd.Series(vals, index=rng[:504])


# ─── Price / signal data fixtures ─────────────────────────────────────────


@pytest.fixture
def stock_universe() -> list[str]:
    return ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"]


@pytest.fixture
def price_panel(stock_universe) -> pd.DataFrame:
    """
    100-day pivot price panel for 5 stocks.
    Prices follow a random walk with seed for reproducibility.
    """
    rng = pd.bdate_range("2024-01-01", periods=100, freq=pd.offsets.CustomBusinessDay(weekmask="1111100"))
    np.random.seed(42)
    data = {}
    for code in stock_universe:
        price = 100.0
        prices = [price]
        for _ in range(1, 100):
            price *= 1 + np.random.normal(0.0005, 0.02)
            prices.append(price)
        data[code] = prices
    return pd.DataFrame(data, index=rng)


@pytest.fixture
def signal_panel(stock_universe) -> pd.DataFrame:
    """
    Signal matrix with top 2 stocks having positive signals.
    """
    rng = pd.bdate_range("2024-01-01", periods=100, freq=pd.offsets.CustomBusinessDay(weekmask="1111100"))
    np.random.seed(123)
    data = {}
    for i, code in enumerate(stock_universe):
        # Make first 2 stocks have consistently positive signals
        if i < 2:
            data[code] = np.random.uniform(0.5, 1.0, 100)
        else:
            data[code] = np.random.uniform(-0.3, 0.3, 100)
    return pd.DataFrame(data, index=rng)


@pytest.fixture
def calendar_dates() -> list[date]:
    """A list of known trading dates for calendar tests."""
    return [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2024, 1, 9),
        date(2024, 1, 10),
        date(2024, 1, 11),
        date(2024, 1, 12),
        date(2024, 1, 15),
    ]
