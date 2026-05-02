"""
Tests for data/common/calendar.py

Covers: TradingCalendar — construction, is_trade_date, get_trade_dates,
        latest_trade_date, next_trade_date, offset, count_trade_days
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from data.common.calendar import TradingCalendar


class TestTradingCalendar:
    def test_empty_calendar(self):
        """Empty calendar → all queries return None or empty."""
        cal = TradingCalendar()
        assert len(cal) == 0
        assert cal.min_date is None
        assert cal.max_date is None

    def test_from_dates(self, calendar_dates):
        """Construct from date list → dates are sorted."""
        cal = TradingCalendar(calendar_dates)
        assert len(cal) == 10
        assert cal.min_date == calendar_dates[0]
        assert cal.max_date == calendar_dates[-1]

    def test_is_trade_date(self, calendar_dates):
        """Known trade dates are recognized; non-trade dates rejected."""
        cal = TradingCalendar(calendar_dates)
        assert cal.is_trade_date(date(2024, 1, 2)) is True
        assert cal.is_trade_date(date(2024, 1, 3)) is True
        # Saturday
        assert cal.is_trade_date(date(2024, 1, 6)) is False
        # Sunday
        assert cal.is_trade_date(date(2024, 1, 7)) is False

    def test_is_trade_date_str(self, calendar_dates):
        """is_trade_date accepts YYYYMMDD string."""
        cal = TradingCalendar(calendar_dates)
        assert cal.is_trade_date("20240102") is True
        assert cal.is_trade_date("20240106") is False

    def test_get_trade_dates(self, calendar_dates):
        """get_trade_dates returns dates in [start, end] inclusive."""
        cal = TradingCalendar(calendar_dates)
        result = cal.get_trade_dates(date(2024, 1, 3), date(2024, 1, 10))
        expected = [
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
            date(2024, 1, 8),
            date(2024, 1, 9),
            date(2024, 1, 10),
        ]
        assert result == expected

    def test_get_trade_dates_out_of_range(self, calendar_dates):
        """No overlap → empty list."""
        cal = TradingCalendar(calendar_dates)
        result = cal.get_trade_dates(date(2025, 1, 1), date(2025, 12, 31))
        assert result == []

    def test_latest_trade_date(self, calendar_dates):
        """Latest trading date before/on a given date."""
        cal = TradingCalendar(calendar_dates)
        # On a trade date
        assert cal.latest_trade_date(date(2024, 1, 10)) == date(2024, 1, 10)
        # Weekend → closest prior trade date (Friday Jan 12)
        assert cal.latest_trade_date(date(2024, 1, 14)) == date(2024, 1, 12)

    def test_latest_trade_date_before_range(self, calendar_dates):
        """Date before earliest trade date → None."""
        cal = TradingCalendar(calendar_dates)
        assert cal.latest_trade_date(date(2023, 1, 1)) is None

    def test_next_trade_date(self, calendar_dates):
        """Next trading day after a given date."""
        cal = TradingCalendar(calendar_dates)
        assert cal.next_trade_date(date(2024, 1, 5)) == date(2024, 1, 8)
        assert cal.next_trade_date(date(2024, 1, 11)) == date(2024, 1, 12)

    def test_next_trade_date_after_range(self, calendar_dates):
        """Date after last trade date → None."""
        cal = TradingCalendar(calendar_dates)
        assert cal.next_trade_date(date(2025, 1, 1)) is None

    def test_offset_forward(self, calendar_dates):
        """offset(+n) moves forward n trading days."""
        cal = TradingCalendar(calendar_dates)
        assert cal.offset(date(2024, 1, 2), 2) == date(2024, 1, 4)
        assert cal.offset(date(2024, 1, 5), 1) == date(2024, 1, 8)

    def test_offset_backward(self, calendar_dates):
        """offset(-n) moves backward n trading days."""
        cal = TradingCalendar(calendar_dates)
        assert cal.offset(date(2024, 1, 10), -2) == date(2024, 1, 8)
        assert cal.offset(date(2024, 1, 8), -1) == date(2024, 1, 5)

    def test_offset_beyond_range(self, calendar_dates):
        """Offset beyond range → None."""
        cal = TradingCalendar(calendar_dates)
        assert cal.offset(date(2024, 1, 2), -1) is None
        assert cal.offset(date(2024, 1, 15), 1) is None

    def test_offset_none_for_non_trade_date(self, calendar_dates):
        """Offset from a non-trade date → nearest upcoming trade date."""
        cal = TradingCalendar(calendar_dates)
        # Jan 6 is Saturday; offset(0) should find nearest trade date
        result = cal.offset(date(2024, 1, 6), 0)
        assert result is not None

    def test_count_trade_days(self, calendar_dates):
        """Count days in range."""
        cal = TradingCalendar(calendar_dates)
        assert cal.count_trade_days(date(2024, 1, 1), date(2024, 1, 31)) == 10
        assert cal.count_trade_days(date(2024, 1, 1), date(2024, 1, 1)) == 0

    def test_from_dataframe(self):
        """from_dataframe parses the Tushare trade_cal format."""
        df = pd.DataFrame({
            "cal_date": ["20240101", "20240102", "20240103", "20240106"],
            "is_open": [0, 1, 1, 0],
            "exchange": ["SSE"] * 4,
        })
        cal = TradingCalendar.from_dataframe(df)
        assert len(cal) == 2
        assert cal.is_trade_date("20240102") is True
        assert cal.is_trade_date("20240101") is False

    def test_len(self, calendar_dates):
        """len(calendar) returns number of trade dates."""
        cal = TradingCalendar(calendar_dates)
        assert len(cal) == 10

    def test_ensure_date_str(self):
        """_ensure_date handles YYYYMMDD strings."""
        result = TradingCalendar._ensure_date("20240102")
        assert result == date(2024, 1, 2)

    def test_ensure_date_str_with_dashes(self):
        """_ensure_date handles YYYY-MM-DD strings."""
        result = TradingCalendar._ensure_date("2024-01-02")
        assert result == date(2024, 1, 2)

    def test_ensure_date_none(self):
        """_ensure_date(None) → today."""
        result = TradingCalendar._ensure_date(None)
        assert result == date.today()
