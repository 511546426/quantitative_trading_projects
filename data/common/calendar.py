"""
A 股交易日历工具。

初始化时从数据库/API 加载交易日历，缓存到内存。
提供常用的交易日判断、偏移、范围查询方法。
"""
from __future__ import annotations

import bisect
import logging
from datetime import date, datetime, timedelta
from typing import Sequence

import pandas as pd

logger = logging.getLogger(__name__)


class TradingCalendar:
    """
    A 股交易日历。

    Parameters
    ----------
    trade_dates : list[date]
        有序的交易日列表。可从数据库或 Tushare 加载后传入。
    """

    def __init__(self, trade_dates: Sequence[date] | None = None):
        self._dates: list[date] = sorted(trade_dates) if trade_dates else []
        self._date_set: set[date] = set(self._dates)

    # ---- 加载 --------------------------------------------------------

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> TradingCalendar:
        """
        从 DataFrame 构造（列: cal_date, is_open）。
        通常来自 Tushare 的 trade_cal 接口。
        """
        open_df = df[df["is_open"] == 1].copy()
        dates = pd.to_datetime(open_df["cal_date"]).dt.date.tolist()
        return cls(sorted(dates))

    @classmethod
    def from_csv(cls, path: str) -> TradingCalendar:
        df = pd.read_csv(path)
        return cls.from_dataframe(df)

    # ---- 判断 --------------------------------------------------------

    def is_trade_date(self, d: date | str) -> bool:
        d = self._ensure_date(d)
        return d in self._date_set

    # ---- 查询 --------------------------------------------------------

    def get_trade_dates(
        self, start: date | str, end: date | str
    ) -> list[date]:
        """返回 [start, end] 闭区间内的所有交易日"""
        s = self._ensure_date(start)
        e = self._ensure_date(end)
        lo = bisect.bisect_left(self._dates, s)
        hi = bisect.bisect_right(self._dates, e)
        return self._dates[lo:hi]

    def latest_trade_date(self, before: date | str | None = None) -> date | None:
        """返回 before（含）之前的最近交易日。默认 before=今天"""
        d = self._ensure_date(before) if before else date.today()
        idx = bisect.bisect_right(self._dates, d) - 1
        return self._dates[idx] if idx >= 0 else None

    def next_trade_date(self, after: date | str) -> date | None:
        """返回 after 之后的第一个交易日（不含 after 本身）"""
        d = self._ensure_date(after)
        idx = bisect.bisect_right(self._dates, d)
        return self._dates[idx] if idx < len(self._dates) else None

    def offset(self, d: date | str, n: int) -> date | None:
        """
        向前（n>0）或向后（n<0）偏移 n 个交易日。

        Returns None 如果超出日历范围。
        """
        d = self._ensure_date(d)
        if d not in self._date_set:
            idx = bisect.bisect_left(self._dates, d)
        else:
            idx = bisect.bisect_left(self._dates, d)
        target = idx + n
        if 0 <= target < len(self._dates):
            return self._dates[target]
        return None

    def count_trade_days(self, start: date | str, end: date | str) -> int:
        """统计 [start, end] 之间的交易日数（含两端）"""
        return len(self.get_trade_dates(start, end))

    @property
    def min_date(self) -> date | None:
        return self._dates[0] if self._dates else None

    @property
    def max_date(self) -> date | None:
        return self._dates[-1] if self._dates else None

    def __len__(self) -> int:
        return len(self._dates)

    # ---- 内部 --------------------------------------------------------

    @staticmethod
    def _ensure_date(d: date | str | None) -> date:
        if d is None:
            return date.today()
        if isinstance(d, str):
            d = d.replace("-", "")
            return datetime.strptime(d, "%Y%m%d").date()
        return d
