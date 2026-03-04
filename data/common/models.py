"""
数据模型定义 + DataFrame Schema 校验。

dataclass 用于类型约束和文档化；
Schema 类用于运行时校验 DataFrame 列结构。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import pandas as pd


# ================================================================
# Dataclass 数据模型
# ================================================================

@dataclass(frozen=True)
class StockInfo:
    """股票基础信息"""
    ts_code: str
    name: str
    industry: str
    market: str
    list_date: date
    delist_date: Optional[date] = None
    is_st: bool = False
    is_delisted: bool = False


@dataclass(frozen=True)
class DailyBar:
    """单条日K线记录"""
    ts_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    pct_chg: float = 0.0
    turn: float = 0.0
    adj_factor: float = 1.0
    adj_close: float = 0.0
    is_suspended: bool = False
    is_limit_up: bool = False
    is_limit_down: bool = False


@dataclass(frozen=True)
class FinancialIndicator:
    """单条财务指标记录"""
    ts_code: str
    ann_date: date
    end_date: date
    roe: Optional[float] = None
    roa: Optional[float] = None
    gross_margin: Optional[float] = None
    net_profit_yoy: Optional[float] = None
    revenue_yoy: Optional[float] = None


@dataclass(frozen=True)
class Valuation:
    """单条估值记录"""
    ts_code: str
    trade_date: date
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    ps_ttm: Optional[float] = None
    total_mv: float = 0.0
    circ_mv: float = 0.0


@dataclass(frozen=True)
class TradeDate:
    """交易日信息"""
    exchange: str
    cal_date: date
    is_open: bool


@dataclass
class CleanReport:
    """清洗报告"""
    input_rows: int = 0
    output_rows: int = 0
    dropped_rows: int = 0
    filled_nulls: int = 0
    flagged_anomalies: int = 0
    details: dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


# ================================================================
# DataFrame Schema 校验
# ================================================================

class _BaseSchema:
    """Schema 基类，提供通用的校验方法"""

    REQUIRED_COLUMNS: dict[str, type] = {}
    OPTIONAL_COLUMNS: dict[str, type] = {}

    @classmethod
    def validate(cls, df: pd.DataFrame) -> list[str]:
        """验证 DataFrame 列结构，返回错误列表（空 = 通过）"""
        errors: list[str] = []
        for col in cls.REQUIRED_COLUMNS:
            if col not in df.columns:
                errors.append(f"缺少必要列: {col}")
        return errors

    @classmethod
    def all_columns(cls) -> list[str]:
        return list(cls.REQUIRED_COLUMNS) + list(cls.OPTIONAL_COLUMNS)


class DailyBarSchema(_BaseSchema):
    """日K线 DataFrame 列定义"""

    REQUIRED_COLUMNS = {
        "ts_code": str,
        "trade_date": str,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        "amount": float,
    }
    OPTIONAL_COLUMNS = {
        "pct_chg": float,
        "turn": float,
        "adj_factor": float,
        "adj_open": float,
        "adj_high": float,
        "adj_low": float,
        "adj_close": float,
        "is_suspended": bool,
        "is_limit_up": bool,
        "is_limit_down": bool,
        "is_one_word_limit": bool,
        "is_anomaly": bool,
        "suspension_days": int,
    }


class FinancialSchema(_BaseSchema):
    """财务指标 DataFrame 列定义"""

    REQUIRED_COLUMNS = {
        "ts_code": str,
        "ann_date": str,
        "end_date": str,
    }
    OPTIONAL_COLUMNS = {
        "roe": float,
        "roa": float,
        "gross_margin": float,
        "net_profit_yoy": float,
        "revenue_yoy": float,
        "is_anomaly": bool,
    }


class ValuationSchema(_BaseSchema):
    """估值 DataFrame 列定义"""

    REQUIRED_COLUMNS = {
        "ts_code": str,
        "trade_date": str,
        "pe_ttm": float,
        "pb": float,
    }
    OPTIONAL_COLUMNS = {
        "ps_ttm": float,
        "total_mv": float,
        "circ_mv": float,
    }


class StockListSchema(_BaseSchema):
    """股票列表 DataFrame 列定义"""

    REQUIRED_COLUMNS = {
        "ts_code": str,
        "name": str,
        "list_date": str,
    }
    OPTIONAL_COLUMNS = {
        "industry": str,
        "market": str,
        "delist_date": str,
        "is_st": bool,
        "is_delisted": bool,
    }
