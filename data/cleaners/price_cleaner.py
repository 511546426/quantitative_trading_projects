"""
行情数据清洗器。

清洗流程:
    1. 基础验证（列、类型、去重）
    2. 复权处理（后复权价格计算）
    3. 停牌标记（volume==0）
    4. 涨跌停标记（区分主板/创业板/科创板/ST）
    5. 价格异常修正
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from data.cleaners.base import BaseCleaner
from data.common.models import CleanReport, DailyBarSchema

logger = logging.getLogger(__name__)

LIMIT_PCT_MAP = {
    "主板": 0.10,
    "创业板": 0.20,
    "科创板": 0.20,
    "北交所": 0.30,
    "ST": 0.05,
}


class PriceCleaner(BaseCleaner):
    """
    日K线数据清洗器。

    Parameters
    ----------
    stock_info : DataFrame, optional
        股票基础信息（含 market, is_st 字段），用于涨跌停判定。
        如果不提供，涨跌停标记将使用默认 10% 阈值。
    """

    def __init__(self, stock_info: pd.DataFrame | None = None):
        self._stock_info = stock_info

    def validate(self, raw_df: pd.DataFrame) -> list[str]:
        return DailyBarSchema.validate(raw_df)

    def clean(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, CleanReport]:
        errors = self.validate(raw_df)
        if errors:
            logger.warning("价格数据校验警告: %s", errors)

        report = CleanReport(input_rows=len(raw_df))
        df = raw_df.copy()

        # [1] 基础清理
        df = self._basic_clean(df, report)

        # [2] 复权处理
        df = self._apply_adj_factor(df, report)

        # [3] 停牌标记
        df = self._mark_suspension(df, report)

        # [4] 涨跌停标记
        df = self._mark_limit(df, report)

        # [5] 价格异常标记
        df = self._mark_anomaly(df, report)

        report.output_rows = len(df)
        report.dropped_rows = report.input_rows - report.output_rows
        return df, report

    # ================================================================
    # 清洗步骤
    # ================================================================

    def _basic_clean(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        """去重 + 类型转换"""
        before = len(df)
        df = df.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
        dup_count = before - len(df)
        if dup_count:
            report.details["duplicates_removed"] = dup_count

        numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        null_count = df[["open", "high", "low", "close"]].isna().sum().sum()
        if null_count:
            report.details["ohlc_nulls"] = int(null_count)

        return df

    def _apply_adj_factor(
        self, df: pd.DataFrame, report: CleanReport
    ) -> pd.DataFrame:
        """计算后复权价格"""
        if "adj_factor" not in df.columns:
            df["adj_factor"] = 1.0
            df["adj_open"] = df["open"]
            df["adj_high"] = df["high"]
            df["adj_low"] = df["low"]
            df["adj_close"] = df["close"]
            return df

        df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce").fillna(1.0)
        df["adj_open"] = df["open"] * df["adj_factor"]
        df["adj_high"] = df["high"] * df["adj_factor"]
        df["adj_low"] = df["low"] * df["adj_factor"]
        df["adj_close"] = df["close"] * df["adj_factor"]

        report.details["adj_factor_applied"] = True
        return df

    def _mark_suspension(
        self, df: pd.DataFrame, report: CleanReport
    ) -> pd.DataFrame:
        """标记停牌（volume == 0）"""
        df["is_suspended"] = df["volume"].fillna(0) == 0

        suspended_count = df["is_suspended"].sum()
        report.details["suspended_count"] = int(suspended_count)

        df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
        df["suspension_days"] = 0

        if suspended_count > 0:
            for code, grp in df.groupby("ts_code"):
                mask = grp["is_suspended"]
                days = mask.astype(int)
                cum = days.groupby((~mask).cumsum()).cumsum()
                df.loc[grp.index, "suspension_days"] = cum.values

        return df

    def _mark_limit(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        """标记涨跌停"""
        df["is_limit_up"] = False
        df["is_limit_down"] = False
        df["is_one_word_limit"] = False

        if "pct_chg" not in df.columns:
            return df

        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0)

        limit_pct = self._get_limit_pct_series(df)

        threshold = limit_pct * 100 - 0.5

        df["is_limit_up"] = (
            (df["pct_chg"] >= threshold)
            & (~df["is_suspended"])
        )
        df["is_limit_down"] = (
            (df["pct_chg"] <= -threshold)
            & (~df["is_suspended"])
        )

        df["is_one_word_limit"] = (
            (df["is_limit_up"] | df["is_limit_down"])
            & (df["open"] == df["close"])
            & (df["high"] == df["low"])
        )

        report.details["limit_up_count"] = int(df["is_limit_up"].sum())
        report.details["limit_down_count"] = int(df["is_limit_down"].sum())
        report.details["one_word_limit_count"] = int(df["is_one_word_limit"].sum())

        return df

    def _mark_anomaly(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        """标记价格异常"""
        df["is_anomaly"] = False

        mask_ohlc = (
            (df["low"] > df[["open", "close"]].min(axis=1) + 0.01)
            | (df["high"] < df[["open", "close"]].max(axis=1) - 0.01)
        ) & (~df["is_suspended"])
        df.loc[mask_ohlc, "is_anomaly"] = True

        mask_neg = (
            (df["open"] <= 0) | (df["high"] <= 0) |
            (df["low"] <= 0) | (df["close"] <= 0)
        ) & (~df["is_suspended"])
        df.loc[mask_neg, "is_anomaly"] = True

        if "pct_chg" in df.columns:
            mask_extreme = (
                (df["pct_chg"].abs() > 22)
                & (~df["is_limit_up"])
                & (~df["is_limit_down"])
                & (~df["is_suspended"])
            )
            df.loc[mask_extreme, "is_anomaly"] = True

        anomaly_count = df["is_anomaly"].sum()
        report.flagged_anomalies = int(anomaly_count)
        report.details["anomaly_count"] = int(anomaly_count)

        return df

    # ================================================================
    # 辅助
    # ================================================================

    def _get_limit_pct_series(self, df: pd.DataFrame) -> pd.Series:
        """根据板块和 ST 状态返回每只股票的涨跌停比例"""
        default_pct = 0.10

        if self._stock_info is None or self._stock_info.empty:
            return pd.Series(default_pct, index=df.index)

        info = self._stock_info[["ts_code", "market", "is_st"]].drop_duplicates(
            subset=["ts_code"]
        )
        merged = df[["ts_code"]].merge(info, on="ts_code", how="left")

        def _row_pct(row):
            if row.get("is_st"):
                return LIMIT_PCT_MAP["ST"]
            market = row.get("market", "主板")
            return LIMIT_PCT_MAP.get(market, default_pct)

        return merged.apply(_row_pct, axis=1)
