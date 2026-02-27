"""
基本面数据清洗器。

核心功能:
    - Point-in-Time (PIT) 对齐（用 ann_date 而非 end_date）
    - 去重（同一报告期保留最新公告）
    - TTM 计算（滚动12个月）
    - 异常值标记
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from data.cleaners.base import BaseCleaner
from data.common.models import CleanReport, FinancialSchema

logger = logging.getLogger(__name__)

ROE_BOUNDS = (-100.0, 200.0)
ROA_BOUNDS = (-50.0, 100.0)
GROWTH_BOUNDS = (-1000.0, 1000.0)


class FundamentalCleaner(BaseCleaner):
    """基本面数据清洗器"""

    def validate(self, raw_df: pd.DataFrame) -> list[str]:
        return FinancialSchema.validate(raw_df)

    def clean(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, CleanReport]:
        errors = self.validate(raw_df)
        if errors:
            logger.warning("财务数据校验警告: %s", errors)

        report = CleanReport(input_rows=len(raw_df))
        df = raw_df.copy()

        # [1] 基础验证 + 类型转换
        df = self._basic_clean(df, report)

        # [2] 去重（同一 ts_code+end_date 保留最新 ann_date）
        df = self._deduplicate(df, report)

        # [3] 异常值标记
        df = self._mark_anomaly(df, report)

        report.output_rows = len(df)
        report.dropped_rows = report.input_rows - report.output_rows
        return df, report

    def build_pit_table(
        self,
        financial_df: pd.DataFrame,
        trade_dates: list[str],
    ) -> pd.DataFrame:
        """
        构建 Point-in-Time 表。

        对每个 trade_date，找到当时已公告的最新财报数据。

        Parameters
        ----------
        financial_df : DataFrame
            已清洗的财务数据（含 ann_date）。
        trade_dates : list[str]
            需要对齐的交易日列表（YYYYMMDD）。

        Returns
        -------
        DataFrame
            每个 (ts_code, trade_date) 对应的最新可用财报。
        """
        if financial_df.empty:
            return pd.DataFrame()

        df = financial_df.copy()
        df = df.sort_values(["ts_code", "ann_date", "end_date"])

        results = []
        for code, grp in df.groupby("ts_code"):
            grp = grp.sort_values("ann_date")
            for td in trade_dates:
                available = grp[grp["ann_date"] <= td]
                if available.empty:
                    continue
                latest = available.iloc[-1].copy()
                latest["trade_date"] = td
                results.append(latest)

        if not results:
            return pd.DataFrame()

        return pd.DataFrame(results).reset_index(drop=True)

    @staticmethod
    def compute_ttm(
        quarterly_df: pd.DataFrame,
        value_col: str,
    ) -> pd.DataFrame:
        """
        计算 TTM（滚动12个月）。

        TTM 规则:
            Q1:   TTM = Q1(今) + 年报(去) - Q1(去)
            H1:   TTM = H1(今) + 年报(去) - H1(去)
            Q3:   TTM = Q3(今) + 年报(去) - Q3(去)
            年报: TTM = 年报(今)

        Parameters
        ----------
        quarterly_df : DataFrame
            含 ts_code, end_date, value_col 列。
        value_col : str
            需要 TTM 化的列名。

        Returns
        -------
        DataFrame
            增加 {value_col}_ttm 列。
        """
        df = quarterly_df.copy()
        df["end_date"] = pd.to_datetime(df["end_date"])
        df = df.sort_values(["ts_code", "end_date"])
        df["quarter"] = df["end_date"].dt.month
        df["year"] = df["end_date"].dt.year

        ttm_col = f"{value_col}_ttm"
        df[ttm_col] = np.nan

        for code, grp in df.groupby("ts_code"):
            grp = grp.sort_values("end_date")
            for idx, row in grp.iterrows():
                q = row["quarter"]
                y = row["year"]
                val = row[value_col]

                if pd.isna(val):
                    continue

                if q == 12:
                    df.loc[idx, ttm_col] = val
                else:
                    annual_mask = (grp["year"] == y - 1) & (grp["quarter"] == 12)
                    same_q_mask = (grp["year"] == y - 1) & (grp["quarter"] == q)

                    annual = grp.loc[annual_mask, value_col]
                    same_q = grp.loc[same_q_mask, value_col]

                    if not annual.empty and not same_q.empty:
                        df.loc[idx, ttm_col] = (
                            val + annual.iloc[0] - same_q.iloc[0]
                        )

        return df

    # ================================================================
    # 清洗步骤
    # ================================================================

    def _basic_clean(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        if "ann_date" in df.columns:
            df["ann_date"] = df["ann_date"].astype(str).str.replace("-", "")
        if "end_date" in df.columns:
            df["end_date"] = df["end_date"].astype(str).str.replace("-", "")

        bad_mask = (
            df["ann_date"].isna()
            | df["end_date"].isna()
            | (df["ann_date"] == "")
            | (df["end_date"] == "")
        )
        dropped = bad_mask.sum()
        if dropped:
            df = df[~bad_mask].copy()
            report.details["missing_date_dropped"] = int(dropped)

        numeric_cols = ["roe", "roa", "gross_margin", "net_profit_yoy", "revenue_yoy"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _deduplicate(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        before = len(df)
        df = df.sort_values(["ts_code", "end_date", "ann_date"])
        df = df.drop_duplicates(subset=["ts_code", "end_date"], keep="last")
        dup = before - len(df)
        if dup:
            report.details["duplicates_removed"] = dup
        return df

    def _mark_anomaly(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        df["is_anomaly"] = False

        if "roe" in df.columns:
            mask = ~df["roe"].between(*ROE_BOUNDS) & df["roe"].notna()
            df.loc[mask, "is_anomaly"] = True

        if "roa" in df.columns:
            mask = ~df["roa"].between(*ROA_BOUNDS) & df["roa"].notna()
            df.loc[mask, "is_anomaly"] = True

        for col in ["net_profit_yoy", "revenue_yoy"]:
            if col in df.columns:
                mask = ~df[col].between(*GROWTH_BOUNDS) & df[col].notna()
                df.loc[mask, "is_anomaly"] = True

        anomaly_count = df["is_anomaly"].sum()
        report.flagged_anomalies = int(anomaly_count)
        report.details["anomaly_count"] = int(anomaly_count)
        return df
