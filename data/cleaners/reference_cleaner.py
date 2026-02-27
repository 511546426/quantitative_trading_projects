"""
基础信息清洗器。

功能:
    - 退市标记
    - ST 标记（区分 ST / *ST）
    - 板块分类
    - 上市天数计算
    - 可交易股票池过滤
"""
from __future__ import annotations

import logging

import pandas as pd

from data.cleaners.base import BaseCleaner
from data.common.models import CleanReport, StockListSchema

logger = logging.getLogger(__name__)

MIN_LISTING_DAYS = 60


class ReferenceCleaner(BaseCleaner):
    """基础信息清洗器"""

    def __init__(self, min_listing_days: int = MIN_LISTING_DAYS):
        self._min_listing_days = min_listing_days

    def validate(self, raw_df: pd.DataFrame) -> list[str]:
        return StockListSchema.validate(raw_df)

    def clean(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, CleanReport]:
        errors = self.validate(raw_df)
        if errors:
            logger.warning("股票列表校验警告: %s", errors)

        report = CleanReport(input_rows=len(raw_df))
        df = raw_df.copy()

        # [1] 退市标记
        df = self._mark_delisted(df, report)

        # [2] ST 标记
        df = self._mark_st(df, report)

        # [3] 板块分类
        df = self._classify_board(df, report)

        report.output_rows = len(df)
        report.dropped_rows = report.input_rows - report.output_rows
        return df, report

    def get_tradable_pool(
        self,
        stock_df: pd.DataFrame,
        trade_date: str,
        exclude_st: bool = True,
        exclude_delisted: bool = True,
        suspended_codes: set[str] | None = None,
    ) -> pd.DataFrame:
        """
        获取指定交易日的可交易股票池。

        过滤条件:
            - 非退市
            - 非 ST（可配置）
            - 上市满 N 个交易日
            - 非停牌

        Parameters
        ----------
        stock_df : DataFrame
            清洗后的股票基础信息。
        trade_date : str
            交易日 (YYYYMMDD)。
        exclude_st : bool
            是否排除 ST 股票。
        exclude_delisted : bool
            是否排除已退市股票。
        suspended_codes : set[str], optional
            当日停牌的股票代码集合。
        """
        df = stock_df.copy()

        if exclude_delisted:
            df = df[~df.get("is_delisted", False)]

        if exclude_st:
            df = df[~df.get("is_st", False)]

        if "list_date" in df.columns and trade_date:
            td = pd.to_datetime(trade_date, format="%Y%m%d")
            ld = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce")
            df["listing_days"] = (td - ld).dt.days
            df = df[df["listing_days"] >= self._min_listing_days]

        if suspended_codes:
            df = df[~df["ts_code"].isin(suspended_codes)]

        logger.info(
            "可交易池: %d 只 (trade_date=%s, excl_st=%s)",
            len(df), trade_date, exclude_st,
        )
        return df.reset_index(drop=True)

    # ================================================================
    # 清洗步骤
    # ================================================================

    def _mark_delisted(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        if "delist_date" in df.columns:
            df["is_delisted"] = df["delist_date"].notna() & (df["delist_date"] != "")
        else:
            df["is_delisted"] = False

        delisted_count = df["is_delisted"].sum()
        report.details["delisted_count"] = int(delisted_count)
        return df

    def _mark_st(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        if "name" in df.columns:
            df["is_st"] = df["name"].str.contains("ST", na=False)
            df["st_type"] = ""
            df.loc[df["name"].str.contains(r"\*ST", na=False), "st_type"] = "*ST"
            df.loc[
                df["name"].str.contains("ST", na=False)
                & ~df["name"].str.contains(r"\*ST", na=False),
                "st_type",
            ] = "ST"
        else:
            df["is_st"] = False
            df["st_type"] = ""

        st_count = df["is_st"].sum()
        report.details["st_count"] = int(st_count)
        return df

    def _classify_board(self, df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
        """根据代码前缀推断板块"""
        if "market" not in df.columns or df["market"].isna().all():
            df["market"] = df["ts_code"].apply(_infer_board)

        board_counts = df["market"].value_counts().to_dict()
        report.details["board_distribution"] = board_counts
        return df


def _infer_board(ts_code: str) -> str:
    code = ts_code.split(".")[0] if "." in ts_code else ts_code
    if code.startswith("688"):
        return "科创板"
    if code.startswith(("300", "301")):
        return "创业板"
    if code.startswith("8"):
        return "北交所"
    return "主板"
