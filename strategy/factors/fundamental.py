"""
基本面因子。

基本面因子来自估值指标 (PE/PB/PS) 和财务质量 (ROE/毛利率/增速)。
数据源: PostgreSQL daily_valuation / financial_indicator 表。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategy.factors.base import BaseFactor, FactorMeta

__all__ = [
    "EP_Factor",
    "BP_Factor",
    "SP_Factor",
    "MarketCap",
    "ROE_Factor",
    "GrossProfitMargin",
    "NetProfitGrowth",
    "RevenueGrowth",
]


class EP_Factor(BaseFactor):
    """
    EP (Earnings-to-Price) 因子，即 1/PE。

    EP 越高 → 估值越便宜 → 价值因子方向做多。
    使用 PE_TTM 的倒数，避免 PE 为负时的问题。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="ep",
            category="fundamental",
            description="盈利收益率(1/PE_TTM)",
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        pe_df = kwargs.get("pe_df")
        if pe_df is None:
            raise ValueError("EP_Factor requires pe_df (PE_TTM pivot DataFrame)")
        pe_clean = pe_df.replace(0, np.nan)
        return 1.0 / pe_clean


class BP_Factor(BaseFactor):
    """
    BP (Book-to-Price) 因子，即 1/PB。

    经典价值因子 (Fama-French HML)。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="bp",
            category="fundamental",
            description="账面市值比(1/PB)",
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        pb_df = kwargs.get("pb_df")
        if pb_df is None:
            raise ValueError("BP_Factor requires pb_df (PB pivot DataFrame)")
        pb_clean = pb_df.replace(0, np.nan)
        return 1.0 / pb_clean


class SP_Factor(BaseFactor):
    """
    SP (Sales-to-Price) 因子，即 1/PS_TTM。

    营收估值因子，对亏损股也有效 (PE 为负时 EP 失效)。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="sp",
            category="fundamental",
            description="营收价格比(1/PS_TTM)",
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        ps_df = kwargs.get("ps_df")
        if ps_df is None:
            raise ValueError("SP_Factor requires ps_df (PS_TTM pivot DataFrame)")
        ps_clean = ps_df.replace(0, np.nan)
        return 1.0 / ps_clean


class MarketCap(BaseFactor):
    """
    市值因子 (SMB)。

    A 股小市值效应显著，direction=-1 表示小市值做多。
    使用对数市值减小量纲影响。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="ln_market_cap",
            category="fundamental",
            description="对数总市值",
            direction=-1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        mv_df = kwargs.get("total_mv_df")
        if mv_df is None:
            raise ValueError("MarketCap requires total_mv_df (总市值 pivot DataFrame)")
        return np.log(mv_df.replace(0, np.nan))


class ROE_Factor(BaseFactor):
    """
    ROE (净资产收益率) 因子。

    高 ROE → 盈利能力强，质量因子核心指标。
    需传入 roe_df (已按季频对齐到日频的 pivot 表)。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="roe",
            category="fundamental",
            description="净资产收益率",
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        roe_df = kwargs.get("roe_df")
        if roe_df is None:
            raise ValueError("ROE_Factor requires roe_df")
        return roe_df


class GrossProfitMargin(BaseFactor):
    """
    毛利率因子。

    高毛利率 → 产品竞争力强 / 护城河深。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="gross_margin",
            category="fundamental",
            description="毛利率",
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        gm_df = kwargs.get("gross_margin_df")
        if gm_df is None:
            raise ValueError("GrossProfitMargin requires gross_margin_df")
        return gm_df


class NetProfitGrowth(BaseFactor):
    """
    净利润同比增速因子。

    成长因子，利润加速增长往往预示股价走强。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="net_profit_yoy",
            category="fundamental",
            description="净利润同比增速",
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        npg_df = kwargs.get("net_profit_yoy_df")
        if npg_df is None:
            raise ValueError("NetProfitGrowth requires net_profit_yoy_df")
        return npg_df


class RevenueGrowth(BaseFactor):
    """
    营收同比增速因子。

    营收增长是利润增长的先行指标。
    """

    def __init__(self):
        self.meta = FactorMeta(
            name="revenue_yoy",
            category="fundamental",
            description="营收同比增速",
            direction=1,
        )

    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        rg_df = kwargs.get("revenue_yoy_df")
        if rg_df is None:
            raise ValueError("RevenueGrowth requires revenue_yoy_df")
        return rg_df
