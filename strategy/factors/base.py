"""
因子基类。

所有因子继承 BaseFactor，实现 compute() 方法。
输入/输出均为 pivot 格式: index=trade_date, columns=ts_code, values=factor_value。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class FactorMeta:
    """因子元信息"""
    name: str
    category: str          # momentum / mean_reversion / volume / volatility / fundamental
    description: str = ""
    window: int = 0        # 需要的历史回看窗口天数
    direction: int = 1     # 1=因子越大越好(做多), -1=因子越小越好


class BaseFactor(ABC):
    """因子抽象基类"""

    meta: FactorMeta

    @abstractmethod
    def compute(self, price_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        计算因子值。

        Parameters
        ----------
        price_df : DataFrame
            pivot 格式价格数据，index=trade_date, columns=ts_code。
            具体含义由因子自行约定（close / volume / amount 等）。
        **kwargs
            额外数据（如估值 DataFrame）。

        Returns
        -------
        DataFrame
            pivot 格式因子值，index=trade_date, columns=ts_code。
        """
        ...

    @property
    def name(self) -> str:
        return self.meta.name

    @property
    def category(self) -> str:
        return self.meta.category

    @staticmethod
    def _rank_normalize(df: pd.DataFrame) -> pd.DataFrame:
        """截面排名归一化到 [0, 1]"""
        return df.rank(axis=1, pct=True)

    @staticmethod
    def _zscore_normalize(df: pd.DataFrame) -> pd.DataFrame:
        """截面 Z-Score 标准化"""
        mean = df.mean(axis=1)
        std = df.std(axis=1)
        return df.sub(mean, axis=0).div(std.replace(0, np.nan), axis=0)

    @staticmethod
    def _winsorize(df: pd.DataFrame, n_sigma: float = 3.0) -> pd.DataFrame:
        """截面去极值 (MAD 法)"""
        median = df.median(axis=1)
        mad = (df.sub(median, axis=0)).abs().median(axis=1)
        mad_e = 1.4826 * mad
        upper = median + n_sigma * mad_e
        lower = median - n_sigma * mad_e
        return df.clip(lower=lower, upper=upper, axis=0)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
