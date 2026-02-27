"""
清洗器基类。

约定:
    1. clean() 纯函数语义，不修改输入 DataFrame
    2. 返回 (cleaned_df, CleanReport)
    3. 异常数据记录到 report，不静默丢弃
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from data.common.models import CleanReport


class BaseCleaner(ABC):
    """数据清洗抽象基类"""

    @abstractmethod
    def clean(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, CleanReport]:
        """
        执行清洗。

        Parameters
        ----------
        raw_df : DataFrame
            原始数据（不会被修改）。

        Returns
        -------
        tuple[DataFrame, CleanReport]
            (清洗后数据, 清洗报告)
        """
        ...

    @abstractmethod
    def validate(self, raw_df: pd.DataFrame) -> list[str]:
        """
        验证输入数据基本格式。

        Returns
        -------
        list[str]
            错误列表（空 = 通过）。
        """
        ...
