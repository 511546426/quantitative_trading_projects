"""
数据写入器基类。

约定:
    1. write_batch: 批量写入（首次灌入/大批量）
    2. upsert: 幂等写入（增量更新）
    3. 所有写入自动处理连接管理
    4. 写入失败抛出 WriteError
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseWriter(ABC):
    """数据写入抽象基类"""

    target_name: str = "base"

    @abstractmethod
    def write_batch(self, df: pd.DataFrame, table: str) -> int:
        """
        批量写入。

        Returns
        -------
        int
            写入行数。
        """
        ...

    @abstractmethod
    def upsert(
        self, df: pd.DataFrame, table: str, conflict_keys: list[str]
    ) -> int:
        """
        幂等写入（存在则更新，不存在则插入）。

        Returns
        -------
        int
            受影响行数。
        """
        ...

    @abstractmethod
    def get_latest_date(
        self, table: str, ts_code: str | None = None
    ) -> str | None:
        """查询某张表的最新数据日期"""
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """检查存储连接是否正常"""
        ...

    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
