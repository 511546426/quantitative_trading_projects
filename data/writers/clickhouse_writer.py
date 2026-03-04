"""
ClickHouse 写入器。

使用 ReplacingMergeTree 引擎自动去重。
批量写入使用 insert_dataframe 接口。
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.common.exceptions import ConnectionLostError, WriteError
from data.writers.base import BaseWriter

logger = logging.getLogger(__name__)

DDL_STOCK_DAILY = """
CREATE TABLE IF NOT EXISTS stock_daily (
    trade_date     Date,
    ts_code        LowCardinality(String),
    open           Float32,
    high           Float32,
    low            Float32,
    close          Float32,
    adj_open       Float32,
    adj_high       Float32,
    adj_low        Float32,
    adj_close      Float32,
    volume         Float64,
    amount         Float64,
    pct_chg        Float32,
    turn           Float32,
    adj_factor     Float32,
    is_suspended   UInt8,
    is_limit_up    UInt8,
    is_limit_down  UInt8,
    is_anomaly     UInt8
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ts_code, trade_date)
"""

DDL_INDEX_DAILY = """
CREATE TABLE IF NOT EXISTS index_daily (
    trade_date  Date,
    ts_code     LowCardinality(String),
    open        Float32,
    high        Float32,
    low         Float32,
    close       Float32,
    volume      Float64,
    amount      Float64,
    pct_chg     Float32
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ts_code, trade_date)
"""


class ClickHouseWriter(BaseWriter):
    """ClickHouse 数据写入器"""

    target_name = "clickhouse"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        database: str = "quant",
        user: str = "default",
        password: str = "",
    ):
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._client = None

    def connect(self) -> None:
        try:
            from clickhouse_driver import Client

            self._client = Client(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password,
            )
            self._client.execute(f"CREATE DATABASE IF NOT EXISTS {self._database}")
            self._client.execute(f"USE {self._database}")
            logger.info("ClickHouse 连接成功: %s:%d/%s", self._host, self._port, self._database)
        except Exception as e:
            raise ConnectionLostError(
                f"ClickHouse 连接失败: {e}", target=self.target_name
            ) from e

    def close(self) -> None:
        if self._client:
            self._client.disconnect()
            self._client = None
        logger.info("ClickHouse 连接关闭")

    def init_tables(self) -> None:
        """创建所有必要的表"""
        self._ensure_client()
        for ddl in [DDL_STOCK_DAILY, DDL_INDEX_DAILY]:
            self._client.execute(ddl)
        logger.info("ClickHouse 表初始化完成")

    def _get_table_columns(self, table: str) -> set[str]:
        """查询 ClickHouse 目标表的实际列名"""
        rows = self._client.execute(
            f"SELECT name FROM system.columns WHERE database = '{self._database}' AND table = '{table}'"
        )
        return {r[0] for r in rows}

    def write_batch(self, df: pd.DataFrame, table: str) -> int:
        self._ensure_client()
        if df.empty:
            return 0

        df_prepared = self._prepare_df(df, table)
        valid_cols = self._get_table_columns(table)
        if valid_cols:
            df_prepared = df_prepared[[c for c in df_prepared.columns if c in valid_cols]]
        try:
            columns = list(df_prepared.columns)
            data = df_prepared.values.tolist()
            col_str = ", ".join(columns)
            self._client.execute(
                f"INSERT INTO {table} ({col_str}) VALUES",
                data,
            )
            logger.info("ClickHouse 写入 %s: %d 行", table, len(df_prepared))
            return len(df_prepared)
        except Exception as e:
            raise WriteError(
                f"ClickHouse 写入失败 ({table}): {e}", target=self.target_name
            ) from e

    def upsert(
        self, df: pd.DataFrame, table: str, conflict_keys: list[str]
    ) -> int:
        """
        ClickHouse ReplacingMergeTree 通过后台 merge 自动去重，
        直接 INSERT 即可实现幂等。
        """
        return self.write_batch(df, table)

    def get_latest_date(
        self, table: str, ts_code: str | None = None
    ) -> str | None:
        self._ensure_client()
        try:
            where = f"WHERE ts_code = '{ts_code}'" if ts_code else ""
            sql = f"SELECT max(trade_date) FROM {table} {where}"
            result = self._client.execute(sql)
            if result and result[0][0]:
                d = result[0][0]
                if hasattr(d, "strftime"):
                    return d.strftime("%Y%m%d")
                return str(d).replace("-", "")
            return None
        except Exception:
            return None

    def count_rows(self, table: str, trade_date: str | None = None) -> int:
        self._ensure_client()
        where = f"WHERE trade_date = '{trade_date}'" if trade_date else ""
        sql = f"SELECT count() FROM {table} FINAL {where}"
        result = self._client.execute(sql)
        return result[0][0] if result else 0

    def health_check(self) -> bool:
        try:
            self._ensure_client()
            self._client.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _ensure_client(self) -> None:
        if self._client is None:
            self.connect()

    def _prepare_df(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        """将 DataFrame 转换为 ClickHouse 兼容格式"""
        df = df.copy()

        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(
                df["trade_date"].astype(str).str.replace("-", ""),
                format="%Y%m%d",
            ).dt.date

        bool_cols = [
            "is_suspended", "is_limit_up", "is_limit_down",
            "is_anomaly", "is_one_word_limit",
        ]
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(int)

        float_cols = [
            "open", "high", "low", "close",
            "adj_open", "adj_high", "adj_low", "adj_close",
            "volume", "amount", "pct_chg", "turn", "adj_factor",
        ]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df
