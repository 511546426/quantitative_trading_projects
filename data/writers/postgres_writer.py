"""
PostgreSQL 写入器。

使用 ON CONFLICT DO UPDATE 实现幂等写入。
支持 stock_info、financial_indicator、daily_valuation、dividend、trade_calendar 表。
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.common.exceptions import ConnectionLostError, WriteError
from data.writers.base import BaseWriter

logger = logging.getLogger(__name__)

DDL_TABLES = {
    "stock_info": """
        CREATE TABLE IF NOT EXISTS stock_info (
            ts_code      VARCHAR(12) PRIMARY KEY,
            name         VARCHAR(30),
            industry     VARCHAR(30),
            market       VARCHAR(10),
            list_date    DATE,
            delist_date  DATE,
            is_st        BOOLEAN DEFAULT FALSE,
            is_delisted  BOOLEAN DEFAULT FALSE,
            st_type      VARCHAR(10) DEFAULT '',
            updated_at   TIMESTAMP DEFAULT NOW()
        )
    """,
    "financial_indicator": """
        CREATE TABLE IF NOT EXISTS financial_indicator (
            ts_code          VARCHAR(12),
            ann_date         DATE,
            end_date         DATE,
            roe              FLOAT,
            roa              FLOAT,
            gross_margin     FLOAT,
            net_profit_yoy   FLOAT,
            revenue_yoy      FLOAT,
            is_anomaly       BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (ts_code, end_date)
        )
    """,
    "daily_valuation": """
        CREATE TABLE IF NOT EXISTS daily_valuation (
            ts_code      VARCHAR(12),
            trade_date   DATE,
            pe_ttm       FLOAT,
            pb           FLOAT,
            ps_ttm       FLOAT,
            total_mv     FLOAT,
            circ_mv      FLOAT,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,
    "dividend": """
        CREATE TABLE IF NOT EXISTS dividend (
            ts_code      VARCHAR(12),
            ann_date     DATE,
            ex_date      DATE,
            div_proc     VARCHAR(20),
            cash_div     FLOAT,
            share_div    FLOAT,
            PRIMARY KEY (ts_code, ann_date)
        )
    """,
    "trade_calendar": """
        CREATE TABLE IF NOT EXISTS trade_calendar (
            exchange     VARCHAR(10),
            cal_date     DATE,
            is_open      BOOLEAN,
            PRIMARY KEY (exchange, cal_date)
        )
    """,
    "backfill_checkpoint": """
        CREATE TABLE IF NOT EXISTS backfill_checkpoint (
            task_name    VARCHAR(50) PRIMARY KEY,
            last_date    DATE,
            total_dates  INT,
            done_dates   INT,
            status       VARCHAR(20),
            error_msg    TEXT,
            started_at   TIMESTAMP,
            updated_at   TIMESTAMP DEFAULT NOW()
        )
    """,
}


class PostgresWriter(BaseWriter):
    """PostgreSQL 数据写入器"""

    target_name = "postgres"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "quant",
        user: str = "postgres",
        password: str = "",
    ):
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._conn = None

    def connect(self) -> None:
        try:
            import psycopg2

            self._conn = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._database,
                user=self._user,
                password=self._password,
            )
            self._conn.autocommit = False
            logger.info(
                "PostgreSQL 连接成功: %s:%d/%s", self._host, self._port, self._database
            )
        except Exception as e:
            raise ConnectionLostError(
                f"PostgreSQL 连接失败: {e}", target=self.target_name
            ) from e

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
        logger.info("PostgreSQL 连接关闭")

    def init_tables(self) -> None:
        """创建所有必要的表"""
        self._ensure_conn()
        cur = self._conn.cursor()
        try:
            for table_name, ddl in DDL_TABLES.items():
                cur.execute(ddl)
            self._conn.commit()
            logger.info("PostgreSQL 表初始化完成")
        except Exception as e:
            self._conn.rollback()
            raise WriteError(f"建表失败: {e}", target=self.target_name) from e
        finally:
            cur.close()

    def write_batch(self, df: pd.DataFrame, table: str) -> int:
        self._ensure_conn()
        if df.empty:
            return 0

        df = self._prepare_df(df)
        valid_cols = self._get_table_columns(table)
        if valid_cols:
            df = df[[c for c in df.columns if c in valid_cols]]
        columns = list(df.columns)
        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        cur = self._conn.cursor()
        try:
            data = [tuple(row) for row in df.values]
            from psycopg2.extras import execute_batch

            sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
            execute_batch(cur, sql, data, page_size=1000)
            self._conn.commit()
            logger.info("PostgreSQL 写入 %s: %d 行", table, len(data))
            return len(data)
        except Exception as e:
            self._conn.rollback()
            raise WriteError(
                f"PostgreSQL 写入失败 ({table}): {e}", target=self.target_name
            ) from e
        finally:
            cur.close()

    def _get_table_columns(self, table: str) -> set[str]:
        """查询 PostgreSQL 目标表的实际列名"""
        cur = self._conn.cursor()
        try:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s AND table_schema = 'public'",
                (table,),
            )
            return {row[0] for row in cur.fetchall()}
        finally:
            cur.close()

    def upsert(
        self, df: pd.DataFrame, table: str, conflict_keys: list[str]
    ) -> int:
        self._ensure_conn()
        if df.empty:
            return 0

        df = self._prepare_df(df)
        valid_cols = self._get_table_columns(table)
        if valid_cols:
            df = df[[c for c in df.columns if c in valid_cols]]
        columns = list(df.columns)
        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict_str = ", ".join(conflict_keys)

        update_cols = [c for c in columns if c not in conflict_keys]
        if update_cols:
            update_str = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in update_cols
            )
            on_conflict = f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
        else:
            on_conflict = f"ON CONFLICT ({conflict_str}) DO NOTHING"

        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) {on_conflict}"

        cur = self._conn.cursor()
        try:
            data = [tuple(row) for row in df.values]
            from psycopg2.extras import execute_batch

            execute_batch(cur, sql, data, page_size=1000)
            self._conn.commit()
            logger.info("PostgreSQL upsert %s: %d 行", table, len(data))
            return len(data)
        except Exception as e:
            self._conn.rollback()
            raise WriteError(
                f"PostgreSQL upsert 失败 ({table}): {e}", target=self.target_name
            ) from e
        finally:
            cur.close()

    def get_latest_date(
        self, table: str, ts_code: str | None = None
    ) -> str | None:
        self._ensure_conn()
        cur = self._conn.cursor()
        try:
            date_col = "cal_date" if table == "trade_calendar" else "trade_date"
            if table in ("stock_info",):
                return None

            where = f"WHERE ts_code = '{ts_code}'" if ts_code else ""
            sql = f"SELECT max({date_col}) FROM {table} {where}"
            cur.execute(sql)
            result = cur.fetchone()
            if result and result[0]:
                return result[0].strftime("%Y%m%d")
            return None
        except Exception:
            return None
        finally:
            cur.close()

    def execute_query(self, sql: str, params: tuple = ()) -> list:
        self._ensure_conn()
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    def health_check(self) -> bool:
        try:
            self._ensure_conn()
            cur = self._conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except Exception:
            return False

    def _ensure_conn(self) -> None:
        if self._conn is None or self._conn.closed:
            self.connect()

    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        """将 DataFrame 转换为 PostgreSQL 兼容格式"""
        df = df.copy()

        date_cols = ["trade_date", "cal_date", "ann_date", "end_date",
                     "list_date", "delist_date", "ex_date"]
        for col in date_cols:
            if col in df.columns:
                s = df[col].astype(str).str.replace("-", "")
                df[col] = pd.to_datetime(s, format="%Y%m%d", errors="coerce").dt.date

        bool_cols = ["is_open", "is_st", "is_delisted", "is_anomaly"]
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)

        df = df.where(pd.notnull(df), None)
        return df
