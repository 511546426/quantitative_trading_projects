"""
Redis 写入器 — 实时行情快照与运行时状态。

Key 设计:
    rt:quote:{ts_code}      Hash  实时行情
    rt:market:stats          Hash  涨跌统计
    strategy:{name}:status   Hash  策略运行状态
    risk:{name}              Hash  风控参数
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.common.exceptions import ConnectionLostError, WriteError
from data.writers.base import BaseWriter

logger = logging.getLogger(__name__)

QUOTE_TTL = 7200  # 行情快照收盘后 2 小时过期


class RedisWriter(BaseWriter):
    """Redis 数据写入器"""

    target_name = "redis"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str = "",
    ):
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._client = None

    def connect(self) -> None:
        try:
            import redis

            self._client = redis.Redis(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password or None,
                decode_responses=True,
            )
            self._client.ping()
            logger.info("Redis 连接成功: %s:%d/%d", self._host, self._port, self._db)
        except Exception as e:
            raise ConnectionLostError(
                f"Redis 连接失败: {e}", target=self.target_name
            ) from e

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        logger.info("Redis 连接关闭")

    def write_batch(self, df: pd.DataFrame, table: str) -> int:
        """将 DataFrame 批量写入 Redis Hash（每行一个 key）"""
        self._ensure_client()
        if df.empty:
            return 0

        try:
            pipe = self._client.pipeline()
            count = 0

            for _, row in df.iterrows():
                ts_code = row.get("ts_code", "")
                key = f"{table}:{ts_code}"
                mapping = {
                    k: str(v) for k, v in row.to_dict().items()
                    if v is not None and pd.notna(v)
                }
                pipe.hset(key, mapping=mapping)
                if table.startswith("rt:"):
                    pipe.expire(key, QUOTE_TTL)
                count += 1

            pipe.execute()
            logger.info("Redis 写入 %s: %d 条", table, count)
            return count
        except Exception as e:
            raise WriteError(
                f"Redis 写入失败 ({table}): {e}", target=self.target_name
            ) from e

    def upsert(
        self, df: pd.DataFrame, table: str, conflict_keys: list[str]
    ) -> int:
        """Redis Hash 天然幂等（同 key 覆盖），等同于 write_batch"""
        return self.write_batch(df, table)

    def write_realtime_quotes(self, df: pd.DataFrame) -> int:
        """写入实时行情快照到 rt:quote:{ts_code}"""
        self._ensure_client()
        if df.empty:
            return 0

        try:
            pipe = self._client.pipeline()
            count = 0

            for _, row in df.iterrows():
                ts_code = row.get("ts_code", "")
                key = f"rt:quote:{ts_code}"
                mapping = {}
                for col in ["open", "high", "low", "close", "volume",
                            "amount", "pct_chg"]:
                    if col in row and pd.notna(row[col]):
                        mapping[col] = str(row[col])
                if mapping:
                    pipe.hset(key, mapping=mapping)
                    pipe.expire(key, QUOTE_TTL)
                    count += 1

            pipe.execute()
            logger.info("Redis 实时行情写入: %d 条", count)
            return count
        except Exception as e:
            raise WriteError(
                f"Redis 实时行情写入失败: {e}", target=self.target_name
            ) from e

    def write_market_stats(self, stats: dict[str, Any]) -> None:
        """写入涨跌统计"""
        self._ensure_client()
        mapping = {k: str(v) for k, v in stats.items()}
        self._client.hset("rt:market:stats", mapping=mapping)
        self._client.expire("rt:market:stats", QUOTE_TTL)

    def get_latest_date(
        self, table: str, ts_code: str | None = None
    ) -> str | None:
        """Redis 不存储历史日期，返回 None"""
        return None

    def health_check(self) -> bool:
        try:
            self._ensure_client()
            return self._client.ping()
        except Exception:
            return False

    def _ensure_client(self) -> None:
        if self._client is None:
            self.connect()
