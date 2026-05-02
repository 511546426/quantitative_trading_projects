"""
统一数据库连接管理

提供 ClickHouse、PostgreSQL、Redis 连接的工厂与缓存，
消除各处重复的连接代码。

用法::

    from data.common.db import get_ch, get_pg, close_all

    ch = get_ch()
    pg = get_pg()

    ch.init_tables()
    pg.init_tables()

    close_all()  # 应用退出时
"""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

from data.common.config import Config

logger = logging.getLogger(__name__)

_instances: dict[str, Any] = {}


def _try_close(key: str) -> None:
    """Safely close a cached connection without removing from cache."""
    inst = _instances.get(key)
    if inst is not None:
        try:
            inst.close()
        except Exception:
            pass


def _load_cfg() -> Config:
    """加载配置（带缓存，避免重复加载 YAML）。"""
    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    return cfg


# ─── ClickHouse ──────────────────────────────────────────────────────────


def get_ch(
    *,
    host: str | None = None,
    port: int | None = None,
    database: str = "quant",
    user: str | None = None,
    password: str | None = None,
    force_reconnect: bool = False,
) -> "ClickHouseWriter":
    """
    获取 ClickHouse 连接（单例，自动配置）。

    Parameters
    ----------
    host : str, optional
        覆盖配置中的 host。
    port : int, optional
        覆盖配置中的 port。
    database : str
        数据库名，默认 "quant"。
    user : str, optional
        覆盖配置中的 user。
    password : str, optional
        覆盖配置中的 password。
    force_reconnect : bool
        True 时强制重新连接。

    Returns
    -------
    ClickHouseWriter
    """
    from data.writers.clickhouse_writer import ClickHouseWriter

    cache_key = "ch"
    if force_reconnect and cache_key in _instances:
        _try_close(cache_key)
        del _instances[cache_key]

    if cache_key not in _instances:
        cfg = _load_cfg()
        ch = ClickHouseWriter(
            host=host or cfg.get("database.clickhouse.host", "localhost"),
            port=port or int(cfg.get("database.clickhouse.port", 9000)),
            database=database,
            user=user or cfg.get("database.clickhouse.user", "default"),
            password=password or cfg.get("database.clickhouse.password", ""),
        )
        ch.connect()
        _instances[cache_key] = ch
        logger.info("ClickHouse 连接已建立: %s:%d/%s", ch._host, ch._port, ch._database)
    elif _instances[cache_key]._client is None:
        try:
            _instances[cache_key].connect()
        except Exception:
            del _instances[cache_key]
            raise

    return _instances[cache_key]


# ─── PostgreSQL ──────────────────────────────────────────────────────────


def get_pg(
    *,
    host: str | None = None,
    port: int | None = None,
    database: str = "quant",
    user: str | None = None,
    password: str | None = None,
    force_reconnect: bool = False,
) -> "PostgresWriter":
    """
    获取 PostgreSQL 连接（单例，自动配置）。

    Parameters 同 get_ch()。
    """
    from data.writers.postgres_writer import PostgresWriter

    cache_key = "pg"
    if force_reconnect and cache_key in _instances:
        _try_close(cache_key)
        del _instances[cache_key]

    if cache_key not in _instances:
        cfg = _load_cfg()
        pg = PostgresWriter(
            host=host or cfg.get("database.postgres.host", "localhost"),
            port=port or int(cfg.get("database.postgres.port", 5432)),
            database=database,
            user=user or cfg.get("database.postgres.user", "postgres"),
            password=password or cfg.get("database.postgres.password", ""),
        )
        pg.connect()
        _instances[cache_key] = pg
        logger.info("PostgreSQL 连接已建立: %s:%d/%s", pg._host, pg._port, pg._database)
    elif _instances[cache_key]._conn is None:
        try:
            _instances[cache_key].connect()
        except Exception:
            del _instances[cache_key]
            raise

    return _instances[cache_key]


# ─── Redis ───────────────────────────────────────────────────────────────


def get_redis(
    *,
    host: str | None = None,
    port: int | None = None,
    password: str | None = None,
    decode_responses: bool = True,
) -> "Redis":
    """
    获取 Redis 连接（单例，自动配置）。

    Returns
    -------
    redis.Redis
    """
    from redis import Redis

    cache_key = "redis"
    if cache_key not in _instances:
        cfg = _load_cfg()
        r = Redis(
            host=host or cfg.get("database.redis.host", "localhost"),
            port=port or int(cfg.get("database.redis.port", 6379)),
            password=password or cfg.get("database.redis.password", None),
            decode_responses=decode_responses,
        )
        _instances[cache_key] = r
        logger.info("Redis 连接已建立")

    return _instances[cache_key]


# ─── 生命周期管理 ──────────────────────────────────────────────────────


def close_all() -> None:
    """关闭所有数据库连接。"""
    for key in list(_instances.keys()):
        try:
            if hasattr(_instances[key], "close"):
                _instances[key].close()
        except Exception as e:
            logger.debug("关闭 %s 连接失败: %s", key, e)
        del _instances[key]
    logger.info("所有数据库连接已关闭")


def init_all_tables() -> None:
    """初始化所有数据库的表结构（幂等，可安全重复调用）。"""
    ch = get_ch()
    pg = get_pg()
    try:
        ch.init_tables()
        pg.init_tables()
        logger.info("所有数据库表结构已初始化")
    except Exception as e:
        logger.error("数据库表初始化失败: %s", e)
        raise


# ─── 上下文管理器 ──────────────────────────────────────────────────────


class DBContext:
    """
    上下文管理器，自动关闭连接。

    用法::

        with DBContext() as db:
            db.ch.query(...)
            db.pg.execute(...)
    """

    def __enter__(self) -> "DBContext":
        self.ch = get_ch()
        self.pg = get_pg()
        return self

    def __exit__(self, *args: Any) -> None:
        close_all()
