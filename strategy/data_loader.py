"""
A 股多因子策略 — 数据加载模块

从 ClickHouse / PostgreSQL 按年分块加载行情、估值、股票池过滤信息。
"""
from __future__ import annotations

import gc
import logging
from typing import Any

import numpy as np
import pandas as pd

from strategy.config import START, END, BENCHMARK

logger = logging.getLogger(__name__)


def connect_db(cfg=None):
    """创建 ClickHouse + PostgreSQL 连接。

    使用 ``data/common/db.py`` 统一工厂（单例、自动配置）；cfg 为 None
    时自动从 YAML + .env 加载（兼容旧调用方传入 cfg）。
    """
    from data.common.db import get_ch, get_pg

    ch = get_ch()
    pg = get_pg()
    return ch, pg


def _ymd_to_ts(s: str) -> pd.Timestamp:
    """YYYYMMDD → Timestamp（日频对齐）。"""
    return pd.Timestamp(f"{s[:4]}-{s[4:6]}-{s[6:8]}")


def _slice_panel(df: pd.DataFrame, date_start: str, date_end: str) -> pd.DataFrame:
    t0, t1 = _ymd_to_ts(date_start), _ymd_to_ts(date_end)
    return df.loc[t0:t1]


def load_price(
    ch,
    date_start: str | None = None,
    date_end: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """按年分块加载复权收盘价和成交额。"""
    ds = date_start or START
    de = date_end or END
    sy, ey = int(ds[:4]), int(de[:4])
    c_close: list[pd.DataFrame] = []
    c_amount: list[pd.DataFrame] = []

    for year in range(sy, ey + 1):
        ys, ye = f"{year}-01-01", f"{year}-12-31"
        sql = f"""
            SELECT ts_code, trade_date,
                   argMax(adj_close, trade_date) AS adj_close,
                   argMax(amount,    trade_date) AS amount
            FROM stock_daily
            WHERE trade_date >= '{ys}' AND trade_date <= '{ye}'
              AND is_suspended = 0
            GROUP BY ts_code, trade_date
        """
        rows = ch._client.execute(sql)
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_close", "amount"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df[["adj_close", "amount"]] = df[["adj_close", "amount"]].astype(np.float32)
        c_close.append(df.pivot(index="trade_date", columns="ts_code", values="adj_close"))
        c_amount.append(df.pivot(index="trade_date", columns="ts_code", values="amount"))
        del df
        gc.collect()
        logger.info("  行情: %d 年", year)

    close = pd.concat(c_close, axis=0, sort=True).sort_index()
    del c_close
    gc.collect()
    amount = pd.concat(c_amount, axis=0, sort=True).sort_index()
    del c_amount
    gc.collect()

    close = _slice_panel(close, ds, de)
    amount = _slice_panel(amount, ds, de)
    logger.info("行情: %d 交易日 × %d 只", *close.shape)
    return close, amount


def load_valuation(
    pg,
    date_start: str | None = None,
    date_end: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """按年分块加载 PB、PE_TTM、流通市值。"""
    ds = date_start or START
    de = date_end or END
    sy, ey = int(ds[:4]), int(de[:4])
    c_pb: list[pd.DataFrame] = []
    c_pe: list[pd.DataFrame] = []
    c_mv: list[pd.DataFrame] = []

    for year in range(sy, ey + 1):
        ys, ye = f"{year}-01-01", f"{year}-12-31"
        rows = pg.execute_query(
            "SELECT ts_code, trade_date, pb, pe_ttm, circ_mv "
            "FROM daily_valuation WHERE trade_date >= %s AND trade_date <= %s",
            (ys, ye),
        )
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "pb", "pe_ttm", "circ_mv"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        for c in ("pb", "pe_ttm", "circ_mv"):
            df[c] = df[c].astype(np.float32)
        c_pb.append(df.pivot(index="trade_date", columns="ts_code", values="pb"))
        c_pe.append(df.pivot(index="trade_date", columns="ts_code", values="pe_ttm"))
        c_mv.append(df.pivot(index="trade_date", columns="ts_code", values="circ_mv"))
        del df
        gc.collect()

    pb = pd.concat(c_pb, axis=0, sort=True).sort_index()
    del c_pb
    gc.collect()
    pe = pd.concat(c_pe, axis=0, sort=True).sort_index()
    del c_pe
    gc.collect()
    mv = pd.concat(c_mv, axis=0, sort=True).sort_index()
    del c_mv
    gc.collect()

    pb = _slice_panel(pb, ds, de)
    pe = _slice_panel(pe, ds, de)
    mv = _slice_panel(mv, ds, de)
    logger.info("估值 (PB+PE+MV): %d 交易日 × %d 只", *pb.shape)
    return pb, pe, mv


def load_exclude_list(pg) -> set:
    rows = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
    )
    return {r[0] for r in rows}


def load_index_close(
    ch,
    ts_code: str = BENCHMARK,
    date_start: str | None = None,
    date_end: str | None = None,
) -> pd.Series | None:
    ds = date_start or START
    de = date_end or END
    sql = f"""
        SELECT trade_date, max(close) AS close
        FROM index_daily
        WHERE ts_code = '{ts_code}'
          AND trade_date >= toDate('{ds[:4]}-{ds[4:6]}-{ds[6:8]}')
          AND trade_date <= toDate('{de[:4]}-{de[4:6]}-{de[6:8]}')
        GROUP BY trade_date ORDER BY trade_date
    """
    try:
        rows = ch._client.execute(sql)
    except Exception as e:
        logger.warning("指数加载失败: %s", e)
        return None
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["trade_date", "close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    s = df.set_index("trade_date")["close"].astype(np.float64)
    logger.info("基准 %s: %d 交易日", ts_code, len(s))
    return s if len(s) >= 50 else None


def _max_drawdown_in_sample(net_ret: pd.Series) -> float:
    """样本内最大回撤"""
    nr = net_ret.dropna()
    if len(nr) < 2:
        return 0.0
    nav = (1 + nr).cumprod()
    return float((nav / nav.cummax() - 1).min())
