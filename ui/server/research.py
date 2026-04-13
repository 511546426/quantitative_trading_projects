"""
单股 K 线 + 简易回测 API（ClickHouse stock_daily + PostgreSQL stock_info）；
并接入 ``regime_switching_strategy.run_regime_model_for_web`` 多因子 v4.1 全市场管线。
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from functools import lru_cache
from typing import Any, Literal

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from data.common.config import Config
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter
from ui.server.deps import require_api_key

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/research", tags=["research"], dependencies=[Depends(require_api_key)])

_TS_CODE_RE = re.compile(r"^[0-9]{6}\.(SH|SZ)$")
_DATE_RE = re.compile(r"^\d{8}$")


def _norm_ymd(s: str) -> str:
    if not _DATE_RE.match(s):
        raise HTTPException(400, "日期须为 YYYYMMDD")
    return s


def _ymd_to_iso(s: str) -> str:
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _validate_ts_code(ts_code: str) -> str:
    u = ts_code.strip().upper()
    if not _TS_CODE_RE.match(u):
        raise HTTPException(400, "ts_code 格式须为 000001.SH / 000001.SZ")
    return u


@lru_cache(maxsize=1)
def _ch() -> ClickHouseWriter:
    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch = ClickHouseWriter(
        host=cfg.get("database.clickhouse.host", "localhost"),
        port=int(cfg.get("database.clickhouse.port", 9000)),
        database="quant",
        user=cfg.get("database.clickhouse.user", "default"),
        password=cfg.get("database.clickhouse.password", ""),
    )
    ch.connect()
    return ch


@lru_cache(maxsize=1)
def _pg() -> PostgresWriter:
    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    pg = PostgresWriter(
        host=cfg.get("database.postgres.host", "localhost"),
        port=int(cfg.get("database.postgres.port", 5432)),
        database="quant",
        user=cfg.get("database.postgres.user", "postgres"),
        password=cfg.get("database.postgres.password", ""),
    )
    pg.connect()
    return pg


@router.get("/stocks")
async def search_stocks(
    q: str = Query("", min_length=0, max_length=32),
    limit: int = Query(40, ge=1, le=100),
) -> dict[str, Any]:
    """按代码或名称模糊搜索（PostgreSQL stock_info）。"""
    q = (q or "").strip()
    if len(q) < 1:
        return {"items": []}
    try:
        pg = _pg()
        pg._ensure_conn()
        cur = pg._conn.cursor()
        try:
            pattern = f"%{q}%"
            cur.execute(
                """
                SELECT ts_code, name, industry, market
                FROM stock_info
                WHERE (NOT is_delisted OR is_delisted IS NULL)
                  AND (ts_code ILIKE %s OR name ILIKE %s)
                ORDER BY ts_code
                LIMIT %s
                """,
                (pattern, pattern, limit),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
        items = [
            {"ts_code": r[0], "name": r[1] or "", "industry": r[2] or "", "market": r[3] or ""}
            for r in rows
        ]
        return {"items": items}
    except Exception as e:
        logger.exception("search_stocks failed")
        raise HTTPException(503, f"数据库不可用: {e}") from e


def _fetch_ohlcv_df(ts_code: str, start: str, end: str) -> pd.DataFrame:
    ch = _ch()
    ch._ensure_client()
    iso_start = _ymd_to_iso(_norm_ymd(start))
    iso_end = _ymd_to_iso(_norm_ymd(end))
    sql = """
        SELECT
            trade_date,
            open, high, low, close,
            adj_open, adj_high, adj_low, adj_close,
            volume, amount, pct_chg, turn
        FROM stock_daily FINAL
        WHERE ts_code = %(code)s
          AND trade_date >= toDate(%(start)s)
          AND trade_date <= toDate(%(end)s)
          AND is_suspended = 0
        ORDER BY trade_date
    """
    rows = ch._client.execute(sql, {"code": ts_code, "start": iso_start, "end": iso_end})
    if not rows:
        return pd.DataFrame()
    cols = [
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "volume",
        "amount",
        "pct_chg",
        "turn",
    ]
    df = pd.DataFrame(rows, columns=cols)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _bars_to_chart(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        d = r["trade_date"]
        if hasattr(d, "strftime"):
            t = d.strftime("%Y-%m-%d")
        else:
            t = str(d)[:10]
        out.append(
            {
                "time": t,
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
                "adj_close": float(r["adj_close"]),
            }
        )
    return out


def _run_backtest_series(
    df: pd.DataFrame,
    strategy: Literal["buy_hold", "ma_cross"],
    fast_ma: int,
    slow_ma: int,
) -> tuple[pd.Series, pd.Series, int]:
    """返回 (策略净值, 基准净值, 近似换仓次数)；仓位用前一日收盘信号，无未来函数。"""
    df = df.sort_values("trade_date").reset_index(drop=True)
    c = df["adj_close"].astype(float)
    ret = c.pct_change().fillna(0.0)

    if strategy == "buy_hold":
        pos = pd.Series(1.0, index=df.index)
    else:
        if fast_ma >= slow_ma or fast_ma < 2:
            raise HTTPException(400, "ma_cross 需要 fast_ma < slow_ma 且 fast_ma>=2")
        ma_f = c.rolling(fast_ma, min_periods=fast_ma).mean()
        ma_s = c.rolling(slow_ma, min_periods=slow_ma).mean()
        raw = (ma_f > ma_s).astype(float)
        pos = raw.shift(1).fillna(0.0)

    strat_ret = pos * ret
    eq_s = (1.0 + strat_ret).cumprod()
    eq_b = (1.0 + ret).cumprod()
    turns = int((pos.diff().abs() > 0).sum())
    return eq_s, eq_b, turns


def _equity_to_chart(df: pd.DataFrame, eq_s: pd.Series, eq_b: pd.Series) -> list[dict[str, Any]]:
    out = []
    for i in range(len(df)):
        d = df.iloc[i]["trade_date"]
        t = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        out.append(
            {
                "time": t,
                "strategy_equity": float(eq_s.iloc[i]),
                "benchmark_equity": float(eq_b.iloc[i]),
            }
        )
    return out


def _metrics(eq_s: pd.Series, eq_b: pd.Series, n_days: int) -> dict[str, float]:
    tr_s = float(eq_s.iloc[-1] - 1.0) if len(eq_s) else 0.0
    tr_b = float(eq_b.iloc[-1] - 1.0) if len(eq_b) else 0.0
    years = max(n_days / 252.0, 1e-9)
    ann_s = (1.0 + tr_s) ** (1.0 / years) - 1.0 if tr_s > -1 else -1.0
    ann_b = (1.0 + tr_b) ** (1.0 / years) - 1.0 if tr_b > -1 else -1.0
    dd_s = float((eq_s / eq_s.cummax() - 1.0).min()) if len(eq_s) else 0.0
    dd_b = float((eq_b / eq_b.cummax() - 1.0).min()) if len(eq_b) else 0.0
    return {
        "total_return_strategy": round(tr_s, 6),
        "total_return_benchmark": round(tr_b, 6),
        "ann_return_strategy": round(ann_s, 6),
        "ann_return_benchmark": round(ann_b, 6),
        "max_drawdown_strategy": round(dd_s, 6),
        "max_drawdown_benchmark": round(dd_b, 6),
        "trading_days": float(n_days),
    }


class SingleStockRunRequest(BaseModel):
    ts_code: str
    start: str = Field(..., description="YYYYMMDD")
    end: str = Field(..., description="YYYYMMDD")
    strategy: Literal["buy_hold", "ma_cross"] = "ma_cross"
    fast_ma: int = Field(5, ge=2, le=120)
    slow_ma: int = Field(20, ge=3, le=250)


@router.post("/single-stock-run")
async def single_stock_run(body: SingleStockRunRequest) -> dict[str, Any]:
    """拉取 K 线并跑一次简易回测，返回画 K 线 + 净值曲线的数据。"""
    ts = _validate_ts_code(body.ts_code)
    s = _norm_ymd(body.start)
    e = _norm_ymd(body.end)
    if s > e:
        raise HTTPException(400, "start 不能晚于 end")

    try:
        df = _fetch_ohlcv_df(ts, s, e)
    except Exception as ex:
        logger.exception("fetch ohlcv failed")
        raise HTTPException(503, f"ClickHouse 查询失败: {ex}") from ex

    if df.empty:
        raise HTTPException(404, "该区间无行情或股票代码不存在")

    eq_s, eq_b, turns = _run_backtest_series(df, body.strategy, body.fast_ma, body.slow_ma)
    metrics = _metrics(eq_s, eq_b, len(df))

    name = ""
    try:
        pg = _pg()
        pg._ensure_conn()
        cur = pg._conn.cursor()
        try:
            cur.execute("SELECT name FROM stock_info WHERE ts_code = %s", (ts,))
            r = cur.fetchone()
            if r:
                name = r[0] or ""
        finally:
            cur.close()
    except Exception:
        pass

    return {
        "ts_code": ts,
        "name": name,
        "start": s,
        "end": e,
        "strategy": body.strategy,
        "fast_ma": body.fast_ma,
        "slow_ma": body.slow_ma,
        "bars": _bars_to_chart(df),
        "equity": _equity_to_chart(df, eq_s, eq_b),
        "metrics": metrics,
        "approx_position_changes": turns,
        "as_of": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/bars")
async def get_bars(
    ts_code: str = Query(...),
    start: str = Query(..., description="YYYYMMDD"),
    end: str = Query(..., description="YYYYMMDD"),
) -> dict[str, Any]:
    """仅取 K 线（不做回测），供单独刷新图表。"""
    ts = _validate_ts_code(ts_code)
    s = _norm_ymd(start)
    e = _norm_ymd(end)
    if s > e:
        raise HTTPException(400, "start 不能晚于 end")
    try:
        df = _fetch_ohlcv_df(ts, s, e)
    except Exception as ex:
        raise HTTPException(503, f"ClickHouse 查询失败: {ex}") from ex
    if df.empty:
        raise HTTPException(404, "无数据")
    return {"ts_code": ts, "bars": _bars_to_chart(df)}


class RegimeModelRunRequest(BaseModel):
    ts_code: str
    start: str = Field(..., description="YYYYMMDD")
    end: str = Field(..., description="YYYYMMDD")


@router.post("/regime-model-run")
async def regime_model_run(body: RegimeModelRunRequest) -> dict[str, Any]:
    """
    运行与 ``strategy/examples/regime_switching_strategy.py`` 主脚本一致的 v4.1 管线（区间切片），
    返回组合净值、标的买入持有净值、该标的在组合中的日度权重（名义杠杆后）。
    首次请求可能较慢（全市场按年加载）。
    """
    from strategy.examples.regime_switching_strategy import run_regime_model_for_web

    ts = _validate_ts_code(body.ts_code)
    s = _norm_ymd(body.start)
    e = _norm_ymd(body.end)
    if s > e:
        raise HTTPException(400, "start 不能晚于 end")
    try:
        out = await asyncio.to_thread(run_regime_model_for_web, s, e, ts)
    except ValueError as ex:
        raise HTTPException(400, str(ex)) from ex
    except Exception as ex:
        logger.exception("regime model run failed")
        raise HTTPException(503, str(ex)) from ex

    name = ""
    try:
        pg = _pg()
        pg._ensure_conn()
        cur = pg._conn.cursor()
        try:
            cur.execute("SELECT name FROM stock_info WHERE ts_code = %s", (ts,))
            row = cur.fetchone()
            if row:
                name = row[0] or ""
        finally:
            cur.close()
    except Exception:
        pass

    out["name"] = name
    try:
        df_b = await asyncio.to_thread(_fetch_ohlcv_df, ts, s, e)
        out["bars"] = _bars_to_chart(df_b) if not df_b.empty else []
    except Exception:
        out["bars"] = []
    out["as_of"] = datetime.utcnow().isoformat() + "Z"
    return out
