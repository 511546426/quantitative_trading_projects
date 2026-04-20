"""
单股 K 线 API（ClickHouse stock_daily + PostgreSQL stock_info）；
多因子回测仅保留 ``regime_switching_strategy.run_regime_model_for_web`` v4.1 管线，
运行期将 ``multifactor_v4`` 日志追加至 ``logs/research_regime.log``（供 WebSocket 日志流订阅）。
双均线 + 指数对标见 ``/api/dashboard/quick-backtest``。
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from functools import lru_cache
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from data.common.config import Config
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter
from ui.server.config import LOG_PATHS
from ui.server.deps import require_api_key

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/research", tags=["research"], dependencies=[Depends(require_api_key)])


class ResearchSyncError(Exception):
    """Raised from ``asyncio.to_thread`` workers; mapped to HTTPException in routes."""

    __slots__ = ("status_code", "detail")

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)

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


def _rebase_equity_series_to_capital(
    series: list[dict[str, Any]], initial_capital: float
) -> list[dict[str, Any]]:
    """
    将归一化累计净值曲线锚定为「区间首日投入 initial_capital 元」：
    组合与买入持有均在首日对齐为同一本金，便于同图对比。
    """
    if not series or initial_capital <= 0:
        return series
    p0 = float(series[0]["portfolio_equity"])
    b0 = float(series[0]["stock_benchmark_equity"])
    if p0 == 0:
        return series
    if b0 == 0:
        b0 = 1.0
    out: list[dict[str, Any]] = []
    for row in series:
        out.append(
            {
                **row,
                "portfolio_equity": initial_capital * float(row["portfolio_equity"]) / p0,
                "stock_benchmark_equity": initial_capital * float(row["stock_benchmark_equity"]) / b0,
            }
        )
    return out


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
    ts_code: str | None = Field(
        None,
        description="可选。留空则仅全市场多因子组合（无单票 K 线）；灰线为 CSI300 买入持有。",
    )
    start: str = Field(..., description="YYYYMMDD")
    end: str = Field(..., description="YYYYMMDD")
    initial_capital: float | None = Field(
        None,
        description="可选。若给出正数，则将返回的 portfolio/stock 净值序列锚定为区间首日该本金（元）；绩效指标仍为收益率口径不变。",
        ge=1_000,
        le=1e12,
    )

    @field_validator("ts_code", mode="before")
    @classmethod
    def _empty_ts_none(cls, v: object) -> str | None:
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v


def _run_regime_model_for_web_with_file_log(
    date_start: str, date_end: str, ts_code: str | None
) -> dict[str, Any]:
    """
    在线程内执行 v4.1，并把 ``multifactor_v4`` 日志追加写入 ``logs/research_regime.log``，
    供 WebSocket 日志流订阅（与回填日志同一机制）。
    """
    from strategy.examples.regime_switching_strategy import run_regime_model_for_web

    log_path = LOG_PATHS["research-regime"]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    strat_log = logging.getLogger("multifactor_v4")
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    ts_disp = ts_code or "(pool-only)"
    strat_log.addHandler(fh)
    try:
        strat_log.info(
            "======== regime-model-run begin ts=%s %s–%s ========",
            ts_disp,
            date_start,
            date_end,
        )
        out = run_regime_model_for_web(date_start, date_end, ts_code)
        strat_log.info("======== regime-model-run end (ok) ========")
        return out
    except Exception:
        strat_log.exception("======== regime-model-run end (error) ========")
        raise
    finally:
        strat_log.removeHandler(fh)
        fh.close()


@router.post("/regime-model-run")
async def regime_model_run(body: RegimeModelRunRequest) -> dict[str, Any]:
    """
    运行与 ``strategy/examples/regime_switching_strategy.py`` 主脚本一致的 v4.1 管线（区间切片）。
    有 ``ts_code`` 时返回组合净值、该标的买入持有、权重与 K 线；无 ``ts_code`` 时仅组合净值（灰线为 CSI300 买入持有），无 K 线。
    首次请求可能较慢（全市场按年加载）；进度写入 ``logs/research_regime.log`` 可在「日志流」中查看。
    """
    raw_ts = body.ts_code
    ts_norm: str | None = _validate_ts_code(raw_ts) if raw_ts else None
    s = _norm_ymd(body.start)
    e = _norm_ymd(body.end)
    if s > e:
        raise HTTPException(400, "start 不能晚于 end")
    # Shield: client disconnect / navigation must not cancel the worker thread mid-run.
    try:
        out = await asyncio.shield(asyncio.to_thread(_run_regime_model_for_web_with_file_log, s, e, ts_norm))
    except ValueError as ex:
        raise HTTPException(400, str(ex)) from ex
    except Exception as ex:
        logger.exception("regime model run failed")
        raise HTTPException(503, str(ex)) from ex

    name = ""
    if ts_norm:
        try:
            pg = _pg()
            pg._ensure_conn()
            cur = pg._conn.cursor()
            try:
                cur.execute("SELECT name FROM stock_info WHERE ts_code = %s", (ts_norm,))
                row = cur.fetchone()
                if row:
                    name = row[0] or ""
            finally:
                cur.close()
        except Exception:
            pass

    out["name"] = name
    cap = body.initial_capital
    if cap is not None and cap > 0 and out.get("series"):
        out["series"] = _rebase_equity_series_to_capital(out["series"], float(cap))
        out["initial_capital"] = float(cap)
    if ts_norm:
        try:
            df_b = await asyncio.shield(asyncio.to_thread(_fetch_ohlcv_df, ts_norm, s, e))
            out["bars"] = _bars_to_chart(df_b) if not df_b.empty else []
        except Exception:
            out["bars"] = []
    else:
        out["bars"] = []
    out["as_of"] = datetime.utcnow().isoformat() + "Z"
    return out
