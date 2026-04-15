"""
策略回测看板 API：收益曲线 vs 沪深300（可换指数）、绩效指标（含胜率/夏普等）。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Literal

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from strategy.backtest.metrics import calc_full_metrics

from ui.server.deps import require_api_key
from ui.server.research import (
    ResearchSyncError,
    _ch,
    _fetch_ohlcv_df,
    _norm_ymd,
    _run_backtest_series,
    _validate_ts_code,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"], dependencies=[Depends(require_api_key)])


def _fetch_index_close_series(ts_code: str, start: str, end: str) -> pd.DataFrame:
    """ClickHouse index_daily：trade_date + close，与股票区间对齐。"""
    ch = _ch()
    ch._ensure_client()
    s = _norm_ymd(start)
    e = _norm_ymd(end)
    iso_s = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    iso_e = f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    sql = """
        SELECT trade_date, close
        FROM index_daily FINAL
        WHERE ts_code = %(code)s
          AND trade_date >= toDate(%(start)s)
          AND trade_date <= toDate(%(end)s)
        ORDER BY trade_date
    """
    rows = ch._client.execute(sql, {"code": ts_code, "start": iso_s, "end": iso_e})
    if not rows:
        return pd.DataFrame(columns=["trade_date", "close"])
    df = pd.DataFrame(rows, columns=["trade_date", "close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close"] = df["close"].astype(float)
    return df


def _quick_backtest_sync(
    ts_code: str,
    start: str,
    end: str,
    strategy: Literal["buy_hold", "ma_cross"],
    fast_ma: int,
    slow_ma: int,
    benchmark_ts_code: str,
) -> dict[str, Any]:
    df = _fetch_ohlcv_df(ts_code, start, end)
    if df.empty:
        raise ResearchSyncError(404, "该区间无股票行情或代码不存在")

    eq_s, eq_bh_stock, turns = _run_backtest_series(df, strategy, fast_ma, slow_ma)

    idx = _fetch_index_close_series(benchmark_ts_code, start, end)
    if idx.empty:
        raise ResearchSyncError(404, f"基准指数无数据: {benchmark_ts_code}，请先回填 index_daily")

    m = df[["trade_date"]].merge(idx, on="trade_date", how="left")
    m["iclose"] = m["close"].ffill().bfill()
    if m["iclose"].isna().all():
        raise ResearchSyncError(404, "基准与股票交易日无重叠")
    ret_i = m["iclose"].pct_change().fillna(0.0)
    eq_bm = (1.0 + ret_i).cumprod()
    if len(eq_bm) and eq_bm.iloc[0] != 0:
        eq_bm = eq_bm / eq_bm.iloc[0]

    strat_ret = (eq_s / eq_s.shift(1) - 1.0).fillna(0.0)
    strat_ret.index = df["trade_date"]
    metrics = calc_full_metrics(strat_ret)
    if "error" in metrics:
        raise ResearchSyncError(400, metrics.get("error", "insufficient data"))
    if "max_drawdown_duration_days" in metrics:
        metrics["max_drawdown_duration_days"] = int(metrics["max_drawdown_duration_days"])

    tr_bm = float(eq_bm.iloc[-1] - 1.0) if len(eq_bm) else 0.0
    dd_bm = float((eq_bm / eq_bm.cummax() - 1.0).min()) if len(eq_bm) else 0.0

    equity = []
    for i in range(len(df)):
        d = df.iloc[i]["trade_date"]
        t = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        equity.append(
            {
                "time": t,
                "strategy_equity": float(eq_s.iloc[i]),
                "benchmark_equity": float(eq_bm.iloc[i]),
                "stock_buyhold_equity": float(eq_bh_stock.iloc[i]),
            }
        )

    return {
        "ts_code": ts_code,
        "benchmark_ts_code": benchmark_ts_code,
        "start": _norm_ymd(start),
        "end": _norm_ymd(end),
        "strategy": strategy,
        "fast_ma": fast_ma,
        "slow_ma": slow_ma,
        "approx_position_changes": turns,
        "equity": equity,
        "metrics_strategy": metrics,
        "metrics_benchmark": {
            "total_return": round(tr_bm, 6),
            "max_drawdown": round(dd_bm, 6),
        },
    }


class QuickBacktestRequest(BaseModel):
    ts_code: str
    start: str = Field(..., description="YYYYMMDD")
    end: str = Field(..., description="YYYYMMDD")
    strategy: Literal["buy_hold", "ma_cross"] = "ma_cross"
    fast_ma: int = Field(5, ge=2, le=120)
    slow_ma: int = Field(20, ge=3, le=250)
    benchmark_ts_code: str = Field("000300.SH", description="默认沪深300")


@router.post("/quick-backtest")
async def quick_backtest(body: QuickBacktestRequest) -> dict[str, Any]:
    """简易双均线/买入持有 vs 指数基准；绩效用 strategy.backtest.metrics。"""
    ts = _validate_ts_code(body.ts_code)
    s = _norm_ymd(body.start)
    e = _norm_ymd(body.end)
    if s > e:
        raise HTTPException(400, "start 不能晚于 end")
    bm = body.benchmark_ts_code.strip().upper() or "000300.SH"
    try:
        return await asyncio.shield(
            asyncio.to_thread(
                _quick_backtest_sync,
                ts,
                s,
                e,
                body.strategy,
                body.fast_ma,
                body.slow_ma,
                bm,
            )
        )
    except ResearchSyncError as ex:
        raise HTTPException(ex.status_code, ex.detail) from ex
