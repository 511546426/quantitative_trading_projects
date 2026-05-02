"""
A 股多因子策略 — Web API 封装

提供 FastAPI 路由调用的回测入口，与 CLI ``main()`` 共用同一套 v4.1 管线。
每次运行将日志追加写入 ``logs/research_regime.log``。
"""
from __future__ import annotations

import gc
import logging
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from strategy.config import (
    START, END, BENCHMARK, TOP_N, REBAL_FREQ, INERTIA, LEVERAGE, REGIME_LEV_MULT,
    DEFAULT_COST_SENSITIVITY_SCENARIOS,
)
from strategy.data_loader import (
    connect_db, load_price, load_valuation, load_exclude_list, load_index_close,
)
from strategy.signal import build_universe, calc_signal, regime_bull_exante
from strategy.portfolio import (
    generate_weights, calc_portfolio_return, apply_portfolio_stop,
    simulate_cash_account_backtest,
    yearly_returns_table,
)
from strategy.backtest.metrics import calc_full_metrics

logger = logging.getLogger(__name__)

LOOKBACK_CALENDAR_DAYS = 420


def _lookback_start(date_start: str) -> str:
    """将用户请求的起始日期前推约 300 个交易日（420 自然日），
    确保 MA60 / MOM120 / 52 周高点等因子有足够回看数据。"""
    dt = datetime.strptime(date_start, "%Y%m%d")
    lb_dt = dt - timedelta(days=LOOKBACK_CALENDAR_DAYS)
    return lb_dt.strftime("%Y%m%d")


def _report_start_ts(date_start: str) -> pd.Timestamp:
    return pd.Timestamp(f"{date_start[:4]}-{date_start[4:6]}-{date_start[6:8]}")


def _load_regime_web_panel(
    ch,
    pg,
    date_start: str,
    date_end: str,
    ts_code: str | None,
    *,
    load_start: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series | None, str, bool]:
    """加载行情/池/估值并计算 signal。

    Parameters
    ----------
    load_start : str | None
        实际数据加载起始日（含 lookback 缓冲）。为 None 时等于 date_start。
    """
    actual_load_start = load_start or date_start
    ts_key = (ts_code or "").strip().upper()
    pool_only = ts_key == ""
    close, amount = load_price(ch, actual_load_start, date_end)
    exclude = load_exclude_list(pg)
    try:
        pb, pe_ttm, circ_mv = load_valuation(pg, actual_load_start, date_end)
    except Exception as e:
        logger.warning("估值加载失败: %s", e)
        pb, pe_ttm, circ_mv = None, None, None

    universe = build_universe(close, amount, exclude, circ_mv=circ_mv)
    del amount
    gc.collect()

    index_close = load_index_close(ch, BENCHMARK, actual_load_start, date_end)

    active = universe.any(axis=0)
    if active.sum() < universe.shape[1]:
        nb = universe.shape[1]
        close = close.loc[:, active]
        universe = universe.loc[:, active]
        if pb is not None:
            pb = pb.reindex(columns=close.columns)
        if pe_ttm is not None:
            pe_ttm = pe_ttm.reindex(columns=close.columns)
        if circ_mv is not None:
            circ_mv = circ_mv.reindex(columns=close.columns)
        logger.info("裁剪: %d → %d 只", nb, int(active.sum()))
    del active
    gc.collect()

    if not pool_only and ts_key not in close.columns:
        raise ValueError(f"标的 {ts_key} 不在模型可交易列（可能无行情或被池过滤）")

    bull = regime_bull_exante(index_close, close.index) if index_close is not None else None
    signal = calc_signal(
        close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull
    )
    del pb, pe_ttm, circ_mv, universe
    gc.collect()
    return close, signal, index_close, ts_key, pool_only


def run_regime_cost_sensitivity_for_web(
    date_start: str,
    date_end: str,
    ts_code: str | None,
    initial_capital: float,
    scenarios: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """整手现金路径，单次加载全市场数据，多组佣金/滑点重放。"""
    ic = float(initial_capital)
    if ic <= 0:
        raise ValueError("initial_capital 须为正数以运行现金成本敏感度")
    rows = scenarios if scenarios is not None else DEFAULT_COST_SENSITIVITY_SCENARIOS
    cfg = __import__("data.common.config", fromlist=["Config"]).Config.load(
        "data/config/settings.yaml", "data/config/sources.yaml"
    )
    lb_start = _lookback_start(date_start)
    rpt_ts = _report_start_ts(date_start)

    ch, pg = connect_db(cfg)
    try:
        close, signal, index_close, ts_key, pool_only = _load_regime_web_panel(
            ch, pg, date_start, date_end, ts_code, load_start=lb_start,
        )
        close_sim = close.loc[rpt_ts:]
        out_rows: list[dict[str, Any]] = []
        for sc in rows:
            label = str(sc.get("label", ""))
            bb = float(sc["buy_bps"])
            sb = float(sc["sell_bps"])
            slpb = float(sc["slip_buy_bps"])
            slps = float(sc["slip_sell_bps"])
            net_ret, turnover, _w = simulate_cash_account_backtest(
                close_sim, signal, ic,
                top_n=TOP_N, rebal_freq=REBAL_FREQ, inertia=INERTIA,
                buy_cost_bps=bb, sell_cost_bps=sb,
                slip_buy_bps=slpb, slip_sell_bps=slps,
            )
            net_ret = apply_portfolio_stop(net_ret, index_close=index_close)
            m = calc_full_metrics(net_ret, turnover)
            out_rows.append({
                "label": label,
                "buy_bps": bb,
                "sell_bps": sb,
                "slip_buy_bps": slpb,
                "slip_sell_bps": slps,
                "annualized_return": round(float(m.get("annualized_return", 0.0) or 0.0), 6),
                "total_return": round(float(m.get("total_return", 0.0) or 0.0), 6),
                "max_drawdown": round(float(m.get("max_drawdown", 0.0) or 0.0), 6),
                "sharpe_ratio": round(float(m.get("sharpe_ratio", 0.0) or 0.0), 6),
                "annualized_turnover": round(float(m.get("annualized_turnover", 0.0) or 0.0), 6),
                "n_trading_days": int(m.get("n_trading_days", 0) or 0),
            })
        return {
            "model": "regime_switching_v4.1",
            "backtest_mode": "cash_lots_cost_sweep",
            "date_start": date_start,
            "date_end": date_end,
            "ts_code": None if pool_only else ts_key,
            "run_scope": "pool" if pool_only else "stock",
            "initial_capital": ic,
            "scenarios": out_rows,
        }
    finally:
        ch.close()
        pg.close()


def run_regime_model_for_web(
    date_start: str,
    date_end: str,
    ts_code: str | None,
    initial_capital: float | None = None,
) -> dict[str, Any]:
    """与 main() 相同的因子、TOP_N、成本与组合止损逻辑。

    数据加载自动前推约 300 个交易日作为因子回看缓冲，
    最终绩效 / 净值序列 / 分年收益仅覆盖用户请求的 ``[date_start, date_end]`` 区间。
    """
    lb_start = _lookback_start(date_start)
    rpt_ts = _report_start_ts(date_start)

    cfg = __import__("data.common.config", fromlist=["Config"]).Config.load(
        "data/config/settings.yaml", "data/config/sources.yaml"
    )
    ch, pg = connect_db(cfg)
    try:
        close, signal, index_close, ts_key, pool_only = _load_regime_web_panel(
            ch, pg, date_start, date_end, ts_code, load_start=lb_start,
        )

        ic_use = float(initial_capital) if initial_capital is not None and float(initial_capital) > 0 else 0.0
        use_cash_lots = ic_use > 0

        if use_cash_lots:
            close_sim = close.loc[rpt_ts:]
            net_ret, turnover, weights = simulate_cash_account_backtest(
                close_sim, signal, ic_use,
                top_n=TOP_N, rebal_freq=REBAL_FREQ, inertia=INERTIA,
            )
        else:
            weights = generate_weights(signal)
            if float(LEVERAGE) != 1.0:
                lev_ser = pd.Series(float(LEVERAGE), index=weights.index)
                if index_close is not None:
                    bflt = regime_bull_exante(index_close, weights.index).astype(np.float64)
                    lev_ser = lev_ser * (1.0 + bflt * (float(REGIME_LEV_MULT) - 1.0))
                weights = weights.multiply(lev_ser, axis=0)
            net_ret, turnover = calc_portfolio_return(weights, close)

        del signal
        gc.collect()

        net_ret = apply_portfolio_stop(net_ret, index_close=index_close)

        net_ret = net_ret.loc[rpt_ts:]
        turnover = turnover.reindex(net_ret.index).fillna(0.0)

        metrics = calc_full_metrics(net_ret, turnover)

        def _json_metrics(m: dict) -> dict[str, Any]:
            out: dict[str, Any] = {}
            for k, v in m.items():
                if isinstance(v, (np.floating, float)):
                    out[k] = float(v)
                elif isinstance(v, (np.integer, int)):
                    out[k] = int(v)
                elif isinstance(v, str):
                    out[k] = v
                elif v is None:
                    out[k] = None
                elif hasattr(v, "isoformat"):
                    out[k] = v.isoformat()
                elif hasattr(v, "item"):
                    out[k] = float(v.item())
                else:
                    out[k] = str(v)
            return out

        port_eq = (1 + net_ret).cumprod()
        if pool_only:
            wcol = pd.Series(0.0, index=net_ret.index)
            if index_close is not None and len(index_close) > 0:
                ic = index_close.reindex(net_ret.index).ffill()
                irt = ic.pct_change(fill_method=None).fillna(0.0)
                bench_eq = (1 + irt).cumprod()
                if len(bench_eq) and float(bench_eq.iloc[0]) != 0:
                    bench_eq = bench_eq / float(bench_eq.iloc[0])
            else:
                bench_eq = pd.Series(1.0, index=net_ret.index)
        else:
            wcol = weights[ts_key].reindex(net_ret.index).fillna(0.0)
            sc = close[ts_key].reindex(net_ret.index).ffill()
            st_ret = sc.pct_change(fill_method=None).fillna(0.0)
            bench_eq = (1 + st_ret).cumprod()

        series: list[dict[str, Any]] = []
        for t in net_ret.index:
            series.append({
                "time": t.strftime("%Y-%m-%d") if hasattr(t, "strftime") else str(t)[:10],
                "portfolio_equity": float(port_eq.loc[t]),
                "stock_benchmark_equity": float(bench_eq.loc[t]),
                "model_weight": float(wcol.loc[t]),
            })

        return {
            "model": "regime_switching_v4.1",
            "run_scope": "pool" if pool_only else "stock",
            "ts_code": None if pool_only else ts_key,
            "benchmark_label": "CSI300买入持有" if pool_only else "标的买入持有",
            "date_start": date_start,
            "date_end": date_end,
            "backtest_mode": "cash_lots" if use_cash_lots else "fractional",
            "metrics_portfolio": _json_metrics(metrics),
            "yearly_returns": yearly_returns_table(
                net_ret, turnover,
            ),
            "series": series,
        }
    finally:
        ch.close()
        pg.close()
