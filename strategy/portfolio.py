"""
A 股多因子策略 — 组合构建与止损模块

包含：权重生成、组合收益、整手现金仿真、净值回撤止损。
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from strategy.config import (
    TOP_N, REBAL_FREQ, INERTIA, LEVERAGE, REGIME_LEV_MULT,
    STOP_LOSS, STOP_LOSS_BULL, STOP_COOLDOWN, STOP_MIN_CASH_DAYS,
    BUY_COST_BPS, SELL_COST_BPS,
    A_SHARE_LOT, CASH_SLIP_BUY_BPS, CASH_SLIP_SELL_BPS,
    CASH_LIMIT_UP_FRAC, CASH_LIMIT_DOWN_FRAC,
    TRADING_DAYS_PER_YEAR,
)
from strategy.signal import regime_bull_exante
from strategy.backtest.metrics import calc_full_metrics, TRADING_DAYS_PER_YEAR as METRICS_YEAR

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 组合层止损
# ═══════════════════════════════════════════════════════════

def apply_portfolio_stop(
    net_ret: pd.Series,
    index_close: pd.Series | None = None,
    stop_loss: float = STOP_LOSS,
    stop_loss_bull: float = STOP_LOSS_BULL,
    min_cash_days: int = STOP_MIN_CASH_DAYS,
    max_cash_days: int = STOP_COOLDOWN,
) -> pd.Series:
    """
    组合层止损保护：回撤超 stop_loss → 清仓。
    重新入场条件（满足任一即可）:
      - 已清仓 max_cash_days 天（强制重入）
      - 已清仓至少 min_cash_days 天 且 CSI300 同时 > 20d MA AND > 60d MA
    """
    if index_close is not None:
        ic = index_close.reindex(net_ret.index).ffill()
        p_ic = ic.shift(1)
        ma20_r = ic.rolling(20, min_periods=10).mean().shift(1)
        ma60_r = ic.rolling(60, min_periods=30).mean().shift(1)
        ok = ((p_ic > ma20_r) & (p_ic > ma60_r)).fillna(True)
    else:
        ok = pd.Series(True, index=net_ret.index)

    bull = regime_bull_exante(index_close, net_ret.index) if index_close is not None else None
    stop_bull = float(stop_loss_bull)

    adjusted = net_ret.copy()
    nav = 1.0
    peak = 1.0
    in_cash = False
    days_in_cash = 0

    for i in range(len(net_ret)):
        sl = stop_bull if bull is not None and bool(bull.iloc[i]) else float(stop_loss)
        if in_cash:
            adjusted.iloc[i] = 0.0
            days_in_cash += 1
            force_reenter = days_in_cash >= max_cash_days
            market_reenter = (days_in_cash >= min_cash_days) and bool(ok.iloc[i])
            if force_reenter or market_reenter:
                in_cash = False
                days_in_cash = 0
                peak = nav
        else:
            nav = nav * (1.0 + adjusted.iloc[i])
            if nav > peak:
                peak = nav
            if nav / peak - 1 < -sl:
                in_cash = True
                adjusted.iloc[i] = 0.0
                days_in_cash = 1

    total_cash_days = int((adjusted == 0.0).sum())
    bull_pct = float(bull.mean() * 100) if bull is not None else 0.0
    logger.info(
        "止损保护: 清仓 %d 天 (占 %.1f%%)，止损=非牛%.0f%%/牛%.0f%%，"
        "最短等待=%d天，最长=%d天，重入=ma20+ma60，趋势牛日=%.1f%%",
        total_cash_days,
        total_cash_days / len(adjusted) * 100,
        stop_loss * 100,
        stop_bull * 100,
        min_cash_days,
        max_cash_days,
        bull_pct,
    )
    return adjusted


def portfolio_stop_invested_start(
    net_ret: pd.Series,
    index_close: pd.Series | None = None,
    stop_loss: float = STOP_LOSS,
    stop_loss_bull: float = STOP_LOSS_BULL,
    min_cash_days: int = STOP_MIN_CASH_DAYS,
    max_cash_days: int = STOP_COOLDOWN,
) -> pd.Series:
    """
    与 apply_portfolio_stop 同一状态机：第 i 日开盘时是否应持有风险仓位。
    供外部回测对齐模型止损/空仓日历。
    """
    if index_close is not None:
        ic = index_close.reindex(net_ret.index).ffill()
        p_ic = ic.shift(1)
        ma20_r = ic.rolling(20, min_periods=10).mean().shift(1)
        ma60_r = ic.rolling(60, min_periods=30).mean().shift(1)
        ok = ((p_ic > ma20_r) & (p_ic > ma60_r)).fillna(True)
    else:
        ok = pd.Series(True, index=net_ret.index)

    bull = regime_bull_exante(index_close, net_ret.index) if index_close is not None else None
    stop_bull = float(stop_loss_bull)

    invested_start: list[bool] = []
    nav = 1.0
    peak = 1.0
    in_cash = False
    days_in_cash = 0

    for i in range(len(net_ret)):
        sl = stop_bull if bull is not None and bool(bull.iloc[i]) else float(stop_loss)
        invested_start.append(not in_cash)
        if in_cash:
            days_in_cash += 1
            force_reenter = days_in_cash >= max_cash_days
            market_reenter = (days_in_cash >= min_cash_days) and bool(ok.iloc[i])
            if force_reenter or market_reenter:
                in_cash = False
                days_in_cash = 0
                peak = nav
        else:
            nav = nav * (1.0 + float(net_ret.iloc[i]))
            if nav > peak:
                peak = nav
            if nav / peak - 1 < -sl:
                in_cash = True
                days_in_cash = 1

    return pd.Series(invested_start, index=net_ret.index, dtype=bool)


# ═══════════════════════════════════════════════════════════
# 持仓权重
# ═══════════════════════════════════════════════════════════

def generate_weights(
    signal: pd.DataFrame,
    top_n: int = TOP_N,
    rebal_freq: int = REBAL_FREQ,
    inertia: float = INERTIA,
) -> pd.DataFrame:
    weights = pd.DataFrame(np.nan, index=signal.index, columns=signal.columns)
    prev_held: set = set()

    for i, dt in enumerate(signal.index):
        if i % rebal_freq != 0:
            continue
        s = signal.loc[dt].dropna().copy()
        if len(s) < top_n:
            continue
        for c in prev_held:
            if c in s.index:
                s[c] += inertia
        top = s.nlargest(top_n)
        weights.loc[dt, :] = 0.0
        weights.loc[dt, top.index] = 1.0 / len(top)
        prev_held = set(top.index)

    weights = weights.ffill().fillna(0)
    return weights


def _pick_top_for_rebalance(
    signal: pd.DataFrame,
    dt: pd.Timestamp,
    prev_held: set[str],
    *,
    top_n: int,
    inertia: float,
) -> tuple[list[str], set[str]] | None:
    """与 ``generate_weights`` 调仓日选股逻辑一致（惯性加分）。"""
    if dt not in signal.index:
        return None
    s = signal.loc[dt].dropna().copy()
    if len(s) < top_n:
        return None
    for c in prev_held:
        if c in s.index:
            s[c] += inertia
    top = s.nlargest(top_n)
    return list(top.index), set(top.index)


# ═══════════════════════════════════════════════════════════
# 组合收益
# ═══════════════════════════════════════════════════════════

def calc_portfolio_return(
    weights: pd.DataFrame,
    close: pd.DataFrame,
    *,
    buy_cost_bps: float | None = None,
    sell_cost_bps: float | None = None,
) -> tuple[pd.Series, pd.Series]:
    """净收益 = port_ret - cost。"""
    bcb = float(BUY_COST_BPS if buy_cost_bps is None else buy_cost_bps)
    scb = float(SELL_COST_BPS if sell_cost_bps is None else sell_cost_bps)
    daily_ret = close.pct_change(fill_method=None).fillna(0).clip(-0.2, 0.2)
    port_ret = (weights.shift(1) * daily_ret).sum(axis=1)

    w_diff = weights.diff().fillna(0)
    buy_t = w_diff.clip(lower=0).sum(axis=1)
    sell_t = (-w_diff.clip(upper=0)).sum(axis=1)
    cost = buy_t * bcb / 1e4 + sell_t * scb / 1e4
    turnover = w_diff.abs().sum(axis=1)

    net_ret = port_ret - cost
    return net_ret.dropna(), turnover


# ═══════════════════════════════════════════════════════════
# 整手现金仿真
# ═══════════════════════════════════════════════════════════

def _mtm_positions_cash(pos: dict[str, int], price_row: pd.Series) -> float:
    v = 0.0
    for c, sh in pos.items():
        if sh <= 0:
            continue
        px = float(price_row.get(c, np.nan))
        if np.isfinite(px) and px > 0:
            v += float(sh) * px
    return v


def _round_robin_buy_lots(
    cash_budget: float,
    symbols: list[str],
    price_row: pd.Series,
    *,
    lot: int = A_SHARE_LOT,
    prev_row: pd.Series | None = None,
    slip_buy_bps: float = 0.0,
    limit_up_frac: float = 10.0,
    buy_cost_bps: float | None = None,
) -> tuple[dict[str, int], float, float]:
    """
    在 cash_budget 内对 symbols 轮询每次各加一手，直至现金不足以再买任一票的一手。
    返回 (持仓股数, 买入总名义本金按昨收口径, 实际花费现金含费与滑点)。
    """
    prices: dict[str, float] = {}
    for s in symbols:
        if s not in price_row.index:
            continue
        p = float(price_row[s])
        if not (np.isfinite(p) and p > 0):
            continue
        if prev_row is not None and limit_up_frac < 2.0 and s in prev_row.index:
            p0 = float(prev_row[s])
            if np.isfinite(p0) and p0 > 0 and (p / p0 - 1.0) >= limit_up_frac:
                continue
        prices[s] = p
    if not prices or cash_budget <= 0:
        return {}, 0.0, 0.0
    bcb = float(buy_cost_bps if buy_cost_bps is not None else BUY_COST_BPS)
    syms = [s for s in symbols if s in prices]
    pos: dict[str, int] = {s: 0 for s in syms}
    rem = float(cash_budget)
    buy_gross = 0.0
    slipf = 1.0 + float(slip_buy_bps) / 1e4
    while rem > 0:
        progressed = False
        for s in syms:
            p = prices[s]
            exec_px = p * slipf
            gross = lot * exec_px
            fee = gross * bcb / 1e4
            need = gross + fee
            if rem + 1e-9 >= need:
                pos[s] += lot
                rem -= need
                buy_gross += lot * p
                progressed = True
        if not progressed:
            break
    out = {s: q for s, q in pos.items() if q > 0}
    spent_total = cash_budget - rem
    return out, buy_gross, spent_total


def _liquidate_all_limits(
    pos: dict[str, int],
    price_row: pd.Series,
    prev_row: pd.Series | None,
    *,
    slip_sell_bps: float,
    limit_down_frac: float,
    sell_cost_bps: float | None = None,
) -> tuple[float, float, dict[str, int]]:
    """
    尽量卖出全部持仓；跌停时该股顺延至后续交易日再卖。
    返回 (卖出名义本金合计, 入 T+1 待交割现金净额, 剩余持仓)。
    """
    scb = float(sell_cost_bps if sell_cost_bps is not None else SELL_COST_BPS)
    slipf = 1.0 - float(slip_sell_bps) / 1e4
    new_pos = dict(pos)
    sell_gross = 0.0
    net_to_pending = 0.0
    for c in list(new_pos.keys()):
        sh = int(new_pos.get(c, 0))
        if sh <= 0:
            continue
        px = float(price_row.get(c, np.nan))
        if not (np.isfinite(px) and px > 0):
            continue
        if prev_row is not None and limit_down_frac > -2.0 and c in prev_row.index:
            p0 = float(prev_row[c])
            if np.isfinite(p0) and p0 > 0:
                ret = px / p0 - 1.0
                if ret <= limit_down_frac:
                    continue
        exec_px = px * slipf
        gross_exec = float(sh) * exec_px
        fee = gross_exec * scb / 1e4
        net_to_pending += gross_exec - fee
        sell_gross += float(sh) * px
        del new_pos[c]
    return sell_gross, net_to_pending, new_pos


def simulate_cash_account_backtest(
    close: pd.DataFrame,
    signal: pd.DataFrame,
    initial_cash: float,
    *,
    top_n: int = TOP_N,
    rebal_freq: int = REBAL_FREQ,
    inertia: float = INERTIA,
    buy_cost_bps: float | None = None,
    sell_cost_bps: float | None = None,
    slip_buy_bps: float | None = None,
    slip_sell_bps: float | None = None,
) -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    """
    现金账户、多头整手、**无融资**；包含 T+1 交割、调仓拆日、涨跌停简化、滑点/佣金。
    未模拟：分红送转、配股、T+0 回转、集合竞价、逐笔队列。
    """
    sbuy = float(CASH_SLIP_BUY_BPS if slip_buy_bps is None else slip_buy_bps)
    ssell = float(CASH_SLIP_SELL_BPS if slip_sell_bps is None else slip_sell_bps)
    bcb = float(buy_cost_bps if buy_cost_bps is not None else BUY_COST_BPS)
    scb = float(sell_cost_bps if sell_cost_bps is not None else SELL_COST_BPS)
    dates = close.index
    cash = float(initial_cash)
    pending_settlement = 0.0
    pos: dict[str, int] = {}
    prev_held: set[str] = set()
    deferred_syms: list[str] | None = None

    equity_vals: list[float] = []
    turn_vals: list[float] = []
    w_rows: list[pd.Series] = []

    def _equity_now(r: pd.Series, pend: float) -> float:
        return float(cash + _mtm_positions_cash(pos, r) + pend)

    for i, dt in enumerate(dates):
        row = close.loc[dt]
        prev_row = close.iloc[i - 1] if i > 0 else None

        cash += float(pending_settlement)
        pending_settlement = 0.0

        turn_t = 0.0

        # 执行上一调仓日决定的买入
        if deferred_syms is not None:
            syms = deferred_syms
            deferred_syms = None
            eq_before_buy = _equity_now(row, pending_settlement)
            new_pos, bg, spent = _round_robin_buy_lots(
                cash,
                syms,
                row,
                prev_row=prev_row,
                slip_buy_bps=sbuy,
                limit_up_frac=CASH_LIMIT_UP_FRAC,
                buy_cost_bps=bcb,
            )
            cash -= spent
            for s, q in new_pos.items():
                if q <= 0:
                    continue
                pos[s] = int(pos.get(s, 0)) + int(q)
            if eq_before_buy > 1e-9 and bg > 0:
                turn_t += bg / eq_before_buy

        is_rebal = i % rebal_freq == 0
        if is_rebal:
            eq_before_sell = _equity_now(row, pending_settlement)
            sg, net_pend, pos = _liquidate_all_limits(
                pos,
                row,
                prev_row,
                slip_sell_bps=ssell,
                limit_down_frac=CASH_LIMIT_DOWN_FRAC,
                sell_cost_bps=scb,
            )
            pending_settlement += net_pend
            if eq_before_sell > 1e-9 and sg > 0:
                turn_t += sg / eq_before_sell

            picked = _pick_top_for_rebalance(
                signal, dt, prev_held, top_n=top_n, inertia=inertia
            )
            if picked is not None:
                syms, prev_held = picked
                deferred_syms = list(syms)
            else:
                prev_held = set()
                deferred_syms = None

        turn_vals.append(turn_t)

        equity = _equity_now(row, pending_settlement)
        equity_vals.append(equity)

        if equity > 1e-12:
            wser = pd.Series(0.0, index=close.columns, dtype=np.float64)
            for c, sh in pos.items():
                if c in wser.index and sh > 0:
                    px = float(row.get(c, np.nan))
                    if np.isfinite(px) and px > 0:
                        wser.loc[c] = float(sh) * px / equity
            w_rows.append(wser)
        else:
            w_rows.append(pd.Series(0.0, index=close.columns, dtype=np.float64))

    equity_s = pd.Series(equity_vals, index=dates, dtype=np.float64)
    raw_ret = equity_s.pct_change(fill_method=None)
    raw_ret = raw_ret.fillna(0.0)
    raw_ret.iloc[0] = 0.0

    turnover = pd.Series(turn_vals, index=dates, dtype=np.float64)
    weights_mv = pd.DataFrame(w_rows, index=dates, columns=close.columns).fillna(0.0)
    logger.info(
        "现金整手(增强): 初始=%.0f 末权益=%.0f 调仓=%d日 买/卖佣%.1f/%.1fbps 滑点买%.1f/卖%.1f",
        float(initial_cash),
        float(equity_s.iloc[-1]) if len(equity_s) else 0.0,
        int(rebal_freq),
        bcb,
        scb,
        sbuy,
        ssell,
    )
    return raw_ret, turnover, weights_mv


# ═══════════════════════════════════════════════════════════
# 完整管线
# ═══════════════════════════════════════════════════════════

def lot_effective_top_n(
    initial_cash_yuan: float,
    *,
    max_names: int = TOP_N,
    min_names: int = 5,
    min_lot_assumed_yuan: float = 4200.0,
) -> int:
    """整手回测建议持仓只数。"""
    lev_peak = float(LEVERAGE) * float(REGIME_LEV_MULT)
    budget = max(float(initial_cash_yuan), 1.0) * lev_peak
    n = int(budget / float(min_lot_assumed_yuan))
    return int(max(min_names, min(int(max_names), n)))


def weights_from_trading_panel(
    close: pd.DataFrame,
    amount: pd.DataFrame,
    exclude: set,
    *,
    index_close: pd.Series | None = None,
    pb: pd.DataFrame | None = None,
    pe_ttm: pd.DataFrame | None = None,
    circ_mv: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """在已对齐的行情面板上生成目标权重矩阵（含杠杆缩放）。"""
    from strategy.signal import build_universe, calc_signal, regime_bull_exante

    universe = build_universe(close, amount, exclude, circ_mv=circ_mv)
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
        logger.info("weights_from_trading_panel 裁剪: %d → %d 只", nb, int(active.sum()))

    bull = regime_bull_exante(index_close, close.index) if index_close is not None else None
    signal = calc_signal(close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull)
    weights = generate_weights(signal)

    if float(LEVERAGE) != 1.0:
        lev_ser = pd.Series(float(LEVERAGE), index=weights.index)
        if index_close is not None:
            bflt = regime_bull_exante(index_close, weights.index).astype(np.float64)
            lev_ser = lev_ser * (1.0 + bflt * (float(REGIME_LEV_MULT) - 1.0))
        weights = weights.multiply(lev_ser, axis=0)
    return weights


def yearly_returns_table(
    net_ret: pd.Series,
    turnover: pd.Series | None = None,
    *,
    min_days_per_year: int = 5,
) -> list[dict[str, Any]]:
    """按自然年聚合：年净收益、年内最大回撤、年化换手。"""
    if net_ret is None or len(net_ret) < 1:
        return []
    nr = net_ret.dropna()
    rows: list[dict[str, Any]] = []
    for y in sorted(set(nr.index.year)):
        sub = nr.loc[nr.index.year == y]
        if len(sub) < min_days_per_year:
            continue
        r = float((1.0 + sub).prod() - 1.0)
        mdd = _max_drawdown_core(sub)
        ann_turn: float | None = None
        if turnover is not None and len(sub) > 0:
            tsub = turnover.reindex(sub.index).astype(np.float64).fillna(0.0)
            ann_turn = float(tsub.mean() * TRADING_DAYS_PER_YEAR)
        rows.append({
            "year": int(y),
            "net_return": round(r, 6),
            "trading_days": int(len(sub)),
            "max_drawdown": round(mdd, 6),
            "annualized_turnover": round(ann_turn, 6) if ann_turn is not None else None,
        })
    return rows


def _max_drawdown_core(net_ret: pd.Series) -> float:
    """样本内最大回撤。"""
    nr = net_ret.dropna()
    if len(nr) < 2:
        return 0.0
    nav = (1 + nr).cumprod()
    return float((nav / nav.cummax() - 1).min())
