#!/usr/bin/env python3
"""
**整手真实回测**（默认终端只输出这一条）：指定本金、100 股一手、买卖分别计费（含最低 5 元）、
按 `regime_switching_strategy` 同一套信号/动态杠杆/模型止损日历做增量调仓，日收益来自 **现金+持仓权益**，
不是「理想日收益 × 本金」的数学缩放。

小数权重、无限拆分的理论曲线请直接运行 `regime_switching_strategy.py`；本脚本加 `--show-model`
时仅多打一份模型百分比绩效作参考（仍非你的整手账户）。

运行:
  python3 strategy/examples/regime_switching_lot_20k.py [--capital 100000]
  python3 strategy/examples/regime_switching_lot_20k.py --show-model
"""
from __future__ import annotations

import argparse
import gc
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from data.common.config import Config
from strategy.backtest.metrics import calc_full_metrics, format_report

_rs_path = Path(__file__).resolve().parent / "regime_switching_strategy.py"
_spec = importlib.util.spec_from_file_location("regime_switching_strategy", _rs_path)
rs = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(rs)

logger = rs.logger

INITIAL_CASH_YUAN = 100_000.0

LOT = 100
MIN_COMMISSION_YUAN = 5.0


def _commission(notional: float, bps: float) -> float:
    if notional <= 0:
        return 0.0
    return max(notional * bps / 1e4, MIN_COMMISSION_YUAN)


def _market_value(positions: dict[str, int], prices: pd.Series) -> float:
    s = 0.0
    for code, sh in positions.items():
        if sh <= 0:
            continue
        px = prices.get(code)
        if px is None or (isinstance(px, float) and np.isnan(px)):
            continue
        s += float(sh) * float(px)
    return s


def _liquidate_all(
    cash: float,
    positions: dict[str, int],
    prices: pd.Series,
) -> tuple[float, dict[str, int]]:
    for code, sh in list(positions.items()):
        if sh <= 0:
            continue
        px = prices.get(code)
        if px is None or (isinstance(px, float) and np.isnan(px)) or float(px) <= 0:
            continue
        gross = float(sh) * float(px)
        cash += gross - _commission(gross, rs.SELL_COST_BPS)
    return cash, {}


def rebalance_delta(
    cash: float,
    positions: dict[str, int],
    w_row: pd.Series,
    prices: pd.Series,
    leverage: float,
) -> tuple[float, dict[str, int], float]:
    w_pos = w_row[w_row > 1e-12]
    pos = {k: int(v) for k, v in positions.items() if v > 0}

    if w_pos.empty or float(w_pos.sum()) <= 0:
        c, p = _liquidate_all(cash, pos, prices)
        return c, p, 0.0

    w_sum = float(w_pos.sum())
    equity = cash + _market_value(pos, prices)
    if equity <= 0:
        c, p = _liquidate_all(cash, pos, prices)
        return c, p, 0.0

    budget = equity * float(leverage)
    targets_sh: dict[str, int] = {}
    for code in w_pos.index:
        px = prices.get(code)
        if px is None or (isinstance(px, float) and np.isnan(px)) or float(px) <= 0:
            continue
        px = float(px)
        tgt_mv = budget * float(w_pos[code]) / w_sum
        sh = int(tgt_mv // (px * LOT)) * LOT
        if sh > 0:
            targets_sh[code] = sh

    all_codes = set(pos) | set(targets_sh)
    deltas = {c: targets_sh.get(c, 0) - pos.get(c, 0) for c in all_codes}

    traded_notional = 0.0
    for code in sorted([c for c in deltas if deltas[c] < 0], key=lambda c: deltas[c]):
        sell_sh = -deltas[code]
        p = prices.get(code)
        if p is None or (isinstance(p, float) and np.isnan(p)) or float(p) <= 0:
            continue
        p = float(p)
        gross = sell_sh * p
        traded_notional += gross
        cash += gross - _commission(gross, rs.SELL_COST_BPS)
        pos[code] = pos.get(code, 0) - sell_sh
        if pos[code] <= 0:
            del pos[code]

    for code in sorted([c for c in deltas if deltas[c] > 0], key=lambda c: -deltas[c]):
        buy_sh = deltas[code]
        p = prices.get(code)
        if p is None or (isinstance(p, float) and np.isnan(p)) or float(p) <= 0:
            continue
        p = float(p)
        gross = buy_sh * p
        traded_notional += gross
        fee = _commission(gross, rs.BUY_COST_BPS)
        cash -= gross + fee
        pos[code] = pos.get(code, 0) + buy_sh

    return cash, pos, traded_notional


def run_ideal_strategy_constrained_backtest(
    close: pd.DataFrame,
    weights: pd.DataFrame,
    invested_start: pd.Series,
    initial_cash: float,
) -> tuple[pd.Series, pd.Series, dict]:
    """
    模型止损日历 + 持仓日按权重整手增量调仓；日收益来自真实权益变化。
    """
    idx = close.index.intersection(weights.index).intersection(invested_start.index)
    close = close.reindex(idx).ffill()
    weights = weights.reindex(idx).ffill().fillna(0.0)
    invested_start = invested_start.reindex(idx).fillna(False)

    w_turn = weights.diff().abs().sum(axis=1)
    rebal = (w_turn > 1e-9).fillna(False)
    if len(rebal) > 0:
        rebal.iloc[0] = bool(float(weights.iloc[0].sum()) > 1e-9)

    cash = float(initial_cash)
    positions: dict[str, int] = {}
    prev_equity = float(initial_cash)

    equity_series: list[float] = []
    ret_series: list[float] = []
    turn_series: list[float] = []

    stats = {
        "rebalance_count": 0,
        "model_stop_flat_days": 0,
        "entry_rebuild_count": 0,
        "final_equity_yuan": initial_cash,
        "total_return": 0.0,
    }

    n = len(idx)
    for i, dt in enumerate(idx):
        row_px = close.loc[dt]
        wrow = weights.loc[dt]
        inv = bool(invested_start.iloc[i])
        turn = 0.0

        if not inv:
            cash, positions = _liquidate_all(cash, positions, row_px)
            equity_eod = cash
            stats["model_stop_flat_days"] += 1
            adj_ret = 0.0 if i == 0 else equity_eod / prev_equity - 1.0
            equity_series.append(equity_eod)
            ret_series.append(adj_ret)
            turn_series.append(0.0)
            prev_equity = equity_eod
            continue

        liquidate_eod = i + 1 < n and not bool(invested_start.iloc[i + 1])
        first_inv = i == 0 or not bool(invested_start.iloc[i - 1])
        weight_rebal = bool(rebal.iloc[i]) and float(wrow.sum()) > 1e-9

        if not liquidate_eod and float(wrow.sum()) > 1e-9:
            if first_inv:
                stats["entry_rebuild_count"] += 1
                cash, positions, _ = rebalance_delta(
                    cash, positions, wrow, row_px, float(rs.LEVERAGE)
                )
                if i > 0:
                    turn = float((wrow - weights.iloc[i - 1]).abs().sum())
                else:
                    turn = float(wrow.abs().sum())
            elif weight_rebal:
                stats["rebalance_count"] += 1
                cash, positions, _ = rebalance_delta(
                    cash, positions, wrow, row_px, float(rs.LEVERAGE)
                )
                turn = float((wrow - weights.iloc[i - 1]).abs().sum())

        equity_eod = cash + _market_value(positions, row_px)
        if liquidate_eod:
            cash, positions = _liquidate_all(cash, positions, row_px)
            equity_eod = cash

        adj_ret = 0.0 if i == 0 else equity_eod / prev_equity - 1.0
        equity_series.append(equity_eod)
        ret_series.append(adj_ret)
        turn_series.append(turn)
        prev_equity = equity_eod

    equity_s = pd.Series(equity_series, index=idx)
    net_ret = pd.Series(ret_series, index=idx)
    turnover = pd.Series(turn_series, index=idx)
    stats["final_equity_yuan"] = float(equity_s.iloc[-1])
    stats["total_return"] = float(equity_s.iloc[-1] / initial_cash - 1.0)
    return net_ret, turnover, stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="整手真实回测（默认）；与 regime_switching_strategy 同策略参数。",
    )
    ap.add_argument(
        "--capital",
        type=float,
        default=None,
        metavar="元",
        help=f"初始本金（覆盖 INITIAL_CASH_YUAN，默认 {INITIAL_CASH_YUAN:g}）",
    )
    ap.add_argument(
        "--show-model",
        action="store_true",
        help="额外打印小数权重模型绩效（百分比），非整手账户",
    )
    args = ap.parse_args()

    initial_cash = float(args.capital) if args.capital is not None else float(INITIAL_CASH_YUAN)

    logger.info("=" * 60)
    logger.info("整手真实回测 本金=%.0f 元  show_model=%s", initial_cash, args.show_model)
    logger.info("=" * 60)

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = rs.connect_db(cfg)
    try:
        close, amount = rs.load_price(ch)
        exclude = rs.load_exclude_list(pg)
        try:
            pb, pe_ttm, circ_mv = rs.load_valuation(pg)
        except Exception as e:
            logger.warning("估值加载失败: %s", e)
            pb, pe_ttm, circ_mv = None, None, None

        universe = rs.build_universe(close, amount, exclude, circ_mv=circ_mv)
        del amount
        gc.collect()

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
            logger.info("裁剪: %d → %d 只", nb, active.sum())
        del active
        gc.collect()

        index_close = rs.load_index_close(ch)

        bull_sig = (
            rs.regime_bull_exante(index_close, close.index) if index_close is not None else None
        )
        signal = rs.calc_signal(
            close,
            universe,
            pb=pb,
            pe_ttm=pe_ttm,
            circ_mv=circ_mv,
            regime_bull=bull_sig,
        )
        del pb, pe_ttm, circ_mv, universe
        gc.collect()

        top_n_lot = rs.lot_effective_top_n(initial_cash)
        inertia_lot = 0.20 if top_n_lot <= 14 else float(rs.INERTIA)
        logger.info(
            "整手专用持仓宽度: TOP_N=%d (估算可覆盖一手，峰值杠杆≈%.2f×)  INERTIA=%.2f",
            top_n_lot,
            float(rs.LEVERAGE) * float(rs.REGIME_LEV_MULT),
            inertia_lot,
        )
        weights = rs.generate_weights(signal, top_n=top_n_lot, inertia=inertia_lot)
        if args.show_model:
            weights_full_model = rs.generate_weights(signal, top_n=rs.TOP_N, inertia=rs.INERTIA)
        del signal
        gc.collect()

        def _apply_regime_leverage(w: pd.DataFrame) -> pd.DataFrame:
            if float(rs.LEVERAGE) == 1.0:
                return w
            lev_ser = pd.Series(float(rs.LEVERAGE), index=w.index)
            if index_close is not None:
                bflt = rs.regime_bull_exante(index_close, w.index).astype(np.float64)
                lev_ser = lev_ser * (1.0 + bflt * (float(rs.REGIME_LEV_MULT) - 1.0))
            return w.multiply(lev_ser, axis=0)

        weights = _apply_regime_leverage(weights)

        logger.info("止损日历（与主脚本一致）→ 整手撮合回测...")
        net_pre, turn_m = rs.calc_portfolio_return(weights, close)
        invested = rs.portfolio_stop_invested_start(net_pre, index_close=index_close)

        net_con, turn_c, st = run_ideal_strategy_constrained_backtest(
            close, weights, invested, initial_cash
        )

        metrics_con = calc_full_metrics(net_con, turn_c)
        eq_real = float(initial_cash) * (1.0 + net_con).cumprod()

        def _yr(nr: pd.Series) -> dict[int, float]:
            return {
                int(y): float((1 + nr[nr.index.year == y]).prod() - 1)
                for y in sorted(set(nr.index.year))
                if len(nr[nr.index.year == y]) >= 5
            }

        def _dd(nr: pd.Series) -> float:
            nav = (1 + nr).cumprod()
            return float((nav / nav.cummax() - 1).min())

        def _print_yearly_real(title: str, nr: pd.Series, eq: pd.Series) -> None:
            print(f"\n  {title}")
            print(f"  {'年份':>4}  {'年度收益':>10}  {'年内回撤':>10}  {'年末权益(元)':>14}")
            print("  " + "-" * 52)
            yearly = _yr(nr)
            for yr, yr_ret in sorted(yearly.items()):
                r_ = nr[nr.index.year == yr]
                dd = _dd(r_)
                last_dt = r_.index.max()
                eq_end = float(eq.loc[last_dt]) if last_dt in eq.index else float("nan")
                print(f"  {yr}   {yr_ret:>+10.1%}  {dd:>10.1%}  {eq_end:>14,.2f}")

        def _print_yearly_pct(title: str, nr: pd.Series) -> None:
            print(f"\n  {title}")
            print(f"  {'年份':>4}  {'年度收益':>10}  {'年内回撤':>10}")
            print("  " + "-" * 34)
            yearly = _yr(nr)
            for yr, yr_ret in sorted(yearly.items()):
                r_ = nr[nr.index.year == yr]
                dd = _dd(r_)
                print(f"  {yr}   {yr_ret:>+10.1%}  {dd:>10.1%}")

        print("\n" + "=" * 60)
        print("  【整手真实回测】100 股/手 · 最低佣 · 与主策略同权重与止损日历")
        print("=" * 60)
        print(format_report(metrics_con))
        print(
            f"\n  初始本金: {initial_cash:,.2f} 元  期末权益: {st['final_equity_yuan']:,.2f} 元"
            f"\n  总收益率: {st['total_return']:+.2%}"
            f"\n  整手 TOP_N: {top_n_lot}（主脚本理想组合仍为 {rs.TOP_N} 只）"
            f"\n  建仓/再入场: {st['entry_rebuild_count']} 次  调仓: {st['rebalance_count']} 次"
            f"\n  同步模型空仓: {st['model_stop_flat_days']} 天"
        )
        _print_yearly_real("分年（整手·年末权益为模拟账户真实值）", net_con, eq_real)

        if args.show_model:
            wf = _apply_regime_leverage(weights_full_model)
            net_full, turn_full = rs.calc_portfolio_return(wf, close)
            net_ideal = rs.apply_portfolio_stop(net_full, index_close=index_close)
            metrics_ideal = calc_full_metrics(net_ideal, turn_full)
            print("\n" + "=" * 60)
            print(f"  【可选参考】小数权重 · TOP_N={rs.TOP_N}（与 regime_switching_strategy 主脚本一致）")
            print("=" * 60)
            print(format_report(metrics_ideal))
            _print_yearly_pct("分年（模型·仅百分比）", net_ideal)

    finally:
        ch.close()
        pg.close()


if __name__ == "__main__":
    main()
