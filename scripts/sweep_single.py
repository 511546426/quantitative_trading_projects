"""
单组参数回测（供 sweep 用，轻量版）
"""
import gc, sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
logging.basicConfig(level=logging.WARNING)

from strategy.config import START, END, INITIAL_CASH
from strategy.data_loader import connect_db, load_price, load_valuation, load_exclude_list, load_index_close
from strategy.signal import build_universe, calc_signal, regime_bull_exante
from strategy.portfolio import generate_weights, simulate_cash_account_backtest, apply_portfolio_stop
from strategy.backtest.metrics import calc_full_metrics

import pandas as pd, numpy as np

IC = float(INITIAL_CASH)
TOP_N = int(sys.argv[1]) if len(sys.argv) > 1 else 10
REBAL = int(sys.argv[2]) if len(sys.argv) > 2 else 60
INERTIA_V = float(sys.argv[3]) if len(sys.argv) > 3 else 0.40
SL = float(sys.argv[4]) if len(sys.argv) > 4 else 0.17
SL_BULL = float(sys.argv[5]) if len(sys.argv) > 5 else 0.27
LABEL = sys.argv[6] if len(sys.argv) > 6 else f"N{TOP_N}_R{REBAL}_I{INERTIA_V}_SL{SL}"

cfg = __import__("data.common.config", fromlist=["Config"]).Config.load(
    "data/config/settings.yaml", "data/config/sources.yaml"
)
ch, pg = connect_db(cfg)

close, amount = load_price(ch)
exclude = load_exclude_list(pg)
pb, pe_ttm, circ_mv = load_valuation(pg)
universe = build_universe(close, amount, exclude, circ_mv=circ_mv)
del amount; gc.collect()
index_close = load_index_close(ch)

active = universe.any(axis=0)
if active.sum() < universe.shape[1]:
    nb = universe.shape[1]
    close = close.loc[:, active]
    universe = universe.loc[:, active]
    if pb is not None: pb = pb.reindex(columns=close.columns)
    if pe_ttm is not None: pe_ttm = pe_ttm.reindex(columns=close.columns)
    if circ_mv is not None: circ_mv = circ_mv.reindex(columns=close.columns)
del active; gc.collect()

bull = regime_bull_exante(index_close, close.index) if index_close is not None else None
signal = calc_signal(close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull)
del pb, pe_ttm, circ_mv, universe; gc.collect()

weights = generate_weights(signal, top_n=TOP_N, rebal_freq=REBAL, inertia=INERTIA_V)
del signal; gc.collect()

net_ret, turnover, _ = simulate_cash_account_backtest(close, weights, IC)
del close, weights; gc.collect()

net_ret = apply_portfolio_stop(net_ret, index_close=index_close, stop_loss=SL, stop_loss_bull=SL_BULL)
m = calc_full_metrics(net_ret, turnover)

ann = float(m.get("annualized_return", 0) or 0)
mdd = float(m.get("max_drawdown", 0) or 0)
sharpe = float(m.get("sharpe_ratio", 0) or 0)
turn = float(m.get("annualized_turnover", 0) or 0)
total = float(m.get("total_return", 0) or 0)

ch.close(); pg.close()

print(f"RESULT|{LABEL}|{ann:.6f}|{total:.6f}|{mdd:.6f}|{sharpe:.6f}|{turn:.6f}")
