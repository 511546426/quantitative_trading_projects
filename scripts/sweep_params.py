"""
参数扫描：自动找年化 15%+ 的配置
数据只加载一次，跑多组 TOP_N / INERTIA / REBAL_FREQ / STOP_LOSS 组合
"""
import gc, sys, logging, json
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

from strategy.config import START, END, INITIAL_CASH
from strategy.data_loader import connect_db, load_price, load_valuation, load_exclude_list, load_index_close
from strategy.signal import build_universe, calc_signal, regime_bull_exante
from strategy.portfolio import generate_weights, simulate_cash_account_backtest, apply_portfolio_stop
from strategy.backtest.metrics import calc_full_metrics

IC = float(INITIAL_CASH)

cfg = __import__("data.common.config", fromlist=["Config"]).Config.load(
    "data/config/settings.yaml", "data/config/sources.yaml"
)
ch, pg = connect_db(cfg)

print("\n加载数据...")
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

print("计算信号...")
bull = regime_bull_exante(index_close, close.index) if index_close is not None else None
signal = calc_signal(close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull)
del pb, pe_ttm, circ_mv, universe; gc.collect()

print("信号就绪，开始参数扫描...\n")

# ── 参数组合 ──────────────────────────────────────────
combos = [
    # (TOP_N, REBAL_FREQ, INERTIA, 标签)
    (8,  60, 0.35, "N8_R60_I35"),
    (8,  60, 0.45, "N8_R60_I45"),
    (10, 60, 0.35, "N10_R60_I35"),
    (10, 60, 0.45, "N10_R60_I45"),
    (10, 70, 0.40, "N10_R70_I40"),
    (12, 60, 0.40, "N12_R60_I40"),
    (12, 50, 0.40, "N12_R50_I40"),
    (15, 60, 0.40, "N15_R60_I40"),
    # 再加一组低换手激进
    (8,  65, 0.50, "N8_R65_I50"),
    (10, 65, 0.50, "N10_R65_I50"),
]

results = []
for top_n, rebal_freq, inertia, label in combos:
    print(f"  {label} ...", end=" ", flush=True)
    weights = generate_weights(signal, top_n=top_n, rebal_freq=rebal_freq, inertia=inertia)

    # 基准止损
    net_ret, turnover, _ = simulate_cash_account_backtest(close, weights, IC)
    net_ret_sl = apply_portfolio_stop(net_ret, index_close=index_close)
    m = calc_full_metrics(net_ret_sl, turnover)

    ann_ret = float(m.get("annualized_return", 0) or 0)
    mdd = float(m.get("max_drawdown", 0) or 0)
    sharpe = float(m.get("sharpe_ratio", 0) or 0)
    turn = float(m.get("annualized_turnover", 0) or 0)
    total = float(m.get("total_return", 0) or 0)

    print(f"年化={ann_ret*100:.1f}% DD={mdd*100:.1f}% 夏普={sharpe:.2f} 换手={turn*100:.0f}%")

    # 收紧止损试试
    for sl_name, sl_val, sl_bull_val in [("SL15_25", 0.15, 0.25), ("SL12_20", 0.12, 0.20)]:
        net_ret_sl2 = apply_portfolio_stop(net_ret, index_close=index_close,
                                           stop_loss=sl_val, stop_loss_bull=sl_bull_val)
        m2 = calc_full_metrics(net_ret_sl2, turnover)
        results.append({
            "label": f"{label}_{sl_name}",
            "top_n": top_n, "rebal_freq": rebal_freq, "inertia": inertia,
            "stop_loss": sl_val, "stop_loss_bull": sl_bull_val,
            "annualized_return": float(m2.get("annualized_return", 0) or 0),
            "max_drawdown": float(m2.get("max_drawdown", 0) or 0),
            "sharpe_ratio": float(m2.get("sharpe_ratio", 0) or 0),
            "annualized_turnover": float(m2.get("annualized_turnover", 0) or 0),
            "total_return": float(m2.get("total_return", 0) or 0),
        })

ch.close(); pg.close()

# ── 排序 & 输出 ──────────────────────────────────────
df = pd.DataFrame(results)
df = df.sort_values("annualized_return", ascending=False)

print("\n" + "=" * 100)
print(f"  参数扫描结果 (初始资金 ¥{IC:,.0f}, {START}~{END})")
print("=" * 100)
print(f"  {'排名':>3}  {'标签':>20}  {'年化收益':>8}  {'总收益':>8}  {'最大回撤':>8}  {'夏普':>6}  {'换手':>6}")
print("  " + "-" * 70)
for i, (_, r) in enumerate(df.iterrows()):
    print(f"  {i+1:>3}  {r['label']:>20}  {r['annualized_return']*100:>7.1f}%  "
          f"{r['total_return']*100:>7.1f}%  {r['max_drawdown']*100:>7.1f}%  "
          f"{r['sharpe_ratio']:>5.2f}  {r['annualized_turnover']*100:>5.0f}%")
    if r['annualized_return'] >= 0.15:
        print(f"  {'':>73} ← 达标 15%+")

# 找出最佳组合
h = df[df['annualized_return'] >= 0.15].sort_values('sharpe_ratio', ascending=False)
print("\n" + "=" * 100)
print("  ✅ 年化 >= 15% 的组合（按夏普排序）")
print("=" * 100)
if len(h) > 0:
    for i, (_, r) in enumerate(h.iterrows()):
        print(f"  {i+1}. {r['label']}: 年化 {r['annualized_return']*100:.1f}%  "
              f"回撤 {r['max_drawdown']*100:.1f}%  夏普 {r['sharpe_ratio']:.2f}  "
              f"换手 {r['annualized_turnover']*100:.0f}%")
else:
    print("  （无组合达到 15% 年化）")
print()
