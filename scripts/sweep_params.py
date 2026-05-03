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
# (TOP_N, REBAL_FREQ, INERTIA, 标签)
combos = [
    # --- 基准对比 ---
    (10, 80, 0.50, "N10_R80_I50"),
    # 当前最优
    # --- N=10: 高 INERTIA + 中频 ---
    (10, 60, 0.55, "N10_R60_I55"),
    (10, 60, 0.60, "N10_R60_I60"),
    (10, 60, 0.65, "N10_R60_I65"),
    (10, 60, 0.70, "N10_R60_I70"),
    (10, 70, 0.50, "N10_R70_I50"),
    (10, 70, 0.55, "N10_R70_I55"),
    (10, 70, 0.60, "N10_R70_I60"),
    (10, 70, 0.65, "N10_R70_I65"),
    (10, 80, 0.55, "N10_R80_I55"),
    (10, 80, 0.60, "N10_R80_I60"),
    (10, 80, 0.65, "N10_R80_I65"),
    (10, 80, 0.70, "N10_R80_I70"),
    (10, 90, 0.50, "N10_R90_I50"),
    (10, 90, 0.55, "N10_R90_I55"),
    (10, 90, 0.60, "N10_R90_I60"),
    (10, 90, 0.65, "N10_R90_I65"),
    # --- N=8: 更集中但惯性保留更高 ---
    (8,  70, 0.50, "N8_R70_I50"),
    (8,  70, 0.55, "N8_R70_I55"),
    (8,  70, 0.60, "N8_R70_I60"),
    (8,  70, 0.65, "N8_R70_I65"),
    (8,  80, 0.50, "N8_R80_I50"),
    (8,  80, 0.55, "N8_R80_I55"),
    (8,  80, 0.60, "N8_R80_I60"),
    (8,  80, 0.65, "N8_R80_I65"),
    (8,  90, 0.55, "N8_R90_I55"),
    (8,  90, 0.60, "N8_R90_I60"),
    (8,  90, 0.65, "N8_R90_I65"),
    # --- N=12: 略分散但用高惯性压换手 ---
    (12, 70, 0.50, "N12_R70_I50"),
    (12, 70, 0.55, "N12_R70_I55"),
    (12, 70, 0.60, "N12_R70_I60"),
    (12, 80, 0.50, "N12_R80_I50"),
    (12, 80, 0.55, "N12_R80_I55"),
    (12, 80, 0.60, "N12_R80_I60"),
    (12, 90, 0.55, "N12_R90_I55"),
    (12, 90, 0.60, "N12_R90_I60"),
    # --- N=15: 更分散 ---
    (15, 70, 0.50, "N15_R70_I50"),
    (15, 70, 0.55, "N15_R70_I55"),
    (15, 80, 0.50, "N15_R80_I50"),
    (15, 80, 0.55, "N15_R80_I55"),
    (15, 80, 0.60, "N15_R80_I60"),
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

    # 多组止损试试
    for sl_name, sl_val, sl_bull_val in [
        ("SL15_25", 0.15, 0.25),
        ("SL12_20", 0.12, 0.20),
        ("SL10_18", 0.10, 0.18),
        ("SL08_15", 0.08, 0.15),
    ]:
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
            "calmar_ratio": float(m2.get("calmar_ratio", 0) or 0),
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

# 复合评分：年化×0.4 + 夏普×0.2 + 卡玛×0.2 + (1-换手/2000)×0.1 + (1+回撤)×0.1
df['score'] = (
    df['annualized_return'] * 0.40
    + df['sharpe_ratio'] * 0.20
    + df['calmar_ratio'] * 0.20
    + (1.0 - df['annualized_turnover'] / 20.0).clip(0, 1) * 0.10
    + (1.0 + df['max_drawdown']).clip(0, 1) * 0.10
)

# 找出最佳组合
h = df[df['annualized_return'] >= 0.15].sort_values('score', ascending=False)
print("\n" + "=" * 120)
print("  ✅ 年化 >= 15% 的组合（按复合评分排序）")
print("=" * 120)
print(f"  {'排名':>3}  {'标签':>20}  {'年化收益':>8}  {'回撤':>7}  {'夏普':>5}  {'卡玛':>5}  {'换手':>5}  {'评分':>5}")
print("  " + "-" * 65)
if len(h) > 0:
    for i, (_, r) in enumerate(h.head(30).iterrows()):
        print(f"  {i+1:>3}  {r['label']:>20}  {r['annualized_return']*100:>7.1f}%  "
              f"{r['max_drawdown']*100:>6.1f}%  {r['sharpe_ratio']:>4.2f}  "
              f"{r['calmar_ratio']:>4.2f}  {r['annualized_turnover']*100:>4.0f}%  "
              f"{r['score']:>4.3f}")
else:
    print("  （无组合达到 15% 年化）")

# 低换手 + 高收益
print("\n" + "=" * 120)
print("  🔽 换手 < 500% 且 年化 >= 15%（按年化排序）")
print("=" * 120)
lo = df[(df['annualized_turnover'] < 5.0) & (df['annualized_return'] >= 0.15)].sort_values('annualized_return', ascending=False)
if len(lo) > 0:
    for i, (_, r) in enumerate(lo.head(20).iterrows()):
        print(f"  {i+1:>3}  {r['label']:>20}  {r['annualized_return']*100:>7.1f}%  "
              f"{r['max_drawdown']*100:>6.1f}%  夏普 {r['sharpe_ratio']:.2f}  "
              f"换手 {r['annualized_turnover']*100:.0f}%")
else:
    print("  （无组合满足换手<500% 且年化>=15%）")

# 低回撤 + 高收益
print("\n" + "=" * 120)
print("  🛡️ 回撤 > -25% 且 年化 >= 15%（按年化排序）")
print("=" * 120)
ld = df[(df['max_drawdown'] > -0.25) & (df['annualized_return'] >= 0.15)].sort_values('annualized_return', ascending=False)
if len(ld) > 0:
    for i, (_, r) in enumerate(ld.head(20).iterrows()):
        print(f"  {i+1:>3}  {r['label']:>20}  {r['annualized_return']*100:>7.1f}%  "
              f"{r['max_drawdown']*100:>6.1f}%  夏普 {r['sharpe_ratio']:.2f}  "
              f"换手 {r['annualized_turnover']*100:.0f}%")
else:
    print("  （无组合满足要求）")
print()
