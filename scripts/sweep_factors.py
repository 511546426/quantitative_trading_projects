"""
因子权重扫描：自动找最佳因子权重组合
数据只加载一次，通过 monkey-patch signal 模块常量扫多组权重
"""
import gc, sys, logging, copy
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

from strategy.config import START, END, INITIAL_CASH, TOP_N, REBAL_FREQ, INERTIA, STOP_LOSS, STOP_LOSS_BULL
from strategy.data_loader import connect_db, load_price, load_valuation, load_exclude_list, load_index_close
import strategy.signal as signal_module
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
universe = signal_module.build_universe(close, amount, exclude, circ_mv=circ_mv)
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

# ── 因子权重组合 ──────────────────────────────────────
# (W_MA60, W_RSI, W_RET60, W_PB, W_SIZE, W_EP, W_MOM120, W_MOM20, W_SUP_TREND, 标签)
combos = [
    # 策略 A: 价值倾斜 (降 MOM120 → 升 PB/EP)
    (0.20, 0.07, 0.05, 0.16, 0.08, 0.12, 0.32, 0.00, 0.00, "A0_基线"),
    (0.20, 0.07, 0.05, 0.18, 0.08, 0.15, 0.27, 0.00, 0.00, "A1_价值增"),
    (0.20, 0.07, 0.05, 0.20, 0.08, 0.18, 0.22, 0.00, 0.00, "A2_价值重"),
    (0.22, 0.07, 0.05, 0.18, 0.08, 0.14, 0.26, 0.00, 0.00, "A3_价值+MA60"),
    # 策略 B: 市值倾斜 (降 MOM120 → 升 SIZE)
    (0.20, 0.07, 0.05, 0.16, 0.12, 0.12, 0.28, 0.00, 0.00, "B1_SIZE增"),
    (0.20, 0.07, 0.05, 0.16, 0.15, 0.12, 0.25, 0.00, 0.00, "B2_SIZE重"),
    # 策略 C: 启用 MOM20
    (0.17, 0.07, 0.05, 0.16, 0.08, 0.12, 0.27, 0.08, 0.00, "C1_MOM20轻"),
    (0.15, 0.07, 0.05, 0.16, 0.08, 0.12, 0.25, 0.12, 0.00, "C2_MOM20中"),
    # 策略 D: 启用 TREND
    (0.17, 0.07, 0.05, 0.16, 0.08, 0.12, 0.27, 0.00, 0.08, "D1_TREND轻"),
    (0.15, 0.07, 0.05, 0.16, 0.08, 0.12, 0.25, 0.00, 0.12, "D2_TREND中"),
    # 策略 E: 均衡分散
    (0.18, 0.08, 0.06, 0.16, 0.10, 0.14, 0.28, 0.00, 0.00, "E1_均衡"),
    (0.15, 0.10, 0.08, 0.18, 0.12, 0.15, 0.22, 0.00, 0.00, "E2_均衡重"),
    # 策略 F: MOM 更重 (更高收益/更高换手)
    (0.17, 0.06, 0.04, 0.14, 0.07, 0.10, 0.42, 0.00, 0.00, "F1_MOM重"),
    (0.15, 0.05, 0.03, 0.12, 0.06, 0.08, 0.51, 0.00, 0.00, "F2_MOM极重"),
    # 策略 G: MA60 + SIZE 反转增强
    (0.25, 0.07, 0.05, 0.14, 0.10, 0.10, 0.29, 0.00, 0.00, "G1_MA60+SIZE"),
    (0.28, 0.07, 0.07, 0.12, 0.12, 0.10, 0.24, 0.00, 0.00, "G2_反转增强"),
    # 策略 H: 全启用实验
    (0.15, 0.06, 0.05, 0.14, 0.08, 0.12, 0.28, 0.06, 0.06, "H1_全启用轻"),
    (0.12, 0.06, 0.05, 0.14, 0.08, 0.12, 0.25, 0.08, 0.10, "H2_全启用中"),
]

# ── 组合参数 ──────────────────────────────────────────
# (TOP_N, REBAL_FREQ, INERTIA, 止损, 止损牛, 标签)
port_combos = [
    (10, 80, 0.50, 0.12, 0.20, "N10_R80_I50_SL12_20"),
    (10, 80, 0.55, 0.12, 0.20, "N10_R80_I55_SL12_20"),
    (10, 70, 0.55, 0.12, 0.20, "N10_R70_I55_SL12_20"),
    (12, 80, 0.55, 0.12, 0.20, "N12_R80_I55_SL12_20"),
]

# 备份原始信号模块常量
orig_attrs = {
    k: getattr(signal_module, k)
    for k in ["W_MA60", "W_RSI", "W_RET60", "W_PB", "W_SIZE", "W_EP", "W_MOM120", "W_MOM20", "W_SUP_TREND"]
}

results = []
n_total = len(combos) * len(port_combos)
n = 0

for w_ma60, w_rsi, w_ret60, w_pb, w_size, w_ep, w_mom120, w_mom20, w_sup, label_f in combos:
    # monkey-patch 信号模块
    for k, v in zip(
        ["W_MA60", "W_RSI", "W_RET60", "W_PB", "W_SIZE", "W_EP", "W_MOM120", "W_MOM20", "W_SUP_TREND"],
        [w_ma60, w_rsi, w_ret60, w_pb, w_size, w_ep, w_mom120, w_mom20, w_sup],
    ):
        setattr(signal_module, k, v)

    # 重新计算信号
    bull = signal_module.regime_bull_exante(index_close, close.index) if index_close is not None else None
    signal = signal_module.calc_signal(
        close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull
    )

    for top_n, rebal_freq, inertia, sl, sl_bull, label_p in port_combos:
        n += 1
        print(f"  [{n}/{n_total}] {label_f} + {label_p} ...", end=" ", flush=True)

        weights = generate_weights(signal, top_n=top_n, rebal_freq=rebal_freq, inertia=inertia)
        net_ret, turnover, _ = simulate_cash_account_backtest(close, weights, IC)
        net_ret_sl = apply_portfolio_stop(net_ret, index_close=index_close,
                                           stop_loss=sl, stop_loss_bull=sl_bull)
        m = calc_full_metrics(net_ret_sl, turnover)

        ann_ret = float(m.get("annualized_return", 0) or 0)
        mdd = float(m.get("max_drawdown", 0) or 0)
        sharpe = float(m.get("sharpe_ratio", 0) or 0)
        turn = float(m.get("annualized_turnover", 0) or 0)
        calmar = float(m.get("calmar_ratio", 0) or 0)
        total = float(m.get("total_return", 0) or 0)

        print(f"年化={ann_ret*100:.1f}% DD={mdd*100:.1f}% 夏普={sharpe:.2f} 换手={turn*100:.0f}%")

        results.append({
            "factor_label": label_f,
            "portfolio_label": label_p,
            "top_n": top_n, "rebal_freq": rebal_freq, "inertia": inertia,
            "stop_loss": sl, "stop_loss_bull": sl_bull,
            "W_MA60": w_ma60, "W_RSI": w_rsi, "W_RET60": w_ret60,
            "W_PB": w_pb, "W_SIZE": w_size, "W_EP": w_ep,
            "W_MOM120": w_mom120, "W_MOM20": w_mom20, "W_SUP_TREND": w_sup,
            "annualized_return": ann_ret,
            "max_drawdown": mdd,
            "sharpe_ratio": sharpe,
            "calmar_ratio": calmar,
            "annualized_turnover": turn,
            "total_return": total,
        })

# 恢复原始常量
for k, v in orig_attrs.items():
    setattr(signal_module, k, v)

ch.close(); pg.close()

df = pd.DataFrame(results)

# 复合评分
df['score'] = (
    df['annualized_return'] * 0.40
    + df['sharpe_ratio'] * 0.20
    + df['calmar_ratio'] * 0.20
    + (1.0 - df['annualized_turnover'] / 20.0).clip(0, 1) * 0.10
    + (1.0 + df['max_drawdown']).clip(0, 1) * 0.10
)

df = df.sort_values('score', ascending=False)

print("\n" + "=" * 130)
print(f"  因子权重扫描结果 (¥{IC:,.0f}, {START}~{END})")
print("=" * 130)
print(f"  {'排名':>3}  {'因子':>12}  {'组合':>18}  {'年化':>7}  {'回撤':>7}  {'夏普':>5}  {'卡玛':>5}  {'换手':>5}  {'评分':>5}")
print("  " + "-" * 75)
for i, (_, r) in enumerate(df.head(30).iterrows()):
    print(f"  {i+1:>3}  {r['factor_label']:>12}  {r['portfolio_label']:>18}  "
          f"{r['annualized_return']*100:>6.1f}%  {r['max_drawdown']*100:>6.1f}%  "
          f"{r['sharpe_ratio']:>4.2f}  {r['calmar_ratio']:>4.2f}  "
          f"{r['annualized_turnover']*100:>4.0f}%  {r['score']:>4.3f}")

# 按因子分组展示最佳
print("\n" + "=" * 130)
print("  📊 各因子组合最佳表现（每组选评分最高的组合参数）")
print("=" * 130)
print(f"  {'因子':>12}  {'组合':>18}  {'年化':>7}  {'回撤':>7}  {'夏普':>5}  {'换手':>5}  {'评分':>5}  {'权重分布':>30}")
print("  " + "-" * 95)
for fl in df['factor_label'].unique():
    best = df[df['factor_label'] == fl].sort_values('score', ascending=False).iloc[0]
    w_str = f"MA60={best['W_MA60']:.2f} RSI={best['W_RSI']:.2f} RET={best['W_RET60']:.2f} PB={best['W_PB']:.2f} SZ={best['W_SIZE']:.2f} EP={best['W_EP']:.2f} M120={best['W_MOM120']:.2f}"
    if best['W_MOM20'] > 0:
        w_str += f" M20={best['W_MOM20']:.2f}"
    if best['W_SUP_TREND'] > 0:
        w_str += f" TR={best['W_SUP_TREND']:.2f}"
    print(f"  {best['factor_label']:>12}  {best['portfolio_label']:>18}  "
          f"{best['annualized_return']*100:>6.1f}%  {best['max_drawdown']*100:>6.1f}%  "
          f"{best['sharpe_ratio']:>4.2f}  {best['annualized_turnover']*100:>4.0f}%  "
          f"{best['score']:>4.3f}")
    print(f"  {'':>12}  {'':>18}  权重: {w_str}")

# 换手 < 500% 且年化 >= 15%
print("\n" + "=" * 130)
print("  🔽 换手 < 500% 且年化 >= 15%（按年化排序）")
print("=" * 130)
lo = df[(df['annualized_turnover'] < 5.0) & (df['annualized_return'] >= 0.15)].sort_values('annualized_return', ascending=False)
if len(lo) > 0:
    for i, (_, r) in enumerate(lo.head(20).iterrows()):
        print(f"  {i+1:>3}  {r['factor_label']:>12}  {r['portfolio_label']:>18}  "
              f"{r['annualized_return']*100:>6.1f}%  {r['max_drawdown']*100:>6.1f}%  "
              f"夏普 {r['sharpe_ratio']:.2f}  换手 {r['annualized_turnover']*100:.0f}%")
else:
    print("  （无组合满足条件）")

# 回撤 > -22% 且年化 >= 15%
print("\n" + "=" * 130)
print("  🛡️ 回撤 > -22% 且年化 >= 15%（按年化排序）")
print("=" * 130)
ld = df[(df['max_drawdown'] > -0.22) & (df['annualized_return'] >= 0.15)].sort_values('annualized_return', ascending=False)
if len(ld) > 0:
    for i, (_, r) in enumerate(ld.head(20).iterrows()):
        print(f"  {i+1:>3}  {r['factor_label']:>12}  {r['portfolio_label']:>18}  "
              f"{r['annualized_return']*100:>6.1f}%  {r['max_drawdown']*100:>6.1f}%  "
              f"夏普 {r['sharpe_ratio']:.2f}  换手 {r['annualized_turnover']*100:.0f}%")
else:
    print("  （无组合满足条件）")

# 综合对比: 超当前基线的组合
base_score = df[df['factor_label'] == 'A0_基线'].sort_values('score', ascending=False).iloc[0]['score']
print("\n" + "=" * 130)
print(f"  ⚡ 超基线 (评分>{base_score:.3f}) 的组合（按评分排序）")
print("=" * 130)
better = df[df['score'] > base_score].sort_values('score', ascending=False)
if len(better) > 0:
    for i, (_, r) in enumerate(better.head(20).iterrows()):
        print(f"  {i+1:>3}  {r['factor_label']:>12}  {r['portfolio_label']:>18}  "
              f"{r['annualized_return']*100:>6.1f}%  {r['max_drawdown']*100:>6.1f}%  "
              f"夏普 {r['sharpe_ratio']:.2f}  换手 {r['annualized_turnover']*100:.0f}%  "
              f"评分 {r['score']:.3f}")
else:
    print("  （无组合超过基线）")
print()
