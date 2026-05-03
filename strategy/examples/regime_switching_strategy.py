"""A 股多因子策略 v4 — 反转 + 价值 + 动量 + 盈利

改进点（vs 上一版）:
  1. 修复 generate_weights 权重堆积致命 bug
  2. 换手率优化: REBAL_FREQ 63, INERTIA 0.30, RET60 替换 RET20
  3. 去除大市值限制（MAX_MV）: 覆盖蓝筹大牛年（2017/2020/2024）
  4. 去除市场择时叠加: 反转策略在熊末反弹最强，择时反而截断收益
  5. 新增 MOM120 正向动量因子: 捕捉"底部复苏+中期趋势"的甜蜜区

v4.1 策略层（目标：全样本年化显著高于无风险利率、牛市年份更激进）:
  - CSI300 趋势牛识别（无前瞻）: 昨收>昨MA60 且 昨MA20>昨MA60
  - 牛市：有效杠杆 × REGIME_LEV_MULT，组合止损阈值放宽为 STOP_LOSS_BULL
  - 牛市：因子权重向 MOM120 倾斜、压低 RET60/MA60（截面信号仍经 rank）
  注：回测非承诺收益；小资金名义持仓宽度可参考 ``lot_effective_top_n()``。

因子基准权重（合计=1.00）:
  MA60 0.20  RSI 0.07  RET60 0.05  PB 0.16  SIZE 0.08  EP 0.12  MOM120 0.32

⸻

本文件已拆分为以下模块（此处仅做 CLI 封装和 re-export）:

  strategy/config.py        — 参数常量
  strategy/data_loader.py   — 数据加载 (ClickHouse / PostgreSQL)
  strategy/signal.py        — 因子信号计算 + 股票池
  strategy/portfolio.py     — 权重生成、组合收益、整手现金仿真、止损
  strategy/web_api.py       — Web API 入口

旧有 import（run_regime_model_for_web 等）仍可继续使用。
"""
import gc
import sys
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ═══════════════════════════════════════════════════════════
# Re-export everything for backward compatibility
# ═══════════════════════════════════════════════════════════

from strategy.config import (
    START, END, INITIAL_CASH,
    W_MA60, W_RSI, W_RET60, W_PB, W_SIZE, W_EP, W_MOM120,
    MIN_AMOUNT, FALLEN_KNIFE, VOL_CUTOFF,
    TOP_N, REBAL_FREQ, INERTIA, LEVERAGE, REGIME_LEV_MULT,
    STOP_LOSS, STOP_LOSS_BULL, STOP_COOLDOWN,
    BUY_COST_BPS, SELL_COST_BPS,
    BENCHMARK, TRADING_DAYS_PER_YEAR,
    DEFAULT_COST_SENSITIVITY_SCENARIOS,
)

from strategy.data_loader import (
    connect_db, load_price, load_valuation, load_exclude_list, load_index_close,
)

from strategy.signal import (
    regime_bull_exante, build_universe, calc_signal,
)

from strategy.portfolio import (
    generate_weights,
    simulate_cash_account_backtest,
    apply_portfolio_stop,
    weights_from_trading_panel,
    yearly_returns_table,
)

from strategy.web_api import (
    run_regime_model_for_web,
    run_regime_cost_sensitivity_for_web,
)

from strategy.backtest.metrics import calc_full_metrics, format_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("multifactor_v4")

_all_exports = [
    name for name in dir() if not name.startswith("_") or name == "_rsi"
]
__all__ = [n for n in _all_exports if isinstance(globals().get(n), (type, int, float, str, bool, list, dict, tuple)) or callable(globals().get(n))]


# ═══════════════════════════════════════════════════════════
# 主流程（仅 CLI 入口）
# ═══════════════════════════════════════════════════════════

def main():
    ic = float(INITIAL_CASH)
    logger.info("=" * 60)
    logger.info("A 股多因子策略 v4.1  %s ~ %s", START, END)
    logger.info(
        "因子(基线): MA60=%.2f RSI=%.2f RET60=%.2f PB=%.2f SIZE=%.2f EP=%.2f MOM120=%.2f",
        W_MA60, W_RSI, W_RET60, W_PB, W_SIZE, W_EP, W_MOM120,
    )
    logger.info(
        "组合: TOP=%d  调仓=%dd  惯性=%.2f  止损=非牛%.0f%%/牛%.0f%%  初始资金=¥%.0f",
        TOP_N, REBAL_FREQ, INERTIA,
        STOP_LOSS * 100, STOP_LOSS_BULL * 100, ic,
    )
    logger.info("=" * 60)

    from data.common.config import Config

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = connect_db(cfg)

    close, amount = load_price(ch)
    exclude = load_exclude_list(pg)

    logger.info("加载估值 (PB + PE + 市值)...")
    try:
        pb, pe_ttm, circ_mv = load_valuation(pg)
    except Exception as e:
        logger.warning("估值加载失败: %s", e)
        pb, pe_ttm, circ_mv = None, None, None

    universe = build_universe(close, amount, exclude, circ_mv=circ_mv)
    del amount
    gc.collect()

    index_close = load_index_close(ch)

    # 裁剪：剔除从未入池的列
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

    # 因子信号
    logger.info("计算六因子复合信号...")
    bull = regime_bull_exante(index_close, close.index) if index_close is not None else None
    signal = calc_signal(
        close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull
    )
    del pb, pe_ttm, circ_mv, universe
    gc.collect()

    # 目标权重（无杠杆，和为 1.0）
    logger.info("生成权重 (TOP=%d, 调仓=%dd)...", TOP_N, REBAL_FREQ)
    weights = generate_weights(signal)
    del signal
    gc.collect()

    # 现金仿真回测
    logger.info("运行现金仿真回测 (¥%.0f)...", ic)
    net_ret, turnover, weights_mv = simulate_cash_account_backtest(
        close, weights, ic,
    )

    # 组合层止损
    net_ret = apply_portfolio_stop(net_ret, index_close=index_close)

    metrics = calc_full_metrics(net_ret, turnover)

    # 报告
    port_eq = (1 + net_ret).cumprod() * ic
    final_equity = float(port_eq.iloc[-1]) if len(port_eq) else ic
    total_ret = final_equity / ic - 1

    print("\n" + "=" * 60)
    print("  A 股多因子策略 v4.1  现金仿真回测")
    print("=" * 60)
    print(f"  初始资金:                ¥{ic:>10,.0f}")
    print(f"  期末权益:                ¥{final_equity:>10,.0f}")
    print(f"  总收益率:                {total_ret:>+10.1%}")
    print(format_report(metrics))

    yrows = yearly_returns_table(net_ret, turnover, min_days_per_year=5)

    print(f"\n  {'年份':>4}  {'年度收益':>10}  {'年内回撤':>10}  {'期末权益':>12}  {'年化换手':>10}")
    print("  " + "-" * 60)
    for r in yrows:
        yr = int(r["year"])
        eq = port_eq.reindex(net_ret.index).loc[net_ret.index.year == yr]
        eq_val = float(eq.iloc[-1]) if len(eq) > 0 else 0.0
        at = r.get("annualized_turnover")
        at_s = f"{float(at):>10.0%}" if at is not None else f"{'—':>10}"
        print(
            f"  {yr}   {float(r['net_return']):>+10.1%}  "
            f"{float(r['max_drawdown']):>10.1%}  "
            f"¥{eq_val:>10,.0f}  {at_s}"
        )

    yr0 = int(yrows[0]["year"]) if yrows else int(START[:4])
    yr1 = int(yrows[-1]["year"]) if yrows else int(END[:4])
    print(f"\n总收益 ({yr0}~{yr1}): {total_ret:+.1%}  →  ¥{final_equity:,.0f}")

    ann_turn = metrics.get("annualized_turnover", 0) or 0
    print(f"\n成本估算（年换手 {ann_turn:.0%}）:")
    for cap in (100_000, 200_000, 300_000, 500_000):
        c = cap * ann_turn * (BUY_COST_BPS + SELL_COST_BPS) / 2 / 1e4
        print(f"   {cap // 10000}万: 年成本 {c:>6.0f} 元 ({c / cap:.1%})")

    logger.info("生成报告...")
    try:
        from strategy.backtest.visualizer import plot_report

        plot_report(
            net_ret,
            title=f"A股多因子策略 v4.1  现金仿真  {START[:4]}~{END[:4]}",
            save_path="docs/reports/multifactor_v4.png",
        )
    except Exception as e:
        logger.warning("可视化失败: %s", e)

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
