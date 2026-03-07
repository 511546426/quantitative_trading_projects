"""
历史模拟演示：用 2025 年全年数据跑一遍纸面交易流程

演示目标：
  - 验证 Runner → Broker → PositionManager 全链路正确
  - 打印每次调仓的详细日志
  - 最终输出年度绩效汇总

运行：
    python3 -m execution.examples.simulate
"""
from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

import pandas as pd

# ─── 日志 ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%H:%M:%S",
    stream  = sys.stdout,
)
logger = logging.getLogger(__name__)

# ─── 沙盒状态目录（模拟用，不污染真实状态） ────────────────────────────────────
SIM_STATE_DIR = "execution/state/sim"


def clean_sim_state():
    p = Path(SIM_STATE_DIR)
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)


def get_trade_dates(ch_client, start: str, end: str) -> list[str]:
    """从 ClickHouse 获取指定区间的真实交易日列表"""
    rows = ch_client.execute("""
        SELECT DISTINCT trade_date
        FROM stock_daily
        WHERE trade_date >= %(s)s AND trade_date <= %(e)s
          AND is_suspended = 0
        ORDER BY trade_date
    """, {"s": start, "e": end})
    return [r[0].strftime("%Y%m%d") if hasattr(r[0], "strftime") else str(r[0]) for r in rows]


def run_simulation(start: str = "20250101", end: str = "20251231",
                   initial_capital: float = 500_000.0):
    from execution.broker.paper_broker import PaperBroker
    from execution.portfolio.position_manager import PositionManager
    from execution.runner import DailyRunner
    from strategy.examples.reversal_value_strategy import (
        build_universe, calc_factors, generate_weights,
    )

    clean_sim_state()

    portfolio_file = f"{SIM_STATE_DIR}/portfolio.json"
    pending_file   = f"{SIM_STATE_DIR}/pending_orders.json"

    broker   = PaperBroker(pending_file)
    runner   = DailyRunner(initial_capital=initial_capital, broker=broker)
    runner.portfolio = PositionManager(portfolio_file, initial_capital)

    trade_dates = get_trade_dates(runner._ch._client, start, end)
    logger.info("模拟区间: %s → %s，共 %d 个交易日", start, end, len(trade_dates))

    # ── 一次性运行策略，得到全期目标权重（效率远高于每日重算） ──────────────────
    logger.info("预计算全期策略权重（仅需运行一次）...")
    close, amount = runner._load_strategy_data(end, lookback=400)
    exclude = runner._load_exclude_list()
    universe  = build_universe(close, amount, exclude)
    signal    = calc_factors(close, universe)
    weights_df = generate_weights(signal)
    # 将 index 统一为 'YYYYMMDD' 字符串，方便按日期查找
    weights_df.index = weights_df.index.strftime("%Y%m%d")
    logger.info("权重矩阵: %d 交易日 × %d 只股票", *weights_df.shape)

    for date in trade_dates:
        _run_one_day(runner, date, weights_df)

    # ─── 绩效汇总 ─────────────────────────────────────────────────────────────
    nav_history = runner.portfolio.state.nav_history
    if len(nav_history) < 2:
        logger.warning("数据不足，无法计算绩效")
        return

    navs  = pd.DataFrame(nav_history).set_index("date")["nav"]
    rets  = navs.pct_change().dropna()
    total = navs.iloc[-1] / navs.iloc[0] - 1
    ann   = (1 + total) ** (252 / len(rets)) - 1
    vol   = rets.std() * (252 ** 0.5)
    sharpe = ann / vol if vol > 0 else 0
    peak  = navs.cummax()
    dd    = (navs - peak) / peak
    mdd   = dd.min()

    trades = runner.portfolio.state.trade_history
    buy_cnt  = sum(1 for t in trades if t["direction"] == "BUY")
    sell_cnt = sum(1 for t in trades if t["direction"] == "SELL")
    total_commission = sum(t.get("commission", 0) for t in trades
                           if isinstance(t, dict) and "commission" in t)

    print(f"\n{'='*55}")
    print(f"{'模拟绩效汇总':^55}")
    print(f"{'='*55}")
    print(f"  模拟区间:   {start} → {end}")
    print(f"  初始本金:   ¥{initial_capital:>12,.0f}")
    print(f"  期末净值:   ¥{navs.iloc[-1]:>12,.0f}")
    print(f"  总收益率:   {total:>+12.2%}")
    print(f"  年化收益:   {ann:>+12.2%}")
    print(f"  年化波动:   {vol:>12.2%}")
    print(f"  夏普比率:   {sharpe:>12.2f}")
    print(f"  最大回撤:   {mdd:>12.2%}")
    print(f"{'─'*55}")
    print(f"  总交易笔数: {buy_cnt + sell_cnt:>5}（买 {buy_cnt}, 卖 {sell_cnt}）")
    print(f"  总手续费:   ¥{total_commission:>10,.0f}")
    print(f"{'='*55}\n")

    runner.portfolio.print_summary()


def _run_one_day(runner, date: str, weights_df: "pd.DataFrame"):
    """单日执行：成交昨日订单 → 更新净值 → 检查是否调仓 → 提交新订单"""
    from execution.oms.order import Direction, Order, OrderStatus
    import math

    price_data = runner._load_price_data(date)
    if not price_data:
        logger.warning("[%s] 无价格数据，跳过", date)
        return

    # 1. 成交昨日挂起订单
    runner._fill_pending(date, price_data)

    # 2. 更新持仓市值
    close_prices = {ts: info["close"] for ts, info in price_data.items() if info["close"] > 0}
    runner.portfolio.update_prices(close_prices, date)
    runner.risk.check_portfolio_drawdown(runner.portfolio)

    # 3. 查找今日目标权重
    if date not in weights_df.index:
        runner.portfolio.save()
        return

    today_w = weights_df.loc[date]
    # 找前一行（前一交易日）
    idx = weights_df.index.get_loc(date)
    if idx > 0:
        prev_w = weights_df.iloc[idx - 1]
        prev_held = set(prev_w[prev_w > 0].index)
    else:
        prev_held = set()

    curr_held = set(today_w[today_w > 0].index)
    if curr_held == prev_held and idx > 0:
        runner.portfolio.save()
        return  # 非调仓日

    target_weights = today_w[today_w > 0].to_dict()
    logger.info("[%s] 调仓日 → 目标 %d 只: %s",
                date, len(target_weights), sorted(target_weights.keys())[:5])

    # 4. 生成订单 → 风控检查 → 提交
    orders = runner._compute_orders(target_weights, price_data, date)
    accepted = rejected = 0
    for order in orders:
        ok, reason = runner.risk.check_all(order, runner.portfolio, price_data)
        if ok:
            order.signal_date = date
            runner.broker.submit_order(order)
            accepted += 1
        else:
            order.status = OrderStatus.REJECTED
            order.reject_reason = reason
            rejected += 1

    if accepted or rejected:
        logger.info("[%s] 订单: %d 通过, %d 被拒", date, accepted, rejected)

    runner.portfolio.save()
    runner.portfolio.print_summary()


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "20250101"
    end   = sys.argv[2] if len(sys.argv) > 2 else "20251231"
    run_simulation(start=start, end=end)
