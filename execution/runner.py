"""
每日执行主入口

调用时序（每个交易日收盘后运行一次）：

  15:15 运行 runner.run(date=today)
    │
    ├─ 1. 用今日开盘价成交昨日挂起订单 → 更新持仓
    ├─ 2. 用今日收盘价更新持仓市值
    ├─ 3. 运行策略 → 生成目标权重
    ├─ 4. 计算调仓差量（目标 vs 当前）
    ├─ 5. 风控检查 → 生成合规订单
    ├─ 6. 提交订单（明日开盘成交）
    └─ 7. 打印账户快照

切换实盘：将 PaperBroker 替换为 QMTBroker，其余代码不变。
"""
from __future__ import annotations

import logging
import math
from typing import Dict, Optional

import pandas as pd

from data.common.config import Config
from data.writers.clickhouse_writer import ClickHouseWriter
from execution.broker.paper_broker import PaperBroker
from execution.oms.order import Direction, Order
from execution.portfolio.position_manager import PositionManager
from execution.risk.pre_trade import PreTradeChecker

logger = logging.getLogger(__name__)

# ─── 路径配置 ──────────────────────────────────────────────────────────────────
STATE_DIR     = "execution/state"
PORTFOLIO_FILE = f"{STATE_DIR}/portfolio.json"
PENDING_FILE   = f"{STATE_DIR}/pending_orders.json"


class DailyRunner:
    """
    将策略信号转换为实际订单的每日调度器。

    使用方式：
        runner = DailyRunner(initial_capital=500_000)
        runner.run(trade_date="20260304")
    """

    def __init__(
        self,
        initial_capital: float = 500_000.0,
        broker=None,
    ):
        cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
        self._ch = ClickHouseWriter(
            host     = cfg.get("database.clickhouse.host", "localhost"),
            port     = int(cfg.get("database.clickhouse.port", 9000)),
            database = "quant",
            user     = cfg.get("database.clickhouse.user", "default"),
            password = cfg.get("database.clickhouse.password", ""),
        )
        self._ch.connect()

        self.portfolio = PositionManager(PORTFOLIO_FILE, initial_capital)
        self.broker    = broker or PaperBroker(PENDING_FILE)
        self.risk      = PreTradeChecker()

    # ─── 主入口 ────────────────────────────────────────────────────────────────

    def run(self, trade_date: Optional[str] = None):
        """
        trade_date: 'YYYYMMDD'，默认为今日
        """
        if trade_date is None:
            trade_date = pd.Timestamp.today().strftime("%Y%m%d")

        logger.info("=" * 60)
        logger.info("日度执行开始 [%s]", trade_date)
        logger.info("=" * 60)

        # 1. 成交昨日挂起订单（用今日开盘价）
        price_data = self._load_price_data(trade_date)
        if not price_data:
            logger.warning("[%s] 无价格数据，跳过（非交易日或数据未更新）", trade_date)
            return

        self._fill_pending(trade_date, price_data)

        # 2. 更新持仓市值
        close_prices = {ts: info["close"] for ts, info in price_data.items() if info["close"] > 0}
        self.portfolio.update_prices(close_prices, trade_date)

        # 3. 风控：组合回撤检查（告警，不自动清仓）
        self.risk.check_portfolio_drawdown(self.portfolio)

        # 4. 生成目标权重（调用策略）
        target_weights = self._get_target_weights(trade_date)
        if target_weights is None:
            logger.info("[%s] 非调仓日，仅更新净值", trade_date)
            self.portfolio.save()
            self.portfolio.print_summary()
            return

        logger.info("目标持仓: %s", list(target_weights.keys()))

        # 5. 计算调仓差量
        orders = self._compute_orders(target_weights, price_data, trade_date)

        # 6. 风控检查 + 提交订单
        accepted = rejected = 0
        for order in orders:
            ok, reason = self.risk.check_all(order, self.portfolio, price_data)
            if ok:
                order.signal_date = trade_date
                self.broker.submit_order(order)
                accepted += 1
            else:
                from execution.oms.order import OrderStatus
                order.status       = OrderStatus.REJECTED
                order.reject_reason = reason
                rejected += 1

        logger.info("订单提交完成: %d 通过, %d 被风控拒绝", accepted, rejected)

        # 7. 保存状态 + 打印快照
        self.portfolio.save()
        self.portfolio.print_summary()

    # ─── 策略调用 ──────────────────────────────────────────────────────────────

    def _get_target_weights(self, trade_date: str) -> Optional[Dict[str, float]]:
        """
        调用反转策略，返回目标权重字典。
        非调仓日（持仓无变化）返回 None。
        """
        from strategy.examples.reversal_value_strategy import (
            TOP_N, REBAL_FREQ, MIN_AMOUNT,
            build_universe, calc_factors, generate_weights,
        )

        logger.info("运行策略计算目标权重...")

        # 加载足够长的历史数据（MA60 需要 60 根 + 预热，取 150 日）
        close, amount = self._load_strategy_data(trade_date, lookback=150)

        if close.empty:
            logger.warning("close 数据为空，跳过")
            return None

        # build_universe 需要 exclude 集合；Runner 中传入空集，依赖流动性/次新过滤
        exclude = self._load_exclude_list()
        universe = build_universe(close, amount, exclude)
        signal   = calc_factors(close, universe)

        weights_df = generate_weights(signal)
        last_weights = weights_df.iloc[-1]
        last_weights = last_weights[last_weights > 0]

        if last_weights.empty:
            logger.warning("策略无持仓信号")
            return {}

        # 非调仓日：与前一日持仓完全相同 → 跳过下单
        if len(weights_df) >= 2:
            prev_held = set(weights_df.iloc[-2].pipe(lambda s: s[s > 0]).index)
            curr_held = set(last_weights.index)
            if curr_held == prev_held:
                logger.info("持仓无变化，非调仓日，跳过下单")
                return None

        return last_weights.to_dict()

    def _load_strategy_data(
        self, end_date: str, lookback: int = 150
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """从 ClickHouse 加载策略所需的价格数据（最近 lookback 个交易日）"""
        import gc, numpy as np
        try:
            rows = self._ch._client.execute("""
                SELECT ts_code, trade_date,
                       argMax(adj_close, trade_date) AS adj_close,
                       argMax(amount,    trade_date) AS amount
                FROM stock_daily
                WHERE trade_date IN (
                    SELECT DISTINCT trade_date FROM stock_daily
                    WHERE trade_date <= %(end)s AND is_suspended = 0
                    ORDER BY trade_date DESC LIMIT %(n)s
                )
                AND is_suspended = 0
                GROUP BY ts_code, trade_date
                ORDER BY trade_date, ts_code
            """, {"end": end_date, "n": lookback})
        except Exception as e:
            logger.error("策略数据加载失败: %s", e)
            return pd.DataFrame(), pd.DataFrame()

        if not rows:
            return pd.DataFrame(), pd.DataFrame()

        df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_close", "amount"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df[["adj_close", "amount"]] = df[["adj_close", "amount"]].astype(np.float32)
        close  = df.pivot(index="trade_date", columns="ts_code", values="adj_close")
        amount = df.pivot(index="trade_date", columns="ts_code", values="amount")
        del df; gc.collect()
        logger.info("策略数据: %d 交易日 × %d 只股票", *close.shape)
        return close, amount

    def _load_exclude_list(self) -> set:
        """加载 ST / 退市股票，失败时返回空集（靠流动性过滤兜底）"""
        try:
            from data.writers.postgres_writer import PostgresWriter
            from data.common.config import Config
            cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
            pg = PostgresWriter(
                host     = cfg.get("database.postgres.host", "localhost"),
                port     = int(cfg.get("database.postgres.port", 5432)),
                database = "quant",
                user     = cfg.get("database.postgres.user", "postgres"),
                password = cfg.get("database.postgres.password", ""),
            )
            pg.connect()
            rows = pg.execute_query(
                "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
            )
            return {r[0] for r in rows}
        except Exception as e:
            logger.warning("ST 列表加载失败（使用空集）: %s", e)
            return set()

    # ─── 调仓差量计算 ──────────────────────────────────────────────────────────

    def _compute_orders(
        self,
        target_weights: Dict[str, float],
        price_data: Dict[str, dict],
        signal_date: str,
    ) -> list[Order]:
        """
        目标权重 → 买卖订单列表。
        逻辑：
          - 不在目标中但当前持有 → 全部卖出
          - 在目标中但未持有     → 买入
          - 权重差异 > 2%        → 调整（先卖超额，再买不足）
        """
        orders = []
        total_value = self.portfolio.get_total_value()
        current_positions = self.portfolio.get_all_positions()

        # 需要卖出的股票
        for ts_code, pos in current_positions.items():
            target_w = target_weights.get(ts_code, 0.0)
            current_w = pos.market_value / total_value

            if target_w == 0.0:  # 不在目标里，全部卖出
                orders.append(Order(
                    ts_code       = ts_code,
                    direction     = Direction.SELL,
                    target_amount = 0.0,
                    target_shares = pos.shares,
                ))

            elif current_w - target_w > 0.02:  # 超配超过 2%，减仓
                excess_value  = (current_w - target_w) * total_value
                info          = price_data.get(ts_code, {})
                price         = info.get("close", pos.last_price)
                shares_to_sell = self._to_round_lot(excess_value / price)
                if shares_to_sell > 0:
                    orders.append(Order(
                        ts_code       = ts_code,
                        direction     = Direction.SELL,
                        target_amount = 0.0,
                        target_shares = shares_to_sell,
                    ))

        # 需要买入的股票
        for ts_code, target_w in target_weights.items():
            info = price_data.get(ts_code, {})
            price = info.get("close", 0.0)
            if price <= 0:
                continue

            current_pos = current_positions.get(ts_code)
            current_w   = (current_pos.market_value / total_value) if current_pos else 0.0

            if target_w - current_w > 0.02:  # 欠配超过 2%，加仓
                buy_amount = (target_w - current_w) * total_value
                shares     = self._to_round_lot(buy_amount / price)
                if shares >= 100:
                    orders.append(Order(
                        ts_code       = ts_code,
                        direction     = Direction.BUY,
                        target_amount = shares * price,
                        target_shares = shares,
                    ))

        logger.info("调仓差量: %d 笔订单（卖 %d, 买 %d）",
                    len(orders),
                    sum(1 for o in orders if o.direction == Direction.SELL),
                    sum(1 for o in orders if o.direction == Direction.BUY))
        return orders

    # ─── 工具方法 ──────────────────────────────────────────────────────────────

    def _fill_pending(self, trade_date: str, price_data: Dict[str, dict]):
        """成交昨日挂起订单，并将成交结果写入持仓"""
        filled = self.broker.fill_pending_orders(trade_date, price_data)
        for order in filled:
            if order.direction == Direction.BUY:
                self.portfolio.apply_buy(
                    order.ts_code, order.filled_shares,
                    order.avg_fill_price, order.commission, trade_date
                )
            else:
                self.portfolio.apply_sell(
                    order.ts_code, order.filled_shares,
                    order.avg_fill_price, order.commission, trade_date
                )
        if filled:
            self.portfolio.save()

    def _load_price_data(self, trade_date: str) -> Dict[str, dict]:
        """从 ClickHouse 加载指定日期的股票价格数据"""
        try:
            rows = self._ch._client.execute("""
                SELECT
                    ts_code,
                    argMax(open,  trade_date) AS open,
                    argMax(close, trade_date) AS close,
                    argMax(pct_chg, trade_date) AS pct_chg,
                    argMax(is_suspended, trade_date) AS is_suspended
                FROM stock_daily
                WHERE trade_date = %(date)s
                GROUP BY ts_code
            """, {"date": trade_date})
        except Exception as e:
            logger.error("价格数据加载失败: %s", e)
            return {}

        return {
            row[0]: {
                "open":         row[1],
                "close":        row[2],
                "pct_chg":      row[3],
                "is_suspended": row[4],
            }
            for row in rows
        }

    @staticmethod
    def _to_round_lot(shares: float) -> int:
        """向下取整到 100 股整数倍"""
        return int(math.floor(shares / 100)) * 100


# ─── 命令行入口 ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%H:%M:%S",
    )

    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    runner   = DailyRunner(initial_capital=500_000.0)
    runner.run(trade_date=date_arg)
