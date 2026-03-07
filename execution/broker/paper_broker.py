"""
纸面交易 Broker

执行逻辑：
  T 日收盘后  → submit_order()：将订单放入待成交队列
  T+1 日开盘 → fill_pending_orders()：以 T+1 开盘价模拟成交，扣除手续费

成本模型（A 股）：
  买入：佣金 2.5bps + 冲击 5bps = 7.5bps（万 0.75）
  卖出：佣金 2.5bps + 印花税 10bps + 冲击 5bps = 17.5bps（万 1.75）
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from execution.broker.base import BaseBroker
from execution.oms.order import Direction, Order, OrderStatus

logger = logging.getLogger(__name__)

BUY_COST_BPS  = 7.5
SELL_COST_BPS = 17.5


class PaperBroker(BaseBroker):

    def __init__(self, pending_file: str):
        """
        pending_file: 持久化待成交订单的 JSON 文件路径（断电重启后可恢复）
        """
        self._pending_file = Path(pending_file)
        self._pending_file.parent.mkdir(parents=True, exist_ok=True)
        self._pending: List[Order] = self._load_pending()

    # ─── 接口实现 ─────────────────────────────────────────────────────────────

    def submit_order(self, order: Order) -> bool:
        order.status = OrderStatus.SUBMITTED
        self._pending.append(order)
        self._save_pending()
        logger.info("订单入队: %s", order)
        return True

    def fill_pending_orders(
        self, trade_date: str, price_data: Dict[str, dict]
    ) -> List[Order]:
        """
        用 trade_date 这天的开盘价撮合所有挂起订单。
        price_data: {ts_code: {open, close, high, low, pct_chg, is_suspended}}
        """
        filled: List[Order] = []
        remaining: List[Order] = []

        for order in self._pending:
            info = price_data.get(order.ts_code)

            if info is None:
                logger.warning("无价格数据 %s，订单保留到下一交易日", order.ts_code)
                remaining.append(order)
                continue

            if info.get("is_suspended", 0) == 1:
                logger.warning("%s 停牌，订单保留", order.ts_code)
                remaining.append(order)
                continue

            fill_price = info.get("open", info.get("close", 0.0))
            if fill_price <= 0:
                remaining.append(order)
                continue

            pct = info.get("pct_chg", 0.0)

            # 涨停无法买入，跌停无法卖出 → 保留到下一交易日
            if order.direction == Direction.BUY and pct >= 9.8:
                logger.warning("%s 涨停(%.1f%%)，买入订单顺延", order.ts_code, pct)
                remaining.append(order)
                continue

            if order.direction == Direction.SELL and pct <= -9.8:
                logger.warning("%s 跌停(%.1f%%)，卖出订单顺延", order.ts_code, pct)
                remaining.append(order)
                continue

            # 计算成交手数和手续费
            if order.direction == Direction.BUY:
                shares = order.target_shares
                commission = shares * fill_price * BUY_COST_BPS / 10000
            else:
                shares = order.target_shares
                commission = shares * fill_price * SELL_COST_BPS / 10000

            commission = max(commission, 5.0)  # 最低 5 元佣金

            order.filled_shares  = shares
            order.avg_fill_price = fill_price
            order.filled_amount  = shares * fill_price
            order.commission     = commission
            order.status         = OrderStatus.FILLED
            order.filled_at      = datetime.now()
            order.fill_date      = trade_date

            logger.info("成交: %s %s %d股 @%.2f 手续费%.2f",
                        trade_date, order, shares, fill_price, commission)
            filled.append(order)

        self._pending = remaining
        self._save_pending()
        return filled

    def get_pending_orders(self) -> List[Order]:
        return list(self._pending)

    def cancel_order(self, order_id: str) -> bool:
        for i, o in enumerate(self._pending):
            if o.order_id == order_id:
                o.status = OrderStatus.CANCELLED
                self._pending.pop(i)
                self._save_pending()
                logger.info("撤单: %s", order_id)
                return True
        return False

    # ─── 持久化 ───────────────────────────────────────────────────────────────

    def _save_pending(self):
        with open(self._pending_file, "w") as f:
            json.dump([o.to_dict() for o in self._pending], f,
                      ensure_ascii=False, indent=2)

    def _load_pending(self) -> List[Order]:
        if self._pending_file.exists():
            with open(self._pending_file) as f:
                data = json.load(f)
            orders = [Order.from_dict(d) for d in data]
            if orders:
                logger.info("恢复 %d 笔挂起订单", len(orders))
            return orders
        return []
