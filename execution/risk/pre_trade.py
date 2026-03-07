"""交易前风控检查（第一道防线）"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from execution.oms.order import Order
    from execution.portfolio.position_manager import PositionManager

logger = logging.getLogger(__name__)

# ─── 风控参数 ──────────────────────────────────────────────────────────────────
MAX_SINGLE_POSITION_WEIGHT = 0.12   # 单只股票最大仓位占总资产比例（12%）
MAX_TOTAL_POSITION_WEIGHT  = 0.95   # 最大总仓位（留 5% 现金垫底）
MAX_DRAWDOWN_STOP          = 0.15   # 组合最大回撤止损线（15%触发人工暂停警报）
LIMIT_THRESHOLD            = 9.8    # 涨跌停判断阈值（A股 ±10%，此处用 9.8% 稳妥）


class PreTradeChecker:
    """
    交易前风控，对每笔订单做合规校验。
    通过则返回 (True, None)；拒绝则返回 (False, reason)。
    """

    def __init__(self, ch_client=None):
        """
        ch_client: ClickHouse 连接，用于查询实时涨跌停/停牌状态。
                   纸面交易模式传入 None 时，从 price_data 字典里查。
        """
        self._ch = ch_client

    def check_all(
        self,
        order: "Order",
        portfolio: "PositionManager",
        price_data: Dict[str, dict],  # {ts_code: {open, close, pct_chg, is_suspended}}
    ) -> tuple[bool, Optional[str]]:
        """
        执行全部检查，返回 (passed, reject_reason)。

        注意：仓位上限和现金充足性不在此检查 — 这两项在调仓日提交时会因
        "旧仓未平"而误报。真正的现金/仓位保护在 PositionManager.apply_buy()
        中执行（到成交日旧仓已被填平，现金已回笼）。
        """
        checks = [
            self._check_suspended,
            self._check_limit_up_down,
        ]
        for fn in checks:
            ok, reason = fn(order, portfolio, price_data)
            if not ok:
                logger.warning("风控拒绝 %s: %s", order, reason)
                return False, reason
        return True, None

    # ─── 单项检查 ──────────────────────────────────────────────────────────────

    def _check_suspended(self, order, portfolio, price_data):
        info = price_data.get(order.ts_code, {})
        if info.get("is_suspended", 0) == 1:
            return False, "suspended"
        return True, None

    def _check_limit_up_down(self, order, portfolio, price_data):
        from execution.oms.order import Direction
        info = price_data.get(order.ts_code, {})
        pct = info.get("pct_chg", 0.0)

        if order.direction == Direction.BUY and pct >= LIMIT_THRESHOLD:
            return False, "limit_up"   # 涨停无法买入

        if order.direction == Direction.SELL and pct <= -LIMIT_THRESHOLD:
            return False, "limit_down"  # 跌停无法卖出

        return True, None

    def _check_position_limit(self, order, portfolio, price_data):
        from execution.oms.order import Direction
        if order.direction == Direction.SELL:
            return True, None  # 卖出不检查仓位上限

        total_value = portfolio.get_total_value()
        current_weight = portfolio.get_position_weight(order.ts_code)
        new_value = order.target_amount + (
            portfolio.get_position(order.ts_code).market_value
            if portfolio.get_position(order.ts_code) else 0.0
        )
        new_weight = new_value / total_value if total_value > 0 else 0

        if new_weight > MAX_SINGLE_POSITION_WEIGHT:
            return False, f"position_limit({new_weight:.1%}>{MAX_SINGLE_POSITION_WEIGHT:.0%})"

        # 总仓位检查
        total_mktval = portfolio.state.total_market_value + order.target_amount
        if total_mktval / total_value > MAX_TOTAL_POSITION_WEIGHT:
            return False, f"total_position_limit"

        return True, None

    def _check_cash_sufficient(self, order, portfolio, price_data):
        from execution.oms.order import Direction
        if order.direction == Direction.SELL:
            return True, None
        if portfolio.get_cash() < order.target_amount:
            return False, f"insufficient_cash({portfolio.get_cash():.0f}<{order.target_amount:.0f})"
        return True, None

    # ─── 组合级风控（每日检查，不阻塞单笔订单） ───────────────────────────────

    def check_portfolio_drawdown(self, portfolio: "PositionManager") -> bool:
        """
        检查是否触发最大回撤止损线。
        返回 True = 正常；False = 需要人工介入（不自动清仓，只告警）。
        """
        nav_history = portfolio.state.nav_history
        if len(nav_history) < 2:
            return True

        navs = [r["nav"] for r in nav_history]
        peak = max(navs)
        current = navs[-1]
        drawdown = (peak - current) / peak

        if drawdown >= MAX_DRAWDOWN_STOP:
            logger.critical(
                "⚠️  最大回撤警报: %.1f%% >= %.0f%% 止损线！请人工检查是否暂停策略。",
                drawdown * 100, MAX_DRAWDOWN_STOP * 100
            )
            return False

        return True
