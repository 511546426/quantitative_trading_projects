"""持仓管理：记录持仓、现金、P&L，状态持久化到 JSON 文件"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Position:
    ts_code:     str
    shares:      int
    avg_cost:    float   # 平均持仓成本（元/股）
    last_price:  float   # 最新价格（每日更新）
    open_date:   str     # 首次建仓日期

    @property
    def market_value(self) -> float:
        return self.shares * self.last_price

    @property
    def cost_value(self) -> float:
        return self.shares * self.avg_cost

    @property
    def unrealized_pnl(self) -> float:
        return self.market_value - self.cost_value

    @property
    def unrealized_pnl_pct(self) -> float:
        if self.cost_value == 0:
            return 0.0
        return self.unrealized_pnl / self.cost_value

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        return cls(**d)


@dataclass
class PortfolioState:
    cash:            float                   # 可用现金
    initial_capital: float                   # 初始本金
    positions:       Dict[str, Position]     = field(default_factory=dict)
    trade_history:   List[dict]              = field(default_factory=list)
    nav_history:     List[dict]              = field(default_factory=list)  # 每日净值记录
    last_update:     str                     = ""

    @property
    def total_market_value(self) -> float:
        return sum(p.market_value for p in self.positions.values())

    @property
    def total_value(self) -> float:
        return self.cash + self.total_market_value

    @property
    def total_return_pct(self) -> float:
        if self.initial_capital == 0:
            return 0.0
        return (self.total_value - self.initial_capital) / self.initial_capital

    def to_dict(self) -> dict:
        return {
            "cash":            self.cash,
            "initial_capital": self.initial_capital,
            "positions":       {k: v.to_dict() for k, v in self.positions.items()},
            "trade_history":   self.trade_history,
            "nav_history":     self.nav_history,
            "last_update":     self.last_update,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PortfolioState":
        state = cls(
            cash            = d["cash"],
            initial_capital = d["initial_capital"],
            trade_history   = d.get("trade_history", []),
            nav_history     = d.get("nav_history", []),
            last_update     = d.get("last_update", ""),
        )
        state.positions = {
            k: Position.from_dict(v) for k, v in d.get("positions", {}).items()
        }
        return state


class PositionManager:
    """持仓管理器，负责：持仓更新、现金管理、P&L 计算、状态持久化"""

    def __init__(self, state_file: str, initial_capital: float = 500_000.0):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_or_init(initial_capital)

    # ─── 持久化 ───────────────────────────────────────────────────────────────

    def _load_or_init(self, initial_capital: float) -> PortfolioState:
        if self.state_file.exists():
            with open(self.state_file) as f:
                data = json.load(f)
            logger.info("加载持仓状态: %s", self.state_file)
            return PortfolioState.from_dict(data)
        logger.info("初始化新账户，本金 %.0f 元", initial_capital)
        return PortfolioState(cash=initial_capital, initial_capital=initial_capital)

    def save(self):
        import datetime
        def _default(o):
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
            raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

        with open(self.state_file, "w") as f:
            json.dump(self.state.to_dict(), f, ensure_ascii=False, indent=2, default=_default)

    # ─── 持仓操作 ─────────────────────────────────────────────────────────────

    def apply_buy(self, ts_code: str, shares: int, fill_price: float,
                  commission: float, fill_date: str):
        """买入成交后更新持仓和现金"""
        cost = shares * fill_price + commission
        if self.state.cash < cost:
            logger.warning("现金不足: 需要 %.0f，可用 %.0f", cost, self.state.cash)
            return

        self.state.cash -= cost

        if ts_code in self.state.positions:
            pos = self.state.positions[ts_code]
            total_shares = pos.shares + shares
            pos.avg_cost = (pos.cost_value + shares * fill_price) / total_shares
            pos.shares   = total_shares
            pos.last_price = fill_price
        else:
            self.state.positions[ts_code] = Position(
                ts_code    = ts_code,
                shares     = shares,
                avg_cost   = fill_price,
                last_price = fill_price,
                open_date  = fill_date,
            )

        self._record_trade(ts_code, "BUY", shares, fill_price, commission, fill_date)
        logger.info("买入 %s %d股 @%.2f，手续费 %.2f，现金剩余 %.0f",
                    ts_code, shares, fill_price, commission, self.state.cash)

    def apply_sell(self, ts_code: str, shares: int, fill_price: float,
                   commission: float, fill_date: str):
        """卖出成交后更新持仓和现金"""
        if ts_code not in self.state.positions:
            logger.warning("卖出失败：无 %s 持仓", ts_code)
            return

        pos = self.state.positions[ts_code]
        actual_shares = min(shares, pos.shares)
        proceeds = actual_shares * fill_price - commission

        self.state.cash += proceeds
        pos.shares -= actual_shares

        if pos.shares <= 0:
            del self.state.positions[ts_code]

        self._record_trade(ts_code, "SELL", actual_shares, fill_price, commission, fill_date)
        logger.info("卖出 %s %d股 @%.2f，手续费 %.2f，现金剩余 %.0f",
                    ts_code, actual_shares, fill_price, commission, self.state.cash)

    def update_prices(self, prices: Dict[str, float], date: str):
        """每日收盘后更新持仓市值"""
        for ts_code, pos in self.state.positions.items():
            if ts_code in prices:
                pos.last_price = prices[ts_code]

        nav = self.state.total_value
        self.state.nav_history.append({"date": date, "nav": nav,
                                        "cash": self.state.cash,
                                        "market_value": self.state.total_market_value})
        self.state.last_update = date
        logger.info("[%s] 账户净值 %.0f（持仓 %.0f + 现金 %.0f）, 总收益 %+.2f%%",
                    date, nav, self.state.total_market_value, self.state.cash,
                    self.state.total_return_pct * 100)

    # ─── 查询 ─────────────────────────────────────────────────────────────────

    def get_position(self, ts_code: str) -> Optional[Position]:
        return self.state.positions.get(ts_code)

    def get_all_positions(self) -> Dict[str, Position]:
        return self.state.positions

    def get_cash(self) -> float:
        return self.state.cash

    def get_total_value(self) -> float:
        return self.state.total_value

    def get_position_weight(self, ts_code: str) -> float:
        """当前某只股票占总资产的比例"""
        total = self.state.total_value
        if total == 0:
            return 0.0
        pos = self.state.positions.get(ts_code)
        if pos is None:
            return 0.0
        return pos.market_value / total

    def print_summary(self):
        s = self.state
        print(f"\n{'='*55}")
        print(f"{'账户快照':^55}")
        print(f"{'='*55}")
        print(f"  总资产:   ¥{s.total_value:>12,.0f}  ({s.total_return_pct:+.2%})")
        print(f"  持仓市值: ¥{s.total_market_value:>12,.0f}")
        print(f"  可用现金: ¥{s.cash:>12,.0f}  ({s.cash/s.total_value:.1%})")
        print(f"{'─'*55}")
        if s.positions:
            print(f"  {'代码':<12}{'股数':>8}{'成本':>10}{'现价':>10}{'盈亏':>10}{'占比':>8}")
            for ts_code, pos in sorted(s.positions.items()):
                print(f"  {ts_code:<12}{pos.shares:>8,}{pos.avg_cost:>10.2f}"
                      f"{pos.last_price:>10.2f}{pos.unrealized_pnl_pct:>+9.1%}"
                      f"{pos.market_value/s.total_value:>8.1%}")
        else:
            print("  （空仓）")
        print(f"{'='*55}\n")

    # ─── 内部 ─────────────────────────────────────────────────────────────────

    def _record_trade(self, ts_code: str, direction: str, shares: int,
                      price: float, commission: float, date: str):
        self.state.trade_history.append({
            "date":       date,
            "ts_code":    ts_code,
            "direction":  direction,
            "shares":     shares,
            "price":      price,
            "commission": commission,
            "amount":     shares * price,
        })
