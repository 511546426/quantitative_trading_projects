"""
事件驱动回测引擎。

结构与实盘引擎完全一致，便于 "一键切换" 到实盘。
精度优先: 逐日推送 Bar, 模拟订单撮合, 精确跟踪仓位和资金。

事件流:
    BarEvent → Strategy.on_bar() → SignalEvent
    → Portfolio.on_signal() → OrderEvent
    → ExecutionHandler.execute() → FillEvent
    → Portfolio.on_fill() → 更新持仓
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

from strategy.backtest.metrics import BacktestResult

logger = logging.getLogger(__name__)


# ================================================================
# 事件定义
# ================================================================

class EventType(Enum):
    BAR = "bar"
    SIGNAL = "signal"
    ORDER = "order"
    FILL = "fill"


@dataclass
class Event:
    event_type: EventType
    timestamp: Any = None


@dataclass
class BarEvent(Event):
    """市场数据事件 (一根 Bar)"""
    data: dict = field(default_factory=dict)

    def __post_init__(self):
        self.event_type = EventType.BAR


@dataclass
class SignalEvent(Event):
    """策略信号事件"""
    ts_code: str = ""
    direction: str = ""   # "BUY" / "SELL"
    strength: float = 1.0
    strategy_id: str = ""

    def __post_init__(self):
        self.event_type = EventType.SIGNAL


@dataclass
class OrderEvent(Event):
    """订单事件"""
    ts_code: str = ""
    direction: str = ""
    quantity: int = 0
    order_type: str = "MARKET"
    limit_price: float = 0.0

    def __post_init__(self):
        self.event_type = EventType.ORDER


@dataclass
class FillEvent(Event):
    """成交事件"""
    ts_code: str = ""
    direction: str = ""
    quantity: int = 0
    fill_price: float = 0.0
    commission: float = 0.0

    def __post_init__(self):
        self.event_type = EventType.FILL


# ================================================================
# 数据处理器
# ================================================================

class HistoricalDataHandler:
    """
    历史数据推送器。

    逐日推送全市场 Bar 数据, 模拟实时数据流。
    """

    def __init__(
        self,
        price_data: pd.DataFrame,
        volume_data: pd.DataFrame | None = None,
    ):
        """
        Parameters
        ----------
        price_data : DataFrame
            长格式数据, 包含 ts_code, trade_date, open, high, low, close, volume, amount 等列。
            或 pivot 格式 (index=date, columns=ts_code) 的 close 价格。
        """
        self._is_pivot = "ts_code" not in price_data.columns
        if self._is_pivot:
            self.dates = list(price_data.index)
            self.close_pivot = price_data
            self.volume_pivot = volume_data
        else:
            self.dates = sorted(price_data["trade_date"].unique())
            self._long_data = price_data.set_index("trade_date")

        self._index = 0

    def reset(self) -> None:
        self._index = 0

    @property
    def current_date(self) -> Any:
        if self._index < len(self.dates):
            return self.dates[self._index]
        return None

    def get_next_bar(self) -> BarEvent | None:
        """获取下一个 Bar 事件"""
        if self._index >= len(self.dates):
            return None

        dt = self.dates[self._index]
        self._index += 1

        if self._is_pivot:
            bar_data = {
                "trade_date": dt,
                "close": self.close_pivot.loc[dt].dropna().to_dict(),
            }
            if self.volume_pivot is not None:
                bar_data["volume"] = self.volume_pivot.loc[dt].dropna().to_dict()
        else:
            rows = self._long_data.loc[dt] if dt in self._long_data.index else pd.DataFrame()
            bar_data = {"trade_date": dt, "bars": rows}

        return BarEvent(timestamp=dt, data=bar_data)

    def get_history(self, n_bars: int) -> pd.DataFrame | None:
        """获取最近 N 根 Bar 的历史数据 (pivot close)"""
        if not self._is_pivot:
            return None
        end = self._index
        start = max(0, end - n_bars)
        return self.close_pivot.iloc[start:end]


# ================================================================
# 策略基类
# ================================================================

class BaseStrategy(ABC):
    """策略抽象基类"""

    strategy_id: str = "base"

    @abstractmethod
    def on_bar(
        self, event: BarEvent, data_handler: HistoricalDataHandler, portfolio: "Portfolio"
    ) -> list[SignalEvent]:
        """
        处理 Bar 事件, 返回信号列表。

        Parameters
        ----------
        event : BarEvent
            当前 Bar 数据。
        data_handler : HistoricalDataHandler
            可访问历史数据。
        portfolio : Portfolio
            当前持仓状态。
        """
        ...


# ================================================================
# 组合管理
# ================================================================

@dataclass
class Position:
    """单只股票的持仓"""
    ts_code: str
    quantity: int = 0
    avg_cost: float = 0.0
    current_price: float = 0.0

    @property
    def market_value(self) -> float:
        return self.quantity * self.current_price

    @property
    def unrealized_pnl(self) -> float:
        return self.quantity * (self.current_price - self.avg_cost)


class Portfolio:
    """组合管理器"""

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_position_pct: float = 0.10,
        max_total_pct: float = 0.80,
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_position_pct = max_position_pct
        self.max_total_pct = max_total_pct
        self.positions: dict[str, Position] = {}
        self.nav_history: list[tuple[Any, float]] = []
        self._trade_log: list[dict] = []

    @property
    def total_market_value(self) -> float:
        return sum(p.market_value for p in self.positions.values())

    @property
    def nav(self) -> float:
        return self.cash + self.total_market_value

    @property
    def position_count(self) -> int:
        return sum(1 for p in self.positions.values() if p.quantity > 0)

    def update_prices(self, prices: dict[str, float]) -> None:
        """更新持仓的当前价格"""
        for code, pos in self.positions.items():
            if code in prices:
                pos.current_price = prices[code]

    def on_signal(self, signal: SignalEvent) -> OrderEvent | None:
        """将信号转化为订单"""
        if signal.direction == "BUY":
            return self._generate_buy_order(signal)
        elif signal.direction == "SELL":
            return self._generate_sell_order(signal)
        return None

    def on_fill(self, fill: FillEvent) -> None:
        """处理成交事件, 更新持仓和现金"""
        code = fill.ts_code
        if fill.direction == "BUY":
            cost = fill.quantity * fill.fill_price + fill.commission
            if code not in self.positions:
                self.positions[code] = Position(ts_code=code)
            pos = self.positions[code]
            total_cost = pos.avg_cost * pos.quantity + fill.fill_price * fill.quantity
            pos.quantity += fill.quantity
            pos.avg_cost = total_cost / pos.quantity if pos.quantity > 0 else 0
            pos.current_price = fill.fill_price
            self.cash -= cost
        elif fill.direction == "SELL":
            revenue = fill.quantity * fill.fill_price - fill.commission
            if code in self.positions:
                self.positions[code].quantity -= fill.quantity
                if self.positions[code].quantity <= 0:
                    del self.positions[code]
            self.cash += revenue

        self._trade_log.append({
            "timestamp": fill.timestamp,
            "ts_code": fill.ts_code,
            "direction": fill.direction,
            "quantity": fill.quantity,
            "price": fill.fill_price,
            "commission": fill.commission,
        })

    def record_nav(self, timestamp: Any) -> None:
        self.nav_history.append((timestamp, self.nav))

    def _generate_buy_order(self, signal: SignalEvent) -> OrderEvent | None:
        max_per_stock = self.initial_capital * self.max_position_pct
        max_total = self.initial_capital * self.max_total_pct

        if self.total_market_value >= max_total:
            return None

        available = min(self.cash * 0.95, max_per_stock)
        if available < 1000:
            return None

        return OrderEvent(
            timestamp=signal.timestamp,
            ts_code=signal.ts_code,
            direction="BUY",
            quantity=int(available),
            order_type="MARKET",
        )

    def _generate_sell_order(self, signal: SignalEvent) -> OrderEvent | None:
        if signal.ts_code not in self.positions:
            return None
        pos = self.positions[signal.ts_code]
        if pos.quantity <= 0:
            return None
        return OrderEvent(
            timestamp=signal.timestamp,
            ts_code=signal.ts_code,
            direction="SELL",
            quantity=pos.quantity,
            order_type="MARKET",
        )


# ================================================================
# 模拟执行
# ================================================================

class SimulatedExecutionHandler:
    """模拟撮合器"""

    def __init__(
        self,
        commission_rate: float = 0.00025,
        stamp_duty_rate: float = 0.001,
        slippage_rate: float = 0.0005,
    ):
        self.commission_rate = commission_rate
        self.stamp_duty_rate = stamp_duty_rate
        self.slippage_rate = slippage_rate

    def execute(
        self, order: OrderEvent, current_prices: dict[str, float]
    ) -> FillEvent | None:
        """模拟撮合"""
        if order.ts_code not in current_prices:
            return None

        price = current_prices[order.ts_code]

        if order.direction == "BUY":
            fill_price = price * (1 + self.slippage_rate)
            quantity = int(order.quantity / fill_price / 100) * 100
            if quantity < 100:
                return None
            commission = quantity * fill_price * self.commission_rate
        else:
            fill_price = price * (1 - self.slippage_rate)
            quantity = order.quantity
            commission = (
                quantity * fill_price * self.commission_rate
                + quantity * fill_price * self.stamp_duty_rate
            )

        return FillEvent(
            timestamp=order.timestamp,
            ts_code=order.ts_code,
            direction=order.direction,
            quantity=quantity,
            fill_price=fill_price,
            commission=commission,
        )


# ================================================================
# 回测引擎
# ================================================================

class EventDrivenBacktester:
    """
    事件驱动回测引擎。

    Parameters
    ----------
    data_handler : HistoricalDataHandler
        历史数据推送器。
    strategy : BaseStrategy
        策略实例。
    initial_capital : float
        初始资金。
    """

    def __init__(
        self,
        data_handler: HistoricalDataHandler,
        strategy: BaseStrategy,
        initial_capital: float = 1_000_000.0,
        max_position_pct: float = 0.10,
    ):
        self.data_handler = data_handler
        self.strategy = strategy
        self.portfolio = Portfolio(
            initial_capital=initial_capital,
            max_position_pct=max_position_pct,
        )
        self.execution = SimulatedExecutionHandler()
        self.event_queue: deque[Event] = deque()

    def run(self) -> BacktestResult:
        """运行回测"""
        self.data_handler.reset()
        bar_count = 0

        while True:
            bar_event = self.data_handler.get_next_bar()
            if bar_event is None:
                break

            bar_count += 1
            current_prices = bar_event.data.get("close", {})
            self.portfolio.update_prices(current_prices)

            signals = self.strategy.on_bar(
                bar_event, self.data_handler, self.portfolio
            )

            for signal in (signals or []):
                order = self.portfolio.on_signal(signal)
                if order is None:
                    continue
                fill = self.execution.execute(order, current_prices)
                if fill is not None:
                    self.portfolio.on_fill(fill)

            self.portfolio.record_nav(bar_event.timestamp)

        nav_series = pd.Series(
            [nav for _, nav in self.portfolio.nav_history],
            index=[dt for dt, _ in self.portfolio.nav_history],
        )
        daily_returns = nav_series.pct_change().dropna()

        logger.info(
            "事件驱动回测完成: %d Bar, 最终 NAV=%.2f, 交易 %d 笔",
            bar_count,
            self.portfolio.nav,
            len(self.portfolio._trade_log),
        )

        return BacktestResult(
            net_returns=daily_returns,
            trades=self.portfolio._trade_log,
            metadata={
                "engine": "event_driven",
                "initial_capital": self.portfolio.initial_capital,
                "final_nav": self.portfolio.nav,
                "n_trades": len(self.portfolio._trade_log),
                "n_bars": bar_count,
            },
        )
