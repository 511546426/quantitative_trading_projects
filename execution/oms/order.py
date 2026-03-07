"""订单数据结构与状态机"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class Direction(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    NEW       = "new"        # 刚创建
    SUBMITTED = "submitted"  # 已提交待成交
    FILLED    = "filled"     # 全部成交
    PARTIAL   = "partial"    # 部分成交
    CANCELLED = "cancelled"  # 已撤销
    REJECTED  = "rejected"   # 被风控拒绝


class RejectReason(str, Enum):
    LIMIT_UP       = "limit_up"        # 涨停，买入被拒
    LIMIT_DOWN     = "limit_down"      # 跌停，卖出被拒
    SUSPENDED      = "suspended"       # 停牌
    POSITION_LIMIT = "position_limit"  # 超过单只仓位上限
    NO_POSITION    = "no_position"     # 卖出时无持仓
    INSUFFICIENT_CASH = "insufficient_cash"


@dataclass
class Order:
    ts_code:       str
    direction:     Direction
    target_amount: float           # 目标买入金额（元）；卖出时为 0（全仓卖出）
    target_shares: int             # 目标股数（100股整数倍）

    order_id:      str             = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status:        OrderStatus     = OrderStatus.NEW
    reject_reason: Optional[str]   = None

    # 成交信息（由 broker 填写）
    filled_shares: int             = 0
    avg_fill_price: float          = 0.0
    filled_amount:  float          = 0.0
    commission:     float          = 0.0

    # 时间戳
    created_at:    datetime        = field(default_factory=datetime.now)
    filled_at:     Optional[datetime] = None

    # 来源（信号日期，用于审计）
    signal_date:   str             = ""
    fill_date:     str             = ""

    def __repr__(self) -> str:
        return (
            f"Order({self.order_id} {self.direction.value} {self.ts_code} "
            f"{self.target_shares}股 [{self.status.value}])"
        )

    def to_dict(self) -> dict:
        return {
            "order_id":       self.order_id,
            "ts_code":        self.ts_code,
            "direction":      self.direction.value,
            "target_amount":  self.target_amount,
            "target_shares":  self.target_shares,
            "status":         self.status.value,
            "reject_reason":  self.reject_reason,
            "filled_shares":  self.filled_shares,
            "avg_fill_price": self.avg_fill_price,
            "filled_amount":  self.filled_amount,
            "commission":     self.commission,
            "created_at":     self.created_at.isoformat(),
            "filled_at":      self.filled_at.isoformat() if self.filled_at else None,
            "signal_date":    self.signal_date,
            "fill_date":      self.fill_date,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Order":
        o = cls(
            ts_code       = d["ts_code"],
            direction     = Direction(d["direction"]),
            target_amount = d["target_amount"],
            target_shares = d["target_shares"],
        )
        o.order_id       = d["order_id"]
        o.status         = OrderStatus(d["status"])
        o.reject_reason  = d.get("reject_reason")
        o.filled_shares  = d.get("filled_shares", 0)
        o.avg_fill_price = d.get("avg_fill_price", 0.0)
        o.filled_amount  = d.get("filled_amount", 0.0)
        o.commission     = d.get("commission", 0.0)
        o.created_at     = datetime.fromisoformat(d["created_at"])
        o.filled_at      = datetime.fromisoformat(d["filled_at"]) if d.get("filled_at") else None
        o.signal_date    = d.get("signal_date", "")
        o.fill_date      = d.get("fill_date", "")
        return o
