"""Broker 抽象基类：定义券商接口契约"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from execution.oms.order import Order


class BaseBroker(ABC):
    """
    所有 Broker 实现（PaperBroker / QMTBroker）必须继承此类。
    上层代码只依赖这个接口，切换实盘时只需替换 Broker 实例。
    """

    @abstractmethod
    def submit_order(self, order: Order) -> bool:
        """提交订单。返回 True = 成功入队，False = 提交失败。"""

    @abstractmethod
    def fill_pending_orders(self, trade_date: str, price_data: Dict[str, dict]) -> List[Order]:
        """
        用指定交易日的价格撮合所有挂起订单。
        price_data: {ts_code: {open, close, pct_chg, is_suspended}}
        返回：本次成交的订单列表
        """

    @abstractmethod
    def get_pending_orders(self) -> List[Order]:
        """返回所有待成交订单"""

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """撤销指定订单"""
