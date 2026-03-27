"""
TWAP 执行算法

将大订单按时间均匀拆分成 N 小笔，每隔 interval_sec 执行一笔。
可选：根据实时行情微调每笔价格（追涨杀跌保护）。
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict

logger = logging.getLogger(__name__)


@dataclass
class TWAPTask:
    order_id: int
    symbol: str
    side: str
    total_qty: int
    price: float
    slices: int
    interval_sec: int
    strategy_id: str

    executed_qty: int = 0
    executed_slices: int = 0
    cancelled: bool = False


class TWAPScheduler:
    """
    管理多个 TWAP 任务，每个任务在独立线程中按时间间隔执行。

    Parameters
    ----------
    execute_fn : callable
        单笔下单函数，签名：
        execute_fn(order_id, symbol, side, qty, price, strategy_id)
    """

    def __init__(self, execute_fn: Callable):
        self._execute_fn = execute_fn
        self._tasks: Dict[int, TWAPTask] = {}
        self._lock = threading.Lock()

    def schedule(
        self,
        order_id: int,
        symbol: str,
        side: str,
        total_qty: int,
        price: float,
        slices: int = 5,
        interval_sec: int = 60,
        strategy_id: str = "",
    ):
        """创建并启动一个 TWAP 任务"""
        task = TWAPTask(
            order_id=order_id,
            symbol=symbol,
            side=side,
            total_qty=total_qty,
            price=price,
            slices=max(slices, 1),
            interval_sec=max(interval_sec, 1),
            strategy_id=strategy_id,
        )

        with self._lock:
            self._tasks[order_id] = task

        t = threading.Thread(target=self._run_task, args=(task,), daemon=True)
        t.start()
        logger.info("TWAP scheduled: %s %s %d shares / %d slices / %ds interval",
                     symbol, side, total_qty, slices, interval_sec)

    def cancel(self, order_id: int):
        with self._lock:
            task = self._tasks.get(order_id)
            if task:
                task.cancelled = True

    def cancel_all(self):
        with self._lock:
            for task in self._tasks.values():
                task.cancelled = True

    def _run_task(self, task: TWAPTask):
        """在独立线程中执行 TWAP 拆单"""
        per_slice = task.total_qty // task.slices
        # A股100股整数倍
        per_slice = (per_slice // 100) * 100
        if per_slice < 100:
            per_slice = 100

        remaining = task.total_qty

        for i in range(task.slices):
            if task.cancelled:
                logger.info("TWAP cancelled: %s (executed %d/%d)",
                            task.symbol, task.executed_qty, task.total_qty)
                break

            # 最后一笔把剩余全部执行
            if i == task.slices - 1:
                qty = remaining
            else:
                qty = min(per_slice, remaining)

            qty = (qty // 100) * 100
            if qty <= 0:
                break

            try:
                self._execute_fn(
                    order_id=task.order_id,
                    symbol=task.symbol,
                    side=task.side,
                    qty=qty,
                    price=task.price,
                    strategy_id=task.strategy_id,
                )
                task.executed_qty += qty
                task.executed_slices += 1
                remaining -= qty

                logger.info("TWAP slice %d/%d: %s %s %d (cumulative %d/%d)",
                            i + 1, task.slices, task.symbol, task.side,
                            qty, task.executed_qty, task.total_qty)
            except Exception as e:
                logger.error("TWAP slice failed: %s", e)

            if i < task.slices - 1 and not task.cancelled:
                time.sleep(task.interval_sec)

        with self._lock:
            self._tasks.pop(task.order_id, None)

        logger.info("TWAP completed: %s %s executed %d/%d in %d slices",
                     task.symbol, task.side, task.executed_qty,
                     task.total_qty, task.executed_slices)
