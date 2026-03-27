"""
QMT 适配器 — 桥接 C++ 引擎与 QMT 券商

架构角色：
    C++ Engine  ──(ZMQ PUSH)──▶  QMT Adapter  ──(xtquant)──▶  券商
    C++ Engine  ◀──(ZMQ PUSH)──  QMT Adapter  ◀──(callback)──  券商

启动方式：
    python -m execution.adapter.qmt_adapter --config execution/config.yaml
"""
from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import zmq

logger = logging.getLogger(__name__)

def _find_repo_root() -> Path:
    """Walk upward from this file to find the repo root (contains engine/ dir)."""
    cur = Path(__file__).resolve().parent
    for _ in range(10):
        if (cur / "engine" / "python").is_dir():
            return cur
        cur = cur.parent
    raise FileNotFoundError("Cannot locate repo root (engine/python/ not found)")

def _load_pb2():
    """Load signal_pb2 from engine/python/, auto-generate if needed."""
    repo_root = _find_repo_root()
    pb_dir = repo_root / "engine" / "python"

    if str(pb_dir) not in sys.path:
        sys.path.insert(0, str(pb_dir))

    pb2_path = pb_dir / "signal_pb2.py"
    if not pb2_path.exists():
        import subprocess
        proto_dir = repo_root / "engine" / "proto"
        subprocess.run([
            "protoc", f"--proto_path={proto_dir}",
            f"--python_out={pb_dir}", str(proto_dir / "signal.proto"),
        ], check=True, capture_output=True)

    import importlib
    return importlib.import_module("signal_pb2")

signal_pb2 = _load_pb2()


class QMTAdapter:
    """
    接收 C++ 引擎的 OrderCommand，通过 xtquant 下单，
    将成交回报以 FillReport 发回 C++ 引擎。

    支持两种模式：
      - qmt:  真实 QMT 交易（需要 xtquant + miniQMT 已登录）
      - mock: 立即模拟成交（开发测试用）
    """

    def __init__(
        self,
        mode: str = "mock",
        zmq_order: str = "ipc:///tmp/quant_orders",
        zmq_fill: str = "ipc:///tmp/quant_fills",
        qmt_path: str = "",
        account_id: str = "",
    ):
        self.mode = mode
        self._running = False
        self._trade_id_counter = 0

        # ZMQ: 收订单
        self._zmq_ctx = zmq.Context()
        self._order_sock = self._zmq_ctx.socket(zmq.PULL)
        self._order_sock.connect(zmq_order)
        self._order_sock.setsockopt(zmq.RCVTIMEO, 200)

        # ZMQ: 发成交回报
        self._fill_sock = self._zmq_ctx.socket(zmq.PUSH)
        self._fill_sock.connect(zmq_fill)

        # QMT 连接
        self._trader = None
        self._account = None
        if mode == "qmt":
            self._init_qmt(qmt_path, account_id)

        # TWAP 调度器
        from execution.algo.twap import TWAPScheduler
        self._twap = TWAPScheduler(self._execute_single_order)

        logger.info("QMTAdapter started: mode=%s order=%s fill=%s",
                     mode, zmq_order, zmq_fill)

    def _init_qmt(self, qmt_path: str, account_id: str):
        """连接 miniQMT 客户端"""
        try:
            from xtquant import xttrader, xtdata
            self._trader = xttrader.XtQuantTrader(qmt_path, int(time.time()))
            self._account = xttrader.StockAccount(account_id)
            self._trader.start()
            conn = self._trader.connect()
            if conn != 0:
                raise ConnectionError(f"QMT connect failed: code={conn}")

            # 注册回调
            class _Callback(xttrader.XtQuantTraderCallback):
                def __init__(self, adapter):
                    super().__init__()
                    self.adapter = adapter

                def on_stock_order(self, data):
                    logger.info("QMT order callback: %s", data.order_id)

                def on_stock_trade(self, data):
                    self.adapter._on_qmt_trade(data)

                def on_order_error(self, data):
                    self.adapter._on_qmt_error(data)

            self._trader.register_callback(_Callback(self))
            self._trader.subscribe(self._account)
            logger.info("QMT connected: account=%s", account_id)
        except ImportError:
            raise RuntimeError(
                "xtquant 未安装。请先安装: pip install xtquant\n"
                "并确保 miniQMT 客户端已启动并登录。"
            )

    def run(self):
        """主循环：接收 OrderCommandBatch 并执行"""
        self._running = True
        logger.info("QMTAdapter main loop started")

        while self._running:
            try:
                msg = self._order_sock.recv(flags=0)
            except zmq.Again:
                continue
            except zmq.ZMQError:
                break

            batch = signal_pb2.OrderCommandBatch()
            if not batch.ParseFromString(msg):
                logger.warning("Failed to parse OrderCommandBatch (%d bytes)", len(msg))
                continue

            logger.info("Received %d orders from engine (strategy=%s)",
                        len(batch.orders), batch.strategy_id)

            for cmd in batch.orders:
                self._dispatch_order(cmd)

    def stop(self):
        self._running = False
        self._twap.cancel_all()
        if self._trader:
            try:
                self._trader.stop()
            except Exception:
                pass
        try:
            self._fill_sock.setsockopt(zmq.LINGER, 0)
            self._fill_sock.close()
        except Exception:
            pass
        try:
            self._order_sock.setsockopt(zmq.LINGER, 0)
            self._order_sock.close()
        except Exception:
            pass
        try:
            self._zmq_ctx.term()
        except Exception:
            pass
        logger.info("QMTAdapter stopped")

    def _dispatch_order(self, cmd):
        """根据 algo 字段决定直接下单还是 TWAP"""
        if cmd.algo == "twap" and cmd.twap_slices > 1:
            self._twap.schedule(
                order_id=cmd.order_id,
                symbol=cmd.symbol,
                side=cmd.side,
                total_qty=cmd.quantity,
                price=cmd.price,
                slices=cmd.twap_slices,
                interval_sec=cmd.twap_interval_sec,
                strategy_id=cmd.strategy_id,
            )
        else:
            self._execute_single_order(
                order_id=cmd.order_id,
                symbol=cmd.symbol,
                side=cmd.side,
                qty=cmd.quantity,
                price=cmd.price,
                strategy_id=cmd.strategy_id,
            )

    def _execute_single_order(
        self,
        order_id: int,
        symbol: str,
        side: str,
        qty: int,
        price: float,
        strategy_id: str = "",
    ):
        """执行单笔委托"""
        if self.mode == "mock":
            self._mock_fill(order_id, symbol, side, qty, price)
        else:
            self._qmt_order(order_id, symbol, side, qty, price)

    def _mock_fill(self, order_id: int, symbol: str, side: str,
                   qty: int, price: float):
        """模拟立即全部成交"""
        self._trade_id_counter += 1
        slippage = 0.001 if side == "BUY" else -0.001
        fill_price = price * (1 + slippage)

        commission = max(qty * fill_price * 0.00025, 5.0)
        stamp_duty = qty * fill_price * 0.001 if side == "SELL" else 0.0

        report = signal_pb2.FillReport()
        report.order_id = order_id
        report.trade_id = self._trade_id_counter
        report.symbol = symbol
        report.side = side
        report.fill_quantity = qty
        report.fill_price = fill_price
        report.commission = commission
        report.stamp_duty = stamp_duty
        report.fill_time_ns = time.time_ns()
        report.status = "FILLED"

        self._send_fill(report)
        logger.info("MOCK fill: %s %s %d@%.4f", symbol, side, qty, fill_price)

    def _qmt_order(self, order_id: int, symbol: str, side: str,
                   qty: int, price: float):
        """通过 xtquant 提交真实委托"""
        try:
            from xtquant import xtconstant

            stock_code = symbol
            if side == "BUY":
                direction = xtconstant.STOCK_BUY
            else:
                direction = xtconstant.STOCK_SELL

            xt_order_id = self._trader.order_stock(
                self._account, stock_code, direction,
                int(qty), xtconstant.FIX_PRICE, price,
                strategy_name="quant_engine",
                order_remark=str(order_id),
            )
            logger.info("QMT order submitted: xt_id=%s engine_id=%d %s %s %d@%.2f",
                        xt_order_id, order_id, symbol, side, qty, price)

            # 成交回报通过 on_stock_trade 回调异步推送
        except Exception as e:
            logger.error("QMT order failed: %s", e)
            report = signal_pb2.FillReport()
            report.order_id = order_id
            report.symbol = symbol
            report.side = side
            report.status = "REJECTED"
            report.reject_reason = str(e)
            self._send_fill(report)

    def _on_qmt_trade(self, data):
        """QMT 成交回调 → FillReport"""
        self._trade_id_counter += 1

        engine_order_id = int(data.order_remark) if data.order_remark.isdigit() else 0

        report = signal_pb2.FillReport()
        report.order_id = engine_order_id
        report.trade_id = self._trade_id_counter
        report.symbol = data.stock_code
        report.side = "BUY" if data.order_type == 23 else "SELL"
        report.fill_quantity = int(data.traded_volume)
        report.fill_price = data.traded_price
        report.commission = data.commission if hasattr(data, 'commission') else 0.0
        report.fill_time_ns = time.time_ns()
        report.status = "FILLED"

        self._send_fill(report)
        logger.info("QMT trade: %s %s %d@%.4f",
                     data.stock_code, report.side,
                     data.traded_volume, data.traded_price)

    def _on_qmt_error(self, data):
        """QMT 错误回调"""
        engine_order_id = int(data.order_remark) if hasattr(data, 'order_remark') and data.order_remark.isdigit() else 0
        report = signal_pb2.FillReport()
        report.order_id = engine_order_id
        report.status = "REJECTED"
        report.reject_reason = str(getattr(data, 'error_msg', 'unknown'))
        self._send_fill(report)
        logger.error("QMT error: %s", data)

    def _send_fill(self, report):
        """发送 FillReport 到 C++ 引擎"""
        try:
            self._fill_sock.send(report.SerializeToString(), zmq.NOBLOCK)
        except zmq.ZMQError as e:
            logger.error("Failed to send fill report: %s", e)


# ── CLI 入口 ──────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    import yaml

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="QMT Adapter for C++ Engine")
    parser.add_argument("--config", default="execution/config.yaml")
    parser.add_argument("--mode", choices=["mock", "qmt"], default=None)
    args = parser.parse_args()

    cfg = {}
    if Path(args.config).exists():
        with open(args.config) as f:
            cfg = yaml.safe_load(f) or {}

    gw_cfg = cfg.get("gateway", {})
    mode = args.mode or gw_cfg.get("type", "mock")

    adapter = QMTAdapter(
        mode=mode,
        zmq_order=cfg.get("zmq", {}).get("order", "ipc:///tmp/quant_orders"),
        zmq_fill=cfg.get("zmq", {}).get("fill", "ipc:///tmp/quant_fills"),
        qmt_path=gw_cfg.get("qmt_path", ""),
        account_id=gw_cfg.get("account_id", ""),
    )

    def _sig_handler(sig, frame):
        adapter.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    adapter.run()
