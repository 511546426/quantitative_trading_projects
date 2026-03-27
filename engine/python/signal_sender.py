"""
signal_sender.py - Python 策略层向 C++ 引擎发送信号

依赖：
    pip install pyzmq protobuf

使用方式：
    from engine.python.signal_sender import SignalSender

    sender = SignalSender()
    sender.send_rebalance(
        strategy_id="reversal_v1",
        weights={"000001.SZ": 0.05, "600000.SH": 0.03},
        total_capital=20000.0,
    )
"""

import time
import zmq
import sys
import subprocess
import importlib
from pathlib import Path
from typing import Dict, Optional

def _load_signal_pb2():
    """Load signal_pb2; auto-generate with protoc if missing."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[1]
    proto_dir = repo_root / "engine" / "proto"
    py_out_dir = script_dir

    if str(py_out_dir) not in sys.path:
        sys.path.insert(0, str(py_out_dir))

    try:
        return importlib.import_module("signal_pb2")
    except ImportError:
        pass

    # Try generate python protobuf code on the fly.
    proto_file = proto_dir / "signal.proto"
    cmd = [
        "protoc",
        f"--proto_path={proto_dir}",
        f"--python_out={py_out_dir}",
        str(proto_file),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except Exception as e:
        raise RuntimeError(
            "signal_pb2 missing and auto-generation failed. "
            "Please install protoc and run: "
            "protoc --proto_path=engine/proto --python_out=engine/python engine/proto/signal.proto"
        ) from e

    return importlib.import_module("signal_pb2")


signal_pb2 = _load_signal_pb2()


class SignalSender:
    """
    向 C++ 引擎发送目标权重信号。

    Parameters
    ----------
    endpoint : str
        ZMQ PUSH socket 地址，默认 ipc:///tmp/quant_signals
        C++ 端必须 bind 同一地址。
    """

    def __init__(self, endpoint: str = "ipc:///tmp/quant_signals") -> None:
        self._ctx    = zmq.Context()
        self._socket = self._ctx.socket(zmq.PUSH)
        self._socket.connect(endpoint)
        self._socket.setsockopt(zmq.SNDHWM, 1000)
        self._socket.setsockopt(zmq.SNDTIMEO, 5000)  # 5s 发送超时
        print(f"[SignalSender] connected to {endpoint}")

    def send_rebalance(
        self,
        strategy_id: str,
        weights: Dict[str, float],
        total_capital: float,
        ref_prices: Optional[Dict[str, float]] = None,
        rebalance_reason: str = "scheduled_rebalance",
    ) -> bool:
        """
        发送一批目标权重信号（一次完整的组合重平衡）。

        Parameters
        ----------
        strategy_id : str
            策略名称，如 "reversal_v1"
        weights : dict
            symbol → 目标权重（0~1），所有 weight 之和 ≤ 1
        total_capital : float
            策略管理的总资金（元）
        ref_prices : dict, optional
            计算权重时的参考价格
        rebalance_reason : str
            本次重平衡触发原因

        Returns
        -------
        bool : 是否发送成功
        """
        batch = signal_pb2.SignalBatch()
        batch.strategy_id    = strategy_id
        batch.total_capital  = total_capital
        batch.batch_time_ns  = time.time_ns()
        batch.rebalance_reason = rebalance_reason

        for symbol, weight in weights.items():
            sig = batch.signals.add()
            sig.symbol         = symbol
            sig.target_weight  = float(weight)
            sig.signal_time_ns = batch.batch_time_ns
            sig.strategy_id    = strategy_id
            if ref_prices and symbol in ref_prices:
                sig.ref_price = float(ref_prices[symbol])

        try:
            self._socket.send(batch.SerializeToString(), zmq.NOBLOCK)
            return True
        except zmq.Again:
            print("[SignalSender] WARNING: send queue full, signal dropped")
            return False
        except zmq.ZMQError as e:
            print(f"[SignalSender] ERROR: {e}")
            return False

    def send_close_all(self, strategy_id: str, symbols: list[str]) -> bool:
        """清仓指定 symbol 列表"""
        batch = signal_pb2.SignalBatch()
        batch.strategy_id    = strategy_id
        batch.batch_time_ns  = time.time_ns()
        batch.rebalance_reason = "close_all"

        for symbol in symbols:
            sig = batch.signals.add()
            sig.symbol         = symbol
            sig.target_weight  = 0.0
            sig.close_position = True
            sig.signal_time_ns = batch.batch_time_ns
            sig.strategy_id    = strategy_id

        try:
            self._socket.send(batch.SerializeToString(), zmq.NOBLOCK)
            return True
        except zmq.ZMQError as e:
            print(f"[SignalSender] ERROR: {e}")
            return False

    def close(self) -> None:
        self._socket.close()
        self._ctx.term()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ────────────────────────────────────────────────────────────
# CLI 测试工具：python signal_sender.py --test
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Send test signals to C++ engine")
    parser.add_argument("--endpoint", default="ipc:///tmp/quant_signals")
    parser.add_argument("--strategy", default="test_strategy")
    parser.add_argument("--capital", type=float, default=20000.0)
    parser.add_argument("--repeat", type=int, default=1)
    # backward-compatible no-op flag (old docs used --test)
    parser.add_argument("--test", action="store_true", help="Run built-in test payload")
    args = parser.parse_args()

    test_weights = {
        "000001.SZ": 0.05,
        "000002.SZ": 0.04,
        "600000.SH": 0.03,
        "600036.SH": 0.06,
    }

    with SignalSender(args.endpoint) as sender:
        for i in range(args.repeat):
            ok = sender.send_rebalance(
                strategy_id=args.strategy,
                weights=test_weights,
                total_capital=args.capital,
                rebalance_reason="test",
            )
            print(f"[{i+1}/{args.repeat}] send: {'OK' if ok else 'FAIL'}")
            if args.repeat > 1:
                time.sleep(1)
