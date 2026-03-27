"""
每日调度入口 — 串联所有模块

使用方式：
    # Mock 模式（开发测试）
    python -m execution.run --mode mock

    # QMT 实盘模式
    python -m execution.run --mode qmt --config execution/config.yaml

    # 指定日期回测
    python -m execution.run --mode mock --date 20260325

流程：
    1. 启动 C++ 引擎（后台）
    2. 启动 QMT 适配器（后台线程）
    3. 运行策略 → 发送信号 → 引擎风控 → 订单指令 → 适配器执行
    4. 等待成交回报
    5. 持久化结果 + 监控告警
    6. 关闭所有组件
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger("execution.run")
REPO_ROOT = Path(__file__).resolve().parents[1]


def _setup_logging(log_dir: str = "logs"):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                f"{log_dir}/run_{datetime.now():%Y%m%d_%H%M%S}.log",
                encoding="utf-8",
            ),
        ],
    )


class DailyScheduler:
    """
    每日调度器：组合 C++ 引擎 + QMT 适配器 + 策略桥接器 + 持久化 + 监控
    """

    def __init__(self, config_path: str = "execution/config.yaml"):
        self.cfg = self._load_config(config_path)
        self._engine_proc: Optional[subprocess.Popen] = None
        self._adapter = None
        self._adapter_thread: Optional[threading.Thread] = None
        self._alert = None
        self._pg = None

    def _load_config(self, path: str) -> dict:
        p = Path(path)
        if p.exists():
            with open(p) as f:
                return yaml.safe_load(f) or {}
        return {}

    def run(self, trade_date: Optional[str] = None, mode: str = "mock"):
        """执行一次完整的日度交易流程"""
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        logger.info("=" * 60)
        logger.info("每日调度开始 [%s] mode=%s", trade_date, mode)
        logger.info("=" * 60)

        try:
            # 1. 初始化组件
            self._init_components(mode)

            # 2. 启动 C++ 引擎
            self._start_engine()
            time.sleep(1)

            # 3. 启动 QMT 适配器
            self._start_adapter(mode)
            time.sleep(0.5)

            # 4. 运行策略并发送信号
            ok = self._run_strategy(trade_date)
            if not ok:
                logger.info("策略无调仓信号，退出")
                return

            # 5. 等待成交回报处理完成
            self._wait_for_fills(timeout_sec=120, mode=mode)

            # 6. 持久化
            self._persist_results(trade_date)

            # 7. 告警汇总
            self._send_summary(trade_date)

            logger.info("每日调度完成 [%s]", trade_date)

        except Exception as e:
            logger.error("调度异常: %s", e, exc_info=True)
            if self._alert:
                self._alert.critical("调度异常", str(e))
        finally:
            self._cleanup()

    def _init_components(self, mode: str):
        """初始化所有子组件"""
        # 监控
        from execution.monitor.alert import AlertManager
        wechat = os.getenv("WECHAT_WEBHOOK", "")
        self._alert = AlertManager(
            wechat_webhook=wechat,
            log_dir=self.cfg.get("monitor", {}).get("log_dir", "logs"),
        )
        self._alert.start_status_monitor()

        # PostgreSQL
        pg_cfg = self.cfg.get("persist", {})
        if pg_cfg.get("pg_host"):
            try:
                from execution.persist.pg_store import PgStore
                self._pg = PgStore(
                    host=pg_cfg.get("pg_host", "127.0.0.1"),
                    port=pg_cfg.get("pg_port", 5432),
                    user=pg_cfg.get("pg_user", "postgres"),
                    password=os.getenv("PG_PASSWORD", pg_cfg.get("pg_password", "")),
                    database=pg_cfg.get("pg_database", "quant"),
                )
                self._pg.connect()
                self._pg.init_tables()
            except Exception as e:
                logger.warning("PostgreSQL 连接失败（继续运行）: %s", e)
                self._pg = None

    def _start_engine(self):
        """启动 C++ 引擎进程"""
        engine_bin = REPO_ROOT / "engine" / "build" / "Release" / "quant_engine"
        engine_cfg = REPO_ROOT / "engine" / "config" / "engine.yaml"

        if not engine_bin.exists():
            logger.warning("C++ 引擎未编译: %s，跳过（仅 Python 模式）", engine_bin)
            return

        self._engine_proc = subprocess.Popen(
            [str(engine_bin), str(engine_cfg)],
            cwd=str(REPO_ROOT / "engine"),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        logger.info("C++ 引擎已启动 PID=%d", self._engine_proc.pid)

    def _start_adapter(self, mode: str):
        """启动 QMT 适配器线程"""
        from execution.adapter.qmt_adapter import QMTAdapter

        zmq_cfg = self.cfg.get("zmq", {})
        gw_cfg = self.cfg.get("gateway", {})

        self._adapter = QMTAdapter(
            mode=mode,
            zmq_order=zmq_cfg.get("order", "ipc:///tmp/quant_orders"),
            zmq_fill=zmq_cfg.get("fill", "ipc:///tmp/quant_fills"),
            qmt_path=os.getenv("QMT_PATH", gw_cfg.get("qmt_path", "")),
            account_id=os.getenv("QMT_ACCOUNT", gw_cfg.get("account_id", "")),
        )

        self._adapter_thread = threading.Thread(
            target=self._adapter.run, daemon=True
        )
        self._adapter_thread.start()
        logger.info("QMT 适配器已启动 mode=%s", mode)

    def _run_strategy(self, trade_date: str) -> bool:
        """运行策略并通过信号桥发送到 C++ 引擎"""
        from execution.bridge.strategy_bridge import StrategyBridge

        zmq_cfg = self.cfg.get("zmq", {})
        bridge = StrategyBridge(
            strategy_id=self.cfg.get("strategy_id", "reversal_v1"),
            total_capital=self.cfg.get("initial_cash", 20_000.0),
            zmq_endpoint=zmq_cfg.get("signal", "ipc:///tmp/quant_signals"),
        )
        return bridge.run_daily(trade_date)

    def _wait_for_fills(self, timeout_sec: int = 120, mode: str = "mock"):
        """等待成交回报处理完成"""
        # Mock 模式成交几乎瞬时，QMT 需要等券商回报
        effective_timeout = 8 if mode == "mock" else timeout_sec
        logger.info("等待成交回报... (timeout=%ds, mode=%s)", effective_timeout, mode)

        start = time.time()
        while time.time() - start < effective_timeout:
            time.sleep(2)
            if self._engine_proc and self._engine_proc.poll() is not None:
                logger.warning("C++ 引擎已退出 (code=%d)", self._engine_proc.returncode)
                break
        logger.info("成交等待结束 (%.1fs)", time.time() - start)

    def _persist_results(self, trade_date: str):
        """通过 ZMQ SUB 获取引擎最新状态并持久化到 PostgreSQL"""
        if not self._pg:
            return

        try:
            import zmq as _zmq
            zmq_cfg = self.cfg.get("zmq", {})
            ctx = _zmq.Context()
            sock = ctx.socket(_zmq.SUB)
            sock.connect(zmq_cfg.get("status", "ipc:///tmp/quant_status"))
            sock.subscribe(b"")
            sock.setsockopt(_zmq.RCVTIMEO, 2000)

            pb_dir = REPO_ROOT / "engine" / "python"
            if str(pb_dir) not in sys.path:
                sys.path.insert(0, str(pb_dir))
            import signal_pb2

            try:
                msg = sock.recv()
                status = signal_pb2.EngineStatus()
                if status.ParseFromString(msg):
                    from datetime import date as _date
                    td = _date(int(trade_date[:4]), int(trade_date[4:6]),
                               int(trade_date[6:8]))
                    self._pg.save_nav(
                        trade_date=td,
                        nav=status.nav,
                        cash=status.cash,
                        market_value=status.nav - status.cash,
                        daily_return=status.daily_pnl / status.nav if status.nav > 0 else 0,
                        max_drawdown=status.max_drawdown,
                    )
                    logger.info("持久化完成: NAV=%.2f dd=%.4f", status.nav, status.max_drawdown)
            except _zmq.Again:
                logger.warning("未收到引擎状态，跳过持久化")
            finally:
                sock.setsockopt(_zmq.LINGER, 0)
                sock.close()
                ctx.term()
        except Exception as e:
            logger.error("持久化失败: %s", e)

    def _send_summary(self, trade_date: str):
        """发送日报"""
        if self._alert:
            self._alert.info("调度完成", f"交易日 {trade_date} 处理完毕")

    def _cleanup(self):
        """清理所有资源"""
        if self._adapter:
            try:
                self._adapter.stop()
            except Exception:
                pass

        if self._engine_proc and self._engine_proc.poll() is None:
            self._engine_proc.terminate()
            try:
                self._engine_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._engine_proc.kill()
            logger.info("C++ 引擎已停止")

        if self._alert:
            self._alert.stop_status_monitor()

        if self._pg:
            self._pg.close()


# ── CLI 入口 ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="QuantEngine Daily Scheduler")
    parser.add_argument("--config", default="execution/config.yaml",
                        help="配置文件路径")
    parser.add_argument("--mode", choices=["mock", "qmt"], default="mock",
                        help="mock=模拟, qmt=真实QMT")
    parser.add_argument("--date", default=None,
                        help="交易日期 YYYYMMDD（默认今天）")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    _setup_logging(args.log_dir)

    scheduler = DailyScheduler(config_path=args.config)

    def _sig_handler(sig, frame):
        logger.info("收到停止信号")
        scheduler._cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    scheduler.run(trade_date=args.date, mode=args.mode)


if __name__ == "__main__":
    main()
