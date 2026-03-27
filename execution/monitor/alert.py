"""
监控告警模块

支持：
  1. 企业微信 Webhook（群机器人）
  2. 日志告警
  3. 状态监听（订阅 C++ 引擎 ZMQ 状态广播）

环境变量：
  WECHAT_WEBHOOK=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
"""
from __future__ import annotations

import json
import logging
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class AlertManager:
    """统一告警管理"""

    def __init__(
        self,
        wechat_webhook: str = "",
        log_dir: str = "logs",
        zmq_status: str = "ipc:///tmp/quant_status",
    ):
        self._webhook = wechat_webhook
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._zmq_status = zmq_status
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False

        self._setup_file_logger()

    def _setup_file_logger(self):
        """独立的执行日志文件"""
        fh = logging.FileHandler(
            self._log_dir / f"execution_{datetime.now():%Y%m%d}.log",
            encoding="utf-8",
        )
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        ))
        logging.getLogger("execution").addHandler(fh)

    # ── 告警发送 ──────────────────────────────────────────────

    def info(self, title: str, content: str):
        logger.info("[%s] %s", title, content)

    def warn(self, title: str, content: str):
        logger.warning("[%s] %s", title, content)
        self._send_wechat(f"⚠️ {title}", content)

    def critical(self, title: str, content: str):
        logger.critical("[%s] %s", title, content)
        self._send_wechat(f"🚨 {title}", content, mentioned_list=["@all"])

    def trade_summary(self, date: str, nav: float, daily_return: float,
                      positions: int, orders: int):
        """每日交易完成后的汇总"""
        msg = (
            f"📊 交易日报 [{date}]\n"
            f"净值: ¥{nav:,.0f}\n"
            f"日收益: {daily_return:+.2%}\n"
            f"持仓数: {positions}\n"
            f"今日订单: {orders}"
        )
        logger.info(msg)
        self._send_wechat("交易日报", msg)

    def _send_wechat(self, title: str, content: str,
                     mentioned_list: Optional[list] = None):
        """发送企业微信群机器人消息"""
        if not self._webhook:
            return

        payload = {
            "msgtype": "text",
            "text": {
                "content": f"[QuantEngine] {title}\n{content}",
            }
        }
        if mentioned_list:
            payload["text"]["mentioned_list"] = mentioned_list

        try:
            import urllib.request
            req = urllib.request.Request(
                self._webhook,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                result = json.loads(resp.read())
                if result.get("errcode") != 0:
                    logger.error("WeChat send failed: %s", result)
        except Exception as e:
            logger.error("WeChat webhook error: %s", e)

    # ── 状态监听 ──────────────────────────────────────────────

    def start_status_monitor(self):
        """后台监听 C++ 引擎状态广播"""
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._status_loop, daemon=True
        )
        self._monitor_thread.start()

    def stop_status_monitor(self):
        self._running = False

    def _status_loop(self):
        import zmq
        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        sock.connect(self._zmq_status)
        sock.subscribe(b"")
        sock.setsockopt(zmq.RCVTIMEO, 1000)

        cur = Path(__file__).resolve().parent
        for _ in range(10):
            if (cur / "engine" / "python").is_dir():
                break
            cur = cur.parent
        pb_dir = cur / "engine" / "python"
        if str(pb_dir) not in sys.path:
            sys.path.insert(0, str(pb_dir))
        import signal_pb2

        logger.info("Status monitor started, subscribing to %s", self._zmq_status)

        last_status = ""
        while self._running:
            try:
                msg = sock.recv()
                status = signal_pb2.EngineStatus()
                if status.ParseFromString(msg):
                    if status.status != last_status:
                        if status.status == "breaker_on":
                            self.critical("熔断触发", status.error_msg)
                        last_status = status.status

                    if status.max_drawdown < -0.10:
                        self.warn("回撤警告",
                                  f"当前回撤 {status.max_drawdown:.2%}")
            except zmq.Again:
                continue
            except Exception as e:
                logger.error("Status monitor error: %s", e)
                time.sleep(1)

        sock.close()
        ctx.term()
