"""
策略桥接器 — 连接 Python 策略输出与 C++ 引擎

职责：
  1. 运行策略，获取目标权重
  2. 获取最新价格作为 ref_price
  3. 通过 SignalSender 发送给 C++ 引擎

使用：
    bridge = StrategyBridge(config)
    bridge.run_daily(trade_date="20260325")
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class StrategyBridge:
    """将策略计算结果转换为引擎信号"""

    def __init__(
        self,
        strategy_id: str = "reversal_v1",
        total_capital: float = 20_000.0,
        zmq_endpoint: str = "ipc:///tmp/quant_signals",
        ch_host: str = "localhost",
        ch_port: int = 9000,
    ):
        self.strategy_id = strategy_id
        self.total_capital = total_capital
        self._zmq_endpoint = zmq_endpoint

        from data.common.config import Config
        from data.writers.clickhouse_writer import ClickHouseWriter

        cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
        self._ch = ClickHouseWriter(
            host=cfg.get("database.clickhouse.host", ch_host),
            port=int(cfg.get("database.clickhouse.port", ch_port)),
            database="quant",
            user=cfg.get("database.clickhouse.user", "default"),
            password=cfg.get("database.clickhouse.password", ""),
        )
        self._ch.connect()

    def run_daily(self, trade_date: Optional[str] = None) -> bool:
        """
        运行策略并发送信号到 C++ 引擎。
        返回 True = 发送成功，False = 无信号或失败。
        """
        if trade_date is None:
            trade_date = pd.Timestamp.today().strftime("%Y%m%d")

        logger.info("StrategyBridge: running for %s", trade_date)

        weights = self._compute_weights(trade_date)
        if weights is None:
            logger.info("StrategyBridge: no rebalance signal for %s", trade_date)
            return False

        if not weights:
            logger.info("StrategyBridge: empty weights, skip")
            return False

        ref_prices = self._get_ref_prices(trade_date, list(weights.keys()))

        # 通过 signal_sender 发给 C++ 引擎
        cur = Path(__file__).resolve().parent
        for _ in range(10):
            if (cur / "engine" / "python").is_dir():
                break
            cur = cur.parent
        sender_dir = cur / "engine" / "python"
        if str(sender_dir) not in sys.path:
            sys.path.insert(0, str(sender_dir))

        from signal_sender import SignalSender

        with SignalSender(self._zmq_endpoint) as sender:
            ok = sender.send_rebalance(
                strategy_id=self.strategy_id,
                weights=weights,
                total_capital=self.total_capital,
                ref_prices=ref_prices,
                rebalance_reason="daily_rebalance",
            )

        if ok:
            logger.info("StrategyBridge: sent %d signals to engine", len(weights))
        else:
            logger.error("StrategyBridge: failed to send signals")

        return ok

    def _compute_weights(self, trade_date: str) -> Optional[Dict[str, float]]:
        """调用策略计算目标权重"""
        try:
            from strategy.examples.reversal_value_strategy import (
                build_universe, calc_factors, generate_weights,
            )
        except ImportError:
            logger.error("Cannot import strategy module")
            return None

        close, amount = self._load_strategy_data(trade_date, lookback=150)
        if close.empty:
            return None

        exclude = self._load_exclude_list()
        universe = build_universe(close, amount, exclude)
        signal = calc_factors(close, universe)
        weights_df = generate_weights(signal)

        if weights_df.empty:
            return None

        last_weights = weights_df.iloc[-1]
        last_weights = last_weights[last_weights > 0]

        if last_weights.empty:
            return {}

        # 检查是否需要调仓
        if len(weights_df) >= 2:
            prev_held = set(weights_df.iloc[-2].pipe(lambda s: s[s > 0]).index)
            curr_held = set(last_weights.index)
            if curr_held == prev_held:
                return None

        return last_weights.to_dict()

    def _get_ref_prices(self, trade_date: str,
                        symbols: list[str]) -> Dict[str, float]:
        """获取参考价格（最新收盘价）"""
        if not symbols:
            return {}
        try:
            placeholders = ", ".join(f"'{s}'" for s in symbols)
            rows = self._ch._client.execute(f"""
                SELECT ts_code, argMax(close, trade_date) AS close
                FROM stock_daily
                WHERE ts_code IN ({placeholders})
                  AND trade_date <= '{trade_date}'
                GROUP BY ts_code
            """)
            return {r[0]: float(r[1]) for r in rows}
        except Exception as e:
            logger.error("Failed to load ref prices: %s", e)
            return {}

    def _load_strategy_data(
        self, end_date: str, lookback: int = 150
    ) -> tuple:
        """从 ClickHouse 加载策略所需的价格数据"""
        import gc
        import numpy as np
        try:
            rows = self._ch._client.execute("""
                SELECT ts_code, trade_date,
                       argMax(adj_close, trade_date) AS adj_close,
                       argMax(amount,    trade_date) AS amount
                FROM stock_daily
                WHERE trade_date IN (
                    SELECT DISTINCT trade_date FROM stock_daily
                    WHERE trade_date <= %(end)s AND is_suspended = 0
                    ORDER BY trade_date DESC LIMIT %(n)s
                )
                AND is_suspended = 0
                GROUP BY ts_code, trade_date
                ORDER BY trade_date, ts_code
            """, {"end": end_date, "n": lookback})
        except Exception as e:
            logger.error("Strategy data load failed: %s", e)
            return pd.DataFrame(), pd.DataFrame()

        if not rows:
            return pd.DataFrame(), pd.DataFrame()

        df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_close", "amount"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df[["adj_close", "amount"]] = df[["adj_close", "amount"]].astype(np.float32)
        close = df.pivot(index="trade_date", columns="ts_code", values="adj_close")
        amount_df = df.pivot(index="trade_date", columns="ts_code", values="amount")
        del df
        gc.collect()
        return close, amount_df

    def _load_exclude_list(self) -> set:
        try:
            from data.writers.postgres_writer import PostgresWriter
            from data.common.config import Config
            cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
            pg = PostgresWriter(
                host=cfg.get("database.postgres.host", "localhost"),
                port=int(cfg.get("database.postgres.port", 5432)),
                database="quant",
                user=cfg.get("database.postgres.user", "postgres"),
                password=cfg.get("database.postgres.password", ""),
            )
            pg.connect()
            rows = pg.execute_query(
                "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
            )
            return {r[0] for r in rows}
        except Exception:
            return set()
