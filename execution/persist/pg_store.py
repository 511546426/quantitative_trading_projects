"""
PostgreSQL 状态持久化

表结构：
  exec_orders    — 所有订单（含状态流转）
  exec_trades    — 成交明细
  exec_positions — 每日持仓快照
  exec_nav       — 每日净值
"""
from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS exec_orders (
    order_id        BIGINT PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL,
    side            VARCHAR(4)  NOT NULL,
    order_type      VARCHAR(10),
    quantity         BIGINT,
    price           DOUBLE PRECISION,
    strategy_id     VARCHAR(50),
    algo            VARCHAR(10),
    status          VARCHAR(20) DEFAULT 'NEW',
    reject_reason   TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS exec_trades (
    trade_id        BIGSERIAL PRIMARY KEY,
    order_id        BIGINT REFERENCES exec_orders(order_id),
    symbol          VARCHAR(20) NOT NULL,
    side            VARCHAR(4)  NOT NULL,
    fill_qty        BIGINT,
    fill_price      DOUBLE PRECISION,
    commission      DOUBLE PRECISION DEFAULT 0,
    stamp_duty      DOUBLE PRECISION DEFAULT 0,
    fill_time       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS exec_positions (
    trade_date      DATE NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    quantity        BIGINT,
    avg_cost        DOUBLE PRECISION,
    last_price      DOUBLE PRECISION,
    market_value    DOUBLE PRECISION,
    unrealized_pnl  DOUBLE PRECISION,
    PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS exec_nav (
    trade_date      DATE PRIMARY KEY,
    nav             DOUBLE PRECISION,
    cash            DOUBLE PRECISION,
    market_value    DOUBLE PRECISION,
    daily_return    DOUBLE PRECISION,
    max_drawdown    DOUBLE PRECISION
);
"""


class PgStore:
    """PostgreSQL 持久化层"""

    def __init__(self, host: str = "127.0.0.1", port: int = 5432,
                 user: str = "postgres", password: str = "",
                 database: str = "quant"):
        self._dsn = dict(host=host, port=port, user=user,
                         password=password, database=database)
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self):
        self._conn = psycopg2.connect(**self._dsn)
        self._conn.autocommit = True
        logger.info("PgStore connected to %s:%d/%s",
                     self._dsn["host"], self._dsn["port"], self._dsn["database"])

    def init_tables(self):
        with self._conn.cursor() as cur:
            cur.execute(DDL)
        logger.info("PgStore tables initialized")

    def close(self):
        if self._conn:
            self._conn.close()

    # ── 订单 ──────────────────────────────────────────────────

    def save_order(self, order_id: int, symbol: str, side: str,
                   order_type: str, quantity: int, price: float,
                   strategy_id: str, algo: str):
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exec_orders
                    (order_id, symbol, side, order_type, quantity, price, strategy_id, algo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    status = EXCLUDED.status, updated_at = NOW()
            """, (order_id, symbol, side, order_type, quantity, price, strategy_id, algo))

    def update_order_status(self, order_id: int, status: str,
                            reject_reason: str = ""):
        with self._conn.cursor() as cur:
            cur.execute("""
                UPDATE exec_orders SET status = %s, reject_reason = %s,
                       updated_at = NOW()
                WHERE order_id = %s
            """, (status, reject_reason, order_id))

    # ── 成交 ──────────────────────────────────────────────────

    def save_trade(self, order_id: int, symbol: str, side: str,
                   fill_qty: int, fill_price: float,
                   commission: float, stamp_duty: float):
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exec_trades
                    (order_id, symbol, side, fill_qty, fill_price, commission, stamp_duty)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (order_id, symbol, side, fill_qty, fill_price, commission, stamp_duty))

    # ── 持仓快照 ──────────────────────────────────────────────

    def save_positions(self, trade_date: date, positions: Dict[str, dict]):
        """
        positions: {symbol: {quantity, avg_cost, last_price, market_value, unrealized_pnl}}
        """
        with self._conn.cursor() as cur:
            for symbol, p in positions.items():
                cur.execute("""
                    INSERT INTO exec_positions
                        (trade_date, symbol, quantity, avg_cost, last_price,
                         market_value, unrealized_pnl)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, symbol) DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        avg_cost = EXCLUDED.avg_cost,
                        last_price = EXCLUDED.last_price,
                        market_value = EXCLUDED.market_value,
                        unrealized_pnl = EXCLUDED.unrealized_pnl
                """, (trade_date, symbol, p["quantity"], p["avg_cost"],
                      p["last_price"], p["market_value"], p["unrealized_pnl"]))

    # ── 净值 ──────────────────────────────────────────────────

    def save_nav(self, trade_date: date, nav: float, cash: float,
                 market_value: float, daily_return: float,
                 max_drawdown: float):
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exec_nav
                    (trade_date, nav, cash, market_value, daily_return, max_drawdown)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date) DO UPDATE SET
                    nav = EXCLUDED.nav, cash = EXCLUDED.cash,
                    market_value = EXCLUDED.market_value,
                    daily_return = EXCLUDED.daily_return,
                    max_drawdown = EXCLUDED.max_drawdown
            """, (trade_date, nav, cash, market_value, daily_return, max_drawdown))

    # ── 查询 ──────────────────────────────────────────────────

    def get_today_orders(self, trade_date: Optional[date] = None) -> List[dict]:
        td = trade_date or date.today()
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM exec_orders
                WHERE created_at::date = %s
                ORDER BY created_at
            """, (td,))
            return cur.fetchall()

    def get_nav_history(self, days: int = 252) -> List[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM exec_nav
                ORDER BY trade_date DESC LIMIT %s
            """, (days,))
            return list(reversed(cur.fetchall()))
