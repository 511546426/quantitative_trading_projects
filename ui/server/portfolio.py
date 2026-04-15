"""
简易持仓与手工成交流水：PostgreSQL 表 manual_trade_ledger + 汇总与集中度预警。
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ui.server.deps import require_api_key
from ui.server.research import _ch, _pg

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/portfolio", tags=["portfolio"], dependencies=[Depends(require_api_key)])

_LEDGER_TABLE = """
CREATE TABLE IF NOT EXISTS manual_trade_ledger (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    ts_code VARCHAR(12) NOT NULL,
    side VARCHAR(4) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    fee DOUBLE PRECISION NOT NULL DEFAULT 0,
    note TEXT DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT manual_trade_ledger_side_chk CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT manual_trade_ledger_qty_chk CHECK (quantity > 0),
    CONSTRAINT manual_trade_ledger_price_chk CHECK (price > 0)
)
"""


def _ensure_ledger_table() -> None:
    pg = _pg()
    pg._ensure_conn()
    cur = pg._conn.cursor()
    try:
        cur.execute(_LEDGER_TABLE)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_manual_trade_ledger_date ON manual_trade_ledger (trade_date DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_manual_trade_ledger_code ON manual_trade_ledger (ts_code)"
        )
        pg._conn.commit()
    finally:
        cur.close()


def _latest_close(ts_code: str) -> tuple[float | None, str | None]:
    ch = _ch()
    ch._ensure_client()
    sql = """
        SELECT trade_date, close
        FROM stock_daily FINAL
        WHERE ts_code = %(code)s
        ORDER BY trade_date DESC
        LIMIT 1
    """
    rows = ch._client.execute(sql, {"code": ts_code})
    if not rows:
        return None, None
    d, c = rows[0]
    t = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
    return float(c), t


class TradeIn(BaseModel):
    trade_date: str = Field(..., description="YYYY-MM-DD")
    ts_code: str
    side: Literal["BUY", "SELL"]
    quantity: float = Field(..., gt=0)
    price: float = Field(..., gt=0)
    fee: float = Field(0, ge=0)
    note: str = ""


@router.post("/trades")
async def add_trade(body: TradeIn) -> dict[str, Any]:
    _ensure_ledger_table()
    code = body.ts_code.strip().upper()
    if "." not in code:
        raise HTTPException(400, "ts_code 须含交易所后缀，如 600000.SH")
    try:
        d = date.fromisoformat(body.trade_date.strip()[:10])
    except ValueError as e:
        raise HTTPException(400, "trade_date 须为 YYYY-MM-DD") from e
    pg = _pg()
    pg._ensure_conn()
    cur = pg._conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO manual_trade_ledger (trade_date, ts_code, side, quantity, price, fee, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (d, code, body.side, body.quantity, body.price, body.fee, body.note[:500]),
        )
        row = cur.fetchone()
        pg._conn.commit()
        return {"id": int(row[0]), "ok": True}
    except Exception as e:
        pg._conn.rollback()
        logger.exception("add_trade failed")
        raise HTTPException(503, str(e)) from e
    finally:
        cur.close()


@router.get("/trades")
async def list_trades(limit: int = Query(200, ge=1, le=2000)) -> dict[str, Any]:
    _ensure_ledger_table()
    pg = _pg()
    pg._ensure_conn()
    cur = pg._conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, trade_date, ts_code, side, quantity, price, fee, note, created_at
            FROM manual_trade_ledger
            ORDER BY trade_date DESC, id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
    items = []
    for r in rows:
        items.append(
            {
                "id": r[0],
                "trade_date": r[1].isoformat() if r[1] else None,
                "ts_code": r[2],
                "side": r[3],
                "quantity": float(r[4]),
                "price": float(r[5]),
                "fee": float(r[6] or 0),
                "note": r[7] or "",
                "created_at": r[8].isoformat() if r[8] else None,
            }
        )
    return {"trades": items}


@router.delete("/trades/{trade_id}")
async def delete_trade(trade_id: int) -> dict[str, Any]:
    _ensure_ledger_table()
    pg = _pg()
    pg._ensure_conn()
    cur = pg._conn.cursor()
    try:
        cur.execute("DELETE FROM manual_trade_ledger WHERE id = %s RETURNING id", (trade_id,))
        row = cur.fetchone()
        if not row:
            pg._conn.rollback()
            raise HTTPException(404, "记录不存在")
        pg._conn.commit()
        return {"ok": True, "deleted_id": trade_id}
    finally:
        cur.close()


@router.get("/summary")
async def portfolio_summary(
    capital: float = Query(500_000, ge=1_000, description="总资金（用于仓位占比）"),
    max_single_pct: float = Query(
        0.25,
        ge=0.05,
        le=1.0,
        description="单标的市值/总资金 超过该比例则预警",
    ),
) -> dict[str, Any]:
    """按流水汇总净持仓，用最近一日收盘价估算市值与集中度。"""
    _ensure_ledger_table()
    pg = _pg()
    pg._ensure_conn()
    cur = pg._conn.cursor()
    try:
        cur.execute(
            """
            SELECT ts_code,
                   SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_qty
            FROM manual_trade_ledger
            GROUP BY ts_code
            HAVING SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) > 1e-6
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    positions: list[dict[str, Any]] = []
    total_mv = 0.0
    warnings: list[str] = []

    for ts_code, net_qty in rows:
        nq = float(net_qty)
        px, asof = _latest_close(ts_code)
        if px is None:
            positions.append(
                {
                    "ts_code": ts_code,
                    "net_quantity": round(nq, 4),
                    "last_close": None,
                    "as_of": None,
                    "market_value": None,
                    "pct_of_capital": None,
                }
            )
            warnings.append(f"{ts_code}: 无行情，无法估值")
            continue
        mv = nq * px
        total_mv += mv
        pct = mv / capital if capital > 0 else 0.0
        flag = pct > max_single_pct
        if flag:
            warnings.append(f"{ts_code}: 市值占资金 {pct:.1%}，超过单标的上限 {max_single_pct:.0%}")
        positions.append(
            {
                "ts_code": ts_code,
                "net_quantity": round(nq, 4),
                "last_close": px,
                "as_of": asof,
                "market_value": round(mv, 2),
                "pct_of_capital": round(pct, 6),
                "concentration_alert": flag,
            }
        )

    total_pct = total_mv / capital if capital > 0 else 0.0
    if total_pct > 0.95:
        warnings.append(f"持仓总市值占资金 {total_pct:.1%}，接近或超过满仓线（示意 95%）")

    return {
        "capital": capital,
        "max_single_pct": max_single_pct,
        "total_market_value": round(total_mv, 2),
        "total_position_pct": round(total_pct, 6),
        "positions": positions,
        "warnings": warnings,
        "poll_hint_sec": int(os.environ.get("QUANT_PORTFOLIO_POLL_SEC", "8")),
    }
