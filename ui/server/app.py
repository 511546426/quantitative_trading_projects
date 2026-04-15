"""
Institutional ops console API: REST + WebSocket log tail + optional SPA static.

Run (from repo root, PYTHONPATH=.)::

    .venv/bin/python -m uvicorn ui.server.app:app --host 127.0.0.1 --port 8787

Use ``./ops.sh web-pro`` so ``QUANT_OPS_BIND`` controls ``--host`` (default loopback).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
from pathlib import Path
from typing import Any, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles

from ui.server.boundary import IpAllowlistMiddleware, client_ip_allowed
from ui.server.config import (
    ALLOWED_CLIENT_IPS,
    API_KEY,
    BIND_HOST,
    BUILD_ID,
    LOG_PATHS,
    OPS_SH,
    PROJECT_DIR,
    TRUSTED_HOSTS,
)
from ui.server.deps import require_api_key
from ui.server.middleware import RequestContextMiddleware, utc_http_timestamp
from ui.server import ops_runner
from ui.server.backtest_dashboard import router as dashboard_router
from ui.server.portfolio import router as portfolio_router
from ui.server.research import router as research_router

DIST_DIR = PROJECT_DIR / "ui" / "ops-console" / "dist"

_SYNC_OPS = frozenset({"status", "start-db", "stop-db", "restart-db", "daily"})


def _ws_key_ok(token: str | None) -> bool:
    if not API_KEY:
        return True
    return (token or "").strip() == API_KEY


def _cors_origins() -> list[str]:
    raw = os.environ.get(
        "QUANT_OPS_CORS",
        "http://127.0.0.1:5173,http://localhost:5173,http://127.0.0.1:8787,http://localhost:8787",
    )
    return [o.strip() for o in raw.split(",") if o.strip()]


app = FastAPI(
    title="Quant Ops Control Plane",
    description="运维控制台后端：同步命令、后台回填、日志 WebSocket。",
    version="1.0.0",
)

# Inner → outer: TrustedHost → IP allowlist → CORS → RequestContext (see Starlette insert(0) order)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)
app.add_middleware(IpAllowlistMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-Server-Time"],
)
app.add_middleware(RequestContextMiddleware)


@app.get("/api/health")
async def health() -> dict[str, Any]:
    body: dict[str, Any] = {
        "ok": True,
        "service": "quant-ops-api",
        "server_time_utc": utc_http_timestamp(),
        "version": app.version,
    }
    if BUILD_ID:
        body["build_id"] = BUILD_ID
    body["bind_host"] = BIND_HOST
    body["network_boundary"] = {
        "trusted_hosts": TRUSTED_HOSTS if TRUSTED_HOSTS != ["*"] else ["*"],
        "client_ip_allowlist_enabled": bool(ALLOWED_CLIENT_IPS),
    }
    return body


@app.get("/api/meta")
async def meta() -> dict[str, Any]:
    out: dict[str, Any] = {
        "project_dir": str(PROJECT_DIR),
        "ops_sh": str(OPS_SH),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "auth_required": bool(API_KEY),
        "log_paths": {k: str(v) for k, v in LOG_PATHS.items()},
        "server_time_utc": utc_http_timestamp(),
        "bind_host": BIND_HOST,
        "network_boundary": {
            "trusted_hosts": TRUSTED_HOSTS if TRUSTED_HOSTS != ["*"] else ["*"],
            "client_ip_allowlist_enabled": bool(ALLOWED_CLIENT_IPS),
        },
    }
    if BUILD_ID:
        out["build_id"] = BUILD_ID
    return out


class SyncOpRequest(BaseModel):
    op: Literal["status", "start-db", "stop-db", "restart-db", "daily"]
    args: list[str] = Field(default_factory=list)


@app.post("/api/ops/sync", dependencies=[Depends(require_api_key)])
async def ops_sync(body: SyncOpRequest) -> dict[str, Any]:
    if body.op not in _SYNC_OPS:
        raise HTTPException(400, "unsupported op")
    code, out = ops_runner.run_sync(body.op, tuple(body.args))
    return {"exit_code": code, "output": out}


class BackfillRequest(BaseModel):
    target: Literal["daily-bars", "index", "valuation"]


_BACKFILL_MAP = {
    "daily-bars": ("backfill-daily", "backfill-daily"),
    "index": ("backfill-index", "backfill-index"),
    "valuation": ("backfill-valuation", "backfill-valuation"),
}


@app.post("/api/ops/backfill", dependencies=[Depends(require_api_key)])
async def ops_backfill(body: BackfillRequest) -> dict[str, Any]:
    ops_cmd, log_key = _BACKFILL_MAP[body.target]
    job_id, err = ops_runner.start_backfill(ops_cmd, log_key)
    if err:
        raise HTTPException(409, err)
    return {"job_id": job_id, "ops_cmd": ops_cmd, "log_key": log_key}


@app.get("/api/jobs", dependencies=[Depends(require_api_key)])
async def jobs() -> dict[str, Any]:
    return {"jobs": ops_runner.list_jobs()}


def _read_tail_bytes(path: Path, max_bytes: int = 256_000) -> tuple[str, int]:
    if not path.is_file():
        return "", 0
    size = path.stat().st_size
    start = max(0, size - max_bytes)
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        f.seek(start)
        chunk = f.read()
    if start > 0 and chunk:
        nl = chunk.find("\n")
        if nl != -1:
            chunk = chunk[nl + 1 :]
    return chunk, path.stat().st_size


def _read_file_range(path: Path, start: int, end: int) -> tuple[str, int]:
    with open(path, "rb") as bf:
        bf.seek(start)
        raw = bf.read(max(0, end - start))
    text = raw.decode("utf-8", errors="replace")
    return text, end


@app.websocket("/api/ws/logs/{log_key}")
async def websocket_log_tail(
    websocket: WebSocket,
    log_key: str,
    token: str | None = Query(None, description="QUANT_OPS_API_KEY when auth enabled"),
) -> None:
    """
    日志 WebSocket：发送协程与 ``receive()`` 并行等待其一结束。
    避免「只发不收」时客户端已断开但服务端仍卡在 ``sleep`` 循环，导致 uvicorn 优雅退出卡住。
    """
    if log_key not in LOG_PATHS:
        await websocket.close(code=4400)
        return
    if not client_ip_allowed(websocket.client):
        await websocket.close(code=4403)
        return
    if not _ws_key_ok(token):
        await websocket.close(code=4401)
        return
    await websocket.accept()
    path = LOG_PATHS[log_key]

    snapshot, pos0 = await asyncio.to_thread(_read_tail_bytes, path)
    try:
        await websocket.send_json({"type": "snapshot", "text": snapshot})
    except (WebSocketDisconnect, RuntimeError):
        with contextlib.suppress(Exception):
            await websocket.close()
        return

    pos_state: list[int] = [pos0]

    async def pump_logs() -> None:
        try:
            while True:
                await asyncio.sleep(0.45)
                if not path.is_file():
                    with contextlib.suppress(WebSocketDisconnect, RuntimeError):
                        await websocket.send_json(
                            {"type": "status", "message": "waiting_for_file"}
                        )
                    continue

                st = await asyncio.to_thread(path.stat)
                size = st.st_size
                if size < pos_state[0]:
                    pos_state[0] = 0
                if size > pos_state[0]:
                    text, new_pos = await asyncio.to_thread(
                        _read_file_range, path, pos_state[0], size
                    )
                    pos_state[0] = new_pos
                    if text:
                        await websocket.send_json({"type": "append", "text": text})
        except WebSocketDisconnect:
            return
        except RuntimeError:
            return
        except asyncio.CancelledError:
            raise

    async def wait_client_disconnect() -> None:
        try:
            while True:
                msg = await websocket.receive()
                if msg.get("type") == "websocket.disconnect":
                    return
        except asyncio.CancelledError:
            raise

    pump_task = asyncio.create_task(pump_logs())
    watch_task = asyncio.create_task(wait_client_disconnect())
    try:
        await asyncio.wait(
            {pump_task, watch_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        raise
    finally:
        for t in (pump_task, watch_task):
            if not t.done():
                t.cancel()
        await asyncio.gather(pump_task, watch_task, return_exceptions=True)
        with contextlib.suppress(Exception):
            await websocket.close(code=1001)


app.include_router(research_router)
app.include_router(dashboard_router)
app.include_router(portfolio_router)


if DIST_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=str(DIST_DIR / "assets")), name="spa-assets")

    @app.get("/")
    async def spa_root() -> FileResponse:
        return FileResponse(DIST_DIR / "index.html")

    @app.get("/{full_path:path}")
    async def spa_fallback(full_path: str) -> FileResponse:
        """
        SPA fallback: for client-side routes like /jobs, always return index.html.
        If the requested path maps to an existing static file under dist/, return that file.
        """
        candidate = (DIST_DIR / full_path).resolve()
        try:
            candidate.relative_to(DIST_DIR.resolve())
        except ValueError:
            return FileResponse(DIST_DIR / "index.html")
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(DIST_DIR / "index.html")
