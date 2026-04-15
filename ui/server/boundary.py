"""Network boundary: optional client IP allowlist (HTTP + WebSocket)."""

from __future__ import annotations

import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from ui.server.config import ALLOWED_CLIENT_IPS

logger = logging.getLogger("quant.ops.boundary")


def client_ip_allowed(client: object | None) -> bool:
    """Return True if no allowlist, else require ``client.host`` in allowlist."""
    if not ALLOWED_CLIENT_IPS:
        return True
    if client is None:
        return False
    host = getattr(client, "host", None)
    if not host:
        return False
    return str(host) in ALLOWED_CLIENT_IPS


class IpAllowlistMiddleware(BaseHTTPMiddleware):
    """Reject HTTP when ``QUANT_OPS_ALLOWED_IPS`` is set and peer not listed."""

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        if not ALLOWED_CLIENT_IPS:
            return await call_next(request)
        if not client_ip_allowed(request.client):
            peer = request.client.host if request.client else "-"
            logger.warning("blocked request from disallowed IP=%s path=%s", peer, request.url.path)
            return JSONResponse(
                {"detail": "client IP not allowed (QUANT_OPS_ALLOWED_IPS)"},
                status_code=403,
            )
        return await call_next(request)
