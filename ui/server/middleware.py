"""Cross-cutting HTTP concerns: request id, server time header, structured access log."""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("quant.ops.http")


def utc_http_timestamp() -> str:
    """RFC3339-like UTC instant for HTTP headers (no subsecond)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class RequestContextMiddleware(BaseHTTPMiddleware):
    """
    Propagate ``X-Request-ID`` (client-supplied or generated), echo on response,
    attach ``X-Server-Time`` (UTC), and emit one access log line per HTTP response.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        header_rid = (request.headers.get("x-request-id") or "").strip()
        rid = header_rid if _looks_like_request_id(header_rid) else str(uuid.uuid4())
        request.state.request_id = rid

        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000

        response.headers["X-Request-ID"] = rid
        response.headers["X-Server-Time"] = utc_http_timestamp()

        client_host = request.client.host if request.client else "-"
        logger.info(
            "%s %s %s -> %s %.1fms request_id=%s",
            client_host,
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
            rid,
        )
        return response


def _looks_like_request_id(s: str) -> bool:
    if not s or len(s) > 128:
        return False
    for ch in s:
        if ch.isalnum() or ch in "-_.":
            continue
        return False
    return True
