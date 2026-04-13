"""Shared FastAPI dependencies."""

from __future__ import annotations

from fastapi import Header, HTTPException

from ui.server.config import API_KEY


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if not API_KEY:
        return
    if (x_api_key or "").strip() != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")
