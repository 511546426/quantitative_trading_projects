"""Runtime config for the ops API server."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
OPS_SH = PROJECT_DIR / "ops.sh"
SCRIPTS_DIR = PROJECT_DIR / "scripts"

LOG_PATHS: dict[str, Path] = {
    "daily": SCRIPTS_DIR / "daily_update.log",
    "backfill-daily": SCRIPTS_DIR / "backfill_daily.log",
    "backfill-index": SCRIPTS_DIR / "backfill_index.log",
    "backfill-valuation": SCRIPTS_DIR / "backfill_valuation.log",
}

TIMEOUT_SYNC = int(os.environ.get("QUANT_OPS_SYNC_TIMEOUT", "120"))
# If set, HTTP (except /api/health) and WebSocket require X-API-Key or ?token=
API_KEY = os.environ.get("QUANT_OPS_API_KEY", "").strip()
# Optional: CI/git SHA shown in /api/health and /api/meta for deployment traceability
BUILD_ID = os.environ.get("QUANT_OPS_BUILD_ID", "").strip()

# Uvicorn / ops.sh: default loopback; set QUANT_OPS_BIND=0.0.0.0 only when you intend LAN exposure
BIND_HOST = os.environ.get("QUANT_OPS_BIND", "127.0.0.1").strip() or "127.0.0.1"

# Optional comma-separated client IPs; when non-empty, HTTP + WS rejected if peer not listed
_ips_raw = os.environ.get("QUANT_OPS_ALLOWED_IPS", "").strip()
ALLOWED_CLIENT_IPS: frozenset[str] | None = (
    frozenset(p.strip() for p in _ips_raw.split(",") if p.strip()) if _ips_raw else None
)

# TrustedHostMiddleware: Host header must match (mitigate Host-header attacks). Use "*" to disable.
_th_raw = os.environ.get("QUANT_OPS_TRUSTED_HOSTS", "").strip()
if _th_raw == "*":
    TRUSTED_HOSTS: list[str] = ["*"]
elif _th_raw:
    TRUSTED_HOSTS = [h.strip() for h in _th_raw.split(",") if h.strip()]
else:
    # IPv6 literal Host (e.g. [::1]:8787) is not matched here; set QUANT_OPS_TRUSTED_HOSTS if needed.
    TRUSTED_HOSTS = ["127.0.0.1", "localhost", "*.localhost"]
