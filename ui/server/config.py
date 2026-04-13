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
