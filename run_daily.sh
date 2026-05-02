#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${PROJECT_DIR}/scripts/daily_update.py"
LOG_DIR="${PROJECT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/daily_update.log"

# .venv 优先（本地开发），不存在则用系统 python（Docker 容器内）
if [[ -x "${PROJECT_DIR}/.venv/bin/python" ]]; then
  PY="${PROJECT_DIR}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="python3"
elif command -v python >/dev/null 2>&1; then
  PY="python"
else
  echo "[ERROR] No python found" >&2
  exit 1
fi

cd "${PROJECT_DIR}"

if [[ ! -f "${SCRIPT}" ]]; then
  echo "[ERROR] daily update script not found: ${SCRIPT}"
  exit 1
fi

DATE_ARG="${1:-}"

{
  echo
  echo "============================================================"
  echo "Run daily update at $(date '+%F %T %Z')"
  echo "============================================================"
  if [[ -n "${DATE_ARG}" ]]; then
    echo "[INFO] target date: ${DATE_ARG}"
    PYTHONPATH="${PROJECT_DIR}" "${PY}" "${SCRIPT}" "${DATE_ARG}"
  else
    PYTHONPATH="${PROJECT_DIR}" "${PY}" "${SCRIPT}"
  fi
} >> "${LOG_FILE}" 2>&1

echo "[OK] done. log: ${LOG_FILE}"
