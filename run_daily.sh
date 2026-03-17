#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/lcw/quantitative_trading_projects"
VENV_PY="${PROJECT_DIR}/.venv/bin/python"
SCRIPT="${PROJECT_DIR}/scripts/daily_update.py"
LOG_FILE="${PROJECT_DIR}/scripts/daily_update.log"

cd "${PROJECT_DIR}"

if [[ ! -x "${VENV_PY}" ]]; then
  echo "[ERROR] venv python not found: ${VENV_PY}"
  echo "        Please create venv and install deps first."
  exit 1
fi

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
    "${VENV_PY}" "${SCRIPT}" "${DATE_ARG}"
  else
    "${VENV_PY}" "${SCRIPT}"
  fi
} >> "${LOG_FILE}" 2>&1

echo "[OK] done. log: ${LOG_FILE}"
