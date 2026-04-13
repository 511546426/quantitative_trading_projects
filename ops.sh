#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/lcw/quantitative_trading_projects"
SCRIPTS_DIR="${PROJECT_DIR}/scripts"

# Docker container names (current convention)
DB_CONTAINERS=("clickhouse" "postgres" "redis")

DAILY_LOG="${SCRIPTS_DIR}/daily_update.log"
BACKFILL_DAILY_LOG="${SCRIPTS_DIR}/backfill_daily.log"
BACKFILL_INDEX_LOG="${SCRIPTS_DIR}/backfill_index.log"
BACKFILL_VALUATION_LOG="${SCRIPTS_DIR}/backfill_valuation.log"

ts() { date '+%F %T %Z'; }

usage() {
  cat <<'EOF'
Usage:
  ./ops.sh <command> [args]

Commands:
  status                 Show DB containers status + key ports
  start-db               Start clickhouse/postgres/redis containers
  stop-db                Stop clickhouse/postgres/redis containers
  restart-db             Restart clickhouse/postgres/redis containers

  daily [YYYYMMDD]       Run daily update (today or given date)
  backfill-daily         Run historical daily bars backfill (checkpointed)
  backfill-index [START] [END]  Run index_daily backfill (000001/300/905, PG checkpoint)
  backfill-valuation     Run historical valuation backfill (checkpointed)

  logs daily             Tail daily update log
  logs backfill-daily    Tail daily backfill log
  logs backfill-index    Tail index backfill log
  logs backfill-valuation Tail valuation backfill log

  web [port]             Start Streamlit Web 运维界面 (default port 8501)
  web-pro [port]         Start FastAPI + 机构级 React 控制台 API (default 8787)
                         前端开发: cd ui/ops-console && npm i && npm run dev
                         生产: cd ui/ops-console && npm run build 后同源托管静态资源

Examples:
  ./ops.sh status
  ./ops.sh start-db
  ./ops.sh daily
  ./ops.sh daily 20260304
  ./ops.sh logs daily
EOF
}

require_project() {
  if [[ ! -d "${PROJECT_DIR}" ]]; then
    echo "[ERROR] PROJECT_DIR not found: ${PROJECT_DIR}" >&2
    exit 1
  fi
  if [[ ! -d "${SCRIPTS_DIR}" ]]; then
    echo "[ERROR] scripts dir not found: ${SCRIPTS_DIR}" >&2
    exit 1
  fi
}

append_header() {
  local logfile="$1"
  shift
  {
    echo
    echo "============================================================"
    echo "$*"
    echo "Time: $(ts)"
    echo "============================================================"
  } >> "${logfile}"
}

docker_installed() {
  command -v docker >/dev/null 2>&1
}

container_exists() {
  local name="$1"
  docker inspect "${name}" >/dev/null 2>&1
}

container_running() {
  local name="$1"
  [[ "$(docker inspect -f '{{.State.Running}}' "${name}" 2>/dev/null || echo false)" == "true" ]]
}

start_db() {
  require_project
  if ! docker_installed; then
    echo "[ERROR] docker not found in PATH" >&2
    exit 1
  fi

  for c in "${DB_CONTAINERS[@]}"; do
    if ! container_exists "${c}"; then
      echo "[ERROR] docker container not found: ${c}" >&2
      echo "        Create it first (e.g. docker compose up -d)" >&2
      exit 1
    fi
  done

  for c in "${DB_CONTAINERS[@]}"; do
    if container_running "${c}"; then
      echo "[OK] ${c} already running"
    else
      echo "[..] starting ${c} ..."
      docker start "${c}" >/dev/null
      echo "[OK] started ${c}"
    fi
  done
}

stop_db() {
  require_project
  for c in "${DB_CONTAINERS[@]}"; do
    if container_exists "${c}" && container_running "${c}"; then
      echo "[..] stopping ${c} ..."
      docker stop "${c}" >/dev/null
      echo "[OK] stopped ${c}"
    else
      echo "[OK] ${c} not running"
    fi
  done
}

restart_db() {
  stop_db
  start_db
}

show_status() {
  require_project
  if ! docker_installed; then
    echo "[ERROR] docker not found in PATH" >&2
    exit 1
  fi

echo "== Docker containers =="
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' | (
  read -r header || true
  echo "${header}"
  for c in "${DB_CONTAINERS[@]}"; do
    docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' | awk -v name="${c}" '$1==name {print $0}'
  done
) || true

  echo
  echo "== Host ports (LISTEN) =="
  # best-effort, no dependency on grep/rg
  ss -ltn 2>/dev/null | python3 -c "import sys; ports={'5432','6379','8123','9000'}; \
lines=[l.rstrip() for l in sys.stdin if 'LISTEN' in l]; \
print('\\n'.join([l for l in lines if any(f':{p} ' in l or l.endswith(f':{p}') for p in ports)]))" || true
}

run_daily() {
  require_project
  start_db >/dev/null

  local date_arg="${1:-}"
  local runner="${PROJECT_DIR}/run_daily.sh"
  if [[ ! -x "${runner}" ]]; then
    echo "[ERROR] runner not executable: ${runner}" >&2
    exit 1
  fi

  append_header "${DAILY_LOG}" "DAILY UPDATE"
  if [[ -n "${date_arg}" ]]; then
    "${runner}" "${date_arg}" >> "${DAILY_LOG}" 2>&1 || {
      echo "[ERROR] daily failed. see log: ${DAILY_LOG}" >&2
      exit 1
    }
  else
    "${runner}" >> "${DAILY_LOG}" 2>&1 || {
      echo "[ERROR] daily failed. see log: ${DAILY_LOG}" >&2
      exit 1
    }
  fi
  echo "[OK] daily done. log: ${DAILY_LOG}"
}

run_backfill_daily() {
  require_project
  start_db >/dev/null

  local py="${PROJECT_DIR}/.venv/bin/python"
  local script="${SCRIPTS_DIR}/backfill_daily.py"
  if [[ ! -x "${py}" ]]; then
    echo "[ERROR] venv python not found: ${py}" >&2
    exit 1
  fi
  if [[ ! -f "${script}" ]]; then
    echo "[ERROR] script not found: ${script}" >&2
    exit 1
  fi

  append_header "${BACKFILL_DAILY_LOG}" "BACKFILL DAILY (historical) — checkpointed"
  (cd "${PROJECT_DIR}" && "${py}" "${script}") >> "${BACKFILL_DAILY_LOG}" 2>&1
  echo "[OK] backfill-daily finished. log: ${BACKFILL_DAILY_LOG}"
}

run_backfill_index() {
  require_project
  start_db >/dev/null

  local py="${PROJECT_DIR}/.venv/bin/python"
  local script="${SCRIPTS_DIR}/backfill_index.py"
  if [[ ! -x "${py}" ]]; then
    echo "[ERROR] venv python not found: ${py}" >&2
    exit 1
  fi
  if [[ ! -f "${script}" ]]; then
    echo "[ERROR] script not found: ${script}" >&2
    exit 1
  fi

  local start="${1:-20200101}"
  local end="${2:-}"
  if [[ -z "${end}" ]]; then
    end="$(date '+%Y%m%d')"
  fi

  append_header "${BACKFILL_INDEX_LOG}" "BACKFILL INDEX_DAILY ${start} ~ ${end}"
  (cd "${PROJECT_DIR}" && "${py}" "${script}" "${start}" "${end}") >> "${BACKFILL_INDEX_LOG}" 2>&1
  echo "[OK] backfill-index finished. log: ${BACKFILL_INDEX_LOG}"
}

run_backfill_valuation() {
  require_project
  start_db >/dev/null

  local py="${PROJECT_DIR}/.venv/bin/python"
  local script="${SCRIPTS_DIR}/backfill_valuation.py"
  if [[ ! -x "${py}" ]]; then
    echo "[ERROR] venv python not found: ${py}" >&2
    exit 1
  fi
  if [[ ! -f "${script}" ]]; then
    echo "[ERROR] script not found: ${script}" >&2
    exit 1
  fi

  append_header "${BACKFILL_VALUATION_LOG}" "BACKFILL VALUATION (historical) — checkpointed"
  (cd "${PROJECT_DIR}" && "${py}" "${script}") >> "${BACKFILL_VALUATION_LOG}" 2>&1
  echo "[OK] backfill-valuation finished. log: ${BACKFILL_VALUATION_LOG}"
}

tail_logs() {
  local which="${1:-}"
  case "${which}" in
    daily)
      tail -f "${DAILY_LOG}"
      ;;
    backfill-daily)
      tail -f "${BACKFILL_DAILY_LOG}"
      ;;
    backfill-index)
      tail -f "${BACKFILL_INDEX_LOG}"
      ;;
    backfill-valuation)
      tail -f "${BACKFILL_VALUATION_LOG}"
      ;;
    *)
      echo "[ERROR] unknown logs target: ${which}" >&2
      echo "        use: daily | backfill-daily | backfill-index | backfill-valuation" >&2
      exit 1
      ;;
  esac
}

main() {
  local cmd="${1:-}"
  shift || true

  case "${cmd}" in
    ""|-h|--help|help)
      usage
      ;;
    status)
      show_status
      ;;
    start-db)
      start_db
      ;;
    stop-db)
      stop_db
      ;;
    restart-db)
      restart_db
      ;;
    daily)
      run_daily "${1:-}"
      ;;
    backfill-daily)
      run_backfill_daily
      ;;
    backfill-index)
      run_backfill_index "${1:-}" "${2:-}"
      ;;
    backfill-valuation)
      run_backfill_valuation
      ;;
    logs)
      tail_logs "${1:-}"
      ;;
    web)
      port="${1:-8501}"
      py="${PROJECT_DIR}/.venv/bin/python"
      dash="${PROJECT_DIR}/ui/ops_dashboard.py"
      if [[ ! -x "${py}" ]] || [[ ! -f "${dash}" ]]; then
        echo "[ERROR] venv or ui/ops_dashboard.py not found" >&2
        exit 1
      fi
      echo "[INFO] 启动 Web 运维界面: http://127.0.0.1:${port}"
      exec "${py}" -m streamlit run "${dash}" --server.port "${port}" --server.address 127.0.0.1
      ;;
    web-pro)
      port="${1:-8787}"
      py="${PROJECT_DIR}/.venv/bin/python"
      if [[ ! -x "${py}" ]]; then
        echo "[ERROR] venv python not found: ${py}" >&2
        exit 1
      fi
      echo "[INFO] Ops API (FastAPI): http://127.0.0.1:${port}/api/health"
      echo "[INFO] 若已 npm run build，则静态 UI 同源根路径 /"
      echo "[INFO] 开发前端: 另开终端 cd ui/ops-console && npm run dev （Vite 代理 /api → 本端口）"
      cd "${PROJECT_DIR}" && exec env PYTHONPATH="${PROJECT_DIR}" \
        "${py}" -m uvicorn ui.server.app:app --host 127.0.0.1 --port "${port}" \
        --timeout-graceful-shutdown 5
      ;;
    *)
      echo "[ERROR] unknown command: ${cmd}" >&2
      echo >&2
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"

