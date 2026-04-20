"""Execute ops.sh synchronously; track background backfill PIDs."""

from __future__ import annotations

import os
import subprocess
import uuid
from threading import Lock
from typing import Any, cast

from ui.server.config import LOG_PATHS, OPS_SH, PROJECT_DIR, TIMEOUT_SYNC


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _truncate_known_log(log_key: str) -> str | None:
    """清空已知回填/日更日志文件。成功返回 None，失败返回错误说明。"""
    p = LOG_PATHS.get(log_key)
    if p is None:
        return "unknown log_key"
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("", encoding="utf-8")
    except OSError as e:
        return str(e)
    return None


def _job_alive(j: dict[str, Any]) -> bool:
    proc = cast(subprocess.Popen[Any] | None, j.get("_proc"))
    if proc is not None:
        return proc.poll() is None
    return _pid_alive(int(j["pid"]))


def run_sync(op: str, args: tuple[str, ...] = (), *, timeout: int = TIMEOUT_SYNC) -> tuple[int, str]:
    """Run ``ops.sh <op> [args...]``. Returns (exit_code, combined_output)."""
    if not OPS_SH.is_file():
        return -1, f"ops.sh not found: {OPS_SH}"
    argv = ["/bin/bash", str(OPS_SH), op] + [a for a in args if a and str(a).strip()]
    try:
        result = subprocess.run(
            argv,
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PATH": os.environ.get("PATH", "")},
        )
        out = (result.stdout or "") + (result.stderr or "")
        return result.returncode, (out.strip() or "(无输出)")
    except subprocess.TimeoutExpired:
        return -1, f"执行超时 ({timeout}s)"
    except Exception as e:
        return -1, str(e)


_jobs_lock = Lock()
_jobs: dict[str, dict[str, Any]] = {}


def _prune_jobs_locked() -> None:
    """
    移除已结束任务。必须对已结束的 ``Popen`` 调用 ``poll()``，否则子进程（bash）
    会以僵尸态残留，``os.kill(pid, 0)`` 在 Linux 上仍视为「存活」，导致任务永远「运行中」。
    """
    to_drop: list[str] = []
    for jid, j in list(_jobs.items()):
        proc = cast(subprocess.Popen[Any] | None, j.get("_proc"))
        if proc is not None:
            if proc.poll() is not None:
                to_drop.append(jid)
            continue
        if not _job_alive(j):
            to_drop.append(jid)
    for jid in to_drop:
        _jobs.pop(jid, None)


def active_backfill_job() -> dict[str, Any] | None:
    """Return one running backfill job if any."""
    with _jobs_lock:
        _prune_jobs_locked()
        for j in _jobs.values():
            if j.get("kind") == "backfill" and _job_alive(j):
                return {k: v for k, v in j.items() if k != "_proc"}
    return None


def start_backfill(ops_cmd: str, log_key: str, *, reset_log: bool = False) -> tuple[str | None, str]:
    """
    Start ``ops.sh <ops_cmd>`` in background (new session).
    Returns (job_id, error_message). error_message empty on success.
    """
    if ops_cmd not in ("backfill-daily", "backfill-index", "backfill-valuation"):
        return None, "invalid backfill command"
    with _jobs_lock:
        _prune_jobs_locked()
        if any(j.get("kind") == "backfill" and _job_alive(j) for j in _jobs.values()):
            return None, "已有回填任务在运行"
    if reset_log:
        terr = _truncate_known_log(log_key)
        if terr:
            return None, f"清空日志失败: {terr}"
    argv = ["/bin/bash", str(OPS_SH), ops_cmd]
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(PROJECT_DIR),
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "PATH": os.environ.get("PATH", "")},
        )
    except OSError as e:
        return None, str(e)
    job_id = uuid.uuid4().hex[:12]
    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "kind": "backfill",
            "ops_cmd": ops_cmd,
            "log_key": log_key,
            "pid": proc.pid,
            "_proc": proc,
        }
    return job_id, ""


def list_jobs() -> list[dict[str, Any]]:
    with _jobs_lock:
        _prune_jobs_locked()
        out = []
        for j in _jobs.values():
            row = {k: v for k, v in j.items() if k != "_proc"}
            out.append({**row, "alive": _job_alive(j)})
        return out
