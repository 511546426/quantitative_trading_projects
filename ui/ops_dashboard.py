"""
量化运维 Web 控制台 — 基于 Streamlit

通过浏览器操作：起停容器、每日更新、历史回填、查看日志。

启动:
    streamlit run ui/ops_dashboard.py --server.port 8501
    或: ./ops.sh web  (若已集成)
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
OPS_SH = PROJECT_DIR / "ops.sh"
LOGS = {
    "daily": PROJECT_DIR / "scripts" / "daily_update.log",
    "backfill-daily": PROJECT_DIR / "scripts" / "backfill_daily.log",
    "backfill-index": PROJECT_DIR / "scripts" / "backfill_index.log",
    "backfill-valuation": PROJECT_DIR / "scripts" / "backfill_valuation.log",
}

# 短任务超时(秒)
TIMEOUT_SHORT = 120


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def start_backfill_background(cmd: str, extra: list[str] | None = None) -> int | None:
    """后台启动 ops.sh 子命令，返回子进程 PID；失败返回 None。"""
    argv = ["/bin/bash", str(OPS_SH), cmd]
    if extra:
        argv.extend(extra)
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(PROJECT_DIR),
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "PATH": os.environ.get("PATH", "")},
        )
        return proc.pid
    except OSError:
        return None


def run_ops(cmd: str, *args: str, timeout: int = TIMEOUT_SHORT) -> tuple[int, str]:
    """执行 ops.sh 子命令，返回 (exit_code, combined_output)"""
    argv = ["/bin/bash", str(OPS_SH), cmd] + [a for a in args if a and str(a).strip()]
    try:
        result = subprocess.run(
            argv,
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**subprocess.os.environ, "PATH": subprocess.os.environ.get("PATH", "")},
        )
        out = (result.stdout or "") + (result.stderr or "")
        return result.returncode, out.strip() or "(无输出)"
    except subprocess.TimeoutExpired:
        return -1, f"执行超时 ({timeout}s)"
    except FileNotFoundError:
        return -1, f"未找到脚本: {OPS_SH}"
    except Exception as e:
        return -1, str(e)


def read_log_tail(log_key: str, lines: int = 200) -> str:
    path = LOGS.get(log_key)
    if not path or not path.exists():
        return f"(日志文件不存在: {path})"
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            tail = f.readlines()[-lines:]
        return "".join(tail) if tail else "(空)"
    except Exception as e:
        return str(e)


def main() -> None:
    import streamlit as st

    st.set_page_config(page_title="量化运维", layout="wide")
    st.title("量化运维控制台")
    st.caption(f"项目目录: `{PROJECT_DIR}`")

    if "log_refresh_ver" not in st.session_state:
        st.session_state["log_refresh_ver"] = 0
    if "backfill_pid" not in st.session_state:
        st.session_state["backfill_pid"] = None
    if "backfill_log_key" not in st.session_state:
        st.session_state["backfill_log_key"] = None

    bp = st.session_state.get("backfill_pid")
    if bp and not _pid_alive(bp):
        st.session_state["backfill_pid"] = None
        st.session_state["backfill_log_key"] = None

    @st.fragment(run_every=3)
    def _live_backfill_log() -> None:
        pid = st.session_state.get("backfill_pid")
        lk = st.session_state.get("backfill_log_key")
        if not pid or not lk:
            return
        if not _pid_alive(pid):
            st.session_state["backfill_pid"] = None
            st.session_state["backfill_log_key"] = None
            st.session_state["log_refresh_ver"] = int(
                st.session_state.get("log_refresh_ver", 0)
            ) + 1
            st.success("回填进程已结束。请在下方「查看日志」选择对应文件查看完整日志。")
            return
        st.caption(f"回填进行中 · PID **{pid}** · 每 3 秒刷新")
        st.code(read_log_tail(lk, 150), language="text")

    # 输出展示区（所有按钮的结果都写到这里）
    output_placeholder = st.empty()

    # 容器操作
    st.subheader("数据库容器")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("查看状态", key="status"):
            with st.spinner("执行中..."):
                code, out = run_ops("status")
            with output_placeholder.container():
                st.code(out, language="text")
                st.caption(f"退出码: {code}")
    with c2:
        if st.button("启动容器", key="start"):
            with st.spinner("启动中..."):
                code, out = run_ops("start-db")
            with output_placeholder.container():
                st.code(out, language="text")
                st.caption(f"退出码: {code}")
    with c3:
        if st.button("停止容器", key="stop"):
            with st.spinner("停止中..."):
                code, out = run_ops("stop-db")
            with output_placeholder.container():
                st.code(out, language="text")
                st.caption(f"退出码: {code}")

    # 每日更新
    st.subheader("每日更新")
    dc1, dc2 = st.columns([1, 3])
    with dc1:
        date_arg = st.text_input("日期 (可选，YYYYMMDD)", placeholder="留空=今天")
    with dc2:
        if st.button("执行每日更新", key="daily"):
            with st.spinner("更新中，请稍候..."):
                code, out = run_ops("daily", date_arg) if date_arg.strip() else run_ops("daily")
            st.session_state["log_refresh_ver"] = int(
                st.session_state.get("log_refresh_ver", 0)
            ) + 1
            with output_placeholder.container():
                st.code(out, language="text")
                st.caption(f"退出码: {code}")
                log_content = read_log_tail("daily", lines=150)
                st.markdown("**最新日志 (daily_update.log)：**")
                st.code(log_content, language="text")

    # 历史回填（后台进程 + 定时刷新，避免整页卡死）
    st.subheader("历史回填")
    st.warning(
        "回填可能需数十分钟。任务在**后台**运行，页面可继续操作；"
        "请勿同时点两个回填。关闭浏览器不会停止任务（进程仍在服务器上跑）。"
    )
    b1, b2, b3 = st.columns(3)
    with b1:
        if st.button("回填日K线", key="backfill-daily"):
            if st.session_state.get("backfill_pid") and _pid_alive(
                st.session_state["backfill_pid"]
            ):
                st.error("已有回填任务在运行，请等待结束或到终端用 ps/kill 处理。")
            else:
                pid = start_backfill_background("backfill-daily")
                if pid is None:
                    st.error("启动失败，请检查终端权限与 ops.sh。")
                else:
                    st.session_state["backfill_pid"] = pid
                    st.session_state["backfill_log_key"] = "backfill-daily"
                    st.session_state["log_refresh_ver"] = int(
                        st.session_state.get("log_refresh_ver", 0)
                    ) + 1
                    with output_placeholder.container():
                        st.success(f"已在后台启动日K线回填 · PID `{pid}`")
    with b2:
        if st.button("回填指数日线", key="backfill-index"):
            if st.session_state.get("backfill_pid") and _pid_alive(
                st.session_state["backfill_pid"]
            ):
                st.error("已有回填任务在运行，请等待结束或到终端用 ps/kill 处理。")
            else:
                pid = start_backfill_background("backfill-index")
                if pid is None:
                    st.error("启动失败，请检查终端权限与 ops.sh。")
                else:
                    st.session_state["backfill_pid"] = pid
                    st.session_state["backfill_log_key"] = "backfill-index"
                    st.session_state["log_refresh_ver"] = int(
                        st.session_state.get("log_refresh_ver", 0)
                    ) + 1
                    with output_placeholder.container():
                        st.success(f"已在后台启动指数回填 · PID `{pid}`（默认 20200101～今日）")
    with b3:
        if st.button("回填估值数据", key="backfill-valuation"):
            if st.session_state.get("backfill_pid") and _pid_alive(
                st.session_state["backfill_pid"]
            ):
                st.error("已有回填任务在运行，请等待结束或到终端用 ps/kill 处理。")
            else:
                pid = start_backfill_background("backfill-valuation")
                if pid is None:
                    st.error("启动失败，请检查终端权限与 ops.sh。")
                else:
                    st.session_state["backfill_pid"] = pid
                    st.session_state["backfill_log_key"] = "backfill-valuation"
                    st.session_state["log_refresh_ver"] = int(
                        st.session_state.get("log_refresh_ver", 0)
                    ) + 1
                    with output_placeholder.container():
                        st.success(f"已在后台启动估值回填 · PID `{pid}`")

    if st.session_state.get("backfill_pid") and _pid_alive(
        st.session_state["backfill_pid"]
    ):
        st.markdown("##### 回填实时进度（日志尾部）")
        _live_backfill_log()

    # 查看日志 — 使用 st.code 只读展示，避免 st.text_area + key 导致 session_state 不刷新
    st.subheader("查看日志")

    log_choice = st.selectbox(
        "选择日志",
        ["daily", "backfill-daily", "backfill-index", "backfill-valuation"],
        format_func=lambda x: {
            "daily": "每日更新 (daily_update.log)",
            "backfill-daily": "日K线回填 (backfill_daily.log)",
            "backfill-index": "指数日线回填 (backfill_index.log)",
            "backfill-valuation": "估值回填 (backfill_valuation.log)",
        }[x],
    )
    tail_lines = st.slider("显示最后 N 行", 50, 500, 100)
    if st.button("刷新日志", key="refresh-log"):
        st.session_state["log_refresh_ver"] = int(
            st.session_state.get("log_refresh_ver", 0)
        ) + 1

    content = read_log_tail(log_choice, lines=tail_lines)
    log_path = LOGS.get(log_choice)
    st.caption(f"当前文件: `{log_path}`")
    st.code(content, language="text")


if __name__ == "__main__":
    main()
