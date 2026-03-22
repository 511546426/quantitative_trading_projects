"""
量化运维 Web 控制台 — 基于 Streamlit

通过浏览器操作：起停容器、每日更新、历史回填、查看日志。

启动:
    streamlit run ui/ops_dashboard.py --server.port 8501
    或: ./ops.sh web  (若已集成)
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
OPS_SH = PROJECT_DIR / "ops.sh"
LOGS = {
    "daily": PROJECT_DIR / "scripts" / "daily_update.log",
    "backfill-daily": PROJECT_DIR / "scripts" / "backfill_daily.log",
    "backfill-valuation": PROJECT_DIR / "scripts" / "backfill_valuation.log",
}

# 短任务超时(秒)，长任务(回填)超时
TIMEOUT_SHORT = 120
TIMEOUT_BACKFILL = 7200  # 2 小时


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
            with output_placeholder.container():
                st.code(out, language="text")
                st.caption(f"退出码: {code}")

    # 历史回填
    st.subheader("历史回填")
    st.warning("回填任务耗时长（数十分钟），执行期间请勿关闭页面。")
    b1, b2 = st.columns(2)
    with b1:
        if st.button("回填日K线", key="backfill-daily"):
            with st.spinner("回填中，请耐心等待..."):
                code, out = run_ops("backfill-daily", timeout=TIMEOUT_BACKFILL)
            with output_placeholder.container():
                st.code(out, language="text")
                st.caption(f"退出码: {code}")
    with b2:
        if st.button("回填估值数据", key="backfill-valuation"):
            with st.spinner("回填中，请耐心等待..."):
                code, out = run_ops("backfill-valuation", timeout=TIMEOUT_BACKFILL)
            with output_placeholder.container():
                st.code(out, language="text")
                st.caption(f"退出码: {code}")

    # 查看日志
    st.subheader("查看日志")
    log_choice = st.selectbox(
        "选择日志",
        ["daily", "backfill-daily", "backfill-valuation"],
        format_func=lambda x: {
            "daily": "每日更新 (daily_update.log)",
            "backfill-daily": "日K线回填 (backfill_daily.log)",
            "backfill-valuation": "估值回填 (backfill_valuation.log)",
        }[x],
    )
    tail_lines = st.slider("显示最后 N 行", 50, 500, 100)
    st.button("刷新日志", key="refresh-log")
    content = read_log_tail(log_choice, lines=tail_lines)
    st.text_area("日志内容", content, height=400, key="log-area")


if __name__ == "__main__":
    main()
