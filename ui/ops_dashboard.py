"""
量化运维 Web 控制台 — 基于 Streamlit（机构级控制台布局）

通过浏览器：起停容器、每日更新、历史回填、查看日志。

启动:
    streamlit run ui/ops_dashboard.py --server.port 8501
"""
from __future__ import annotations

import html
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
OPS_SH = PROJECT_DIR / "ops.sh"
LOGS = {
    "daily": PROJECT_DIR / "scripts" / "daily_update.log",
    "backfill-daily": PROJECT_DIR / "scripts" / "backfill_daily.log",
    "backfill-index": PROJECT_DIR / "scripts" / "backfill_index.log",
    "backfill-valuation": PROJECT_DIR / "scripts" / "backfill_valuation.log",
}

TIMEOUT_SHORT = 120

# —— 设计体系：深色控制台 + 窄色语义（Bloomberg / 终端类产品的信息密度取向）——
THEME_CSS = """
:root {
  --bg0: #0a0e14;
  --bg1: #0f1419;
  --bg2: #151b24;
  --border: #243044;
  --text: #e8eef7;
  --muted: #8b9cb3;
  --accent: #3d9df0;
  --accent-dim: rgba(61, 157, 240, 0.15);
  --ok: #3dd68c;
  --warn: #f0b429;
  --err: #f25c54;
  --card: #111822;
  --mono: ui-monospace, "Cascadia Code", "SF Mono", Consolas, Menlo, monospace;
  --sans: ui-sans-serif, system-ui, "Segoe UI", Roboto, "PingFang SC",
          "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
}
[data-testid="stAppViewContainer"],
[data-testid="stHeader"] {
  background: var(--bg0) !important;
}
[data-testid="stToolbar"] { visibility: hidden; height: 0; }
.block-container {
  padding-top: 1.25rem !important;
  padding-bottom: 2rem !important;
  max-width: 1480px !important;
}
section[data-testid="stSidebar"] {
  background: linear-gradient(180deg, var(--bg1) 0%, var(--bg0) 100%) !important;
  border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"] .block-container { padding-top: 1.5rem; }
h1, h2, h3 { font-family: var(--sans) !important; letter-spacing: -0.02em; }
.stMarkdown, .stText, label, p, span { color: var(--text) !important; }
[data-testid="stCaptionContainer"] { color: var(--muted) !important; }

.q-brand {
  font-family: var(--sans);
  font-weight: 700;
  font-size: 1.05rem;
  letter-spacing: 0.06em;
  color: var(--text);
  text-transform: uppercase;
}
.q-brand span { color: var(--accent) !important; font-weight: 800; }
.q-sub { font-size: 0.78rem; color: var(--muted); margin-top: 0.35rem; line-height: 1.45; }

.metric-row { display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 0.5rem 0 1rem; }
.metric-card {
  flex: 1 1 140px;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 0.85rem 1rem;
  min-width: 140px;
  box-shadow: 0 1px 0 rgba(255,255,255,0.04) inset;
}
.metric-card .k {
  font-size: 0.68rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  font-family: var(--sans);
}
.metric-card .v {
  font-size: 1.35rem;
  font-weight: 600;
  color: var(--text);
  margin-top: 0.2rem;
  font-variant-numeric: tabular-nums;
  font-family: var(--sans);
}
.metric-card .s { font-size: 0.72rem; color: var(--muted); margin-top: 0.35rem; }
.metric-card.accent-left { border-left: 3px solid var(--accent); }
.metric-card.ok-left { border-left: 3px solid var(--ok); }
.metric-card.warn-left { border-left: 3px solid var(--warn); }
.metric-card.err-left { border-left: 3px solid var(--err); }

.panel-title {
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 0.5rem;
  font-family: var(--sans);
}
.console-wrap {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 8px;
  min-height: 280px;
  max-height: 62vh;
  overflow: auto;
  font-family: var(--mono);
  font-size: 0.78rem;
  line-height: 1.45;
  padding: 0.85rem 1rem;
}
.log-scroll {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 8px;
  max-height: 52vh;
  overflow: auto;
  font-family: var(--mono);
  font-size: 0.78rem;
  line-height: 1.5;
  padding: 0.85rem 1rem;
}
.log-line { color: #c8d4e4; }
.log-err { color: #ff8a80; }
.log-warn { color: #ffe082; }
.log-info { color: #80cbc4; }

div.stButton > button {
  font-family: var(--sans) !important;
  font-weight: 600 !important;
  border-radius: 6px !important;
  border: 1px solid var(--border) !important;
  background: var(--bg2) !important;
  color: var(--text) !important;
  transition: border-color 0.15s, background 0.15s !important;
}
div.stButton > button:hover {
  border-color: var(--accent) !important;
  background: var(--accent-dim) !important;
}
[data-baseweb="tab"] { font-family: var(--sans) !important; color: var(--muted) !important; }
[data-baseweb="tab"][aria-selected="true"] {
  color: var(--accent) !important;
  border-bottom-color: var(--accent) !important;
}
.stTabs [data-baseweb="tab-list"] {
  gap: 0.5rem;
  border-bottom-color: var(--border) !important;
}
.stSlider label { color: var(--muted) !important; }
.stTextInput input, .stSelectbox div[data-baseweb="select"] > div {
  background-color: var(--bg2) !important;
  border-color: var(--border) !important;
  color: var(--text) !important;
}
"""


def _inject_css() -> None:
    import streamlit as st

    st.markdown(f"<style>{THEME_CSS}</style>", unsafe_allow_html=True)


def _metric_cards_html(items: list[tuple[str, str, str, str]]) -> str:
    """items: (key_line, value_line, sub_line, modifier_class)"""
    parts = ['<div class="metric-row">']
    for k, v, s, mod in items:
        parts.append(f'<div class="metric-card {mod}">')
        parts.append(f'<div class="k">{html.escape(k)}</div>')
        parts.append(f'<div class="v">{html.escape(v)}</div>')
        if s:
            parts.append(f'<div class="s">{html.escape(s)}</div>')
        parts.append("</div>")
    parts.append("</div>")
    return "".join(parts)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def start_backfill_background(cmd: str, extra: list[str] | None = None) -> int | None:
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


def _render_log_risk_colored(text: str) -> None:
    import streamlit as st

    lines = text.splitlines()
    chunks: list[str] = []
    for line in lines:
        esc = html.escape(line)
        lower = line.upper()
        if re.search(r"\b(ERROR|CRITICAL|FATAL|EXCEPTION|TRACEBACK)\b", lower):
            cls = "log-err"
        elif re.search(r"\b(WARN|WARNING)\b", lower) or "警告" in line:
            cls = "log-warn"
        elif re.search(r"\b(INFO|DEBUG)\b", lower) or "INFO" in line:
            cls = "log-info"
        else:
            cls = "log-line"
        chunks.append(f'<span class="{cls}">{esc}</span>')
    body = "<br>\n".join(chunks)
    st.markdown(f'<div class="log-scroll">{body}</div>', unsafe_allow_html=True)


def _record_last_ops(cmd: str, code: int, out: str) -> None:
    import streamlit as st

    st.session_state["last_ops"] = {
        "cmd": cmd,
        "code": code,
        "out": out,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _sidebar() -> None:
    import streamlit as st

    with st.sidebar:
        st.markdown(
            '<p class="q-brand">Quant <span>Ops</span></p>'
            '<p class="q-sub">数据管线 · 容器编排 · 任务与日志统一入口<br>'
            "Institutional console layout</p>",
            unsafe_allow_html=True,
        )
        st.divider()
        st.markdown("**环境**")
        st.caption(f"Python `{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}`")
        st.caption(f"工作区 `{PROJECT_DIR}`")
        st.divider()
        st.markdown("**说明**")
        st.caption(
            "回填任务在服务器后台运行；关闭页面不会终止进程。"
            " 执行结果固定显示在右侧「执行控制台」。"
        )


def main() -> None:
    import streamlit as st

    st.set_page_config(
        page_title="Quant Ops · 控制台",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={"About": "量化运维控制台 — Streamlit"},
    )
    _inject_css()
    _sidebar()

    if "log_refresh_ver" not in st.session_state:
        st.session_state["log_refresh_ver"] = 0
    if "backfill_pid" not in st.session_state:
        st.session_state["backfill_pid"] = None
    if "backfill_log_key" not in st.session_state:
        st.session_state["backfill_log_key"] = None
    if "last_ops" not in st.session_state:
        st.session_state["last_ops"] = None

    bp = st.session_state.get("backfill_pid")
    if bp and not _pid_alive(bp):
        st.session_state["backfill_pid"] = None
        st.session_state["backfill_log_key"] = None

    st.markdown(
        "## 运维控制台",
        help=None,
    )
    st.caption("统一调度 · 可审计输出 · 高密度信息布局")

    now = datetime.now().strftime("%H:%M:%S")
    bf_pid = st.session_state.get("backfill_pid")
    bf_live = bf_pid and _pid_alive(bf_pid)
    last = st.session_state.get("last_ops")

    hero_items = [
        (
            "Session time",
            now,
            "本地时钟",
            "accent-left",
        ),
        (
            "Backfill",
            "运行中" if bf_live else "空闲",
            f"PID {bf_pid}" if bf_live else "无后台回填",
            "warn-left" if bf_live else "ok-left",
        ),
        (
            "Last command",
            (last or {}).get("cmd", "—")[:18] + ("…" if len((last or {}).get("cmd", "")) > 18 else ""),
            (last or {}).get("ts", "尚未执行") or "",
            "accent-left",
        ),
        (
            "Last exit",
            str((last or {}).get("code", "—")),
            "0=成功" if (last or {}).get("code") == 0 else ("非0=失败/超时" if last else ""),
            "ok-left" if (last or {}).get("code") == 0 else ("err-left" if last else "accent-left"),
        ),
    ]
    st.markdown(_metric_cards_html(hero_items), unsafe_allow_html=True)

    left, right = st.columns([1.15, 1.0], gap="large")

    with right:
        st.markdown('<p class="panel-title">执行控制台</p>', unsafe_allow_html=True)
        output_placeholder = st.empty()
        with output_placeholder.container():
            st.markdown(
                '<div class="console-wrap"><span class="log-muted" style="color:#8b9cb3">'
                "等待操作 — 左侧执行任务后，此处显示 stdout/stderr 与退出码。</span></div>",
                unsafe_allow_html=True,
            )

    def show_console(code: int, out: str) -> None:
        esc = html.escape(out)
        badge = "color:var(--ok)" if code == 0 else "color:var(--err)"
        header = (
            f'<div style="margin-bottom:0.65rem;font-size:0.72rem;{badge}">exit {code}</div>'
            '<pre style="margin:0;white-space:pre-wrap;word-break:break-word;'
            'color:#c8d4e4;font-family:inherit">'
            f"{esc}</pre>"
        )
        with output_placeholder.container():
            st.markdown(f'<div class="console-wrap">{header}</div>', unsafe_allow_html=True)

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
            st.success("回填进程已结束。请在「日志中心」查看完整日志。")
            return
        st.caption(f"回填进行中 · PID **{pid}** · 每 3 秒刷新")
        _render_log_risk_colored(read_log_tail(lk, 150))

    with left:
        t_overview, t_infra, t_pipeline, t_logs = st.tabs(
            ["总览", "数据基建", "日更与回填", "日志中心"]
        )

        with t_overview:
            st.markdown("**快捷健康检查**")
            o1, o2 = st.columns(2)
            with o1:
                if st.button("刷新容器状态", use_container_width=True, key="ov_status"):
                    with st.spinner("查询中…"):
                        code, out = run_ops("status")
                    _record_last_ops("status", code, out)
                    show_console(code, out)
            with o2:
                if st.button("打开执行控制台说明", use_container_width=True, key="ov_hint"):
                    show_console(
                        0,
                        "所有「执行」类按钮的输出都会写入右侧「执行控制台」。\n"
                        "日志类内容在「日志中心」查看，可按 ERROR/WARN 语义着色。\n"
                        "回填为后台任务：请勿并行启动两个回填。",
                    )
            st.info("日常建议顺序：**状态 → 启动容器 → 每日更新**；大范围历史用「日更与回填」。")

        with t_infra:
            st.markdown("**数据库容器**")
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("查看状态", use_container_width=True, key="status"):
                    with st.spinner("执行中…"):
                        code, out = run_ops("status")
                    _record_last_ops("status", code, out)
                    show_console(code, out)
            with c2:
                if st.button("启动容器", use_container_width=True, key="start"):
                    with st.spinner("启动中…"):
                        code, out = run_ops("start-db")
                    _record_last_ops("start-db", code, out)
                    show_console(code, out)
            with c3:
                if st.button("停止容器", use_container_width=True, key="stop"):
                    with st.spinner("停止中…"):
                        code, out = run_ops("stop-db")
                    _record_last_ops("stop-db", code, out)
                    show_console(code, out)

        with t_pipeline:
            st.markdown("**每日更新**")
            dc1, dc2 = st.columns([1, 2])
            with dc1:
                date_arg = st.text_input("日期 (可选)", placeholder="YYYYMMDD 留空=今天")
            with dc2:
                st.write("")  # vertical align
                if st.button("执行每日更新", use_container_width=True, key="daily"):
                    with st.spinner("更新中…"):
                        code, out = (
                            run_ops("daily", date_arg)
                            if date_arg.strip()
                            else run_ops("daily")
                        )
                    _record_last_ops("daily", code, out)
                    st.session_state["log_refresh_ver"] = int(
                        st.session_state.get("log_refresh_ver", 0)
                    ) + 1
                    log_content = read_log_tail("daily", lines=150)
                    show_console(code, f"{out}\n\n--- daily_update.log (tail) ---\n{log_content}")

            st.markdown("**历史回填（后台）**")
            st.caption(
                "可能持续数十分钟；与页面无关。请勿同时启动两个回填任务。"
            )
            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("回填日K线", use_container_width=True, key="backfill-daily"):
                    if st.session_state.get("backfill_pid") and _pid_alive(
                        st.session_state["backfill_pid"]
                    ):
                        st.error("已有回填任务在运行。")
                    else:
                        pid = start_backfill_background("backfill-daily")
                        if pid is None:
                            st.error("启动失败，请检查 ops.sh 与权限。")
                        else:
                            st.session_state["backfill_pid"] = pid
                            st.session_state["backfill_log_key"] = "backfill-daily"
                            st.session_state["log_refresh_ver"] = int(
                                st.session_state.get("log_refresh_ver", 0)
                            ) + 1
                            show_console(0, f"后台已启动日K线回填 · PID {pid}")
            with b2:
                if st.button("回填指数日线", use_container_width=True, key="backfill-index"):
                    if st.session_state.get("backfill_pid") and _pid_alive(
                        st.session_state["backfill_pid"]
                    ):
                        st.error("已有回填任务在运行。")
                    else:
                        pid = start_backfill_background("backfill-index")
                        if pid is None:
                            st.error("启动失败。")
                        else:
                            st.session_state["backfill_pid"] = pid
                            st.session_state["backfill_log_key"] = "backfill-index"
                            st.session_state["log_refresh_ver"] = int(
                                st.session_state.get("log_refresh_ver", 0)
                            ) + 1
                            show_console(0, f"后台已启动指数回填 · PID {pid}（默认区间见脚本）")
            with b3:
                if st.button("回填估值数据", use_container_width=True, key="backfill-valuation"):
                    if st.session_state.get("backfill_pid") and _pid_alive(
                        st.session_state["backfill_pid"]
                    ):
                        st.error("已有回填任务在运行。")
                    else:
                        pid = start_backfill_background("backfill-valuation")
                        if pid is None:
                            st.error("启动失败。")
                        else:
                            st.session_state["backfill_pid"] = pid
                            st.session_state["backfill_log_key"] = "backfill-valuation"
                            st.session_state["log_refresh_ver"] = int(
                                st.session_state.get("log_refresh_ver", 0)
                            ) + 1
                            show_console(0, f"后台已启动估值回填 · PID {pid}")

            if st.session_state.get("backfill_pid") and _pid_alive(
                st.session_state["backfill_pid"]
            ):
                st.markdown("##### 回填实时跟踪")
                _live_backfill_log()

        with t_logs:
            st.markdown("**日志浏览**")
            log_choice = st.selectbox(
                "日志文件",
                ["daily", "backfill-daily", "backfill-index", "backfill-valuation"],
                format_func=lambda x: {
                    "daily": "每日更新 (daily_update.log)",
                    "backfill-daily": "日K线回填 (backfill_daily.log)",
                    "backfill-index": "指数日线回填 (backfill_index.log)",
                    "backfill-valuation": "估值回填 (backfill_valuation.log)",
                }[x],
                label_visibility="collapsed",
            )
            tail_lines = st.slider("尾部行数", 50, 500, 120)
            lc1, lc2 = st.columns([1, 3])
            with lc1:
                if st.button("刷新", use_container_width=True, key="refresh-log"):
                    st.session_state["log_refresh_ver"] = int(
                        st.session_state.get("log_refresh_ver", 0)
                    ) + 1
            log_path = LOGS.get(log_choice)
            st.caption(f"`{log_path}` · refresh #{st.session_state.get('log_refresh_ver', 0)}")
            content = read_log_tail(log_choice, lines=tail_lines)
            _render_log_risk_colored(content)


if __name__ == "__main__":
    main()
