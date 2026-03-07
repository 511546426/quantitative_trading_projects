"""
回测可视化报告生成器

输出一张 5 面板综合图表（PNG），包含：
  1. 净值曲线（策略 vs 基准等权）
  2. 回撤水下图
  3. 滚动 Sharpe（252日）
  4. 月度收益热力图
  5. 年度收益柱状图

用法：
    from strategy.backtest.visualizer import plot_report
    plot_report(net_returns, title="反转策略", save_path="docs/reports/reversal.png")
"""
from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# 确保 matplotlib 可写目录
_cfg_dir = Path(__file__).resolve().parents[2] / ".mplconfig"
_cfg_dir.mkdir(exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_cfg_dir))

import matplotlib
matplotlib.use("Agg")  # 无 GUI 模式，适合服务器

# 注册中文字体（Noto Sans CJK SC，Linux 系统通用）
from matplotlib import font_manager as _fm
_CJK_PATHS = [
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/arphic/uming.ttc",
    "/System/Library/Fonts/PingFang.ttc",           # macOS
    "C:/Windows/Fonts/msyh.ttc",                    # Windows 微软雅黑
]
for _p in _CJK_PATHS:
    if Path(_p).exists():
        _fm.fontManager.addfont(_p)
        _prop = _fm.FontProperties(fname=_p)
        matplotlib.rcParams["font.family"] = _prop.get_name()
        break

matplotlib.rcParams["axes.unicode_minus"] = False   # 负号正常显示

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns

warnings.filterwarnings("ignore")

# ─── 颜色主题 ──────────────────────────────────────────────────────────────────
STRATEGY_COLOR  = "#2196F3"   # 蓝色：策略净值
BENCHMARK_COLOR = "#9E9E9E"   # 灰色：基准
DRAWDOWN_COLOR  = "#EF5350"   # 红色：回撤
POSITIVE_COLOR  = "#26A69A"   # 绿色：正收益
NEGATIVE_COLOR  = "#EF5350"   # 红色：负收益
BG_COLOR        = "#FAFAFA"
GRID_COLOR      = "#E0E0E0"

FONT_TITLE  = {"fontsize": 11, "fontweight": "bold", "color": "#212121"}
FONT_LABEL  = {"fontsize": 9, "color": "#616161"}
FONT_TICK   = {"labelsize": 8, "colors": "#757575"}


def plot_report(
    net_returns: pd.Series,
    benchmark_returns: Optional[pd.Series] = None,
    title: str = "策略回测报告",
    save_path: str = "docs/reports/backtest_report.png",
    initial_capital: float = 1.0,
) -> str:
    """
    生成综合回测报告图表并保存为 PNG。

    Parameters
    ----------
    net_returns       : 策略每日净收益率序列（已扣成本）
    benchmark_returns : 基准每日收益率序列（可选，默认不显示基准）
    title             : 图表标题
    save_path         : 输出 PNG 路径
    initial_capital   : 初始本金（仅用于显示）

    Returns
    -------
    str : 保存路径
    """
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)

    net_returns = net_returns.dropna()
    nav   = (1 + net_returns).cumprod() * initial_capital
    dates = nav.index

    # ─── 基础统计 ──────────────────────────────────────────────────────────────
    total_ret  = nav.iloc[-1] / nav.iloc[0] - 1
    n_years    = len(net_returns) / 252
    ann_ret    = (1 + total_ret) ** (1 / n_years) - 1 if n_years > 0 else 0
    ann_vol    = net_returns.std() * np.sqrt(252)
    sharpe     = (net_returns.mean() - 0.025 / 252) / net_returns.std() * np.sqrt(252)
    peak       = nav.cummax()
    drawdown   = nav / peak - 1
    max_dd     = drawdown.min()
    calmar     = ann_ret / abs(max_dd) if abs(max_dd) > 1e-9 else 0
    win_rate   = (net_returns > 0).mean()

    # 年度收益
    annual_rets = _calc_annual_returns(net_returns)

    # 月度收益矩阵
    monthly_matrix = _calc_monthly_matrix(net_returns)

    # 滚动 Sharpe
    rolling_sharpe = _calc_rolling_sharpe(net_returns, window=252)

    # ─── 图布局 ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(16, 14), facecolor=BG_COLOR)
    fig.suptitle(title, fontsize=16, fontweight="bold", color="#212121",
                 y=0.98, x=0.5)

    gs = gridspec.GridSpec(
        4, 2,
        figure=fig,
        height_ratios=[2.2, 1.0, 1.0, 1.6],
        hspace=0.45,
        wspace=0.3,
        left=0.07, right=0.97, top=0.94, bottom=0.05,
    )

    ax_nav      = fig.add_subplot(gs[0, :])    # 顶部全宽：净值曲线
    ax_dd       = fig.add_subplot(gs[1, :])    # 第2行全宽：回撤
    ax_rolling  = fig.add_subplot(gs[2, 0])   # 第3行左：滚动Sharpe
    ax_annual   = fig.add_subplot(gs[2, 1])   # 第3行右：年度收益
    ax_heatmap  = fig.add_subplot(gs[3, :])   # 底部全宽：月度热力图
    ax_stats    = ax_heatmap.inset_axes([0.78, 0.0, 0.22, 1.0])  # 热力图右侧指标表

    _style_ax(ax_nav, ax_dd, ax_rolling, ax_annual, ax_heatmap)
    _apply_date_formatter(ax_nav, ax_dd, ax_rolling)

    # ─── Panel 1: 净值曲线 ────────────────────────────────────────────────────
    ax_nav.plot(dates, nav, color=STRATEGY_COLOR, lw=1.8, label="策略净值", zorder=3)
    if benchmark_returns is not None:
        bm_nav = (1 + benchmark_returns.reindex(dates).fillna(0)).cumprod() * initial_capital
        ax_nav.plot(dates, bm_nav, color=BENCHMARK_COLOR, lw=1.2,
                    linestyle="--", label="等权基准", zorder=2)
    ax_nav.axhline(initial_capital, color=GRID_COLOR, lw=0.8, zorder=1)
    ax_nav.fill_between(dates, initial_capital, nav,
                         where=(nav >= initial_capital),
                         alpha=0.08, color=POSITIVE_COLOR)
    ax_nav.fill_between(dates, initial_capital, nav,
                         where=(nav < initial_capital),
                         alpha=0.12, color=NEGATIVE_COLOR)
    ax_nav.set_title("净值曲线", **FONT_TITLE)
    ax_nav.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"{v:.2f}" if initial_capital == 1.0 else f"¥{v:,.0f}"))
    ax_nav.legend(loc="upper left", fontsize=8, framealpha=0.8)
    _annotate_nav(ax_nav, nav, total_ret, ann_ret)

    # ─── Panel 2: 回撤水下图 ─────────────────────────────────────────────────
    ax_dd.fill_between(dates, drawdown * 100, 0,
                        color=DRAWDOWN_COLOR, alpha=0.5, label="回撤")
    ax_dd.plot(dates, drawdown * 100, color=DRAWDOWN_COLOR, lw=0.8)
    ax_dd.axhline(max_dd * 100, color=DRAWDOWN_COLOR, lw=0.8,
                   linestyle=":", alpha=0.8)
    ax_dd.text(dates[-1], max_dd * 100 + 0.3,
               f"  MaxDD {max_dd:.1%}", color=DRAWDOWN_COLOR,
               va="bottom", ha="right", fontsize=8)
    ax_dd.set_title("回撤（%）", **FONT_TITLE)
    ax_dd.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    ax_dd.set_ylim(min(drawdown.min() * 100 * 1.2, -1), 1)

    # ─── Panel 3: 滚动 Sharpe ─────────────────────────────────────────────────
    if rolling_sharpe is not None:
        rs = rolling_sharpe.dropna()
        ax_rolling.plot(rs.index, rs, color=STRATEGY_COLOR, lw=1.2)
        ax_rolling.axhline(0, color=GRID_COLOR, lw=0.8)
        ax_rolling.axhline(1.0, color=POSITIVE_COLOR, lw=0.8,
                            linestyle="--", alpha=0.6, label="Sharpe=1")
        ax_rolling.fill_between(rs.index, rs, 0,
                                  where=(rs >= 0), alpha=0.1, color=POSITIVE_COLOR)
        ax_rolling.fill_between(rs.index, rs, 0,
                                  where=(rs < 0), alpha=0.15, color=NEGATIVE_COLOR)
        ax_rolling.legend(fontsize=7, framealpha=0.8)
    ax_rolling.set_title("滚动 Sharpe（252日）", **FONT_TITLE)

    # ─── Panel 4: 年度收益柱状图 ──────────────────────────────────────────────
    if annual_rets is not None and not annual_rets.empty:
        colors = [POSITIVE_COLOR if v >= 0 else NEGATIVE_COLOR
                  for v in annual_rets.values]
        bars = ax_annual.bar(annual_rets.index.astype(str), annual_rets.values * 100,
                              color=colors, width=0.65, zorder=3)
        ax_annual.axhline(0, color="#BDBDBD", lw=0.8, zorder=2)
        for bar, val in zip(bars, annual_rets.values):
            va = "bottom" if val >= 0 else "top"
            offset = 0.5 if val >= 0 else -0.5
            ax_annual.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height() + offset,
                            f"{val:.0%}", ha="center", va=va, fontsize=7.5,
                            color="#424242")
        ax_annual.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
        ax_annual.tick_params(axis="x", rotation=45, labelsize=7.5)
    ax_annual.set_title("年度收益（%）", **FONT_TITLE)

    # ─── Panel 5: 月度收益热力图 ──────────────────────────────────────────────
    if monthly_matrix is not None and not monthly_matrix.empty:
        cmap = LinearSegmentedColormap.from_list(
            "rg", [NEGATIVE_COLOR, "#FFFFFF", POSITIVE_COLOR], N=256)
        vmax = max(abs(monthly_matrix.values[~np.isnan(monthly_matrix.values)]).max(), 0.01)
        annot = monthly_matrix.applymap(
            lambda v: f"{v:.1%}" if not np.isnan(v) else "")
        sns.heatmap(
            monthly_matrix * 100,
            ax=ax_heatmap,
            cmap=cmap, center=0, vmin=-vmax * 100, vmax=vmax * 100,
            annot=annot.values, fmt="", annot_kws={"size": 7.5},
            linewidths=0.5, linecolor="#E0E0E0",
            cbar_kws={"shrink": 0.6, "pad": 0.01},
            xticklabels=["1月","2月","3月","4月","5月","6月",
                          "7月","8月","9月","10月","11月","12月"],
        )
        ax_heatmap.tick_params(axis="x", labelsize=8, rotation=0)
        ax_heatmap.tick_params(axis="y", labelsize=8, rotation=0)
    ax_heatmap.set_title("月度收益热力图（%）", **FONT_TITLE)
    ax_heatmap.set_xlabel("")
    ax_heatmap.set_ylabel("")

    # ─── 指标汇总表（嵌入热力图右侧） ───────────────────────────────────────
    ax_stats.axis("off")
    stats = [
        ("总收益率",   f"{total_ret:+.2%}"),
        ("年化收益",   f"{ann_ret:+.2%}"),
        ("年化波动",   f"{ann_vol:.2%}"),
        ("Sharpe",     f"{sharpe:.2f}"),
        ("Calmar",     f"{calmar:.2f}"),
        ("最大回撤",   f"{max_dd:.2%}"),
        ("胜率(日)",   f"{win_rate:.1%}"),
        ("回测年数",   f"{n_years:.1f} 年"),
    ]
    col_labels = ["指标", "数值"]
    table = ax_stats.table(
        cellText=stats,
        colLabels=col_labels,
        cellLoc="center",
        loc="center",
        bbox=[0.0, 0.0, 1.0, 1.0],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor(GRID_COLOR)
        if r == 0:
            cell.set_facecolor("#1565C0")
            cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#E3F2FD")
        else:
            cell.set_facecolor("#FAFAFA")
        # 数值列右对齐
        if c == 1 and r > 0:
            cell.set_text_props(ha="right")
            val = stats[r - 1][1]
            if val.startswith("+"):
                cell.set_text_props(color=POSITIVE_COLOR, ha="right")
            elif val.startswith("-"):
                cell.set_text_props(color=NEGATIVE_COLOR, ha="right")

    plt.savefig(save_path, dpi=150, bbox_inches="tight",
                facecolor=BG_COLOR, edgecolor="none")
    plt.close(fig)
    print(f"📊 报告已保存: {save_path}")
    return save_path


# ─── 辅助函数 ──────────────────────────────────────────────────────────────────

def _calc_annual_returns(returns: pd.Series) -> pd.Series:
    try:
        annual = returns.groupby(returns.index.year).apply(
            lambda r: (1 + r).prod() - 1
        )
        return annual
    except Exception:
        return pd.Series(dtype=float)


def _calc_monthly_matrix(returns: pd.Series) -> Optional[pd.DataFrame]:
    try:
        monthly = returns.groupby([returns.index.year, returns.index.month]).apply(
            lambda r: (1 + r).prod() - 1
        )
        monthly.index = pd.MultiIndex.from_tuples(monthly.index)
        matrix = monthly.unstack(level=1)
        matrix.columns = range(1, matrix.shape[1] + 1)
        # 补齐 12 个月的列
        for m in range(1, 13):
            if m not in matrix.columns:
                matrix[m] = np.nan
        matrix = matrix[sorted(matrix.columns)]
        matrix.index.name = None
        matrix.columns.name = None
        return matrix
    except Exception:
        return None


def _calc_rolling_sharpe(returns: pd.Series, window: int = 252) -> Optional[pd.Series]:
    if len(returns) < window:
        window = max(len(returns) // 2, 20)
    if window < 5:
        return None
    roll = returns.rolling(window)
    mean = roll.mean()
    std  = roll.std()
    rs   = mean / std * np.sqrt(252)
    return rs


def _annotate_nav(ax, nav: pd.Series, total_ret: float, ann_ret: float):
    """在净值曲线末端标注最终收益"""
    last_date = nav.index[-1]
    last_val  = nav.iloc[-1]
    ax.annotate(
        f" {total_ret:+.1%}\n 年化 {ann_ret:+.1%}",
        xy=(last_date, last_val),
        xytext=(last_date, last_val),
        fontsize=9,
        color=STRATEGY_COLOR,
        fontweight="bold",
        va="center",
    )


def _style_ax(*axes):
    for ax in axes:
        ax.set_facecolor(BG_COLOR)
        ax.grid(True, color=GRID_COLOR, lw=0.5, zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["left", "bottom"]].set_color(GRID_COLOR)
        ax.tick_params(**FONT_TICK)


def _apply_date_formatter(*axes):
    """仅对使用日期 index 的轴设置日期格式（避免污染分类轴）"""
    for ax in axes:
        try:
            ax.xaxis.set_major_locator(matplotlib.dates.AutoDateLocator())
            ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m"))
            plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=7.5)
        except Exception:
            pass
