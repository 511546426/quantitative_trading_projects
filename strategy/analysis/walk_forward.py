"""
Walk-Forward 样本外验证模块

通过滚动/扩展时间窗口验证策略参数的稳定性，避免过拟合。

核心思路:
  1. 将全区间划分为 N 个不重叠的验证窗口
  2. 每个窗口: 训练集 → 优化参数 → 测试集(样本外) → 记录绩效
  3. 汇总所有样本外绩效: OOS Sharpe、夏普衰减、参数稳定性
"""
from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class WalkForwardWindow:
    """单窗口结果"""
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    best_params: dict[str, Any]
    in_sample_metrics: dict[str, Any]
    out_of_sample_metrics: dict[str, Any]
    oos_annualized_return: float = 0.0
    oos_max_drawdown: float = 0.0
    oos_sharpe: float = 0.0


@dataclass
class WalkForwardReport:
    """Walk-Forward 汇总报告"""
    windows: list[WalkForwardWindow] = field(default_factory=list)
    @property
    def oos_sharpes(self) -> list[float]:
        return [w.oos_sharpe for w in self.windows]

    @property
    def oos_annual_returns(self) -> list[float]:
        return [w.oos_annualized_return for w in self.windows]

    @property
    def avg_oos_sharpe(self) -> float:
        vals = self.oos_sharpes
        return float(np.mean(vals)) if vals else 0.0

    @property
    def avg_oos_annual_return(self) -> float:
        vals = self.oos_annual_returns
        return float(np.mean(vals)) if vals else 0.0

    @property
    def sharpe_std(self) -> float:
        vals = self.oos_sharpes
        return float(np.std(vals)) if len(vals) > 1 else 0.0

    def summary(self) -> str:
        if not self.windows:
            return "Walk-Forward: 无窗口结果"
        lines = [
            "=" * 60,
            "Walk-Forward 样本外验证报告",
            "=" * 60,
            f"窗口数: {len(self.windows)}",
            f"平均 OOS 夏普: {self.avg_oos_sharpe:.3f}",
            f"夏普标准差:    {self.sharpe_std:.3f}",
            f"平均 OOS 年化: {self.avg_oos_annual_return:+.2%}",
            "-" * 60,
            f"{'窗口':>6} {'训练集':>22} {'测试集':>22} {'OOS年化':>10} {'OOS夏普':>10}",
            "-" * 60,
        ]
        for i, w in enumerate(self.windows):
            lines.append(
                f"{i:>6}  {w.train_start}-{w.train_end}  {w.test_start}-{w.test_end}  "
                f"{w.oos_annualized_return:>+9.2%}  {w.oos_sharpe:>9.3f}"
            )
        lines.append("=" * 60)
        return "\n".join(lines)


def walk_forward_validate(
    *,
    strategy_fn: Callable[..., dict[str, Any]],
    param_grid: dict[str, list[Any]],
    train_years: int = 5,
    test_years: int = 1,
    min_train_years: int = 3,
    scoring_metric: str = "sharpe_ratio",
    higher_is_better: bool = True,
    logger_prefix: str = "WF",
    **strategy_kwargs: Any,
) -> WalkForwardReport:
    """
    Walk-Forward 样本外验证。

    Parameters
    ----------
    strategy_fn
        策略函数，接收 params 关键字参数和 **strategy_kwargs，
        返回一个包含 ``sharpe_ratio``、``annualized_return``、
        ``max_drawdown`` 等字段的 dict。
    param_grid
        参数网格: ``{"top_n": [20, 30], "inertia": [0.2, 0.3]}``。
    train_years
        每个窗口训练集长度（年）。
    test_years
        每个窗口测试集长度（年）。
    min_train_years
        最小训练年数。
    scoring_metric
        用于选择最佳参数的指标名。
    higher_is_better
        True 表示该指标越大越好。
    strategy_kwargs
        透传给 strategy_fn 的固定参数。

    Returns
    -------
    WalkForwardReport
        包含每个窗口及汇总指标。
    """
    # 从 strategy_kwargs 中提取日期信息
    all_dates = strategy_kwargs.get("all_dates")
    if all_dates is None or len(all_dates) < 2:
        raise ValueError("walk_forward_validate 需要 strategy_kwargs['all_dates']（交易日列表或 DatetimeIndex）")

    n_dates = len(all_dates)
    total_years = n_dates / 252
    if total_years < train_years + test_years:
        raise ValueError(
            f"总年份 {total_years:.1f} 不足 (train={train_years} + test={test_years})"
        )

    train_days = train_years * 252
    test_days = test_years * 252
    step = test_days

    param_keys = list(param_grid.keys())
    param_values = list(param_grid.values())
    all_combos = list(itertools.product(*param_values))

    if not param_keys or not all_combos:
        raise ValueError("param_grid 为空")

    report = WalkForwardReport()
    logger.info(
        "%s 参数搜索空间: %d 种组合 × %d 个窗口",
        logger_prefix, len(all_combos),
        max(0, (n_dates - train_days) // step),
    )

    window_idx = 0
    for start_idx in range(0, n_dates - train_days, step):
        train_end_idx = start_idx + train_days
        test_end_idx = min(train_end_idx + test_days, n_dates)
        if test_end_idx - train_end_idx < 20:
            break  # 测试集太短

        train_dates = all_dates[start_idx:train_end_idx]
        test_dates = all_dates[train_end_idx:test_end_idx]

        train_start_str = str(train_dates[0])[:10]
        train_end_str = str(train_dates[-1])[:10]
        test_start_str = str(test_dates[0])[:10]
        test_end_str = str(test_dates[-1])[:10]

        logger.info(
            "%s 窗口 %d: 训练 %s ~ %s (%d天) → 测试 %s ~ %s (%d天)",
            logger_prefix, window_idx,
            train_start_str, train_end_str, len(train_dates),
            test_start_str, test_end_str, len(test_dates),
        )

        # —— 参数扫描（训练集） ——
        best_score = -1e18 if higher_is_better else 1e18
        best_params: dict[str, Any] = {}
        best_is_metrics: dict[str, Any] = {}

        for combo in all_combos:
            params = dict(zip(param_keys, combo))
            try:
                metrics = strategy_fn(
                    **params,
                    date_start=train_start_str,
                    date_end=train_end_str,
                    **{k: v for k, v in strategy_kwargs.items() if k != "all_dates"},
                )
            except Exception as e:
                logger.debug("%s 参数 %s 失败: %s", logger_prefix, params, e)
                continue

            score = metrics.get(scoring_metric, -1e18) or -1e18
            if higher_is_better and score > best_score:
                best_score = score
                best_params = params
                best_is_metrics = metrics
            elif not higher_is_better and score < best_score:
                best_score = score
                best_params = params
                best_is_metrics = metrics

        if not best_params:
            logger.warning("%s 窗口 %d: 无有效参数组合", logger_prefix, window_idx)
            window_idx += 1
            continue

        # —— 样本外回测 ——
        try:
            oos_metrics = strategy_fn(
                **best_params,
                date_start=test_start_str,
                date_end=test_end_str,
                **{k: v for k, v in strategy_kwargs.items() if k != "all_dates"},
            )
        except Exception as e:
            logger.error("%s OOS 失败: %s", logger_prefix, e)
            window_idx += 1
            continue

        oos_ann_ret = float(oos_metrics.get("annualized_return", 0.0) or 0.0)
        oos_sharpe = float(oos_metrics.get("sharpe_ratio", 0.0) or 0.0)
        oos_mdd = float(oos_metrics.get("max_drawdown", 0.0) or 0.0)

        window_result = WalkForwardWindow(
            train_start=train_start_str,
            train_end=train_end_str,
            test_start=test_start_str,
            test_end=test_end_str,
            best_params=best_params,
            in_sample_metrics=best_is_metrics,
            out_of_sample_metrics=oos_metrics,
            oos_annualized_return=oos_ann_ret,
            oos_max_drawdown=oos_mdd,
            oos_sharpe=oos_sharpe,
        )
        report.windows.append(window_result)

        logger.info(
            "%s 窗口 %d 完成: 最佳参数=%s, IS Sharpe=%.3f, "
            "OOS Sharpe=%.3f, OOS年化=%+.2%%, OOS回撤=%.1f%%",
            logger_prefix, window_idx,
            best_params,
            best_is_metrics.get("sharpe_ratio", 0),
            oos_sharpe, oos_ann_ret, oos_mdd * 100,
        )
        window_idx += 1

    logger.info(
        "%s Walk-Forward 完成: %d 窗口, 平均 OOS Sharpe=%.3f ± %.3f",
        logger_prefix, len(report.windows),
        report.avg_oos_sharpe, report.sharpe_std,
    )
    return report
