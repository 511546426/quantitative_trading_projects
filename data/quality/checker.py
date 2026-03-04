"""
数据质量检查引擎。

使用方式:
    1. 注册检查规则
    2. 传入 DataFrame + context
    3. 输出 QualityReport
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

LEVEL_ORDER = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}


@dataclass
class RuleResult:
    """单条规则的检查结果"""
    rule_id: str
    description: str
    level: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class QualityReport:
    """质量检查报告"""
    context: dict
    results: list[RuleResult]
    overall_level: str
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def has_critical_issue(self) -> bool:
        return any(r.level == "CRITICAL" and not r.passed for r in self.results)

    @property
    def has_error(self) -> bool:
        return any(r.level in ("CRITICAL", "ERROR") and not r.passed for r in self.results)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    def summary(self) -> str:
        lines = [
            f"质量报告 [{self.context.get('trade_date', '?')}] "
            f"— {self.overall_level}",
            f"  通过: {self.passed_count}, 失败: {self.failed_count}",
        ]
        for r in self.results:
            icon = "✅" if r.passed else {"WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "🔴"}.get(r.level, "❓")
            lines.append(f"  {icon} [{r.rule_id}] {r.message}")
        return "\n".join(lines)


class QualityRule(ABC):
    """质量检查规则基类"""

    rule_id: str = ""
    description: str = ""
    data_types: list[str] = []

    @abstractmethod
    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        ...

    def applies_to(self, context: dict) -> bool:
        if not self.data_types:
            return True
        return context.get("data_type") in self.data_types


class QualityChecker:
    """数据质量检查引擎"""

    def __init__(self):
        self._rules: list[QualityRule] = []

    def register(self, rule: QualityRule) -> None:
        self._rules.append(rule)
        logger.debug("注册质量规则: %s", rule.rule_id)

    def register_all(self, rules: list[QualityRule]) -> None:
        for rule in rules:
            self.register(rule)

    def check(self, df: pd.DataFrame, context: dict) -> QualityReport:
        """
        对 DataFrame 执行所有适用的质量规则。

        Parameters
        ----------
        df : DataFrame
            待检查的数据。
        context : dict
            上下文信息，如 {'trade_date': '20260226', 'data_type': 'daily_price'}
        """
        results: list[RuleResult] = []

        for rule in self._rules:
            if rule.applies_to(context):
                try:
                    result = rule.evaluate(df, context)
                    results.append(result)
                except Exception as e:
                    results.append(RuleResult(
                        rule_id=rule.rule_id,
                        description=rule.description,
                        level="ERROR",
                        passed=False,
                        message=f"规则执行异常: {e}",
                    ))

        if results:
            max_level_val = max(
                LEVEL_ORDER.get(r.level, 0) for r in results if not r.passed
            ) if any(not r.passed for r in results) else 0
            overall = {v: k for k, v in LEVEL_ORDER.items()}.get(max_level_val, "INFO")
        else:
            overall = "INFO"

        report = QualityReport(
            context=context,
            results=results,
            overall_level=overall,
        )

        logger.info("质量检查完成: %s", report.summary())
        return report
