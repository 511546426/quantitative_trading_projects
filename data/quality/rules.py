"""
数据质量检查规则集。

完整性 (C01~C04):  记录数、非空率、日期连续性、指数完整
合理性 (V01~V06):  OHLC 逻辑、价格正数、涨跌幅、成交量、复权因子、估值
一致性 (S01~S03):  跨表一致
时效性 (T01~T02):  更新延迟、数据新鲜度
"""
from __future__ import annotations

import pandas as pd

from data.quality.checker import QualityRule, RuleResult


# ================================================================
# 完整性检查 (Completeness)
# ================================================================

class RecordCountRule(QualityRule):
    """C01: 交易日数据记录数"""

    rule_id = "C01"
    description = "交易日数据记录数"
    data_types = ["daily_price"]

    def __init__(
        self,
        critical_min: int = 1000,
        error_min: int = 3000,
        warn_min: int = 4000,
    ):
        self.critical_min = critical_min
        self.error_min = error_min
        self.warn_min = warn_min

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        count = len(df)

        if count < self.critical_min:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="CRITICAL", passed=False,
                message=f"记录数 {count} < {self.critical_min} (CRITICAL)",
                details={"count": count},
            )
        if count < self.error_min:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="ERROR", passed=False,
                message=f"记录数 {count} < {self.error_min}",
                details={"count": count},
            )
        if count < self.warn_min:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="WARNING", passed=False,
                message=f"记录数 {count} < {self.warn_min}",
                details={"count": count},
            )

        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level="INFO", passed=True,
            message=f"记录数 {count} ✓",
            details={"count": count},
        )


class NonNullRule(QualityRule):
    """C02: 必要字段非空率"""

    rule_id = "C02"
    description = "OHLCV 字段非空率"
    data_types = ["daily_price"]

    def __init__(self, columns: list[str] | None = None, error_pct: float = 0.95, warn_pct: float = 0.99):
        self.columns = columns or ["open", "high", "low", "close", "volume"]
        self.error_pct = error_pct
        self.warn_pct = warn_pct

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        if df.empty:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="ERROR", passed=False, message="DataFrame 为空",
            )

        cols = [c for c in self.columns if c in df.columns]
        if not cols:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="WARNING", passed=False, message="无匹配列",
            )

        non_null_rate = 1.0 - df[cols].isna().sum().sum() / (len(df) * len(cols))

        if non_null_rate < self.error_pct:
            level, passed = "ERROR", False
        elif non_null_rate < self.warn_pct:
            level, passed = "WARNING", False
        else:
            level, passed = "INFO", True

        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level=level, passed=passed,
            message=f"非空率 {non_null_rate:.2%}",
            details={"non_null_rate": non_null_rate},
        )


class DateContinuityRule(QualityRule):
    """C03: 与交易日历对比，不应有缺失交易日"""

    rule_id = "C03"
    description = "日期连续性"
    data_types = ["daily_price"]

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        expected_dates = context.get("expected_dates", [])
        if not expected_dates:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="INFO", passed=True, message="无期望日期集，跳过",
            )

        actual_dates = set(df["trade_date"].unique()) if "trade_date" in df.columns else set()
        missing = set(expected_dates) - actual_dates
        missing_count = len(missing)

        if missing_count > 3:
            level, passed = "ERROR", False
        elif missing_count > 0:
            level, passed = "WARNING", False
        else:
            level, passed = "INFO", True

        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level=level, passed=passed,
            message=f"缺失 {missing_count} 个交易日",
            details={"missing_dates": sorted(missing)[:10]},
        )


# ================================================================
# 合理性检查 (Validity)
# ================================================================

class OHLCLogicRule(QualityRule):
    """V01: OHLC 逻辑关系 (low <= open/close <= high)"""

    rule_id = "V01"
    description = "OHLC 逻辑关系"
    data_types = ["daily_price"]

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        if df.empty:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="INFO", passed=True, message="空数据",
            )

        required = ["open", "high", "low", "close"]
        if not all(c in df.columns for c in required):
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="WARNING", passed=False, message="缺少 OHLC 列",
            )

        active = df[df.get("is_suspended", False) == False] if "is_suspended" in df.columns else df

        violations = (
            (active["low"] > active[["open", "close"]].min(axis=1) + 0.01)
            | (active["high"] < active[["open", "close"]].max(axis=1) - 0.01)
        )
        bad_count = violations.sum()

        level = "ERROR" if bad_count > 0 else "INFO"
        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level=level, passed=bad_count == 0,
            message=f"OHLC 逻辑违反: {bad_count} 条",
            details={"violation_count": int(bad_count)},
        )


class PricePositiveRule(QualityRule):
    """V02: 价格为正"""

    rule_id = "V02"
    description = "价格正数检查"
    data_types = ["daily_price"]

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        if df.empty:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="INFO", passed=True, message="空数据",
            )

        active = df[df.get("is_suspended", False) == False] if "is_suspended" in df.columns else df
        price_cols = [c for c in ["open", "high", "low", "close"] if c in active.columns]

        neg_mask = (active[price_cols] <= 0).any(axis=1)
        bad_count = neg_mask.sum()

        level = "ERROR" if bad_count > 0 else "INFO"
        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level=level, passed=bad_count == 0,
            message=f"非正价格: {bad_count} 条",
            details={"count": int(bad_count)},
        )


class PctChangeRule(QualityRule):
    """V03: 涨跌幅合理性"""

    rule_id = "V03"
    description = "涨跌幅合理性"
    data_types = ["daily_price"]

    def __init__(self, max_pct: float = 22.0):
        self.max_pct = max_pct

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        if "pct_chg" not in df.columns or df.empty:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="INFO", passed=True, message="无涨跌幅列",
            )

        extreme = df["pct_chg"].abs() > self.max_pct
        count = extreme.sum()

        level = "WARNING" if count > 0 else "INFO"
        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level=level, passed=count == 0,
            message=f"超 ±{self.max_pct}% 的记录: {count} 条",
            details={"count": int(count)},
        )


class AdjFactorContinuityRule(QualityRule):
    """V05: 复权因子连续性"""

    rule_id = "V05"
    description = "复权因子连续性"
    data_types = ["daily_price"]

    def __init__(self, error_threshold: float = 0.5, warn_threshold: float = 0.2):
        self.error_threshold = error_threshold
        self.warn_threshold = warn_threshold

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        if "adj_factor" not in df.columns or df.empty:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="INFO", passed=True, message="无复权因子列",
            )

        df_sorted = df.sort_values(["ts_code", "trade_date"])
        change_ratios = []

        for _, grp in df_sorted.groupby("ts_code"):
            if len(grp) < 2:
                continue
            adj = grp["adj_factor"].values
            changes = abs(adj[1:] / adj[:-1] - 1)
            change_ratios.extend(changes)

        if not change_ratios:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="INFO", passed=True, message="数据不足",
            )

        import numpy as np
        max_change = float(np.nanmax(change_ratios))

        if max_change > self.error_threshold:
            level, passed = "ERROR", False
        elif max_change > self.warn_threshold:
            level, passed = "WARNING", False
        else:
            level, passed = "INFO", True

        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level=level, passed=passed,
            message=f"最大复权因子变化: {max_change:.2%}",
            details={"max_change": max_change},
        )


class ValuationBoundsRule(QualityRule):
    """V06: 估值合理性"""

    rule_id = "V06"
    description = "估值合理性"
    data_types = ["valuation"]

    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        if df.empty:
            return RuleResult(
                rule_id=self.rule_id, description=self.description,
                level="INFO", passed=True, message="空数据",
            )

        issues = 0
        if "pe_ttm" in df.columns:
            issues += ((df["pe_ttm"].abs() > 10000) & df["pe_ttm"].notna()).sum()
        if "pb" in df.columns:
            issues += ((df["pb"] > 1000) | (df["pb"] < 0)).sum()

        level = "WARNING" if issues > 0 else "INFO"
        return RuleResult(
            rule_id=self.rule_id, description=self.description,
            level=level, passed=issues == 0,
            message=f"估值异常: {issues} 条",
            details={"count": int(issues)},
        )
