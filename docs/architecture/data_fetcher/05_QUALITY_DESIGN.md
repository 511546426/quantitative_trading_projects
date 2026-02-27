# 数据质量体系设计

## 核心思想

数据质量是量化系统的生命线——**垃圾数据 = 垃圾策略**。
质量检查在清洗后、写入后各执行一次，发现关键问题立即告警。

---

## 质量检查分级

| 级别 | 含义 | 处理方式 | 示例 |
|------|------|----------|------|
| **CRITICAL** | 数据不可用 | 阻塞写入 + 立即告警 | 全市场数据为空 |
| **ERROR** | 数据有严重问题 | 写入但告警 | 某批次 50%+ 记录异常 |
| **WARNING** | 数据有小问题 | 写入 + 记录 | 个别股票缺失换手率 |
| **INFO** | 正常统计信息 | 仅记录 | 今日数据行数 5012 |

---

## 检查规则体系

### 完整性检查 (Completeness)

```
[C01] 交易日数据记录数
  规则: 交易日 A 股数据记录数 ≥ 4000
  级别: CRITICAL (< 1000), ERROR (< 3000), WARNING (< 4000)
  说明: 全 A 约 5000+，去掉停牌/退市后正常应 ≥ 4500

[C02] 必要字段非空率
  规则: OHLCV 字段非空率 ≥ 99%
  级别: ERROR (< 95%), WARNING (< 99%)
  
[C03] 日期连续性
  规则: 与交易日历对比，不应有缺失交易日
  级别: ERROR (缺失 > 3 天), WARNING (缺失 1~3 天)
  
[C04] 指数数据完整
  规则: 沪深 300 / 中证 500 / 上证指数 数据必须存在
  级别: CRITICAL (任一缺失)
```

### 合理性检查 (Validity)

```
[V01] OHLC 逻辑关系
  规则: low ≤ min(open, close) 且 max(open, close) ≤ high
  级别: ERROR
  
[V02] 价格为正
  规则: OHLC > 0（停牌除外）
  级别: ERROR

[V03] 涨跌幅合理性
  规则: 非新股 |pct_chg| ≤ 22%（科创板/创业板 ±20%，留 2% 余量）
  级别: WARNING (超限但合理解释), ERROR (无法解释)

[V04] 成交量合理性
  规则: volume ≥ 0, 非停牌股 volume > 0
  级别: WARNING

[V05] 复权因子连续性
  规则: 相邻交易日 adj_factor 变化不超过 50%
  级别: ERROR (变化 > 50%)，WARNING (变化 > 20%)
  说明: 正常除权除息 adj_factor 变化通常 < 15%

[V06] 估值合理性
  规则: PE_TTM ∈ (-10000, 10000), PB ∈ (0, 1000)
  级别: WARNING
```

### 一致性检查 (Consistency)

```
[S01] 跨表一致性
  规则: stock_daily 的股票集合 ⊆ stock_info 的股票集合
  级别: ERROR (出现未知股票代码)

[S02] 日期跨表一致
  规则: stock_daily 和 daily_valuation 同一交易日的记录数偏差 < 5%
  级别: WARNING

[S03] 复权因子与价格一致
  规则: adj_close / close ≈ adj_factor（误差 < 0.01）
  级别: ERROR
```

### 时效性检查 (Timeliness)

```
[T01] 数据更新延迟
  规则: 交易日 16:00 前日K线数据应更新完毕
  级别: WARNING (16:00~17:00), ERROR (> 17:00)

[T02] 数据新鲜度
  规则: 最新数据日期应为最近交易日
  级别: ERROR (差距 > 2 交易日), WARNING (差距 = 1 交易日)
```

---

## 质量检查引擎

```python
class QualityChecker:
    """
    数据质量检查引擎。
    
    使用方式：
    1. 注册检查规则
    2. 传入 DataFrame，执行所有适用规则
    3. 输出 QualityReport
    """
    
    def __init__(self):
        self.rules: list[QualityRule] = []
    
    def register(self, rule: QualityRule):
        self.rules.append(rule)
    
    def check(self, df: pd.DataFrame, context: dict) -> QualityReport:
        """
        context 示例: {
            'trade_date': '20260226',
            'data_type': 'daily_price',
            'source': 'tushare'
        }
        """
        results = []
        for rule in self.rules:
            if rule.applies_to(context):
                result = rule.evaluate(df, context)
                results.append(result)
        
        return QualityReport(
            context=context,
            results=results,
            overall_level=max(r.level for r in results) if results else 'INFO'
        )
```

```python
class QualityRule(ABC):
    """单条质量检查规则"""
    
    rule_id: str           # 'C01', 'V01' 等
    description: str
    data_types: list[str]  # 适用的数据类型
    
    @abstractmethod
    def evaluate(self, df: pd.DataFrame, context: dict) -> RuleResult:
        ...
    
    def applies_to(self, context: dict) -> bool:
        return context.get('data_type') in self.data_types
```

```python
@dataclass
class RuleResult:
    rule_id: str
    level: str          # CRITICAL / ERROR / WARNING / INFO
    passed: bool
    message: str
    details: dict       # 详细数据（异常行号、异常值等）
    
@dataclass
class QualityReport:
    context: dict
    results: list[RuleResult]
    overall_level: str
    has_critical_issue: bool   # 是否有 CRITICAL 级别问题
    generated_at: datetime
```

---

## 质量看板（运维可视化）

```
┌─────────────────────────────────────────────────────────────────┐
│                    数据质量日报 - 2026-02-26                     │
├────────────┬────────────┬────────────┬──────────────────────────┤
│  检查项    │   状态     │   详情     │   趋势 (近 7 日)         │
├────────────┼────────────┼────────────┼──────────────────────────┤
│ C01 记录数 │  ✅ PASS   │ 5012 条   │ ▁▂▂▂▂▂▃ (稳定)          │
│ C02 非空率 │  ✅ PASS   │ 99.8%     │ ▂▂▂▂▂▂▂ (稳定)          │
│ C03 日期   │  ✅ PASS   │ 无缺失    │ ▂▂▂▂▂▂▂                 │
│ V01 OHLC   │  ✅ PASS   │ 0 异常    │ ▂▂▂▂▂▂▂                 │
│ V03 涨跌幅 │  ⚠️ WARN  │ 2 只超限  │ ▂▂▁▂▂▂▃ (偶发)          │
│ V05 复权   │  ✅ PASS   │ 3 只变化  │ ▂▂▂▂▁▂▂ (除权日正常)    │
│ T01 延迟   │  ✅ PASS   │ 15:42 完成│ ▂▂▂▂▂▂▂                 │
├────────────┴────────────┴────────────┴──────────────────────────┤
│ 总评: ⚠️ WARNING (2 只股票涨跌幅超限需人工确认)                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 告警通知

```
告警渠道:
  CRITICAL → 邮件 + 企业微信（即时）
  ERROR    → 邮件（每日汇总）
  WARNING  → 写入日志（每日报告中查看）
  INFO     → 仅写入日志

告警内容模板:
  [CRITICAL] 数据质量告警 - 2026-02-26
  ─────────────────────
  规则: C01 - 交易日数据记录数
  期望: ≥ 4000 条
  实际: 127 条
  影响: 日K线数据严重不完整，可能影响所有策略信号
  建议: 检查 Tushare API 状态，必要时手动补数据
```
