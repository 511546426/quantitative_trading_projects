# 清洗层详细设计

## 核心思想

清洗层的职责是**将采集到的原始数据转化为可直接用于策略研究的标准化数据**。
A 股有大量特殊规则（复权、停牌、涨跌停、ST、财报时间），
清洗层是这些规则的唯一执行点——上层策略代码不应重复处理。

---

## 类结构图

```
                BaseCleaner (ABC)
                ├── validate(raw_df) -> bool
                ├── clean(raw_df) -> pd.DataFrame
                └── report() -> CleanReport
                       │
          ┌────────────┼─────────────┐
          │            │             │
   PriceCleaner  FundamentalCleaner  ReferenceCleaner
```

---

## 清洗器基类

```python
class BaseCleaner(ABC):
    """
    清洗器基类。

    约定：
    1. clean() 纯函数语义，不修改输入 DataFrame
    2. 返回新 DataFrame + CleanReport（记录处理统计）
    3. 清洗过程中发现的异常数据记录到 report，不静默丢弃
    """

    @abstractmethod
    def clean(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, CleanReport]:
        """
        执行清洗，返回 (cleaned_df, report)
        """
        ...

    @abstractmethod
    def validate(self, raw_df: pd.DataFrame) -> list[str]:
        """
        验证输入数据基本格式，返回错误列表（空列表=通过）
        检查内容：必要列是否存在、类型是否正确等
        """
        ...
```

```python
@dataclass
class CleanReport:
    """清洗报告，记录本次清洗的统计信息"""
    input_rows: int
    output_rows: int
    dropped_rows: int
    filled_nulls: int              # 填充的空值数量
    flagged_anomalies: int         # 标记的异常值数量
    details: dict                  # 详细信息（按类别）
    timestamp: datetime
```

---

## PriceCleaner — 行情数据清洗

这是最重要的清洗器，处理日K线数据的各种 A 股特殊情况。

### 清洗流程

```
原始日K线数据
     │
     ▼
[1] 基础验证
     │ - 必要列检查 (ts_code, trade_date, OHLCV)
     │ - 类型转换 (确保数值列为 float)
     │ - 去重 (ts_code + trade_date)
     ▼
[2] 复权处理
     │ - 合并复权因子 (adj_factor)
     │ - 计算后复权价格 (adj_open/adj_high/adj_low/adj_close)
     │ - 保留原始价格 + 后复权价格共存
     ▼
[3] 停牌标记
     │ - volume == 0 → is_suspended = True
     │ - 停牌天数统计 (连续停牌计数)
     ▼
[4] 涨跌停标记
     │ - 根据市场板块判定限制比例 (10%/20%/5%)
     │ - is_limit_up: 涨停且有量 → 可观察但不追
     │ - is_limit_down: 跌停 → 不可卖出
     │ - is_one_word_limit: 一字涨/跌停 → 完全不可交易
     ▼
[5] 价格异常修正
     │ - OHLC 逻辑关系验证 (low <= open/close <= high)
     │ - 单日涨幅超限但非涨跌停 → 标记为异常
     │ - 价格为 0 或负数 → 标记为异常
     ▼
[6] 输出标准化 DataFrame
```

### 复权处理详细逻辑

```
后复权价格 = 原始价格 × 复权因子

adj_close = close × adj_factor
adj_open  = open  × adj_factor
adj_high  = high  × adj_factor
adj_low   = low   × adj_factor

存储策略：
  ClickHouse 同时存储原始价格和后复权价格
  - 原始价格：用于展示、涨跌停判定
  - 后复权价格：用于回测计算收益率

为什么选后复权而非前复权：
  - 前复权：每次有除权，历史所有价格都变 → 不断修改历史数据
  - 后复权：历史价格不变，只有最新价格在变 → 对存储友好
  - 回测中使用后复权计算收益率是等价的
```

### 涨跌停判定逻辑

```
涨跌停板规则：
  主板 / 深市主板: ±10%
  创业板 (300xxx): ±20% (注册制后)
  科创板 (688xxx): ±20%
  北交所 (8xxxxx): ±30%
  ST / *ST:       ±5%

判定方法：
  理论涨停价 = round(pre_close × (1 + limit_pct), 2)
  理论跌停价 = round(pre_close × (1 - limit_pct), 2)
  
  is_limit_up   = (close == 理论涨停价) and (volume > 0)
  is_limit_down = (close == 理论跌停价) and (volume > 0)
  is_one_word_limit_up   = (open == high == close == 理论涨停价)
  is_one_word_limit_down = (open == low  == close == 理论跌停价)

注意：
  - 四舍五入到分（2位小数），这是交易所规则
  - 新股上市前 5 日无涨跌停限制（科创板/创业板注册制）
  - 需要 stock_info 表的 market 字段来判断板块
```

### 输出列定义

| 列名 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| open/high/low/close | float | 原始价格 |
| adj_open/adj_high/adj_low/adj_close | float | 后复权价格 |
| volume | float | 成交量（手） |
| amount | float | 成交额 |
| pct_chg | float | 涨跌幅 |
| turn | float | 换手率 |
| adj_factor | float | 复权因子 |
| is_suspended | bool | 是否停牌 |
| suspension_days | int | 连续停牌天数 |
| is_limit_up | bool | 是否涨停 |
| is_limit_down | bool | 是否跌停 |
| is_one_word_limit | bool | 是否一字板 |
| is_anomaly | bool | 价格异常标记 |

---

## FundamentalCleaner — 基本面数据清洗

### 核心问题：Point-in-Time 对齐

```
关键概念（PIT）：
  在回测的任何一个时间点 T，只能使用 T 之前已经公告的数据。
  
错误做法：
  用 end_date (报告期末) 作为数据可用日期
  → 例如 2025-Q1 的 ROE，报告期末是 2025-03-31
  → 但可能到 2025-04-28 才公告
  → 如果在 2025-04-01 使用，就是未来数据泄露

正确做法：
  用 ann_date (公告日期) 作为数据可用日期
  → 只有在 ann_date 及之后，该财务数据才能使用
```

### 清洗流程

```
原始财务数据
     │
     ▼
[1] 基础验证
     │ - 必须有 ann_date 和 end_date
     │ - ann_date 必须 >= end_date
     ▼
[2] 去重与冲突处理
     │ - 同一 (ts_code, end_date) 可能有多条（预告/快报/正式）
     │ - 保留 ann_date 最新的一条（最终修正值）
     ▼
[3] 时间对齐
     │ - 构建 PIT 时间序列
     │ - 在任意 trade_date，对应最近一次已公告的财报数据
     ▼
[4] 异常值处理
     │ - ROE/ROA 超出 [-100%, 200%] 标记为异常
     │ - 营收/利润增长超 ±1000% 标记为异常
     │ - 保留原值但加标记，不自动修正（交给策略层决定）
     ▼
[5] TTM 计算（滚动12个月）
     │ - 利润/营收等流量指标需要 TTM 化
     │ - TTM = Q1+Q2+Q3+Q4(最近4个季度)
     │ - 或者 TTM = 最新年报 + 最新单季 - 去年同期单季
     ▼
[6] 输出
```

### TTM 计算规则

```
场景：现在是 2025-08-15，已有以下已公告的财报

已公告:
  2024 年报 (end_date: 2024-12-31, ann_date: 2025-04-20)
  2025 Q1  (end_date: 2025-03-31, ann_date: 2025-04-28)
  2025 半年报 (end_date: 2025-06-30, ann_date: 2025-08-10)

TTM 净利润 = 2025H1净利润 + 2024年报净利润 - 2024H1净利润

公式泛化：
  若最新为 Q1: TTM = Q1(今) + 年报(去) - Q1(去)
  若最新为 H1: TTM = H1(今) + 年报(去) - H1(去)
  若最新为 Q3: TTM = Q3(今) + 年报(去) - Q3(去)
  若最新为年报: TTM = 年报(今)
```

---

## ReferenceCleaner — 基础信息清洗

### 清洗流程

```
原始股票列表
     │
     ▼
[1] 退市标记
     │ - delist_date 不为空 → is_delisted = True
     │ - 退市整理期股票标记
     ▼
[2] ST 标记
     │ - name 包含 'ST' → is_st = True
     │ - 区分 ST / *ST / S / SST
     ▼
[3] 板块分类
     │ - 主板: 60xxxx.SH, 00xxxx.SZ
     │ - 创业板: 300xxx.SZ, 301xxx.SZ
     │ - 科创板: 688xxx.SH
     │ - 北交所: 8xxxxx.BJ
     ▼
[4] 上市天数计算
     │ - listing_days = trade_date - list_date (交易日口径)
     ▼
[5] 行业标准化
     │ - 统一行业分类标准（申万一级 / 中信一级）
     ▼
[6] 输出可交易股票池
     过滤条件:
     - 非退市
     - 非 ST（可配置）
     - 上市满 N 个交易日（默认 60）
     - 非停牌
```

---

## 清洗层设计原则汇总

| 原则 | 说明 |
|------|------|
| 不修改原始数据 | 输入 DataFrame 不被修改，返回新的 DataFrame |
| 标记不删除 | 异常数据加 flag 列标记，不在清洗层直接删除（策略层决定） |
| 可审计 | CleanReport 记录每一步的处理数量，方便排查问题 |
| 幂等 | 同一份原始数据，多次清洗结果完全一致 |
| 可配置 | 涨跌停比例、上市天数阈值等通过配置文件控制 |
