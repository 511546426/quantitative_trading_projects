# 数据基础设施层设计

## 职责

数据层是整个系统的"神经系统"，负责：
- 从多个来源采集原始数据
- 清洗、标准化、存储
- 向上层提供统一的数据访问接口（屏蔽数据源差异）

---

## 数据分类

### 1. 行情数据（Price Data）

| 类型 | 内容 | 频率 | 优先级 |
|------|------|------|--------|
| 日K线 | OHLCV + 成交额 + 换手率 | 每日收盘后 | 最高 |
| 分钟K线 | 1min/5min OHLCV | 实时 | 高 |
| Tick 数据 | 逐笔成交、逐笔委托 | 实时（未来） | 中 |
| 复权数据 | 前复权/后复权价格 | 日更 | 最高 |

### 2. 基本面数据（Fundamental Data）

| 类型 | 内容 | 更新频率 |
|------|------|--------|
| 财务三表 | 利润表/资产负债表/现金流量表 | 季报/半年报/年报 |
| 估值指标 | PE/PB/PS/PCF | 日更 |
| 分红送配 | 历史分红、除权除息 | 事件驱动 |
| 股本结构 | 总股本/流通股/限售解禁 | 事件驱动 |

### 3. 市场微观结构数据

| 类型 | 内容 |
|------|------|
| 龙虎榜 | 大单买卖席位 |
| 融资融券 | 融资余额/融券余量 |
| 北向资金 | 陆股通净买入 |
| 大宗交易 | 折溢价信息 |

### 4. 宏观/情绪数据（可选，进阶）

- 央行利率、CPI、PMI
- 新闻舆情（NLP 因子）

---

## 数据源选型

| 数据源 | 类型 | 费用 | 适合场景 | 备注 |
|--------|------|------|----------|------|
| **Tushare Pro** | 综合 | 免费/付费积分 | 日频历史 + 基本面 | **首选，性价比最高** |
| **AKShare** | 综合 | 免费 | 补充数据（北向/龙虎榜） | 免费但稳定性一般 |
| **BaoStock** | 历史行情 | 免费 | 免费日频/分钟数据 | 数据稳定 |
| **JoinQuant (JQData)** | 综合 | 付费 | 策略研究平台 | 初期可用平台回测 |
| **Wind** | 机构级 | 贵 | 高精度基本面 | 有条件再用 |
| **miniQMT** | 实盘行情+交易 | 随券商免费 | **实盘执行首选** | 迅投科技，支持实时 |

---

## 存储架构设计

```
数据分层存储策略

原始数据 (Raw)          清洗数据 (Cleaned)       分析数据 (Feature)
   │                        │                        │
   ▼                        ▼                        ▼
本地文件/对象存储        ClickHouse/Parquet       特征计算结果缓存
(备份/审计用)           (主要查询引擎)           (Redis/内存)
```

### 核心存储选型

#### ClickHouse（日K线/分钟K线）
```sql
-- 日K线表设计示例
CREATE TABLE stock_daily (
    trade_date  Date,
    ts_code     LowCardinality(String),   -- 股票代码 000001.SZ
    open        Float32,
    high        Float32,
    low         Float32,
    close       Float32,
    volume      Float64,                   -- 成交量（手）
    amount      Float64,                   -- 成交额（元）
    adj_factor  Float32,                   -- 复权因子
    pct_chg     Float32,                   -- 涨跌幅%
    turn        Float32                    -- 换手率%
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ts_code, trade_date);
```

**选 ClickHouse 的理由：**
- 列式存储，全市场 5000 只股票 20 年日线查询 < 100ms
- 向量化执行，天然适合因子计算
- SQL 接口，学习成本低

#### PostgreSQL（基本面 + 元数据）
```sql
-- 股票基础信息
CREATE TABLE stock_info (
    ts_code     VARCHAR(12) PRIMARY KEY,
    name        VARCHAR(20),
    industry    VARCHAR(30),
    market      VARCHAR(10),   -- 主板/创业板/科创板
    list_date   DATE,
    delist_date DATE
);

-- 财务数据（季频）
CREATE TABLE financial_indicator (
    ts_code     VARCHAR(12),
    end_date    DATE,
    roe         FLOAT,          -- 净资产收益率
    roa         FLOAT,
    gross_profit_margin FLOAT,
    net_profit_growth   FLOAT,
    -- ... 更多指标
    PRIMARY KEY (ts_code, end_date)
);
```

#### Redis（实时状态）
- 实时行情快照（当日 OHLCV）
- 策略运行状态
- 风控参数（动态调整不需要重启）

---

## 数据获取模块设计（Python）

```
data/
├── fetchers/
│   ├── base_fetcher.py          # 抽象基类，统一接口
│   ├── tushare_fetcher.py       # Tushare 数据获取
│   ├── akshare_fetcher.py       # AKShare 补充
│   └── qmt_fetcher.py           # miniQMT 实时行情
├── cleaners/
│   ├── price_cleaner.py         # 价格清洗（复权/停牌处理）
│   └── fundamental_cleaner.py
├── storage/
│   ├── clickhouse_client.py
│   ├── postgres_client.py
│   └── redis_client.py
├── pipeline/
│   ├── daily_update.py          # 每日收盘后自动更新
│   └── backfill.py              # 历史数据回填
└── utils/
    ├── calendar.py              # A 股交易日历
    └── adjust.py                # 复权计算
```

### 关键设计：数据获取抽象层

```python
# base_fetcher.py 核心接口
class BaseFetcher(ABC):
    @abstractmethod
    def get_daily_bars(self, 
                       ts_codes: List[str], 
                       start_date: str, 
                       end_date: str) -> pd.DataFrame:
        """返回标准化 DataFrame，列名固定"""
        ...
    
    @abstractmethod
    def get_realtime_quote(self, ts_codes: List[str]) -> Dict:
        ...
```

**好处**：上层代码不依赖具体数据源，未来切换 Wind 只需换实现类。

---

## A 股数据特殊处理

### 1. 复权处理（极重要！）
```
前复权：以当前价格为基准，往历史调整 → 用于图表展示
后复权：以上市价格为基准，往当前调整 → 用于历史回测（推荐）

陷阱：不复权直接用原始价格回测，会产生大量虚假信号
```

### 2. 停牌处理
```python
# 停牌股票的处理策略
# 1. 停牌期间不产生交易信号
# 2. 停牌期间持仓不计算换手（仓位保持）
# 3. 复牌后第一天通常有大幅跳空，需特殊处理
def handle_suspension(df: pd.DataFrame) -> pd.DataFrame:
    # volume == 0 视为停牌
    df['is_suspended'] = df['volume'] == 0
    return df
```

### 3. 涨跌停处理
```python
# 涨跌停股票不可交易（即使有信号）
# 无量涨停 = 强势，无量跌停 = 恐慌
def is_limit_up(row) -> bool:
    return abs(row['pct_chg'] - 10.0) < 0.01 and row['volume'] > 0

def is_limit_down(row) -> bool:
    return abs(row['pct_chg'] + 10.0) < 0.01
```

### 4. 新股/次新股过滤
```python
# 上市不足 N 个交易日的股票通常不纳入选股池
MIN_LISTING_DAYS = 60  # 至少上市 60 交易日
```

---

## 数据质量保障

```python
# 每日数据更新后执行数据质量检查
class DataQualityChecker:
    def check_daily_update(self, date: str):
        checks = [
            self._check_trade_count(date),      # 今日应有 N 只股票数据
            self._check_price_anomaly(date),     # 价格异常（非涨跌停但超 ±15%）
            self._check_volume_anomaly(date),    # 成交量为负
            self._check_adj_factor(date),        # 复权因子突变
        ]
        return all(checks)
```

---

## 本阶段交付物

- [ ] Tushare 数据获取脚本（日K线/基本面）
- [ ] ClickHouse 建表 + 数据入库脚本
- [ ] 历史数据回填脚本（2010~至今）
- [ ] 每日定时更新脚本（cron）
- [ ] 数据质量检查报告
- [ ] 交易日历工具类
