# 写入层详细设计

## 核心思想

写入层负责**将清洗后的数据持久化到对应的存储引擎**。
关键要求：幂等写入（重复插入不产生重复数据）、批量优化、连接池管理。

---

## 类结构图

```
                 BaseWriter (ABC)
                 ├── write_batch(df, table)
                 ├── upsert(df, table, keys)
                 ├── health_check() -> bool
                 └── get_latest_date(table, ts_code) -> str
                        │
          ┌─────────────┼──────────────┐
          │             │              │
  ClickHouseWriter  PostgresWriter  RedisWriter
```

---

## 写入器基类

```python
class BaseWriter(ABC):
    """
    数据写入器基类。
    
    约定：
    1. write_batch: 批量写入，适合首次灌入
    2. upsert: 幂等更新，用于增量更新（已存在则更新，不存在则插入）
    3. 所有写入操作自动处理连接管理
    4. 写入失败抛出 WriteError，调用方决定重试策略
    """
    
    @abstractmethod
    def write_batch(self, df: pd.DataFrame, table: str) -> int:
        """
        批量写入，返回写入行数。
        适用场景：历史数据回填、大批量写入。
        """
        ...
    
    @abstractmethod
    def upsert(self, df: pd.DataFrame, table: str, 
               conflict_keys: list[str]) -> int:
        """
        幂等写入（INSERT ... ON CONFLICT UPDATE）。
        conflict_keys: 唯一约束列，如 ['ts_code', 'trade_date']。
        返回受影响的行数。
        """
        ...
    
    @abstractmethod
    def get_latest_date(self, table: str, 
                        ts_code: str | None = None) -> str | None:
        """
        查询某张表的最新数据日期。
        用于增量更新时确定起始日期。
        """
        ...
    
    @abstractmethod
    def health_check(self) -> bool:
        """检查存储连接是否正常"""
        ...
```

---

## ClickHouseWriter

### 职责
- 日K线数据写入
- 分钟K线数据写入（未来）
- 复权后价格数据

### 表结构

```sql
-- 日K线主表
CREATE TABLE stock_daily (
    trade_date     Date,
    ts_code        LowCardinality(String),
    open           Float32,
    high           Float32,
    low            Float32,
    close          Float32,
    adj_open       Float32,          -- 后复权开盘价
    adj_high       Float32,
    adj_low        Float32,
    adj_close      Float32,          -- 后复权收盘价
    volume         Float64,          -- 成交量（手）
    amount         Float64,          -- 成交额（千元）
    pct_chg        Float32,          -- 涨跌幅 (%)
    turn           Float32,          -- 换手率 (%)
    adj_factor     Float32,          -- 复权因子
    is_suspended   UInt8,            -- 停牌标记
    is_limit_up    UInt8,            -- 涨停标记
    is_limit_down  UInt8,            -- 跌停标记
    is_anomaly     UInt8             -- 异常标记
) ENGINE = ReplacingMergeTree()      -- 自动去重
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ts_code, trade_date);

-- 指数日线
CREATE TABLE index_daily (
    trade_date  Date,
    ts_code     LowCardinality(String),
    open        Float32,
    high        Float32,
    low         Float32,
    close       Float32,
    volume      Float64,
    amount      Float64,
    pct_chg     Float32
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ts_code, trade_date);
```

### 幂等写入策略

```
ClickHouse 使用 ReplacingMergeTree 引擎：
  - 按 ORDER BY 列 (ts_code, trade_date) 去重
  - 后台异步 merge 时自动保留最新版本
  - 写入不需要显式 ON CONFLICT 处理
  
写入流程：
  1. 直接 INSERT（即使有重复也无妨）
  2. ClickHouse 后台 merge 会自动去重
  3. 查询时加 FINAL 关键字确保获取去重后结果
     SELECT * FROM stock_daily FINAL WHERE ts_code = '000001.SZ'
  4. 或者在查询层用 argMax 聚合确保唯一

批量写入优化：
  - 使用 clickhouse-driver 的 insert_dataframe 批量接口
  - 单批次建议 10000~50000 行
  - 禁用逐行插入
```

---

## PostgresWriter

### 职责
- 股票基础信息
- 财务指标数据
- 估值数据
- 分红送配数据

### 表结构

```sql
-- 股票基础信息
CREATE TABLE stock_info (
    ts_code      VARCHAR(12) PRIMARY KEY,
    name         VARCHAR(30),
    industry     VARCHAR(30),
    market       VARCHAR(10),
    list_date    DATE,
    delist_date  DATE,
    is_st        BOOLEAN DEFAULT FALSE,
    is_delisted  BOOLEAN DEFAULT FALSE,
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- 财务指标（PIT）
CREATE TABLE financial_indicator (
    ts_code          VARCHAR(12),
    ann_date         DATE,           -- 公告日期（PIT 关键）
    end_date         DATE,           -- 报告期末
    roe              FLOAT,
    roa              FLOAT,
    gross_margin     FLOAT,
    net_profit_yoy   FLOAT,
    revenue_yoy      FLOAT,
    is_anomaly       BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (ts_code, end_date)
);

-- 每日估值
CREATE TABLE daily_valuation (
    ts_code      VARCHAR(12),
    trade_date   DATE,
    pe_ttm       FLOAT,
    pb           FLOAT,
    ps_ttm       FLOAT,
    total_mv     FLOAT,          -- 总市值（万元）
    circ_mv      FLOAT,          -- 流通市值（万元）
    PRIMARY KEY (ts_code, trade_date)
);

-- 分红送配
CREATE TABLE dividend (
    ts_code      VARCHAR(12),
    ann_date     DATE,
    ex_date      DATE,            -- 除权除息日
    div_proc     VARCHAR(10),
    cash_div     FLOAT,
    share_div    FLOAT,
    PRIMARY KEY (ts_code, ex_date)
);

-- 交易日历
CREATE TABLE trade_calendar (
    exchange     VARCHAR(10),     -- SSE / SZSE
    cal_date     DATE,
    is_open      BOOLEAN,
    PRIMARY KEY (exchange, cal_date)
);
```

### 幂等写入策略

```
PostgreSQL 使用 ON CONFLICT DO UPDATE:
  INSERT INTO stock_info (ts_code, name, industry, ...)
  VALUES (...)
  ON CONFLICT (ts_code) DO UPDATE SET
    name = EXCLUDED.name,
    industry = EXCLUDED.industry,
    updated_at = NOW();

批量优化：
  - 使用 psycopg2 的 execute_values 或 copy_from
  - 单批次建议 1000~5000 行
  - 大表增量更新使用临时表 + merge 模式
```

---

## RedisWriter（P3）

### 职责
- 实时行情快照（当日 OHLCV）
- 策略运行状态
- 风控参数缓存

### 数据结构设计

```
Key 设计：
  实时行情:  rt:quote:{ts_code}     → Hash {open, high, low, close, volume, ...}
  涨跌统计:  rt:market:stats        → Hash {up_count, down_count, limit_up_count, ...}
  策略状态:  strategy:{name}:status → Hash {running, last_signal_time, ...}
  风控参数:  risk:{name}            → Hash {max_position, max_drawdown, ...}

TTL 设计：
  实时行情: 当日有效，收盘后 2 小时过期
  策略状态: 无过期（手动管理）
  风控参数: 无过期

写入策略：
  使用 Pipeline 批量写入（减少网络往返）
  实时行情推送频率 ≤ 3 秒/次
```

---

## 增量更新逻辑

```
DailyUpdateFlow:

1. 查询各表最新日期
   latest_price_date = writer.get_latest_date('stock_daily')
   latest_fund_date  = writer.get_latest_date('financial_indicator')

2. 确定需要更新的日期范围
   today = calendar.latest_trade_date()
   price_start = calendar.next_trade_date(latest_price_date)
   
3. 仅拉取缺失的数据
   if price_start <= today:
       new_data = fetcher.get_daily_bars(start_date=price_start, end_date=today)
       cleaned  = cleaner.clean(new_data)
       writer.upsert(cleaned, 'stock_daily', ['ts_code', 'trade_date'])

4. 验证写入结果
   actual_count = writer.count('stock_daily', trade_date=today)
   expected_min = 4000  # A 股正常交易日至少 4000 只
   assert actual_count >= expected_min
```

---

## 连接管理

```
ConnectionPool 设计：

ClickHouse:
  - 使用 clickhouse-driver 连接池
  - 最小连接: 2, 最大连接: 10
  - 空闲超时: 300s
  - 连接健康检查间隔: 60s

PostgreSQL:
  - 使用 psycopg2.pool.ThreadedConnectionPool
  - 最小连接: 2, 最大连接: 10

Redis:
  - 使用 redis-py ConnectionPool
  - 最大连接: 20

所有连接池支持：
  - 自动重连
  - 连接健康检查
  - 优雅关闭（等待进行中的操作完成）
```
