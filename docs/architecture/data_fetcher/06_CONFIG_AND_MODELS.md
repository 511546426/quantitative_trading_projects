# 配置管理与数据模型定义

## 配置管理

### 设计原则

```
1. 敏感信息（API Token、数据库密码）走环境变量或 .env，不入 Git
2. 业务配置（限速参数、清洗阈值）走 YAML 文件，入 Git
3. 运行时可调参数（风控阈值）走 Redis 或数据库，可动态更新
4. 所有配置有默认值，缺失时不 crash
```

### 配置文件结构

#### settings.yaml — 主配置

```yaml
project:
  name: "quant_trading"
  env: "dev"                        # dev / staging / prod

database:
  clickhouse:
    host: ${CH_HOST:-localhost}
    port: ${CH_PORT:-9000}
    database: "quant"
    user: ${CH_USER:-default}
    password: ${CH_PASSWORD:-}
    pool_size: 10
    
  postgres:
    host: ${PG_HOST:-localhost}
    port: ${PG_PORT:-5432}
    database: "quant"
    user: ${PG_USER:-postgres}
    password: ${PG_PASSWORD:-}
    pool_size: 10
    
  redis:
    host: ${REDIS_HOST:-localhost}
    port: ${REDIS_PORT:-6379}
    db: 0
    password: ${REDIS_PASSWORD:-}
    pool_size: 20

logging:
  level: "INFO"                      # DEBUG / INFO / WARNING / ERROR
  format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
  file: "logs/data_pipeline.log"
  rotation: "10 MB"
  retention: 30                       # 保留 30 天

notification:
  email:
    enabled: false
    smtp_host: ""
    recipients: []
  wechat:
    enabled: false
    webhook_url: ${WECHAT_WEBHOOK:-}
```

#### sources.yaml — 数据源配置

```yaml
sources:
  tushare:
    enabled: true
    priority: 1                       # 1=最高优先级
    token: ${TUSHARE_TOKEN}
    rate_limit:
      capacity: 10                    # 令牌桶容量
      refill_rate: 3.0                # 每秒补充令牌
    retry:
      max_retries: 3
      backoff: [1, 2, 4]             # 指数退避（秒）
    timeout: 30                       # 请求超时（秒）
    capabilities:                     # 该数据源支持的数据类型
      - daily_bars
      - adj_factor
      - stock_list
      - financial_indicator
      - valuation
      - dividend
      - index_daily
      - trade_calendar
      
  akshare:
    enabled: true
    priority: 2
    rate_limit:
      capacity: 2
      refill_rate: 1.0
    retry:
      max_retries: 2
      backoff: [2, 5]
    timeout: 30
    capabilities:
      - north_flow
      - dragon_tiger
      - margin_trading
      
  baostock:
    enabled: true
    priority: 3                       # 备用源
    rate_limit:
      capacity: 5
      refill_rate: 2.0
    retry:
      max_retries: 2
      backoff: [1, 3]
    timeout: 30
    capabilities:
      - daily_bars
      - adj_factor
      - stock_list
      
  qmt:
    enabled: false                    # Phase 3 再启用
    priority: 0                       # 实时数据源，不参与批量排序
    capabilities:
      - realtime_quote

fallback:
  daily_bars: [tushare, baostock]     # 降级顺序
  stock_list: [tushare, baostock]
  financial_indicator: [tushare]      # 无备用源
  valuation: [tushare]
```

#### schedules.yaml — 见 04_PIPELINE_DESIGN.md

---

### 配置加载机制

```
加载优先级（高 → 低）：
  1. 环境变量 (适合 CI/CD 和生产环境)
  2. .env 文件 (本地开发)
  3. YAML 配置文件 (默认值)
  
敏感字段处理：
  YAML 中使用 ${VAR_NAME:-default} 语法
  运行时从环境变量替换
  打印配置时自动遮蔽密码类字段 (显示为 ****)
```

```python
class Config:
    """
    配置管理器 — 单例模式。
    
    使用:
      config = Config.load('config/settings.yaml')
      ch_host = config.database.clickhouse.host
    """
    
    @classmethod
    def load(cls, path: str) -> 'Config':
        # 1. 加载 .env 文件到环境变量
        # 2. 读取 YAML 文件
        # 3. 递归替换 ${VAR} 占位符
        # 4. 构建 Config 对象
        ...
    
    def get(self, key_path: str, default=None):
        """
        点号分隔路径访问:
        config.get('database.clickhouse.host', 'localhost')
        """
        ...
```

---

## 数据模型定义 (dataclass)

### 为什么用 dataclass 而非裸 DataFrame

```
问题: 函数间传递 pd.DataFrame，列名拼写错误在运行时才暴露
方案: 关键数据结构用 dataclass 定义，提供类型检查和文档化
     DataFrame 依然用于批量数据传输，但结构由 dataclass 约定
```

### 核心数据模型

```python
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class StockInfo:
    """股票基础信息"""
    ts_code: str
    name: str
    industry: str
    market: str               # 主板/创业板/科创板/北交所
    list_date: date
    delist_date: Optional[date]
    is_st: bool
    is_delisted: bool


@dataclass(frozen=True)
class DailyBar:
    """单条日K线记录"""
    ts_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float             # 成交量（手）
    amount: float             # 成交额（千元）
    pct_chg: float            # 涨跌幅 (%)
    turn: float               # 换手率 (%)
    adj_factor: float
    adj_close: float          # 后复权收盘价
    is_suspended: bool
    is_limit_up: bool
    is_limit_down: bool


@dataclass(frozen=True)
class FinancialIndicator:
    """单条财务指标记录"""
    ts_code: str
    ann_date: date            # 公告日期 (PIT)
    end_date: date            # 报告期末
    roe: Optional[float]
    roa: Optional[float]
    gross_margin: Optional[float]
    net_profit_yoy: Optional[float]
    revenue_yoy: Optional[float]


@dataclass(frozen=True)
class Valuation:
    """单条估值记录"""
    ts_code: str
    trade_date: date
    pe_ttm: Optional[float]
    pb: Optional[float]
    ps_ttm: Optional[float]
    total_mv: float           # 总市值（万元）
    circ_mv: float            # 流通市值（万元）


@dataclass(frozen=True)
class TradeDate:
    """交易日信息"""
    exchange: str             # SSE / SZSE
    cal_date: date
    is_open: bool
```

### DataFrame Schema 约定

```python
class DailyBarSchema:
    """
    日K线 DataFrame 的列定义。
    作为文档和运行时校验使用，不是 ORM。
    """
    REQUIRED_COLUMNS = {
        'ts_code':    str,
        'trade_date': str,
        'open':       float,
        'high':       float,
        'low':        float,
        'close':      float,
        'volume':     float,
        'amount':     float,
    }
    
    OPTIONAL_COLUMNS = {
        'pct_chg':      float,
        'turn':         float,
        'adj_factor':   float,
        'adj_close':    float,
        'is_suspended': bool,
        'is_limit_up':  bool,
        'is_limit_down': bool,
    }
    
    @classmethod
    def validate(cls, df: pd.DataFrame) -> list[str]:
        """验证 DataFrame 是否符合 Schema，返回错误列表"""
        errors = []
        for col, dtype in cls.REQUIRED_COLUMNS.items():
            if col not in df.columns:
                errors.append(f"缺少必要列: {col}")
        return errors
```

---

## 环境变量清单

| 变量名 | 说明 | 必须 | 默认值 |
|--------|------|------|--------|
| TUSHARE_TOKEN | Tushare Pro API Token | 是 | - |
| CH_HOST | ClickHouse 地址 | 否 | localhost |
| CH_PORT | ClickHouse 端口 | 否 | 9000 |
| CH_USER | ClickHouse 用户 | 否 | default |
| CH_PASSWORD | ClickHouse 密码 | 否 | (空) |
| PG_HOST | PostgreSQL 地址 | 否 | localhost |
| PG_PORT | PostgreSQL 端口 | 否 | 5432 |
| PG_USER | PostgreSQL 用户 | 否 | postgres |
| PG_PASSWORD | PostgreSQL 密码 | 否 | (空) |
| REDIS_HOST | Redis 地址 | 否 | localhost |
| REDIS_PORT | Redis 端口 | 否 | 6379 |
| REDIS_PASSWORD | Redis 密码 | 否 | (空) |
| WECHAT_WEBHOOK | 企业微信机器人 URL | 否 | (空) |
| LOG_LEVEL | 日志级别 | 否 | INFO |

---

## .gitignore 需排除的内容

```
# 敏感配置
.env
config/secrets/

# 数据文件
data/raw/
data/cache/
*.parquet
*.csv

# 日志
logs/

# Python
__pycache__/
*.pyc
.venv/
*.egg-info/

# IDE
.vscode/
.idea/

# 系统
.DS_Store
```
