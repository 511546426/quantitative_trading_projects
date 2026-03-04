# 采集层详细设计

## 核心思想

采集层的职责是**从外部数据源拉取原始数据，转换为统一的内部格式**。
通过抽象基类隔离数据源差异，上层代码永远不直接调用 Tushare/AKShare 的 API。

---

## 类结构图

```
                    BaseFetcher (ABC)
                    ├── get_stock_list()
                    ├── get_daily_bars()
                    ├── get_adj_factor()
                    ├── get_financial_indicator()
                    ├── get_valuation()
                    ├── get_dividend()
                    └── get_index_daily()
                         │
          ┌──────────────┼──────────────┬──────────────┐
          │              │              │              │
   TushareFetcher  AKShareFetcher  BaoStockFetcher  QMTFetcher
   (主力数据源)    (补充数据)      (免费备用)       (实时行情)
```

---

## 抽象基类接口定义

```python
class BaseFetcher(ABC):
    """
    数据采集抽象基类。
    
    设计约定：
    1. 所有方法返回 pd.DataFrame，列名使用统一命名规范（见下方）
    2. 日期格式统一为 'YYYYMMDD' 字符串（与 Tushare 保持一致）
    3. 股票代码格式统一为 '{code}.{exchange}'，如 '000001.SZ'
    4. 方法抛出 FetchError 而非底层异常
    5. 每个方法自带速率限制（由装饰器统一处理）
    """
    
    # === 基础信息 ===
    
    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取全部 A 股股票列表
        
        Returns:
            DataFrame with columns:
            - ts_code:    str, 股票代码 (000001.SZ)
            - name:       str, 股票简称
            - industry:   str, 所属行业
            - market:     str, 市场类型 (主板/创业板/科创板/北交所)
            - list_date:  str, 上市日期 (YYYYMMDD)
            - delist_date: str | None, 退市日期
            - is_st:      bool, 当前是否 ST
        """
        ...
    
    # === 行情数据 ===
    
    @abstractmethod
    def get_daily_bars(self,
                       ts_code: str | None = None,
                       trade_date: str | None = None,
                       start_date: str | None = None,
                       end_date: str | None = None) -> pd.DataFrame:
        """
        获取日K线数据。支持两种查询模式：
        - 按股票查询：指定 ts_code + 日期范围
        - 按日期查询：指定 trade_date，获取全市场当日数据
        
        Returns:
            DataFrame with columns:
            - ts_code:    str, 股票代码
            - trade_date: str, 交易日期 (YYYYMMDD)
            - open:       float, 开盘价
            - high:       float, 最高价
            - low:        float, 最低价
            - close:      float, 收盘价
            - volume:     float, 成交量（手）
            - amount:     float, 成交额（千元）
            - pct_chg:    float, 涨跌幅 (%)
            - turn:       float, 换手率 (%)
        """
        ...
    
    @abstractmethod
    def get_adj_factor(self,
                       ts_code: str | None = None,
                       trade_date: str | None = None,
                       start_date: str | None = None,
                       end_date: str | None = None) -> pd.DataFrame:
        """
        获取复权因子
        
        Returns:
            DataFrame with columns:
            - ts_code:    str
            - trade_date: str
            - adj_factor: float, 复权因子
        """
        ...
    
    @abstractmethod
    def get_index_daily(self,
                        ts_code: str,
                        start_date: str,
                        end_date: str) -> pd.DataFrame:
        """
        获取指数日线（沪深300/中证500/上证指数等）
        
        Returns: 同 get_daily_bars 格式
        """
        ...
    
    # === 基本面数据 ===
    
    @abstractmethod
    def get_financial_indicator(self,
                                ts_code: str,
                                start_date: str | None = None,
                                end_date: str | None = None) -> pd.DataFrame:
        """
        获取财务指标
        
        Returns:
            DataFrame with columns:
            - ts_code:     str
            - ann_date:    str, 公告日期（用于 point-in-time 对齐）
            - end_date:    str, 报告期末日期
            - roe:         float, 净资产收益率 (%)
            - roa:         float, 总资产收益率 (%)
            - gross_margin: float, 毛利率 (%)
            - net_profit_yoy: float, 净利润同比增长 (%)
            - revenue_yoy:  float, 营收同比增长 (%)
        """
        ...
    
    @abstractmethod
    def get_valuation(self,
                      ts_code: str | None = None,
                      trade_date: str | None = None,
                      start_date: str | None = None,
                      end_date: str | None = None) -> pd.DataFrame:
        """
        获取每日估值指标
        
        Returns:
            DataFrame with columns:
            - ts_code:      str
            - trade_date:   str
            - pe_ttm:       float, 滚动市盈率
            - pb:           float, 市净率
            - ps_ttm:       float, 滚动市销率
            - total_mv:     float, 总市值（万元）
            - circ_mv:      float, 流通市值（万元）
        """
        ...
    
    @abstractmethod
    def get_dividend(self,
                     ts_code: str | None = None,
                     ann_date: str | None = None) -> pd.DataFrame:
        """
        获取分红送股数据
        
        Returns:
            DataFrame with columns:
            - ts_code:      str
            - ann_date:     str, 公告日期
            - ex_date:      str, 除权除息日
            - div_proc:     str, 分红进度 (实施/预案/...)
            - cash_div:     float, 每股现金分红（税前，元）
            - share_div:    float, 每股送转（股）
        """
        ...

    # === 生命周期 ===
    
    @abstractmethod
    def connect(self) -> None:
        """初始化连接（API token 验证等）"""
        ...
    
    @abstractmethod
    def close(self) -> None:
        """释放资源"""
        ...
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, *args):
        self.close()
```

---

## 统一列名规范

所有 Fetcher 必须输出以下标准列名，不同数据源需在实现内部做映射：

### 日K线标准列

| 标准列名 | 类型 | 说明 | Tushare 原名 | AKShare 原名 | BaoStock 原名 |
|----------|------|------|-------------|-------------|--------------|
| ts_code | str | 000001.SZ | ts_code | - (需拼接) | code (需转换) |
| trade_date | str | 20260226 | trade_date | 日期 (需转换) | date (需转换) |
| open | float | 开盘价 | open | 开盘 | open |
| high | float | 最高价 | high | 最高 | high |
| low | float | 最低价 | low | 最低 | low |
| close | float | 收盘价 | close | 收盘 | close |
| volume | float | 成交量（手） | vol | 成交量 | volume |
| amount | float | 成交额（千元） | amount | 成交额 | amount |
| pct_chg | float | 涨跌幅% | pct_chg | 涨跌幅 | pctChg |
| turn | float | 换手率% | turnover_rate | 换手率 | turn |

---

## 各数据源适配器设计

### TushareFetcher（首选，P0）

```
职责：全市场日K线、基本面、估值、分红
限制：
  - 免费账户：每分钟 200 次请求
  - 积分限制：部分接口需要 2000+ 积分
  - 单次返回上限：5000 条
策略：
  - 按日期拉取全市场（get_daily_bars(trade_date=xxx)），一次拉一天
  - 内置分页逻辑，超过 5000 条自动翻页
  - 使用令牌桶限速器控制请求频率
```

### AKShareFetcher（补充，P2）

```
职责：北向资金、龙虎榜、融资融券等 Tushare 不提供或需高积分的数据
限制：
  - 无官方限速，但频繁请求会被封 IP
  - 接口变化频繁，需要版本适配
策略：
  - 速率限制：最多 1 req/s
  - 接口签名监控：定期检查接口是否变更
  - 作为 Tushare 的补充，不作为主力源
```

### BaoStockFetcher（备用，P2）

```
职责：Tushare 不可用时的免费备用源
限制：
  - 需要 login/logout，有状态连接
  - 数据更新略有延迟（通常 T+1 晚上才更新完）
策略：
  - 仅在 Tushare 故障时自动降级使用
  - 实现 fallback 逻辑
```

### QMTFetcher（实盘，P3）

```
职责：实盘阶段的实时行情推送
限制：
  - 需要券商客户端在线
  - 仅支持实时行情，不提供历史批量
策略：
  - 独立运行模式，推送到 Redis
  - 与其他 Fetcher 接口一致但语义不同（流式 vs 批量）
```

---

## 数据源降级策略（Fallback）

```
正常模式：
  日K线: TushareFetcher
  基本面: TushareFetcher
  北向/龙虎榜: AKShareFetcher

降级模式（Tushare 不可用）：
  日K线: BaoStockFetcher (自动切换)
  基本面: 跳过（非关键，等恢复后补数据）
  
降级触发条件：
  1. 连续 3 次请求失败
  2. API 返回异常状态码
  3. 数据量明显异常（全市场 < 100 条）

降级恢复：
  每 5 分钟尝试恢复主数据源
  恢复成功后自动切换回主源
```

```
FetcherRouter
├── primary: TushareFetcher
├── fallback: BaoStockFetcher
├── supplementary: AKShareFetcher
│
├── get_daily_bars():
│   try:
│     return primary.get_daily_bars(...)
│   except FetchError:
│     log.warning("Tushare 失败，降级到 BaoStock")
│     return fallback.get_daily_bars(...)
│
└── health_check():
      定期检测各数据源可用性
```

---

## 速率限制设计

### 令牌桶算法

```
RateLimiter
├── capacity: int      # 桶容量（突发请求上限）
├── refill_rate: float # 每秒补充令牌数
├── tokens: float      # 当前令牌数
│
├── acquire(n=1):
│   若 tokens >= n: 消耗 n 个令牌，立即返回
│   否则: 阻塞等待至令牌足够
│
└── 各数据源配置:
      Tushare:  capacity=10, refill_rate=3.0  (每秒 3 次，突发 10 次)
      AKShare:  capacity=2,  refill_rate=1.0  (每秒 1 次)
      BaoStock: capacity=5,  refill_rate=2.0  (每秒 2 次)
```

---

## 重试策略

```
RetryPolicy
├── max_retries: 3
├── backoff: exponential (1s, 2s, 4s)
├── retryable_exceptions:
│   - ConnectionError       # 网络问题
│   - TimeoutError          # 超时
│   - RateLimitError        # 限频（等待后重试）
│   - EmptyDataError        # 空数据（可能是临时问题）
│
├── non_retryable:
│   - AuthError             # Token 无效
│   - InvalidParamError     # 参数错误
│
└── 使用方式: @retry 装饰器统一包装
```

---

## 异常体系

```
FetchError (base)
├── ConnectionError      # 网络连接失败
├── AuthError            # 认证失败（Token 过期/无效）
├── RateLimitError       # 触发限频
├── TimeoutError         # 请求超时
├── EmptyDataError       # 返回空数据（可能是非交易日/数据未就绪）
├── DataFormatError      # 返回数据格式异常
└── SourceUnavailableError  # 数据源完全不可用
```

---

## Fetcher 生命周期

```
                    ┌──────────┐
                    │  创建    │
                    └────┬─────┘
                         │ connect()
                    ┌────▼─────┐
            ┌──────→│  就绪    │◀──────┐
            │       └────┬─────┘       │
            │            │ fetch()     │ 成功 / 重试后成功
            │       ┌────▼─────┐       │
            │       │  请求中  │───────┘
            │       └────┬─────┘
            │            │ 连续失败
            │       ┌────▼─────┐
            │       │  降级    │── 定期探测 ──→ 恢复 ──┐
            │       └────┬─────┘                       │
            │            │                             │
            │            └─────────────────────────────┘
            │
            │ close()
    ┌───────▼──┐
    │  已关闭  │
    └──────────┘
```
