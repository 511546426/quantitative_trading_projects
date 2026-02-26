# 策略研究层设计

## 职责

策略层负责从数据中发现 Alpha（超额收益来源），并将其转化为可执行的交易信号。

---

## 策略分类（适合个人实盘的方向）

### 一、量价因子策略（推荐起步）

**逻辑**：基于历史价格/成交量数据计算因子，做多强势/低估个股，做空弱势（A 股无法做空，则回避）

| 策略类型 | 典型因子 | 持仓周期 | 难度 |
|----------|----------|----------|------|
| 动量策略 | 过去 20 日涨幅、相对强弱 | 周/月 | ★★☆ |
| 均值回归 | RSI超卖、布林带下轨 | 日/周 | ★★☆ |
| 成交量异动 | 量比、OBV 背离 | 日 | ★★★ |
| 低波动因子 | 过去 N 日波动率 | 月 | ★★☆ |

### 二、基本面因子策略（中期目标）

**逻辑**：财务质量好、估值低的公司长期跑赢

| 策略类型 | 典型因子 | 持仓周期 | 难度 |
|----------|----------|----------|------|
| 价值策略 | PB/PE/PS | 季/年 | ★★☆ |
| 质量策略 | ROE/毛利率/净利润增速 | 月/季 | ★★★ |
| 成长策略 | 营收/利润增速加速度 | 月/季 | ★★★ |
| 多因子综合 | 价值 + 质量 + 动量 | 月 | ★★★★ |

### 三、事件驱动策略（进阶）

| 策略类型 | 触发事件 | 难度 |
|----------|----------|------|
| 财报超预期 | 业绩预告/快报 | ★★★★ |
| 大股东增持 | 公告解析 | ★★★★ |
| 指数调整 | 沪深 300/中证 500 成分股变更 | ★★★★ |

---

## 因子研究框架

```
因子研究流程
────────────────────────────────────────────────────
原始数据
   │
   ▼
因子计算（Feature Engineering）
   │ 横截面标准化 / 行业中性化 / 市值中性化
   ▼
因子有效性检验
   │ IC/ICIR 分析
   │ 分层回测（十分组）
   │ 因子衰减测试
   ▼
因子组合（Multi-Factor）
   │ 等权 / IC 加权 / 机器学习加权
   ▼
信号生成
   │ 每日/每周计算信号，确定买卖列表
   ▼
回测验证
────────────────────────────────────────────────────
```

### 因子有效性指标

```python
# IC（信息系数）= 因子值与未来收益率的 Spearman 相关系数
# 好的因子：|IC| > 0.05，ICIR > 0.5

def calc_ic(factor_df: pd.DataFrame, 
            return_df: pd.DataFrame, 
            forward_n: int = 5) -> pd.Series:
    """
    factor_df: index=date, columns=ts_code, values=factor_value
    return_df: index=date, columns=ts_code, values=forward_return
    """
    ic_series = []
    for date in factor_df.index:
        f = factor_df.loc[date].dropna()
        r = return_df.loc[date].dropna()
        common = f.index.intersection(r.index)
        if len(common) < 30:
            continue
        ic = spearmanr(f[common], r[common]).correlation
        ic_series.append(ic)
    return pd.Series(ic_series)
```

---

## 回测引擎设计（Python）

### 向量化回测（速度优先）

```python
# 核心思想：用矩阵运算替代逐日循环
# 适合：参数扫描、因子研究

class VectorizedBacktester:
    """
    所有计算基于矩阵，速度比事件驱动快 100~1000 倍
    代价：无法精确模拟订单撮合、成本摩擦建模较粗糙
    """
    def run(self, 
            signals: pd.DataFrame,    # shape=(dates, stocks), 1=买 -1=卖 0=不持仓
            prices: pd.DataFrame,     # shape=(dates, stocks)
            cost_bps: float = 15      # 单边成本 15bps = 0.15%
           ) -> BacktestResult:
        
        # 计算目标持仓权重
        weights = self._signals_to_weights(signals)
        
        # 计算每日收益
        daily_returns = prices.pct_change()
        portfolio_returns = (weights.shift(1) * daily_returns).sum(axis=1)
        
        # 扣除交易成本
        turnover = weights.diff().abs().sum(axis=1)
        costs = turnover * cost_bps / 10000
        net_returns = portfolio_returns - costs
        
        return BacktestResult(net_returns)
```

### 事件驱动回测（精度优先）

```python
# 适合：策略最终验证、模拟实盘
# 结构与实盘引擎完全一致，便于 "一键切换"

class EventDrivenBacktester:
    def __init__(self):
        self.data_handler = HistoricalDataHandler()
        self.strategy = None
        self.portfolio = Portfolio()
        self.execution_handler = SimulatedExecutionHandler()
        self.event_queue = Queue()
    
    def run(self):
        while True:
            # 1. 推送市场数据事件
            bar_event = self.data_handler.get_next_bar()
            if bar_event is None:
                break
            
            # 2. 策略处理，生成信号事件
            signal_event = self.strategy.on_bar(bar_event)
            
            # 3. 仓位管理，生成订单事件
            order_event = self.portfolio.on_signal(signal_event)
            
            # 4. 模拟撮合，生成成交事件
            fill_event = self.execution_handler.execute(order_event)
            
            # 5. 更新组合状态
            self.portfolio.on_fill(fill_event)
```

---

## 回测陷阱（重要！）

### 1. 未来函数（Look-ahead Bias）
```python
# 错误：用当天收盘价计算的因子，在当天收盘时买入
factor_today = calc_factor(close_today)   # ❌ 用收盘价算因子
signal_today = generate_signal(factor_today)
buy_at_close_today(signal_today)          # ❌ 当天收盘成交？不可能！

# 正确：因子用昨天数据，今天开盘买入
factor_yesterday = calc_factor(close_yesterday)  # ✅
signal = generate_signal(factor_yesterday)
buy_at_open_today(signal)                         # ✅ T+1 开盘成交
```

### 2. 幸存者偏差（Survivorship Bias）
```python
# 错误：只用当前还在市的股票做历史回测
# 正确：回测时的股票池 = 当时实际存在的股票（含已退市）
# ClickHouse 中需要保存所有历史上市过的股票
```

### 3. 交易成本低估
```python
# A 股实际交易成本（单边）
COST_TABLE = {
    'commission': 0.00025,    # 佣金 2.5bps（可以谈更低）
    'stamp_duty': 0.001,      # 印花税 10bps（仅卖出）
    'slippage': 0.0005,       # 滑点 5bps（保守估计）
}
# 单边约 8~15bps，双边约 16~25bps
# 换手率高的策略，成本吃掉收益是主要风险
```

### 4. 过拟合（Overfitting）
```python
# Walk-Forward Optimization（WFO）是防过拟合的标准做法
# 训练集 ──────────────┐
# 验证集         ──────┤
# 测试集（不能回头看）  └──────
# 滚动窗口，每次只用历史数据优化参数
```

---

## 策略评估指标

| 指标 | 计算公式 | 达标线 |
|------|----------|--------|
| 年化收益率 | `(1+r)^252 - 1` | > 20% |
| 最大回撤 | `max(peak - trough) / peak` | < 20% |
| **夏普比率** | `mean(r) / std(r) * sqrt(252)` | **> 1.5** |
| **卡玛比率** | `年化收益 / 最大回撤` | **> 1.0** |
| 年化换手率 | `双边换手 / 年` | < 500%（日频策略） |
| 胜率 | `盈利次数 / 总次数` | > 50%（趋势策略可以 < 50%） |
| 盈亏比 | `平均盈利 / 平均亏损` | > 1.5 |

---

## 本阶段交付物

- [ ] 基础因子库（20+ 量价因子）
- [ ] 因子 IC 分析框架
- [ ] 向量化回测引擎 v1
- [ ] 事件驱动回测引擎 v1
- [ ] 策略评估报告模板（含收益/回撤/成本分析）
- [ ] 第一个可盈利策略（回测夏普 > 1.5）
