# 风险控制层设计（RMS）

## 核心理念

> "风控不是为了不赚钱，而是为了活得足够久，让策略的期望收益充分实现。"

个人实盘的最大敌人不是不够聪明，而是**一次性大亏导致心态崩溃或本金耗尽**。

---

## 风险控制三道防线

```
第一道防线（策略层）     第二道防线（OMS层）     第三道防线（外部）
────────────────────     ────────────────────     ────────────────────
信号过滤               订单合规检查             券商风控
  ├ 停牌股不买          ├ 单笔金额上限            ├ 最大持仓比例
  ├ 涨跌停不追          ├ 单日总买入上限           ├ 信用账户规则
  ├ 退市风险股过滤       ├ 持仓集中度检查           └ 融资杠杆限制
  └ 流动性过滤          ├ 重复信号防护
                        └ 异常价格保护
```

---

## 仓位管理方法

### Kelly 公式（理论最优，实践需打折）

```python
def kelly_fraction(win_rate: float, win_loss_ratio: float) -> float:
    """
    win_rate: 胜率（0~1）
    win_loss_ratio: 盈亏比（平均盈利 / 平均亏损）
    """
    b = win_loss_ratio
    p = win_rate
    q = 1 - p
    kelly = (b * p - q) / b
    # 实际使用半凯利，降低波动
    return kelly * 0.5

# 例：胜率 55%，盈亏比 1.5
# kelly = (1.5 * 0.55 - 0.45) / 1.5 = 0.25 → 半凯利 12.5% 仓位
```

### 固定比例仓位法（初期推荐）

```python
class PositionSizer:
    def __init__(self, total_capital: float):
        self.total_capital = total_capital
        self.max_single_position = 0.10   # 单只股票最大 10%
        self.max_total_position = 0.80    # 最大总仓位 80%（留 20% 现金）
        self.max_sector_position = 0.30   # 单行业最大 30%
    
    def calc_position_size(self, 
                           signal_strength: float,  # 0~1
                           current_positions: Dict) -> float:
        """返回建议买入金额"""
        available_cash = self._get_available_cash(current_positions)
        target_amount = self.total_capital * self.max_single_position * signal_strength
        return min(target_amount, available_cash * 0.5)
```

---

## 动态止损体系

### 策略级止损

```python
class StrategyStopLoss:
    """整个策略（组合层面）的止损"""
    
    def __init__(self, max_drawdown_pct: float = 0.15):
        self.max_drawdown = max_drawdown_pct  # 组合最大回撤 15% 触发
        self.peak_value = None
    
    def check(self, current_value: float) -> bool:
        """返回 True = 需要触发止损（清仓/减仓）"""
        if self.peak_value is None:
            self.peak_value = current_value
            return False
        self.peak_value = max(self.peak_value, current_value)
        drawdown = (self.peak_value - current_value) / self.peak_value
        return drawdown >= self.max_drawdown
```

### 个股止损

```python
class StockStopLoss:
    """单只股票的止损规则"""
    
    # 三种止损方式，根据策略类型选择
    
    def fixed_stop(self, entry_price: float, stop_pct: float = 0.08) -> float:
        """固定比例止损：亏损 8% 止损"""
        return entry_price * (1 - stop_pct)
    
    def atr_stop(self, entry_price: float, atr: float, multiplier: float = 2.5) -> float:
        """ATR 止损：波动率自适应，趋势跟踪策略推荐"""
        return entry_price - multiplier * atr
    
    def time_stop(self, entry_date: date, holding_days: int = 10) -> bool:
        """时间止损：持仓超过 N 天未盈利，直接出场"""
        return (date.today() - entry_date).days >= holding_days
```

---

## 关键风控参数

| 参数 | 初始值 | 说明 |
|------|--------|------|
| 单只股票最大仓位 | 10% | 防止黑天鹅重创 |
| 最大持仓只数 | 10~20 只 | 分散但不过度分散 |
| 单行业最大仓位 | 30% | 防行业系统性风险 |
| 组合最大回撤止损 | 15% | 触发后暂停策略 |
| 单只股票止损 | 8% | 固定止损线 |
| 最大总仓位 | 80% | 保留安全垫 |
| 日内最大亏损 | 3% | 触发后当日不再开仓 |

---

## 风控参数动态调整

市场状态决定仓位

```python
class MarketRegimeDetector:
    """市场状态检测，动态调整整体仓位"""
    
    def detect(self, index_data: pd.DataFrame) -> str:
        ma20 = index_data['close'].rolling(20).mean().iloc[-1]
        ma60 = index_data['close'].rolling(60).mean().iloc[-1]
        current = index_data['close'].iloc[-1]
        
        if current > ma20 > ma60:
            return 'bull'    # 牛市：满仓操作
        elif current < ma20 < ma60:
            return 'bear'    # 熊市：半仓或空仓
        else:
            return 'neutral' # 震荡：七成仓

REGIME_POSITION_LIMIT = {
    'bull': 0.80,
    'neutral': 0.60,
    'bear': 0.30,
}
```

---

## 本阶段交付物

- [ ] 仓位计算模块
- [ ] 个股止损模块（固定止损 + ATR 止损）
- [ ] 组合最大回撤监控
- [ ] 市场状态检测器
- [ ] 风控参数配置文件（YAML）
- [ ] 风控事件日志
