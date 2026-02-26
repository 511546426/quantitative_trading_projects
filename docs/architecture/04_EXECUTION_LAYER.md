# 执行层设计（交易执行 + OMS）

## 职责

执行层接收策略信号，经过风控校验后，将订单发送到券商，并管理订单全生命周期。

---

## 执行层架构

```
策略信号
   │
   ▼
┌─────────────────────────────────────┐
│         信号预处理                   │
│  ・标准化信号格式                    │
│  ・合并同一股票的多个信号             │
│  ・过滤明显不可执行的信号             │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│         风控校验（RMS）              │
│  ・仓位合规检查                      │
│  ・资金充足性检查                    │
│  ・涨跌停检查                        │
│  ・重复订单检查                      │
└─────────────────┬───────────────────┘
                  │ 通过
                  ▼
┌─────────────────────────────────────┐
│         订单管理系统（OMS）          │
│  ・订单生成（含执行算法选择）         │
│  ・订单状态跟踪（待发/已发/成交/撤销）│
│  ・撤单/补单逻辑                     │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│         券商接口层                   │
│  ・miniQMT（推荐个人实盘起步）       │
│  ・XTP（机构级，低延迟）             │
│  ・CTP（期货，未来扩展）             │
└─────────────────────────────────────┘
```

---

## 阶段一：Python 执行层（起步）

### 使用 miniQMT 接口

```python
# 推荐：迅投 miniQMT，大多数主流券商支持，免费
# 支持：A 股、ETF、可转债；提供实时行情 + 交易接口

from xtquant import xtdata, xttrader

class QMTExecutionHandler:
    def __init__(self, account_id: str, qmt_path: str):
        self.trader = xttrader.XtQuantTrader(qmt_path, account_id)
        self.account = xttrader.StockAccount(account_id)
        self.trader.start()
        self.trader.connect()
    
    def buy(self, ts_code: str, amount: float, order_type: str = 'MARKET'):
        """
        ts_code: '000001.SZ'
        amount: 买入金额（元），内部转换为手数
        """
        # 1. 获取当前价格
        price = self._get_latest_price(ts_code)
        # 2. 计算手数（A 股最小单位 100 股）
        shares = int(amount / price / 100) * 100
        if shares < 100:
            return None  # 金额太小，不交易
        
        # 3. 发送订单
        order_id = self.trader.order_stock(
            self.account,
            stock_code=ts_code,
            order_type=xttrader.ORDER_TYPE_MARKET,  # 市价单
            order_volume=shares,
            price=0,
            strategy_name='quant',
            order_remark=''
        )
        return order_id
    
    def sell(self, ts_code: str, shares: int):
        """卖出全部或指定手数"""
        ...
```

### 订单状态机

```
NEW → SUBMITTED → PENDING_OPEN → PARTIALLY_FILLED → FILLED
                      ↓                                ↓
                   CANCELLED                        CANCELLED（部分撤）
```

```python
from enum import Enum

class OrderStatus(Enum):
    NEW = "new"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

@dataclass
class Order:
    order_id: str
    ts_code: str
    direction: str          # 'BUY' / 'SELL'
    target_shares: int
    filled_shares: int = 0
    avg_fill_price: float = 0.0
    status: OrderStatus = OrderStatus.NEW
    created_at: datetime = field(default_factory=datetime.now)
    strategy_id: str = ""   # 来自哪个策略（多策略时用）
```

---

## 阶段二：C++ 执行引擎（核心目标）

> 详见 [C++ 架构设计文档](../cpp-design/CPP_ENGINE.md)，此处仅列出与执行层相关的设计。

### 为什么需要 C++ 执行引擎？

| 维度 | Python | C++ |
|------|--------|-----|
| 订单处理延迟 | 毫秒级 | 微秒级 |
| 并发策略数量 | 受 GIL 限制 | 真并发，无上限 |
| 内存控制 | 不可控 GC | 精确控制 |
| 私募技术要求 | 不够 | 主流 |

**对个人实盘**：A 股日频策略，Python 完全够用。
**对职业发展**：C++ 执行引擎是私募的标配，是核心技术壁垒。

### C++ 执行引擎核心组件

```
engine/
├── core/
│   ├── event_bus.h          # 事件总线（SPSC lockfree queue）
│   ├── order.h              # 订单数据结构（POD，缓存友好）
│   └── position.h           # 持仓数据结构
├── oms/
│   ├── order_manager.h      # 订单生命周期管理
│   ├── position_manager.h   # 持仓管理
│   └── pnl_calculator.h     # 实时 PnL 计算
├── risk/
│   ├── pre_trade_check.h    # 交易前风控
│   └── real_time_risk.h     # 实时风控（持仓/回撤监控）
├── gateway/
│   ├── base_gateway.h       # 抽象接口
│   ├── xtp_gateway.cpp      # XTP 实现
│   └── ctp_gateway.cpp      # CTP 实现（期货）
└── algo/
    ├── twap.cpp             # TWAP 算法
    └── vwap.cpp             # VWAP 算法
```

---

## 执行算法

### 为什么需要执行算法？

个股日成交额较小时，大单直接市价成交会显著推高价格（市场冲击成本）。

### TWAP（时间加权平均）

```
将一笔大单拆成 N 份，每隔固定时间发一份
适合：流动性较差、价格较为稳定的股票
```

### VWAP（成交量加权平均）

```
根据历史各时段成交量分布，按比例在不同时段发单
适合：跟踪市场平均成本
```

### 实盘中的简化版（日频策略）

```python
# 日频策略，每日只在开盘/收盘附近交易
# 简单起步：集合竞价 + 开盘 9:30~9:45 分批买入

def morning_auction_order(self, orders: List[Order]):
    """集合竞价（9:15~9:25）挂单，通常成交价格较好"""
    for order in orders:
        self.submit_order(order, order_type='LIMIT', 
                         price=order.ref_price * 0.999)  # 略低于参考价
```

---

## 本阶段交付物

**阶段一（Python）**
- [ ] QMT 交易接口封装
- [ ] 订单状态机实现
- [ ] 持仓管理模块
- [ ] 交易日志记录

**阶段二（C++）**
- [ ] C++ 事件总线（SPSC）
- [ ] OMS 核心逻辑
- [ ] XTP/QMT Gateway
- [ ] TWAP 执行算法
- [ ] Python-C++ 信号通信（ZMQ）
