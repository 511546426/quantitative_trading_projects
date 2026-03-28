# A 股量化交易系统

个人量化工程：**Python 数据与策略研究** + **C++ 执行引擎** + **Python 实盘桥接（QMT 等）**。目标是在可控复杂度下形成可复现的研究—回测—信号—执行闭环。

---

## 目录与职责

| 路径 | 说明 |
|------|------|
| `data/` | 配置、Tushare 拉取、ClickHouse / PostgreSQL 写入与运维脚本 |
| `strategy/` | 回测指标、可视化、示例策略（多因子 v4、反转价值等） |
| `engine/` | C++17 交易引擎：Protobuf 信号、OMS、ZMQ 等（CMake + Conan） |
| `execution/` | 实盘侧：风控、组合、QMT 适配、ZMQ 桥、运行入口 |
| `docs/reports/` | 回测生成的图表（如 `multifactor_v4.png`） |

---

## 系统架构（与仓库对应）

```
                    ┌─────────────────────────────────────┐
                    │  外部：Tushare / 券商 QMT (xtquant)   │
                    └─────────────────┬───────────────────┘
                                      │
┌─────────────────────────────────────▼─────────────────────────────────────┐
│ 数据层 (Python)                                                            │
│  ClickHouse: stock_daily, index_daily                                     │
│  PostgreSQL: daily_valuation, stock_info, trade_calendar, …               │
│  配置: data/config/settings.yaml, sources.yaml                             │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │ SQL 读取
┌─────────────────────────────────────▼─────────────────────────────────────┐
│ 策略研究层 (Python)                                                         │
│  示例: strategy/examples/regime_switching_strategy.py（多因子 v4 + 止损）    │
│        strategy/examples/reversal_value_strategy.py                        │
│  工具: strategy/backtest/metrics.py, visualizer.py                         │
└─────────────────────────────────────┬─────────────────────────────────────┘
                        回测净值 / 年度收益              Protobuf / ZMQ
                                      │                            │
                    （不经过 C++）     │                            ▼
                                      │          ┌─────────────────────────────────┐
                                      │          │ C++ 执行引擎 (engine/)            │
                                      │          │  信号接收 · 风控 · OMS · 网关桩   │
                                      │          └─────────────────┬───────────────┘
                                      │                            │
                                      │          ┌─────────────────▼───────────────┐
                                      │          │ execution/：QMT 适配、桥接、风控   │
                                      └──────────┤  pre_trade、position 等         │
                                                 └─────────────────────────────────┘
```

**研究与实盘分工**

- **回测**：在历史行情与估值上重算信号与权重，用日收益与换手估计成本与净值；**不启动** C++ 进程，也**不模拟**交易所撮合、滑点微观结构、涨跌停排队等（除非你在策略里显式建模）。
- **实盘**：策略侧（Python）产生目标权重或订单意图 → **Protobuf + ZMQ**（或项目内桥接模块）→ C++ 引擎 / QMT 适配器 → 券商。

---

## 多因子策略 v4（当前主示例）

**文件**：`strategy/examples/regime_switching_strategy.py`

**思路**：截面多因子打分 + 季度调仓 + 惯性保留部分老仓 + **名义 2× 杠杆**（权重缩放）+ **组合净值回撤止损** + **CSI300 双均线重入**。

**因子（权重合计 1.0，截面 rank 后加权）**

| 因子 | 权重 | 含义（高分 = 更想持有） |
|------|------|-------------------------|
| MA60 | 0.25 | 价格相对 60 日均线偏低（均值回复） |
| RSI | 0.07 | RSI 低（超卖） |
| RET60 | 0.07 | 60 日收益率低（中期反转） |
| MOM120 | 0.25 | 120 日收益率高（中期动量/复苏） |
| PB | 0.16 | 市净率低 |
| SIZE | 0.08 | 流通市值小 |
| EP | 0.12 | 盈利收益率 1/PE（剔除异常 PE） |

**股票池（与回测数据一致）**

- 至少 60 日上市、非 ST/退市、20 日均成交额 ≥ 1 亿（千元口径 `MIN_AMOUNT=100_000`）
- 相对 52 周高：`close/252d_max >= 0.30`（过滤过度深跌）
- 波动率：剔除截面 20 日波动率最高的约 30%（`VOL_CUTOFF=0.70`）

**组合**

- `TOP_N=30`，`REBAL_FREQ=63`（交易日），`INERTIA=0.30`（上期持仓在调仓日加分）
- `LEVERAGE=2.0`：对权重整体乘以 2（回测意义上的名义杠杆；实盘请与融资能力一致）
- 成本：`BUY_COST_BPS=7.5`，`SELL_COST_BPS=17.5`（按换手估算）

**组合层止损（后处理净值序列）**

- 自高点回撤超过 `STOP_LOSS=15%`：当日起视为清仓（当日收益置 0）
- 重入：空仓满 `STOP_COOLDOWN=63` 日强制恢复，或满 10 日且 **昨收 CSI300 同时高于 20 日与 60 日均线**（信号均用前一日，避免前视）

**已知结论（实验记录，非承诺收益）**

- 在 2010-01-04～2026-03-20、当前数据与参数下，全样本回测曾得到约 **总收益 +8470% 量级、年化约 33%、最大回撤约 -42%**（以你本机最近一次完整跑批为准）。
- **指数均线日缩放杠杆**、**过度收紧波动/深跌过滤**、**名义杠杆从 2.0 降到 ~1.85** 等改动，在同样框架下曾显著拉低长期收益或伤害特定年份；参数改动应用小步验证。

---

## 回测说明

**前置条件**

1. Python 3.10+，建议虚拟环境：`python -m venv .venv && source .venv/bin/activate`
2. 安装依赖：`pip install -r requirements.txt`
3. 数据库：ClickHouse、PostgreSQL 按项目 `data/` 侧配置启动，`.env` 或 `data/config/settings.yaml` 中填写连接信息（与 `ClickHouseWriter` / `PostgresWriter` 一致）
4. 库内已有与策略区间匹配的 **日线行情、指数、日度估值** 等表数据（由 Tushare 拉取脚本写入）

**运行示例**

```bash
python strategy/examples/regime_switching_strategy.py
```

**输出**

- 终端：全样本指标、**分年收益与年内最大回撤**、成本粗算
- 图表：`docs/reports/multifactor_v4.png`（若路径不存在可先创建 `docs/reports/`）

**指标理解**

- **年化收益率**：按回测区间长度复利折算，不是「每一年都达到该数字」
- **年度收益**：日历年内日收益连乘，便于看单年好坏
- 回测为 **理想化成交**（收盘价、无涨跌停不可成交、无盘中熔断等），实盘需打折评估

---

## 执行层与 QMT（简表）

| 组件 | 作用 |
|------|------|
| `engine/` | C++ 引擎：接收信号、订单与风控管线（详见 `engine/src`、`engine/include`） |
| `engine/proto/signal.proto` | 与 Python 共用的信号消息定义 |
| `execution/adapter/qmt_adapter.py` | 与 miniQMT / xtquant 方向的适配说明与 ZMQ 端点约定 |
| `execution/bridge/strategy_bridge.py` | 策略侧与引擎之间的桥接示例 |
| `execution/risk/pre_trade.py` | 盘前/下单前风控示例 |

具体端点以 `execution/config.yaml`、`engine/config/engine.yaml` 为准。

---

## C++ 引擎构建（摘要）

```bash
cd engine
# 按项目既有 Conan/CMake 流程配置（见 engine 目录内 CMakeLists.txt）
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j"$(nproc)"
```

---

## 设计原则（保留）

1. **研究与执行分离**：研究在 Python；低延迟与下单在 C++；边界用明确消息格式（如 Protobuf）。
2. **回测不等于实盘**：回测验证逻辑与粗量级；实盘需接真实行情、费用与风控。
3. **风控多层**：信号层约束 + OMS/适配器层 + 券商端。
4. **渐进演进**：先跑通数据与回测，再接信号与模拟盘，最后小资金实盘。

---

## 免责声明

策略与参数仅供研究与学习；历史回测表现不构成未来收益承诺。投资有风险。
