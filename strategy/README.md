# 策略层（`strategy/`）说明

目标：**库与脚本分离**，避免把「可复用回测/因子逻辑」和「某一条具体策略的参数大脚本」混在同一认知里。

---

## 1. 库代码（优先复用、适合加 pytest）

| 目录 | 职责 |
|------|------|
| `backtest/` | 向量化/事件回测骨架、`metrics` 绩效指标、`visualizer` 出图 |
| `factors/` | 截面因子基类与实现（动量、价值、波动等） |
| `signals/` | 信号合成（如与因子、IC 分析衔接） |
| `analysis/` | IC 等横截面研究工具 |

**原则**：新业务优先 **调用库 API**；少在 `examples/` 里复制粘贴 `metrics` 公式。

---

## 2. 示例脚本（`examples/`）——「一条策略一个文件」

以下脚本顶部多有 `sys.path.insert(...)`，便于在仓库根目录执行：

```bash
cd /path/to/quantitative_trading_projects
PYTHONPATH=. python strategy/examples/<脚本>.py
```

更推荐（与包结构一致）：

```bash
PYTHONPATH=. python -m strategy.examples.<模块名>
```

（部分脚本 `if __name__ == "__main__"` 已按 `-m` 方式书写，以脚本内说明为准。）

### 2.1 主策略（多因子 v4.1）

| 文件 | 定位 |
|------|------|
| **`regime_switching_strategy.py`** | **唯一入口**：全市场多因子 + 止损 + CSI300 牛熊杠杆；**Web** `regime-model-run`、**execution** `weights_from_trading_panel` 均与此一致。 |

对外说明「多因子 v4.1」即指本文件。

### 2.2 其他示例

| 文件 | 定位 |
|------|------|
| `momentum_strategy.py` | 经典 12-1 动量 + 向量化回测器演示。 |
| `factor_research.py` | 低内存因子 IC 扫描脚本。 |
| `longshort_analysis.py` | 多空/分析类示例。 |

详细一行说明见 **`examples/README.md`**。

---

## 3. 与仓库其它部分的关系

- **数据**：示例脚本普遍直接读 `data` 层配置与 ClickHouse / PostgreSQL（与 `data/` 管道回填结果一致）。
- **Web**：`ui/server/research.py` 仅依赖 **`run_regime_model_for_web`**（定义在 `regime_switching_strategy.py`），勿在多处复制 v4.1 主循环。
- **执行**：`execution/` 按需引用 **`regime_switching_strategy.weights_from_trading_panel`** 等与 v4.1 一致的权重管线。

---

## 4. 若仍觉得「乱」，下一步可做的代码级整理（未在本次自动执行）

- 将主脚本的**公共数据加载**抽到 `strategy/pipelines/regime_data.py`（需一轮回归对比净值）。
- 为 `run_regime_model_for_web` 单独小模块，示例脚本只做 CLI 薄封装（改动面大，需单独 PR）。

当前提交以 **文档与包边界** 为主；执行层权重与 v4.1 主脚本对齐。
