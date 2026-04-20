# `strategy/examples/` 脚本一览

执行前请在**仓库根目录**设置 `PYTHONPATH=.`（或使用 `python -m strategy.examples.<name>`）。

| 模块 | 一句话 |
|------|--------|
| `regime_switching_strategy.py` | **主策略 v4.1**；含 `run_regime_model_for_web`（Web）与 `weights_from_trading_panel`（execution 日线权重）。 |
| `momentum_strategy.py` | 12-1 动量 + `VectorizedBacktester` 演示。 |
| `factor_research.py` | 因子 IC 低内存扫描。 |
| `longshort_analysis.py` | 多空分析示例。 |

**不要**在业务代码里默认 `import *` 示例脚本；应只 import **明确导出的函数**（如 `run_regime_model_for_web`）。
