"""
策略层包：因子库、回测库、信号与「可执行脚本示例」分离。

- **库代码**（可复用、宜写单测）：`backtest/`、`factors/`、`signals/`、`analysis/`
- **示例脚本**（参数与管线自成一体、可能较大）：`examples/`

脚本入口与变体关系见 ``strategy/README.md`` 与 ``examples/README.md``。
"""
