"""
execution - 交易执行层

接上 QMT 账户即可实盘：
    python -m execution.run --config execution/config.yaml

架构：
    策略信号 → 风控检查 → 订单拆分（TWAP/VWAP） → QMT 下单 → 成交回报 → 持仓/PnL 更新

模块：
    gateway/   - 券商接口（QMT / Mock）
    oms/       - 订单管理 + 持仓管理 + PnL
    risk/      - 交易前风控 + 实时监控
    algo/      - 执行算法（TWAP / VWAP）
    persist/   - 状态持久化（PostgreSQL）
    monitor/   - 监控告警（企微 / 邮件）
"""
__version__ = "0.1.0"
