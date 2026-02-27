"""
A股量化交易系统 — 数据获取模块

模块结构:
    fetchers/   数据采集层（Tushare/AKShare/BaoStock）
    cleaners/   数据清洗层（复权/停牌/涨跌停/PIT对齐）
    writers/    数据写入层（ClickHouse/PostgreSQL/Redis）
    pipeline/   流水线编排（每日更新/历史回填/调度）
    quality/    数据质量检查
    common/     公共工具（配置/模型/限速/重试/日历）
"""
