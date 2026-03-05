"""
动量策略回测示例。

策略逻辑 (经典 12-1 动量 + 低波动过滤):
    1. 计算 12 个月动量 (跳过最近 1 个月, 避免短期反转)
    2. 用低波动率过滤高风险股票
    3. 剔除 ST / 停牌 / 涨跌停 / 次新股
    4. 等权持有 top 20, 周频调仓
    5. A 股实际成本模型 (佣金+印花税+滑点)

用法:
    python -m strategy.examples.momentum_strategy
"""
import sys
import logging
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from data.common.config import Config
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter
from strategy.factors.momentum import Momentum12_1, ReturnN
from strategy.factors.volatility import RealizedVolatility
from strategy.signals.signal_generator import SignalGenerator, SignalConfig
from strategy.backtest.vectorized import (
    VectorizedBacktester,
    CostModel,
    WeightScheme,
)
from strategy.backtest.metrics import format_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("momentum_strategy")


def load_price_data(ch: ClickHouseWriter, start: str, end: str) -> dict[str, pd.DataFrame]:
    """从 ClickHouse 加载行情数据并转为 pivot 格式"""
    sql = f"""
        SELECT ts_code, trade_date,
               argMax(adj_close, trade_date) AS adj_close,
               argMax(volume, trade_date) AS volume,
               argMax(turn, trade_date) AS turn
        FROM stock_daily
        WHERE trade_date >= '{start}' AND trade_date <= '{end}'
          AND is_suspended = 0
        GROUP BY ts_code, trade_date
        ORDER BY trade_date, ts_code
    """
    rows = ch._client.execute(sql)
    df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_close", "volume", "turn"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    close_pivot = df.pivot(index="trade_date", columns="ts_code", values="adj_close")
    volume_pivot = df.pivot(index="trade_date", columns="ts_code", values="volume")
    turn_pivot = df.pivot(index="trade_date", columns="ts_code", values="turn")

    return {
        "close": close_pivot,
        "volume": volume_pivot,
        "turn": turn_pivot,
    }


def load_stock_filter(pg: PostgresWriter) -> set[str]:
    """加载 ST / 退市股票列表用于过滤"""
    rows = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
    )
    return {r[0] for r in rows}


def load_benchmark(ch: ClickHouseWriter, start: str, end: str) -> pd.Series:
    """加载沪深300指数作为基准"""
    sql = f"""
        SELECT trade_date, close
        FROM index_daily
        WHERE ts_code = '000300.SH'
          AND trade_date >= '{start}' AND trade_date <= '{end}'
        ORDER BY trade_date
    """
    rows = ch._client.execute(sql)
    df = pd.DataFrame(rows, columns=["trade_date", "close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.set_index("trade_date")
    return df["close"].pct_change()


def apply_filters(
    signals: pd.DataFrame,
    close_pivot: pd.DataFrame,
    exclude_codes: set[str],
    min_listing_days: int = 60,
) -> pd.DataFrame:
    """应用交易过滤规则"""
    for code in exclude_codes:
        if code in signals.columns:
            signals[code] = 0.0

    valid_count = close_pivot.notna().cumsum()
    for code in signals.columns:
        if code in valid_count.columns:
            mask = valid_count[code] < min_listing_days
            signals.loc[mask, code] = 0.0

    return signals


def main():
    logger.info("=" * 60)
    logger.info("动量策略回测")
    logger.info("=" * 60)

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")

    ch = ClickHouseWriter(
        host=cfg.get("database.clickhouse.host", "localhost"),
        port=int(cfg.get("database.clickhouse.port", 9000)),
        database="quant",
        user=cfg.get("database.clickhouse.user", "default"),
        password=cfg.get("database.clickhouse.password", ""),
    )
    ch.connect()

    pg = PostgresWriter(
        host=cfg.get("database.postgres.host", "localhost"),
        port=int(cfg.get("database.postgres.port", 5432)),
        database="quant",
        user=cfg.get("database.postgres.user", "postgres"),
        password=cfg.get("database.postgres.password", ""),
    )
    pg.connect()

    start_date = "2020-01-01"
    end_date = "2026-03-04"

    logger.info("加载行情数据: %s ~ %s", start_date, end_date)
    data = load_price_data(ch, start_date.replace("-", ""), end_date.replace("-", ""))
    close = data["close"]
    logger.info("数据维度: %d 交易日 × %d 只股票", *close.shape)

    exclude = load_stock_filter(pg)
    logger.info("过滤 ST/退市股票: %d 只", len(exclude))

    benchmark = load_benchmark(ch, start_date.replace("-", ""), end_date.replace("-", ""))

    mom_12_1 = Momentum12_1()
    ret_20 = ReturnN(20)
    vol_factor = RealizedVolatility(n=20)

    logger.info("计算因子...")
    mom_values = mom_12_1.compute(close)
    ret_values = ret_20.compute(close)
    vol_values = vol_factor.compute(close)

    mom_rank = mom_values.rank(axis=1, pct=True)
    ret_rank = ret_values.rank(axis=1, pct=True)
    vol_rank = vol_values.rank(axis=1, pct=True)

    combined = 0.5 * mom_rank + 0.3 * ret_rank + 0.2 * (1 - vol_rank)
    combined[vol_rank > 0.8] = np.nan

    signal_gen = SignalGenerator(SignalConfig(top_n=30))
    signals = signal_gen._top_n_signal(combined)
    signals = apply_filters(signals, close, exclude)

    cost = CostModel(commission_bps=2.5, stamp_duty_bps=10.0, slippage_bps=5.0)
    backtester = VectorizedBacktester(
        cost_model=cost,
        weight_scheme=WeightScheme.EQUAL,
        max_stocks=30,
        rebalance_freq=21,
    )

    logger.info("运行向量化回测...")
    result = backtester.run(signals, close, benchmark=benchmark)

    metrics = result.summary()
    report = format_report(metrics)
    print("\n" + report)

    logger.info("夏普比率: %.3f", metrics.get("sharpe_ratio", 0))
    logger.info("年化收益: %.2f%%", metrics.get("annualized_return", 0) * 100)
    logger.info("最大回撤: %.2f%%", metrics.get("max_drawdown", 0) * 100)

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
