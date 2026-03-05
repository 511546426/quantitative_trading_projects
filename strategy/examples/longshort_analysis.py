"""
多空因子组合分析。

理论测试: 如果可以做空, 反转因子能达到多高的 Sharpe?
用于:
  - 测量因子的真实 alpha 上限
  - 指导长期选股策略改进方向

注: A 股实盘无法做空个股, 此脚本仅用于研究目的。
  实际应用中可用沪深 300 期货对冲 beta, 近似实现。

用法:
    python -m strategy.examples.longshort_analysis
"""
import gc
import sys
import logging
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from data.common.config import Config
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter
from strategy.backtest.metrics import calc_full_metrics, format_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("longshort")

START = "20220101"
END   = "20260304"
MIN_AMOUNT = 100_000


def connect_db(cfg):
    ch = ClickHouseWriter(
        host=cfg.get("database.clickhouse.host", "localhost"),
        port=int(cfg.get("database.clickhouse.port", 9000)),
        database="quant", user=cfg.get("database.clickhouse.user", "default"),
        password=cfg.get("database.clickhouse.password", ""),
    )
    ch.connect()
    pg = PostgresWriter(
        host=cfg.get("database.postgres.host", "localhost"),
        port=int(cfg.get("database.postgres.port", 5432)),
        database="quant", user=cfg.get("database.postgres.user", "postgres"),
        password=cfg.get("database.postgres.password", ""),
    )
    pg.connect()
    return ch, pg


def load_price(ch):
    sql = f"""
        SELECT ts_code, trade_date,
               argMax(adj_close, trade_date) AS adj_close,
               argMax(amount, trade_date) AS amount
        FROM stock_daily
        WHERE trade_date >= '{START}' AND trade_date <= '{END}'
          AND is_suspended = 0
        GROUP BY ts_code, trade_date ORDER BY trade_date, ts_code
    """
    rows = ch._client.execute(sql)
    df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_close", "amount"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df[["adj_close", "amount"]] = df[["adj_close", "amount"]].astype(np.float32)
    close  = df.pivot(index="trade_date", columns="ts_code", values="adj_close")
    amount = df.pivot(index="trade_date", columns="ts_code", values="amount")
    del df; gc.collect()
    return close, amount


def build_universe(close, amount, exclude):
    mask = close.notna()
    for c in exclude:
        if c in mask.columns:
            mask[c] = False
    mask = mask & (close.notna().cumsum() >= 60)
    mask = mask & (amount.rolling(20, min_periods=10).mean() >= MIN_AMOUNT)
    return mask


def factor_ma60_dev(close):
    return close / close.rolling(60, min_periods=40).mean()


def quintile_longshort(
    factor: pd.DataFrame,
    universe: pd.DataFrame,
    daily_ret: pd.DataFrame,
    rebalance_freq: int = 21,
    n_per_side: int = 100,
    cost_bps: float = 17.5,  # buy: 7.5bps; sell: 17.5bps avg
) -> dict:
    """
    多空五分组测试.
    做多 bottom (低 factor) 100 只, 做空 top (高 factor) 100 只.
    """
    factor_u = factor.where(universe)

    long_weights  = pd.DataFrame(np.nan, index=factor.index, columns=factor.columns)
    short_weights = pd.DataFrame(np.nan, index=factor.index, columns=factor.columns)

    prev_long = set()
    prev_short = set()

    for i, dt in enumerate(factor.index):
        if i % rebalance_freq != 0:
            continue
        row = factor_u.loc[dt].dropna()
        if len(row) < n_per_side * 3:
            continue

        bottom = row.nsmallest(n_per_side)
        top    = row.nlargest(n_per_side)

        long_weights.loc[dt]  = 0.0
        short_weights.loc[dt] = 0.0
        long_weights.loc[dt, bottom.index]  =  1.0 / n_per_side
        short_weights.loc[dt, top.index]    = -1.0 / n_per_side

        prev_long  = set(bottom.index)
        prev_short = set(top.index)

    long_w  = long_weights.ffill().fillna(0)
    short_w = short_weights.ffill().fillna(0)

    lret = (long_w.shift(1)  * daily_ret).sum(axis=1)
    sret = (short_w.shift(1) * daily_ret).sum(axis=1)   # already negative weights
    ls_ret = lret + sret   # long + short legs

    # cost (approximate)
    lw_diff = long_w.diff().fillna(0)
    sw_diff = short_w.diff().fillna(0)
    turn = (lw_diff.abs() + sw_diff.abs()).sum(axis=1)
    cost = turn * cost_bps / 1e4
    net_ls = ls_ret - cost

    m = calc_full_metrics(net_ls, turn)
    m["long_only_sharpe"] = calc_full_metrics(lret - lw_diff.clip(lower=0).sum(axis=1)*cost_bps/1e4, lw_diff.abs().sum(axis=1)).get("sharpe_ratio", 0)
    return m


def quintile_decomposition(
    factor: pd.DataFrame,
    universe: pd.DataFrame,
    daily_ret: pd.DataFrame,
    rebalance_freq: int = 21,
    n_quintile: int = 5,
) -> pd.DataFrame:
    """五分组收益分解"""
    factor_u = factor.where(universe)
    n_per_group = None

    group_returns = {i+1: [] for i in range(n_quintile)}
    dates = []

    for i, dt in enumerate(factor.index):
        if i % rebalance_freq != 0:
            continue
        row = factor_u.loc[dt].dropna()
        n = len(row)
        if n < n_quintile * 20:
            continue
        n_per_group = n // n_quintile
        groups = pd.qcut(row, q=n_quintile, labels=False, duplicates="drop")
        dates.append(dt)
        for g in range(n_quintile):
            codes_g = groups[groups == g].index
            # group return over next rebalance_freq days
            if i + rebalance_freq < len(factor.index):
                end_dt = factor.index[i + rebalance_freq]
                ret_slice = daily_ret.loc[dt:end_dt, codes_g]
                group_returns[g + 1].append(ret_slice.mean().mean())
            else:
                group_returns[g + 1].append(np.nan)

    result = pd.DataFrame(group_returns, index=pd.DatetimeIndex(dates))
    mean_ret  = result.mean() * 252 / rebalance_freq
    return pd.DataFrame({
        "group": [f"Q{i+1}" for i in range(n_quintile)],
        "annual_return": mean_ret.values,
    })


def main():
    logger.info("=" * 60)
    logger.info("多空因子分析 (%s ~ %s)", START, END)
    logger.info("=" * 60)

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = connect_db(cfg)

    close, amount = load_price(ch)
    logger.info("行情: %d 日 × %d 股", *close.shape)

    st_rows = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st=TRUE OR is_delisted=TRUE"
    )
    exclude  = {r[0] for r in st_rows}
    universe = build_universe(close, amount, exclude)
    del amount; gc.collect()
    logger.info("股票池平均 %.0f 只", universe.sum(axis=1).mean())

    daily_ret = close.pct_change(fill_method=None).fillna(0).clip(-0.11, 0.11)
    universe_ret = daily_ret.where(universe).mean(axis=1)

    factor = factor_ma60_dev(close)

    # ─── 五分组收益分解 ───
    logger.info("五分组收益分解 (rebalance=21d)...")
    quinq = quintile_decomposition(factor, universe, daily_ret, 21)
    print("\n" + "=" * 40)
    print("MA60偏离率 五分组收益 (annualized)")
    print("  Q1=最低偏离(最超卖) ... Q5=最高偏离(最超买)")
    print("-" * 40)
    for _, row in quinq.iterrows():
        bar = "█" * int(max(0, row["annual_return"] * 50))
        print(f"  {row['group']}: {row['annual_return']:+.2%}  {bar}")

    # ─── 多空组合 ───
    logger.info("多空组合测试 (freq=21, n=100, no cost)...")
    m_nc = quintile_longshort(factor, universe, daily_ret, 21, 100, cost_bps=0)
    logger.info("多空组合测试 (freq=21, n=100, with cost)...")
    m_wc = quintile_longshort(factor, universe, daily_ret, 21, 100, cost_bps=17.5)

    print("\n" + "=" * 60)
    print("多空组合绩效 (MA60偏离率因子)")
    print("=" * 60)
    print(f"\n--- 不含成本 ---")
    print(f"  夏普比率:      {m_nc.get('sharpe_ratio', 0):.3f}")
    print(f"  年化收益率:    {m_nc.get('annualized_return', 0):.2%}")
    print(f"  年化波动率:    {m_nc.get('annualized_volatility', 0):.2%}")
    print(f"  最大回撤:      {m_nc.get('max_drawdown', 0):.2%}")
    print(f"  做多腿夏普:    {m_nc.get('long_only_sharpe', 0):.3f}")

    print(f"\n--- 含成本 (买7.5bps+卖17.5bps avg) ---")
    print(f"  夏普比率:      {m_wc.get('sharpe_ratio', 0):.3f}")
    print(f"  年化收益率:    {m_wc.get('annualized_return', 0):.2%}")
    print(f"  年化波动率:    {m_wc.get('annualized_volatility', 0):.2%}")
    print(f"  最大回撤:      {m_wc.get('max_drawdown', 0):.2%}")
    print(f"  累计成本:      {m_wc.get('cumulative_cost', 0):.2%}")
    print(f"  做多腿夏普:    {m_wc.get('long_only_sharpe', 0):.3f}")

    # 不同调仓频率
    print(f"\n--- 不同调仓频率 (含成本, n=100) ---")
    print(f"{'频率':>6} {'夏普':>8} {'年化收益':>10} {'换手':>10}")
    for freq in [5, 10, 21, 42]:
        m = quintile_longshort(factor, universe, daily_ret, freq, 100, 17.5)
        print(f"  {freq:>4}d  {m.get('sharpe_ratio',0):>7.3f}  {m.get('annualized_return',0):>9.2%}  {(m.get('annualized_turnover',0) or 0):>9.0%}")

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
