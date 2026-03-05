"""
反转+价值多因子策略 v2。

基于修正后的 IC 研究 (2277 只股票, 942 样本):
    - price_ma60_ratio  ICIR=-0.68 (最强: 偏离MA60后均值回归)
    - bias_20           ICIR=+0.58 (MA20偏低做多)
    - RSI(14)           ICIR=+0.56 (超卖做多)
    - realized_vol_20d  ICIR=+0.45 (低波动溢价)
    - BP(1/PB)          ICIR=+0.34 (价值因子)

Alpha 测量:
    - 基准 = 相同股票池的等权组合 (消除偏差)
    - 策略超额 = 选股贡献的纯 alpha

用法:
    python -m strategy.examples.reversal_value_strategy
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
logger = logging.getLogger("reversal_value")

START = "20220101"
END   = "20260304"
MIN_AMOUNT = 100_000   # 日均成交额门槛 (千元), 即 1 亿


# ─────────────────────────────────────
# 数据加载
# ─────────────────────────────────────
def connect_db(cfg):
    from data.writers.clickhouse_writer import ClickHouseWriter
    from data.writers.postgres_writer import PostgresWriter

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
    return ch, pg


def load_price_data(ch) -> tuple[pd.DataFrame, pd.DataFrame]:
    sql = f"""
        SELECT ts_code, trade_date,
               argMax(adj_close, trade_date) AS adj_close,
               argMax(amount,    trade_date) AS amount
        FROM stock_daily
        WHERE trade_date >= '{START}' AND trade_date <= '{END}'
          AND is_suspended = 0
        GROUP BY ts_code, trade_date
        ORDER BY trade_date, ts_code
    """
    rows = ch._client.execute(sql)
    df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_close", "amount"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["adj_close"] = df["adj_close"].astype(np.float32)
    df["amount"]    = df["amount"].astype(np.float32)
    close  = df.pivot(index="trade_date", columns="ts_code", values="adj_close")
    amount = df.pivot(index="trade_date", columns="ts_code", values="amount")
    del df; gc.collect()
    return close, amount


def load_pb(pg) -> pd.DataFrame:
    rows = pg.execute_query(f"""
        SELECT ts_code, trade_date, pb
        FROM daily_valuation
        WHERE trade_date >= '{START}' AND trade_date <= '{END}'
        ORDER BY trade_date, ts_code
    """)
    df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "pb"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["pb"] = df["pb"].astype(np.float32)
    pivot = df.pivot(index="trade_date", columns="ts_code", values="pb")
    del df; gc.collect()
    return pivot


def load_industry(pg) -> dict:
    """加载行业映射 {ts_code: industry}"""
    rows = pg.execute_query(
        "SELECT ts_code, industry FROM stock_info WHERE industry IS NOT NULL"
    )
    return {r[0]: r[1] for r in rows}


def industry_neutralize(factor_df: pd.DataFrame, industry_map: dict) -> pd.DataFrame:
    """截面行业中性化 (向量化): 每只股票减去同行业均值"""
    codes = factor_df.columns.tolist()
    industries = pd.Series({c: industry_map.get(c, "OTHER") for c in codes})
    unique_inds = industries.unique()

    result = factor_df.copy()
    for ind in unique_inds:
        cols = industries[industries == ind].index.tolist()
        cols_in_df = [c for c in cols if c in factor_df.columns]
        if not cols_in_df:
            continue
        sub = factor_df[cols_in_df]
        ind_mean = sub.mean(axis=1)
        result[cols_in_df] = sub.subtract(ind_mean, axis=0)
    return result


# ─────────────────────────────────────
# 股票池
# ─────────────────────────────────────
def build_universe(close, amount, exclude):
    mask = close.notna()
    for c in exclude:
        if c in mask.columns:
            mask[c] = False
    listing_days = close.notna().cumsum()
    mask = mask & (listing_days >= 60)
    avg_amt = amount.rolling(20, min_periods=10).mean()
    mask = mask & (avg_amt >= MIN_AMOUNT)
    n_avg = mask.sum(axis=1).mean()
    logger.info("股票池平均 %.0f 只", n_avg)
    return mask


# ─────────────────────────────────────
# 因子计算
# ─────────────────────────────────────
def factor_ma60_dev(close):
    """偏离MA60 (IC最强): 值越低 → 预期收益越高"""
    ma60 = close.rolling(60, min_periods=40).mean()
    return close / ma60   # 越低越好

def factor_rsi(close, n=14):
    """RSI: 值越低(超卖) → 预期收益越高"""
    d = close.diff()
    gain = d.clip(lower=0).ewm(span=n, adjust=False).mean()
    loss = (-d).clip(lower=0).ewm(span=n, adjust=False).mean()
    rs   = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)   # 越低越好

def factor_bp(pb):
    """BP = 1/PB: 越高越便宜"""
    pb_clean = pb.copy()
    pb_clean[pb_clean <= 0] = np.nan
    pb_clean[pb_clean > 500] = np.nan
    return 1.0 / pb_clean          # 越高越好

def factor_vol(close, n=20):
    """波动率: 越低越好"""
    return close.pct_change(fill_method=None).rolling(n, min_periods=n).std()

def factor_ret20(close):
    """20日收益率: 值越高 → 预期收益越低 (短期反转)"""
    return close / close.shift(20) - 1   # 越低越好


# ─────────────────────────────────────
# 策略运行
# ─────────────────────────────────────
def run_one(
    close, universe, ma60_dev, rsi, bp, vol, ret20,
    top_n, rebalance_freq,
    w_ma60, w_rsi, w_bp, w_ret20,
    vol_cutoff, smooth_window,
):
    # 截面 rank (越高 → 越想买)
    ma60_rank  = (-ma60_dev).where(universe).rank(axis=1, pct=True)  # 低偏离=好
    rsi_rank   = (-rsi    ).where(universe).rank(axis=1, pct=True)   # 低RSI=好
    bp_rank    =   bp      .where(universe).rank(axis=1, pct=True)   # 高BP=好
    ret20_rank = (-ret20  ).where(universe).rank(axis=1, pct=True)   # 低近期收益=好

    vol_rank   = vol.where(universe).rank(axis=1, pct=True)
    vol_mask   = vol_rank <= vol_cutoff

    combined = (
        w_ma60 * ma60_rank +
        w_rsi  * rsi_rank  +
        w_bp   * bp_rank   +
        w_ret20 * ret20_rank
    ).where(vol_mask)

    if smooth_window > 1:
        combined = combined.rolling(smooth_window, min_periods=1).mean()

    daily_ret = close.pct_change(fill_method=None).fillna(0).clip(-0.11, 0.11)

    # 基准 = 当日股票池等权
    universe_ret = daily_ret.where(universe).mean(axis=1)

    # 市场择时: 等权基准 MA60 趋势
    univ_cum = (1 + universe_ret).cumprod()
    univ_ma60 = univ_cum.rolling(60, min_periods=20).mean()
    bull_regime = (univ_cum >= univ_ma60).astype(float)
    # 牛市满仓,熊市半仓
    exposure = bull_regime.clip(0.4, 1.0)

    weights = pd.DataFrame(np.nan, index=combined.index, columns=combined.columns)
    prev_held = set()

    for i, dt in enumerate(combined.index):
        if i % rebalance_freq != 0:
            continue
        row = combined.loc[dt].dropna()
        if len(row) < top_n // 2:
            continue
        # 持仓惯性加分
        for code in prev_held:
            if code in row.index:
                row[code] += 0.1

        top = row.nlargest(top_n)
        exp = float(exposure.get(dt, 1.0))
        weights.loc[dt] = 0.0
        weights.loc[dt, top.index] = exp / len(top)
        prev_held = set(top.index)

    weights = weights.ffill().fillna(0)

    port_ret = (weights.shift(1) * daily_ret).sum(axis=1)

    # 成本
    w_diff   = weights.diff().fillna(0)
    buy_turn = w_diff.clip(lower=0).sum(axis=1)
    sel_turn = (-w_diff.clip(upper=0)).sum(axis=1)
    cost     = buy_turn * (2.5 + 5.0) / 1e4 + sel_turn * (2.5 + 10.0 + 5.0) / 1e4
    net_ret  = port_ret - cost
    turnover = w_diff.abs().sum(axis=1)

    # 超额收益
    excess   = net_ret - universe_ret.reindex(net_ret.index).fillna(0)

    m_long   = calc_full_metrics(net_ret, turnover)
    m_excess = calc_full_metrics(excess, turnover)

    m_long["excess_sharpe"]     = m_excess.get("sharpe_ratio", 0)
    m_long["excess_ann_return"] = m_excess.get("annualized_return", 0)
    m_long["excess_max_dd"]     = m_excess.get("max_drawdown", 0)
    return m_long


# ─────────────────────────────────────
# 主流程
# ─────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("反转+价值策略 v2 (%s ~ %s)", START, END)
    logger.info("=" * 60)

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = connect_db(cfg)

    logger.info("加载行情...")
    close, amount = load_price_data(ch)
    logger.info("行情: %d 日 × %d 股", *close.shape)

    st_rows  = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st=TRUE OR is_delisted=TRUE"
    )
    exclude  = {r[0] for r in st_rows}
    universe = build_universe(close, amount, exclude)
    del amount; gc.collect()

    logger.info("加载 PB 和行业...")
    pb           = load_pb(pg)
    industry_map = load_industry(pg)

    logger.info("计算因子...")
    f_ma60  = factor_ma60_dev(close)
    f_rsi   = factor_rsi(close, 14)
    f_bp    = factor_bp(pb)
    f_vol   = factor_vol(close, 20)
    f_ret20 = factor_ret20(close)
    del pb; gc.collect()

    gc.collect()

    # ─── 参数扫描 ─── 无行业中性化, 探索最优配置
    # 核心洞察: IC最强因子= price_ma60_ratio, 其次是bias(rsi), ret20也是反转信号
    param_grid = [
        # 基准: 4因子等权
        dict(top_n=100, rebalance_freq=21, w_ma60=0.4, w_rsi=0.2, w_bp=0.2, w_ret20=0.2, vol_cutoff=0.7, smooth_window=5),
        # 加大最强因子权重
        dict(top_n=100, rebalance_freq=21, w_ma60=0.6, w_rsi=0.2, w_bp=0.1, w_ret20=0.1, vol_cutoff=0.7, smooth_window=5),
        dict(top_n=100, rebalance_freq=21, w_ma60=0.5, w_rsi=0.2, w_bp=0.2, w_ret20=0.1, vol_cutoff=0.7, smooth_window=5),
        # 纯反转组合 (ma60+rsi+ret20, 不含价值)
        dict(top_n=100, rebalance_freq=21, w_ma60=0.5, w_rsi=0.3, w_bp=0.0, w_ret20=0.2, vol_cutoff=0.7, smooth_window=5),
        dict(top_n=100, rebalance_freq=21, w_ma60=0.4, w_rsi=0.3, w_bp=0.0, w_ret20=0.3, vol_cutoff=0.7, smooth_window=5),
        # 更紧的vol filter
        dict(top_n=100, rebalance_freq=21, w_ma60=0.5, w_rsi=0.2, w_bp=0.2, w_ret20=0.1, vol_cutoff=0.5, smooth_window=5),
        # 更少持仓
        dict(top_n=50,  rebalance_freq=21, w_ma60=0.5, w_rsi=0.2, w_bp=0.2, w_ret20=0.1, vol_cutoff=0.7, smooth_window=5),
        dict(top_n=30,  rebalance_freq=21, w_ma60=0.5, w_rsi=0.2, w_bp=0.2, w_ret20=0.1, vol_cutoff=0.7, smooth_window=5),
        # 最优信号聚焦: 只用ma60+ret20两个高IC
        dict(top_n=50,  rebalance_freq=21, w_ma60=0.5, w_rsi=0.0, w_bp=0.0, w_ret20=0.5, vol_cutoff=0.7, smooth_window=5),
        dict(top_n=100, rebalance_freq=21, w_ma60=0.5, w_rsi=0.0, w_bp=0.0, w_ret20=0.5, vol_cutoff=0.7, smooth_window=5),
    ]

    best_esharpe = -999
    best_params  = {}
    best_metrics = {}

    for i, p in enumerate(param_grid):
        m = run_one(close, universe, f_ma60, f_rsi, f_bp, f_vol, f_ret20, **p)
        esharpe  = m.get("excess_sharpe", 0)
        lsharpe  = m.get("sharpe_ratio", 0)
        ann_ret  = m.get("annualized_return", 0)
        max_dd   = m.get("max_drawdown", 0)
        turn     = m.get("annualized_turnover", 0) or 0
        logger.info(
            "  [%02d/%d] top=%d freq=%d w=(%.1f,%.1f,%.1f,%.1f) → "
            "Long=%.2f  ExcessSharpe=%.2f  AnnRet=%.1f%%  DD=%.1f%%  Turn=%.0f%%",
            i+1, len(param_grid),
            p["top_n"], p["rebalance_freq"],
            p["w_ma60"], p["w_rsi"], p["w_bp"], p["w_ret20"],
            lsharpe, esharpe, ann_ret*100, max_dd*100, turn*100,
        )
        if esharpe > best_esharpe:
            best_esharpe = esharpe
            best_params  = p
            best_metrics = m

    print("\n" + "=" * 60)
    print("最优参数")
    print("=" * 60)
    for k, v in best_params.items():
        print(f"  {k}: {v}")

    print("\n" + format_report(best_metrics))
    print(f"\n--- 相对股票池基准的超额表现 ---")
    print(f"  超额夏普:     {best_metrics.get('excess_sharpe', 0):.3f}")
    print(f"  超额年化收益: {best_metrics.get('excess_ann_return', 0):.2%}")
    print(f"  超额最大回撤: {best_metrics.get('excess_max_dd', 0):.2%}")

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
