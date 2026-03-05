"""
全因子 IC 研究脚本 (低内存版)。

在 4GB 内存环境下运行，逐因子计算 IC 后释放内存。

用法:
    python -m strategy.examples.factor_research
"""
import gc
import sys
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from data.common.config import Config
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("factor_research")

START = "20220101"
END = "20260304"
FORWARD_DAYS = [5, 10, 20]
MIN_STOCKS = 50


def load_core_data(ch: ClickHouseWriter) -> tuple[pd.DataFrame, pd.DataFrame]:
    """只加载 close 和 amount (float32)"""
    sql = f"""
        SELECT ts_code, trade_date,
               argMax(adj_close, trade_date) AS adj_close,
               argMax(amount, trade_date) AS amount
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
    df["amount"] = df["amount"].astype(np.float32)

    close = df.pivot(index="trade_date", columns="ts_code", values="adj_close")
    amount = df.pivot(index="trade_date", columns="ts_code", values="amount")
    del df
    gc.collect()
    logger.info("行情数据: %d 日 × %d 股", *close.shape)
    return close, amount


def load_valuation_col(pg: PostgresWriter, col: str) -> pd.DataFrame:
    """按列加载估值数据"""
    rows = pg.execute_query(f"""
        SELECT ts_code, trade_date, {col}
        FROM daily_valuation
        WHERE trade_date >= '{START}' AND trade_date <= '{END}'
        ORDER BY trade_date, ts_code
    """)
    df = pd.DataFrame(rows, columns=["ts_code", "trade_date", col])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df[col] = df[col].astype(np.float32)
    pivot = df.pivot(index="trade_date", columns="ts_code", values=col)
    del df
    gc.collect()
    return pivot


def build_universe(close: pd.DataFrame, amount: pd.DataFrame, exclude: set) -> pd.DataFrame:
    """构建可交易股票池"""
    mask = close.notna()
    for code in exclude:
        if code in mask.columns:
            mask[code] = False
    listing_days = close.notna().cumsum()
    mask = mask & (listing_days >= 60)
    avg_amt = amount.rolling(20, min_periods=10).mean()
    mask = mask & (avg_amt > 100000)
    logger.info("股票池: 平均 %.0f 只可交易", mask.sum(axis=1).mean())
    return mask


def calc_ic_fast(factor_values: pd.DataFrame, fwd_return: pd.DataFrame, min_stocks: int) -> dict:
    """快速 IC 计算"""
    common_dates = factor_values.index.intersection(fwd_return.index)
    common_codes = factor_values.columns.intersection(fwd_return.columns)
    fv = factor_values.loc[common_dates, common_codes]
    fr = fwd_return.loc[common_dates, common_codes]

    ics = []
    for dt in common_dates:
        f_row = fv.loc[dt].dropna()
        r_row = fr.loc[dt].dropna()
        overlap = f_row.index.intersection(r_row.index)
        if len(overlap) < min_stocks:
            continue
        corr, _ = spearmanr(f_row[overlap].values, r_row[overlap].values)
        if not np.isnan(corr):
            ics.append(corr)

    if not ics:
        return {"ic_mean": np.nan, "icir": np.nan, "ic_pos": np.nan, "n": 0}

    ic_arr = np.array(ics)
    ic_mean = ic_arr.mean()
    ic_std = ic_arr.std()
    return {
        "ic_mean": round(ic_mean, 5),
        "icir": round(ic_mean / ic_std, 4) if ic_std > 0 else 0,
        "ic_pos": round((ic_arr > 0).mean(), 4),
        "n": len(ics),
    }


def compute_factor_and_ic(
    name: str,
    factor_values: pd.DataFrame,
    close: pd.DataFrame,
    universe: pd.DataFrame,
    direction: int = 1,
) -> list[dict]:
    """计算单个因子的 IC (所有 forward days)"""
    fv = factor_values.where(universe)
    if direction == -1:
        fv = -fv

    results = []
    for fwd in FORWARD_DAYS:
        fwd_ret = close.pct_change(fwd, fill_method=None).shift(-fwd)
        fwd_ret = fwd_ret.where(universe)
        ic = calc_ic_fast(fv, fwd_ret, MIN_STOCKS)
        results.append({
            "factor": name,
            "fwd": fwd,
            "direction": direction,
            **ic,
        })
    return results


def main():
    logger.info("=" * 60)
    logger.info("全因子 IC 研究 (低内存版)")
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

    close, amount = load_core_data(ch)

    st_rows = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
    )
    exclude = {r[0] for r in st_rows}
    universe = build_universe(close, amount, exclude)
    del amount
    gc.collect()

    all_results = []

    # === 动量因子 ===
    logger.info("--- 动量因子 ---")

    for n in [5, 10, 20, 60]:
        name = f"return_{n}d"
        fv = close / close.shift(n) - 1
        all_results.extend(compute_factor_and_ic(name, fv, close, universe, 1))
        logger.info("  ✓ %s", name)
        del fv; gc.collect()

    fv = close / close.shift(20) - 1
    mkt = fv.mean(axis=1)
    fv_rs = fv.sub(mkt, axis=0)
    all_results.extend(compute_factor_and_ic("relative_strength_20d", fv_rs, close, universe, 1))
    logger.info("  ✓ relative_strength_20d")
    del fv, fv_rs, mkt; gc.collect()

    for n in [20, 60]:
        name = f"price_ma{n}_ratio"
        ma = close.rolling(n, min_periods=n).mean()
        fv = close / ma
        all_results.extend(compute_factor_and_ic(name, fv, close, universe, 1))
        logger.info("  ✓ %s", name)
        del ma, fv; gc.collect()

    ret_12m = close / close.shift(252) - 1
    ret_1m = close / close.shift(21) - 1
    fv = ret_12m - ret_1m
    all_results.extend(compute_factor_and_ic("momentum_12_1", fv, close, universe, 1))
    logger.info("  ✓ momentum_12_1")
    del ret_12m, ret_1m, fv; gc.collect()

    # === 反转因子 ===
    logger.info("--- 反转因子 ---")

    delta = close.diff()
    gain = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
    loss = (-delta).clip(lower=0).ewm(span=14, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    fv = 100 - 100 / (1 + rs)
    all_results.extend(compute_factor_and_ic("rsi_14", fv, close, universe, -1))
    logger.info("  ✓ rsi_14")
    del delta, gain, loss, rs, fv; gc.collect()

    ma20 = close.rolling(20, min_periods=20).mean()
    fv = (close - ma20) / ma20
    all_results.extend(compute_factor_and_ic("bias_20", fv, close, universe, -1))
    logger.info("  ✓ bias_20")

    std20 = close.rolling(20, min_periods=20).std()
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20
    width = (upper - lower).replace(0, np.nan)
    fv = (close - lower) / width
    all_results.extend(compute_factor_and_ic("boll_pos_20", fv, close, universe, -1))
    logger.info("  ✓ boll_pos_20")
    del ma20, std20, upper, lower, width, fv; gc.collect()

    # === 波动率因子 ===
    logger.info("--- 波动率因子 ---")

    daily_ret = close.pct_change(fill_method=None)
    for n in [20, 60]:
        name = f"realized_vol_{n}d"
        fv = daily_ret.rolling(n, min_periods=n).std()
        all_results.extend(compute_factor_and_ic(name, fv, close, universe, -1))
        logger.info("  ✓ %s", name)
        del fv; gc.collect()

    fv = daily_ret.clip(upper=0).rolling(20, min_periods=20).std()
    all_results.extend(compute_factor_and_ic("downside_vol_20d", fv, close, universe, -1))
    logger.info("  ✓ downside_vol_20d")
    del daily_ret, fv; gc.collect()

    # === 换手率因子 (需重新加载 turn) ===
    logger.info("--- 换手率因子 ---")
    sql_turn = f"""
        SELECT ts_code, trade_date, argMax(turn, trade_date) AS turn
        FROM stock_daily
        WHERE trade_date >= '{START}' AND trade_date <= '{END}' AND is_suspended = 0
        GROUP BY ts_code, trade_date ORDER BY trade_date, ts_code
    """
    rows_t = ch._client.execute(sql_turn)
    df_t = pd.DataFrame(rows_t, columns=["ts_code", "trade_date", "turn"])
    df_t["trade_date"] = pd.to_datetime(df_t["trade_date"])
    df_t["turn"] = df_t["turn"].astype(np.float32)
    turn_pivot = df_t.pivot(index="trade_date", columns="ts_code", values="turn")
    del rows_t, df_t; gc.collect()

    fv = turn_pivot.rolling(20, min_periods=20).mean()
    all_results.extend(compute_factor_and_ic("turnover_20d", fv, close, universe, -1))
    logger.info("  ✓ turnover_20d")
    del turn_pivot, fv; gc.collect()

    # === 基本面因子 (逐列加载) ===
    logger.info("--- 基本面因子 ---")

    pe = load_valuation_col(pg, "pe_ttm")
    fv = 1.0 / pe.replace(0, np.nan)
    all_results.extend(compute_factor_and_ic("ep", fv, close, universe, 1))
    logger.info("  ✓ ep")
    del pe, fv; gc.collect()

    pb = load_valuation_col(pg, "pb")
    fv = 1.0 / pb.replace(0, np.nan)
    all_results.extend(compute_factor_and_ic("bp", fv, close, universe, 1))
    logger.info("  ✓ bp")
    del pb, fv; gc.collect()

    ps = load_valuation_col(pg, "ps_ttm")
    fv = 1.0 / ps.replace(0, np.nan)
    all_results.extend(compute_factor_and_ic("sp", fv, close, universe, 1))
    logger.info("  ✓ sp")
    del ps, fv; gc.collect()

    mv = load_valuation_col(pg, "total_mv")
    fv = np.log(mv.replace(0, np.nan))
    all_results.extend(compute_factor_and_ic("ln_market_cap", fv, close, universe, -1))
    logger.info("  ✓ ln_market_cap")
    del mv, fv; gc.collect()

    # === 输出结果 ===
    df = pd.DataFrame(all_results)

    print("\n" + "=" * 85)
    print("因子 IC 分析结果")
    print("=" * 85)

    for fwd in FORWARD_DAYS:
        subset = df[df["fwd"] == fwd].copy()
        subset["abs_icir"] = subset["icir"].abs()
        subset = subset.sort_values("abs_icir", ascending=False)
        print(f"\n--- 预测 {fwd} 日收益 (按 |ICIR| 降序) ---")
        print(subset[["factor", "direction", "ic_mean", "icir", "ic_pos", "n"]].to_string(index=False))

    best = df[df["fwd"] == 10].copy()
    best["abs_icir"] = best["icir"].abs()
    good = best[best["abs_icir"] > 0.3].sort_values("abs_icir", ascending=False)
    if not good.empty:
        print(f"\n★ 推荐因子 (10日 |ICIR| > 0.3):")
        for _, row in good.iterrows():
            d = "↑" if (row["ic_mean"] > 0) == (row["direction"] == 1) else "↓"
            print(f"  {d} {row['factor']}: IC={row['ic_mean']:.4f}, ICIR={row['icir']:.3f}")

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
