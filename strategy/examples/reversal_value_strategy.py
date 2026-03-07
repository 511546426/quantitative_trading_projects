"""
A 股反转多因子策略（最终版）。

回测结论（2020-2026，含 2022 熊市完整周期）：
    Sharpe = 0.60  年化 = 16.8%  MaxDD = -43%
    各年: 2020:+41% 2021:+8% 2022:-5% 2023:+6% 2024:+7% 2025:+58% 2026:-3%

策略逻辑：
    信号 = 0.6 × rank(-price/MA60)   ← 最强因子 ICIR=-0.68
          + 0.2 × rank(-RSI14)        ← 超卖 ICIR=+0.56
          + 0.2 × rank(-Return20d)    ← 短期反转
    过滤：剔除波动率最高 30%（vol_cutoff=0.7）
    持仓：等权持有 15 只（集中持高信号股）
    换仓：月频（每 21 个交易日），含持仓惯性加分

注意：
    - 不做均线择时（均线择时在 A 股牛市中会切断涨幅）
    - 流动性门槛：日均成交额 > 1 亿
    - 排除 ST、次新股（上市 < 60 天）

用法：
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
logger = logging.getLogger("reversal_strategy")

# ─────────────────────────────────────────────────────────────
# 最终参数（经参数扫描确认，勿随意修改）
# ─────────────────────────────────────────────────────────────
START        = "20200101"   # 回测起始日
END          = "20260304"   # 回测终止日（实盘时改为今日）
MIN_AMOUNT   = 100_000      # 日均成交额门槛（千元），即 1 亿

TOP_N        = 15           # 持仓只数：15 > 20 > 30（越少 Sharpe 越高，但集中度更高）
REBAL_FREQ   = 21           # 调仓周期（交易日），约 1 个月
W_MA60       = 0.6          # MA60 偏离率权重（最强因子）
W_RSI        = 0.2          # RSI 超卖权重
W_RET20      = 0.2          # 20 日反转权重
VOL_CUTOFF   = 0.7          # 波动率过滤：剔除排名最高的 30%
INERTIA      = 0.05         # 持仓惯性加分（已持有的股票信号+0.05，降低换手）

# 成本参数（A 股实盘）
BUY_COST_BPS  = 7.5         # 买入：佣金 2.5bps + 冲击 5bps
SELL_COST_BPS = 17.5        # 卖出：佣金 2.5bps + 印花税 10bps + 冲击 5bps


# ─────────────────────────────────────────────────────────────
# 数据加载
# ─────────────────────────────────────────────────────────────
def connect_db(cfg):
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


def load_price(ch) -> tuple[pd.DataFrame, pd.DataFrame]:
    """加载复权收盘价和成交额（float32 节省内存）"""
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
    df[["adj_close", "amount"]] = df[["adj_close", "amount"]].astype(np.float32)
    close  = df.pivot(index="trade_date", columns="ts_code", values="adj_close")
    amount = df.pivot(index="trade_date", columns="ts_code", values="amount")
    del df; gc.collect()
    logger.info("行情数据: %d 交易日 × %d 只股票", *close.shape)
    return close, amount


def load_exclude_list(pg) -> set:
    """加载 ST / 退市股票代码"""
    rows = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
    )
    return {r[0] for r in rows}


# ─────────────────────────────────────────────────────────────
# 股票池构建
# ─────────────────────────────────────────────────────────────
def build_universe(close: pd.DataFrame, amount: pd.DataFrame, exclude: set) -> pd.DataFrame:
    """
    可交易股票池：
      - 排除 ST / 退市
      - 排除次新股（上市 < 60 个交易日）
      - 排除流动性不足（日均成交额 < MIN_AMOUNT 千元）
    """
    mask = close.notna()

    # 排除 ST / 退市
    for code in exclude:
        if code in mask.columns:
            mask[code] = False

    # 次新股过滤（累计出现天数 < 60）
    mask = mask & (close.notna().cumsum() >= 60)

    # 流动性过滤（20 日滚动均值）
    avg_amount = amount.rolling(20, min_periods=10).mean()
    mask = mask & (avg_amount >= MIN_AMOUNT)

    logger.info("股票池平均 %.0f 只/日", mask.sum(axis=1).mean())
    return mask


# ─────────────────────────────────────────────────────────────
# 因子计算
# ─────────────────────────────────────────────────────────────
def calc_factors(close: pd.DataFrame, universe: pd.DataFrame) -> tuple:
    """
    计算三个因子并做截面百分位 rank（值越高 → 买入意愿越强）：
      1. rank(-price/MA60) : 低于 MA60 越多 → rank 越高（均值回归做多）
      2. rank(-RSI14)      : RSI 越低（超卖）→ rank 越高（反弹做多）
      3. rank(-Return20d)  : 近 20 日跌幅越大 → rank 越高（短期反转）
    过滤：rank(vol20) > VOL_CUTOFF 的股票置 NaN（排除高波动股）
    """
    # 因子 1: MA60 偏离率（最强，权重 0.6）
    ma60 = close.rolling(60, min_periods=40).mean()
    f_ma60_dev = (-close / ma60).where(universe)          # 低偏离 = 高信号

    # 因子 2: RSI(14)（权重 0.2）
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
    loss  = (-delta).clip(lower=0).ewm(span=14, adjust=False).mean()
    rsi   = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    f_rsi_inv = (-rsi).where(universe)                    # 低 RSI = 高信号

    # 因子 3: 20 日收益率反转（权重 0.2）
    ret20 = close / close.shift(20) - 1
    f_ret20_rev = (-ret20).where(universe)                # 近期跌多 = 高信号

    # 波动率（用于过滤，不参与信号合成）
    vol20 = close.pct_change(fill_method=None).rolling(20, min_periods=20).std()
    vol_rank = vol20.where(universe).rank(axis=1, pct=True)
    vol_mask = vol_rank <= VOL_CUTOFF                     # True = 低波动，允许入选

    # 截面百分位 rank
    r_ma60  = f_ma60_dev.rank(axis=1, pct=True)
    r_rsi   = f_rsi_inv.rank(axis=1, pct=True)
    r_ret20 = f_ret20_rev.rank(axis=1, pct=True)

    # 加权合成信号，并应用波动率过滤
    signal = (W_MA60 * r_ma60 + W_RSI * r_rsi + W_RET20 * r_ret20).where(vol_mask)

    logger.info(
        "因子权重: MA60=%.1f  RSI=%.1f  Ret20=%.1f  vol_cutoff=%.1f",
        W_MA60, W_RSI, W_RET20, VOL_CUTOFF,
    )
    return signal


# ─────────────────────────────────────────────────────────────
# 持仓权重生成
# ─────────────────────────────────────────────────────────────
def generate_weights(
    signal: pd.DataFrame,
    top_n: int = TOP_N,
    rebal_freq: int = REBAL_FREQ,
    inertia: float = INERTIA,
) -> pd.DataFrame:
    """
    月频调仓，等权持有 top_n 只最高信号股。
    持仓惯性：已持有的股票信号加 `inertia`，减少不必要换手。
    """
    weights   = pd.DataFrame(np.nan, index=signal.index, columns=signal.columns)
    prev_held = set()

    for i, dt in enumerate(signal.index):
        if i % rebal_freq != 0:
            continue
        row = signal.loc[dt].dropna()
        if len(row) < top_n:
            continue

        # 持仓惯性加分
        for code in prev_held:
            if code in row.index:
                row[code] += inertia

        top = row.nlargest(top_n)
        weights.loc[dt] = 0.0
        weights.loc[dt, top.index] = 1.0 / top_n  # 等权
        prev_held = set(top.index)

    # 非调仓日沿用上一次权重
    weights = weights.ffill().fillna(0)
    return weights


# ─────────────────────────────────────────────────────────────
# 组合收益计算
# ─────────────────────────────────────────────────────────────
def calc_portfolio_return(
    weights: pd.DataFrame,
    close: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    """返回 (净收益率序列, 换手率序列)，已扣除成本"""
    daily_ret = close.pct_change(fill_method=None).fillna(0).clip(-0.11, 0.11)
    port_ret  = (weights.shift(1) * daily_ret).sum(axis=1)

    w_diff    = weights.diff().fillna(0)
    buy_turn  = w_diff.clip(lower=0).sum(axis=1)
    sell_turn = (-w_diff.clip(upper=0)).sum(axis=1)
    cost      = buy_turn * BUY_COST_BPS / 1e4 + sell_turn * SELL_COST_BPS / 1e4

    net_ret  = port_ret - cost
    turnover = w_diff.abs().sum(axis=1)
    return net_ret.dropna(), turnover


# ─────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("A 股反转策略（最终版）%s ~ %s", START, END)
    logger.info("持仓: %d 只  调仓: 每 %d 日  因子: MA60(%.1f)+RSI(%.1f)+Ret20(%.1f)",
                TOP_N, REBAL_FREQ, W_MA60, W_RSI, W_RET20)
    logger.info("=" * 60)

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = connect_db(cfg)

    # ── 数据 ──
    close, amount = load_price(ch)
    exclude       = load_exclude_list(pg)
    universe      = build_universe(close, amount, exclude)
    del amount; gc.collect()

    # ── 因子与信号 ──
    logger.info("计算因子信号...")
    signal = calc_factors(close, universe)

    # ── 权重 ──
    logger.info("生成持仓权重（top=%d，月频）...", TOP_N)
    weights = generate_weights(signal, TOP_N, REBAL_FREQ, INERTIA)

    # ── 回测 ──
    logger.info("运行回测...")
    net_ret, turnover = calc_portfolio_return(weights, close)

    m = calc_full_metrics(net_ret, turnover)

    # ── 报告 ──
    print("\n" + format_report(m))

    print("\n── 逐年收益 ──")
    print(f"  {'年份':>4}  {'年度收益':>8}  {'最大回撤':>8}")
    for yr in range(2020, 2027):
        r_ = net_ret[f"{yr}0101":f"{yr}1231"]
        if len(r_) < 20:
            continue
        cum = (1 + r_).prod() - 1
        dd  = ((1 + r_).cumprod() / (1 + r_).cumprod().cummax() - 1).min()
        tag = " ← 大牛市" if yr in (2020, 2025) else (" ← 熊市" if yr == 2022 else "")
        print(f"  {yr}   {cum:>+8.1%}  {dd:>8.1%}{tag}")

    ann_turn = (m.get("annualized_turnover", 0) or 0)
    print(f"\n── 成本估算（年换手 {ann_turn:.0%}）──")
    for cap in [100_000, 200_000, 300_000, 500_000]:
        c = cap * ann_turn * (BUY_COST_BPS + SELL_COST_BPS) / 2 / 1e4
        print(f"  {cap//10000:>3}万本金：年成本约 {c:>6.0f} 元 ({c/cap:.1%})")

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
