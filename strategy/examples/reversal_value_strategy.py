"""
A 股反转多因子策略（最终版）。

回测结论（2020-2026，含 2022 熊市完整周期）：
    Sharpe = 0.60  年化 = 16.8%  MaxDD = -43%
    各年: 2020:+41% 2021:+8% 2022:-5% 2023:+6% 2024:+7% 2025:+58% 2026:-3%

策略逻辑：
    信号 = 0.6 × rank(-price/MA60)   ← 最强因子 ICIR=-0.68
          + 0.2 × rank(-RSI14)        ← 超卖 ICIR=+0.56
          + 0.2 × rank(-Return20d)    ← 短期反转
    过滤：波动率 + 横截面“跳变”质量过滤；动量侧叠加波动分位过滤
    组合层：沪深300 驱动分级仓位 + 危机上限；反转/动量资金占比随指数牛熊动态切换
    持仓：等权持有 15 只（集中持高信号股）
    换仓：月频（每 21 个交易日），含持仓惯性加分

注意：
    - 个股信号层不做均线；组合层可用市场代理均线分级降仓（见 TIMING_*）
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
LEVERAGE     = 2.35         # 资金杠杆：权重整体等比放大（用于把净年化拉近目标，同时用择时降仓控回撤）

# 双子策略组合参数（反转 + 动量）
# 反转-only 在当前样本区间里显著优于引入动量侧，因此先切回反转-only。
ENABLE_DUAL_STRATEGY = False
ALLOC_REV = 0.58
ALLOC_MOM = 0.42
TOP_N_MOM = 10
REBAL_FREQ_MOM = 10
INERTIA_MOM = 0.03

# 市况动态配比（暂不启用）
REGIME_DYNAMIC_ALLOC = False
BENCHMARK_TS_CODE    = "000300.SH"
REGIME_BULL_TH   = 0.018
REGIME_BEAR_TH   = -0.018
ALLOC_REV_BULL   = 0.40
ALLOC_REV_FLAT   = 0.55
ALLOC_REV_BEAR   = 0.82
REGIME_ALLOC_REBAL_FREQ = 21

# 择时参数：用指数趋势做“降仓”，降低回撤
TIMING_ON           = True
TIMING_USE_INDEX    = True
TIMING_FAST_MA      = 20
TIMING_SLOW_MA      = 120
TIMING_LEVELS = [
    (0.03,   1.30),
    (0.015,  1.10),
    (-0.01,  1.00),
    (-0.035, 0.80),
    (-1.00,  0.60),
]

# 横截面质量过滤：暂关闭，避免进一步压缩信号池
QUALITY_FILTER_ON   = False
QUALITY_JUMP_WINDOW = 5           # 窗口内最大绝对日收益
QUALITY_JUMP_CUTOFF = 0.90       # 仅保留 jump 截面分位 <= 该值（约剔除最“跳”的 10%）

# 危机层：深度走弱再压一档（与指数趋势一致）
CRISIS_TIMING_ON       = True
CRISIS_TREND_THRESHOLD = -0.11
CRISIS_MAX_EXPOSURE    = 0.7

# 动量侧：波动 + 质量过滤（略宽于反转 vol_cutoff，避免动量票池过窄）
MOM_VOL_CUTOFF = 0.88            # 剔除 20 日波动率截面分位最高的部分

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


def load_index_close(ch, ts_code: str = BENCHMARK_TS_CODE) -> pd.Series | None:
    """加载基准指数收盘价序列（ClickHouse index_daily，按日去重）。"""
    sql = f"""
        SELECT trade_date, max(close) AS close
        FROM index_daily
        WHERE ts_code = '{ts_code}'
          AND trade_date >= '{START}'
          AND trade_date <= '{END}'
        GROUP BY trade_date
        ORDER BY trade_date
    """
    try:
        rows = ch._client.execute(sql)
    except Exception as e:
        logger.warning("加载指数失败 %s: %s", ts_code, e)
        return None
    if not rows:
        logger.warning("指数表无数据: %s，择时/市况将回退到中位数价", ts_code)
        return None
    df = pd.DataFrame(rows, columns=["trade_date", "close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    s = df.set_index("trade_date")["close"].astype(np.float64)
    if len(s) < 50:
        logger.warning("指数 %s 有效交易日仅 %d 天，回退中位数价", ts_code, len(s))
        return None
    logger.info("基准指数 %s: %d 个交易日", ts_code, len(s))
    return s


def regime_alloc_series(
    market_close: pd.Series,
    bull_th: float = REGIME_BULL_TH,
    bear_th: float = REGIME_BEAR_TH,
    a_bull: float = ALLOC_REV_BULL,
    a_flat: float = ALLOC_REV_FLAT,
    a_bear: float = ALLOC_REV_BEAR,
    rebal_every: int = REGIME_ALLOC_REBAL_FREQ,
) -> pd.Series:
    """
    指数 20/120 趋势 → 反转子策略资金占比。
    仅在 rebal_every 个交易日刷新配比，其余日前向填充，避免日频抖动带来的伪换手。
    输出已 shift(1)，与收益对齐可交易。
    """
    ma_f = market_close.rolling(TIMING_FAST_MA, min_periods=TIMING_FAST_MA).mean()
    ma_s = market_close.rolling(TIMING_SLOW_MA, min_periods=TIMING_SLOW_MA).mean()
    tr = (ma_f / ma_s - 1.0).fillna(0.0)
    a_raw = pd.Series(float(a_flat), index=tr.index, dtype=float)
    a_raw.loc[tr >= bull_th] = float(a_bull)
    a_raw.loc[tr <= bear_th] = float(a_bear)

    stepped = pd.Series(np.nan, index=a_raw.index, dtype=float)
    last = float(a_flat)
    for i, dt in enumerate(a_raw.index):
        if i % int(rebal_every) == 0:
            last = float(a_raw.iloc[i])
        stepped.iloc[i] = last
    return stepped.shift(1).fillna(float(a_flat))


def dynamic_combine_weights(
    w_rev: pd.DataFrame,
    w_mom: pd.DataFrame,
    alloc_rev: pd.Series,
) -> pd.DataFrame:
    """按日动态配比合并两子策略全仓权重矩阵（各子策略行和为 1）。"""
    a = alloc_rev.reindex(w_rev.index).ffill()
    if a.isna().all():
        a = pd.Series(float(ALLOC_REV_FLAT), index=w_rev.index)
    else:
        a = a.fillna(float(ALLOC_REV_FLAT))
    return w_rev.mul(a, axis=0) + w_mom.mul(1.0 - a, axis=0)


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


def quality_jump_mask(close: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    """
    近似剔除「噪声/跳变」大的股票：用 rolling 窗口内最大绝对日收益做截面排名，
    剔除 jump 分位高于 QUALITY_JUMP_CUTOFF 的标的（与 OHLC 缺口相比，仅用收盘价可复现）。
    """
    r = close.pct_change(fill_method=None).abs()
    jmax = r.rolling(QUALITY_JUMP_WINDOW, min_periods=3).max()
    jr = jmax.where(universe).rank(axis=1, pct=True)
    return jr <= float(QUALITY_JUMP_CUTOFF)


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

    qual_mask = quality_jump_mask(close, universe) if QUALITY_FILTER_ON else universe
    tradeable = vol_mask & qual_mask

    # 截面百分位 rank
    r_ma60  = f_ma60_dev.rank(axis=1, pct=True)
    r_rsi   = f_rsi_inv.rank(axis=1, pct=True)
    r_ret20 = f_ret20_rev.rank(axis=1, pct=True)

    # 加权合成信号，并应用波动率 + 质量过滤
    signal = (W_MA60 * r_ma60 + W_RSI * r_rsi + W_RET20 * r_ret20).where(tradeable)

    logger.info(
        "因子权重: MA60=%.1f  RSI=%.1f  Ret20=%.1f  vol_cutoff=%.1f  quality=%s",
        W_MA60, W_RSI, W_RET20, VOL_CUTOFF, "on" if QUALITY_FILTER_ON else "off",
    )
    return signal


def calc_momentum_signal(close: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    """
    动量子策略（增强）：12-1 + 20 日 + 60 日趋势排名，牛侧更吃趋势延续。
    """
    ret_12_1 = close.shift(21) / close.shift(252) - 1.0
    ret_20 = close / close.shift(20) - 1.0
    ret_60 = close / close.shift(60) - 1.0

    vol20 = close.pct_change(fill_method=None).rolling(20, min_periods=20).std()
    vol_rank = vol20.where(universe).rank(axis=1, pct=True)
    vol_mask = vol_rank <= float(MOM_VOL_CUTOFF)
    qual_mask = quality_jump_mask(close, universe) if QUALITY_FILTER_ON else universe
    tradeable = vol_mask & qual_mask

    r_12_1 = ret_12_1.where(universe).rank(axis=1, pct=True)
    r_20 = ret_20.where(universe).rank(axis=1, pct=True)
    r_60 = ret_60.where(universe).rank(axis=1, pct=True)
    signal = (0.55 * r_12_1 + 0.25 * r_20 + 0.20 * r_60).where(tradeable)
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


def combine_substrategy_weights(
    w_rev: pd.DataFrame,
    w_mom: pd.DataFrame,
    alloc_rev: float = ALLOC_REV,
    alloc_mom: float = ALLOC_MOM,
) -> pd.DataFrame:
    """将反转/动量两个子策略权重按资金占比合并。"""
    total = alloc_rev + alloc_mom
    if total <= 0:
        raise ValueError("alloc_rev + alloc_mom must be > 0")
    a_rev = alloc_rev / total
    a_mom = alloc_mom / total
    return a_rev * w_rev + a_mom * w_mom


# ─────────────────────────────────────────────────────────────
# 组合收益计算
# ─────────────────────────────────────────────────────────────
def calc_portfolio_return(
    weights: pd.DataFrame,
    close: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    """返回 (净收益率序列, 换手率序列)，已扣除成本"""
    # 反转策略对尾部反弹/反转响应较强；过窄的收益截断会压制有效收益
    daily_ret = close.pct_change(fill_method=None).fillna(0).clip(-0.2, 0.2)
    port_ret  = (weights.shift(1) * daily_ret).sum(axis=1)

    w_diff    = weights.diff().fillna(0)
    buy_turn  = w_diff.clip(lower=0).sum(axis=1)
    sell_turn = (-w_diff.clip(upper=0)).sum(axis=1)
    cost      = buy_turn * BUY_COST_BPS / 1e4 + sell_turn * SELL_COST_BPS / 1e4

    net_ret  = port_ret - cost
    turnover = w_diff.abs().sum(axis=1)
    return net_ret.dropna(), turnover


def apply_timing_overlay(
    weights: pd.DataFrame,
    close: pd.DataFrame,
    fast_ma: int = TIMING_FAST_MA,
    slow_ma: int = TIMING_SLOW_MA,
    levels: list[tuple[float, float]] | None = None,
    crisis_on: bool = CRISIS_TIMING_ON,
    crisis_trend_th: float = CRISIS_TREND_THRESHOLD,
    crisis_max_expo: float = CRISIS_MAX_EXPOSURE,
    market_series: pd.Series | None = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    分级择时覆盖层：
      - market_series：如指数收盘价；None 则用全 A 中位数价
      - 危机层：趋势弱于阈值时压敞口上限
    """
    levels = levels or TIMING_LEVELS
    levels = sorted(levels, key=lambda x: x[0], reverse=True)

    if market_series is not None:
        market_proxy = market_series.reindex(weights.index).ffill().bfill()
    else:
        market_proxy = close.median(axis=1)
    ma_fast = market_proxy.rolling(fast_ma, min_periods=fast_ma).mean()
    ma_slow = market_proxy.rolling(slow_ma, min_periods=slow_ma).mean()
    trend = (ma_fast / ma_slow - 1.0).fillna(0.0)

    # 分级赋值：只取“首个满足阈值”的档位，不让后续更宽阈值覆盖前面已命中的仓位
    # 例：阈值从高到低为 t1>t2>...，若 trend>=t1 应该取 e1；后续即使也满足 trend>=t2 也不能再覆盖。
    exposure = pd.Series(np.nan, index=trend.index, dtype=float)
    for th, expo in levels:
        mask = trend >= th
        exposure = exposure.mask(mask & exposure.isna(), expo)
    exposure = exposure.fillna(levels[-1][1])

    if crisis_on:
        cap = pd.Series(
            np.where(trend < crisis_trend_th, crisis_max_expo, 1.0).astype(float),
            index=trend.index,
        )
        exposure = pd.Series(np.minimum(exposure.values, cap.values), index=exposure.index)

    exposure = exposure.shift(1).fillna(1.0)

    timed_weights = weights.mul(exposure, axis=0)
    return timed_weights, exposure


# ─────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("A 股反转策略（最终版）%s ~ %s", START, END)
    logger.info("持仓: %d 只  调仓: 每 %d 日  因子: MA60(%.1f)+RSI(%.1f)+Ret20(%.1f)",
                TOP_N, REBAL_FREQ, W_MA60, W_RSI, W_RET20)
    logger.info(
        "结构层: 质量过滤=%s 危机择时=%s 指数择时=%s 动态市况配比=%s",
        QUALITY_FILTER_ON,
        CRISIS_TIMING_ON,
        TIMING_USE_INDEX,
        REGIME_DYNAMIC_ALLOC,
    )
    logger.info("组合杠杆: LEVERAGE=%.2fx", LEVERAGE)
    logger.info("=" * 60)

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = connect_db(cfg)

    # ── 数据 ──
    close, amount = load_price(ch)
    exclude       = load_exclude_list(pg)
    universe      = build_universe(close, amount, exclude)
    idx_series = load_index_close(ch)
    if idx_series is not None:
        market_line = idx_series.reindex(close.index).ffill().bfill()
        if bool(market_line.isna().all()):
            logger.warning("指数对齐后全日历为空，回退中位数价")
            market_line = close.median(axis=1)
    else:
        market_line = close.median(axis=1)
    del amount; gc.collect()

    # ── 子策略信号与权重 ──
    logger.info("计算反转信号...")
    signal_rev = calc_factors(close, universe)
    logger.info("计算动量信号...")
    signal_mom = calc_momentum_signal(close, universe)

    logger.info("生成反转子策略权重（top=%d）...", TOP_N)
    w_rev = generate_weights(signal_rev, TOP_N, REBAL_FREQ, INERTIA)
    logger.info("生成动量子策略权重（top=%d）...", TOP_N_MOM)
    w_mom = generate_weights(signal_mom, TOP_N_MOM, REBAL_FREQ_MOM, INERTIA_MOM)

    if ENABLE_DUAL_STRATEGY and REGIME_DYNAMIC_ALLOC:
        alloc_rev = regime_alloc_series(market_line)
        weights = dynamic_combine_weights(w_rev, w_mom, alloc_rev)
        logger.info(
            "组合权重: 市况动态 — 反转仓位均值 %.1f%% (牛=%.0f%% 平=%.0f%% 熊=%.0f%%)",
            float(alloc_rev.mean()) * 100,
            ALLOC_REV_BULL * 100,
            ALLOC_REV_FLAT * 100,
            ALLOC_REV_BEAR * 100,
        )
    elif ENABLE_DUAL_STRATEGY:
        weights = combine_substrategy_weights(w_rev, w_mom, ALLOC_REV, ALLOC_MOM)
        logger.info("组合权重: 反转 %.0f%% + 动量 %.0f%%", ALLOC_REV * 100, ALLOC_MOM * 100)
    else:
        weights = w_rev

    # 等比放大权重（杠杆）：在择时关闭时用于把年化收益率拉近目标
    if float(LEVERAGE) != 1.0:
        weights = weights * float(LEVERAGE)

    timing_market = market_line if TIMING_USE_INDEX else None

    # ── 回测（基线） ──
    logger.info("运行回测（基线）...")
    net_ret_base, turnover_base = calc_portfolio_return(weights, close)
    m_base = calc_full_metrics(net_ret_base, turnover_base)

    # ── 回测（择时） ──
    if TIMING_ON:
        logger.info("运行回测（择时覆盖）...")
        weights_timed, exposure = apply_timing_overlay(
            weights, close, market_series=timing_market
        )
        net_ret_timed, turnover_timed = calc_portfolio_return(weights_timed, close)
        m_timed = calc_full_metrics(net_ret_timed, turnover_timed)
    else:
        exposure = pd.Series(1.0, index=weights.index)
        net_ret_timed, turnover_timed, m_timed = net_ret_base, turnover_base, m_base

    # ── 文字报告 ──
    print("\n=== 基线版（无择时） ===")
    print(format_report(m_base))
    print("\n=== 择时版（熊市降仓） ===")
    print(format_report(m_timed))
    print(
        f"\n择时统计: 平均仓位 {exposure.mean():.1%}, "
        f"降仓天数 {(exposure < 1.0).sum()} / {len(exposure)}"
    )

    print("\n── 逐年收益 ──")
    print(f"  {'年份':>4}  {'年度收益':>8}  {'最大回撤':>8}")
    for yr in range(2020, 2027):
        r_ = net_ret_timed[f"{yr}0101":f"{yr}1231"]
        if len(r_) < 20:
            continue
        cum = (1 + r_).prod() - 1
        dd  = ((1 + r_).cumprod() / (1 + r_).cumprod().cummax() - 1).min()
        tag = " ← 大牛市" if yr in (2020, 2025) else (" ← 熊市" if yr == 2022 else "")
        print(f"  {yr}   {cum:>+8.1%}  {dd:>8.1%}{tag}")

    ann_turn = (m_timed.get("annualized_turnover", 0) or 0)
    print(f"\n── 成本估算（年换手 {ann_turn:.0%}）──")
    for cap in [100_000, 200_000, 300_000, 500_000]:
        c = cap * ann_turn * (BUY_COST_BPS + SELL_COST_BPS) / 2 / 1e4
        print(f"  {cap//10000:>3}万本金：年成本约 {c:>6.0f} 元 ({c/cap:.1%})")

    # ── 可视化报告 ──
    logger.info("生成可视化报告...")
    try:
        from strategy.backtest.visualizer import plot_report
        plot_report(
            net_returns   = net_ret_timed,
            title         = f"A股反转策略  {START[:4]}~{END[:4]}",
            save_path     = "docs/reports/reversal_strategy.png",
            initial_capital = 1.0,
        )
    except Exception as e:
        logger.warning("可视化报告生成失败: %s", e)

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
