"""A 股多因子策略 v4 — 反转 + 价值 + 动量 + 盈利

改进点（vs 上一版）:
  1. 修复 generate_weights 权重堆积致命 bug
  2. 换手率优化: REBAL_FREQ 63, INERTIA 0.30, RET60 替换 RET20
  3. 去除大市值限制（MAX_MV）: 覆盖蓝筹大牛年（2017/2020/2024）
  4. 去除市场择时叠加: 反转策略在熊末反弹最强，择时反而截断收益
  5. 新增 MOM120 正向动量因子: 捕捉"底部复苏+中期趋势"的甜蜜区

因子权重（合计=1.00）:
  MA60 0.25  RSI 0.07  RET60 0.07  PB 0.16  SIZE 0.08  EP 0.12  MOM120 0.25
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
logger = logging.getLogger("multifactor_v4")


# ═══════════════════════════════════════════════════════════
# 参数
# ═══════════════════════════════════════════════════════════
START = "20100104"
END   = "20260320"

# ── 因子权重（总和=1.0） ──
W_MA60   = 0.25   # 均值回复
W_RSI    = 0.07   # RSI 超卖
W_RET60  = 0.07   # 60日反转
W_PB     = 0.16   # P/B 低估值
W_SIZE   = 0.08   # 小市值倾斜
W_EP     = 0.12   # 盈利收益率 = 1/PE
W_MOM120 = 0.25   # 120日正向动量

# ── 股票池 ──
MIN_AMOUNT   = 100_000     # 日均成交额 (千元) = 1亿
# MAX_MV 不设上限: 覆盖大盘牛市年（2017/2020/2024）
FALLEN_KNIFE = 0.30        # 52周最高价 30% 以下排除
VOL_CUTOFF   = 0.70        # 波动率截面分位过滤（剔除最高 30% 波动股）

# ── 组合 ──
TOP_N      = 30
REBAL_FREQ = 63             # 季度调仓，降换手
INERTIA    = 0.30           # 更强惯性，持续减少不必要换仓
LEVERAGE   = 2.00           # 基线

# ── 组合层止损（对抗 2011/2018 类全年熊市）──
STOP_LOSS    = 0.15         # 从近期高点回撤 15% → 清仓（杠杆高，门槛相应收紧）
STOP_COOLDOWN = 63          # 最长清仓等待天数

# ── 成本 ──
BUY_COST_BPS  = 7.5
SELL_COST_BPS = 17.5

BENCHMARK = "000300.SH"


# ═══════════════════════════════════════════════════════════
# 数据加载（直接复用已验证的加载逻辑）
# ═══════════════════════════════════════════════════════════
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
    """按年分块加载复权收盘价和成交额（与反转策略完全相同的实现）。"""
    sy, ey = int(START[:4]), int(END[:4])
    c_close: list[pd.DataFrame] = []
    c_amount: list[pd.DataFrame] = []

    for year in range(sy, ey + 1):
        ys, ye = f"{year}-01-01", f"{year}-12-31"
        sql = f"""
            SELECT ts_code, trade_date,
                   argMax(adj_close, trade_date) AS adj_close,
                   argMax(amount,    trade_date) AS amount
            FROM stock_daily
            WHERE trade_date >= '{ys}' AND trade_date <= '{ye}'
              AND is_suspended = 0
            GROUP BY ts_code, trade_date
        """
        rows = ch._client.execute(sql)
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_close", "amount"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df[["adj_close", "amount"]] = df[["adj_close", "amount"]].astype(np.float32)
        c_close.append(df.pivot(index="trade_date", columns="ts_code", values="adj_close"))
        c_amount.append(df.pivot(index="trade_date", columns="ts_code", values="amount"))
        del df; gc.collect()
        logger.info("  行情: %d 年", year)

    close  = pd.concat(c_close,  axis=0, sort=True).sort_index(); del c_close;  gc.collect()
    amount = pd.concat(c_amount, axis=0, sort=True).sort_index(); del c_amount; gc.collect()
    logger.info("行情: %d 交易日 × %d 只", *close.shape)
    return close, amount


def load_valuation(pg) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """按年分块加载 PB、PE_TTM、流通市值（与反转策略风格一致）。"""
    sy, ey = int(START[:4]), int(END[:4])
    c_pb: list[pd.DataFrame] = []
    c_pe: list[pd.DataFrame] = []
    c_mv: list[pd.DataFrame] = []

    for year in range(sy, ey + 1):
        ys, ye = f"{year}-01-01", f"{year}-12-31"
        rows = pg.execute_query(
            "SELECT ts_code, trade_date, pb, pe_ttm, circ_mv "
            "FROM daily_valuation WHERE trade_date >= %s AND trade_date <= %s",
            (ys, ye),
        )
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "pb", "pe_ttm", "circ_mv"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        for c in ("pb", "pe_ttm", "circ_mv"):
            df[c] = df[c].astype(np.float32)
        c_pb.append(df.pivot(index="trade_date", columns="ts_code", values="pb"))
        c_pe.append(df.pivot(index="trade_date", columns="ts_code", values="pe_ttm"))
        c_mv.append(df.pivot(index="trade_date", columns="ts_code", values="circ_mv"))
        del df; gc.collect()

    pb = pd.concat(c_pb, axis=0, sort=True).sort_index(); del c_pb; gc.collect()
    pe = pd.concat(c_pe, axis=0, sort=True).sort_index(); del c_pe; gc.collect()
    mv = pd.concat(c_mv, axis=0, sort=True).sort_index(); del c_mv; gc.collect()
    logger.info("估值 (PB+PE+MV): %d 交易日 × %d 只", *pb.shape)
    return pb, pe, mv


def load_exclude_list(pg) -> set:
    rows = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
    )
    return {r[0] for r in rows}


def load_index_close(ch, ts_code: str = BENCHMARK) -> pd.Series | None:
    sql = f"""
        SELECT trade_date, max(close) AS close
        FROM index_daily
        WHERE ts_code = '{ts_code}'
          AND trade_date >= '{START}' AND trade_date <= '{END}'
        GROUP BY trade_date ORDER BY trade_date
    """
    try:
        rows = ch._client.execute(sql)
    except Exception as e:
        logger.warning("指数加载失败: %s", e)
        return None
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["trade_date", "close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    s = df.set_index("trade_date")["close"].astype(np.float64)
    logger.info("基准 %s: %d 交易日", ts_code, len(s))
    return s if len(s) >= 50 else None


# ═══════════════════════════════════════════════════════════
# 股票池（与反转策略逻辑相同，添加市值上限）
# ═══════════════════════════════════════════════════════════
def build_universe(
    close: pd.DataFrame,
    amount: pd.DataFrame,
    exclude: set,
    circ_mv: pd.DataFrame | None = None,
) -> pd.DataFrame:
    mask = close.notna()

    for code in exclude:
        if code in mask.columns:
            mask[code] = False

    mask = mask & (close.notna().cumsum() >= 60)

    avg_amt = amount.rolling(20, min_periods=10).mean()
    mask = mask & (avg_amt >= MIN_AMOUNT)
    del avg_amt; gc.collect()

    h52 = close.rolling(252, min_periods=60).max()
    mask = mask & ((close / h52) >= FALLEN_KNIFE)
    del h52; gc.collect()

    # 不设市值上限：允许大盘股进入，由 SIZE 因子自然倾斜
    # circ_mv 只用于 SIZE 因子计算，不做过滤

    logger.info("股票池: 日均 %.0f 只", mask.sum(axis=1).mean())
    return mask


# ═══════════════════════════════════════════════════════════
# 因子计算（使用与反转策略完全相同的方式：直接 pandas 运算）
# ═══════════════════════════════════════════════════════════
def _rsi(close: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss = (-delta).clip(lower=0).ewm(span=period, adjust=False).mean()
    return 100 - 100 / (1 + gain / loss.replace(0, np.nan))


def apply_portfolio_stop(
    net_ret: pd.Series,
    index_close: pd.Series | None = None,
    stop_loss: float = STOP_LOSS,
    min_cash_days: int = 10,
    max_cash_days: int = STOP_COOLDOWN,
) -> pd.Series:
    """
    组合层止损保护：回撤超 stop_loss → 清仓。
    重新入场条件（满足任一即可）:
      - 已清仓 max_cash_days 天（强制重入）
      - 已清仓至少 min_cash_days 天 且 CSI300 同时 > 20d MA AND > 60d MA

    双均线重入优势:
      - 2018/2011 真熊市: 双均线长期压制 → 较长空仓 → 避免反复止损循环
      - 2024/09 暴涨: 爆发式大涨足以同时穿越双均线 → 快速重入
    """
    if index_close is not None:
        ic   = index_close.reindex(net_ret.index).ffill()
        p_ic = ic.shift(1)
        ma20_r = ic.rolling(20, min_periods=10).mean().shift(1)
        ma60_r = ic.rolling(60, min_periods=30).mean().shift(1)
        ok = ((p_ic > ma20_r) & (p_ic > ma60_r)).fillna(True)
    else:
        ok = pd.Series(True, index=net_ret.index)

    adjusted = net_ret.copy()
    nav   = 1.0
    peak  = 1.0
    in_cash = False
    days_in_cash = 0

    for i in range(len(net_ret)):
        if in_cash:
            adjusted.iloc[i] = 0.0
            days_in_cash += 1
            force_reenter  = days_in_cash >= max_cash_days
            market_reenter = (days_in_cash >= min_cash_days) and bool(ok.iloc[i])
            if force_reenter or market_reenter:
                in_cash = False
                days_in_cash = 0
                peak = nav
        else:
            nav = nav * (1.0 + adjusted.iloc[i])
            if nav > peak:
                peak = nav
            if nav / peak - 1 < -stop_loss:
                in_cash = True
                adjusted.iloc[i] = 0.0
                days_in_cash = 1

    total_cash_days = int((adjusted == 0.0).sum())
    logger.info(
        "止损保护: 清仓 %d 天 (占 %.1f%%)，止损=%.0f%%，最短等待=%d天，最长=%d天，重入=ma20+ma60",
        total_cash_days,
        total_cash_days / len(adjusted) * 100,
        stop_loss * 100,
        min_cash_days,
        max_cash_days,
    )

    return adjusted


def calc_signal(
    close: pd.DataFrame,
    universe: pd.DataFrame,
    pb: pd.DataFrame | None = None,
    pe_ttm: pd.DataFrame | None = None,
    circ_mv: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    六因子复合信号（直接 pandas 运算，无闭包，无 .values 副作用风险）。

    方向：rank 值越高 → 买入意愿越强。
    """
    idx, cols = close.index, close.columns
    w_total = 0.0
    signal = pd.DataFrame(0.0, index=idx, columns=cols)

    # ── F1: MA60 偏离均值回复 ──
    ma60 = close.rolling(60, min_periods=40).mean()
    r1 = (-close / ma60).where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + W_MA60 * r1
    w_total += W_MA60
    del ma60, r1; gc.collect()
    logger.info("  F1 MA60   W=%.2f", W_MA60)

    # ── F2: RSI 超卖 ──
    rsi = _rsi(close)
    r2 = (-rsi).where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + W_RSI * r2
    w_total += W_RSI
    del rsi, r2; gc.collect()
    logger.info("  F2 RSI    W=%.2f", W_RSI)

    # ── F3: 60日中期反转（换手更低，信号更稳定）──
    ret60 = close / close.shift(60) - 1
    r3 = (-ret60).where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + W_RET60 * r3
    w_total += W_RET60
    del ret60, r3; gc.collect()
    logger.info("  F3 RET60  W=%.2f", W_RET60)

    # ── F7: 120日正向动量（复苏趋势，与短期反转形成 V 形选股）──
    mom120 = close / close.shift(120) - 1
    r7 = mom120.where(universe).rank(axis=1, pct=True).fillna(0.0)   # 高动量 → 高分
    signal = signal + W_MOM120 * r7
    w_total += W_MOM120
    del mom120, r7; gc.collect()
    logger.info("  F7 MOM120 W=%.2f", W_MOM120)

    # ── F4: P/B 低估值 ──
    if pb is not None:
        pb_al = pb.reindex(index=idx, columns=cols).ffill()
        pb_pos = pb_al.where(pb_al > 0.1)
        r4 = (-pb_pos).where(universe).rank(axis=1, pct=True).fillna(0.0)
        signal = signal + W_PB * r4
        w_total += W_PB
        del pb_al, pb_pos, r4; gc.collect()
        logger.info("  F4 PB     W=%.2f", W_PB)

    # ── F5: SIZE 小市值（log 反转，市值越小 rank 越高）──
    if circ_mv is not None:
        mv = circ_mv.reindex(index=idx, columns=cols).ffill()
        mv_pos = mv.where(mv > 0)
        r5 = (-np.log1p(mv_pos)).where(universe).rank(axis=1, pct=True).fillna(0.0)
        signal = signal + W_SIZE * r5
        w_total += W_SIZE
        del mv, mv_pos, r5; gc.collect()
        logger.info("  F5 SIZE   W=%.2f", W_SIZE)

    # ── F6: EP 盈利收益率 = 1/PE_TTM（盈利且 PE 合理的股票）──
    if pe_ttm is not None:
        pe_al = pe_ttm.reindex(index=idx, columns=cols).ffill()
        pe_pos = pe_al.where(pe_al > 1.0)   # 排除亏损股及 PE < 1 的异常值
        ep = 1.0 / pe_pos
        r6 = ep.where(universe).rank(axis=1, pct=True).fillna(0.0)
        signal = signal + W_EP * r6
        w_total += W_EP
        del pe_al, pe_pos, ep, r6; gc.collect()
        logger.info("  F6 EP     W=%.2f", W_EP)

    # 归一化（如果部分因子数据缺失，权重之和可能 < 1.0）
    if w_total > 0:
        signal = signal / w_total

    # 波动率过滤：剔除高波动股（与反转策略相同）
    pct_chg = close.pct_change(fill_method=None)
    vol20 = pct_chg.rolling(20, min_periods=20).std()
    del pct_chg; gc.collect()
    vol_mask = vol20.where(universe).rank(axis=1, pct=True) <= VOL_CUTOFF
    del vol20; gc.collect()

    signal = signal.where(vol_mask & universe)

    pct = signal.notna().mean().mean() * 100
    logger.info("复合信号就绪: 有效率 %.0f%% (因子权重合计 %.2f)", pct, w_total)
    return signal


# ═══════════════════════════════════════════════════════════
# 持仓权重（与反转策略完全相同的实现）
# ═══════════════════════════════════════════════════════════
def generate_weights(
    signal: pd.DataFrame,
    top_n: int = TOP_N,
    rebal_freq: int = REBAL_FREQ,
    inertia: float = INERTIA,
) -> pd.DataFrame:
    weights = pd.DataFrame(np.nan, index=signal.index, columns=signal.columns)
    prev_held: set = set()

    for i, dt in enumerate(signal.index):
        if i % rebal_freq != 0:
            continue
        s = signal.loc[dt].dropna().copy()
        if len(s) < top_n:
            continue
        for c in prev_held:
            if c in s.index:
                s[c] += inertia
        top = s.nlargest(top_n)
        weights.loc[dt, :] = 0.0                      # 先全部清零，防止旧持仓 ffill 堆积
        weights.loc[dt, top.index] = 1.0 / len(top)
        prev_held = set(top.index)

    weights = weights.ffill().fillna(0)
    return weights


# ═══════════════════════════════════════════════════════════
# 组合收益（与反转策略完全相同，简洁可靠）
# ═══════════════════════════════════════════════════════════
def calc_portfolio_return(
    weights: pd.DataFrame,
    close: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    """净收益 = port_ret - cost。不加 vol 靶向，结果可验证。"""
    daily_ret = close.pct_change(fill_method=None).fillna(0).clip(-0.2, 0.2)
    port_ret = (weights.shift(1) * daily_ret).sum(axis=1)

    w_diff = weights.diff().fillna(0)
    buy_t  = w_diff.clip(lower=0).sum(axis=1)
    sell_t = (-w_diff.clip(upper=0)).sum(axis=1)
    cost = buy_t * BUY_COST_BPS / 1e4 + sell_t * SELL_COST_BPS / 1e4
    turnover = w_diff.abs().sum(axis=1)

    net_ret = port_ret - cost
    return net_ret.dropna(), turnover


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════
def main():
    logger.info("=" * 60)
    logger.info("A 股多因子策略 v4  %s ~ %s", START, END)
    logger.info(
        "因子: MA60=%.2f RSI=%.2f RET60=%.2f PB=%.2f SIZE=%.2f EP=%.2f MOM120=%.2f",
        W_MA60, W_RSI, W_RET60, W_PB, W_SIZE, W_EP, W_MOM120,
    )
    logger.info("组合: TOP=%d  调仓=%dd  惯性=%.2f  杠杆=%.2fx  止损=%.0f%%", TOP_N, REBAL_FREQ, INERTIA, LEVERAGE, STOP_LOSS * 100)
    logger.info("=" * 60)

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = connect_db(cfg)

    close, amount = load_price(ch)
    exclude = load_exclude_list(pg)

    logger.info("加载估值 (PB + PE + 市值)...")
    try:
        pb, pe_ttm, circ_mv = load_valuation(pg)
    except Exception as e:
        logger.warning("估值加载失败: %s", e)
        pb, pe_ttm, circ_mv = None, None, None

    universe = build_universe(close, amount, exclude, circ_mv=circ_mv)
    del amount; gc.collect()

    # 裁剪：剔除从未入池的列
    active = universe.any(axis=0)
    if active.sum() < universe.shape[1]:
        nb = universe.shape[1]
        close    = close.loc[:, active]
        universe = universe.loc[:, active]
        if pb     is not None: pb     = pb.reindex(columns=close.columns)
        if pe_ttm is not None: pe_ttm = pe_ttm.reindex(columns=close.columns)
        if circ_mv is not None: circ_mv = circ_mv.reindex(columns=close.columns)
        logger.info("裁剪: %d → %d 只", nb, active.sum())
    del active; gc.collect()

    # ── 因子信号 ──
    logger.info("计算六因子复合信号...")
    signal = calc_signal(close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv)
    del pb, pe_ttm, circ_mv, universe; gc.collect()

    # ── 权重 ──
    logger.info("生成权重 (TOP=%d, 调仓=%dd)...", TOP_N, REBAL_FREQ)
    weights = generate_weights(signal)
    del signal; gc.collect()

    if float(LEVERAGE) != 1.0:
        weights = weights * float(LEVERAGE)

    # ── 回测 ──
    logger.info("运行回测...")
    net_ret, turnover = calc_portfolio_return(weights, close)

    # 合理性检查
    net_min = float(net_ret.min())
    if net_min < -1.0:
        logger.error("检测到不可能的日收益 %.4f，请检查数据！", net_min)

    # ── 组合层止损（市场条件触发重新入场，捕捉暴涨反弹）──
    index_close = load_index_close(ch)
    net_ret = apply_portfolio_stop(net_ret, index_close=index_close)

    metrics = calc_full_metrics(net_ret, turnover)

    # ── 报告 ──
    print("\n" + "=" * 60)
    print("  A 股多因子策略 v4 回测结果")
    print("=" * 60)
    print(format_report(metrics))

    def _yr(nr: pd.Series) -> dict[int, float]:
        return {
            int(y): float((1 + nr[nr.index.year == y]).prod() - 1)
            for y in sorted(set(nr.index.year))
            if len(nr[nr.index.year == y]) >= 5
        }

    def _dd(nr: pd.Series) -> float:
        nav = (1 + nr).cumprod()
        return float((nav / nav.cummax() - 1).min())

    print(f"\n  {'年份':>4}  {'年度收益':>10}  {'年内回撤':>10}")
    print("  " + "-" * 34)
    yearly = _yr(net_ret)
    for yr, yr_ret in yearly.items():
        r_ = net_ret[net_ret.index.year == yr]
        dd = _dd(r_)
        print(f"  {yr}   {yr_ret:>+10.1%}  {dd:>10.1%}")

    total = float((1 + net_ret).prod() - 1)
    yr0 = min(yearly.keys()) if yearly else int(START[:4])
    yr1 = max(yearly.keys()) if yearly else int(END[:4])
    print(f"\n总收益 ({yr0}~{yr1}): {total:+.1%}")

    ann_turn = metrics.get("annualized_turnover", 0) or 0
    print(f"\n成本估算（年换手 {ann_turn:.0%}）:")
    for cap in (100_000, 200_000, 300_000, 500_000):
        c = cap * ann_turn * (BUY_COST_BPS + SELL_COST_BPS) / 2 / 1e4
        print(f"   {cap // 10000}万: 年成本 {c:>6.0f} 元 ({c / cap:.1%})")

    logger.info("生成报告...")
    try:
        from strategy.backtest.visualizer import plot_report
        plot_report(
            net_ret,
            title=f"A股多因子策略 v4  {START[:4]}~{END[:4]}",
            save_path="docs/reports/multifactor_v4.png",
        )
    except Exception as e:
        logger.warning("可视化失败: %s", e)

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
