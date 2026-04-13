"""A 股多因子策略 v4 — 反转 + 价值 + 动量 + 盈利

改进点（vs 上一版）:
  1. 修复 generate_weights 权重堆积致命 bug
  2. 换手率优化: REBAL_FREQ 63, INERTIA 0.30, RET60 替换 RET20
  3. 去除大市值限制（MAX_MV）: 覆盖蓝筹大牛年（2017/2020/2024）
  4. 去除市场择时叠加: 反转策略在熊末反弹最强，择时反而截断收益
  5. 新增 MOM120 正向动量因子: 捕捉"底部复苏+中期趋势"的甜蜜区

v4.1 策略层（目标：全样本年化显著高于无风险利率、牛市年份更激进）:
  - CSI300 趋势牛识别（无前瞻）: 昨收>昨MA60 且 昨MA20>昨MA60
  - 牛市：有效杠杆 × REGIME_LEV_MULT，组合止损阈值放宽为 STOP_LOSS_BULL
  - 牛市：因子权重向 MOM120 倾斜、压低 RET60/MA60（截面信号仍经 rank）
  注：回测非承诺收益；10 万级本金请配合 regime_switching_lot_20k 看整手路径。

因子基准权重（合计=1.00）:
  MA60 0.20  RSI 0.07  RET60 0.05  PB 0.16  SIZE 0.08  EP 0.12  MOM120 0.32
"""
import gc
import sys
import logging
from pathlib import Path
from typing import Any

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

# ── 因子权重（总和=1.0，牛市内在 calc_signal 中动态向动量倾斜） ──
W_MA60   = 0.20   # 均值回复
W_RSI    = 0.07   # RSI 超卖
W_RET60  = 0.05   # 60日反转
W_PB     = 0.16   # P/B 低估值
W_SIZE   = 0.08   # 小市值倾斜
W_EP     = 0.12   # 盈利收益率 = 1/PE
W_MOM120 = 0.32   # 120日正向动量

# ── 股票池 ──
MIN_AMOUNT   = 100_000     # 日均成交额 (千元) = 1亿
# MAX_MV 不设上限: 覆盖大盘牛市年（2017/2020/2024）
FALLEN_KNIFE = 0.30        # 52周最高价 30% 以下排除
VOL_CUTOFF   = 0.70        # 波动率截面分位过滤（剔除最高 30% 波动股）

# ── 组合 ──
TOP_N      = 30
REBAL_FREQ = 50             # 约 2.5 个月调仓，略增趋势年响应（换手上升）
INERTIA    = 0.24           # 略减惯性，便于跟上排名
LEVERAGE   = 2.22           # 基线名义杠杆

# ── 趋势牛增厚（CSI300 regime_bull_exante）──
REGIME_LEV_MULT = 1.14      # 牛市再乘（峰值约 2.22×1.14）

# ── 组合层止损（对抗 2011/2018；牛市放宽以减少趋势中震出）──
STOP_LOSS      = 0.17       # 非牛市
STOP_LOSS_BULL = 0.27       # 牛市
STOP_COOLDOWN  = 63         # 最长清仓等待天数

# ── 成本 ──
BUY_COST_BPS  = 7.5
SELL_COST_BPS = 17.5

BENCHMARK = "000300.SH"


def lot_effective_top_n(
    initial_cash_yuan: float,
    *,
    max_names: int = TOP_N,
    min_names: int = 5,
    min_lot_assumed_yuan: float = 4200.0,
) -> int:
    """
    整手回测建议持仓只数：在峰值名义杠杆下，按「每只至少一手」的粗略预算上限。
    信号仍为全市场同一套；仅持仓宽度随本金缩小，避免 2 万本金硬摊 30 只导致严重欠配。
    """
    lev_peak = float(LEVERAGE) * float(REGIME_LEV_MULT)
    budget = max(float(initial_cash_yuan), 1.0) * lev_peak
    n = int(budget / float(min_lot_assumed_yuan))
    return int(max(min_names, min(int(max_names), n)))


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


def _ymd_to_ts(s: str) -> pd.Timestamp:
    """YYYYMMDD → Timestamp（日频对齐）。"""
    return pd.Timestamp(f"{s[:4]}-{s[4:6]}-{s[6:8]}")


def _slice_panel(df: pd.DataFrame, date_start: str, date_end: str) -> pd.DataFrame:
    t0, t1 = _ymd_to_ts(date_start), _ymd_to_ts(date_end)
    return df.loc[t0:t1]


def load_price(
    ch,
    date_start: str | None = None,
    date_end: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """按年分块加载复权收盘价和成交额（与反转策略完全相同的实现）。"""
    ds = date_start or START
    de = date_end or END
    sy, ey = int(ds[:4]), int(de[:4])
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
    close = _slice_panel(close, ds, de)
    amount = _slice_panel(amount, ds, de)
    logger.info("行情: %d 交易日 × %d 只", *close.shape)
    return close, amount


def load_valuation(
    pg,
    date_start: str | None = None,
    date_end: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """按年分块加载 PB、PE_TTM、流通市值（与反转策略风格一致）。"""
    ds = date_start or START
    de = date_end or END
    sy, ey = int(ds[:4]), int(de[:4])
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
    pb = _slice_panel(pb, ds, de)
    pe = _slice_panel(pe, ds, de)
    mv = _slice_panel(mv, ds, de)
    logger.info("估值 (PB+PE+MV): %d 交易日 × %d 只", *pb.shape)
    return pb, pe, mv


def load_exclude_list(pg) -> set:
    rows = pg.execute_query(
        "SELECT ts_code FROM stock_info WHERE is_st = TRUE OR is_delisted = TRUE"
    )
    return {r[0] for r in rows}


def load_index_close(
    ch,
    ts_code: str = BENCHMARK,
    date_start: str | None = None,
    date_end: str | None = None,
) -> pd.Series | None:
    ds = date_start or START
    de = date_end or END
    sql = f"""
        SELECT trade_date, max(close) AS close
        FROM index_daily
        WHERE ts_code = '{ts_code}'
          AND trade_date >= toDate('{ds[:4]}-{ds[4:6]}-{ds[6:8]}')
          AND trade_date <= toDate('{de[:4]}-{de[4:6]}-{de[6:8]}')
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


def regime_bull_exante(index_close: pd.Series, align_index: pd.DatetimeIndex) -> pd.Series:
    """
    趋势牛（无前瞻）：用前一交易日 CSI300 判断，用于当日杠杆/止损/因子。
    条件：昨收 > 昨MA60 且 昨MA20 > 昨MA60。
    """
    ic = index_close.reindex(align_index).ffill()
    ma20 = ic.rolling(20, min_periods=10).mean()
    ma60 = ic.rolling(60, min_periods=30).mean()
    bull = (ic.shift(1) > ma60.shift(1)) & (ma20.shift(1) > ma60.shift(1))
    return bull.fillna(False)


def apply_portfolio_stop(
    net_ret: pd.Series,
    index_close: pd.Series | None = None,
    stop_loss: float = STOP_LOSS,
    stop_loss_bull: float = STOP_LOSS_BULL,
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

    bull = regime_bull_exante(index_close, net_ret.index) if index_close is not None else None
    stop_bull = float(stop_loss_bull)

    adjusted = net_ret.copy()
    nav   = 1.0
    peak  = 1.0
    in_cash = False
    days_in_cash = 0

    for i in range(len(net_ret)):
        sl = stop_bull if bull is not None and bool(bull.iloc[i]) else float(stop_loss)
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
            if nav / peak - 1 < -sl:
                in_cash = True
                adjusted.iloc[i] = 0.0
                days_in_cash = 1

    total_cash_days = int((adjusted == 0.0).sum())
    bull_pct = float(bull.mean() * 100) if bull is not None else 0.0
    logger.info(
        "止损保护: 清仓 %d 天 (占 %.1f%%)，止损=非牛%.0f%%/牛%.0f%%，最短等待=%d天，最长=%d天，重入=ma20+ma60，趋势牛日=%.1f%%",
        total_cash_days,
        total_cash_days / len(adjusted) * 100,
        stop_loss * 100,
        stop_bull * 100,
        min_cash_days,
        max_cash_days,
        bull_pct,
    )

    return adjusted


def portfolio_stop_invested_start(
    net_ret: pd.Series,
    index_close: pd.Series | None = None,
    stop_loss: float = STOP_LOSS,
    stop_loss_bull: float = STOP_LOSS_BULL,
    min_cash_days: int = 10,
    max_cash_days: int = STOP_COOLDOWN,
) -> pd.Series:
    """
    与 apply_portfolio_stop 同一状态机：第 i 日开盘时是否应持有风险仓位。
    供 regime_switching_lot_20k 等整手回测对齐模型日历。
    """
    if index_close is not None:
        ic = index_close.reindex(net_ret.index).ffill()
        p_ic = ic.shift(1)
        ma20_r = ic.rolling(20, min_periods=10).mean().shift(1)
        ma60_r = ic.rolling(60, min_periods=30).mean().shift(1)
        ok = ((p_ic > ma20_r) & (p_ic > ma60_r)).fillna(True)
    else:
        ok = pd.Series(True, index=net_ret.index)

    bull = regime_bull_exante(index_close, net_ret.index) if index_close is not None else None
    stop_bull = float(stop_loss_bull)

    invested_start: list[bool] = []
    nav = 1.0
    peak = 1.0
    in_cash = False
    days_in_cash = 0

    for i in range(len(net_ret)):
        sl = stop_bull if bull is not None and bool(bull.iloc[i]) else float(stop_loss)
        invested_start.append(not in_cash)
        if in_cash:
            days_in_cash += 1
            force_reenter = days_in_cash >= max_cash_days
            market_reenter = (days_in_cash >= min_cash_days) and bool(ok.iloc[i])
            if force_reenter or market_reenter:
                in_cash = False
                days_in_cash = 0
                peak = nav
        else:
            nav = nav * (1.0 + float(net_ret.iloc[i]))
            if nav > peak:
                peak = nav
            if nav / peak - 1 < -sl:
                in_cash = True
                days_in_cash = 1

    return pd.Series(invested_start, index=net_ret.index, dtype=bool)


def calc_signal(
    close: pd.DataFrame,
    universe: pd.DataFrame,
    pb: pd.DataFrame | None = None,
    pe_ttm: pd.DataFrame | None = None,
    circ_mv: pd.DataFrame | None = None,
    regime_bull: pd.Series | None = None,
) -> pd.DataFrame:
    """
    六因子复合信号（直接 pandas 运算，无闭包，无 .values 副作用风险）。

    方向：rank 值越高 → 买入意愿越强。
    regime_bull: 与 regime_bull_exante 对齐的布尔序列，牛市略向动量倾斜。
    """
    idx, cols = close.index, close.columns
    if regime_bull is None:
        bf = np.zeros((len(idx), 1), dtype=np.float64)
    else:
        bf = regime_bull.reindex(idx).fillna(False).astype(float).to_numpy(dtype=np.float64)[
            :, np.newaxis
        ]

    wm60 = W_MA60 * (1.0 - 0.32 * bf)
    wret = W_RET60 * (1.0 - 0.62 * bf)
    wmom = W_MOM120 * (1.0 + 0.52 * bf)

    w_total = 0.0
    signal = pd.DataFrame(0.0, index=idx, columns=cols)

    # ── F1: MA60 偏离均值回复 ──
    ma60 = close.rolling(60, min_periods=40).mean()
    r1 = (-close / ma60).where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + wm60 * r1
    w_total += float(W_MA60)
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
    signal = signal + wret * r3
    w_total += float(W_RET60)
    del ret60, r3; gc.collect()
    logger.info("  F3 RET60  W=%.2f", W_RET60)

    # ── F7: 120日正向动量（复苏趋势，与短期反转形成 V 形选股）──
    mom120 = close / close.shift(120) - 1
    r7 = mom120.where(universe).rank(axis=1, pct=True).fillna(0.0)   # 高动量 → 高分
    signal = signal + wmom * r7
    w_total += float(W_MOM120)
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

    # 按行归一化（牛市 wm60/wret/wmom 随日变化，须用当日有效权重和）
    w_eff_sum = wm60 + W_RSI + wret + wmom
    if pb is not None:
        w_eff_sum = w_eff_sum + W_PB
    if circ_mv is not None:
        w_eff_sum = w_eff_sum + W_SIZE
    if pe_ttm is not None:
        w_eff_sum = w_eff_sum + W_EP
    signal = signal / w_eff_sum

    # 波动率过滤：剔除高波动股（与反转策略相同）
    pct_chg = close.pct_change(fill_method=None)
    vol20 = pct_chg.rolling(20, min_periods=20).std()
    del pct_chg; gc.collect()
    vol_mask = vol20.where(universe).rank(axis=1, pct=True) <= VOL_CUTOFF
    del vol20; gc.collect()

    signal = signal.where(vol_mask & universe)

    pct = signal.notna().mean().mean() * 100
    logger.info(
        "复合信号就绪: 有效率 %.0f%% (基准因子权重合计 %.2f，牛市动态加权已按行归一)",
        pct,
        w_total,
    )
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
# Web / API：同一套 v4.1 管线（可指定区间）
# ═══════════════════════════════════════════════════════════
def run_regime_model_for_web(date_start: str, date_end: str, ts_code: str) -> dict[str, Any]:
    """
    与 ``main()`` 相同的因子、TOP_N、杠杆、成本与组合止损逻辑；
    额外返回指定 ``ts_code`` 在组合中的日度权重及「该标的买入持有」基准净值，供前端与 K 线对照。

    Parameters
    ----------
    date_start, date_end
        YYYYMMDD，须在 ``START``/``END`` 与数据覆盖范围内。
    ts_code
        如 ``601318.SH``；须存在于裁剪后的行情列中。

    Raises
    ------
    ValueError
        未知标的或数据不足以回测。
    """
    ts_code = ts_code.strip().upper()
    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    ch, pg = connect_db(cfg)
    try:
        close, amount = load_price(ch, date_start, date_end)
        exclude = load_exclude_list(pg)
        try:
            pb, pe_ttm, circ_mv = load_valuation(pg, date_start, date_end)
        except Exception as e:
            logger.warning("估值加载失败: %s", e)
            pb, pe_ttm, circ_mv = None, None, None

        universe = build_universe(close, amount, exclude, circ_mv=circ_mv)
        del amount
        gc.collect()

        index_close = load_index_close(ch, BENCHMARK, date_start, date_end)

        active = universe.any(axis=0)
        if active.sum() < universe.shape[1]:
            nb = universe.shape[1]
            close = close.loc[:, active]
            universe = universe.loc[:, active]
            if pb is not None:
                pb = pb.reindex(columns=close.columns)
            if pe_ttm is not None:
                pe_ttm = pe_ttm.reindex(columns=close.columns)
            if circ_mv is not None:
                circ_mv = circ_mv.reindex(columns=close.columns)
            logger.info("裁剪: %d → %d 只", nb, int(active.sum()))
        del active
        gc.collect()

        if ts_code not in close.columns:
            raise ValueError(f"标的 {ts_code} 不在模型可交易列（可能无行情或被池过滤）")

        bull = regime_bull_exante(index_close, close.index) if index_close is not None else None
        signal = calc_signal(
            close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull
        )
        del pb, pe_ttm, circ_mv, universe
        gc.collect()

        weights = generate_weights(signal)
        del signal
        gc.collect()

        if float(LEVERAGE) != 1.0:
            lev_ser = pd.Series(float(LEVERAGE), index=weights.index)
            if index_close is not None:
                bflt = regime_bull_exante(index_close, weights.index).astype(np.float64)
                lev_ser = lev_ser * (1.0 + bflt * (float(REGIME_LEV_MULT) - 1.0))
            weights = weights.multiply(lev_ser, axis=0)

        net_ret, turnover = calc_portfolio_return(weights, close)
        net_ret = apply_portfolio_stop(net_ret, index_close=index_close)
        metrics = calc_full_metrics(net_ret, turnover)

        def _json_metrics(m: dict) -> dict[str, Any]:
            out: dict[str, Any] = {}
            for k, v in m.items():
                if isinstance(v, (np.floating, float)):
                    out[k] = float(v)
                elif isinstance(v, (np.integer, int)):
                    out[k] = int(v)
                elif isinstance(v, str):
                    out[k] = v
                elif v is None:
                    out[k] = None
                elif hasattr(v, "isoformat"):
                    out[k] = v.isoformat()
                elif hasattr(v, "item"):
                    out[k] = float(v.item())
                else:
                    out[k] = str(v)
            return out

        wcol = weights[ts_code].reindex(net_ret.index).fillna(0.0)
        port_eq = (1 + net_ret).cumprod()
        sc = close[ts_code].reindex(net_ret.index).ffill()
        st_ret = sc.pct_change(fill_method=None).fillna(0.0)
        bench_eq = (1 + st_ret).cumprod()

        series: list[dict[str, Any]] = []
        for t in net_ret.index:
            series.append(
                {
                    "time": t.strftime("%Y-%m-%d") if hasattr(t, "strftime") else str(t)[:10],
                    "portfolio_equity": float(port_eq.loc[t]),
                    "stock_benchmark_equity": float(bench_eq.loc[t]),
                    "model_weight": float(wcol.loc[t]),
                }
            )

        return {
            "model": "regime_switching_v4.1",
            "ts_code": ts_code,
            "date_start": date_start,
            "date_end": date_end,
            "metrics_portfolio": _json_metrics(metrics),
            "series": series,
        }
    finally:
        ch.close()
        pg.close()


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════
def main():
    logger.info("=" * 60)
    logger.info("A 股多因子策略 v4.1  %s ~ %s", START, END)
    logger.info(
        "因子(基线): MA60=%.2f RSI=%.2f RET60=%.2f PB=%.2f SIZE=%.2f EP=%.2f MOM120=%.2f",
        W_MA60, W_RSI, W_RET60, W_PB, W_SIZE, W_EP, W_MOM120,
    )
    logger.info(
        "组合: TOP=%d  调仓=%dd  惯性=%.2f  基线杠杆=%.2f  牛市杠杆×%.2f  止损=非牛%.0f%%/牛%.0f%%",
        TOP_N,
        REBAL_FREQ,
        INERTIA,
        LEVERAGE,
        REGIME_LEV_MULT,
        STOP_LOSS * 100,
        STOP_LOSS_BULL * 100,
    )
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

    index_close = load_index_close(ch)

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

    # ── 因子信号（CSI300 趋势牛 → 动量权重略增）──
    logger.info("计算六因子复合信号...")
    bull = regime_bull_exante(index_close, close.index) if index_close is not None else None
    signal = calc_signal(
        close, universe, pb=pb, pe_ttm=pe_ttm, circ_mv=circ_mv, regime_bull=bull
    )
    del pb, pe_ttm, circ_mv, universe; gc.collect()

    # ── 权重 ──
    logger.info("生成权重 (TOP=%d, 调仓=%dd)...", TOP_N, REBAL_FREQ)
    weights = generate_weights(signal)
    del signal; gc.collect()

    if float(LEVERAGE) != 1.0:
        lev_ser = pd.Series(float(LEVERAGE), index=weights.index)
        if index_close is not None:
            bflt = regime_bull_exante(index_close, weights.index).astype(np.float64)
            lev_ser = lev_ser * (1.0 + bflt * (float(REGIME_LEV_MULT) - 1.0))
        weights = weights.multiply(lev_ser, axis=0)

    # ── 回测 ──
    logger.info("运行回测...")
    net_ret, turnover = calc_portfolio_return(weights, close)

    # 合理性检查
    net_min = float(net_ret.min())
    if net_min < -1.0:
        logger.error("检测到不可能的日收益 %.4f，请检查数据！", net_min)

    # ── 组合层止损（市场条件触发重新入场；牛市阈值更宽）──
    net_ret = apply_portfolio_stop(net_ret, index_close=index_close)

    metrics = calc_full_metrics(net_ret, turnover)

    # ── 报告 ──
    print("\n" + "=" * 60)
    print("  A 股多因子策略 v4.1 回测结果")
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
            title=f"A股多因子策略 v4.1  {START[:4]}~{END[:4]}",
            save_path="docs/reports/multifactor_v4.png",
        )
    except Exception as e:
        logger.warning("可视化失败: %s", e)

    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
