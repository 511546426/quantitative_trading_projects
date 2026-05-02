"""
A 股多因子策略 — 因子信号计算模块

多因子截面打分 + CSI300 趋势牛调节 + 波动率过滤。
"""
from __future__ import annotations

import gc
import logging
from typing import Any

import numpy as np
import pandas as pd

from strategy.config import (
    W_MA60, W_RSI, W_RET60, W_PB, W_SIZE, W_EP, W_MOM120,
    MIN_AMOUNT, FALLEN_KNIFE, VOL_CUTOFF,
)

logger = logging.getLogger(__name__)


def _rsi(close: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """计算 RSI 指标。"""
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


def build_universe(
    close: pd.DataFrame,
    amount: pd.DataFrame,
    exclude: set,
    circ_mv: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """构建股票池：排除ST/退市/次新股/低成交/过度深跌/高波动。"""
    mask = close.notna()

    for code in exclude:
        if code in mask.columns:
            mask[code] = False

    # 至少 60 日上市
    mask = mask & (close.notna().cumsum() >= 60)

    # 日均成交额 >= MIN_AMOUNT
    avg_amt = amount.rolling(20, min_periods=10).mean()
    mask = mask & (avg_amt >= MIN_AMOUNT)
    del avg_amt
    gc.collect()

    # 不低于 52 周最高价的 FALLEN_KNIFE
    h52 = close.rolling(252, min_periods=60).max()
    mask = mask & ((close / h52) >= FALLEN_KNIFE)
    del h52
    gc.collect()

    logger.info("股票池: 日均 %.0f 只", mask.sum(axis=1).mean())
    return mask


def calc_signal(
    close: pd.DataFrame,
    universe: pd.DataFrame,
    pb: pd.DataFrame | None = None,
    pe_ttm: pd.DataFrame | None = None,
    circ_mv: pd.DataFrame | None = None,
    regime_bull: pd.Series | None = None,
) -> pd.DataFrame:
    """
    七因子复合信号。

    方向：rank 值越高 → 买入意愿越强。
    regime_bull: 牛市略向动量倾斜。
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

    # F1: MA60 偏离均值回复
    ma60 = close.rolling(60, min_periods=40).mean()
    r1 = (-close / ma60).where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + wm60 * r1
    w_total += float(W_MA60)
    del ma60, r1
    gc.collect()
    logger.info("  F1 MA60   W=%.2f", W_MA60)

    # F2: RSI 超卖
    rsi = _rsi(close)
    r2 = (-rsi).where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + W_RSI * r2
    w_total += W_RSI
    del rsi, r2
    gc.collect()
    logger.info("  F2 RSI    W=%.2f", W_RSI)

    # F3: 60日中期反转
    ret60 = close / close.shift(60) - 1
    r3 = (-ret60).where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + wret * r3
    w_total += float(W_RET60)
    del ret60, r3
    gc.collect()
    logger.info("  F3 RET60  W=%.2f", W_RET60)

    # F7: 120日正向动量
    mom120 = close / close.shift(120) - 1
    r7 = mom120.where(universe).rank(axis=1, pct=True).fillna(0.0)
    signal = signal + wmom * r7
    w_total += float(W_MOM120)
    del mom120, r7
    gc.collect()
    logger.info("  F7 MOM120 W=%.2f", W_MOM120)

    # F4: P/B 低估值
    if pb is not None:
        pb_al = pb.reindex(index=idx, columns=cols).ffill()
        pb_pos = pb_al.where(pb_al > 0.1)
        r4 = (-pb_pos).where(universe).rank(axis=1, pct=True).fillna(0.0)
        signal = signal + W_PB * r4
        w_total += W_PB
        del pb_al, pb_pos, r4
        gc.collect()
        logger.info("  F4 PB     W=%.2f", W_PB)

    # F5: SIZE 小市值
    if circ_mv is not None:
        mv = circ_mv.reindex(index=idx, columns=cols).ffill()
        mv_pos = mv.where(mv > 0)
        r5 = (-np.log1p(mv_pos)).where(universe).rank(axis=1, pct=True).fillna(0.0)
        signal = signal + W_SIZE * r5
        w_total += W_SIZE
        del mv, mv_pos, r5
        gc.collect()
        logger.info("  F5 SIZE   W=%.2f", W_SIZE)

    # F6: EP 盈利收益率 = 1/PE_TTM
    if pe_ttm is not None:
        pe_al = pe_ttm.reindex(index=idx, columns=cols).ffill()
        pe_pos = pe_al.where(pe_al > 1.0)
        ep = 1.0 / pe_pos
        r6 = ep.where(universe).rank(axis=1, pct=True).fillna(0.0)
        signal = signal + W_EP * r6
        w_total += W_EP
        del pe_al, pe_pos, ep, r6
        gc.collect()
        logger.info("  F6 EP     W=%.2f", W_EP)

    # 按行归一化
    w_eff_sum = wm60 + W_RSI + wret + wmom
    if pb is not None:
        w_eff_sum = w_eff_sum + W_PB
    if circ_mv is not None:
        w_eff_sum = w_eff_sum + W_SIZE
    if pe_ttm is not None:
        w_eff_sum = w_eff_sum + W_EP
    signal = signal / w_eff_sum

    # 波动率过滤
    pct_chg = close.pct_change(fill_method=None)
    vol20 = pct_chg.rolling(20, min_periods=20).std()
    del pct_chg
    gc.collect()
    vol_mask = vol20.where(universe).rank(axis=1, pct=True) <= VOL_CUTOFF
    del vol20
    gc.collect()

    signal = signal.where(vol_mask & universe)

    pct = signal.notna().mean().mean() * 100
    logger.info(
        "复合信号就绪: 有效率 %.0f%% (基准因子权重合计 %.2f，牛市动态加权已按行归一)",
        pct,
        w_total,
    )
    return signal
