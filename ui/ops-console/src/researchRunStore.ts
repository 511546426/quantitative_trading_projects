/**
 * Long-running research HTTP calls live here so switching React routes does not
 * lose in-flight work or results (same tab; server also shields heavy threads).
 */

import { message } from "antd";
import { useSyncExternalStore } from "react";
import client from "./api/client";

export type BarRow = {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  adj_close: number;
};

export type RegimeSeriesRow = {
  time: string;
  portfolio_equity: number;
  stock_benchmark_equity: number;
  model_weight: number;
};

/** 多因子组合回测页（/research）仅使用 v4.1 管线结果 */
export type YearlyReturnRow = {
  year: number;
  net_return: number;
  trading_days: number;
  /** 年内净值最大回撤（负值），与脚本分年「年内回撤」一致 */
  max_drawdown?: number | null;
  /** 当年日度换手均值×252，与全样本「年化换手」定义一致 */
  annualized_turnover?: number | null;
};

export type RegimeRun = {
  mode: "regime";
  ts_code: string | null;
  /** stock=带对照票；pool=仅全市场组合，灰线为 CSI300 买入持有 */
  run_scope?: "pool" | "stock";
  /** 传入 initial_capital 时为 cash_lots（整手现金），否则为 fractional（理想权重+杠杆） */
  backtest_mode?: "cash_lots" | "fractional";
  benchmark_label?: string;
  /** 后端实际使用的回测起止日 YYYYMMDD */
  date_start?: string;
  date_end?: string;
  name: string;
  model: string;
  bars: BarRow[];
  series: RegimeSeriesRow[];
  metrics_portfolio: Record<string, number | string | null | undefined>;
  /** 自然年 × 扣费后净日收益复利年收益 */
  yearly_returns?: YearlyReturnRow[];
  /** 若请求传入 initial_capital，服务端将净值锚定到该本金（元）并回显 */
  initial_capital?: number;
};

export type ResearchRunSnapshot = {
  loading: boolean;
  error: string | null;
  result: RegimeRun | null;
};

const listeners = new Set<() => void>();

let snapshot: ResearchRunSnapshot = {
  loading: false,
  error: null,
  result: null,
};

function emit() {
  listeners.forEach((fn) => fn());
}

export function subscribeResearchRun(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getResearchRunSnapshot(): ResearchRunSnapshot {
  return snapshot;
}

export function useResearchRun(): ResearchRunSnapshot {
  return useSyncExternalStore(subscribeResearchRun, getResearchRunSnapshot, getResearchRunSnapshot);
}

function onPathResearch(): boolean {
  return window.location.pathname === "/research" || window.location.pathname.endsWith("/research");
}

export async function startRegimeRun(
  ts_code: string | null | undefined,
  start: string,
  end: string,
  initial_capital?: number | null,
): Promise<void> {
  if (snapshot.loading) {
    message.warning("已有回测任务在进行中，请稍候");
    return;
  }
  snapshot = { loading: true, error: null, result: null };
  emit();
  try {
    const body: Record<string, string | number> = { start, end };
    const ts = ts_code?.trim();
    if (ts) {
      body.ts_code = ts.toUpperCase();
    }
    if (initial_capital != null && initial_capital > 0) {
      body.initial_capital = initial_capital;
    }
    const { data } = await client.post<Omit<RegimeRun, "mode">>(
      "/api/research/regime-model-run",
      body,
      { timeout: 600_000 },
    );
    snapshot = { loading: false, error: null, result: { ...data, mode: "regime" } };
    emit();
    if (onPathResearch()) {
      message.success("多因子管线回测完成（全市场加载，可能较慢）");
    } else {
      message.success({
        content: "多因子组合回测已完成，请切回「多因子组合回测」查看图表。",
        duration: 8,
      });
    }
  } catch (e: unknown) {
    const err = e as { response?: { data?: { detail?: string } } };
    const detail = err.response?.data?.detail ?? "请求失败";
    snapshot = { loading: false, error: detail, result: null };
    emit();
    message.error(detail);
  }
}

export function clearResearchRun(): void {
  snapshot = { loading: false, error: null, result: null };
  emit();
}
