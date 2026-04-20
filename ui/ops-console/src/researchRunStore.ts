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

/** 单股研究页仅使用多因子 v4.1 管线结果 */
export type RegimeRun = {
  mode: "regime";
  ts_code: string;
  name: string;
  model: string;
  bars: BarRow[];
  series: RegimeSeriesRow[];
  metrics_portfolio: Record<string, number | string | null | undefined>;
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

export async function startRegimeRun(ts_code: string, start: string, end: string): Promise<void> {
  if (snapshot.loading) {
    message.warning("已有回测任务在进行中，请稍候");
    return;
  }
  snapshot = { loading: true, error: null, result: null };
  emit();
  try {
    const { data } = await client.post<Omit<RegimeRun, "mode">>(
      "/api/research/regime-model-run",
      { ts_code, start, end },
      { timeout: 600_000 },
    );
    snapshot = { loading: false, error: null, result: { ...data, mode: "regime" } };
    emit();
    if (onPathResearch()) {
      message.success("多因子管线回测完成（全市场加载，可能较慢）");
    } else {
      message.success({
        content: "单股多因子回测已完成，请切回「单股研究」查看图表。",
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
