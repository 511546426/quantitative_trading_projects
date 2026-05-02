import axios from "axios";

/** 数字/字符串 → 百分比，null/undefined/NaN → "—" */
export function pct(n: number | string | null | undefined): string {
  if (n === null || n === undefined) return "—";
  const x = typeof n === "string" ? parseFloat(n) : n;
  if (Number.isNaN(x)) return "—";
  return `${(x * 100).toFixed(2)}%`;
}

/** 数字 → 人民币整数格式 */
export function cny(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return `¥${n.toLocaleString("zh-CN", { maximumFractionDigits: 0 })}`;
}

/** 数字 → 换手率百分比（小数转百分数，保留 1 位） */
export function pctTurnover(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

/** 统一解析 Axios 错误体中的 detail 字段 */
export function apiErrorDetail(e: unknown, fallback = "请求失败"): string {
  if (axios.isAxiosError(e) && e.response?.data) {
    const d = e.response.data as Record<string, unknown>;
    if (typeof d.detail === "string") return d.detail;
  }
  if (e instanceof Error) return e.message;
  return fallback;
}
