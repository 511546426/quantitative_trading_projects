import { Card, Descriptions, Empty, Table, Tabs, Typography } from "antd";
import {
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  LineSeries,
  createChart,
  type Time,
} from "lightweight-charts";
import { useEffect, useRef } from "react";
import type { RegimeRun, YearlyReturnRow } from "../../researchRunStore";
import { cny, pct, pctTurnover } from "../../utils";

function fmtYmd(s: string): string {
  if (s.length === 8) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  return s;
}

interface Props {
  result: RegimeRun;
}

export default function ResearchCharts({ result }: Props) {
  const equityRef = useRef<HTMLDivElement | null>(null);
  const candleRef = useRef<HTMLDivElement | null>(null);
  const weightRef = useRef<HTMLDivElement | null>(null);

  if (!result?.series?.length) {
    return (
      <Card style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)" }}>
        <Empty description="暂无回测数据" />
      </Card>
    );
  }

  const m = result.metrics_portfolio;
  const pool = result.run_scope === "pool" || !result.ts_code;

  return (
    <div>
      {/* 绩效摘要卡片 */}
      <div style={{ display: "flex", gap: 10, marginBottom: 14, flexWrap: "wrap" }}>
        <Kv label="年化收益" value={pct(m.annualized_return as number)} color="var(--green)" />
        <Kv label="夏普比率" value={String(m.sharpe_ratio ?? "—")} />
        <Kv label="最大回撤" value={pct(m.max_drawdown as number)} color="var(--red)" />
        <Kv label="总收益" value={pct(m.total_return as number)} />
        <Kv label="年化换手" value={m.annualized_turnover != null ? pctTurnover(m.annualized_turnover as number) : "—"} />
        <Kv label="交易日" value={String(m.n_trading_days ?? "—")} color="var(--text-secondary)" />
        <Kv label="模型" value={result.model} color="var(--text-secondary)" />
        {result.date_start && result.date_end && (
          <Kv label="回测区间" value={`${fmtYmd(result.date_start)}–${fmtYmd(result.date_end)}`} color="var(--text-secondary)" />
        )}
      </div>

      {/* Tab 结果 */}
      <Tabs
        defaultActiveKey="equity"
        size="small"
        style={{ background: "var(--bg-container)", borderRadius: "var(--radius-lg)", padding: "0 12px" }}
        items={[
          { key: "equity", label: "净值曲线", children: <EquityTab result={result} equityRef={equityRef} weightRef={weightRef} pool={pool} /> },
          { key: "yearly", label: "分年收益", children: <YearlyTab yearly={result.yearly_returns} /> },
          ...(pool || !result.bars?.length ? [] : [{ key: "kline", label: "K 线", children: <KlineTab bars={result.bars} candleRef={candleRef} /> }]),
        ]}
      />
    </div>
  );
}

function Kv({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ minWidth: 100, padding: "10px 14px", background: "var(--bg-elevated)", borderRadius: "var(--radius)", border: "1px solid var(--border)" }}>
      <div style={{ fontSize: 10, color: "var(--text-secondary)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 }}>{label}</div>
      <div style={{ fontSize: 16, fontWeight: 600, color: color ?? "var(--text-primary)" }}>{value}</div>
    </div>
  );
}

/* ── Tab: 净值曲线 ── */
function EquityTab({ result, equityRef, weightRef, pool }: { result: RegimeRun; equityRef: React.MutableRefObject<HTMLDivElement | null>; weightRef: React.MutableRefObject<HTMLDivElement | null>; pool: boolean }) {
  useEffect(() => {
    const el2 = equityRef.current;
    const el3 = weightRef.current;
    if (!el2) return;

    const chartOptions = {
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#8b9cb3" },
      grid: { vertLines: { color: "var(--border)" }, horzLines: { color: "var(--border)" } },
      rightPriceScale: { borderColor: "var(--border)" },
      timeScale: { borderColor: "var(--border)" },
    };

    const eqChart = createChart(el2, { ...chartOptions, width: el2.clientWidth, height: 280 });
    const yuanAxis = Boolean(result.initial_capital && result.initial_capital > 0);
    const priceFormat = yuanAxis ? { priceFormat: { type: "custom" as const, formatter: (p: number) => `¥${p.toLocaleString("zh-CN", { maximumFractionDigits: 0 })}` } } : {};
    eqChart.addSeries(LineSeries, { color: "#2f6feb", lineWidth: 2, ...priceFormat })
      .setData(result.series.map((r) => ({ time: r.time as Time, value: r.portfolio_equity })));
    eqChart.addSeries(LineSeries, { color: "#78909c", lineWidth: 1, ...priceFormat })
      .setData(result.series.map((r) => ({ time: r.time as Time, value: r.stock_benchmark_equity })));

    let wChart: ReturnType<typeof createChart> | null = null;
    if (!pool && el3) {
      wChart = createChart(el3, { ...chartOptions, width: el3.clientWidth, height: 100 });
      wChart.addSeries(HistogramSeries, { color: "#5c6bc0", priceFormat: { type: "price", precision: 2, minMove: 0.01 } })
        .setData(result.series.map((r) => ({ time: r.time as Time, value: r.model_weight * 100, color: r.model_weight > 0 ? "#5c6bc0" : "#37474f" })));
    }

    const ro = new ResizeObserver(() => { eqChart.applyOptions({ width: el2.clientWidth }); if (wChart && el3) wChart.applyOptions({ width: el3.clientWidth }); });
    ro.observe(el2); if (wChart && el3) ro.observe(el3);
    return () => { ro.disconnect(); eqChart.remove(); wChart?.remove(); };
  }, [result, equityRef, weightRef, pool]);

  return (
    <div style={{ padding: "12px 0" }}>
      {result.initial_capital != null && result.initial_capital > 0 ? (
        <Descriptions size="small" column={3} style={{ marginBottom: 12 }}>
          <Descriptions.Item label="初始本金">{cny(result.initial_capital)}</Descriptions.Item>
          <Descriptions.Item label="期末净值">{cny(lastSnapshot(result)?.portfolio_equity)}</Descriptions.Item>
          <Descriptions.Item label="期末基准">{cny(lastSnapshot(result)?.stock_benchmark_equity)}</Descriptions.Item>
        </Descriptions>
      ) : null}
      <div ref={equityRef} style={{ width: "100%", minHeight: 280 }} />
      {!pool && result.ts_code ? (
        <>
          <Typography.Text type="secondary" style={{ fontSize: 11, display: "block", margin: "8px 0 4px" }}>该标的日度权重（%，名义杠杆后）</Typography.Text>
          <div ref={weightRef} style={{ width: "100%", minHeight: 100 }} />
        </>
      ) : <div ref={weightRef} style={{ display: "none" }} />}
    </div>
  );
}

function lastSnapshot(result: RegimeRun) {
  return result.series[result.series.length - 1] ?? { portfolio_equity: 0, stock_benchmark_equity: 0 };
}

/* ── Tab: 分年收益 ── */
function YearlyTab({ yearly }: { yearly: YearlyReturnRow[] | undefined }) {
  if (!yearly?.length) return <Empty description="暂无分年数据" style={{ padding: 40 }} />;
  return (
    <Table<YearlyReturnRow> size="small" rowKey="year" pagination={false} dataSource={yearly}
      style={{ marginTop: 8 }}
      columns={[
        { title: "年份", dataIndex: "year", width: 80 },
        { title: "年收益率", dataIndex: "net_return", render: (v: number) => <span style={{ color: v >= 0 ? "var(--green)" : "var(--red)" }}>{pct(v)}</span> },
        { title: "最大回撤", dataIndex: "max_drawdown", width: 100, render: (v: number | null | undefined) => pct(v) },
        { title: "年化换手", dataIndex: "annualized_turnover", width: 100, render: (v: number | null | undefined) => v != null ? pctTurnover(v) : "—" },
        { title: "交易日", dataIndex: "trading_days", width: 80 },
      ]}
    />
  );
}

/* ── Tab: K 线 ── */
function KlineTab({ bars, candleRef }: { bars: { time: string; open: number; high: number; low: number; close: number }[]; candleRef: React.MutableRefObject<HTMLDivElement | null> }) {
  useEffect(() => {
    const el = candleRef.current;
    if (!el || !bars?.length) return;
    const chart = createChart(el, {
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#8b9cb3" },
      grid: { vertLines: { color: "var(--border)" }, horzLines: { color: "var(--border)" } },
      width: el.clientWidth, height: 320,
      rightPriceScale: { borderColor: "var(--border)" },
      timeScale: { borderColor: "var(--border)" },
    });
    chart.addSeries(CandlestickSeries, { upColor: "#26a69a", downColor: "#ef5350", borderVisible: false, wickUpColor: "#26a69a", wickDownColor: "#ef5350" })
      .setData(bars.map((b) => ({ time: b.time as Time, open: b.open, high: b.high, low: b.low, close: b.close })));
    const ro = new ResizeObserver(() => chart.applyOptions({ width: el.clientWidth }));
    ro.observe(el);
    return () => { ro.disconnect(); chart.remove(); };
  }, [bars, candleRef]);

  if (!bars?.length) return <Empty description="无 K 线数据" style={{ padding: 40 }} />;
  return <div ref={candleRef} style={{ width: "100%", minHeight: 320, marginTop: 8 }} />;
}
