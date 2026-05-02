import { Button, Card, Col, DatePicker, Descriptions, Form, Input, InputNumber, Row, Select, Skeleton, Table, Tabs, Typography, message } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import { LineSeries, createChart, ColorType, type Time } from "lightweight-charts";
import { useEffect, useRef, useState } from "react";
import client from "../api/client";
import { apiErrorDetail, pct } from "../utils";

type QuickBacktestResp = {
  ts_code: string; benchmark_ts_code: string; start: string; end: string;
  strategy: string; approx_position_changes: number;
  equity: { time: string; strategy_equity: number; benchmark_equity: number; stock_buyhold_equity: number }[];
  metrics_strategy: Record<string, number | string>;
  metrics_benchmark: { total_return: number; max_drawdown: number };
};

export default function BacktestDashboardPage() {
  const [form] = Form.useForm();
  const [range, setRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(365, "day"), dayjs()]);
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<QuickBacktestResp | null>(null);
  const chartRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = chartRef.current;
    if (!el || !data?.equity?.length) return;
    const chart = createChart(el, {
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#8b9cb3" },
      grid: { vertLines: { color: "var(--border)" }, horzLines: { color: "var(--border)" } },
      width: el.clientWidth, height: 340,
      rightPriceScale: { borderColor: "var(--border)" }, timeScale: { borderColor: "var(--border)" },
    });
    chart.addSeries(LineSeries, { color: "#2f6feb", lineWidth: 2 }).setData(data.equity.map((r) => ({ time: r.time as Time, value: r.strategy_equity })));
    chart.addSeries(LineSeries, { color: "#f5a623", lineWidth: 1 }).setData(data.equity.map((r) => ({ time: r.time as Time, value: r.benchmark_equity })));
    chart.addSeries(LineSeries, { color: "#78909c", lineWidth: 1, lineStyle: 2 as const }).setData(data.equity.map((r) => ({ time: r.time as Time, value: r.stock_buyhold_equity })));
    const ro = new ResizeObserver(() => chart.applyOptions({ width: el.clientWidth }));
    ro.observe(el);
    return () => { ro.disconnect(); chart.remove(); };
  }, [data]);

  async function onFinish(v: { ts_code: string; strategy: "ma_cross" | "buy_hold"; fast_ma: number; slow_ma: number; benchmark_ts_code?: string }) {
    setLoading(true); setData(null);
    try {
      const { data: d } = await client.post<QuickBacktestResp>("/api/dashboard/quick-backtest", {
        ts_code: (v.ts_code ?? "").trim().toUpperCase(), start: range[0].format("YYYYMMDD"), end: range[1].format("YYYYMMDD"),
        strategy: v.strategy, fast_ma: v.fast_ma, slow_ma: v.slow_ma,
        benchmark_ts_code: (v.benchmark_ts_code ?? "000300.SH").trim() || "000300.SH",
      });
      setData(d); message.success("回测完成");
    } catch (e: unknown) { message.error(apiErrorDetail(e, "请求失败")); }
    finally { setLoading(false); }
  }

  const m = data?.metrics_strategy;
  const metricRows = m ? [
    { k: "区间总收益", v: pct(m.total_return as number) }, { k: "年化收益", v: pct(m.annualized_return as number) },
    { k: "年化波动", v: pct(m.annualized_volatility as number) }, { k: "夏普比率", v: String(m.sharpe_ratio ?? "—") },
    { k: "最大回撤", v: pct(m.max_drawdown as number) }, { k: "卡玛比率", v: String(m.calmar_ratio ?? "—") },
    { k: "Sortino", v: String(m.sortino_ratio ?? "—") }, { k: "胜率", v: pct(m.win_rate as number) },
    { k: "盈亏比", v: String(m.profit_loss_ratio ?? "—") }, { k: "交易日", v: String(m.n_trading_days ?? "—") },
  ] : [];

  return (
    <div>
      <Typography.Title level={4} style={{ margin: "0 0 4px", fontSize: 18 }}>策略回测看板</Typography.Title>
      <Typography.Paragraph type="secondary" style={{ margin: "0 0 16px", fontSize: 12 }}>
        基于日线双均线或买入持有策略；绩效含夏普、卡玛、Sortino 等指标。
      </Typography.Paragraph>

      <Card style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)", marginBottom: 16 }}>
        <Form form={form} layout="vertical" onFinish={onFinish}
          initialValues={{ ts_code: "601318.SH", strategy: "ma_cross", fast_ma: 5, slow_ma: 20, benchmark_ts_code: "000300.SH" }}>
          <Row gutter={12} align="bottom">
            <Col xs={24} md={6}><Form.Item name="ts_code" label="标的" rules={[{ required: true }]}><Input placeholder="600000.SH" size="small" /></Form.Item></Col>
            <Col xs={24} md={5}>
              <Typography.Text type="secondary" style={{ fontSize: 11 }}>回测区间</Typography.Text>
              <DatePicker.RangePicker size="small" style={{ width: "100%", marginTop: 2 }} value={range}
                onChange={(vals) => { if (vals?.[0] && vals[1]) setRange([vals[0], vals[1]]); }} />
            </Col>
            <Col xs={12} md={3}><Form.Item name="strategy" label="策略"><Select size="small" options={[{ value: "ma_cross", label: "双均线" }, { value: "buy_hold", label: "买入持有" }]} /></Form.Item></Col>
            <Col xs={8} md={2}><Form.Item name="fast_ma" label="快线"><InputNumber min={2} max={120} size="small" style={{ width: "100%" }} /></Form.Item></Col>
            <Col xs={8} md={2}><Form.Item name="slow_ma" label="慢线"><InputNumber min={3} max={250} size="small" style={{ width: "100%" }} /></Form.Item></Col>
            <Col xs={24} md={6}><Form.Item label=" "><Button type="primary" htmlType="submit" loading={loading} block size="small">运行</Button></Form.Item></Col>
          </Row>
        </Form>
      </Card>

      {loading ? (
        <Card style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)" }}>
          <Skeleton active paragraph={{ rows: 4 }} />
        </Card>
      ) : data ? (
        <Card style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)" }}>
          <Descriptions size="small" column={{ xs: 1, sm: 2, md: 4 }} style={{ marginBottom: 12 }}>
            <Descriptions.Item label="标的">{data.ts_code}</Descriptions.Item>
            <Descriptions.Item label="基准">{data.benchmark_ts_code}</Descriptions.Item>
            <Descriptions.Item label="区间">{data.start} — {data.end}</Descriptions.Item>
            <Descriptions.Item label="换仓次数">{data.approx_position_changes}</Descriptions.Item>
          </Descriptions>
          <Tabs size="small" items={[
            { key: "metrics", label: "绩效指标", children: <Table size="small" pagination={false} rowKey="k" columns={[{ title: "指标", dataIndex: "k", width: 160 }, { title: "数值", dataIndex: "v" }]} dataSource={metricRows} /> },
            { key: "chart", label: "净值曲线", children: <div ref={chartRef} style={{ width: "100%", minHeight: 340, marginTop: 8 }} /> },
          ]} />
        </Card>
      ) : null}
    </div>
  );
}
