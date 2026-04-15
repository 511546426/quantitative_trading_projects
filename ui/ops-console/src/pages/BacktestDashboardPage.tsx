import { LineChartOutlined } from "@ant-design/icons";
import {
  Button,
  Card,
  Col,
  DatePicker,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Row,
  Select,
  Table,
  Typography,
  message,
} from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import { ColorType, LineSeries, createChart, type Time } from "lightweight-charts";
import { useEffect, useRef, useState } from "react";
import client from "../api/client";

type EquityPoint = {
  time: string;
  strategy_equity: number;
  benchmark_equity: number;
  stock_buyhold_equity: number;
};

type QuickBacktestResp = {
  ts_code: string;
  benchmark_ts_code: string;
  start: string;
  end: string;
  strategy: string;
  approx_position_changes: number;
  equity: EquityPoint[];
  metrics_strategy: Record<string, number | string>;
  metrics_benchmark: { total_return: number; max_drawdown: number };
};

function pct4(x: number | string | undefined): string {
  if (x === undefined || x === null) return "—";
  const n = typeof x === "string" ? parseFloat(x) : x;
  if (Number.isNaN(n)) return "—";
  return `${(n * 100).toFixed(2)}%`;
}

export default function BacktestDashboardPage() {
  const [form] = Form.useForm();
  const [range, setRange] = useState<[Dayjs, Dayjs]>([
    dayjs().subtract(365, "day"),
    dayjs(),
  ]);
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<QuickBacktestResp | null>(null);
  const chartRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = chartRef.current;
    if (!el || !data?.equity?.length) return;
    const chart = createChart(el, {
      layout: {
        background: { type: ColorType.Solid, color: "#0c1017" },
        textColor: "#8b9cb3",
      },
      grid: { vertLines: { color: "#1e2836" }, horzLines: { color: "#1e2836" } },
      width: el.clientWidth,
      height: 380,
      rightPriceScale: { borderColor: "#1e2836" },
      timeScale: { borderColor: "#1e2836" },
    });
    const s1 = chart.addSeries(LineSeries, { color: "#2f6feb", lineWidth: 2 });
    const s2 = chart.addSeries(LineSeries, { color: "#f5a623", lineWidth: 1 });
    const s3 = chart.addSeries(LineSeries, { color: "#78909c", lineWidth: 1, lineStyle: 2 });
    s1.setData(data.equity.map((r) => ({ time: r.time as Time, value: r.strategy_equity })));
    s2.setData(data.equity.map((r) => ({ time: r.time as Time, value: r.benchmark_equity })));
    s3.setData(data.equity.map((r) => ({ time: r.time as Time, value: r.stock_buyhold_equity })));
    const ro = new ResizeObserver(() => chart.applyOptions({ width: el.clientWidth }));
    ro.observe(el);
    return () => {
      ro.disconnect();
      chart.remove();
    };
  }, [data]);

  async function onFinish(v: {
    ts_code: string;
    strategy: "ma_cross" | "buy_hold";
    fast_ma: number;
    slow_ma: number;
    benchmark_ts_code?: string;
  }) {
    const start = range[0].format("YYYYMMDD");
    const end = range[1].format("YYYYMMDD");
    setLoading(true);
    setData(null);
    try {
      const { data: d } = await client.post<QuickBacktestResp>("/api/dashboard/quick-backtest", {
        ts_code: (v.ts_code ?? "").trim().toUpperCase(),
        start,
        end,
        strategy: v.strategy,
        fast_ma: v.fast_ma,
        slow_ma: v.slow_ma,
        benchmark_ts_code: (v.benchmark_ts_code ?? "000300.SH").trim() || "000300.SH",
      });
      setData(d);
      message.success("回测完成");
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      message.error(err.response?.data?.detail ?? "请求失败");
    } finally {
      setLoading(false);
    }
  }

  const m = data?.metrics_strategy;

  const metricRows = m
    ? [
        { k: "区间总收益", v: pct4(m.total_return as number) },
        { k: "年化收益", v: pct4(m.annualized_return as number) },
        { k: "年化波动", v: pct4(m.annualized_volatility as number) },
        { k: "夏普比率", v: String(m.sharpe_ratio ?? "—") },
        { k: "最大回撤", v: pct4(m.max_drawdown as number) },
        { k: "卡玛比率", v: String(m.calmar_ratio ?? "—") },
        { k: "Sortino", v: String(m.sortino_ratio ?? "—") },
        { k: "胜率（日收益>0）", v: pct4(m.win_rate as number) },
        { k: "盈亏比", v: String(m.profit_loss_ratio ?? "—") },
        { k: "交易日数", v: String(m.n_trading_days ?? "—") },
      ]
    : [];

  return (
    <div>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        <LineChartOutlined style={{ marginRight: 8 }} />
        策略回测看板
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        基于日线 <Typography.Text code>stock_daily</Typography.Text> 与指数{" "}
        <Typography.Text code>index_daily</Typography.Text>（默认沪深300）。策略为简易双均线或买入持有；绩效指标由{" "}
        <Typography.Text code>strategy/backtest/metrics.py</Typography.Text> 计算（含胜率、夏普等）。
      </Typography.Paragraph>

      <Card bordered={false} style={{ marginBottom: 16 }}>
        <Form
          form={form}
          layout="vertical"
          onFinish={onFinish}
          initialValues={{
            ts_code: "601318.SH",
            strategy: "ma_cross",
            fast_ma: 5,
            slow_ma: 20,
            benchmark_ts_code: "000300.SH",
          }}
        >
          <Row gutter={16}>
            <Col xs={24} md={8}>
              <Form.Item name="ts_code" label="标的代码" rules={[{ required: true }]}>
                <Input placeholder="如 601318.SH" />
              </Form.Item>
            </Col>
            <Col xs={24} md={16}>
              <Typography.Text type="secondary">回测区间</Typography.Text>
              <div style={{ marginTop: 6 }}>
                <DatePicker.RangePicker
                  style={{ width: "100%" }}
                  value={range}
                  onChange={(vals) => {
                    if (vals?.[0] && vals[1]) setRange([vals[0], vals[1]]);
                  }}
                />
              </div>
            </Col>
            <Col xs={24} md={6}>
              <Form.Item name="strategy" label="策略">
                <Select
                  options={[
                    { value: "ma_cross", label: "双均线" },
                    { value: "buy_hold", label: "买入持有" },
                  ]}
                />
              </Form.Item>
            </Col>
            <Col xs={12} md={4}>
              <Form.Item name="fast_ma" label="快线">
                <InputNumber min={2} max={120} style={{ width: "100%" }} />
              </Form.Item>
            </Col>
            <Col xs={12} md={4}>
              <Form.Item name="slow_ma" label="慢线">
                <InputNumber min={3} max={250} style={{ width: "100%" }} />
              </Form.Item>
            </Col>
            <Col xs={24} md={8}>
              <Form.Item name="benchmark_ts_code" label="基准指数 ts_code">
                <Input placeholder="000300.SH" />
              </Form.Item>
            </Col>
            <Col xs={24} md={6}>
              <Form.Item label=" ">
                <Button type="primary" htmlType="submit" loading={loading} block>
                  运行回测
                </Button>
              </Form.Item>
            </Col>
          </Row>
        </Form>
      </Card>

      {data && (
        <>
          <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }} style={{ marginBottom: 16 }}>
            <Descriptions.Item label="标的">{data.ts_code}</Descriptions.Item>
            <Descriptions.Item label="基准">{data.benchmark_ts_code}</Descriptions.Item>
            <Descriptions.Item label="区间">
              {data.start} — {data.end}
            </Descriptions.Item>
            <Descriptions.Item label="基准总收益">
              {pct4(data.metrics_benchmark.total_return)}
            </Descriptions.Item>
            <Descriptions.Item label="基准最大回撤">
              {pct4(data.metrics_benchmark.max_drawdown)}
            </Descriptions.Item>
            <Descriptions.Item label="近似换仓次数">{data.approx_position_changes}</Descriptions.Item>
          </Descriptions>

          <Card title="策略绩效摘要" style={{ marginBottom: 16 }} bordered={false}>
            <Table
              size="small"
              pagination={false}
              rowKey="k"
              columns={[
                { title: "指标", dataIndex: "k", width: 180 },
                { title: "数值", dataIndex: "v" },
              ]}
              dataSource={metricRows}
            />
          </Card>

          <Card title="净值曲线：蓝=策略 · 橙=基准指数 · 灰虚线=标的买入持有" bordered={false}>
            <div ref={chartRef} style={{ width: "100%", minHeight: 380 }} />
          </Card>
        </>
      )}
    </div>
  );
}
