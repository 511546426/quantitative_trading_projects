import { LineChartOutlined, SearchOutlined } from "@ant-design/icons";
import {
  Alert,
  AutoComplete,
  Button,
  Card,
  Col,
  DatePicker,
  Descriptions,
  Input,
  InputNumber,
  Row,
  Select,
  Typography,
  message,
} from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import {
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  LineSeries,
  createChart,
  type Time,
} from "lightweight-charts";
import { useCallback, useEffect, useRef, useState } from "react";
import client from "../api/client";

type BarRow = {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  adj_close: number;
};

type EquityRow = { time: string; strategy_equity: number; benchmark_equity: number };

type SimpleRun = {
  mode: "simple";
  ts_code: string;
  name: string;
  strategy: string;
  fast_ma: number;
  slow_ma: number;
  bars: BarRow[];
  equity: EquityRow[];
  metrics: Record<string, number>;
  approx_position_changes: number;
};

type RegimeSeriesRow = {
  time: string;
  portfolio_equity: number;
  stock_benchmark_equity: number;
  model_weight: number;
};

type RegimeRun = {
  mode: "regime";
  ts_code: string;
  name: string;
  model: string;
  bars: BarRow[];
  series: RegimeSeriesRow[];
  metrics_portfolio: Record<string, number | string | null | undefined>;
};

type RunState = SimpleRun | RegimeRun | null;

function pct(n: number | string | null | undefined): string {
  if (n === null || n === undefined) return "—";
  const x = typeof n === "string" ? parseFloat(n) : n;
  if (Number.isNaN(x)) return "—";
  return `${(x * 100).toFixed(2)}%`;
}

export default function StockResearchPage() {
  const [tsCode, setTsCode] = useState("601318.SH");
  const [range, setRange] = useState<[Dayjs, Dayjs]>([
    dayjs().subtract(730, "day"),
    dayjs(),
  ]);
  const [engineMode, setEngineMode] = useState<"simple" | "regime">("regime");
  const [strategy, setStrategy] = useState<"buy_hold" | "ma_cross">("ma_cross");
  const [fastMa, setFastMa] = useState(5);
  const [slowMa, setSlowMa] = useState(20);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<RunState>(null);
  const [options, setOptions] = useState<{ value: string; label: string }[]>([]);
  const searchTimer = useRef<number>(0);

  const candleRef = useRef<HTMLDivElement>(null);
  const equityRef = useRef<HTMLDivElement>(null);
  const weightRef = useRef<HTMLDivElement>(null);

  const fetchOptions = useCallback((q: string) => {
    window.clearTimeout(searchTimer.current);
    if (q.trim().length < 1) {
      setOptions([]);
      return;
    }
    searchTimer.current = window.setTimeout(async () => {
      try {
        const { data } = await client.get<{ items: { ts_code: string; name: string }[] }>(
          "/api/research/stocks",
          { params: { q: q.trim(), limit: 40 } },
        );
        setOptions(
          (data.items ?? []).map((it) => ({
            value: it.ts_code,
            label: `${it.ts_code} ${it.name}`,
          })),
        );
      } catch {
        setOptions([]);
      }
    }, 280);
  }, []);

  useEffect(() => {
    const el = candleRef.current;
    const el2 = equityRef.current;
    const el3 = weightRef.current;
    if (!el || !el2 || !result?.bars?.length) return;

    const chart = createChart(el, {
      layout: {
        background: { type: ColorType.Solid, color: "#0c1017" },
        textColor: "#8b9cb3",
      },
      grid: {
        vertLines: { color: "#1e2836" },
        horzLines: { color: "#1e2836" },
      },
      width: el.clientWidth,
      height: 360,
      rightPriceScale: { borderColor: "#1e2836" },
      timeScale: { borderColor: "#1e2836" },
    });
    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
    });
    series.setData(
      result.bars.map((b) => ({
        time: b.time as Time,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      })),
    );

    const eqChart = createChart(el2, {
      layout: {
        background: { type: ColorType.Solid, color: "#0c1017" },
        textColor: "#8b9cb3",
      },
      grid: {
        vertLines: { color: "#1e2836" },
        horzLines: { color: "#1e2836" },
      },
      width: el2.clientWidth,
      height: result.mode === "regime" ? 200 : 220,
      rightPriceScale: { borderColor: "#1e2836" },
      timeScale: { borderColor: "#1e2836" },
    });
    const sLine = eqChart.addSeries(LineSeries, { color: "#2f6feb", lineWidth: 2 });
    const bLine = eqChart.addSeries(LineSeries, { color: "#78909c", lineWidth: 1 });

    if (result.mode === "simple") {
      sLine.setData(
        result.equity.map((r) => ({ time: r.time as Time, value: r.strategy_equity })),
      );
      bLine.setData(
        result.equity.map((r) => ({ time: r.time as Time, value: r.benchmark_equity })),
      );
    } else {
      sLine.setData(
        result.series.map((r) => ({ time: r.time as Time, value: r.portfolio_equity })),
      );
      bLine.setData(
        result.series.map((r) => ({ time: r.time as Time, value: r.stock_benchmark_equity })),
      );
    }

    let wChart: ReturnType<typeof createChart> | null = null;
    if (result.mode === "regime" && el3) {
      wChart = createChart(el3, {
        layout: {
          background: { type: ColorType.Solid, color: "#0c1017" },
          textColor: "#8b9cb3",
        },
        grid: {
          vertLines: { color: "#1e2836" },
          horzLines: { color: "#1e2836" },
        },
        width: el3.clientWidth,
        height: 120,
        rightPriceScale: { borderColor: "#1e2836" },
        timeScale: { borderColor: "#1e2836" },
      });
      const hist = wChart.addSeries(HistogramSeries, {
        color: "#5c6bc0",
        priceFormat: { type: "price", precision: 2, minMove: 0.01 },
      });
      hist.setData(
        result.series.map((r) => ({
          time: r.time as Time,
          value: r.model_weight * 100,
          color: r.model_weight > 0 ? "#5c6bc0" : "#37474f",
        })),
      );
    }

    const ro = new ResizeObserver(() => {
      chart.applyOptions({ width: el.clientWidth });
      eqChart.applyOptions({ width: el2.clientWidth });
      if (wChart && el3) wChart.applyOptions({ width: el3.clientWidth });
    });
    ro.observe(el);
    ro.observe(el2);
    if (el3) ro.observe(el3);
    return () => {
      ro.disconnect();
      chart.remove();
      eqChart.remove();
      wChart?.remove();
    };
  }, [result]);

  async function run() {
    const start = range[0].format("YYYYMMDD");
    const end = range[1].format("YYYYMMDD");
    setLoading(true);
    setResult(null);
    try {
      if (engineMode === "simple") {
        const { data } = await client.post<Omit<SimpleRun, "mode">>("/api/research/single-stock-run", {
          ts_code: tsCode.trim().toUpperCase(),
          start,
          end,
          strategy,
          fast_ma: fastMa,
          slow_ma: slowMa,
        });
        setResult({ ...data, mode: "simple" });
        message.success("回测完成");
      } else {
        const { data } = await client.post<Omit<RegimeRun, "mode">>(
          "/api/research/regime-model-run",
          { ts_code: tsCode.trim().toUpperCase(), start, end },
          { timeout: 600_000 },
        );
        setResult({ ...data, mode: "regime" });
        message.success("多因子管线回测完成（全市场加载，可能较慢）");
      }
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string }; status?: number } };
      message.error(err.response?.data?.detail ?? "请求失败");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        <LineChartOutlined style={{ marginRight: 8 }} />
        单股 K 线与回测
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        数据来自 ClickHouse <Typography.Text code>stock_daily</Typography.Text>；名称搜索来自
        PostgreSQL <Typography.Text code>stock_info</Typography.Text>。
        <strong> 多因子 v4.1</strong> 与脚本{" "}
        <Typography.Text code>strategy/examples/regime_switching_strategy.py</Typography.Text>{" "}
        同一套截面因子、TOP_N、名义杠杆、成本与组合止损；下图蓝线为<strong>全组合净值</strong>，灰线为<strong>该标的买入持有</strong>，柱状为<strong>该标的在组合中的日度权重</strong>（非「只交易这一只」的独立账户）。
      </Typography.Paragraph>

      {engineMode === "regime" && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message="多因子模式会按年加载全市场行情与估值，首次计算可能占用数 GB 内存并耗时数分钟，请仅在数据已回填的机器上使用。"
        />
      )}

      <Card bordered={false} style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} lg={10}>
            <Typography.Text type="secondary">股票</Typography.Text>
            <AutoComplete
              style={{ width: "100%", marginTop: 6 }}
              options={options}
              value={tsCode}
              onSearch={fetchOptions}
              onSelect={(v) => setTsCode(v)}
              onChange={(v) => setTsCode(String(v))}
              placeholder="代码或名称搜索，如 平安 / 601318"
            >
              <Input prefix={<SearchOutlined />} allowClear />
            </AutoComplete>
          </Col>
          <Col xs={24} lg={14}>
            <Typography.Text type="secondary">区间</Typography.Text>
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
          <Col xs={24} md={8}>
            <Typography.Text type="secondary">回测引擎</Typography.Text>
            <Select
              style={{ width: "100%", marginTop: 6 }}
              value={engineMode}
              onChange={(v) => setEngineMode(v)}
              options={[
                { value: "regime", label: "多因子 v4.1（仓库主策略）" },
                { value: "simple", label: "简易双均线（单标的演示）" },
              ]}
            />
          </Col>
          {engineMode === "simple" && (
            <>
              <Col xs={24} md={8}>
                <Typography.Text type="secondary">均线策略</Typography.Text>
                <Select
                  style={{ width: "100%", marginTop: 6 }}
                  value={strategy}
                  onChange={(v) => setStrategy(v)}
                  options={[
                    { value: "ma_cross", label: "双均线（前收信号）" },
                    { value: "buy_hold", label: "买入持有" },
                  ]}
                />
              </Col>
              {strategy === "ma_cross" && (
                <>
                  <Col xs={12} md={4}>
                    <Typography.Text type="secondary">快线</Typography.Text>
                    <InputNumber
                      style={{ width: "100%", marginTop: 6 }}
                      min={2}
                      max={120}
                      value={fastMa}
                      onChange={(v) => setFastMa(Number(v) || 5)}
                    />
                  </Col>
                  <Col xs={12} md={4}>
                    <Typography.Text type="secondary">慢线</Typography.Text>
                    <InputNumber
                      style={{ width: "100%", marginTop: 6 }}
                      min={3}
                      max={250}
                      value={slowMa}
                      onChange={(v) => setSlowMa(Number(v) || 20)}
                    />
                  </Col>
                </>
              )}
            </>
          )}
          <Col xs={24} md={8}>
            <Typography.Text type="secondary" style={{ opacity: 0 }}>
              .
            </Typography.Text>
            <Button type="primary" block loading={loading} onClick={() => void run()} style={{ marginTop: 6 }}>
              拉取 K 线并回测
            </Button>
          </Col>
        </Row>
      </Card>

      {result && (
        <>
          {result.mode === "simple" ? (
            <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }} style={{ marginBottom: 16 }}>
              <Descriptions.Item label="证券">{result.ts_code}</Descriptions.Item>
              <Descriptions.Item label="名称">{result.name || "—"}</Descriptions.Item>
              <Descriptions.Item label="策略">{result.strategy}</Descriptions.Item>
              <Descriptions.Item label="策略收益">
                {(result.metrics.total_return_strategy * 100).toFixed(2)}%
              </Descriptions.Item>
              <Descriptions.Item label="基准（买入持有）">
                {(result.metrics.total_return_benchmark * 100).toFixed(2)}%
              </Descriptions.Item>
              <Descriptions.Item label="策略年化">
                {(result.metrics.ann_return_strategy * 100).toFixed(2)}%
              </Descriptions.Item>
              <Descriptions.Item label="基准年化">
                {(result.metrics.ann_return_benchmark * 100).toFixed(2)}%
              </Descriptions.Item>
              <Descriptions.Item label="策略最大回撤">
                {(result.metrics.max_drawdown_strategy * 100).toFixed(2)}%
              </Descriptions.Item>
              <Descriptions.Item label="换仓次数（近似）">{result.approx_position_changes}</Descriptions.Item>
            </Descriptions>
          ) : (
            <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }} style={{ marginBottom: 16 }}>
              <Descriptions.Item label="证券">{result.ts_code}</Descriptions.Item>
              <Descriptions.Item label="名称">{result.name || "—"}</Descriptions.Item>
              <Descriptions.Item label="模型">{result.model}</Descriptions.Item>
              <Descriptions.Item label="组合总收益">
                {pct(result.metrics_portfolio.total_return as number)}
              </Descriptions.Item>
              <Descriptions.Item label="组合年化">
                {pct(result.metrics_portfolio.annualized_return as number)}
              </Descriptions.Item>
              <Descriptions.Item label="夏普">{result.metrics_portfolio.sharpe_ratio ?? "—"}</Descriptions.Item>
              <Descriptions.Item label="最大回撤">
                {pct(result.metrics_portfolio.max_drawdown as number)}
              </Descriptions.Item>
              <Descriptions.Item label="年化换手">
                {result.metrics_portfolio.annualized_turnover != null
                  ? `${((result.metrics_portfolio.annualized_turnover as number) * 100).toFixed(1)}%`
                  : "—"}
              </Descriptions.Item>
              <Descriptions.Item label="交易日">{result.metrics_portfolio.n_trading_days ?? "—"}</Descriptions.Item>
            </Descriptions>
          )}

          <Card title="K 线（原始价）" bordered={false} style={{ marginBottom: 16 }}>
            <div ref={candleRef} style={{ width: "100%", minHeight: 360 }} />
          </Card>
          <Card
            title={
              result.mode === "regime"
                ? "净值：蓝=多因子组合（含止损）；灰=该标的买入持有"
                : "净值：蓝=策略；灰=该标的买入持有"
            }
            bordered={false}
            style={{ marginBottom: 16 }}
          >
            <div ref={equityRef} style={{ width: "100%", minHeight: result.mode === "regime" ? 200 : 220 }} />
          </Card>
          {result.mode === "regime" && (
            <Card title="该标的在组合中的日度权重（%，名义杠杆后）" bordered={false} style={{ marginBottom: 16 }}>
              <div ref={weightRef} style={{ width: "100%", minHeight: 120 }} />
            </Card>
          )}
        </>
      )}
    </div>
  );
}
