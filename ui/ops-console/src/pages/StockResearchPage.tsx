import { LineChartOutlined, SearchOutlined } from "@ant-design/icons";
import {
  Alert,
  AutoComplete,
  Button,
  Card,
  Checkbox,
  Col,
  DatePicker,
  Descriptions,
  Input,
  InputNumber,
  Row,
  Table,
  Typography,
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
import { Link } from "react-router-dom";
import client from "../api/client";
import { clearResearchRun, startRegimeRun, useResearchRun, type YearlyReturnRow } from "../researchRunStore";

function pct(n: number | string | null | undefined): string {
  if (n === null || n === undefined) return "—";
  const x = typeof n === "string" ? parseFloat(n) : n;
  if (Number.isNaN(x)) return "—";
  return `${(x * 100).toFixed(2)}%`;
}

function cny(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return `¥${n.toLocaleString("zh-CN", { maximumFractionDigits: 0 })}`;
}

export default function StockResearchPage() {
  const [tsCode, setTsCode] = useState("601318.SH");
  /** 不选对照股票：仅全市场组合净值 + CSI300 灰线，无 K 线/权重 */
  const [poolOnly, setPoolOnly] = useState(false);
  /** 勾选后才传给 API；默认关闭以保持原先「相对净值乘数」曲线 */
  const [anchorCapitalEnabled, setAnchorCapitalEnabled] = useState(false);
  const [initialCapital, setInitialCapital] = useState<number>(1_000_000);
  const [range, setRange] = useState<[Dayjs, Dayjs]>([
    dayjs().subtract(730, "day"),
    dayjs(),
  ]);
  const { loading, result } = useResearchRun();
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
    const el2 = equityRef.current;
    if (!el2 || !result?.series?.length) return;

    const pool = result.run_scope === "pool" || !result.ts_code;
    const el = candleRef.current;
    const el3 = weightRef.current;

    let cChart: ReturnType<typeof createChart> | null = null;
    if (!pool && result.bars?.length && el) {
      cChart = createChart(el, {
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
      const series = cChart.addSeries(CandlestickSeries, {
        upColor: "#26a69a",
        downColor: "#ef5350",
        borderVisible: false,
        wickUpColor: "#26a69a",
        wickDownColor: "#ef5350",
      });
      series.setData(
        result.bars!.map((b) => ({
          time: b.time as Time,
          open: b.open,
          high: b.high,
          low: b.low,
          close: b.close,
        })),
      );
    }

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
      height: 200,
      rightPriceScale: { borderColor: "#1e2836" },
      timeScale: { borderColor: "#1e2836" },
    });
    const yuanAxis = Boolean(result.initial_capital && result.initial_capital > 0);
    const lineOpts = { color: "#2f6feb", lineWidth: 2 as const };
    const lineOpts2 = { color: "#78909c", lineWidth: 1 as const };
    const sLine = eqChart.addSeries(LineSeries, {
      ...lineOpts,
      ...(yuanAxis
        ? {
            priceFormat: {
              type: "custom" as const,
              formatter: (p: number) =>
                `¥${p.toLocaleString("zh-CN", { maximumFractionDigits: 0 })}`,
            },
          }
        : {}),
    });
    const bLine = eqChart.addSeries(LineSeries, {
      ...lineOpts2,
      ...(yuanAxis
        ? {
            priceFormat: {
              type: "custom" as const,
              formatter: (p: number) =>
                `¥${p.toLocaleString("zh-CN", { maximumFractionDigits: 0 })}`,
            },
          }
        : {}),
    });
    sLine.setData(
      result.series.map((r) => ({ time: r.time as Time, value: r.portfolio_equity })),
    );
    bLine.setData(
      result.series.map((r) => ({ time: r.time as Time, value: r.stock_benchmark_equity })),
    );

    let wChart: ReturnType<typeof createChart> | null = null;
    if (!pool && el3) {
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
      if (cChart && el) cChart.applyOptions({ width: el.clientWidth });
      eqChart.applyOptions({ width: el2.clientWidth });
      if (wChart && el3) wChart.applyOptions({ width: el3.clientWidth });
    });
    if (cChart && el) ro.observe(el);
    ro.observe(el2);
    if (wChart && el3) ro.observe(el3);
    return () => {
      ro.disconnect();
      cChart?.remove();
      eqChart.remove();
      wChart?.remove();
    };
  }, [result]);

  function run() {
    const start = range[0].format("YYYYMMDD");
    const end = range[1].format("YYYYMMDD");
    void startRegimeRun(
      poolOnly ? null : tsCode.trim().toUpperCase(),
      start,
      end,
      anchorCapitalEnabled ? initialCapital : null,
    );
  }

  return (
    <div>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        <LineChartOutlined style={{ marginRight: 8 }} />
        多因子组合回测
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        蓝线与绩效为<strong>全市场多因子组合</strong>（与{" "}
        <Typography.Text code>strategy/examples/regime_switching_strategy.py</Typography.Text> 一致）。勾选「仅全市场组合」时不传对照股票：灰线为{" "}
        <Typography.Text strong>CSI300 买入持有</Typography.Text>，无 K 线与单票权重。选股票时灰线为该标的买入持有，并展示 K 线与权重。可选「按初始本金展示净值」将两条净值锚定为区间首日该本金（元）。数据来自
        ClickHouse <Typography.Text code>stock_daily</Typography.Text>；双均线单票请用侧栏{" "}
        <Typography.Text strong>回测看板</Typography.Text>。
      </Typography.Paragraph>

      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        message="多因子管线会按年加载全市场行情与估值，首次计算可能占用数 GB 内存并耗时数分钟。切换侧栏其他页面不会中断服务端计算；完成后会有提示，返回本页即可查看图表。"
      />

      {loading ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="回测进行中 · 查看实时日志"
          description={
            <>
              服务端将策略日志写入{" "}
              <Typography.Text code>logs/research_regime.log</Typography.Text>。请打开侧栏{" "}
              <Link to="/logs?log=research-regime">日志流</Link>，并确认下拉框为「research_regime.log（多因子回测）」即可
              WebSocket 实时滚屏（与回填日志相同机制）。
            </>
          }
        />
      ) : null}

      <Card bordered={false} style={{ marginBottom: 16 }}>
        <Checkbox
          checked={poolOnly}
          onChange={(e) => setPoolOnly(e.target.checked)}
          style={{ display: "block", marginBottom: 12 }}
        >
          仅全市场组合（不选对照股票；灰线=沪深300 买入持有，无 K 线与权重）
        </Checkbox>
        <Row gutter={[16, 16]}>
          <Col xs={24} lg={10}>
            <Typography.Text type="secondary">对照股票（可选）</Typography.Text>
            <AutoComplete
              style={{ width: "100%", marginTop: 6 }}
              options={options}
              value={tsCode}
              disabled={poolOnly}
              onSearch={fetchOptions}
              onSelect={(v) => setTsCode(v)}
              onChange={(v) => setTsCode(String(v))}
              placeholder="代码或名称搜索；全市场模式可不填"
            >
              <Input prefix={<SearchOutlined />} allowClear />
            </AutoComplete>
          </Col>
          <Col xs={24} lg={8}>
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
          <Col xs={24} lg={6}>
            <Checkbox
              checked={anchorCapitalEnabled}
              onChange={(e) => setAnchorCapitalEnabled(e.target.checked)}
              style={{ display: "block", marginBottom: 4 }}
            >
              按初始本金展示净值（元）
            </Checkbox>
            <Typography.Text type="secondary">初始本金（元）</Typography.Text>
            <div style={{ marginTop: 6 }}>
              <InputNumber
                style={{ width: "100%" }}
                min={1_000}
                max={1e12}
                step={10_000}
                value={initialCapital}
                disabled={!anchorCapitalEnabled}
                onChange={(v) => setInitialCapital(typeof v === "number" ? v : 1_000_000)}
              />
            </div>
          </Col>
          <Col xs={24} md={8}>
            <Typography.Text type="secondary" style={{ opacity: 0 }}>
              .
            </Typography.Text>
            <Button type="primary" block loading={loading} onClick={run} style={{ marginTop: 6 }}>
              {poolOnly ? "运行全市场组合回测" : "拉取 K 线并回测"}
            </Button>
            {result && (
              <Button type="link" block disabled={loading} onClick={() => clearResearchRun()} style={{ marginTop: 4 }}>
                清除图表与结果
              </Button>
            )}
          </Col>
        </Row>
      </Card>

      {result && (
        <>
          <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }} style={{ marginBottom: 16 }}>
            <Descriptions.Item label="模式">
              {result.run_scope === "pool" || !result.ts_code ? "全市场组合" : "组合 + 对照票"}
            </Descriptions.Item>
            <Descriptions.Item label="证券">{result.ts_code ?? "—（未选）"}</Descriptions.Item>
            <Descriptions.Item label="名称">{result.name || "—"}</Descriptions.Item>
            <Descriptions.Item label="模型">{result.model}</Descriptions.Item>
            {result.initial_capital != null && result.initial_capital > 0 && (
              <>
                <Descriptions.Item label="初始本金（锚定）">
                  {cny(result.initial_capital)}
                </Descriptions.Item>
                <Descriptions.Item label="期末组合净值（元）">
                  {cny(result.series[result.series.length - 1]?.portfolio_equity)}
                </Descriptions.Item>
                <Descriptions.Item label={`期末${result.benchmark_label ?? "买入持有"}（元）`}>
                  {cny(result.series[result.series.length - 1]?.stock_benchmark_equity)}
                </Descriptions.Item>
              </>
            )}
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

          {result.yearly_returns && result.yearly_returns.length > 0 ? (
            <Card
              title="分年收益率（已扣交易成本，含组合止损）"
              bordered={false}
              style={{ marginBottom: 16 }}
            >
              <Typography.Paragraph type="secondary" style={{ marginBottom: 8 }}>
                各自然年内对日度<strong>净收益</strong>连乘得到年总回报，与命令行脚本分年口径一致；不含额外「再扣一遍成本」。
              </Typography.Paragraph>
              <Table<YearlyReturnRow>
                size="small"
                rowKey="year"
                pagination={false}
                columns={[
                  { title: "年份", dataIndex: "year", width: 88 },
                  {
                    title: "年收益率",
                    dataIndex: "net_return",
                    render: (v: number) => pct(v),
                  },
                  { title: "当年交易日", dataIndex: "trading_days", width: 120 },
                ]}
                dataSource={result.yearly_returns}
              />
            </Card>
          ) : null}

          {result.bars?.length ? (
            <Card title="K 线（原始价）" bordered={false} style={{ marginBottom: 16 }}>
              <div ref={candleRef} style={{ width: "100%", minHeight: 360 }} />
            </Card>
          ) : (
            <div ref={candleRef} style={{ display: "none" }} aria-hidden />
          )}
          <Card
            title={
              result.run_scope === "pool" || !result.ts_code
                ? result.initial_capital
                  ? "净值（元）：蓝=多因子全组合（含止损）；灰=CSI300 买入持有"
                  : "净值：蓝=多因子全组合（含止损）；灰=CSI300 买入持有"
                : result.initial_capital
                  ? "净值（元）：蓝=多因子全组合（含止损）；灰=该标的买入持有"
                  : "净值：蓝=多因子全组合（含止损）；灰=该标的买入持有"
            }
            bordered={false}
            style={{ marginBottom: 16 }}
          >
            <div ref={equityRef} style={{ width: "100%", minHeight: 200 }} />
          </Card>
          {result.run_scope !== "pool" && result.ts_code ? (
            <Card title="该标的在组合中的日度权重（%，名义杠杆后）" bordered={false} style={{ marginBottom: 16 }}>
              <div ref={weightRef} style={{ width: "100%", minHeight: 120 }} />
            </Card>
          ) : (
            <div ref={weightRef} style={{ display: "none" }} aria-hidden />
          )}
        </>
      )}
    </div>
  );
}
