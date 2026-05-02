import { SearchOutlined } from "@ant-design/icons";
import {
  AutoComplete,
  Button,
  Checkbox,
  DatePicker,
  Input,
  InputNumber,
  Table,
  Typography,
  message,
} from "antd";
import type { Dayjs } from "dayjs";
import { useCallback, useEffect, useRef, useState } from "react";
import client from "../../api/client";
import { apiErrorDetail, pct, pctTurnover } from "../../utils";
import type { RegimeRun } from "../../researchRunStore";

type CostScenarioRow = {
  label: string;
  buy_bps: number;
  sell_bps: number;
  slip_buy_bps: number;
  slip_sell_bps: number;
  annualized_return: number;
  total_return: number;
  max_drawdown: number;
  sharpe_ratio: number;
  annualized_turnover: number;
  n_trading_days: number;
};

interface Props {
  tsCode: string;
  poolOnly: boolean;
  anchorCapitalEnabled: boolean;
  initialCapital: number;
  range: [Dayjs, Dayjs];
  loading: boolean;
  result: RegimeRun | null;
  setTsCode: (v: string) => void;
  setPoolOnly: (v: boolean) => void;
  setAnchorCapitalEnabled: (v: boolean) => void;
  setInitialCapital: (v: number) => void;
  setRange: (v: [Dayjs, Dayjs]) => void;
  onRun: () => void;
  onClear: () => void;
}

export default function ResearchConfigForm({
  tsCode, poolOnly, anchorCapitalEnabled, initialCapital, range,
  loading, result, setTsCode, setPoolOnly, setAnchorCapitalEnabled,
  setInitialCapital, setRange, onRun, onClear,
}: Props) {
  const [options, setOptions] = useState<{ value: string; label: string }[]>([]);
  const searchTimer = useRef<number>(0);
  const [costSensLoading, setCostSensLoading] = useState(false);
  const [costSensRows, setCostSensRows] = useState<CostScenarioRow[] | null>(null);
  const [costSensErr, setCostSensErr] = useState<string | null>(null);

  const fetchOptions = useCallback((q: string) => {
    window.clearTimeout(searchTimer.current);
    if (q.trim().length < 1) { setOptions([]); return; }
    searchTimer.current = window.setTimeout(async () => {
      try {
        const { data } = await client.get<{ items: { ts_code: string; name: string }[] }>(
          "/api/research/stocks", { params: { q: q.trim(), limit: 40 } },
        );
        setOptions((data.items ?? []).map((it) => ({ value: it.ts_code, label: `${it.ts_code} ${it.name}` })));
      } catch { setOptions([]); }
    }, 280);
  }, []);

  useEffect(() => { if (!result) { setCostSensRows(null); setCostSensErr(null); } }, [result]);

  const costSensEligible = initialCapital >= 1000 && (anchorCapitalEnabled || result?.backtest_mode === "cash_lots");

  const runCostSensitivity = useCallback(async () => {
    if (initialCapital < 1000) { message.warning("请填写初始本金（≥1000 元）"); return; }
    if (!result?.series?.length) { message.warning("请先运行一次主回测"); return; }
    const cap = result.initial_capital != null && result.initial_capital > 0 ? result.initial_capital : initialCapital;
    const start = range[0].format("YYYYMMDD");
    const end = range[1].format("YYYYMMDD");
    setCostSensLoading(true);
    setCostSensErr(null);
    try {
      const body: Record<string, string | number> = { start, end, initial_capital: cap };
      if (!poolOnly && tsCode.trim()) body.ts_code = tsCode.trim().toUpperCase();
      const { data } = await client.post<{ scenarios: CostScenarioRow[] }>(
        "/api/research/regime-cost-sensitivity", body, { timeout: 600_000 },
      );
      setCostSensRows(data.scenarios ?? []);
      message.success("成本敏感度计算完成");
    } catch (e: unknown) { const d = apiErrorDetail(e); setCostSensErr(d); message.error(d); }
    finally { setCostSensLoading(false); }
  }, [result, range, poolOnly, tsCode, anchorCapitalEnabled, initialCapital]);

  const costColumns = [
    { title: "场景", dataIndex: "label" as const, width: 120, ellipsis: true },
    { title: "买佣", dataIndex: "buy_bps" as const, width: 56, align: "right" as const },
    { title: "卖佣", dataIndex: "sell_bps" as const, width: 56, align: "right" as const },
    { title: "买滑", dataIndex: "slip_buy_bps" as const, width: 56, align: "right" as const },
    { title: "卖滑", dataIndex: "slip_sell_bps" as const, width: 56, align: "right" as const },
    { title: "年化", dataIndex: "annualized_return" as const, width: 68, render: (v: number) => pct(v) },
    { title: "总收益", dataIndex: "total_return" as const, width: 68, render: (v: number) => pct(v) },
    { title: "回撤", dataIndex: "max_drawdown" as const, width: 68, render: (v: number) => pct(v) },
    { title: "夏普", dataIndex: "sharpe_ratio" as const, width: 56 },
    { title: "换手", dataIndex: "annualized_turnover" as const, width: 68, render: (v: number) => pctTurnover(v) },
  ];

  return (
    <div>
      {/* 配置区 */}
      <div className="stat-card" style={{ marginBottom: 12, padding: 14 }}>
        <Checkbox checked={poolOnly} onChange={(e) => setPoolOnly(e.target.checked)} style={{ marginBottom: 10, display: "block" }}>
          仅全市场组合
        </Checkbox>

        <Typography.Text type="secondary" style={{ fontSize: 11, display: "block", marginBottom: 4 }}>对照股票</Typography.Text>
        <AutoComplete style={{ width: "100%", marginBottom: 10 }} options={options} value={tsCode}
          disabled={poolOnly} onSearch={fetchOptions} onSelect={(v) => setTsCode(v)} onChange={(v) => setTsCode(String(v))}
          placeholder="代码或名称搜索">
          <Input prefix={<SearchOutlined />} allowClear size="small" />
        </AutoComplete>

        <Typography.Text type="secondary" style={{ fontSize: 11, display: "block", marginBottom: 4 }}>区间</Typography.Text>
        <DatePicker.RangePicker style={{ width: "100%", marginBottom: 10 }} size="small" value={range}
          onChange={(vals) => { if (vals?.[0] && vals[1]) setRange([vals[0], vals[1]]); }} />

        <Checkbox checked={anchorCapitalEnabled} onChange={(e) => setAnchorCapitalEnabled(e.target.checked)} style={{ marginBottom: 6, display: "block" }}>
          整手现金（元）
        </Checkbox>
        <InputNumber style={{ width: "100%", marginBottom: 10 }} size="small" min={1_000} max={1e12} step={10_000}
          value={initialCapital} disabled={!anchorCapitalEnabled}
          onChange={(v) => setInitialCapital(typeof v === "number" ? v : 1_000_000)} />

        <Button type="primary" block loading={loading} onClick={onRun} style={{ marginBottom: 4 }}>
          {poolOnly ? "运行全市场组合" : "拉取 K 线并回测"}
        </Button>
        {result && (
          <Button type="link" block disabled={loading} onClick={onClear} size="small">
            清除结果
          </Button>
        )}
      </div>

      {/* 成本敏感度 */}
      {costSensEligible ? (
        <div className="stat-card" style={{ marginBottom: 12, padding: 14 }}>
          <Typography.Title level={5} style={{ margin: "0 0 6px", fontSize: 13 }}>成本敏感度</Typography.Title>
          <Button type="default" size="small" block loading={costSensLoading}
            disabled={loading || !result?.series?.length} onClick={() => void runCostSensitivity()}>
            扫描
          </Button>
          {costSensErr ? <Typography.Text type="danger" style={{ fontSize: 11, marginTop: 4, display: "block" }}>{costSensErr}</Typography.Text> : null}
        </div>
      ) : null}

      {/* 敏感度结果表 */}
      {costSensRows && costSensRows.length > 0 ? (
        <Table<CostScenarioRow> size="small" rowKey={(r) => r.label} pagination={false} tableLayout="fixed" scroll={{ x: 700 }}
          columns={costColumns} dataSource={costSensRows} style={{ marginBottom: 12 }} />
      ) : null}

      {/* 图例说明 */}
      <Typography.Text type="secondary" style={{ fontSize: 10, display: "block", lineHeight: 1.6, padding: "0 4px" }}>
        灰线为 CSI300 买入持有。勾选整手现金后启用 A 股 100 股一手、
        T+1、±9.5% 涨跌停等约束。
      </Typography.Text>
    </div>
  );
}
