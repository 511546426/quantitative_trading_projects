import { ReloadOutlined, WalletOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Popconfirm,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  message,
} from "antd";
import { useCallback, useEffect, useState } from "react";
import client from "../api/client";

type TradeRow = {
  id: number;
  trade_date: string | null;
  ts_code: string;
  side: string;
  quantity: number;
  price: number;
  fee: number;
  note: string;
  created_at: string | null;
};

type PositionRow = {
  ts_code: string;
  net_quantity: number;
  last_close: number | null;
  as_of: string | null;
  market_value: number | null;
  pct_of_capital: number | null;
  concentration_alert?: boolean;
};

type SummaryResp = {
  capital: number;
  max_single_pct: number;
  total_market_value: number;
  total_position_pct: number;
  positions: PositionRow[];
  warnings: string[];
  poll_hint_sec: number;
};

export default function PortfolioMonitorPage() {
  const [trades, setTrades] = useState<TradeRow[]>([]);
  const [summary, setSummary] = useState<SummaryResp | null>(null);
  const [capital, setCapital] = useState(500_000);
  const [maxSingle, setMaxSingle] = useState(0.25);
  const [form] = Form.useForm();

  const loadTrades = useCallback(async () => {
    try {
      const { data } = await client.get<{ trades: TradeRow[] }>("/api/portfolio/trades", { params: { limit: 200 } });
      setTrades(data.trades ?? []);
    } catch {
      setTrades([]);
    }
  }, []);

  const loadSummary = useCallback(async () => {
    try {
      const { data } = await client.get<SummaryResp>("/api/portfolio/summary", {
        params: { capital, max_single_pct: maxSingle },
      });
      setSummary(data);
    } catch {
      setSummary(null);
    }
  }, [capital, maxSingle]);

  useEffect(() => {
    void loadTrades();
  }, [loadTrades]);

  useEffect(() => {
    void loadSummary();
    const id = window.setInterval(() => void loadSummary(), 8_000);
    return () => window.clearInterval(id);
  }, [loadSummary]);

  async function onAddTrade(v: {
    trade_date: string;
    ts_code: string;
    side: "BUY" | "SELL";
    quantity: number;
    price: number;
    fee?: number;
    note?: string;
  }) {
    try {
      await client.post("/api/portfolio/trades", {
        trade_date: v.trade_date,
        ts_code: v.ts_code.trim().toUpperCase(),
        side: v.side,
        quantity: v.quantity,
        price: v.price,
        fee: v.fee ?? 0,
        note: v.note ?? "",
      });
      message.success("已记录");
      form.resetFields();
      await loadTrades();
      await loadSummary();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      message.error(err.response?.data?.detail ?? "保存失败");
    }
  }

  async function onDelete(id: number) {
    try {
      await client.delete(`/api/portfolio/trades/${id}`);
      message.success("已删除");
      await loadTrades();
      await loadSummary();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      message.error(err.response?.data?.detail ?? "删除失败");
    }
  }

  return (
    <div>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        <WalletOutlined style={{ marginRight: 8 }} />
        持仓与手工流水
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        简易记账：成交写入 PostgreSQL 表 <Typography.Text code>manual_trade_ledger</Typography.Text>
        ；下方按净股数 × 最近收盘价估算市值（非券商实时持仓）。汇总每 <Typography.Text code>8s</Typography.Text>{" "}
        自动刷新；单标的占资金比例超过阈值时标红预警。
      </Typography.Paragraph>

      {summary?.warnings?.length ? (
        <Alert type="warning" showIcon style={{ marginBottom: 12 }} message="风控提示" description={summary.warnings.join("；")} />
      ) : null}

      <Card title="汇总（准实时）" extra={<Button icon={<ReloadOutlined />} onClick={() => void loadSummary()} size="small" />}>
        <Space wrap style={{ marginBottom: 12 }}>
          <Typography.Text type="secondary">总资金（元）</Typography.Text>
          <InputNumber min={1000} step={10000} value={capital} onChange={(v) => setCapital(Number(v) || 500_000)} />
          <Typography.Text type="secondary">单标的上限</Typography.Text>
          <InputNumber
            min={0.05}
            max={1}
            step={0.01}
            value={maxSingle}
            onChange={(v) => setMaxSingle(Number(v) || 0.25)}
          />
        </Space>
        {summary ? (
          <DescriptionsMini summary={summary} />
        ) : (
          <Typography.Text type="secondary">无法加载汇总（检查数据库与行情）</Typography.Text>
        )}
        <Table<PositionRow>
          style={{ marginTop: 12 }}
          size="small"
          rowKey="ts_code"
          pagination={false}
          dataSource={summary?.positions ?? []}
          columns={[
            { title: "代码", dataIndex: "ts_code", width: 110 },
            { title: "净股数", dataIndex: "net_quantity", width: 90 },
            { title: "最近收盘", dataIndex: "last_close", render: (x) => (x == null ? "—" : x.toFixed(3)) },
            { title: "收盘日", dataIndex: "as_of", width: 110 },
            {
              title: "市值",
              dataIndex: "market_value",
              render: (x) => (x == null ? "—" : x.toLocaleString(undefined, { maximumFractionDigits: 0 })),
            },
            {
              title: "占资金",
              dataIndex: "pct_of_capital",
              render: (x, row) =>
                x == null ? (
                  "—"
                ) : (
                  <Tag color={row.concentration_alert ? "error" : "default"}>{(x * 100).toFixed(2)}%</Tag>
                ),
            },
          ]}
        />
      </Card>

      <Card title="记一笔成交" style={{ marginTop: 16 }} bordered={false}>
        <Form
          form={form}
          layout="inline"
          onFinish={onAddTrade}
          style={{ rowGap: 12 }}
          initialValues={{ side: "BUY", trade_date: new Date().toISOString().slice(0, 10) }}
        >
          <Form.Item name="trade_date" label="日期" rules={[{ required: true }]}>
            <Input style={{ width: 130 }} placeholder="YYYY-MM-DD" />
          </Form.Item>
          <Form.Item name="ts_code" label="代码" rules={[{ required: true }]}>
            <Input style={{ width: 120 }} placeholder="600000.SH" />
          </Form.Item>
          <Form.Item name="side" label="方向" rules={[{ required: true }]}>
            <Select style={{ width: 90 }} options={[{ value: "BUY", label: "买" }, { value: "SELL", label: "卖" }]} />
          </Form.Item>
          <Form.Item name="quantity" label="数量" rules={[{ required: true }]}>
            <InputNumber min={0.0001} style={{ width: 120 }} />
          </Form.Item>
          <Form.Item name="price" label="价格" rules={[{ required: true }]}>
            <InputNumber min={0.0001} style={{ width: 120 }} />
          </Form.Item>
          <Form.Item name="fee" label="手续费">
            <InputNumber min={0} style={{ width: 100 }} />
          </Form.Item>
          <Form.Item name="note" label="备注">
            <Input style={{ width: 160 }} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit">
              保存
            </Button>
          </Form.Item>
        </Form>
      </Card>

      <Card title="成交流水" style={{ marginTop: 16 }} bordered={false}>
        <Button size="small" onClick={() => void loadTrades()} style={{ marginBottom: 8 }}>
          刷新列表
        </Button>
        <Table<TradeRow>
          size="small"
          rowKey="id"
          dataSource={trades}
          scroll={{ x: 900 }}
          columns={[
            { title: "ID", dataIndex: "id", width: 60 },
            { title: "日期", dataIndex: "trade_date", width: 110 },
            { title: "代码", dataIndex: "ts_code", width: 100 },
            { title: "方向", dataIndex: "side", width: 70 },
            { title: "数量", dataIndex: "quantity", width: 90 },
            { title: "价格", dataIndex: "price", width: 90 },
            { title: "费用", dataIndex: "fee", width: 80 },
            { title: "备注", dataIndex: "note", ellipsis: true },
            {
              title: "操作",
              width: 80,
              render: (_, row) => (
                <Popconfirm title="删除该笔？" onConfirm={() => void onDelete(row.id)}>
                  <Button type="link" size="small" danger>
                    删除
                  </Button>
                </Popconfirm>
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}

function DescriptionsMini({ summary }: { summary: SummaryResp }) {
  return (
    <Space size="large" wrap>
      <Typography.Text>
        持仓总市值 <Typography.Text strong>{summary.total_market_value.toLocaleString()}</Typography.Text> 元
      </Typography.Text>
      <Typography.Text>
        总市值/资金{" "}
        <Typography.Text strong type={summary.total_position_pct > 0.95 ? "danger" : undefined}>
          {(summary.total_position_pct * 100).toFixed(2)}%
        </Typography.Text>
      </Typography.Text>
    </Space>
  );
}
