import { PlusOutlined, ReloadOutlined } from "@ant-design/icons";
import { Button, Card, Drawer, Empty, Form, Input, InputNumber, Popconfirm, Select, Space, Table, Tag, Typography, message } from "antd";
import { useCallback, useEffect, useState } from "react";
import client from "../api/client";
import { apiErrorDetail } from "../utils";

type TradeRow = { id: number; trade_date: string | null; ts_code: string; side: string; quantity: number; price: number; fee: number; note: string; created_at: string | null };
type PositionRow = { ts_code: string; net_quantity: number; last_close: number | null; as_of: string | null; market_value: number | null; pct_of_capital: number | null; concentration_alert?: boolean };
type SummaryResp = { capital: number; max_single_pct: number; total_market_value: number; total_position_pct: number; positions: PositionRow[]; warnings: string[]; poll_hint_sec: number };

export default function PortfolioMonitorPage() {
  const [trades, setTrades] = useState<TradeRow[]>([]);
  const [summary, setSummary] = useState<SummaryResp | null>(null);
  const [capital, setCapital] = useState(500_000);
  const [maxSingle, setMaxSingle] = useState(0.25);
  const [form] = Form.useForm();
  const [drawer, setDrawer] = useState(false);

  const loadTrades = useCallback(async () => {
    try { const { data } = await client.get<{ trades: TradeRow[] }>("/api/portfolio/trades", { params: { limit: 200 } }); setTrades(data.trades ?? []); }
    catch { setTrades([]); }
  }, []);
  const loadSummary = useCallback(async () => {
    try { const { data } = await client.get<SummaryResp>("/api/portfolio/summary", { params: { capital, max_single_pct: maxSingle } }); setSummary(data); }
    catch { setSummary(null); }
  }, [capital, maxSingle]);
  useEffect(() => { void loadTrades(); }, [loadTrades]);
  useEffect(() => { void loadSummary(); const id = window.setInterval(() => void loadSummary(), 8_000); return () => window.clearInterval(id); }, [loadSummary]);

  async function onAddTrade(v: { trade_date: string; ts_code: string; side: "BUY" | "SELL"; quantity: number; price: number; fee?: number; note?: string }) {
    try {
      await client.post("/api/portfolio/trades", { trade_date: v.trade_date, ts_code: v.ts_code.trim().toUpperCase(), side: v.side, quantity: v.quantity, price: v.price, fee: v.fee ?? 0, note: v.note ?? "" });
      message.success("已记录"); form.resetFields(); setDrawer(false); await loadTrades(); await loadSummary();
    } catch (e: unknown) { message.error(apiErrorDetail(e, "保存失败")); }
  }
  async function onDelete(id: number) {
    try { await client.delete(`/api/portfolio/trades/${id}`); message.success("已删除"); await loadTrades(); await loadSummary(); }
    catch (e: unknown) { message.error(apiErrorDetail(e, "删除失败")); }
  }

  const nPositions = summary?.positions?.length ?? 0;
  const totalMv = summary?.total_market_value ?? 0;
  const totalPct = summary?.total_position_pct ?? 0;

  return (
    <div>
      <Typography.Title level={4} style={{ margin: "0 0 4px", fontSize: 18 }}>持仓与手工流水</Typography.Title>
      <Typography.Paragraph type="secondary" style={{ margin: "0 0 16px", fontSize: 12 }}>
        按净股数 × 最近收盘价估算市值。汇总每 8s 自动刷新。
      </Typography.Paragraph>

      {/* 汇总条 */}
      <div style={{ display: "flex", gap: 10, marginBottom: 14, flexWrap: "wrap" }}>
        <SummaryBox label="持仓总市值" value={`¥${totalMv.toLocaleString()}`} />
        <SummaryBox label="持仓证券数" value={String(nPositions)} />
        <SummaryBox label="总市值/资金" value={`${(totalPct * 100).toFixed(2)}%`} color={totalPct > 0.95 ? "var(--red)" : undefined} />
        <SummaryBox label="总资金" value={`¥${capital.toLocaleString()}`} color="var(--text-secondary)" />
      </div>

      {/* 风控提示 */}
      {summary?.warnings?.length ? (
        <div style={{ marginBottom: 12, padding: "8px 12px", background: "var(--orange-bg)", border: "1px solid var(--orange)", borderRadius: "var(--radius)", fontSize: 12, color: "var(--orange)" }}>
          {summary.warnings.join("；")}
        </div>
      ) : null}

      <Card style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)", marginBottom: 16 }}>
        <Space style={{ marginBottom: 12 }}>
          <Typography.Text type="secondary" style={{ fontSize: 11 }}>总资金</Typography.Text>
          <InputNumber size="small" min={1000} step={10000} value={capital} onChange={(v) => setCapital(Number(v) || 500_000)} />
          <Typography.Text type="secondary" style={{ fontSize: 11 }}>单标的上限</Typography.Text>
          <InputNumber size="small" min={0.05} max={1} step={0.01} value={maxSingle} onChange={(v) => setMaxSingle(Number(v) || 0.25)} />
          <Button size="small" icon={<ReloadOutlined />} onClick={() => void loadSummary()} />
          <Button size="small" type="primary" icon={<PlusOutlined />} onClick={() => setDrawer(true)}>记一笔</Button>
        </Space>
        <Table<PositionRow> size="small" rowKey="ts_code" pagination={false} dataSource={summary?.positions ?? []}
          locale={{ emptyText: <Empty description="暂无持仓" /> }}
          columns={[
            { title: "代码", dataIndex: "ts_code", width: 100 },
            { title: "净股数", dataIndex: "net_quantity", width: 80 },
            { title: "最近收盘", dataIndex: "last_close", render: (x) => x == null ? "—" : x.toFixed(3) },
            { title: "收盘日", dataIndex: "as_of", width: 100 },
            { title: "市值", dataIndex: "market_value", render: (x) => x == null ? "—" : x.toLocaleString(undefined, { maximumFractionDigits: 0 }) },
            { title: "占资金", dataIndex: "pct_of_capital", width: 90, render: (x, row) => x == null ? "—" : <Tag color={row.concentration_alert ? "error" : "default"}>{(x * 100).toFixed(2)}%</Tag> },
          ]} />
      </Card>

      <Card style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
          <Typography.Text strong style={{ fontSize: 13 }}>成交流水</Typography.Text>
          <Button size="small" icon={<ReloadOutlined />} onClick={() => void loadTrades()} />
        </div>
        <Table<TradeRow> size="small" rowKey="id" dataSource={trades} scroll={{ x: 800 }}
          locale={{ emptyText: <Empty description="暂无成交" /> }}
          columns={[
            { title: "ID", dataIndex: "id", width: 52 }, { title: "日期", dataIndex: "trade_date", width: 100 },
            { title: "代码", dataIndex: "ts_code", width: 90 }, { title: "方向", dataIndex: "side", width: 60 },
            { title: "数量", dataIndex: "quantity", width: 80 }, { title: "价格", dataIndex: "price", width: 80 },
            { title: "费用", dataIndex: "fee", width: 72 }, { title: "备注", dataIndex: "note", ellipsis: true },
            { title: "操作", width: 68, render: (_, row) => <Popconfirm title="删除？" onConfirm={() => void onDelete(row.id)}><Button type="link" size="small" danger>删除</Button></Popconfirm> },
          ]} />
      </Card>

      <Drawer title="记一笔成交" open={drawer} onClose={() => setDrawer(false)} width={380}>
        <Form form={form} layout="vertical" onFinish={onAddTrade}
          initialValues={{ side: "BUY", trade_date: new Date().toISOString().slice(0, 10) }}>
          <Form.Item name="trade_date" label="日期" rules={[{ required: true }]}><Input placeholder="YYYY-MM-DD" /></Form.Item>
          <Form.Item name="ts_code" label="代码" rules={[{ required: true }]}><Input placeholder="600000.SH" /></Form.Item>
          <Form.Item name="side" label="方向" rules={[{ required: true }]}><Select options={[{ value: "BUY", label: "买" }, { value: "SELL", label: "卖" }]} /></Form.Item>
          <Form.Item name="quantity" label="数量" rules={[{ required: true }]}><InputNumber min={0.0001} style={{ width: "100%" }} /></Form.Item>
          <Form.Item name="price" label="价格" rules={[{ required: true }]}><InputNumber min={0.0001} style={{ width: "100%" }} /></Form.Item>
          <Form.Item name="fee" label="手续费"><InputNumber min={0} style={{ width: "100%" }} /></Form.Item>
          <Form.Item name="note" label="备注"><Input /></Form.Item>
          <Button type="primary" htmlType="submit" block>保存</Button>
        </Form>
      </Drawer>
    </div>
  );
}

function SummaryBox({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ padding: "10px 14px", background: "var(--bg-elevated)", borderRadius: "var(--radius)", border: "1px solid var(--border)" }}>
      <div style={{ fontSize: 10, color: "var(--text-secondary)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 }}>{label}</div>
      <div style={{ fontSize: 16, fontWeight: 600, color: color ?? "var(--text-primary)" }}>{value}</div>
    </div>
  );
}
