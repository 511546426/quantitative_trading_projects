import { ClearOutlined, SearchOutlined } from "@ant-design/icons";
import { Button, Input, Select, Space, Switch, Typography } from "antd";
import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

const LOG_OPTIONS = [
  { value: "daily", label: "daily_update.log" },
  { value: "research-regime", label: "research_regime.log（多因子回测）" },
  { value: "backfill-daily", label: "backfill_daily.log" },
  { value: "backfill-index", label: "backfill_index.log" },
  { value: "backfill-valuation", label: "backfill_valuation.log" },
];

const MAX_CHARS = 480_000;

export default function LogsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const validLogKeys = useMemo(() => new Set(LOG_OPTIONS.map((o) => o.value)), []);
  const [logKey, setLogKey] = useState(() => {
    const u = new URLSearchParams(window.location.search).get("log");
    return u && validLogKeys.has(u) ? u : "daily";
  });
  const [text, setText] = useState("");
  const [filter, setFilter] = useState("");
  const [wsState, setWsState] = useState<"connecting" | "open" | "closed">("connecting");
  const [autoScroll, setAutoScroll] = useState(true);
  const preRef = useRef<HTMLPreElement>(null);

  const logParam = searchParams.get("log");
  useEffect(() => {
    if (logParam && validLogKeys.has(logParam)) setLogKey((prev) => (prev === logParam ? prev : logParam));
  }, [logParam, validLogKeys]);

  useEffect(() => {
    setText(""); setWsState("connecting");
    const params = new URLSearchParams();
    const k = localStorage.getItem("quant_ops_api_key");
    if (k) params.set("token", k);
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const qs = params.toString();
    const ws = new WebSocket(`${proto}//${window.location.host}/api/ws/logs/${logKey}${qs ? `?${qs}` : ""}`);
    ws.onopen = () => setWsState("open");
    ws.onclose = () => setWsState("closed");
    ws.onerror = () => setWsState("closed");
    ws.onmessage = (ev) => {
      try {
        const d = JSON.parse(ev.data as string) as { type?: string; text?: string };
        if (d.type === "snapshot") { setText(d.text ?? ""); return; }
        if (d.type === "append" && d.text) { setText((prev) => { const n = prev + d.text!; return n.length > MAX_CHARS ? n.slice(-MAX_CHARS) : n; }); }
      } catch { /* ignore */ }
    };
    return () => ws.close();
  }, [logKey]);

  useEffect(() => { if (!autoScroll || !preRef.current) return; preRef.current.scrollTop = preRef.current.scrollHeight; }, [text, autoScroll]);

  // 过滤行: 按关键词/级别
  const filteredText = useMemo(() => {
    if (!filter) return text;
    return text.split("\n").filter((l) => l.toLowerCase().includes(filter.toLowerCase())).join("\n");
  }, [text, filter]);

  // 高亮级别
  const highlighted = useMemo(() => {
    return filteredText
      .replace(/(ERROR|CRITICAL)/g, '<span style="color:#ef5350;font-weight:600">$1</span>')
      .replace(/\b(WARNING|WARN)\b/g, '<span style="color:#f5a623;font-weight:600">$1</span>')
      .replace(/\b(INFO)\b/g, '<span style="color:#2f6feb">$1</span>')
      .replace(/\b(DEBUG)\b/g, '<span style="color:#546e7a">$1</span>');
  }, [filteredText]);

  return (
    <div>
      <Typography.Title level={4} style={{ margin: "0 0 4px", fontSize: 18 }}>日志流</Typography.Title>
      <Typography.Paragraph type="secondary" style={{ margin: "0 0 16px", fontSize: 12 }}>
        WebSocket 实时推送文件尾部，支持多文件切换与日志级别高亮。
      </Typography.Paragraph>

      <div style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)", padding: 14 }}>
        <Space wrap style={{ marginBottom: 10 }}>
          <Select size="small" style={{ minWidth: 240 }} value={logKey} options={LOG_OPTIONS}
            onChange={(v) => { setLogKey(v); setSearchParams({ log: v }, { replace: true }); }} />
          <Typography.Text type="secondary" style={{ fontSize: 11 }}>
            WS:{" "}{wsState === "open" ? <Typography.Text type="success">已连接</Typography.Text> : wsState === "connecting" ? "连接中…" : <Typography.Text type="danger">断开</Typography.Text>}
          </Typography.Text>
          <Switch size="small" checked={autoScroll} onChange={setAutoScroll} checkedChildren="自动滚" unCheckedChildren="手动" />
          <Button size="small" icon={<ClearOutlined />} onClick={() => setText("")}>清空</Button>
        </Space>
        <Input size="small" prefix={<SearchOutlined />} placeholder="过滤日志内容…" allowClear value={filter} onChange={(e) => setFilter(e.target.value)} style={{ marginBottom: 8 }} />

        <pre ref={preRef} className="q-terminal-scroll"
          style={{ margin: 0, height: "min(62vh, 640px)", overflow: "auto", padding: 12, background: "var(--bg-terminal)", border: "1px solid var(--border)", borderRadius: "var(--radius)", fontSize: 12, lineHeight: 1.5, whiteSpace: "pre-wrap", wordBreak: "break-word", fontFamily: "var(--font-mono)" }}
          dangerouslySetInnerHTML={{ __html: highlighted || "（空或等待日志写入）" }} />
      </div>
    </div>
  );
}
