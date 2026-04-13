import { ClearOutlined } from "@ant-design/icons";
import { Button, Card, Select, Space, Switch, Typography } from "antd";
import { useEffect, useRef, useState } from "react";

const LOG_OPTIONS = [
  { value: "daily", label: "daily_update.log" },
  { value: "backfill-daily", label: "backfill_daily.log" },
  { value: "backfill-index", label: "backfill_index.log" },
  { value: "backfill-valuation", label: "backfill_valuation.log" },
];

const MAX_CHARS = 480_000;

export default function LogsPage() {
  const [logKey, setLogKey] = useState("daily");
  const [text, setText] = useState("");
  const [wsState, setWsState] = useState<"connecting" | "open" | "closed">("connecting");
  const [autoScroll, setAutoScroll] = useState(true);
  const preRef = useRef<HTMLPreElement>(null);

  useEffect(() => {
    setText("");
    setWsState("connecting");
    const params = new URLSearchParams();
    const k = localStorage.getItem("quant_ops_api_key");
    if (k) params.set("token", k);
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const qs = params.toString();
    const url = `${proto}//${window.location.host}/api/ws/logs/${logKey}${qs ? `?${qs}` : ""}`;
    const ws = new WebSocket(url);

    ws.onopen = () => setWsState("open");
    ws.onclose = () => setWsState("closed");
    ws.onerror = () => setWsState("closed");

    ws.onmessage = (ev) => {
      try {
        const d = JSON.parse(ev.data as string) as {
          type?: string;
          text?: string;
        };
        if (d.type === "snapshot") {
          setText(d.text ?? "");
          return;
        }
        if (d.type === "append" && d.text) {
          setText((prev) => {
            const n = prev + d.text!;
            return n.length > MAX_CHARS ? n.slice(-MAX_CHARS) : n;
          });
        }
      } catch {
        /* ignore malformed */
      }
    };

    return () => {
      ws.close();
    };
  }, [logKey]);

  useEffect(() => {
    if (!autoScroll || !preRef.current) return;
    preRef.current.scrollTop = preRef.current.scrollHeight;
  }, [text, autoScroll]);

  return (
    <div>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        日志流
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        WebSocket 订阅文件尾部：先推送快照，再增量追加。适合观察长任务与回填进度。
      </Typography.Paragraph>

      <Card bordered={false}>
        <Space wrap style={{ marginBottom: 12 }}>
          <span style={{ color: "rgba(255,255,255,0.45)" }}>文件</span>
          <Select
            style={{ minWidth: 260 }}
            value={logKey}
            options={LOG_OPTIONS}
            onChange={(v) => setLogKey(v)}
          />
          <Typography.Text type="secondary">
            WS:{" "}
            {wsState === "open" ? (
              <Typography.Text type="success">已连接</Typography.Text>
            ) : wsState === "connecting" ? (
              "连接中…"
            ) : (
              <Typography.Text type="danger">已断开（切换文件重连）</Typography.Text>
            )}
          </Typography.Text>
          <Switch checked={autoScroll} onChange={setAutoScroll} checkedChildren="自动滚底" unCheckedChildren="手动" />
          <Button icon={<ClearOutlined />} onClick={() => setText("")}>
            清空视图
          </Button>
        </Space>

        <pre
          ref={preRef}
          className="q-terminal-scroll"
          style={{
            margin: 0,
            height: "min(62vh, 640px)",
            overflow: "auto",
            padding: 14,
            background: "#080b10",
            border: "1px solid #1e2836",
            borderRadius: 6,
            fontSize: 12,
            lineHeight: 1.45,
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
            fontFamily:
              'ui-monospace, "Cascadia Code", "SF Mono", Consolas, Menlo, monospace',
          }}
        >
          {text || "（空或等待日志写入）"}
        </pre>
      </Card>
    </div>
  );
}
