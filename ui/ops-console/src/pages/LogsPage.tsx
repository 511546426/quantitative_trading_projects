import { ClearOutlined } from "@ant-design/icons";
import { Button, Card, Select, Space, Switch, Typography } from "antd";
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
  const [wsState, setWsState] = useState<"connecting" | "open" | "closed">("connecting");
  const [autoScroll, setAutoScroll] = useState(true);
  const preRef = useRef<HTMLPreElement>(null);

  const logParam = searchParams.get("log");
  useEffect(() => {
    if (logParam && validLogKeys.has(logParam)) {
      setLogKey((prev) => (prev === logParam ? prev : logParam));
    }
  }, [logParam, validLogKeys]);

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
        WebSocket 订阅文件尾部：先推送快照，再增量追加。磁盘上的日志默认<strong>持续追加</strong>（脚本多次运行会堆在同一文件里）；若从「任务与回填」启动并勾选
        「启动前清空该日志」，服务端会先截断对应文件再跑本次任务。浏览器内仅保留约 {Math.round(MAX_CHARS / 1000)}k
        字符以防卡顿，与文件大小无关。
      </Typography.Paragraph>

      <Card bordered={false}>
        <Space wrap style={{ marginBottom: 12 }}>
          <span style={{ color: "rgba(255,255,255,0.45)" }}>文件</span>
          <Select
            style={{ minWidth: 260 }}
            value={logKey}
            options={LOG_OPTIONS}
            onChange={(v) => {
              setLogKey(v);
              setSearchParams({ log: v }, { replace: true });
            }}
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
