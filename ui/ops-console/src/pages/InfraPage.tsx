import { PlayCircleOutlined } from "@ant-design/icons";
import { Alert, Button, Card, Space, Typography } from "antd";
import { useState } from "react";
import client from "../api/client";

export default function InfraPage() {
  const [loading, setLoading] = useState<string | null>(null);
  const [last, setLast] = useState<{ code: number; out: string; op: string } | null>(null);

  async function run(op: "status" | "start-db" | "stop-db" | "restart-db") {
    setLoading(op);
    setLast(null);
    try {
      const { data } = await client.post<{ exit_code: number; output: string }>("/api/ops/sync", { op, args: [] });
      setLast({ code: data.exit_code, out: data.output, op });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setLast({ code: -1, out: msg, op });
    } finally {
      setLoading(null);
    }
  }

  return (
    <div>
      <Typography.Title level={4} style={{ margin: "0 0 4px", fontSize: 18 }}>数据基建</Typography.Title>
      <Typography.Paragraph type="secondary" style={{ margin: "0 0 16px", fontSize: 12 }}>
        Docker 容器：ClickHouse / PostgreSQL / Redis。操作结果在下方「执行输出」。
      </Typography.Paragraph>

      <Card style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)" }}>
        <Space wrap>
          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            loading={loading === "status"}
            onClick={() => run("status")}
          >
            查看状态
          </Button>
          <Button loading={loading === "start-db"} onClick={() => run("start-db")}>
            启动容器
          </Button>
          <Button loading={loading === "stop-db"} onClick={() => run("stop-db")}>
            停止容器
          </Button>
          <Button loading={loading === "restart-db"} onClick={() => run("restart-db")}>
            重启容器
          </Button>
        </Space>
      </Card>

      {last && (
        <Alert
          style={{ marginTop: 16 }}
          type={last.code === 0 ? "success" : "error"}
          showIcon
          message={`${last.op} · exit ${last.code}`}
        />
      )}

      <Card title="执行输出" style={{ marginTop: 16, background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)" }}>
        <pre className="q-terminal-scroll" style={{
          margin: 0,
          padding: 12,
          background: "var(--bg-terminal)",
          border: "1px solid var(--border)",
          borderRadius: "var(--radius)",
          fontSize: 12,
          lineHeight: 1.5,
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
          fontFamily: "var(--font-mono)",
          maxHeight: "min(55vh, 560px)",
          overflow: "auto",
        }}>{last?.out ?? "等待操作…"}</pre>
      </Card>
    </div>
  );
}
