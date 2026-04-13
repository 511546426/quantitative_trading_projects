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
      const { data } = await client.post<{ exit_code: number; output: string }>(
        "/api/ops/sync",
        { op, args: [] },
      );
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
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        数据基建
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        Docker 容器：ClickHouse / PostgreSQL / Redis。操作结果在下方「执行输出」。
      </Typography.Paragraph>

      <Card bordered={false}>
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

      <Card title="执行输出" style={{ marginTop: 16 }} bordered={false}>
        <pre
          className="q-terminal-scroll"
          style={{
            margin: 0,
            maxHeight: "min(55vh, 560px)",
            overflow: "auto",
            padding: 12,
            background: "#080b10",
            border: "1px solid #1e2836",
            borderRadius: 6,
            fontSize: 12,
            lineHeight: 1.5,
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
            fontFamily:
              'ui-monospace, "Cascadia Code", "SF Mono", Consolas, Menlo, monospace',
          }}
        >
          {last?.out ?? "等待操作…"}
        </pre>
      </Card>
    </div>
  );
}
