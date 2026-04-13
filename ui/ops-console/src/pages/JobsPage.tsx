import { CalendarOutlined, ReloadOutlined, RocketOutlined } from "@ant-design/icons";
import { App, Button, Card, Input, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { useCallback, useEffect, useState } from "react";
import client from "../api/client";

type JobRow = {
  id: string;
  kind: string;
  ops_cmd?: string;
  log_key?: string;
  pid: number;
  alive: boolean;
};

export default function JobsPage() {
  const { message, modal } = App.useApp();
  const [rows, setRows] = useState<JobRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [dailyDate, setDailyDate] = useState("");
  const [dailyBusy, setDailyBusy] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const { data } = await client.get<{ jobs: JobRow[] }>("/api/jobs");
      setRows(data.jobs ?? []);
    } catch {
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const t = window.setInterval(() => void refresh(), 3000);
    return () => window.clearInterval(t);
  }, [refresh]);

  async function runDaily() {
    setDailyBusy(true);
    try {
      const args = dailyDate.trim() ? [dailyDate.trim()] : [];
      const { data } = await client.post<{ exit_code: number; output: string }>(
        "/api/ops/sync",
        { op: "daily", args },
      );
      if (data.exit_code === 0) message.success("每日更新完成");
      else message.error(`退出码 ${data.exit_code}`);
      modal.info({
        title: "每日更新输出",
        width: 720,
        content: (
          <pre style={{ maxHeight: 360, overflow: "auto", fontSize: 12, whiteSpace: "pre-wrap" }}>
            {data.output}
          </pre>
        ),
      });
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      message.error(err.response?.data?.detail ?? "请求失败");
    } finally {
      setDailyBusy(false);
    }
  }

  async function startBackfill(target: "daily-bars" | "index" | "valuation", label: string) {
    modal.confirm({
      title: `启动后台回填：${label}`,
      content: "同一时间仅允许一个回填进程；任务在服务器侧持续运行。",
      okText: "启动",
      cancelText: "取消",
      onOk: async () => {
        try {
          const { data } = await client.post("/api/ops/backfill", { target });
          message.success(`已启动 · job ${data.job_id}`);
          await refresh();
        } catch (e: unknown) {
          const err = e as { response?: { data?: { detail?: string } } };
          message.error(err.response?.data?.detail ?? "启动失败");
        }
      },
    });
  }

  const columns: ColumnsType<JobRow> = [
    { title: "Job", dataIndex: "id", width: 140, render: (t) => <Typography.Text code>{t}</Typography.Text> },
    { title: "类型", dataIndex: "kind", width: 100 },
    { title: "命令", dataIndex: "ops_cmd", ellipsis: true },
    { title: "PID", dataIndex: "pid", width: 90 },
    {
      title: "状态",
      dataIndex: "alive",
      width: 100,
      render: (a: boolean) =>
        a ? <Tag color="processing">运行中</Tag> : <Tag>已结束</Tag>,
    },
  ];

  return (
    <div>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        任务与回填
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        后台回填通过 <Typography.Text code>ops.sh</Typography.Text> 启动；此处展示近期由控制台发起的任务 PID。
      </Typography.Paragraph>

      <Card title="每日更新（同步）" style={{ marginBottom: 16 }} bordered={false}>
        <Space wrap align="start">
          <Input
            style={{ width: 200 }}
            placeholder="YYYYMMDD，留空=今天"
            value={dailyDate}
            onChange={(e) => setDailyDate(e.target.value)}
            allowClear
          />
          <Button
            type="primary"
            icon={<CalendarOutlined />}
            loading={dailyBusy}
            onClick={() => void runDaily()}
          >
            执行每日更新
          </Button>
        </Space>
      </Card>

      <Card bordered={false}>
        <Space wrap style={{ marginBottom: 16 }}>
          <Button icon={<ReloadOutlined />} onClick={() => void refresh()} loading={loading}>
            刷新列表
          </Button>
          <Button
            type="primary"
            icon={<RocketOutlined />}
            onClick={() => startBackfill("daily-bars", "日 K 线")}
          >
            回填日 K
          </Button>
          <Button onClick={() => startBackfill("index", "指数日线")}>回填指数</Button>
          <Button onClick={() => startBackfill("valuation", "估值")}>回填估值</Button>
        </Space>
        <Table<JobRow>
          size="small"
          rowKey="id"
          loading={loading}
          columns={columns}
          dataSource={rows}
          pagination={false}
          locale={{ emptyText: "暂无由本控制台发起的后台任务" }}
        />
      </Card>
    </div>
  );
}
