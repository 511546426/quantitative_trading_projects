import { CloudOutlined, FolderOpenOutlined, SafetyOutlined } from "@ant-design/icons";
import { Card, Col, Descriptions, Row, Statistic, Typography } from "antd";
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import client, { fetchMeta, type MetaResponse } from "../api/client";

export default function DashboardPage() {
  const [meta, setMeta] = useState<MetaResponse | null>(null);
  const [health, setHealth] = useState<string>("…");

  useEffect(() => {
    fetchMeta().then(setMeta).catch(() => setMeta(null));
    client
      .get("/api/health")
      .then(() => setHealth("正常"))
      .catch(() => setHealth("不可达"));
  }, []);

  return (
    <div>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        运行总览
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        与 Streamlit 版共用同一套 <Typography.Text code>ops.sh</Typography.Text>{" "}
        能力；本控制台为前后端分离架构，适合内网部署与权限收口。
      </Typography.Paragraph>

      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card bordered={false}>
            <Statistic title="API 健康" value={health} prefix={<SafetyOutlined />} />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card bordered={false}>
            <Statistic
              title="Python 运行时"
              value={meta?.python ?? "—"}
              prefix={<CloudOutlined />}
            />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card bordered={false}>
            <Statistic
              title="鉴权"
              value={meta?.auth_required ? "需要 API Key" : "开放（仅内网）"}
            />
          </Card>
        </Col>
      </Row>

      <Card title="环境" style={{ marginTop: 16 }} bordered={false}>
        <Descriptions column={1} size="small" labelStyle={{ width: 140 }}>
          <Descriptions.Item label="项目根">
            <Typography.Text code copyable>
              {meta?.project_dir ?? "加载失败"}
            </Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="ops.sh">
            <Typography.Text code copyable>
              {meta?.ops_sh ?? "—"}
            </Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="快捷入口">
            <Link to="/infra">数据基建</Link>
            <span style={{ margin: "0 8px", color: "#555" }}>|</span>
            <Link to="/jobs">任务与回填</Link>
            <span style={{ margin: "0 8px", color: "#555" }}>|</span>
            <Link to="/logs">日志流</Link>
            <span style={{ margin: "0 8px", color: "#555" }}>|</span>
            <Link to="/research">单股研究</Link>
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Card
        title={
          <span>
            <FolderOpenOutlined style={{ marginRight: 8 }} />
            日志文件映射
          </span>
        }
        style={{ marginTop: 16 }}
        bordered={false}
      >
        {meta?.log_paths ? (
          <Descriptions column={1} size="small">
            {Object.entries(meta.log_paths).map(([k, v]) => (
              <Descriptions.Item key={k} label={k}>
                <Typography.Text code copyable style={{ fontSize: 12 }}>
                  {v}
                </Typography.Text>
              </Descriptions.Item>
            ))}
          </Descriptions>
        ) : (
          <Typography.Text type="secondary">无法读取 /api/meta</Typography.Text>
        )}
      </Card>
    </div>
  );
}
