import {
  ApiOutlined,
  CloudServerOutlined,
  DashboardOutlined,
  DeploymentUnitOutlined,
  FileTextOutlined,
  LineChartOutlined,
} from "@ant-design/icons";
import {
  Badge,
  Button,
  Drawer,
  Form,
  Input,
  Layout,
  Menu,
  Space,
  Typography,
} from "antd";
import { useEffect, useState } from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { fetchMeta } from "../api/client";

const { Header, Sider, Content } = Layout;

const menu = [
  { key: "/", icon: <DashboardOutlined />, label: "总览" },
  { key: "/infra", icon: <CloudServerOutlined />, label: "数据基建" },
  { key: "/jobs", icon: <DeploymentUnitOutlined />, label: "任务与回填" },
  { key: "/logs", icon: <FileTextOutlined />, label: "日志流" },
  { key: "/research", icon: <LineChartOutlined />, label: "单股研究" },
];

export default function AppShell() {
  const nav = useNavigate();
  const loc = useLocation();
  const [authRequired, setAuthRequired] = useState(false);
  const [drawer, setDrawer] = useState(false);
  const [keyForm] = Form.useForm();

  useEffect(() => {
    fetchMeta()
      .then((m) => setAuthRequired(m.auth_required))
      .catch(() => {});
  }, []);

  const hasKey = !!localStorage.getItem("quant_ops_api_key");

  return (
    <Layout style={{ minHeight: "100%" }}>
      <Sider width={220} breakpoint="lg" collapsedWidth={0}>
        <div style={{ padding: "20px 16px 12px" }}>
          <Typography.Text strong style={{ fontSize: 13, letterSpacing: "0.08em" }}>
            QUANT
            <span style={{ color: "#2f6feb", marginLeft: 4 }}>OPS</span>
          </Typography.Text>
          <Typography.Paragraph
            type="secondary"
            style={{ margin: "6px 0 0", fontSize: 11, lineHeight: 1.45 }}
          >
            Control Plane · 卖方级运维壳
          </Typography.Paragraph>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[loc.pathname === "/" ? "/" : loc.pathname]}
          items={menu}
          onClick={({ key }) => nav(key)}
          style={{ borderInlineEnd: "none" }}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            paddingInline: 20,
            borderBottom: "1px solid #1e2836",
          }}
        >
          <Typography.Title level={5} style={{ margin: 0, fontWeight: 600 }}>
            量化数据与容器编排
          </Typography.Title>
          <Space>
            {authRequired ? (
              <Badge status={hasKey ? "success" : "warning"} text="API Key" />
            ) : (
              <Badge status="default" text="无 Key 模式" />
            )}
            <Button type="default" icon={<ApiOutlined />} onClick={() => setDrawer(true)}>
              连接与凭据
            </Button>
          </Space>
        </Header>
        <Content style={{ margin: 20 }}>
          <Outlet />
        </Content>
      </Layout>

      <Drawer
        title="连接与凭据"
        open={drawer}
        onClose={() => setDrawer(false)}
        width={400}
      >
        <Typography.Paragraph type="secondary" style={{ fontSize: 13 }}>
          若服务端设置了环境变量 <Typography.Text code>QUANT_OPS_API_KEY</Typography.Text>
          ，请在此填写相同值；请求头将携带{" "}
          <Typography.Text code>X-API-Key</Typography.Text>，WebSocket 使用查询参数{" "}
          <Typography.Text code>token</Typography.Text>。
        </Typography.Paragraph>
        <Form
          form={keyForm}
          layout="vertical"
          onFinish={(v: { apiKey?: string }) => {
            const v0 = (v.apiKey ?? "").trim();
            if (v0) localStorage.setItem("quant_ops_api_key", v0);
            else localStorage.removeItem("quant_ops_api_key");
            setDrawer(false);
          }}
        >
          <Form.Item name="apiKey" label="API Key（可选）">
            <Input.Password placeholder="与 QUANT_OPS_API_KEY 一致" autoComplete="off" />
          </Form.Item>
          <Space>
            <Button type="primary" htmlType="submit">
              保存
            </Button>
            <Button
              danger
              onClick={() => {
                localStorage.removeItem("quant_ops_api_key");
                keyForm.resetFields();
                setDrawer(false);
              }}
            >
              清除
            </Button>
          </Space>
        </Form>
      </Drawer>
    </Layout>
  );
}
