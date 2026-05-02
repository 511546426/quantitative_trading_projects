import {
  ApiOutlined,
  BarChartOutlined,
  CloudServerOutlined,
  DashboardOutlined,
  DeploymentUnitOutlined,
  FileTextOutlined,
  LineChartOutlined,
  MenuFoldOutlined,
  MenuOutlined,
  WalletOutlined,
} from "@ant-design/icons";
import { Badge, Button, Drawer, Form, Input, Layout, Menu, Space, Typography } from "antd";
import { useEffect, useState } from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { useHealth } from "../hooks/useHealth";
import { useMeta } from "../hooks/useMeta";
import { getHttpTrace, subscribeHttpTrace } from "../api/traceStore";

const { Sider, Content } = Layout;

const menu = [
  { key: "/", icon: <DashboardOutlined />, label: "总览" },
  { key: "/infra", icon: <CloudServerOutlined />, label: "数据基建" },
  { key: "/jobs", icon: <DeploymentUnitOutlined />, label: "任务与回填" },
  { key: "/logs", icon: <FileTextOutlined />, label: "日志流" },
  { key: "/research", icon: <LineChartOutlined />, label: "多因子组合回测" },
  { key: "/backtest", icon: <BarChartOutlined />, label: "回测看板" },
  { key: "/portfolio", icon: <WalletOutlined />, label: "持仓与流水" },
];

const pageTitles: Record<string, string> = {
  "/": "运行总览",
  "/infra": "数据基建",
  "/jobs": "任务与回填",
  "/logs": "日志流",
  "/research": "多因子组合回测",
  "/backtest": "策略回测看板",
  "/portfolio": "持仓与手工流水",
};

export default function AppShell() {
  const nav = useNavigate();
  const loc = useLocation();
  const health = useHealth();
  const meta = useMeta();
  const [collapsed, setCollapsed] = useState(false);
  const [drawer, setDrawer] = useState(false);
  const [keyForm] = Form.useForm();
  const [, traceTick] = useState(0);
  const [, forceUpdate] = useState(0);
  const [clock, setClock] = useState("");

  // clock tick
  useEffect(() => {
    const id = window.setInterval(() => {
      setClock(new Date().toLocaleTimeString("zh-CN", { hour12: false }));
    }, 1000);
    return () => window.clearInterval(id);
  }, []);

  // trace subscription
  useEffect(() => {
    const unsub = subscribeHttpTrace(() => { traceTick((n) => n + 1); });
    return () => { unsub(); };
  }, []);

  // API key change detection
  useEffect(() => {
    const id = window.setInterval(() => forceUpdate((n) => n + 1), 2000);
    return () => window.clearInterval(id);
  }, []);

  const trace = getHttpTrace();
  const hasKey = !!localStorage.getItem("quant_ops_api_key");
  const selectedKey = loc.pathname === "/" ? "/" : "/" + loc.pathname.split("/").filter(Boolean)[0];
  const pageTitle = pageTitles[selectedKey] ?? "量化数据与容器编排";

  const healthDot = health.ok === null ? "unknown" : health.ok ? "ok" : "error";

  const authRequired = meta?.auth_required ?? false;

  return (
    <Layout style={{ minHeight: "100vh", background: "var(--bg-layout)" }}>
      {/* ── Top Bar ── */}
      <header
        style={{
          height: 48,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 16px",
          background: "var(--bg-container)",
          borderBottom: "1px solid var(--border)",
          zIndex: 100,
          position: "sticky",
          top: 0,
        }}
      >
        <Space>
          <Button
            type="text"
            icon={collapsed ? <MenuOutlined /> : <MenuFoldOutlined />}
            onClick={() => setCollapsed((c) => !c)}
            style={{ color: "var(--text-secondary)", fontSize: 16, width: 32, height: 32 }}
          />
          <Typography.Text strong style={{ fontSize: 13, letterSpacing: "0.08em", color: "var(--text-primary)" }}>
            QUANT<span style={{ color: "var(--primary)", marginLeft: 2 }}>OPS</span>
          </Typography.Text>
          <span style={{ width: 1, height: 20, background: "var(--border)", margin: "0 8px" }} />
          <Typography.Text style={{ fontSize: 13, color: "var(--text-secondary)" }}>
            {pageTitle}
          </Typography.Text>
        </Space>

        <Space size="middle">
          <Space size={4}>
            <span className={`status-dot ${healthDot}`} />
            <Typography.Text style={{ fontSize: 12, color: "var(--text-secondary)" }}>
              {health.serverTime ? health.serverTime.slice(11, 19) + " UTC" : "—"}
            </Typography.Text>
          </Space>
          <Typography.Text code style={{ fontSize: 11, color: "var(--text-tertiary)" }}>
            {clock}
          </Typography.Text>
          {authRequired ? (
            <Badge status={hasKey ? "success" : "warning"} text="Key" style={{ fontSize: 11 }} />
          ) : null}
          <Button type="text" size="small" icon={<ApiOutlined />} onClick={() => setDrawer(true)} style={{ color: "var(--text-secondary)" }} />
        </Space>
      </header>

      <Layout style={{ background: "var(--bg-layout)", flex: 1 }}>
        {/* ── Sidebar ── */}
        <Sider
          width={200}
          collapsedWidth={48}
          collapsible
          collapsed={collapsed}
          trigger={null}
          style={{
            background: "var(--bg-sider)",
            borderRight: "1px solid var(--border)",
            overflow: "auto",
          }}
        >
          <Menu
            theme="dark"
            mode="inline"
            selectedKeys={[selectedKey]}
            items={menu}
            onClick={({ key }) => { nav(key); if (collapsed) setCollapsed(false); }}
            style={{
              background: "transparent",
              borderInlineEnd: "none",
              marginTop: 4,
            }}
          />
        </Sider>

        {/* ── Content ── */}
        <Content style={{ padding: 20, overflow: "auto" }}>
          <div className="content-area">
            <Outlet />
          </div>
        </Content>
      </Layout>

      {/* ── Footer ── */}
      <footer
        style={{
          height: 28,
          display: "flex",
          alignItems: "center",
          padding: "0 16px",
          background: "var(--bg-container)",
          borderTop: "1px solid var(--border)",
          fontSize: 11,
          color: "var(--text-tertiary)",
          gap: 16,
        }}
      >
        <span>X-Request-ID: {trace.requestId ?? "—"}</span>
        <span>Server-Time: {trace.serverTimeHeader ?? "—"}</span>
        <span style={{ flex: 1 }} />
        <span>v{health.serverTime ? "1.0" : "—"}</span>
      </footer>

      {/* ── API Key Drawer ── */}
      <Drawer title="连接与凭据" open={drawer} onClose={() => setDrawer(false)} width={360}>
        <Typography.Paragraph type="secondary" style={{ fontSize: 13 }}>
          若服务端设置了 <Typography.Text code>QUANT_OPS_API_KEY</Typography.Text>，请在此填写。
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
          <Form.Item name="apiKey" label="API Key">
            <Input.Password placeholder="与 QUANT_OPS_API_KEY 一致" autoComplete="off" />
          </Form.Item>
          <Space>
            <Button type="primary" htmlType="submit">保存</Button>
            <Button danger onClick={() => { localStorage.removeItem("quant_ops_api_key"); keyForm.resetFields(); setDrawer(false); }}>
              清除
            </Button>
          </Space>
        </Form>
      </Drawer>
    </Layout>
  );
}
