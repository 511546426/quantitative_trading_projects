import {
  BarChartOutlined,
  CloudServerOutlined,
  FileTextOutlined,
  LineChartOutlined,
  WalletOutlined,
} from "@ant-design/icons";
import { Card, Descriptions, Typography } from "antd";
import { useNavigate } from "react-router-dom";
import { useHealth } from "../hooks/useHealth";
import { useMeta } from "../hooks/useMeta";

const quickLinks = [
  { key: "/research", icon: <LineChartOutlined />, title: "多因子组合回测", desc: "全市场因子组合、整手现金仿真、成本敏感度分析" },
  { key: "/backtest", icon: <BarChartOutlined />, title: "策略回测看板", desc: "双均线/买入持有单标的回测与绩效评价" },
  { key: "/logs", icon: <FileTextOutlined />, title: "日志流", desc: "WebSocket 实时日志查看，支持多文件切换" },
  { key: "/portfolio", icon: <WalletOutlined />, title: "持仓与流水", desc: "手工成交记账、持仓汇总与集中度监控" },
  { key: "/infra", icon: <CloudServerOutlined />, title: "数据基建", desc: "Docker 容器管理（ClickHouse/PostgreSQL/Redis）" },
];

export default function DashboardPage() {
  const nav = useNavigate();
  const health = useHealth();
  const meta = useMeta();

  return (
    <div>
      {/* ── Status Row ── */}
      <div style={{ display: "flex", gap: 12, marginBottom: 24, flexWrap: "wrap" }}>
        <StatusCard
          label="API 服务"
          ok={health.ok}
          value={health.ok === true ? "正常" : health.ok === false ? "不可达" : "检测中"}
          sub={health.serverTime ? `UTC ${health.serverTime.slice(11, 19)}` : "—"}
        />
        <StatusCard
          label="Python 运行时"
          ok={meta ? true : null}
          value={meta?.python ?? "—"}
        />
        <StatusCard
          label="鉴权"
          ok={meta ? !meta.auth_required || !!localStorage.getItem("quant_ops_api_key") : null}
          value={meta?.auth_required ? "需 API Key" : "开放"}
        />
        <StatusCard
          label="构建标识"
          ok={null}
          value={health.serverTime ? "已部署" : "—"}
          sub={meta?.build_id}
        />
      </div>

      {/* ── Quick Entry Cards ── */}
      <Typography.Title level={5} style={{ margin: "0 0 12px", color: "var(--text-secondary)", fontSize: 12, letterSpacing: "0.05em", textTransform: "uppercase" }}>
        快速入口
      </Typography.Title>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: 12, marginBottom: 24 }}>
        {quickLinks.map(({ key, icon, title, desc }) => (
          <div key={key} className="quick-card" onClick={() => nav(key)}>
            <div className="icon">{icon}</div>
            <div className="info">
              <h4>{title}</h4>
              <p>{desc}</p>
            </div>
          </div>
        ))}
      </div>

      {/* ── Environment ── */}
      <Card title="环境" bordered={false} style={{ background: "var(--bg-container)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)" }}>
        <Descriptions column={1} size="small" labelStyle={{ width: 140 }}>
          <Descriptions.Item label="项目根">
            <Typography.Text code style={{ fontSize: 12 }}>{meta?.project_dir ?? "—"}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="运维脚本">
            <Typography.Text code style={{ fontSize: 12 }}>{meta?.ops_sh ?? "—"}</Typography.Text>
          </Descriptions.Item>
          {meta?.log_paths ? (
            <Descriptions.Item label="日志文件">
              {Object.entries(meta.log_paths).map(([k, v]) => (
                <Typography.Text key={k} code style={{ fontSize: 11, display: "block" }}>{v}</Typography.Text>
              ))}
            </Descriptions.Item>
          ) : null}
        </Descriptions>
      </Card>
    </div>
  );
}

function StatusCard({ label, ok, value, sub }: { label: string; ok: boolean | null; value: string; sub?: string | null }) {
  const dotClass = ok === null ? "unknown" : ok ? "ok" : "error";
  return (
    <div className="stat-card" style={{ minWidth: 180, flex: 1 }}>
      <div className="label">
        <span className={`status-dot ${dotClass}`} />
        {label}
      </div>
      <div className="value">{value}</div>
      {sub ? <div className="sub">{sub}</div> : null}
    </div>
  );
}
