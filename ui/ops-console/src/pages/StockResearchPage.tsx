import { Alert, Skeleton, Typography } from "antd";
import dayjs from "dayjs";
import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { clearResearchRun, startRegimeRun, useResearchRun } from "../researchRunStore";
import ResearchCharts from "./research/ResearchCharts";
import ResearchConfigForm from "./research/ResearchConfigForm";

export default function StockResearchPage() {
  const [tsCode, setTsCode] = useState("601318.SH");
  const [poolOnly, setPoolOnly] = useState(false);
  const [anchorCapitalEnabled, setAnchorCapitalEnabled] = useState(true);
  const [initialCapital, setInitialCapital] = useState<number>(1_000_000);
  const [range, setRange] = useState<[dayjs.Dayjs, dayjs.Dayjs]>([dayjs().subtract(730, "day"), dayjs()]);
  const { loading, result } = useResearchRun();

  const mounted = useRef(false);
  useEffect(() => {
    if (mounted.current && !loading) {
      clearResearchRun();
    }
    mounted.current = true;
  }, [poolOnly, range[0].valueOf(), range[1].valueOf()]);

  function run() {
    const start = range[0].format("YYYYMMDD");
    const end = range[1].format("YYYYMMDD");
    void startRegimeRun(poolOnly ? null : tsCode.trim().toUpperCase(), start, end, anchorCapitalEnabled ? initialCapital : null);
  }

  return (
    <div>
      {/* 页面顶部 */}
      <div style={{ marginBottom: 16 }}>
        <Typography.Title level={4} style={{ margin: "0 0 4px", fontSize: 18 }}>多因子组合回测</Typography.Title>
        <Typography.Paragraph type="secondary" style={{ margin: 0, fontSize: 12 }}>
          蓝线为全市场多因子组合（与 <Typography.Text code>regime_switching_strategy.py</Typography.Text>
          v4.1 一致）。选股票时展示 K 线与权重灰线为对照票/CSI300 买入持有。
        </Typography.Paragraph>
      </div>

      {loading ? (
        <Alert type="warning" showIcon style={{ marginBottom: 12, padding: "8px 12px", fontSize: 12 }}
          message={<span>回测进行中 · 查看 <Link to="/logs?log=research-regime">实时日志</Link></span>} />
      ) : null}

      {/* 两栏布局 */}
      <div style={{ display: "flex", gap: 16, alignItems: "flex-start" }}>
        {/* 左栏：配置面板 */}
        <div style={{ width: "35%", minWidth: 280, maxWidth: 400, position: "sticky", top: 64 }}>
          <ResearchConfigForm
            tsCode={tsCode} poolOnly={poolOnly} anchorCapitalEnabled={anchorCapitalEnabled}
            initialCapital={initialCapital} range={range} loading={loading} result={result}
            setTsCode={setTsCode} setPoolOnly={setPoolOnly} setAnchorCapitalEnabled={setAnchorCapitalEnabled}
            setInitialCapital={setInitialCapital} setRange={setRange}
            onRun={run} onClear={() => clearResearchRun()} />
        </div>

        {/* 右栏：结果面板 */}
        <div style={{ flex: 1, minWidth: 0 }}>
          {loading ? (
            <div style={{ background: "var(--bg-container)", borderRadius: "var(--radius-lg)", border: "1px solid var(--border)", padding: 16 }}>
              <Skeleton active paragraph={{ rows: 6 }} />
            </div>
          ) : result ? (
            <ResearchCharts result={result} />
          ) : (
            <div style={{ background: "var(--bg-container)", borderRadius: "var(--radius-lg)", border: "1px solid var(--border)", padding: 40, textAlign: "center" }}>
              <Typography.Text type="secondary">请在左侧配置参数并运行回测</Typography.Text>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
