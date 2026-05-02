import { Descriptions, Table, Typography } from "antd";
import { Empty } from "antd";
import type { RegimeRun, YearlyReturnRow } from "../../researchRunStore";
import { cny, pct, pctTurnover } from "../../utils";

interface Props {
  result: RegimeRun;
}

export default function ResearchPerformanceTable({ result }: Props) {
  const m = result.metrics_portfolio;
  const last = result.series?.[result.series.length - 1];

  return (
    <>
      {result.backtest_mode === "cash_lots" ? (
        <div style={{ marginBottom: 12, padding: "8px 12px", background: "rgba(47,111,235,0.1)", borderRadius: 4, border: "1px solid rgba(47,111,235,0.3)" }}>
          <Typography.Text strong style={{ color: "#91caff" }}>当前为整手现金账户回测（增强约束）</Typography.Text>
          <Typography.Paragraph type="secondary" style={{ margin: "4px 0 0", fontSize: 13 }}>
            T+1 卖出款次日入账、调仓买入顺延至下一交易日、±9.5% 近似涨跌停（不买涨停、跌停不卖顺延）、买卖滑点各 2bp；停牌/无价不成交。未模拟：分红送转、配股、集合竞价与逐笔队列。
          </Typography.Paragraph>
        </div>
      ) : null}

      <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }} style={{ marginBottom: 16 }}>
        <Descriptions.Item label="模式">
          {result.run_scope === "pool" || !result.ts_code ? "全市场组合" : "组合 + 对照票"}
        </Descriptions.Item>
        <Descriptions.Item label="证券">{result.ts_code ?? "—（未选）"}</Descriptions.Item>
        <Descriptions.Item label="名称">{result.name || "—"}</Descriptions.Item>
        <Descriptions.Item label="模型">{result.model}</Descriptions.Item>
        <Descriptions.Item label="回测口径">
          {result.backtest_mode === "cash_lots" ? "整手现金账户" : "理想权重+杠杆"}
        </Descriptions.Item>
        {result.initial_capital != null && result.initial_capital > 0 && (
          <>
            <Descriptions.Item label="初始本金（锚定）">{cny(result.initial_capital)}</Descriptions.Item>
            <Descriptions.Item label="期末组合净值（元）">{cny(last?.portfolio_equity)}</Descriptions.Item>
            <Descriptions.Item label={`期末${result.benchmark_label ?? "买入持有"}（元）`}>
              {cny(last?.stock_benchmark_equity)}
            </Descriptions.Item>
          </>
        )}
        <Descriptions.Item label="组合总收益">{pct(m.total_return as number)}</Descriptions.Item>
        <Descriptions.Item label="组合年化">{pct(m.annualized_return as number)}</Descriptions.Item>
        <Descriptions.Item label="夏普">{m.sharpe_ratio ?? "—"}</Descriptions.Item>
        <Descriptions.Item label="最大回撤">{pct(m.max_drawdown as number)}</Descriptions.Item>
        <Descriptions.Item label="年化换手">
          {m.annualized_turnover != null ? pctTurnover(m.annualized_turnover as number) : "—"}
        </Descriptions.Item>
        <Descriptions.Item label="交易日">{m.n_trading_days ?? "—"}</Descriptions.Item>
      </Descriptions>

      {result.yearly_returns && result.yearly_returns.length > 0 ? (
        <div style={{ marginBottom: 16 }}>
          <Typography.Title level={5} style={{ marginTop: 0 }}>分年收益率（已扣交易成本，含组合止损）</Typography.Title>
          <Typography.Paragraph type="secondary" style={{ marginBottom: 8 }}>
            各自然年内对日度<strong>净收益</strong>连乘得到年总回报；<strong>最大回撤</strong>为当年净值曲线内样本最大回撤；<strong>年化换手</strong>为当年日度换手（权重变化绝对和）均值×252。
          </Typography.Paragraph>
          <Table<YearlyReturnRow>
            size="small"
            rowKey="year"
            pagination={false}
            locale={{ emptyText: <Empty description="暂无分年数据" /> }}
            columns={[
              { title: "年份", dataIndex: "year", width: 88 },
              { title: "年收益率", dataIndex: "net_return", render: (v: number) => pct(v) },
              {
                title: "最大回撤",
                dataIndex: "max_drawdown",
                width: 110,
                render: (v: number | null | undefined) => pct(v),
              },
              {
                title: "年化换手",
                dataIndex: "annualized_turnover",
                width: 110,
                render: (v: number | null | undefined) => (v != null ? pctTurnover(v) : "—"),
              },
              { title: "当年交易日", dataIndex: "trading_days", width: 120 },
            ]}
            dataSource={result.yearly_returns}
          />
        </div>
      ) : null}
    </>
  );
}
