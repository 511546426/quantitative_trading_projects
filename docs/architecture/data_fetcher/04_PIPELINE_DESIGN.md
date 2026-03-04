# 流水线编排设计

## 核心思想

Pipeline 层是**编排者**，将 Fetcher → Cleaner → Writer 串联成完整的数据流。
有两种核心场景：**每日增量更新** 和 **历史数据回填**。

---

## 每日更新流水线（DailyPipeline）

### 触发时机

```
交易日 15:30 后触发（收盘后30分钟，等数据源更新完毕）
非交易日不执行（依赖交易日历判断）
```

### 执行流程

```
                    DailyPipeline.run(trade_date)
                              │
                ┌─────────────┼─────────────────┐
                │             │                 │
                ▼             ▼                 ▼
         [Task 1]       [Task 2]          [Task 3]
         日K线更新      基本面更新         辅助数据更新
                │             │                 │
                │             │                 │
    ┌───────────┤      ┌──────┤          ┌──────┤
    │           │      │      │          │      │
    ▼           ▼      ▼      ▼          ▼      ▼
  全市场      指数    估值   财务指标   北向    龙虎榜
  日K线      日线    数据   (如有新公告) 资金
    │           │      │      │          │      │
    ▼           ▼      ▼      ▼          ▼      ▼
  [清洗]     [清洗]  [清洗]  [清洗]    [清洗]  [清洗]
    │           │      │      │          │      │
    ▼           ▼      ▼      ▼          ▼      ▼
  [质检]     [质检]  [质检]  [质检]    [质检]  [质检]
    │           │      │      │          │      │
    ▼           ▼      ▼      ▼          ▼      ▼
[写入CH]   [写入CH] [写入PG] [写入PG] [写入PG] [写入PG]
    │           │      │      │          │      │
    └───────────┴──────┴──────┴──────────┴──────┘
                              │
                              ▼
                    [质量报告生成]
                              │
                              ▼
                    [发送通知 (成功/失败)]
```

### Task 依赖关系

```
独立 Task（可并行）:
  Task 1: 日K线 + 指数日线
  Task 2: 估值数据
  Task 3: 北向资金 + 龙虎榜

有依赖的 Task（需串行）:
  Task 1 完成后 → 复权价格计算（需要日K + 复权因子）
  Task 2 完成后 → 估值异常检查（需要当日收盘价对比）

财务指标：
  仅在财报季（1/4/7/10月）加密检查
  非财报季每周检查一次新公告
```

### 伪代码

```python
class DailyPipeline:
    def __init__(self, fetcher, cleaners, writers, calendar, quality_checker):
        self.fetcher = fetcher
        self.cleaners = cleaners
        self.writers = writers
        self.calendar = calendar
        self.checker = quality_checker

    def run(self, trade_date: str) -> PipelineReport:
        report = PipelineReport(trade_date=trade_date)
        
        # 前置检查
        if not self.calendar.is_trade_date(trade_date):
            report.status = 'SKIPPED'
            report.message = f'{trade_date} 非交易日'
            return report
        
        try:
            # Phase 1: 行情数据（优先级最高）
            self._update_price_data(trade_date, report)
            
            # Phase 2: 估值数据
            self._update_valuation(trade_date, report)
            
            # Phase 3: 辅助数据（可选，失败不阻塞）
            self._update_supplementary(trade_date, report)
            
            # Phase 4: 质量检查
            quality_result = self.checker.check_daily(trade_date)
            report.quality = quality_result
            
            if quality_result.has_critical_issue:
                report.status = 'WARNING'
            else:
                report.status = 'SUCCESS'
                
        except Exception as e:
            report.status = 'FAILED'
            report.error = str(e)
            
        # 发送通知
        self._notify(report)
        return report
```

---

## 历史回填流水线（BackfillPipeline）

### 场景

```
1. 系统初始化，需要灌入 2010~至今的全量历史数据
2. 新增数据字段，需要回填历史值
3. 数据源切换后，需要补充缺失数据
```

### 回填策略

```
按日期分片回填（而非按股票分片）：
  理由: Tushare 按日期查全市场效率更高，单次 API 返回一天全市场数据
  
  日期范围: 2010-01-01 ~ 至今
  分片大小: 1 天（每次拉取1个交易日的全市场数据）
  并发度:   1（串行，避免触发限频）
  
  总数据量估算:
    交易日: ~15年 × 245天 = ~3675 天
    每日股票数: ~5000 只
    总行数: ~18,000,000（1800 万行日K线）

  预计耗时:
    Tushare 限速 3 req/s，每天需 1~2 次请求
    3675 天 × 0.5s = ~30 分钟（日K线）
    加上基本面/估值: 总计 2~4 小时
```

### 断点续传

```
BackfillCheckpoint:
  存储位置: PostgreSQL (backfill_checkpoint 表)
  
  CREATE TABLE backfill_checkpoint (
      task_name    VARCHAR(50) PRIMARY KEY,
      last_date    DATE,            -- 最后成功处理的日期
      total_dates  INT,             -- 总日期数
      done_dates   INT,             -- 已完成日期数
      status       VARCHAR(20),     -- RUNNING / PAUSED / COMPLETED / FAILED
      started_at   TIMESTAMP,
      updated_at   TIMESTAMP
  );

  流程:
    1. 启动时查询 checkpoint，获取 last_date
    2. 从 last_date + 1 开始继续
    3. 每处理完一天，更新 checkpoint
    4. 意外中断后，下次自动从断点继续
    5. 完成后 status 置为 COMPLETED
```

### 伪代码

```python
class BackfillPipeline:
    def run(self, 
            start_date: str, 
            end_date: str, 
            data_types: list[str] = ['daily', 'valuation', 'financial']):
        
        trade_dates = self.calendar.get_trade_dates(start_date, end_date)
        checkpoint = self._load_checkpoint()
        
        if checkpoint and checkpoint.last_date:
            start_idx = trade_dates.index(checkpoint.last_date) + 1
            trade_dates = trade_dates[start_idx:]
        
        progress_bar = tqdm(trade_dates, desc='Backfill')
        
        for date in progress_bar:
            try:
                if 'daily' in data_types:
                    self._backfill_daily(date)
                if 'valuation' in data_types:
                    self._backfill_valuation(date)
                if 'financial' in data_types:
                    self._backfill_financial(date)
                    
                self._save_checkpoint(date)
                progress_bar.set_postfix(date=date)
                
            except RateLimitError:
                # 限频时暂停，等待后继续
                time.sleep(60)
                continue
            except Exception as e:
                self._save_checkpoint(date, status='FAILED', error=str(e))
                raise
```

---

## 调度器设计

### 定时任务表

| 任务 | 触发时间 | 频率 | 依赖 |
|------|----------|------|------|
| daily_price_update | 交易日 15:30 | 每交易日 | 无 |
| daily_valuation_update | 交易日 16:00 | 每交易日 | daily_price_update |
| financial_check | 每周一 18:00 | 每周 | 无 |
| quality_report | 交易日 17:00 | 每交易日 | 所有 daily 任务 |
| stock_list_sync | 每月 1 日 09:00 | 每月 | 无 |

### 调度器架构

```
Scheduler
├── 方案选择:
│   Phase 0-1: APScheduler (Python 进程内调度，简单)
│   Phase 2+:  系统 cron + 独立 Python 脚本 (更可靠)
│   Phase 4:   Airflow / Prefect (可视化 DAG，企业级)
│
├── 容错:
│   - 任务失败自动重试 (最多 3 次，间隔递增)
│   - 超时熔断 (单任务最长 30 分钟)
│   - 上游任务失败时下游任务自动跳过
│
└── 监控:
    - 任务执行日志 → PostgreSQL
    - 异常告警 → 邮件 / 企业微信
    - 执行耗时统计 → 趋势监控
```

### 调度配置 (schedules.yaml)

```yaml
schedules:
  daily_price:
    cron: "30 15 * * 1-5"      # 周一~周五 15:30
    task: daily_pipeline.update_price
    timeout_minutes: 30
    retry:
      max_retries: 3
      backoff_seconds: [60, 300, 900]
    depends_on: []
    
  daily_valuation:
    cron: "0 16 * * 1-5"
    task: daily_pipeline.update_valuation
    timeout_minutes: 20
    retry:
      max_retries: 3
      backoff_seconds: [60, 300, 900]
    depends_on: [daily_price]
    
  weekly_financial:
    cron: "0 18 * * 1"          # 每周一 18:00
    task: daily_pipeline.update_financial
    timeout_minutes: 60
    retry:
      max_retries: 2
      backoff_seconds: [120, 600]
    depends_on: []
    
  quality_report:
    cron: "0 17 * * 1-5"
    task: quality.generate_report
    timeout_minutes: 10
    depends_on: [daily_price, daily_valuation]
```

---

## Pipeline 执行报告

```python
@dataclass
class PipelineReport:
    trade_date: str
    status: str                      # SUCCESS / WARNING / FAILED / SKIPPED
    tasks: list[TaskReport]          # 各子任务报告
    quality: QualityReport | None
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    error: str | None

@dataclass
class TaskReport:
    name: str                        # 'daily_price' / 'valuation' / ...
    status: str
    rows_fetched: int
    rows_cleaned: int
    rows_written: int
    duration_seconds: float
    error: str | None
```

```
报告示例:
┌─────────────────────────────────────────────────┐
│ Daily Pipeline Report - 2026-02-26              │
├─────────────────────────────────────────────────┤
│ Status: SUCCESS                                 │
│ Duration: 45.2s                                 │
│                                                 │
│ Tasks:                                          │
│   ✅ daily_price:    5012 rows, 12.3s          │
│   ✅ index_daily:      15 rows,  1.1s          │
│   ✅ valuation:      5012 rows,  8.7s          │
│   ✅ north_flow:        1 row,   2.1s          │
│   ⚠️  financial:     skipped (non-report season)│
│                                                 │
│ Quality:                                        │
│   ✅ Record count: 5012 (expected ≥ 4000)      │
│   ✅ Price anomaly: 0 found                    │
│   ⚠️  Missing turnover: 3 stocks               │
└─────────────────────────────────────────────────┘
```
