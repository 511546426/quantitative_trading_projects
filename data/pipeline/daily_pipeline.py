"""
每日数据更新流水线。

交易日 15:30 后触发，流程:
    1. 拉取当日全市场日K线 + 复权因子
    2. 清洗（复权/停牌/涨跌停标记）
    3. 质量检查
    4. 写入 ClickHouse / PostgreSQL
    5. 生成报告 & 通知
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from data.cleaners.price_cleaner import PriceCleaner
from data.cleaners.reference_cleaner import ReferenceCleaner
from data.common.calendar import TradingCalendar
from data.common.exceptions import EmptyDataError, FetchError
from data.fetchers.base import BaseFetcher
from data.quality.checker import QualityChecker, QualityReport
from data.writers.base import BaseWriter

logger = logging.getLogger(__name__)


@dataclass
class TaskReport:
    """子任务报告"""
    name: str
    status: str = "PENDING"
    rows_fetched: int = 0
    rows_cleaned: int = 0
    rows_written: int = 0
    duration_seconds: float = 0.0
    error: str | None = None


@dataclass
class PipelineReport:
    """流水线报告"""
    trade_date: str
    status: str = "PENDING"
    tasks: list[TaskReport] = field(default_factory=list)
    quality: QualityReport | None = None
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
    duration_seconds: float = 0.0
    error: str | None = None

    def summary(self) -> str:
        lines = [
            f"{'='*50}",
            f"Daily Pipeline Report — {self.trade_date}",
            f"Status: {self.status}",
            f"Duration: {self.duration_seconds:.1f}s",
            f"{'='*50}",
        ]
        for t in self.tasks:
            icon = "✅" if t.status == "SUCCESS" else "❌" if t.status == "FAILED" else "⏭️"
            lines.append(
                f"  {icon} {t.name}: {t.rows_written} rows, {t.duration_seconds:.1f}s"
                + (f" ({t.error})" if t.error else "")
            )
        if self.quality:
            lines.append(f"\nQuality: {self.quality.overall_level}")
        return "\n".join(lines)


class DailyPipeline:
    """
    每日数据更新流水线。

    Parameters
    ----------
    fetcher : BaseFetcher
        数据采集器（或 FetcherRouter）。
    ch_writer : BaseWriter
        ClickHouse 写入器。
    pg_writer : BaseWriter
        PostgreSQL 写入器。
    calendar : TradingCalendar
        交易日历。
    quality_checker : QualityChecker, optional
        质量检查器。
    stock_info : DataFrame, optional
        股票基础信息（用于清洗时的涨跌停判定）。
    """

    def __init__(
        self,
        fetcher: BaseFetcher,
        ch_writer: BaseWriter,
        pg_writer: BaseWriter,
        calendar: TradingCalendar,
        quality_checker: QualityChecker | None = None,
        stock_info: pd.DataFrame | None = None,
    ):
        self.fetcher = fetcher
        self.ch_writer = ch_writer
        self.pg_writer = pg_writer
        self.calendar = calendar
        self.checker = quality_checker
        self.stock_info = stock_info

        self.price_cleaner = PriceCleaner(stock_info=stock_info)
        self.ref_cleaner = ReferenceCleaner()

    def run(self, trade_date: str) -> PipelineReport:
        """执行每日更新"""
        report = PipelineReport(trade_date=trade_date)
        start = time.time()

        if not self.calendar.is_trade_date(trade_date):
            report.status = "SKIPPED"
            report.error = f"{trade_date} 非交易日"
            logger.info(report.error)
            return report

        try:
            self._update_daily_price(trade_date, report)
            self._update_index(trade_date, report)
            self._update_valuation(trade_date, report)

            if self.checker:
                self._run_quality_check(trade_date, report)

            has_failure = any(t.status == "FAILED" for t in report.tasks)
            has_critical = report.quality and report.quality.has_critical_issue
            if has_critical:
                report.status = "CRITICAL"
            elif has_failure:
                report.status = "WARNING"
            else:
                report.status = "SUCCESS"

        except Exception as e:
            report.status = "FAILED"
            report.error = str(e)
            logger.exception("Daily pipeline 执行异常: %s", e)

        report.finished_at = datetime.now()
        report.duration_seconds = time.time() - start
        logger.info(report.summary())
        return report

    # ================================================================
    # 子任务
    # ================================================================

    def _update_daily_price(self, trade_date: str, report: PipelineReport) -> None:
        task = TaskReport(name="daily_price")
        start = time.time()

        try:
            raw_bars = self.fetcher.get_daily_bars(trade_date=trade_date)
            task.rows_fetched = len(raw_bars)

            raw_adj = self.fetcher.get_adj_factor(trade_date=trade_date)
            raw_bars = raw_bars.merge(
                raw_adj[["ts_code", "trade_date", "adj_factor"]],
                on=["ts_code", "trade_date"],
                how="left",
            )

            cleaned, clean_report = self.price_cleaner.clean(raw_bars)
            task.rows_cleaned = len(cleaned)

            written = self.ch_writer.upsert(
                cleaned, "stock_daily", ["ts_code", "trade_date"]
            )
            task.rows_written = written
            task.status = "SUCCESS"

        except EmptyDataError as e:
            task.status = "SKIPPED"
            task.error = str(e)
        except Exception as e:
            task.status = "FAILED"
            task.error = str(e)
            logger.error("daily_price 失败: %s", e)

        task.duration_seconds = time.time() - start
        report.tasks.append(task)

    def _update_index(self, trade_date: str, report: PipelineReport) -> None:
        task = TaskReport(name="index_daily")
        start = time.time()

        indices = ["000001.SH", "000300.SH", "000905.SH"]

        try:
            all_dfs = []
            for idx_code in indices:
                try:
                    df = self.fetcher.get_index_daily(
                        ts_code=idx_code,
                        start_date=trade_date,
                        end_date=trade_date,
                    )
                    all_dfs.append(df)
                except (EmptyDataError, NotImplementedError):
                    pass

            if all_dfs:
                combined = pd.concat(all_dfs, ignore_index=True)
                task.rows_fetched = len(combined)
                written = self.ch_writer.upsert(
                    combined, "index_daily", ["ts_code", "trade_date"]
                )
                task.rows_written = written

            task.status = "SUCCESS"
        except Exception as e:
            task.status = "FAILED"
            task.error = str(e)
            logger.error("index_daily 失败: %s", e)

        task.duration_seconds = time.time() - start
        report.tasks.append(task)

    def _update_valuation(self, trade_date: str, report: PipelineReport) -> None:
        task = TaskReport(name="valuation")
        start = time.time()

        try:
            raw = self.fetcher.get_valuation(trade_date=trade_date)
            task.rows_fetched = len(raw)

            written = self.pg_writer.upsert(
                raw, "daily_valuation", ["ts_code", "trade_date"]
            )
            task.rows_written = written
            task.status = "SUCCESS"

        except EmptyDataError as e:
            task.status = "SKIPPED"
            task.error = str(e)
        except Exception as e:
            task.status = "FAILED"
            task.error = str(e)
            logger.error("valuation 失败: %s", e)

        task.duration_seconds = time.time() - start
        report.tasks.append(task)

    def _run_quality_check(self, trade_date: str, report: PipelineReport) -> None:
        price_task = next(
            (t for t in report.tasks if t.name == "daily_price"), None
        )
        if not price_task or price_task.status != "SUCCESS":
            return

        try:
            from data.writers.clickhouse_writer import ClickHouseWriter
            if isinstance(self.ch_writer, ClickHouseWriter):
                count = self.ch_writer.count_rows("stock_daily", trade_date)
                dummy_df = pd.DataFrame({"count": [count]})
                context = {
                    "trade_date": trade_date,
                    "data_type": "daily_price",
                }
                report.quality = self.checker.check(dummy_df, context)
        except Exception as e:
            logger.warning("质量检查失败: %s", e)

    # ================================================================
    # 辅助任务
    # ================================================================

    def sync_stock_list(self) -> int:
        """同步股票列表到 PostgreSQL"""
        raw = self.fetcher.get_stock_list()
        cleaned, _ = self.ref_cleaner.clean(raw)
        written = self.pg_writer.upsert(
            cleaned, "stock_info", ["ts_code"]
        )
        self.stock_info = cleaned
        self.price_cleaner = PriceCleaner(stock_info=cleaned)
        logger.info("股票列表同步完成: %d 条", written)
        return written

    def sync_trade_calendar(
        self, start_date: str = "19900101", end_date: str = "20301231"
    ) -> int:
        """同步交易日历到 PostgreSQL"""
        raw = self.fetcher.get_trade_calendar(
            exchange="SSE", start_date=start_date, end_date=end_date
        )
        written = self.pg_writer.upsert(
            raw, "trade_calendar", ["exchange", "cal_date"]
        )
        logger.info("交易日历同步完成: %d 条", written)
        return written
