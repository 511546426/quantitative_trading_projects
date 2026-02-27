"""
历史数据回填流水线。

功能:
    - 按日期分片，逐日拉取全市场数据
    - 断点续传（checkpoint 存储在 PostgreSQL）
    - 进度展示
    - 限频等待与异常处理
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from data.cleaners.price_cleaner import PriceCleaner
from data.common.calendar import TradingCalendar
from data.common.exceptions import EmptyDataError, FetchError, RateLimitError
from data.fetchers.base import BaseFetcher
from data.writers.base import BaseWriter

logger = logging.getLogger(__name__)


@dataclass
class BackfillProgress:
    """回填进度"""
    task_name: str
    total_dates: int
    done_dates: int
    last_date: str
    status: str  # RUNNING / PAUSED / COMPLETED / FAILED
    started_at: datetime
    error_msg: str = ""

    @property
    def progress_pct(self) -> float:
        return self.done_dates / self.total_dates * 100 if self.total_dates else 0


class BackfillPipeline:
    """
    历史数据回填流水线。

    Parameters
    ----------
    fetcher : BaseFetcher
        数据采集器。
    ch_writer : BaseWriter
        ClickHouse 写入器。
    pg_writer : BaseWriter
        PostgreSQL 写入器（用于 checkpoint + 基本面数据）。
    calendar : TradingCalendar
        交易日历。
    stock_info : DataFrame, optional
        股票基础信息。
    """

    def __init__(
        self,
        fetcher: BaseFetcher,
        ch_writer: BaseWriter,
        pg_writer: BaseWriter,
        calendar: TradingCalendar,
        stock_info: pd.DataFrame | None = None,
    ):
        self.fetcher = fetcher
        self.ch_writer = ch_writer
        self.pg_writer = pg_writer
        self.calendar = calendar
        self.price_cleaner = PriceCleaner(stock_info=stock_info)

    def run(
        self,
        start_date: str,
        end_date: str,
        data_types: list[str] | None = None,
        task_name: str = "backfill_daily",
    ) -> BackfillProgress:
        """
        执行历史回填。

        Parameters
        ----------
        start_date : str
            起始日期 (YYYYMMDD)。
        end_date : str
            结束日期 (YYYYMMDD)。
        data_types : list[str]
            回填数据类型: ['daily', 'valuation', 'index']。
        task_name : str
            任务名（用于 checkpoint）。
        """
        if data_types is None:
            data_types = ["daily"]

        trade_dates = self.calendar.get_trade_dates(start_date, end_date)
        trade_date_strs = [d.strftime("%Y%m%d") for d in trade_dates]

        checkpoint = self._load_checkpoint(task_name)
        if checkpoint and checkpoint.last_date:
            remaining = [d for d in trade_date_strs if d > checkpoint.last_date]
            done = len(trade_date_strs) - len(remaining)
            logger.info(
                "从断点续传: %s, 已完成 %d/%d",
                checkpoint.last_date, done, len(trade_date_strs),
            )
        else:
            remaining = trade_date_strs
            done = 0

        progress = BackfillProgress(
            task_name=task_name,
            total_dates=len(trade_date_strs),
            done_dates=done,
            last_date=checkpoint.last_date if checkpoint else "",
            status="RUNNING",
            started_at=datetime.now(),
        )

        self._save_checkpoint(progress)

        for i, date_str in enumerate(remaining):
            try:
                if "daily" in data_types:
                    self._backfill_daily(date_str)

                if "valuation" in data_types:
                    self._backfill_valuation(date_str)

                if "index" in data_types:
                    self._backfill_index(date_str)

                progress.done_dates += 1
                progress.last_date = date_str

                if (i + 1) % 10 == 0:
                    self._save_checkpoint(progress)
                    logger.info(
                        "回填进度: %d/%d (%.1f%%) — %s",
                        progress.done_dates,
                        progress.total_dates,
                        progress.progress_pct,
                        date_str,
                    )

            except RateLimitError as e:
                logger.warning("触发限频，等待 60s: %s", e)
                time.sleep(60)
                continue

            except EmptyDataError:
                progress.done_dates += 1
                progress.last_date = date_str
                continue

            except Exception as e:
                progress.status = "FAILED"
                progress.error_msg = str(e)
                self._save_checkpoint(progress)
                logger.error("回填失败 (%s): %s", date_str, e)
                raise

        progress.status = "COMPLETED"
        self._save_checkpoint(progress)
        logger.info(
            "回填完成: %d 个交易日, %s ~ %s",
            progress.total_dates, start_date, end_date,
        )
        return progress

    # ================================================================
    # 单日回填
    # ================================================================

    def _backfill_daily(self, trade_date: str) -> None:
        """回填单日 K 线数据"""
        raw_bars = self.fetcher.get_daily_bars(trade_date=trade_date)

        try:
            raw_adj = self.fetcher.get_adj_factor(trade_date=trade_date)
            raw_bars = raw_bars.merge(
                raw_adj[["ts_code", "trade_date", "adj_factor"]],
                on=["ts_code", "trade_date"],
                how="left",
            )
        except (EmptyDataError, NotImplementedError):
            pass

        cleaned, _ = self.price_cleaner.clean(raw_bars)
        self.ch_writer.upsert(cleaned, "stock_daily", ["ts_code", "trade_date"])

    def _backfill_valuation(self, trade_date: str) -> None:
        """回填单日估值数据"""
        try:
            raw = self.fetcher.get_valuation(trade_date=trade_date)
            self.pg_writer.upsert(
                raw, "daily_valuation", ["ts_code", "trade_date"]
            )
        except (EmptyDataError, NotImplementedError):
            pass

    def _backfill_index(self, trade_date: str) -> None:
        """回填单日指数数据"""
        indices = ["000001.SH", "000300.SH", "000905.SH"]
        for idx_code in indices:
            try:
                df = self.fetcher.get_index_daily(
                    ts_code=idx_code,
                    start_date=trade_date,
                    end_date=trade_date,
                )
                self.ch_writer.upsert(
                    df, "index_daily", ["ts_code", "trade_date"]
                )
            except (EmptyDataError, NotImplementedError):
                pass

    # ================================================================
    # 断点续传
    # ================================================================

    def _load_checkpoint(self, task_name: str) -> BackfillProgress | None:
        try:
            rows = self.pg_writer.execute_query(
                "SELECT last_date, total_dates, done_dates, status, started_at "
                "FROM backfill_checkpoint WHERE task_name = %s",
                (task_name,),
            )
            if rows:
                row = rows[0]
                return BackfillProgress(
                    task_name=task_name,
                    last_date=row[0].strftime("%Y%m%d") if row[0] else "",
                    total_dates=row[1] or 0,
                    done_dates=row[2] or 0,
                    status=row[3] or "PAUSED",
                    started_at=row[4] or datetime.now(),
                )
        except Exception:
            pass
        return None

    def _save_checkpoint(self, progress: BackfillProgress) -> None:
        try:
            df = pd.DataFrame([{
                "task_name": progress.task_name,
                "last_date": progress.last_date,
                "total_dates": progress.total_dates,
                "done_dates": progress.done_dates,
                "status": progress.status,
                "error_msg": progress.error_msg,
                "started_at": progress.started_at,
            }])
            self.pg_writer.upsert(
                df, "backfill_checkpoint", ["task_name"]
            )
        except Exception as e:
            logger.warning("保存 checkpoint 失败: %s", e)
