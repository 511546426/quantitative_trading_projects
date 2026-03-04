"""
定时调度器。

Phase 0~1: 使用 APScheduler 进程内调度。
Phase 2+:  可迁移到系统 cron 或 Airflow。
"""
from __future__ import annotations

import logging
from typing import Any, Callable

import yaml

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """
    基于 APScheduler 的数据流水线调度器。

    Parameters
    ----------
    config_path : str
        调度配置文件路径 (schedules.yaml)。
    """

    def __init__(self, config_path: str | None = None):
        self._config_path = config_path
        self._scheduler = None
        self._tasks: dict[str, Callable] = {}

    def register_task(self, name: str, func: Callable) -> None:
        """注册可调度的任务函数"""
        self._tasks[name] = func
        logger.info("注册任务: %s", name)

    def start(self) -> None:
        """启动调度器"""
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger

        self._scheduler = BlockingScheduler()

        schedules = self._load_config()
        for job_name, job_cfg in schedules.items():
            task_path = job_cfg.get("task", "")
            cron_expr = job_cfg.get("cron", "")
            timeout = job_cfg.get("timeout_minutes", 30)

            task_func = self._resolve_task(task_path)
            if not task_func:
                logger.warning("任务 %s 未注册，跳过: %s", task_path, job_name)
                continue

            trigger = CronTrigger.from_crontab(cron_expr)

            self._scheduler.add_job(
                func=self._wrap_task(task_func, job_name, timeout),
                trigger=trigger,
                id=job_name,
                name=job_name,
                misfire_grace_time=60,
                max_instances=1,
            )
            logger.info(
                "已调度: %s [%s] -> %s", job_name, cron_expr, task_path
            )

        logger.info("调度器启动，已注册 %d 个任务", len(schedules))

        try:
            self._scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("调度器停止")

    def stop(self) -> None:
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            logger.info("调度器已停止")

    # ================================================================
    # 内部方法
    # ================================================================

    def _load_config(self) -> dict:
        if not self._config_path:
            return {}
        try:
            with open(self._config_path, encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            return raw.get("schedules", {})
        except Exception as e:
            logger.error("加载调度配置失败: %s", e)
            return {}

    def _resolve_task(self, task_path: str) -> Callable | None:
        """从注册表中查找任务"""
        if task_path in self._tasks:
            return self._tasks[task_path]
        parts = task_path.split(".")
        for i in range(len(parts), 0, -1):
            key = ".".join(parts[:i])
            if key in self._tasks:
                return self._tasks[key]
        return None

    @staticmethod
    def _wrap_task(
        func: Callable, job_name: str, timeout_minutes: int
    ) -> Callable:
        """给任务添加日志和异常捕获"""
        import time

        def wrapper():
            logger.info("开始执行: %s", job_name)
            start = time.time()
            try:
                func()
                elapsed = time.time() - start
                logger.info(
                    "完成: %s (%.1fs)", job_name, elapsed
                )
            except Exception as e:
                elapsed = time.time() - start
                logger.exception(
                    "失败: %s (%.1fs): %s", job_name, elapsed, e
                )

        return wrapper


def create_scheduler(
    config_path: str,
    daily_pipeline: Any = None,
) -> PipelineScheduler:
    """
    工厂函数：创建并配置调度器。

    Parameters
    ----------
    config_path : str
        schedules.yaml 路径。
    daily_pipeline : DailyPipeline, optional
        每日更新流水线实例。
    """
    scheduler = PipelineScheduler(config_path=config_path)

    if daily_pipeline:
        from datetime import date

        def _run_daily():
            today = date.today().strftime("%Y%m%d")
            daily_pipeline.run(today)

        def _run_valuation():
            today = date.today().strftime("%Y%m%d")
            daily_pipeline._update_valuation(today, None)

        def _run_financial():
            pass

        def _run_quality():
            pass

        def _sync_stock_list():
            daily_pipeline.sync_stock_list()

        scheduler.register_task("daily_pipeline.update_price", _run_daily)
        scheduler.register_task("daily_pipeline.update_valuation", _run_valuation)
        scheduler.register_task("daily_pipeline.update_financial", _run_financial)
        scheduler.register_task("daily_pipeline.sync_stock_list", _sync_stock_list)
        scheduler.register_task("quality.generate_report", _run_quality)

    return scheduler
