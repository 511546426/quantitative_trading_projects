"""
每日收盘后自动更新脚本。

功能:
    1. 同步股票列表 & 交易日历（每周一）
    2. 拉取当日 K 线 + 复权因子 → ClickHouse
    3. 拉取当日估值数据 → PostgreSQL
    4. 输出执行报告

用法:
    python scripts/daily_update.py            # 更新今天
    python scripts/daily_update.py 20260225   # 更新指定日期
    python scripts/daily_update.py --backfill 3  # 补最近3个交易日
"""
import sys
import logging
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data.common.config import Config
from data.common.calendar import TradingCalendar
from data.fetchers.tushare_fetcher import TushareFetcher
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter
from data.cleaners.reference_cleaner import ReferenceCleaner
from data.pipeline.daily_pipeline import DailyPipeline

LOG_FILE = Path(__file__).parent / "daily_update.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("daily_update")


def build_pipeline(cfg: Config) -> tuple[DailyPipeline, TradingCalendar]:
    """组装所有组件，返回 pipeline 和 calendar"""
    ch = ClickHouseWriter(
        host=cfg.get("database.clickhouse.host", "localhost"),
        port=int(cfg.get("database.clickhouse.port", 9000)),
        database="quant",
        user=cfg.get("database.clickhouse.user", "default"),
        password=cfg.get("database.clickhouse.password", ""),
    )
    ch.connect()
    ch.init_tables()

    pg = PostgresWriter(
        host=cfg.get("database.postgres.host", "localhost"),
        port=int(cfg.get("database.postgres.port", 5432)),
        database="quant",
        user=cfg.get("database.postgres.user", "postgres"),
        password=cfg.get("database.postgres.password", ""),
    )
    pg.connect()
    pg.init_tables()

    token = cfg.get("sources.tushare.token", "")
    fetcher = TushareFetcher(token=token)
    fetcher.connect()

    cal_df = fetcher.get_trade_calendar(
        exchange="SSE", start_date="20000101", end_date="20301231"
    )
    calendar = TradingCalendar.from_dataframe(cal_df)

    ref_cleaner = ReferenceCleaner()
    stock_df = fetcher.get_stock_list()
    stock_cleaned, _ = ref_cleaner.clean(stock_df)

    pipeline = DailyPipeline(
        fetcher=fetcher,
        ch_writer=ch,
        pg_writer=pg,
        calendar=calendar,
        stock_info=stock_cleaned,
    )

    return pipeline, calendar


def run_daily(pipeline: DailyPipeline, calendar: TradingCalendar, target_date: str):
    """执行单日更新"""
    today_weekday = datetime.strptime(target_date, "%Y%m%d").weekday()
    if today_weekday == 0:
        logger.info("周一 — 同步股票列表和交易日历")
        try:
            n = pipeline.sync_stock_list()
            logger.info("股票列表同步: %d 条", n)
        except Exception as e:
            logger.error("股票列表同步失败: %s", e)
        try:
            n = pipeline.sync_trade_calendar()
            logger.info("交易日历同步: %d 条", n)
        except Exception as e:
            logger.error("交易日历同步失败: %s", e)

    report = pipeline.run(target_date)
    print(report.summary())
    return report


def main():
    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
    pipeline, calendar = build_pipeline(cfg)

    args = sys.argv[1:]

    if len(args) >= 2 and args[0] == "--backfill":
        n_days = int(args[1])
        today = date.today()
        recent_dates = []
        d = today
        while len(recent_dates) < n_days:
            if calendar.is_trade_date(d.strftime("%Y%m%d")):
                recent_dates.append(d.strftime("%Y%m%d"))
            d = date.fromordinal(d.toordinal() - 1)
        recent_dates.reverse()

        logger.info("补更新最近 %d 个交易日: %s", n_days, recent_dates)
        for td in recent_dates:
            run_daily(pipeline, calendar, td)
    elif len(args) == 1 and args[0].isdigit():
        target = args[0]
        logger.info("更新指定日期: %s", target)
        run_daily(pipeline, calendar, target)
    else:
        target = date.today().strftime("%Y%m%d")
        logger.info("更新今日: %s", target)
        run_daily(pipeline, calendar, target)

    logger.info("每日更新流程结束")


if __name__ == "__main__":
    main()
