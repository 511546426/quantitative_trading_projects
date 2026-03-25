"""
指数日线历史回填 → ClickHouse index_daily

与 DailyPipeline._update_index / BackfillPipeline._backfill_index 一致：
  000001.SH、000300.SH、000905.SH，按交易日逐日拉取（与股票 backfill 区间对齐）。

用法:
  python scripts/backfill_index.py
  python scripts/backfill_index.py 20200101 20260304

断点续传：PostgreSQL 表 backfill_checkpoint，task_name=backfill_index
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data.common.config import Config
from data.common.calendar import TradingCalendar
from data.fetchers.tushare_fetcher import TushareFetcher
from data.pipeline.backfill_pipeline import BackfillPipeline
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backfill_index")

DEFAULT_START = "20200101"


def main() -> None:
    parser = argparse.ArgumentParser(description="回填 index_daily（上证/沪深300/中证500）")
    parser.add_argument(
        "start",
        nargs="?",
        default=DEFAULT_START,
        help=f"起始 YYYYMMDD（默认 {DEFAULT_START}）",
    )
    parser.add_argument(
        "end",
        nargs="?",
        default=date.today().strftime("%Y%m%d"),
        help="结束 YYYYMMDD（默认今日）",
    )
    args = parser.parse_args()
    start_date, end_date = args.start, args.end

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")

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

    pipeline = BackfillPipeline(
        fetcher=fetcher,
        ch_writer=ch,
        pg_writer=pg,
        calendar=calendar,
        stock_info=None,
    )

    logger.info("指数回填: %s ~ %s，写入表 index_daily", start_date, end_date)
    pipeline.run(
        start_date=start_date,
        end_date=end_date,
        data_types=["index"],
        task_name="backfill_index",
    )

    fetcher.close()
    ch.close()
    pg.close()
    print(f"[OK] index_daily 回填完成（区间 {start_date} ~ {end_date}）")


if __name__ == "__main__":
    main()
