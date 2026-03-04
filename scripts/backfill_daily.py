"""
历史日K线回填脚本 — 2010年至今全市场日K线 + 复权因子 + 估值数据

特性:
    - 断点续传：记录已完成日期，中断后可恢复
    - 限速控制：每次请求间隔 0.4 秒，不超频
    - 进度显示：实时打印进度和预估剩余时间
"""
import sys
import time
import json
import logging
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data.common.config import Config
from data.common.calendar import TradingCalendar
from data.fetchers.tushare_fetcher import TushareFetcher
from data.writers.clickhouse_writer import ClickHouseWriter
from data.writers.postgres_writer import PostgresWriter
from data.cleaners.price_cleaner import PriceCleaner
from data.cleaners.reference_cleaner import ReferenceCleaner

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

CHECKPOINT_FILE = Path(__file__).parent / "backfill_checkpoint.json"
START_DATE = "20100104"
SLEEP_BETWEEN = 0.35


def load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text())
        return set(data.get("completed_dates", []))
    return set()


def save_checkpoint(completed: set[str]):
    CHECKPOINT_FILE.write_text(json.dumps({
        "completed_dates": sorted(completed),
        "last_updated": datetime.now().isoformat(),
    }))


def main():
    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")

    ch = ClickHouseWriter(
        host=cfg.get("database.clickhouse.host", "localhost"),
        port=int(cfg.get("database.clickhouse.port", 9000)),
        database="quant",
        user=cfg.get("database.clickhouse.user", "default"),
        password=cfg.get("database.clickhouse.password", ""),
    )
    ch.connect()

    pg = PostgresWriter(
        host=cfg.get("database.postgres.host", "localhost"),
        port=int(cfg.get("database.postgres.port", 5432)),
        database="quant",
        user=cfg.get("database.postgres.user", "postgres"),
        password=cfg.get("database.postgres.password", ""),
    )
    pg.connect()

    token = cfg.get("sources.tushare.token", "")
    fetcher = TushareFetcher(token=token)
    fetcher.connect()

    cal_df = fetcher.get_trade_calendar(exchange="SSE", start_date="20000101", end_date="20301231")
    calendar = TradingCalendar.from_dataframe(cal_df)

    ref_cleaner = ReferenceCleaner()
    stock_df = fetcher.get_stock_list()
    stock_cleaned, _ = ref_cleaner.clean(stock_df)
    price_cleaner = PriceCleaner(stock_info=stock_cleaned)

    today_str = date.today().strftime("%Y%m%d")
    trade_dates = calendar.get_trade_dates(START_DATE, today_str)
    trade_date_strs = [d.strftime("%Y%m%d") for d in trade_dates]

    completed = load_checkpoint()
    todo = [d for d in trade_date_strs if d not in completed]

    total = len(trade_date_strs)
    done = len(completed)
    remaining = len(todo)

    print(f"{'='*60}")
    print(f"历史数据回填")
    print(f"{'='*60}")
    print(f"  日期范围:   {START_DATE} ~ {today_str}")
    print(f"  总交易日:   {total}")
    print(f"  已完成:     {done}")
    print(f"  待回填:     {remaining}")
    print(f"  预估时间:   {remaining * 0.8 / 60:.0f} 分钟")
    print(f"{'='*60}\n")

    if remaining == 0:
        print("✅ 所有日期已回填完毕！")
        return

    start_time = time.time()
    errors = []

    for i, td in enumerate(todo, 1):
        try:
            time.sleep(SLEEP_BETWEEN)
            daily = fetcher.get_daily_bars(trade_date=td)

            time.sleep(SLEEP_BETWEEN)
            adj = fetcher.get_adj_factor(trade_date=td)
            daily = daily.merge(
                adj[["ts_code", "trade_date", "adj_factor"]],
                on=["ts_code", "trade_date"],
                how="left",
            )

            cleaned, report = price_cleaner.clean(daily)
            ch.upsert(cleaned, "stock_daily", ["ts_code", "trade_date"])

            completed.add(td)
            if i % 10 == 0:
                save_checkpoint(completed)

            elapsed = time.time() - start_time
            speed = i / elapsed
            eta = (remaining - i) / speed / 60

            print(
                f"  [{done + i}/{total}] {td}  "
                f"{report.output_rows:>5} 条  "
                f"涨停 {report.details.get('limit_up_count', 0):>3}  "
                f"跌停 {report.details.get('limit_down_count', 0):>3}  "
                f"ETA {eta:.0f}min",
                flush=True,
            )

        except Exception as e:
            errors.append((td, str(e)))
            print(f"  [{done + i}/{total}] {td}  ❌ {str(e)[:60]}", flush=True)
            time.sleep(1)
            continue

    save_checkpoint(completed)

    elapsed_total = (time.time() - start_time) / 60
    print(f"\n{'='*60}")
    print(f"回填完成!")
    print(f"{'='*60}")
    print(f"  成功: {remaining - len(errors)} 天")
    print(f"  失败: {len(errors)} 天")
    print(f"  耗时: {elapsed_total:.1f} 分钟")

    if errors:
        print(f"\n失败日期:")
        for td, err in errors[:20]:
            print(f"  {td}: {err[:60]}")

    fetcher.close()
    ch.close()
    pg.close()


if __name__ == "__main__":
    main()
