"""
历史估值数据回填脚本 — PE/PB/PS/总市值/流通市值

存入 PostgreSQL daily_valuation 表
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
from data.writers.postgres_writer import PostgresWriter

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

CHECKPOINT_FILE = Path(__file__).parent / "backfill_valuation_checkpoint.json"
START_DATE = "20100104"
SLEEP_BETWEEN = 0.4


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

    cal_df = fetcher.get_trade_calendar(exchange="SSE", start_date="20000101", end_date="20301231")
    calendar = TradingCalendar.from_dataframe(cal_df)

    today_str = date.today().strftime("%Y%m%d")
    trade_dates = calendar.get_trade_dates(START_DATE, today_str)
    trade_date_strs = [d.strftime("%Y%m%d") for d in trade_dates]

    completed = load_checkpoint()
    todo = [d for d in trade_date_strs if d not in completed]

    total = len(trade_date_strs)
    done = len(completed)
    remaining = len(todo)

    print(f"{'='*60}")
    print(f"估值数据回填 (PE/PB/PS/市值)")
    print(f"{'='*60}")
    print(f"  日期范围:   {START_DATE} ~ {today_str}")
    print(f"  总交易日:   {total}")
    print(f"  已完成:     {done}")
    print(f"  待回填:     {remaining}")
    print(f"  预估时间:   {remaining * 0.5 / 60:.0f} 分钟")
    print(f"{'='*60}\n")

    if remaining == 0:
        print("✅ 所有日期已回填完毕！")
        return

    start_time = time.time()
    errors = []

    for i, td in enumerate(todo, 1):
        try:
            time.sleep(SLEEP_BETWEEN)
            val = fetcher.get_valuation(trade_date=td)
            pg.upsert(val, "daily_valuation", ["ts_code", "trade_date"])

            completed.add(td)
            if i % 10 == 0:
                save_checkpoint(completed)

            elapsed = time.time() - start_time
            speed = i / elapsed
            eta = (remaining - i) / speed / 60

            print(
                f"  [{done + i}/{total}] {td}  "
                f"{len(val):>5} 条  "
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
    print(f"估值回填完成!")
    print(f"{'='*60}")
    print(f"  成功: {remaining - len(errors)} 天")
    print(f"  失败: {len(errors)} 天")
    print(f"  耗时: {elapsed_total:.1f} 分钟")

    if errors:
        print(f"\n失败日期:")
        for td, err in errors[:20]:
            print(f"  {td}: {err[:60]}")

    fetcher.close()
    pg.close()


if __name__ == "__main__":
    main()
