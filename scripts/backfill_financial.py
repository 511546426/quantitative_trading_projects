"""
财务指标历史回填脚本 — ROE / ROA / 毛利率 / 营收增长率

数据来源：Tushare Pro → fina_indicator（每股 1 次请求）
存入表：PostgreSQL financial_indicator
冲突策略：ON CONFLICT (ts_code, end_date) DO UPDATE

运行方式：
    python3 scripts/backfill_financial.py           # 全量（全A股）
    python3 scripts/backfill_financial.py --resume  # 断点续传
    python3 scripts/backfill_financial.py --test    # 仅测试前5只股票

预计耗时：7000只 × 0.35s/只 ≈ 约 40 分钟
"""
import sys
import time
import json
import logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data.common.config import Config
from data.fetchers.tushare_fetcher import TushareFetcher
from data.writers.postgres_writer import PostgresWriter

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt= "%H:%M:%S",
)
logger = logging.getLogger("backfill_financial")

CHECKPOINT_FILE = Path(__file__).parent / "backfill_financial_checkpoint.json"
SLEEP_BETWEEN   = 0.35      # 每次请求间隔（秒），2000积分上限约 200次/分钟
BATCH_SAVE      = 100       # 每处理多少只股票保存一次 checkpoint


# ─── Checkpoint ───────────────────────────────────────────────────────────────

def load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text())
        done = set(data.get("completed", []))
        logger.info("断点续传：已完成 %d 只股票", len(done))
        return done
    return set()


def save_checkpoint(completed: set[str]):
    CHECKPOINT_FILE.write_text(json.dumps({
        "completed":    sorted(completed),
        "last_updated": datetime.now().isoformat(),
        "count":        len(completed),
    }, ensure_ascii=False, indent=2))


# ─── 主流程 ───────────────────────────────────────────────────────────────────

def main():
    test_mode  = "--test"   in sys.argv
    resume     = "--resume" in sys.argv or CHECKPOINT_FILE.exists()

    cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")

    pg = PostgresWriter(
        host     = cfg.get("database.postgres.host", "localhost"),
        port     = int(cfg.get("database.postgres.port", 5432)),
        database = "quant",
        user     = cfg.get("database.postgres.user", "postgres"),
        password = cfg.get("database.postgres.password", ""),
    )
    pg.connect()
    pg.init_tables()

    token   = cfg.get("sources.tushare.token", "")
    fetcher = TushareFetcher(token=token)
    fetcher.connect()

    # ── 获取全部股票列表 ──────────────────────────────────────────────────────
    # 只拉 A 股个股（.SZ / .SH 普通股票），排除指数和其他品种
    # A股个股代码规则（精确号段，排除可转债/ETF/指数/基金）：
    #   沪市个股：600xxx / 601xxx / 603xxx / 605xxx / 688xxx（科创板）
    #   深市主板：000xxx / 001xxx / 002xxx / 003xxx
    #   深市创业板：300xxx / 301xxx
    rows = pg.execute_query("""
        SELECT ts_code FROM stock_info
        WHERE (
            ts_code ~ '^60[0135][0-9]{3}\\.SH$'        -- 沪市主板 600/601/603/605
         OR ts_code ~ '^688[0-9]{3}\\.SH$'              -- 科创板
         OR ts_code ~ '^00[0-3][0-9]{3}\\.SZ$'          -- 深市主板 000/001/002/003
         OR ts_code ~ '^30[01][0-9]{3}\\.SZ$'            -- 深市创业板 300/301
        )
        AND is_delisted = FALSE
        ORDER BY ts_code
    """)
    all_codes = [r[0] for r in rows]
    logger.info("股票池共 %d 只", len(all_codes))

    if test_mode:
        all_codes = all_codes[:5]
        logger.info("测试模式：仅处理前 5 只")

    # ── 断点续传 ──────────────────────────────────────────────────────────────
    completed  = load_checkpoint() if resume else set()
    todo_codes = [c for c in all_codes if c not in completed]
    logger.info("待处理: %d 只（已完成: %d 只）", len(todo_codes), len(completed))

    # ── 逐股拉取 ─────────────────────────────────────────────────────────────
    success = fail = skip = 0
    t0 = time.time()

    for i, ts_code in enumerate(todo_codes, 1):
        try:
            df = fetcher.get_financial_indicator(ts_code=ts_code)

            if df is None or df.empty:
                skip += 1
                completed.add(ts_code)
                continue

            rows_written = pg.upsert(
                df,
                table         = "financial_indicator",
                conflict_keys = ["ts_code", "end_date"],
            )
            success += 1
            completed.add(ts_code)

        except Exception as e:
            fail += 1
            logger.warning("%-12s 失败: %s", ts_code, e)

        # 进度日志（每50只）
        if i % 50 == 0 or i == len(todo_codes):
            elapsed  = time.time() - t0
            rate     = i / elapsed * 60
            remaining = (len(todo_codes) - i) / (i / elapsed) / 60 if i > 0 else 0
            logger.info(
                "进度 %d/%d  成功=%d 失败=%d 跳过=%d  "
                "速率=%.0f只/分  预计剩余=%.0f分钟",
                i, len(todo_codes), success, fail, skip, rate, remaining
            )

        # 定期保存 checkpoint
        if i % BATCH_SAVE == 0:
            save_checkpoint(completed)

        time.sleep(SLEEP_BETWEEN)

    # ── 最终保存 checkpoint ───────────────────────────────────────────────────
    save_checkpoint(completed)

    elapsed_min = (time.time() - t0) / 60
    logger.info("=" * 55)
    logger.info("回填完成！耗时 %.1f 分钟", elapsed_min)
    logger.info("成功: %d  失败: %d  跳过(无数据): %d", success, fail, skip)

    # ── 验证入库结果 ──────────────────────────────────────────────────────────
    r = pg.execute_query(
        "SELECT COUNT(*), COUNT(DISTINCT ts_code), MIN(end_date), MAX(end_date) "
        "FROM financial_indicator"
    )
    logger.info("financial_indicator 表: %d 行, %d 只股票, %s ~ %s",
                r[0][0], r[0][1], r[0][2], r[0][3])

    pg.close()
    fetcher.close()


if __name__ == "__main__":
    main()
