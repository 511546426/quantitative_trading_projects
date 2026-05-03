# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A-share (Chinese stock market) quantitative trading system with three layers: Python data pipeline & strategy research, C++ execution engine, and Python live-trading bridge (QMT). Research and execution are separated — backtests run entirely in Python; live trading sends signals via Protobuf+ZMQ to the C++ engine, which routes orders through a QMT adapter.

## Architecture

```
data/ (Python)         — data pipeline: Tushare/Akshare/Baostock fetchers → cleaners → ClickHouse/PostgreSQL/Redis
strategy/ (Python)     — backtest framework (metrics, visualizer, vectorized/event-driven backtest), factor library, example strategies
engine/ (C++17)        — ZMQ signal receiver, OMS (order/position), pre-trade risk checks, mock gateway. CMake+Conan build.
execution/ (Python)    — live trading: QMT adapter, ZMQ bridge, TWAP algo, risk, portfolio, monitoring
ui/ (Python+React)     — Streamlit dashboard + FastAPI backend + React ops console
scripts/ (Python)      — historical backfill scripts (daily, index, valuation) with checkpointing
```

### Data Flow

```
External (Tushare / QMT xtquant)
  → data/ fetchers → cleaners → writers → ClickHouse (stock_daily, index_daily)
                                         → PostgreSQL (daily_valuation, stock_info, trade_calendar)
  → strategy/ reads from ClickHouse/PostgreSQL for backtest
  → strategy/ regimemodel exports weights → Protobuf + ZMQ → engine/ (C++)
  → engine/ runs OMS/risk → OrderCommand → ZMQ → execution/ QMT adapter → broker
```

### Key Config Files

- `data/config/settings.yaml` — database connections, pipeline settings
- `data/config/sources.yaml` — data source definitions
- `data/config/schedules.yaml` — scheduler cron config
- `execution/config.yaml` — live trading config (gateway type, ZMQ endpoints, algo params)
- `strategy/config.py` — strategy parameters (factor weights, universe filter, stop-loss, leverage)
- `.env` — secrets (Tushare token, DB passwords)

## Common Commands

### Environment Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in TUSHARE_TOKEN and DB passwords
```

### Data Pipeline (via ops.sh)

```bash
./ops.sh start-db                # start ClickHouse + PostgreSQL + Redis (Docker)
./ops.sh daily [YYYYMMDD]        # daily incremental update (runs at 15:30+)
./ops.sh backfill-daily          # historical daily bar backfill (checkpointed)
./ops.sh backfill-valuation      # historical valuation backfill (checkpointed)
./ops.sh backfill-index [START] [END]  # index daily backfill
./ops.sh logs daily              # tail daily update logs
./ops.sh status                  # check container/port status
```

### Backtest & Research

```bash
PYTHONPATH=. python strategy/examples/regime_switching_strategy.py   # main multi-factor v4.1 strategy
PYTHONPATH=. python strategy/examples/momentum_strategy.py
PYTHONPATH=. python strategy/examples/factor_research.py
```

### Web UI

```bash
./ops.sh web               # Streamlit dashboard (port 8501)
./ops.sh web-pro           # FastAPI backend (port 8787) — serves React build at /
cd ui/ops-console && npm install && npm run dev   # React dev server (Vite, port 5173)
```

### Tests

```bash
python -m pytest tests/                    # run all tests
python -m pytest tests/test_metrics.py     # single test file
python -m pytest tests/ -k "drawdown"      # run tests matching keyword
python -m pytest tests/ --cov              # with coverage
```

### C++ Engine Build

```bash
cd engine
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j"$(nproc)"
```

### Docker Full Stack

```bash
docker compose up -d          # start all services (clickhouse, postgres, redis, backend, frontend)
docker compose logs -f        # follow logs
docker compose exec backend python scripts/backfill_daily.py   # run backfill in container
```

### Production React Build

```bash
cd ui/ops-console && npm run build   # produces dist/ served by FastAPI
```

## Key Patterns

- All Python scripts use `PYTHONPATH=.` or `python -m` style execution to resolve package imports
- Database connections are singletons managed by `data.common.db` (get_ch/get_pg/get_redis)
- Backtest strategies are standalone files in `strategy/examples/` — they import shared libs from `strategy/backtest/`, `strategy/factors/`, etc.
- The main strategy (`regime_switching_strategy.py`) exports `run_regime_model_for_web` for the FastAPI endpoint and `weights_from_trading_panel` for the execution layer
- Historical data backfill scripts use checkpoint JSON files to track progress incrementally
- ZMQ IPC endpoints connect the C++ engine and Python execution layer (signals, orders, fills, status)
- Protobuf message definitions live in `engine/proto/signal.proto` — shared between C++ and Python
