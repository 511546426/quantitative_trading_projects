#pragma once
#include "types.h"

namespace quant {

// ────────────────────────────────────────────────────────────
// Bar - K 线（用于行情推送，策略信号计算）
// ────────────────────────────────────────────────────────────
struct alignas(64) Bar {
    Nanos    timestamp_ns = 0;   // Bar 结束时间（纳秒）
    Price    open         = 0.0;
    Price    high         = 0.0;
    Price    low          = 0.0;
    Price    close        = 0.0;
    Amount   volume       = 0.0; // 成交量（股）
    Amount   amount       = 0.0; // 成交额（元）
    Symbol   symbol       = {};
    int32_t  period_sec   = 0;   // Bar 周期（86400 = 日线）
    uint32_t _pad         = 0;
};

static_assert(sizeof(Bar) <= 128, "Bar too large");

// ────────────────────────────────────────────────────────────
// Tick - 实时行情（盘中使用）
// ────────────────────────────────────────────────────────────
struct alignas(64) Tick {
    Nanos    timestamp_ns = 0;
    Price    last_price   = 0.0;
    Price    bid1         = 0.0;
    Price    ask1         = 0.0;
    Quantity bid1_qty     = 0;
    Quantity ask1_qty     = 0;
    Amount   volume       = 0.0;
    Amount   amount       = 0.0;
    Price    high_limit   = 0.0;   // 涨停价
    Price    low_limit    = 0.0;   // 跌停价
    Symbol   symbol       = {};
    uint32_t _pad         = 0;
};

static_assert(sizeof(Tick) <= 128, "Tick too large");

} // namespace quant
