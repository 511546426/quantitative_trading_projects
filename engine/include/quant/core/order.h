#pragma once
#include "types.h"
#include <atomic>

namespace quant {

// ────────────────────────────────────────────────────────────
// 全局订单 ID 生成器（线程安全）
// ────────────────────────────────────────────────────────────
class OrderIdGen {
public:
    static int64_t next() noexcept {
        static std::atomic<int64_t> counter{1};
        return counter.fetch_add(1, std::memory_order_relaxed);
    }
};

// ────────────────────────────────────────────────────────────
// Order - 核心订单结构
//
// 设计要点：
//   1. alignas(64)：一个对象不跨 cache line，避免 false sharing
//   2. 固定大小数组存股票代码，无堆分配
//   3. 成员按大小从大到小排列，减少 padding bytes
// ────────────────────────────────────────────────────────────
struct alignas(64) Order {
    // ── 8 字节 × 4 ──
    int64_t    order_id      = 0;
    int64_t    strategy_id   = STRATEGY_NONE;
    Nanos      created_ns    = 0;   // 引擎创建时间（纳秒）
    Nanos      submitted_ns  = 0;   // 发送到 gateway 时间

    // ── 8 字节 × 2 ──
    Price      limit_price   = 0.0; // 市价单为 0
    Price      avg_fill_price = 0.0;

    // ── 8 字节 × 2 ──
    Quantity   target_qty    = 0;
    Quantity   filled_qty    = 0;

    // ── 12 字节 ──
    Symbol     symbol        = {};

    // ── 3 字节 ──
    Side       side          = Side::Buy;
    OrderType  type          = OrderType::Limit;
    OrderStatus status       = OrderStatus::New;

    // padding 由编译器填充到 64 字节

    // ── 辅助方法 ──
    [[nodiscard]] Quantity remaining() const noexcept {
        return target_qty - filled_qty;
    }

    [[nodiscard]] bool is_active() const noexcept {
        return status == OrderStatus::New
            || status == OrderStatus::Submitted
            || status == OrderStatus::Partial;
    }

    [[nodiscard]] bool is_done() const noexcept {
        return !is_active();
    }

    [[nodiscard]] Amount notional() const noexcept {
        return static_cast<double>(target_qty) * limit_price;
    }
};

static_assert(sizeof(Order) <= 128, "Order too large");
static_assert(alignof(Order) == 64, "Order not cache-line aligned");

// ────────────────────────────────────────────────────────────
// OrderRequest - 策略层发出的"意图"，尚未经过风控
// ────────────────────────────────────────────────────────────
struct OrderRequest {
    Symbol      symbol;
    StrategyId  strategy_id  = STRATEGY_NONE;
    Quantity    qty          = 0;
    Price       price        = 0.0;
    Side        side         = Side::Buy;
    OrderType   type         = OrderType::Limit;
    Nanos       signal_ns    = 0;   // 信号生成时间（来自 Python）
};

} // namespace quant
