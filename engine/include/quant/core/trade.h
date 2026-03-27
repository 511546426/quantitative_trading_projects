#pragma once
#include "types.h"

namespace quant {

// ────────────────────────────────────────────────────────────
// Trade - 单次成交记录（一笔订单可能对应多次成交）
// ────────────────────────────────────────────────────────────
struct alignas(64) Trade {
    int64_t    trade_id      = 0;
    int64_t    order_id      = 0;   // 关联订单
    int64_t    strategy_id   = STRATEGY_NONE;
    Nanos      fill_ns       = 0;   // 成交时间（纳秒）

    Price      fill_price    = 0.0;
    Amount     commission    = 0.0; // 佣金（元）
    Amount     stamp_duty    = 0.0; // 印花税（元，仅卖出）

    Quantity   fill_qty      = 0;
    Symbol     symbol        = {};

    Side       side          = Side::Buy;
    uint8_t    _pad[3]       = {};  // 对齐填充

    // ── 辅助方法 ──
    [[nodiscard]] Amount total_cost() const noexcept {
        return commission + stamp_duty;
    }

    [[nodiscard]] Amount gross_amount() const noexcept {
        return fill_price * static_cast<double>(fill_qty);
    }

    [[nodiscard]] Amount net_amount() const noexcept {
        if (side == Side::Buy)
            return gross_amount() + total_cost();
        else
            return gross_amount() - total_cost();
    }
};

static_assert(sizeof(Trade) <= 128, "Trade too large");

// ────────────────────────────────────────────────────────────
// A 股成本计算（近似）
//   买入：佣金万分之 2.5，最低 5 元
//   卖出：佣金万分之 2.5 + 印花税千分之 1
// ────────────────────────────────────────────────────────────
inline void fill_costs(Trade& t, double commission_rate = 0.00025,
                       double stamp_duty_rate = 0.001) noexcept {
    const Amount gross = t.fill_price * static_cast<double>(t.fill_qty);
    t.commission = std::max(gross * commission_rate, 5.0);
    t.stamp_duty = (t.side == Side::Sell) ? gross * stamp_duty_rate : 0.0;
}

} // namespace quant
