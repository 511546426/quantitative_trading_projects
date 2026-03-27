#pragma once
#include <cstdint>
#include <string_view>
#include <array>
#include <algorithm>

namespace quant {

// ────────────────────────────────────────────────────────────
// 基础数值类型别名
// ────────────────────────────────────────────────────────────
using Price    = double;
using Quantity = int64_t;   // 股数（以股为单位）
using Amount   = double;    // 金额（元）
using Nanos    = int64_t;   // 纳秒时间戳

// ────────────────────────────────────────────────────────────
// 股票代码：固定长度数组，避免堆分配
// 格式：000001.SZ / 600000.SH
// ────────────────────────────────────────────────────────────
static constexpr size_t SYMBOL_LEN = 12;
using Symbol = std::array<char, SYMBOL_LEN>;

inline Symbol make_symbol(std::string_view s) noexcept {
    Symbol sym{};
    size_t n = std::min(s.size(), SYMBOL_LEN - 1);
    std::copy_n(s.data(), n, sym.data());
    return sym;
}

inline std::string_view symbol_view(const Symbol& sym) noexcept {
    return std::string_view(sym.data());
}

// ────────────────────────────────────────────────────────────
// 策略 ID
// ────────────────────────────────────────────────────────────
using StrategyId = int32_t;
static constexpr StrategyId STRATEGY_NONE = 0;

// ────────────────────────────────────────────────────────────
// 交易所代码
// ────────────────────────────────────────────────────────────
enum class Exchange : uint8_t {
    UNKNOWN = 0,
    SSE     = 1,   // 上交所 .SH
    SZSE    = 2,   // 深交所 .SZ
    BSE     = 3,   // 北交所 .BJ
};

inline Exchange exchange_from_symbol(const Symbol& sym) noexcept {
    std::string_view sv = symbol_view(sym);
    if (sv.ends_with(".SH")) return Exchange::SSE;
    if (sv.ends_with(".SZ")) return Exchange::SZSE;
    if (sv.ends_with(".BJ")) return Exchange::BSE;
    return Exchange::UNKNOWN;
}

// ────────────────────────────────────────────────────────────
// 方向：买 / 卖
// ────────────────────────────────────────────────────────────
enum class Side : uint8_t {
    Buy  = 0,
    Sell = 1,
};

// ────────────────────────────────────────────────────────────
// 订单类型
// ────────────────────────────────────────────────────────────
enum class OrderType : uint8_t {
    Market = 0,   // 市价单（尽量成交）
    Limit  = 1,   // 限价单
    FAK    = 2,   // Fill And Kill（部分成交后撤）
    FOK    = 3,   // Fill Or Kill（全部成交或全部撤）
};

// ────────────────────────────────────────────────────────────
// 订单状态
// ────────────────────────────────────────────────────────────
enum class OrderStatus : uint8_t {
    New       = 0,
    Submitted = 1,
    Partial   = 2,
    Filled    = 3,
    Cancelled = 4,
    Rejected  = 5,
};

// ────────────────────────────────────────────────────────────
// 风控决策结果
// ────────────────────────────────────────────────────────────
enum class RiskResult : uint8_t {
    Pass   = 0,
    Reject = 1,
    Reduce = 2,   // 降低数量后通过
};

} // namespace quant
