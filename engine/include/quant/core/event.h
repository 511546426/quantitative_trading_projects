#pragma once
#include "order.h"
#include "trade.h"
#include "bar.h"
#include <variant>
#include <cstring>

namespace quant {

// ────────────────────────────────────────────────────────────
// 事件类型标签
// ────────────────────────────────────────────────────────────
enum class EventType : uint8_t {
    None            = 0,
    Tick            = 1,   // 行情 Tick
    Bar             = 2,   // K 线
    OrderRequest    = 3,   // 策略发出的订单请求
    OrderAccepted   = 4,   // 交易所接受订单
    OrderRejected   = 5,   // 交易所拒绝订单
    TradeUpdate     = 6,   // 成交回报
    CancelRequest   = 7,   // 撤单请求
    CancelConfirm   = 8,   // 撤单确认
    RiskBreaker     = 9,   // 风控熔断
    Shutdown        = 255, // 关闭引擎
};

// ────────────────────────────────────────────────────────────
// RiskBreakerEvent - 风控熔断事件
// ────────────────────────────────────────────────────────────
struct RiskBreakerEvent {
    Nanos      triggered_ns = 0;
    char       reason[48]   = {};  // 触发原因（固定长度，无堆分配）
};

// ────────────────────────────────────────────────────────────
// OrderRejectedEvent
// ────────────────────────────────────────────────────────────
struct OrderRejectedEvent {
    int64_t order_id = 0;
    char    reason[56] = {};
};

// ────────────────────────────────────────────────────────────
// CancelRequestEvent
// ────────────────────────────────────────────────────────────
struct CancelRequestEvent {
    int64_t order_id = 0;
    Nanos   ts_ns    = 0;
};

// ────────────────────────────────────────────────────────────
// Event - 引擎内部流通的事件包
//
// 使用 std::variant 避免继承 + 虚函数，允许内联处理。
// 注意：variant 大小 = max(sizeof(T)) + discriminant
// ────────────────────────────────────────────────────────────
struct Event {
    EventType type = EventType::None;
    // 1 字节 type + 变长 payload
    std::variant<
        std::monostate,
        Tick,
        Bar,
        OrderRequest,
        Order,
        Trade,
        OrderRejectedEvent,
        CancelRequestEvent,
        RiskBreakerEvent
    > payload;

    // ── 工厂方法 ──
    static Event make_order_request(OrderRequest req) {
        return {EventType::OrderRequest, std::move(req)};
    }
    static Event make_trade(Trade trade) {
        return {EventType::TradeUpdate, std::move(trade)};
    }
    static Event make_tick(Tick tick) {
        return {EventType::Tick, std::move(tick)};
    }
    static Event make_shutdown() {
        return {EventType::Shutdown, std::monostate{}};
    }
    static Event make_risk_breaker(const char* reason) {
        RiskBreakerEvent e;
        e.triggered_ns = 0; // caller should set
        std::strncpy(e.reason, reason, sizeof(e.reason) - 1);
        return {EventType::RiskBreaker, e};
    }
};

} // namespace quant
