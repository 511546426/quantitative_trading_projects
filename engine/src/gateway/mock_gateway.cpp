#include "quant/gateway/base_gateway.h"
#include "quant/gateway/gateway_factory.h"
#include "quant/infra/logger.h"
#include "quant/infra/timer.h"
#include <unordered_map>
#include <thread>
#include <mutex>
#include <queue>
#include <atomic>
#include <cmath>

namespace quant::gateway {

// ────────────────────────────────────────────────────────────
// MockGateway - 内存撮合引擎（用于测试 / 策略联调）
//
// 行为：
//   - Market 单：立即以 last_price 全部成交
//   - Limit  单：价格符合时撮合（买 <= ask，卖 >= bid），否则挂单等待
//   - 模拟 A 股交易成本（佣金 + 印花税）
//   - 模拟网络延迟（configurable delay_ms）
// ────────────────────────────────────────────────────────────
class MockGateway : public IBaseGateway {
public:
    explicit MockGateway(int delay_ms = 0) : delay_ms_(delay_ms) {}

    std::string name() const override { return "MockGateway"; }

    bool connect() override {
        connected_ = true;
        LOG_INFO("MockGateway: connected");
        if (callback_) callback_->on_connected();
        return true;
    }

    void disconnect() override {
        connected_ = false;
        LOG_INFO("MockGateway: disconnected");
        if (callback_) callback_->on_disconnected();
    }

    bool is_connected() const override { return connected_; }

    // ── 行情更新（由测试驱动调用）────────────────────────

    void update_price(const Symbol& symbol, Price bid, Price ask,
                      Price high_limit = 0, Price low_limit = 0) {
        const std::string key(symbol_view(symbol));
        std::lock_guard lock(mtx_);
        auto& q = quotes_[key];
        q.bid        = bid;
        q.ask        = ask;
        q.last_price = (bid + ask) / 2.0;
        q.high_limit = high_limit;
        q.low_limit  = low_limit;

        // 尝试撮合挂单
        try_match_pending(symbol, q.last_price);
    }

    // ── IBaseGateway 接口 ────────────────────────────────

    int64_t send_order(const Order& order) override {
        if (!connected_) {
            LOG_WARN("MockGateway: not connected, rejecting order_id={}", order.order_id);
            if (callback_) callback_->on_order_rejected(order.order_id, "not connected");
            return order.order_id;
        }

        simulate_delay();

        const std::string key(symbol_view(order.symbol));

        // 接受
        if (callback_) callback_->on_order_accepted(order.order_id);

        std::lock_guard lock(mtx_);
        const auto& q = get_quote(key);

        if (order.type == OrderType::Market) {
            // 市价单：立即以 last_price 成交
            const Price price = (q.last_price > 0) ? q.last_price : order.limit_price;
            fill_order(order, price, order.target_qty);
        } else {
            // 限价单：检查是否能立即成交
            bool can_fill = false;
            if (order.side == Side::Buy  && q.ask > 0 && order.limit_price >= q.ask)
                can_fill = true;
            if (order.side == Side::Sell && q.bid > 0 && order.limit_price <= q.bid)
                can_fill = true;

            if (can_fill) {
                const Price fill_price = (order.side == Side::Buy) ? q.ask : q.bid;
                fill_order(order, fill_price, order.target_qty);
            } else {
                // 挂单等待
                pending_orders_[order.order_id] = order;
                LOG_DEBUG("MockGateway: order_id={} queued (limit not met)", order.order_id);
            }
        }
        return order.order_id;
    }

    bool cancel_order(int64_t order_id) override {
        simulate_delay();
        std::lock_guard lock(mtx_);
        auto it = pending_orders_.find(order_id);
        if (it == pending_orders_.end()) {
            if (callback_)
                callback_->on_cancel_rejected(order_id, "order not found or already filled");
            return false;
        }
        pending_orders_.erase(it);
        if (callback_) callback_->on_cancel_confirm(order_id);
        LOG_DEBUG("MockGateway: order_id={} cancelled", order_id);
        return true;
    }

    std::vector<Position> query_positions() override { return {}; }
    double query_cash()                             override { return 0.0; }
    bool   query_orders()                           override { return true; }

private:
    struct Quote {
        Price bid = 0, ask = 0, last_price = 0;
        Price high_limit = 0, low_limit = 0;
    };

    const Quote& get_quote(const std::string& sym) {
        static const Quote empty_quote{};
        auto it = quotes_.find(sym);
        return (it != quotes_.end()) ? it->second : empty_quote;
    }

    void fill_order(const Order& order, Price fill_price, Quantity qty) {
        if (!callback_) return;
        Trade t;
        t.trade_id   = trade_id_gen_.fetch_add(1, std::memory_order_relaxed);
        t.order_id   = order.order_id;
        t.strategy_id = order.strategy_id;
        t.symbol     = order.symbol;
        t.side       = order.side;
        t.fill_qty   = qty;
        t.fill_price = fill_price;
        t.fill_ns    = infra::Clock::now_ns();
        fill_costs(t);  // 计算佣金 + 印花税

        callback_->on_trade(t);

        LOG_INFO("MockGateway: FILL order_id={} {} qty={} price={:.4f} comm={:.2f}",
                 order.order_id,
                 order.side == Side::Buy ? "BUY" : "SELL",
                 qty, fill_price, t.commission);
    }

    // 尝试撮合挂单（行情更新时调用，调用方持锁）
    void try_match_pending(const Symbol& symbol, Price last_price) {
        std::vector<int64_t> to_fill;
        for (auto& [oid, o] : pending_orders_) {
            if (o.symbol != symbol) continue;
            bool match = false;
            if (o.side == Side::Buy  && last_price <= o.limit_price) match = true;
            if (o.side == Side::Sell && last_price >= o.limit_price) match = true;
            if (match) to_fill.push_back(oid);
        }
        for (int64_t oid : to_fill) {
            const Order& o = pending_orders_.at(oid);
            fill_order(o, last_price, o.target_qty);
            pending_orders_.erase(oid);
        }
    }

    void simulate_delay() {
        if (delay_ms_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
        }
    }

    std::atomic<bool>    connected_{false};
    std::atomic<int64_t> trade_id_gen_{1};
    int                  delay_ms_;
    std::mutex           mtx_;
    std::unordered_map<std::string, Quote>  quotes_;
    std::unordered_map<int64_t, Order>      pending_orders_;
};

// ────────────────────────────────────────────────────────────
// GatewayFactory 实现
// ────────────────────────────────────────────────────────────
std::unique_ptr<IBaseGateway> GatewayFactory::create(const std::string& type,
                                                      const std::string& /*config_path*/) {
    if (type == "mock") {
        return std::make_unique<MockGateway>();
    }
    throw std::runtime_error("Unknown gateway type: " + type
                             + " (supported: mock)");
}

} // namespace quant::gateway
