#include "quant/oms/order_manager.h"
#include "quant/infra/timer.h"

namespace quant::oms {

int64_t OrderManager::create_order(const OrderRequest& req) {
    Order o;
    o.order_id    = OrderIdGen::next();
    o.strategy_id = req.strategy_id;
    o.symbol      = req.symbol;
    o.side        = req.side;
    o.type        = req.type;
    o.limit_price = req.price;
    o.target_qty  = req.qty;
    o.filled_qty  = 0;
    o.status      = OrderStatus::New;
    o.created_ns  = infra::Clock::now_ns();

    const int64_t oid = o.order_id;
    orders_[oid] = o;
    notify_order(orders_[oid]);

    LOG_INFO("OrderManager: created order_id={} symbol={} side={} qty={} price={}",
             oid,
             symbol_view(o.symbol),
             o.side == Side::Buy ? "BUY" : "SELL",
             o.target_qty,
             o.limit_price);

    return oid;
}

void OrderManager::on_order_accepted(int64_t order_id) {
    auto* o = find(order_id);
    if (!o) {
        LOG_WARN("OrderManager: accepted unknown order_id={}", order_id);
        return;
    }
    o->status       = OrderStatus::Submitted;
    o->submitted_ns = infra::Clock::now_ns();
    notify_order(*o);
}

void OrderManager::on_order_rejected(int64_t order_id, const std::string& reason) {
    auto* o = find(order_id);
    if (!o) {
        LOG_WARN("OrderManager: rejected unknown order_id={}", order_id);
        return;
    }
    o->status = OrderStatus::Rejected;
    notify_order(*o);
    LOG_WARN("OrderManager: order_id={} REJECTED reason={}", order_id, reason);
}

void OrderManager::on_trade(const Trade& trade) {
    auto* o = find(trade.order_id);
    if (!o) {
        LOG_WARN("OrderManager: trade for unknown order_id={}", trade.order_id);
        return;
    }

    // 更新平均成交价（加权平均）
    const double prev_amount = o->avg_fill_price * static_cast<double>(o->filled_qty);
    const double new_amount  = trade.fill_price  * static_cast<double>(trade.fill_qty);
    o->filled_qty    += trade.fill_qty;
    if (o->filled_qty > 0) {
        o->avg_fill_price = (prev_amount + new_amount)
                          / static_cast<double>(o->filled_qty);
    }

    if (o->filled_qty >= o->target_qty) {
        o->status = OrderStatus::Filled;
    } else {
        o->status = OrderStatus::Partial;
    }

    notify_order(*o);
    if (on_trade_update_) on_trade_update_(trade);

    LOG_INFO("OrderManager: trade order_id={} fill_qty={} fill_price={:.4f} status={}",
             trade.order_id,
             trade.fill_qty,
             trade.fill_price,
             static_cast<int>(o->status));
}

void OrderManager::on_cancel_confirm(int64_t order_id) {
    auto* o = find(order_id);
    if (!o) return;
    o->status = OrderStatus::Cancelled;
    notify_order(*o);
    LOG_INFO("OrderManager: order_id={} CANCELLED filled={}/{}",
             order_id, o->filled_qty, o->target_qty);
}

bool OrderManager::request_cancel(int64_t order_id) {
    const auto* o = find(order_id);
    if (!o || o->is_done()) return false;
    LOG_DEBUG("OrderManager: cancel requested for order_id={}", order_id);
    return true;
}

std::vector<int64_t> OrderManager::cancel_all(const Symbol& symbol) {
    std::vector<int64_t> ids;
    for (auto& [oid, o] : orders_) {
        if (o.symbol == symbol && o.is_active()) {
            ids.push_back(oid);
        }
    }
    return ids;
}

const Order* OrderManager::find(int64_t order_id) const {
    auto it = orders_.find(order_id);
    return it != orders_.end() ? &it->second : nullptr;
}

Order* OrderManager::find(int64_t order_id) {
    auto it = orders_.find(order_id);
    return it != orders_.end() ? &it->second : nullptr;
}

std::vector<const Order*> OrderManager::active_orders() const {
    std::vector<const Order*> result;
    for (const auto& [_, o] : orders_) {
        if (o.is_active()) result.push_back(&o);
    }
    return result;
}

std::vector<const Order*> OrderManager::active_orders(StrategyId sid) const {
    std::vector<const Order*> result;
    for (const auto& [_, o] : orders_) {
        if (o.strategy_id == sid && o.is_active())
            result.push_back(&o);
    }
    return result;
}

} // namespace quant::oms
