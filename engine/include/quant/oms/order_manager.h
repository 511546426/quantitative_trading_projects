#pragma once
#include "../core/order.h"
#include "../core/trade.h"
#include "../infra/logger.h"
#include <unordered_map>
#include <vector>
#include <functional>
#include <mutex>

namespace quant::oms {

// ────────────────────────────────────────────────────────────
// 回调类型
// ────────────────────────────────────────────────────────────
using OnOrderUpdate = std::function<void(const Order&)>;
using OnTradeUpdate = std::function<void(const Trade&)>;

// ────────────────────────────────────────────────────────────
// OrderManager - 订单全生命周期管理
//
// 职责：
//   1. 接收 OrderRequest → 创建 Order 对象（分配 order_id）
//   2. 跟踪订单状态变化（New → Submitted → Partial/Filled/Cancelled）
//   3. 合并成交回报，更新 Order.filled_qty / avg_fill_price
//   4. 对外暴露查询接口（按 order_id / symbol / strategy_id）
// ────────────────────────────────────────────────────────────
class OrderManager {
public:
    OrderManager() = default;

    // ── 订单创建 ─────────────────────────────────────────────

    // 从策略 OrderRequest 创建订单，返回 order_id
    int64_t create_order(const OrderRequest& req);

    // ── 状态更新（由 Gateway 回调触发）─────────────────────

    void on_order_accepted(int64_t order_id);
    void on_order_rejected(int64_t order_id, const std::string& reason);
    void on_trade(const Trade& trade);
    void on_cancel_confirm(int64_t order_id);

    // ── 主动操作 ────────────────────────────────────────────

    // 标记订单为"待撤"并返回 order_id（具体撤单由 Gateway 执行）
    bool request_cancel(int64_t order_id);

    // 批量撤掉某 symbol 的所有活跃订单
    std::vector<int64_t> cancel_all(const Symbol& symbol);

    // ── 查询接口 ────────────────────────────────────────────

    const Order* find(int64_t order_id) const;
    Order*       find(int64_t order_id);

    // 返回所有活跃订单（New / Submitted / Partial）
    std::vector<const Order*> active_orders() const;

    // 按 strategy_id 查询活跃订单
    std::vector<const Order*> active_orders(StrategyId sid) const;

    size_t total_orders() const { return orders_.size(); }

    // ── 回调注册 ────────────────────────────────────────────

    void set_on_order_update(OnOrderUpdate cb) { on_order_update_ = std::move(cb); }
    void set_on_trade_update(OnTradeUpdate cb) { on_trade_update_ = std::move(cb); }

private:
    // order_id → Order
    std::unordered_map<int64_t, Order> orders_;

    OnOrderUpdate on_order_update_;
    OnTradeUpdate on_trade_update_;

    void notify_order(const Order& o) {
        if (on_order_update_) on_order_update_(o);
    }
};

} // namespace quant::oms
