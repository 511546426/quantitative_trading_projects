#pragma once
#include "../core/order.h"
#include "../core/trade.h"
#include "../core/position.h"
#include "../core/bar.h"
#include <string>
#include <vector>

namespace quant::gateway {

// ────────────────────────────────────────────────────────────
// IGatewayCallback - 由引擎实现，gateway 通过此接口回调
// ────────────────────────────────────────────────────────────
class IGatewayCallback {
public:
    virtual ~IGatewayCallback() = default;

    // 订单被交易所接受（返回交易所分配的 exchange_order_id）
    virtual void on_order_accepted(int64_t order_id) = 0;

    // 订单被交易所拒绝
    virtual void on_order_rejected(int64_t order_id, const std::string& reason) = 0;

    // 撤单确认
    virtual void on_cancel_confirm(int64_t order_id) = 0;

    // 撤单失败（如订单已成交）
    virtual void on_cancel_rejected(int64_t order_id, const std::string& reason) = 0;

    // 成交回报
    virtual void on_trade(const Trade& trade) = 0;

    // 行情 Tick 推送（可选实现）
    virtual void on_tick(const Tick& tick) {}

    // 连接状态变化
    virtual void on_connected()    {}
    virtual void on_disconnected() {}

    // Gateway 错误
    virtual void on_error(int error_code, const std::string& msg) = 0;
};

// ────────────────────────────────────────────────────────────
// IBaseGateway - 抽象 Gateway 接口
//
// 设计原则：
//   面向接口编程，支持热替换 Mock / XTP / QMT 等实现。
//   所有操作均为异步：发单后通过回调通知结果。
// ────────────────────────────────────────────────────────────
class IBaseGateway {
public:
    virtual ~IBaseGateway() = default;

    // ── 连接管理 ────────────────────────────────────────────

    virtual bool connect()                 = 0;
    virtual void disconnect()              = 0;
    virtual bool is_connected() const      = 0;
    virtual std::string name() const       = 0;

    // ── 订单操作（异步，结果通过 callback 回调）────────────

    // 发送订单，返回本地 order_id（即 Order.order_id）
    virtual int64_t send_order(const Order& order) = 0;

    // 撤单请求
    virtual bool cancel_order(int64_t order_id) = 0;

    // ── 同步查询（连接成功后可调用）────────────────────────

    virtual std::vector<Position> query_positions() = 0;
    virtual double                query_cash()      = 0;
    virtual bool                  query_orders()    = 0;  // 查询当日委托

    // ── 回调注册 ────────────────────────────────────────────

    void set_callback(IGatewayCallback* cb) { callback_ = cb; }

    // ── 心跳（部分 gateway 需要定期发心跳）─────────────────

    virtual void heartbeat() {}

protected:
    IGatewayCallback* callback_ = nullptr;
};

} // namespace quant::gateway
