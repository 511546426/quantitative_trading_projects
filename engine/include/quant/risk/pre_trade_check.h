#pragma once
#include "../core/order.h"
#include "../core/position.h"
#include "../core/types.h"
#include <atomic>
#include <string>

namespace quant::risk {

// ────────────────────────────────────────────────────────────
// RiskConfig - 风控参数（可从 YAML 配置文件加载）
// ────────────────────────────────────────────────────────────
struct RiskConfig {
    double max_single_position_ratio  = 0.10;   // 单股最大仓位比例
    double max_total_position_ratio   = 0.95;   // 最大总仓位比例
    double max_order_amount           = 500'000.0; // 单笔最大金额（元）
    double max_daily_loss_ratio       = 0.03;   // 日内最大亏损比例（触发熔断）
    double max_drawdown_ratio         = 0.15;   // 历史最大回撤触发熔断
    double min_order_amount           = 100.0;  // 单笔最小金额（元），低于此不发单
    int    max_active_orders_per_sym  = 3;      // 单股最大活跃订单数
    bool   allow_short                = false;  // 是否允许做空（A股不允许）
};

// ────────────────────────────────────────────────────────────
// CheckContext - 单次风控检查所需的上下文信息
// ────────────────────────────────────────────────────────────
struct CheckContext {
    const PortfolioSnapshot& snap;
    double   total_capital   = 0.0;  // 总资本（含冻结）
    int      active_orders   = 0;    // 当前 symbol 活跃委托数
    Price    last_price      = 0.0;  // 当前最新价
    bool     is_limit_up     = false; // 是否涨停（不能买）
    bool     is_limit_down   = false; // 是否跌停（不能卖）
    bool     is_suspended    = false; // 是否停牌
};

// ────────────────────────────────────────────────────────────
// PreTradeChecker - 交易前风控
//
// 热路径设计：check() 必须 noexcept，无动态内存分配。
// 使用 reject_reason（固定长度字符数组）记录拒绝原因。
// ────────────────────────────────────────────────────────────
class PreTradeChecker {
public:
    explicit PreTradeChecker(RiskConfig cfg) : cfg_(cfg) {}

    // ── 主检查入口 ──────────────────────────────────────────

    // 检查订单是否通过风控。
    // 若返回 Reduce，order.target_qty 会被修改为允许的数量。
    // reject_reason 指向静态字符串（调用方不需要释放）。
    RiskResult check(Order& order,
                     const CheckContext& ctx,
                     const char** reject_reason = nullptr) const noexcept;

    // ── 全局熔断 ────────────────────────────────────────────

    // 触发熔断：所有后续 check() 均返回 Reject
    void trigger_circuit_breaker(const char* reason) const noexcept;

    // 重置熔断（需要人工干预确认）
    void reset_circuit_breaker() noexcept;

    [[nodiscard]] bool is_breaker_on() const noexcept {
        return circuit_breaker_.load(std::memory_order_acquire);
    }

    const RiskConfig& config() const noexcept { return cfg_; }

private:
    RiskResult check_circuit_breaker() const noexcept;
    RiskResult check_market_status(const Order& o, const CheckContext& ctx,
                                   const char** reason) const noexcept;
    RiskResult check_position_limit(Order& o, const CheckContext& ctx,
                                    const char** reason) const noexcept;
    RiskResult check_order_amount(const Order& o, const CheckContext& ctx,
                                  const char** reason) const noexcept;
    RiskResult check_daily_loss(const CheckContext& ctx,
                                const char** reason) const noexcept;
    RiskResult check_active_orders(const CheckContext& ctx,
                                   const char** reason) const noexcept;

    RiskConfig cfg_;
    mutable std::atomic<bool> circuit_breaker_{false};
};

} // namespace quant::risk
