#include "quant/risk/pre_trade_check.h"
#include "quant/infra/logger.h"
#include <cmath>

namespace quant::risk {

RiskResult PreTradeChecker::check(Order& order,
                                  const CheckContext& ctx,
                                  const char** reject_reason) const noexcept {
    const char* dummy = nullptr;
    if (!reject_reason) reject_reason = &dummy;

    // 1. 熔断优先
    if (auto r = check_circuit_breaker(); r != RiskResult::Pass) {
        *reject_reason = "circuit breaker active";
        return r;
    }

    // 2. 停牌 / 涨跌停检查
    if (auto r = check_market_status(order, ctx, reject_reason); r != RiskResult::Pass)
        return r;

    // 3. 日内亏损熔断
    if (auto r = check_daily_loss(ctx, reject_reason); r != RiskResult::Pass)
        return r;

    // 4. 订单金额检查（最小金额 / 最大金额）
    if (auto r = check_order_amount(order, ctx, reject_reason); r != RiskResult::Pass)
        return r;

    // 5. 持仓上限检查（可能降量）
    if (auto r = check_position_limit(order, ctx, reject_reason); r != RiskResult::Pass)
        return r;

    // 6. 活跃订单数上限
    if (auto r = check_active_orders(ctx, reject_reason); r != RiskResult::Pass)
        return r;

    return RiskResult::Pass;
}

void PreTradeChecker::trigger_circuit_breaker(const char* reason) const noexcept {
    circuit_breaker_.store(true, std::memory_order_release);
    LOG_CRITICAL("PreTradeChecker: CIRCUIT BREAKER TRIGGERED: {}", reason);
}

void PreTradeChecker::reset_circuit_breaker() noexcept {
    circuit_breaker_.store(false, std::memory_order_release);
    LOG_WARN("PreTradeChecker: circuit breaker RESET by operator");
}

// ──────────────────────────────────────────────────────────

RiskResult PreTradeChecker::check_circuit_breaker() const noexcept {
    if (circuit_breaker_.load(std::memory_order_acquire))
        return RiskResult::Reject;
    return RiskResult::Pass;
}

RiskResult PreTradeChecker::check_market_status(const Order& o,
                                                 const CheckContext& ctx,
                                                 const char** reason) const noexcept {
    if (ctx.is_suspended) {
        *reason = "stock is suspended";
        return RiskResult::Reject;
    }
    if (o.side == Side::Buy && ctx.is_limit_up) {
        *reason = "cannot buy at limit-up price";
        return RiskResult::Reject;
    }
    if (o.side == Side::Sell && ctx.is_limit_down) {
        *reason = "cannot sell at limit-down price";
        return RiskResult::Reject;
    }
    if (!cfg_.allow_short && o.side == Side::Sell) {
        const auto* pos = ctx.snap.find(o.symbol);
        const Quantity available = pos ? pos->available() : 0;
        if (o.target_qty > available) {
            *reason = "short selling not allowed / insufficient position";
            return RiskResult::Reject;
        }
    }
    return RiskResult::Pass;
}

RiskResult PreTradeChecker::check_position_limit(Order& o,
                                                  const CheckContext& ctx,
                                                  const char** reason) const noexcept {
    if (o.side != Side::Buy) return RiskResult::Pass;

    const double nav = ctx.snap.nav();
    if (nav <= 0) {
        *reason = "invalid portfolio NAV";
        return RiskResult::Reject;
    }

    // 当前 symbol 已有持仓市值
    const std::string sym_str(symbol_view(o.symbol));
    double current_mv = 0.0;
    if (auto it = ctx.snap.positions.find(sym_str); it != ctx.snap.positions.end())
        current_mv = it->second.market_value();

    const double new_mv   = o.limit_price * static_cast<double>(o.target_qty);
    const double total_mv = current_mv + new_mv;
    const double ratio    = total_mv / nav;

    if (ratio > cfg_.max_single_position_ratio) {
        // 尝试降量
        const double allowed_mv  = cfg_.max_single_position_ratio * nav - current_mv;
        if (allowed_mv < cfg_.min_order_amount || ctx.last_price <= 0) {
            *reason = "single position limit exceeded";
            return RiskResult::Reject;
        }
        const Quantity allowed_qty = static_cast<Quantity>(allowed_mv / ctx.last_price / 100) * 100;
        if (allowed_qty < 100) {
            *reason = "single position limit: too small after reduce";
            return RiskResult::Reject;
        }
        LOG_WARN("PreTradeChecker: reduce qty {} -> {} (position limit)",
                 o.target_qty, allowed_qty);
        o.target_qty = allowed_qty;
        return RiskResult::Reduce;
    }

    // 总仓位上限
    if (ctx.snap.total_position_ratio() + new_mv / nav > cfg_.max_total_position_ratio) {
        *reason = "total position ratio limit exceeded";
        return RiskResult::Reject;
    }

    return RiskResult::Pass;
}

RiskResult PreTradeChecker::check_order_amount(const Order& o,
                                                const CheckContext& ctx,
                                                const char** reason) const noexcept {
    const double amount = o.limit_price * static_cast<double>(o.target_qty);

    if (amount < cfg_.min_order_amount) {
        *reason = "order amount below minimum";
        return RiskResult::Reject;
    }
    if (amount > cfg_.max_order_amount) {
        *reason = "order amount exceeds maximum";
        return RiskResult::Reject;
    }
    if (o.side == Side::Buy && amount > ctx.snap.cash) {
        *reason = "insufficient available cash";
        return RiskResult::Reject;
    }
    return RiskResult::Pass;
}

RiskResult PreTradeChecker::check_daily_loss(const CheckContext& ctx,
                                              const char** reason) const noexcept {
    const double loss_ratio = ctx.snap.daily_loss_ratio();
    if (loss_ratio < -cfg_.max_daily_loss_ratio) {
        *reason = "daily loss limit triggered";
        trigger_circuit_breaker("daily loss exceeded");
        return RiskResult::Reject;
    }
    return RiskResult::Pass;
}

RiskResult PreTradeChecker::check_active_orders(const CheckContext& ctx,
                                                  const char** reason) const noexcept {
    if (ctx.active_orders >= cfg_.max_active_orders_per_sym) {
        *reason = "too many active orders for this symbol";
        return RiskResult::Reject;
    }
    return RiskResult::Pass;
}

} // namespace quant::risk
