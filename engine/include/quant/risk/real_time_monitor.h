#pragma once
#include "pre_trade_check.h"
#include "../core/position.h"
#include "../infra/logger.h"
#include <functional>
#include <atomic>

namespace quant::risk {

// 风控熔断触发回调
using OnBreakerTriggered = std::function<void(const char* reason)>;

// ────────────────────────────────────────────────────────────
// RealTimeMonitor - 实时风控监控（运行在独立线程）
//
// 定期（每秒或每 tick）检查：
//   1. 当前回撤是否超过阈值 → 触发熔断，撤全部持仓
//   2. 单股亏损是否超过止损线 → 发送清仓信号
//   3. 组合暴露是否超过上限
// ────────────────────────────────────────────────────────────
class RealTimeMonitor {
public:
    RealTimeMonitor(PreTradeChecker& checker, const RiskConfig& cfg)
        : checker_(checker), cfg_(cfg) {}

    // 每次行情更新后调用
    void on_portfolio_update(const PortfolioSnapshot& snap) {
        check_drawdown(snap);
        check_position_exposure(snap);
    }

    void set_on_breaker(OnBreakerTriggered cb) {
        on_breaker_ = std::move(cb);
    }

    [[nodiscard]] bool is_running() const noexcept { return running_; }
    void stop() noexcept { running_ = false; }

private:
    void check_drawdown(const PortfolioSnapshot& snap) {
        const double nav = snap.nav();
        if (nav > peak_nav_) peak_nav_ = nav;

        if (peak_nav_ <= 0) return;
        const double dd = (nav - peak_nav_) / peak_nav_;

        if (dd < -cfg_.max_drawdown_ratio && !checker_.is_breaker_on()) {
            const char* reason = "max drawdown exceeded";
            checker_.trigger_circuit_breaker(reason);
            if (on_breaker_) on_breaker_(reason);
        }
    }

    void check_position_exposure(const PortfolioSnapshot& snap) {
        const double nav = snap.nav();
        if (nav <= 0) return;
        const double exp = snap.total_position_ratio();
        if (exp > cfg_.max_total_position_ratio) {
            LOG_WARN("RealTimeMonitor: position exposure {:.1f}% > limit {:.1f}%",
                     exp * 100, cfg_.max_total_position_ratio * 100);
        }
    }

    PreTradeChecker& checker_;
    const RiskConfig& cfg_;
    OnBreakerTriggered on_breaker_;
    double peak_nav_ = 0.0;
    bool   running_  = true;
};

} // namespace quant::risk
