#pragma once
#include "../core/position.h"
#include <vector>
#include <cmath>

namespace quant::oms {

// ────────────────────────────────────────────────────────────
// PnlSnapshot - 某时刻的 PnL 快照
// ────────────────────────────────────────────────────────────
struct PnlSnapshot {
    double nav              = 0.0;   // 净资产
    double unrealized_pnl   = 0.0;   // 持仓浮盈
    double realized_pnl     = 0.0;   // 今日已实现
    double total_pnl        = 0.0;   // unrealized + realized
    double daily_return     = 0.0;   // 今日收益率
    double max_drawdown     = 0.0;   // 最大回撤（负值）
    double peak_nav         = 0.0;   // 历史 NAV 峰值
    Nanos  timestamp_ns     = 0;
};

// ────────────────────────────────────────────────────────────
// PnlCalculator - 实时 PnL 计算器
//
// 从 PortfolioSnapshot 计算各类收益指标，
// 同时维护历史 NAV 序列以计算最大回撤。
// ────────────────────────────────────────────────────────────
class PnlCalculator {
public:
    explicit PnlCalculator(double initial_nav)
        : initial_nav_(initial_nav), peak_nav_(initial_nav) {}

    // 根据最新 PortfolioSnapshot 更新指标
    PnlSnapshot update(const PortfolioSnapshot& snap) {
        PnlSnapshot pnl;
        pnl.timestamp_ns   = snap.snapshot_ns;
        pnl.nav            = snap.nav();
        pnl.realized_pnl   = snap.realized_pnl;

        double unrealized = 0.0;
        for (const auto& [_, pos] : snap.positions) {
            unrealized += pos.unrealized_pnl();
        }
        pnl.unrealized_pnl = unrealized;
        pnl.total_pnl      = pnl.realized_pnl + pnl.unrealized_pnl;

        // 日收益率
        if (snap.daily_start_nav > 0) {
            pnl.daily_return = (pnl.nav - snap.daily_start_nav) / snap.daily_start_nav;
        }

        // 更新峰值 + 最大回撤
        if (pnl.nav > peak_nav_) peak_nav_ = pnl.nav;
        const double dd = (peak_nav_ > 0) ? (pnl.nav - peak_nav_) / peak_nav_ : 0.0;
        if (dd < max_drawdown_) max_drawdown_ = dd;

        pnl.peak_nav    = peak_nav_;
        pnl.max_drawdown = max_drawdown_;

        last_ = pnl;
        nav_history_.push_back(pnl.nav);

        return pnl;
    }

    [[nodiscard]] const PnlSnapshot& last() const noexcept { return last_; }

    // 计算年化收益率（简单复利，假设 252 交易日/年）
    [[nodiscard]] double annualized_return(int trading_days = 252) const noexcept {
        if (nav_history_.empty() || initial_nav_ <= 0) return 0.0;
        const double total_return = (last_.nav - initial_nav_) / initial_nav_;
        const int    days         = static_cast<int>(nav_history_.size());
        if (days <= 0) return 0.0;
        return std::pow(1.0 + total_return, static_cast<double>(trading_days) / days) - 1.0;
    }

    // Calmar 比率（年化收益 / |最大回撤|）
    [[nodiscard]] double calmar() const noexcept {
        if (max_drawdown_ >= 0) return 0.0;
        return annualized_return() / std::abs(max_drawdown_);
    }

private:
    double initial_nav_;
    double peak_nav_;
    double max_drawdown_ = 0.0;
    PnlSnapshot last_;
    std::vector<double> nav_history_;
};

} // namespace quant::oms
