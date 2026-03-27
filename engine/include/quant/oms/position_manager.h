#pragma once
#include "../core/position.h"
#include "../core/trade.h"
#include "../infra/logger.h"
#include <functional>

namespace quant::oms {

using OnPositionUpdate = std::function<void(const Position&)>;

// ────────────────────────────────────────────────────────────
// PositionManager - 持仓状态管理
//
// 职责：
//   1. 维护 PortfolioSnapshot（现金 + 各股持仓）
//   2. 响应成交回报，更新持仓 / 均价 / 现金
//   3. 响应行情推送，更新最新价 / 浮盈
//   4. 提供查询接口（单股 / 全组合快照）
// ────────────────────────────────────────────────────────────
class PositionManager {
public:
    explicit PositionManager(Amount initial_cash) {
        snap_.cash            = initial_cash;
        snap_.daily_start_nav = initial_cash;
    }

    // ── 事件处理 ─────────────────────────────────────────────

    // 成交回报 → 更新持仓 + 现金
    void on_trade(const Trade& trade);

    // 行情更新 → 更新最新价（用于 PnL 计算）
    void on_price_update(const Symbol& symbol, Price price);

    // 每日开盘时重置日内 PnL 基准
    void on_day_start();

    // 冻结现金（买入委托发出时）
    bool freeze_cash(Amount amount) noexcept;

    // 解冻现金（撤单 / 拒单时）
    void unfreeze_cash(Amount amount) noexcept;

    // 冻结持仓（卖出委托发出时）
    bool freeze_position(const Symbol& symbol, Quantity qty) noexcept;

    // 解冻持仓（卖单撤单 / 拒单时）
    void unfreeze_position(const Symbol& symbol, Quantity qty) noexcept;

    // ── 查询接口 ────────────────────────────────────────────

    const PortfolioSnapshot& snapshot() const noexcept { return snap_; }
    PortfolioSnapshot& snapshot() noexcept { return snap_; }

    const Position* get_position(const Symbol& sym) const noexcept {
        return snap_.find(sym);
    }

    Amount nav()         const noexcept { return snap_.nav(); }
    Amount cash()        const noexcept { return snap_.cash; }
    Amount market_value() const noexcept { return snap_.market_value(); }

    // ── 回调 ────────────────────────────────────────────────

    void set_on_position_update(OnPositionUpdate cb) {
        on_position_update_ = std::move(cb);
    }

private:
    PortfolioSnapshot snap_;
    OnPositionUpdate  on_position_update_;
};

} // namespace quant::oms
