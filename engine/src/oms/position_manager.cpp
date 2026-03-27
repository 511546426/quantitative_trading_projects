#include "quant/oms/position_manager.h"
#include "quant/infra/timer.h"
#include <algorithm>

namespace quant::oms {

void PositionManager::on_trade(const Trade& trade) {
    const std::string sym_str(symbol_view(trade.symbol));
    Position& pos = snap_.get_or_create(trade.symbol);
    pos.updated_ns = trade.fill_ns;

    if (trade.side == Side::Buy) {
        // 解冻现金 + 买入持仓
        const Amount gross = trade.fill_price * static_cast<double>(trade.fill_qty);
        const Amount total_paid = gross + trade.total_cost();
        snap_.frozen_cash -= std::min(snap_.frozen_cash, total_paid);

        pos.on_buy(trade.fill_qty, trade.fill_price, trade.total_cost());

        LOG_INFO("PositionManager: BUY {} qty={} price={:.4f} cost={:.2f} new_qty={}",
                 sym_str, trade.fill_qty, trade.fill_price,
                 trade.total_cost(), pos.qty);

    } else {
        // 卖出 → 增加现金
        const Amount gross  = trade.fill_price * static_cast<double>(trade.fill_qty);
        const Amount net    = gross - trade.total_cost();
        snap_.cash         += net;
        snap_.realized_pnl += net - pos.avg_cost * static_cast<double>(trade.fill_qty);

        pos.on_sell(trade.fill_qty);

        LOG_INFO("PositionManager: SELL {} qty={} price={:.4f} net_cash=+{:.2f} remain_qty={}",
                 sym_str, trade.fill_qty, trade.fill_price, net, pos.qty);

        if (pos.is_empty()) {
            snap_.positions.erase(sym_str);
        }
    }

    if (on_position_update_) on_position_update_(pos);
}

void PositionManager::on_price_update(const Symbol& symbol, Price price) {
    auto* pos = snap_.find(symbol);
    if (pos) {
        pos->last_price = price;
        pos->updated_ns = infra::Clock::now_ns();
    }
}

void PositionManager::on_day_start() {
    snap_.realized_pnl    = 0.0;
    snap_.daily_start_nav = snap_.nav();
    LOG_INFO("PositionManager: day start NAV={:.2f}", snap_.daily_start_nav);
}

bool PositionManager::freeze_cash(Amount amount) noexcept {
    if (snap_.cash < amount) return false;
    snap_.cash         -= amount;
    snap_.frozen_cash  += amount;
    return true;
}

void PositionManager::unfreeze_cash(Amount amount) noexcept {
    const Amount to_unfreeze = std::min(snap_.frozen_cash, amount);
    snap_.frozen_cash -= to_unfreeze;
    snap_.cash        += to_unfreeze;
}

bool PositionManager::freeze_position(const Symbol& symbol, Quantity qty) noexcept {
    auto* pos = snap_.find(symbol);
    if (!pos || pos->available() < qty) return false;
    pos->frozen_qty += qty;
    return true;
}

void PositionManager::unfreeze_position(const Symbol& symbol, Quantity qty) noexcept {
    auto* pos = snap_.find(symbol);
    if (!pos) return;
    pos->frozen_qty = std::max(int64_t{0}, pos->frozen_qty - qty);
}

} // namespace quant::oms
