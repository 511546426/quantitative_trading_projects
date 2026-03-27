#pragma once
#include "types.h"
#include "trade.h"
#include <unordered_map>
#include <string>
#include <vector>

namespace quant {

// ────────────────────────────────────────────────────────────
// Position - 单只股票的持仓信息
// ────────────────────────────────────────────────────────────
struct Position {
    Symbol     symbol       = {};
    Quantity   qty          = 0;       // 持仓数量（股）
    Quantity   frozen_qty   = 0;       // 在途卖出中，已冻结数量
    Price      avg_cost     = 0.0;     // 持仓均价（含交易成本）
    Price      last_price   = 0.0;     // 最新价（行情推送）
    Nanos      updated_ns   = 0;       // 最后更新时间

    [[nodiscard]] Quantity available() const noexcept {
        return qty - frozen_qty;
    }

    [[nodiscard]] Amount market_value() const noexcept {
        return last_price * static_cast<double>(qty);
    }

    [[nodiscard]] Amount unrealized_pnl() const noexcept {
        return (last_price - avg_cost) * static_cast<double>(qty);
    }

    [[nodiscard]] Amount cost_basis() const noexcept {
        return avg_cost * static_cast<double>(qty);
    }

    [[nodiscard]] bool is_empty() const noexcept {
        return qty == 0;
    }

    // 买入后更新均价
    void on_buy(Quantity fill_qty, Price fill_price, Amount cost) noexcept {
        const Amount old_basis = avg_cost * static_cast<double>(qty);
        const Amount new_basis = fill_price * static_cast<double>(fill_qty) + cost;
        qty += fill_qty;
        if (qty > 0) {
            avg_cost = (old_basis + new_basis) / static_cast<double>(qty);
        }
    }

    // 卖出后更新数量（均价不变，等下次买入重算）
    void on_sell(Quantity fill_qty) noexcept {
        qty       -= fill_qty;
        frozen_qty = std::max(int64_t{0}, frozen_qty - fill_qty);
        if (qty == 0) avg_cost = 0.0;
    }
};

// ────────────────────────────────────────────────────────────
// PortfolioSnapshot - 某时刻的组合快照（风控使用）
// ────────────────────────────────────────────────────────────
struct PortfolioSnapshot {
    // key = "000001.SZ"（std::string 方便映射查找）
    std::unordered_map<std::string, Position> positions;
    Amount cash            = 0.0;   // 可用资金
    Amount frozen_cash     = 0.0;   // 已冻结（买入在途）
    Amount realized_pnl    = 0.0;   // 今日已实现 PnL
    Amount daily_start_nav = 0.0;   // 当日开盘净值
    Nanos  snapshot_ns     = 0;

    [[nodiscard]] Amount total_cash() const noexcept {
        return cash + frozen_cash;
    }

    [[nodiscard]] Amount market_value() const noexcept {
        Amount mv = 0.0;
        for (const auto& [_, pos] : positions)
            mv += pos.market_value();
        return mv;
    }

    [[nodiscard]] Amount nav() const noexcept {
        return total_cash() + market_value();
    }

    [[nodiscard]] double position_ratio(const std::string& sym) const noexcept {
        const double n = nav();
        if (n <= 0) return 0.0;
        auto it = positions.find(sym);
        if (it == positions.end()) return 0.0;
        return it->second.market_value() / n;
    }

    [[nodiscard]] double total_position_ratio() const noexcept {
        const double n = nav();
        if (n <= 0) return 0.0;
        return market_value() / n;
    }

    [[nodiscard]] double daily_loss_ratio() const noexcept {
        if (daily_start_nav <= 0) return 0.0;
        return (nav() - daily_start_nav) / daily_start_nav;
    }

    const Position* find(const Symbol& sym) const noexcept {
        auto it = positions.find(std::string(symbol_view(sym)));
        return it != positions.end() ? &it->second : nullptr;
    }

    Position* find(const Symbol& sym) noexcept {
        auto it = positions.find(std::string(symbol_view(sym)));
        return it != positions.end() ? &it->second : nullptr;
    }

    Position& get_or_create(const Symbol& sym) {
        auto key = std::string(symbol_view(sym));
        auto& pos = positions[key];
        if (pos.symbol[0] == '\0') pos.symbol = sym;
        return pos;
    }
};

} // namespace quant
