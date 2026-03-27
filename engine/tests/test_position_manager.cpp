#include <gtest/gtest.h>
#include "quant/oms/position_manager.h"

using namespace quant;
using namespace quant::oms;

class PositionManagerTest : public ::testing::Test {
protected:
    PositionManager mgr{20000.0};

    Trade make_buy(const char* sym, Quantity qty, Price price) {
        Trade t;
        t.trade_id   = 1;
        t.order_id   = 1;
        t.symbol     = make_symbol(sym);
        t.side       = Side::Buy;
        t.fill_qty   = qty;
        t.fill_price = price;
        t.fill_ns    = 0;
        fill_costs(t);
        return t;
    }

    Trade make_sell(const char* sym, Quantity qty, Price price) {
        Trade t = make_buy(sym, qty, price);
        t.side = Side::Sell;
        fill_costs(t);
        return t;
    }
};

TEST_F(PositionManagerTest, InitialState) {
    EXPECT_DOUBLE_EQ(mgr.cash(), 20000.0);
    EXPECT_DOUBLE_EQ(mgr.market_value(), 0.0);
    EXPECT_DOUBLE_EQ(mgr.nav(), 20000.0);
}

TEST_F(PositionManagerTest, BuyUpdatesPosition) {
    const Trade t = make_buy("000001.SZ", 1000, 10.0);
    mgr.freeze_cash(t.gross_amount() + t.total_cost());
    mgr.on_trade(t);

    const auto* pos = mgr.get_position(make_symbol("000001.SZ"));
    ASSERT_NE(pos, nullptr);
    EXPECT_EQ(pos->qty, 1000);
    EXPECT_GT(pos->avg_cost, 10.0);  // 含佣金
}

TEST_F(PositionManagerTest, SellUpdatesPosition) {
    // 先买入
    const Trade buy = make_buy("000001.SZ", 1000, 10.0);
    mgr.freeze_cash(buy.gross_amount() + buy.total_cost());
    mgr.on_trade(buy);

    const double cash_after_buy = mgr.cash();

    // 再卖出
    const Trade sell = make_sell("000001.SZ", 1000, 11.0);
    mgr.freeze_position(make_symbol("000001.SZ"), 1000);
    mgr.on_trade(sell);

    // 持仓清空
    EXPECT_EQ(mgr.get_position(make_symbol("000001.SZ")), nullptr);
    // 现金增加
    EXPECT_GT(mgr.cash(), cash_after_buy);
}

TEST_F(PositionManagerTest, FreezeCash) {
    EXPECT_TRUE(mgr.freeze_cash(5000.0));
    EXPECT_DOUBLE_EQ(mgr.cash(), 15000.0);
    EXPECT_DOUBLE_EQ(mgr.snapshot().frozen_cash, 5000.0);

    EXPECT_FALSE(mgr.freeze_cash(16000.0));  // 超出可用现金
}

TEST_F(PositionManagerTest, UnfreezeCash) {
    mgr.freeze_cash(5000.0);
    mgr.unfreeze_cash(3000.0);
    EXPECT_DOUBLE_EQ(mgr.cash(), 18000.0);
    EXPECT_DOUBLE_EQ(mgr.snapshot().frozen_cash, 2000.0);
}

TEST_F(PositionManagerTest, PriceUpdate) {
    const Trade t = make_buy("000001.SZ", 1000, 10.0);
    mgr.freeze_cash(t.gross_amount() + t.total_cost());
    mgr.on_trade(t);

    mgr.on_price_update(make_symbol("000001.SZ"), 12.0);

    const auto* pos = mgr.get_position(make_symbol("000001.SZ"));
    ASSERT_NE(pos, nullptr);
    EXPECT_DOUBLE_EQ(pos->last_price, 12.0);
    EXPECT_NEAR(pos->unrealized_pnl(), 2000.0, 100.0);  // ~2000 元浮盈（扣除成本近似）
}
