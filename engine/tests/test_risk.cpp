#include <gtest/gtest.h>
#include "quant/risk/pre_trade_check.h"

using namespace quant;
using namespace quant::risk;

class RiskTest : public ::testing::Test {
protected:
    RiskConfig cfg;
    PortfolioSnapshot snap;

    void SetUp() override {
        cfg.max_single_position_ratio = 0.10;
        cfg.max_total_position_ratio  = 0.80;
        cfg.max_order_amount          = 100'000.0;
        cfg.min_order_amount          = 100.0;
        cfg.max_daily_loss_ratio      = 0.05;
        cfg.allow_short               = false;

        snap.cash            = 20'000.0;
        snap.daily_start_nav = 20'000.0;
    }

    Order make_order(const char* sym, Side side, Quantity qty, Price price) {
        Order o;
        o.order_id    = 1;
        o.symbol      = make_symbol(sym);
        o.side        = side;
        o.type        = OrderType::Limit;
        o.limit_price = price;
        o.target_qty  = qty;
        o.status      = OrderStatus::New;
        return o;
    }

    CheckContext make_ctx(Price last = 10.0) {
        return CheckContext{
            .snap          = snap,
            .total_capital = snap.nav(),
            .active_orders = 0,
            .last_price    = last,
        };
    }
};

TEST_F(RiskTest, PassNormalOrder) {
    PreTradeChecker checker(cfg);
    auto order = make_order("000001.SZ", Side::Buy, 100, 10.0);
    const char* reason = nullptr;
    EXPECT_EQ(checker.check(order, make_ctx(), &reason), RiskResult::Pass);
    EXPECT_EQ(reason, nullptr);
}

TEST_F(RiskTest, RejectInsufficientCash) {
    PreTradeChecker checker(cfg);
    auto order = make_order("000001.SZ", Side::Buy, 10000, 10.0);  // 100,000 > 20,000
    const char* reason = nullptr;
    EXPECT_EQ(checker.check(order, make_ctx(), &reason), RiskResult::Reject);
    EXPECT_NE(reason, nullptr);
}

TEST_F(RiskTest, RejectSuspended) {
    PreTradeChecker checker(cfg);
    auto order = make_order("000001.SZ", Side::Buy, 100, 10.0);
    auto ctx = make_ctx();
    ctx.is_suspended = true;
    const char* reason = nullptr;
    EXPECT_EQ(checker.check(order, ctx, &reason), RiskResult::Reject);
    EXPECT_STREQ(reason, "stock is suspended");
}

TEST_F(RiskTest, RejectShortSell) {
    PreTradeChecker checker(cfg);
    auto order = make_order("000001.SZ", Side::Sell, 100, 10.0);
    // 没有持仓，做空被拒
    const char* reason = nullptr;
    EXPECT_EQ(checker.check(order, make_ctx(), &reason), RiskResult::Reject);
}

TEST_F(RiskTest, ReducePositionLimit) {
    PreTradeChecker checker(cfg);
    // 买入 2000 股 * 10 = 20,000，超过单股 10% 限制（20,000 * 10% = 2,000）
    auto order = make_order("000001.SZ", Side::Buy, 2000, 10.0);
    const char* reason = nullptr;
    const auto result = checker.check(order, make_ctx(), &reason);
    // 应该 Reduce（降到 200 股以内）
    EXPECT_EQ(result, RiskResult::Reduce);
    EXPECT_LE(order.target_qty, 200);
}

TEST_F(RiskTest, CircuitBreaker) {
    PreTradeChecker checker(cfg);
    checker.trigger_circuit_breaker("test");

    auto order = make_order("000001.SZ", Side::Buy, 100, 10.0);
    const char* reason = nullptr;
    EXPECT_EQ(checker.check(order, make_ctx(), &reason), RiskResult::Reject);
    EXPECT_STREQ(reason, "circuit breaker active");

    checker.reset_circuit_breaker();
    EXPECT_EQ(checker.check(order, make_ctx(), &reason), RiskResult::Pass);
}

TEST_F(RiskTest, DailyLossTriggerBreaker) {
    PreTradeChecker checker(cfg);
    // 模拟今日亏损 6% > max_daily_loss_ratio(5%)
    snap.cash            = 18'800.0;
    snap.daily_start_nav = 20'000.0;

    auto order = make_order("000001.SZ", Side::Buy, 100, 10.0);
    const char* reason = nullptr;
    const auto result = checker.check(order, make_ctx(), &reason);
    EXPECT_EQ(result, RiskResult::Reject);
    EXPECT_TRUE(checker.is_breaker_on());
}

TEST_F(RiskTest, BelowMinAmount) {
    PreTradeChecker checker(cfg);
    auto order = make_order("000001.SZ", Side::Buy, 5, 10.0);  // 50元 < 100元最小
    const char* reason = nullptr;
    EXPECT_EQ(checker.check(order, make_ctx(), &reason), RiskResult::Reject);
}
