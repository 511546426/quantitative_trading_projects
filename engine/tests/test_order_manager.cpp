#include <gtest/gtest.h>
#include "quant/oms/order_manager.h"

using namespace quant;
using namespace quant::oms;

class OrderManagerTest : public ::testing::Test {
protected:
    OrderManager mgr;

    OrderRequest make_req(const char* sym, Side side, Quantity qty, Price price) {
        OrderRequest req;
        req.symbol = make_symbol(sym);
        req.side   = side;
        req.qty    = qty;
        req.price  = price;
        req.type   = OrderType::Limit;
        return req;
    }
};

TEST_F(OrderManagerTest, CreateOrder) {
    const auto req = make_req("000001.SZ", Side::Buy, 1000, 12.50);
    const int64_t oid = mgr.create_order(req);
    EXPECT_GT(oid, 0);
    EXPECT_EQ(mgr.total_orders(), 1u);

    const Order* o = mgr.find(oid);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->target_qty, 1000);
    EXPECT_DOUBLE_EQ(o->limit_price, 12.50);
    EXPECT_EQ(o->status, OrderStatus::New);
    EXPECT_TRUE(o->is_active());
}

TEST_F(OrderManagerTest, OrderAccepted) {
    const int64_t oid = mgr.create_order(make_req("000001.SZ", Side::Buy, 500, 10.0));
    mgr.on_order_accepted(oid);

    const Order* o = mgr.find(oid);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->status, OrderStatus::Submitted);
}

TEST_F(OrderManagerTest, OrderRejected) {
    const int64_t oid = mgr.create_order(make_req("000001.SZ", Side::Buy, 500, 10.0));
    mgr.on_order_rejected(oid, "insufficient funds");

    const Order* o = mgr.find(oid);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->status, OrderStatus::Rejected);
    EXPECT_TRUE(o->is_done());
}

TEST_F(OrderManagerTest, PartialFill) {
    const int64_t oid = mgr.create_order(make_req("000001.SZ", Side::Buy, 1000, 10.0));
    mgr.on_order_accepted(oid);

    Trade t;
    t.trade_id   = 1;
    t.order_id   = oid;
    t.symbol     = make_symbol("000001.SZ");
    t.side       = Side::Buy;
    t.fill_qty   = 400;
    t.fill_price = 10.01;
    t.fill_ns    = 0;
    fill_costs(t);

    mgr.on_trade(t);
    const Order* o = mgr.find(oid);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->status, OrderStatus::Partial);
    EXPECT_EQ(o->filled_qty, 400);
    EXPECT_NEAR(o->avg_fill_price, 10.01, 1e-6);
    EXPECT_EQ(o->remaining(), 600);
}

TEST_F(OrderManagerTest, FullFill_AvgPrice) {
    const int64_t oid = mgr.create_order(make_req("000001.SZ", Side::Buy, 1000, 10.0));
    mgr.on_order_accepted(oid);

    // 两笔成交
    for (int i = 0; i < 2; ++i) {
        Trade t;
        t.trade_id   = i + 1;
        t.order_id   = oid;
        t.symbol     = make_symbol("000001.SZ");
        t.side       = Side::Buy;
        t.fill_qty   = 500;
        t.fill_price = (i == 0) ? 10.0 : 10.4;
        fill_costs(t);
        mgr.on_trade(t);
    }

    const Order* o = mgr.find(oid);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->status, OrderStatus::Filled);
    EXPECT_NEAR(o->avg_fill_price, 10.2, 1e-6);
}

TEST_F(OrderManagerTest, ActiveOrders) {
    mgr.create_order(make_req("000001.SZ", Side::Buy, 500, 10.0));
    mgr.create_order(make_req("000002.SZ", Side::Buy, 300, 8.0));
    const int64_t oid3 = mgr.create_order(make_req("000001.SZ", Side::Sell, 200, 11.0));

    EXPECT_EQ(mgr.active_orders().size(), 3u);

    mgr.on_order_rejected(oid3, "test");
    EXPECT_EQ(mgr.active_orders().size(), 2u);
}

TEST_F(OrderManagerTest, CancelOrder) {
    const int64_t oid = mgr.create_order(make_req("600000.SH", Side::Buy, 1000, 20.0));
    mgr.on_order_accepted(oid);

    EXPECT_TRUE(mgr.request_cancel(oid));
    mgr.on_cancel_confirm(oid);

    const Order* o = mgr.find(oid);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->status, OrderStatus::Cancelled);
    EXPECT_TRUE(o->is_done());
}

TEST_F(OrderManagerTest, Callback) {
    int callback_count = 0;
    mgr.set_on_order_update([&](const Order&) { ++callback_count; });

    const int64_t oid = mgr.create_order(make_req("000001.SZ", Side::Buy, 100, 10.0));
    mgr.on_order_accepted(oid);
    EXPECT_EQ(callback_count, 2);  // create + accepted
}
