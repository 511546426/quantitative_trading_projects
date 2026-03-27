#include <benchmark/benchmark.h>
#include "quant/risk/pre_trade_check.h"

using namespace quant;
using namespace quant::risk;

// ────────────────────────────────────────────────────────────
// 风控检查延迟：目标 < 10μs
// ────────────────────────────────────────────────────────────
static void BM_PreTradeCheck(benchmark::State& state) {
    RiskConfig cfg;
    PreTradeChecker checker(cfg);

    PortfolioSnapshot snap;
    snap.cash            = 20000.0;
    snap.daily_start_nav = 20000.0;

    CheckContext ctx{
        .snap          = snap,
        .total_capital = 20000.0,
        .active_orders = 0,
        .last_price    = 10.0,
    };

    Order order;
    order.order_id    = 1;
    order.symbol      = make_symbol("000001.SZ");
    order.side        = Side::Buy;
    order.type        = OrderType::Limit;
    order.limit_price = 10.0;
    order.target_qty  = 100;
    order.status      = OrderStatus::New;

    for (auto _ : state) {
        Order o = order;
        const auto result = checker.check(o, ctx);
        benchmark::DoNotOptimize(result);
    }

    state.SetLabel("target: <10μs");
}
BENCHMARK(BM_PreTradeCheck)->Iterations(1000000);

