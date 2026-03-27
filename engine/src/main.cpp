#include "quant/infra/logger.h"
#include "quant/infra/timer.h"
#include "quant/oms/order_manager.h"
#include "quant/oms/position_manager.h"
#include "quant/oms/pnl_calculator.h"
#include "quant/risk/pre_trade_check.h"
#include "quant/risk/real_time_monitor.h"
#include "quant/gateway/base_gateway.h"
#include "quant/gateway/gateway_factory.h"
#include "quant/core/event.h"
#include "signal.pb.h"

#include <zmq.hpp>
#include <yaml-cpp/yaml.h>
#include <csignal>
#include <atomic>
#include <thread>
#include <iostream>
#include <filesystem>

// ────────────────────────────────────────────────────────────
// 全局停止标志（SIGINT / SIGTERM 触发）
// ────────────────────────────────────────────────────────────
static std::atomic<bool> g_shutdown{false};

static void signal_handler(int /*sig*/) {
    g_shutdown = true;
}

// ────────────────────────────────────────────────────────────
// EngineCore - 将所有组件串联起来的引擎核心
//
// 线程模型（参见设计文档）：
//   Thread 1: main - 信号接收 (ZMQ PULL)
//   Thread 2: decision loop - 风控 → OMS → Gateway
//   Thread 3: fill processor - 成交 → 持仓更新
//   Thread 4: monitor - 实时风控检查
// ────────────────────────────────────────────────────────────
class EngineCore : public quant::gateway::IGatewayCallback {
public:
    explicit EngineCore(double initial_cash, const std::string& gateway_type = "mock")
        : position_mgr_(initial_cash)
        , pnl_calc_(initial_cash)
        , risk_checker_(quant::risk::RiskConfig{})
        , risk_monitor_(risk_checker_, quant::risk::RiskConfig{})
    {
        // Gateway
        gateway_ = quant::gateway::GatewayFactory::create(gateway_type);
        gateway_->set_callback(this);

        // OMS 回调 → 持仓更新
        order_mgr_.set_on_trade_update([this](const quant::Trade& t) {
            position_mgr_.on_trade(t);
            const auto pnl = pnl_calc_.update(position_mgr_.snapshot());
            LOG_INFO("PnL: NAV={:.2f} daily={:.2f}% drawdown={:.2f}%",
                     pnl.nav, pnl.daily_return * 100.0, pnl.max_drawdown * 100.0);
        });

        // 风控熔断 → 停止
        risk_monitor_.set_on_breaker([](const char* reason) {
            LOG_CRITICAL("RISK BREAKER: {} — engine halted", reason);
            g_shutdown = true;
        });
    }

    bool start() {
        if (!gateway_->connect()) {
            LOG_CRITICAL("EngineCore: gateway connect failed");
            return false;
        }
        position_mgr_.on_day_start();
        LOG_INFO("EngineCore: started, NAV={:.2f}", position_mgr_.nav());
        return true;
    }

    // 处理来自 Python 的批量信号
    void on_signal_batch(const quant::proto::SignalBatch& batch) {
        LOG_INFO("EngineCore: received batch strategy={} signals={}",
                 batch.strategy_id(), batch.signals_size());

        for (const auto& sig : batch.signals()) {
            process_signal(sig, batch.total_capital());
        }
    }

    void shutdown() {
        LOG_INFO("EngineCore: shutdown initiated");
        gateway_->disconnect();
    }

private:
    void process_signal(const quant::proto::Signal& sig, double total_capital) {
        const quant::Symbol symbol = quant::make_symbol(sig.symbol());
        const double nav           = position_mgr_.nav();

        // 获取当前持仓
        const auto* pos = position_mgr_.get_position(symbol);
        const quant::Quantity current_qty = pos ? pos->qty : 0;

        // 计算目标数量
        const double ref_price = sig.ref_price() > 0 ? sig.ref_price() : 1.0;
        const double target_amount = sig.target_weight() * (total_capital > 0 ? total_capital : nav);
        const quant::Quantity target_qty = (static_cast<quant::Quantity>(target_amount / ref_price) / 100) * 100;
        const quant::Quantity delta = target_qty - current_qty;

        if (sig.close_position()) {
            if (current_qty > 0) place_order(symbol, quant::Side::Sell, current_qty, ref_price);
            return;
        }

        if (std::abs(delta) < 100) {
            LOG_DEBUG("EngineCore: skip {} delta={} (< 1 lot)", sig.symbol(), delta);
            return;
        }

        if (delta > 0) {
            place_order(symbol, quant::Side::Buy, delta, ref_price);
        } else {
            place_order(symbol, quant::Side::Sell, -delta, ref_price);
        }
    }

    void place_order(const quant::Symbol& symbol, quant::Side side,
                     quant::Quantity qty, quant::Price price) {
        quant::OrderRequest req;
        req.symbol = symbol;
        req.side   = side;
        req.qty    = qty;
        req.price  = price;
        req.type   = quant::OrderType::Limit;

        // 构建检查上下文
        quant::risk::CheckContext ctx{
            .snap          = position_mgr_.snapshot(),
            .total_capital = position_mgr_.nav(),
            .active_orders = 0,
            .last_price    = price,
        };

        // 风控检查
        quant::Order order;
        order.order_id    = quant::OrderIdGen::next();
        order.symbol      = req.symbol;
        order.side        = req.side;
        order.type        = req.type;
        order.limit_price = req.price;
        order.target_qty  = req.qty;
        order.status      = quant::OrderStatus::New;
        order.created_ns  = quant::infra::Clock::now_ns();

        const char* reject_reason = nullptr;
        const auto result = risk_checker_.check(order, ctx, &reject_reason);

        if (result == quant::RiskResult::Reject) {
            LOG_WARN("EngineCore: order REJECTED symbol={} reason={}",
                     quant::symbol_view(symbol), reject_reason);
            return;
        }

        // 冻结资金 / 持仓
        if (side == quant::Side::Buy) {
            const double amount = price * static_cast<double>(order.target_qty);
            if (!position_mgr_.freeze_cash(amount)) {
                LOG_WARN("EngineCore: insufficient cash for {}", quant::symbol_view(symbol));
                return;
            }
        } else {
            if (!position_mgr_.freeze_position(symbol, order.target_qty)) {
                LOG_WARN("EngineCore: insufficient position for {}", quant::symbol_view(symbol));
                return;
            }
        }

        // 风控可能把数量降级（RiskResult::Reduce），把结果回填到请求。
        req.qty   = order.target_qty;
        req.price = order.limit_price;

        // 通过 OMS 记录 + 发给 Gateway（必须使用同一个 order_id）
        const int64_t oid = order_mgr_.create_order(req);
        const quant::Order* managed = order_mgr_.find(oid);
        if (!managed) {
            LOG_ERROR("EngineCore: internal error, order not found after create, oid={}", oid);
            return;
        }
        gateway_->send_order(*managed);
    }

    // ── IGatewayCallback ────────────────────────────────────

    void on_order_accepted(int64_t order_id) override {
        order_mgr_.on_order_accepted(order_id);
    }
    void on_order_rejected(int64_t order_id, const std::string& reason) override {
        order_mgr_.on_order_rejected(order_id, reason);
    }
    void on_cancel_confirm(int64_t order_id) override {
        order_mgr_.on_cancel_confirm(order_id);
    }
    void on_cancel_rejected(int64_t order_id, const std::string& reason) override {
        LOG_WARN("Cancel rejected: order_id={} reason={}", order_id, reason);
    }
    void on_trade(const quant::Trade& trade) override {
        order_mgr_.on_trade(trade);
    }
    void on_error(int code, const std::string& msg) override {
        LOG_ERROR("Gateway error: code={} msg={}", code, msg);
    }

    quant::oms::OrderManager    order_mgr_;
    quant::oms::PositionManager position_mgr_;
    quant::oms::PnlCalculator   pnl_calc_;
    quant::risk::PreTradeChecker  risk_checker_;
    quant::risk::RealTimeMonitor  risk_monitor_;
    std::unique_ptr<quant::gateway::IBaseGateway> gateway_;
};

// ────────────────────────────────────────────────────────────
// main
// ────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    // ── 信号处理 ──────────────────────────────────────────
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // ── 配置 ──────────────────────────────────────────────
    std::string config_path = (argc > 1) ? argv[1] : "config/engine.yaml";
    double      initial_cash = 20000.0;
    std::string gateway_type = "mock";
    std::string zmq_endpoint = "ipc:///tmp/quant_signals";
    std::string log_file     = "logs/engine.log";

    if (std::filesystem::exists(config_path)) {
        try {
            YAML::Node cfg = YAML::LoadFile(config_path);
            initial_cash = cfg["initial_cash"].as<double>(initial_cash);
            gateway_type = cfg["gateway"].as<std::string>(gateway_type);
            zmq_endpoint = cfg["zmq_endpoint"].as<std::string>(zmq_endpoint);
            log_file     = cfg["log_file"].as<std::string>(log_file);
        } catch (const std::exception& e) {
            std::cerr << "Config parse error: " << e.what() << "\n";
        }
    }

    // ── 日志 ──────────────────────────────────────────────
    std::filesystem::create_directories("logs");
    quant::infra::Logger::init(log_file);
    LOG_INFO("QuantEngine v0.1.0 starting...");
    LOG_INFO("gateway={} initial_cash={:.2f}", gateway_type, initial_cash);

    // ── Protobuf ──────────────────────────────────────────
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // ── 引擎启动 ──────────────────────────────────────────
    EngineCore engine(initial_cash, gateway_type);
    if (!engine.start()) {
        LOG_CRITICAL("Engine failed to start");
        return 1;
    }

    // ── ZMQ 信号接收（阻塞主线程）─────────────────────────
    zmq::context_t ctx(1);
    zmq::socket_t  pull_sock(ctx, zmq::socket_type::pull);
    pull_sock.bind(zmq_endpoint);
    pull_sock.set(zmq::sockopt::rcvtimeo, 200);

    LOG_INFO("Engine ready, listening on {}", zmq_endpoint);

    while (!g_shutdown) {
        zmq::message_t msg;
        const auto result = pull_sock.recv(msg, zmq::recv_flags::none);
        if (!result) continue;

        quant::proto::SignalBatch batch;
        if (batch.ParseFromArray(msg.data(), static_cast<int>(msg.size()))) {
            engine.on_signal_batch(batch);
        } else {
            LOG_WARN("Failed to parse signal batch ({} bytes)", msg.size());
        }
    }

    engine.shutdown();
    LOG_INFO("QuantEngine stopped cleanly");

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
