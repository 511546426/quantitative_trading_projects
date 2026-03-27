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

static std::atomic<bool> g_shutdown{false};
static void signal_handler(int) { g_shutdown = true; }

// ── YAML → RiskConfig 解析 ───────────────────────────────────
static quant::risk::RiskConfig parse_risk_config(const YAML::Node& cfg) {
    quant::risk::RiskConfig rc;
    if (!cfg["risk"]) return rc;
    auto r = cfg["risk"];
    rc.max_single_position_ratio = r["max_single_position_ratio"].as<double>(rc.max_single_position_ratio);
    rc.max_total_position_ratio  = r["max_total_position_ratio"].as<double>(rc.max_total_position_ratio);
    rc.max_order_amount          = r["max_order_amount"].as<double>(rc.max_order_amount);
    rc.max_daily_loss_ratio      = r["max_daily_loss_ratio"].as<double>(rc.max_daily_loss_ratio);
    rc.max_drawdown_ratio        = r["max_drawdown_ratio"].as<double>(rc.max_drawdown_ratio);
    rc.min_order_amount          = r["min_order_amount"].as<double>(rc.min_order_amount);
    rc.max_active_orders_per_sym = r["max_active_orders_per_sym"].as<int>(rc.max_active_orders_per_sym);
    rc.allow_short               = r["allow_short"].as<bool>(rc.allow_short);
    return rc;
}

// ────────────────────────────────────────────────────────────
// EngineCore - 双向 ZMQ 架构
// ────────────────────────────────────────────────────────────
class EngineCore : public quant::gateway::IGatewayCallback {
public:
    EngineCore(double initial_cash,
               const std::string& gateway_type,
               zmq::socket_t& order_push,
               const std::string& algo,
               int twap_slices,
               int twap_interval,
               const quant::risk::RiskConfig& risk_cfg)
        : position_mgr_(initial_cash)
        , pnl_calc_(initial_cash)
        , risk_checker_(risk_cfg)
        , risk_monitor_(risk_checker_, risk_cfg)
        , order_push_(order_push)
        , algo_(algo)
        , twap_slices_(twap_slices)
        , twap_interval_(twap_interval)
        , use_external_gateway_(gateway_type == "qmt")
    {
        if (!use_external_gateway_) {
            gateway_ = quant::gateway::GatewayFactory::create(gateway_type);
            gateway_->set_callback(this);
        }

        order_mgr_.set_on_trade_update([this](const quant::Trade& t) {
            position_mgr_.on_trade(t);
            const auto pnl = pnl_calc_.update(position_mgr_.snapshot());

            // 实时风控：每笔成交后检查回撤和仓位暴露
            risk_monitor_.on_portfolio_update(position_mgr_.snapshot());

            LOG_INFO("PnL: NAV={:.2f} daily={:.2f}% dd={:.2f}%",
                     pnl.nav, pnl.daily_return * 100.0, pnl.max_drawdown * 100.0);
        });

        risk_monitor_.set_on_breaker([](const char* reason) {
            LOG_CRITICAL("RISK BREAKER: {} — engine halted", reason);
            g_shutdown = true;
        });
    }

    bool start() {
        if (gateway_ && !gateway_->connect()) {
            LOG_CRITICAL("EngineCore: gateway connect failed");
            return false;
        }
        position_mgr_.on_day_start();
        LOG_INFO("EngineCore: started, NAV={:.2f} mode={} risk=[pos={:.0f}% dd={:.0f}%]",
                 position_mgr_.nav(),
                 use_external_gateway_ ? "QMT" : "MOCK",
                 risk_checker_.config().max_single_position_ratio * 100,
                 risk_checker_.config().max_drawdown_ratio * 100);
        return true;
    }

    void on_signal_batch(const quant::proto::SignalBatch& batch) {
        LOG_INFO("EngineCore: batch strategy={} signals={}",
                 batch.strategy_id(), batch.signals_size());

        quant::proto::OrderCommandBatch cmd_batch;
        cmd_batch.set_batch_time_ns(quant::infra::Clock::now_ns());
        cmd_batch.set_strategy_id(batch.strategy_id());

        for (const auto& sig : batch.signals()) {
            process_signal(sig, batch.total_capital(), cmd_batch);
        }

        if (use_external_gateway_ && cmd_batch.orders_size() > 0) {
            std::string serialized;
            cmd_batch.SerializeToString(&serialized);
            zmq::message_t msg(serialized.data(), serialized.size());
            order_push_.send(msg, zmq::send_flags::dontwait);
            LOG_INFO("EngineCore: pushed {} orders to QMT adapter", cmd_batch.orders_size());
        }
    }

    void on_fill_report(const quant::proto::FillReport& report) {
        const int64_t oid = report.order_id();

        if (report.status() == "REJECTED") {
            order_mgr_.on_order_rejected(oid, report.reject_reason());
            auto* order = order_mgr_.find(oid);
            if (order) {
                if (order->side == quant::Side::Buy) {
                    position_mgr_.unfreeze_cash(
                        order->limit_price * static_cast<double>(order->target_qty));
                } else {
                    position_mgr_.unfreeze_position(order->symbol, order->target_qty);
                }
            }
            LOG_WARN("FillReport: order {} REJECTED: {}", oid, report.reject_reason());
            return;
        }

        if (report.status() == "CANCELLED") {
            order_mgr_.on_cancel_confirm(oid);
            auto* order = order_mgr_.find(oid);
            if (order) {
                auto remaining = order->remaining();
                if (order->side == quant::Side::Buy) {
                    position_mgr_.unfreeze_cash(
                        order->limit_price * static_cast<double>(remaining));
                } else {
                    position_mgr_.unfreeze_position(order->symbol, remaining);
                }
            }
            return;
        }

        if (report.fill_quantity() > 0) {
            // 首次成交时标记为 accepted
            auto* order = order_mgr_.find(oid);
            if (order && order->status == quant::OrderStatus::New) {
                order_mgr_.on_order_accepted(oid);
            }

            quant::Trade trade;
            trade.trade_id    = report.trade_id();
            trade.order_id    = oid;
            trade.symbol      = quant::make_symbol(report.symbol());
            trade.side        = (report.side() == "BUY") ? quant::Side::Buy : quant::Side::Sell;
            trade.fill_qty    = report.fill_quantity();
            trade.fill_price  = report.fill_price();
            trade.commission  = report.commission();
            trade.stamp_duty  = report.stamp_duty();
            trade.fill_ns     = report.fill_time_ns();

            order_mgr_.on_trade(trade);

            LOG_INFO("FillReport: {} {} {}@{:.4f} comm={:.2f}",
                     report.symbol(), report.side(),
                     report.fill_quantity(), report.fill_price(),
                     report.commission());
        }
    }

    quant::proto::EngineStatus get_status() const {
        quant::proto::EngineStatus s;
        const auto& snap = position_mgr_.snapshot();
        s.set_nav(snap.nav());
        s.set_cash(snap.cash);
        s.set_daily_pnl(pnl_calc_.last().total_pnl);
        s.set_max_drawdown(pnl_calc_.last().max_drawdown);
        s.set_timestamp_ns(quant::infra::Clock::now_ns());
        s.set_status(risk_checker_.is_breaker_on() ? "breaker_on" : "running");
        return s;
    }

    void shutdown() {
        LOG_INFO("EngineCore: shutdown");
        if (gateway_) gateway_->disconnect();
    }

private:
    void process_signal(const quant::proto::Signal& sig, double total_capital,
                        quant::proto::OrderCommandBatch& cmd_batch) {
        const quant::Symbol symbol = quant::make_symbol(sig.symbol());
        const double nav           = position_mgr_.nav();
        const auto* pos            = position_mgr_.get_position(symbol);
        const quant::Quantity current_qty = pos ? pos->qty : 0;

        const double ref_price = sig.ref_price() > 0 ? sig.ref_price() : 1.0;
        const double target_amount = sig.target_weight()
                                   * (total_capital > 0 ? total_capital : nav);
        const quant::Quantity target_qty =
            (static_cast<quant::Quantity>(target_amount / ref_price) / 100) * 100;

        quant::Quantity delta = target_qty - current_qty;

        if (sig.close_position()) {
            if (current_qty <= 0) return;
            delta = -current_qty;
        }

        if (std::abs(delta) < 100) return;

        const quant::Side side = (delta > 0) ? quant::Side::Buy : quant::Side::Sell;
        quant::Quantity qty = std::abs(delta);

        // 构建 OrderRequest（OMS 会分配 order_id）
        quant::OrderRequest req;
        req.symbol = symbol;
        req.side   = side;
        req.qty    = qty;
        req.price  = ref_price;
        req.type   = quant::OrderType::Limit;

        // 在 OMS 创建订单以获得统一的 order_id
        const int64_t oid = order_mgr_.create_order(req);
        quant::Order* managed = order_mgr_.find(oid);
        if (!managed) {
            LOG_ERROR("EngineCore: failed to create order");
            return;
        }

        // 风控检查（用 OMS 管理的 order 对象）
        const auto active = order_mgr_.active_orders();
        int active_for_symbol = 0;
        for (const auto* ao : active) {
            if (ao->symbol == symbol && ao->order_id != oid) ++active_for_symbol;
        }

        quant::risk::CheckContext ctx{
            .snap          = position_mgr_.snapshot(),
            .total_capital = nav,
            .active_orders = active_for_symbol,
            .last_price    = ref_price,
        };

        const char* reject_reason = nullptr;
        auto result = risk_checker_.check(*managed, ctx, &reject_reason);
        if (result == quant::RiskResult::Reject) {
            LOG_WARN("EngineCore: REJECTED {} reason={}", sig.symbol(), reject_reason);
            order_mgr_.on_order_rejected(oid, reject_reason ? reject_reason : "risk");
            return;
        }

        qty = managed->target_qty;

        // 冻结资金/持仓
        if (side == quant::Side::Buy) {
            if (!position_mgr_.freeze_cash(ref_price * static_cast<double>(qty))) {
                LOG_WARN("EngineCore: insufficient cash for {}", sig.symbol());
                order_mgr_.on_order_rejected(oid, "insufficient_cash");
                return;
            }
        } else {
            if (!position_mgr_.freeze_position(symbol, qty)) {
                LOG_WARN("EngineCore: insufficient position for {}", sig.symbol());
                order_mgr_.on_order_rejected(oid, "insufficient_position");
                return;
            }
        }

        if (use_external_gateway_) {
            auto* cmd = cmd_batch.add_orders();
            cmd->set_order_id(oid);
            cmd->set_symbol(sig.symbol());
            cmd->set_side(side == quant::Side::Buy ? "BUY" : "SELL");
            cmd->set_order_type("LIMIT");
            cmd->set_quantity(qty);
            cmd->set_price(ref_price);
            cmd->set_strategy_id(sig.strategy_id());
            cmd->set_created_ns(managed->created_ns);
            cmd->set_algo(algo_);
            cmd->set_twap_slices(twap_slices_);
            cmd->set_twap_interval_sec(twap_interval_);
        } else {
            gateway_->send_order(*managed);
        }
    }

    // ── IGatewayCallback（Mock 模式用）─────────────────────
    void on_order_accepted(int64_t order_id) override {
        order_mgr_.on_order_accepted(order_id);
    }
    void on_order_rejected(int64_t order_id, const std::string& reason) override {
        order_mgr_.on_order_rejected(order_id, reason);
    }
    void on_cancel_confirm(int64_t order_id) override {
        order_mgr_.on_cancel_confirm(order_id);
    }
    void on_cancel_rejected(int64_t, const std::string&) override {}
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

    zmq::socket_t& order_push_;
    std::string algo_;
    int twap_slices_;
    int twap_interval_;
    bool use_external_gateway_;
};

// ────────────────────────────────────────────────────────────
// main
// ────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::string config_path = (argc > 1) ? argv[1] : "config/engine.yaml";
    double      initial_cash = 20000.0;
    std::string gateway_type = "mock";
    std::string algo         = "twap";
    int         twap_slices  = 5;
    int         twap_interval = 60;
    std::string zmq_signal   = "ipc:///tmp/quant_signals";
    std::string zmq_order    = "ipc:///tmp/quant_orders";
    std::string zmq_fill     = "ipc:///tmp/quant_fills";
    std::string zmq_status   = "ipc:///tmp/quant_status";
    std::string log_file     = "logs/engine.log";
    quant::risk::RiskConfig risk_cfg;

    if (std::filesystem::exists(config_path)) {
        try {
            YAML::Node cfg = YAML::LoadFile(config_path);
            initial_cash  = cfg["initial_cash"].as<double>(initial_cash);
            gateway_type  = cfg["gateway"].as<std::string>(gateway_type);
            algo          = cfg["algo"].as<std::string>(algo);
            twap_slices   = cfg["twap_slices"].as<int>(twap_slices);
            twap_interval = cfg["twap_interval_sec"].as<int>(twap_interval);
            zmq_signal    = cfg["zmq_signal"].as<std::string>(zmq_signal);
            zmq_order     = cfg["zmq_order"].as<std::string>(zmq_order);
            zmq_fill      = cfg["zmq_fill"].as<std::string>(zmq_fill);
            zmq_status    = cfg["zmq_status"].as<std::string>(zmq_status);
            log_file      = cfg["log_file"].as<std::string>(log_file);
            risk_cfg      = parse_risk_config(cfg);
        } catch (const std::exception& e) {
            std::cerr << "Config parse error: " << e.what() << "\n";
        }
    }

    std::filesystem::create_directories("logs");
    quant::infra::Logger::init(log_file);
    LOG_INFO("QuantEngine v0.3.0 starting...");
    LOG_INFO("gateway={} algo={} cash={:.0f} risk=[pos{:.0f}% dd{:.0f}% loss{:.0f}%]",
             gateway_type, algo, initial_cash,
             risk_cfg.max_single_position_ratio * 100,
             risk_cfg.max_drawdown_ratio * 100,
             risk_cfg.max_daily_loss_ratio * 100);

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    zmq::context_t zmq_ctx(1);

    zmq::socket_t sig_pull(zmq_ctx, zmq::socket_type::pull);
    sig_pull.bind(zmq_signal);
    sig_pull.set(zmq::sockopt::rcvtimeo, 100);

    zmq::socket_t ord_push(zmq_ctx, zmq::socket_type::push);
    ord_push.bind(zmq_order);

    zmq::socket_t fill_pull(zmq_ctx, zmq::socket_type::pull);
    fill_pull.bind(zmq_fill);
    fill_pull.set(zmq::sockopt::rcvtimeo, 50);

    zmq::socket_t status_pub(zmq_ctx, zmq::socket_type::pub);
    status_pub.bind(zmq_status);

    EngineCore engine(initial_cash, gateway_type, ord_push,
                      algo, twap_slices, twap_interval, risk_cfg);
    if (!engine.start()) {
        LOG_CRITICAL("Engine failed to start");
        return 1;
    }

    LOG_INFO("Engine ready: signals={} orders={} fills={} status={}",
             zmq_signal, zmq_order, zmq_fill, zmq_status);

    int status_counter = 0;
    while (!g_shutdown) {
        {
            zmq::message_t msg;
            if (sig_pull.recv(msg, zmq::recv_flags::none)) {
                quant::proto::SignalBatch batch;
                if (batch.ParseFromArray(msg.data(), static_cast<int>(msg.size()))) {
                    engine.on_signal_batch(batch);
                } else {
                    LOG_WARN("Failed to parse signal batch ({} bytes)", msg.size());
                }
            }
        }

        {
            zmq::message_t msg;
            if (fill_pull.recv(msg, zmq::recv_flags::none)) {
                quant::proto::FillReport report;
                if (report.ParseFromArray(msg.data(), static_cast<int>(msg.size()))) {
                    engine.on_fill_report(report);
                } else {
                    LOG_WARN("Failed to parse fill report ({} bytes)", msg.size());
                }
            }
        }

        if (++status_counter >= 10) {
            status_counter = 0;
            auto status = engine.get_status();
            std::string data;
            status.SerializeToString(&data);
            zmq::message_t smsg(data.data(), data.size());
            status_pub.send(smsg, zmq::send_flags::dontwait);
        }
    }

    engine.shutdown();
    LOG_INFO("QuantEngine stopped cleanly");
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
