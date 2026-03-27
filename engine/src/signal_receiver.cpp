#include "quant/infra/logger.h"
#include "quant/infra/timer.h"
#include "quant/core/order.h"
#include "quant/infra/spsc_queue.h"
#include "signal.pb.h"

#include <zmq.hpp>
#include <thread>
#include <atomic>
#include <string>
#include <functional>

namespace quant {

// ────────────────────────────────────────────────────────────
// SignalReceiver - 接收 Python 策略信号并转换为 OrderRequest
//
// 通信模式：ZMQ PULL socket（Python PUSH → C++ PULL）
// 传输层：IPC（同机通信，延迟 < 50μs）
// 序列化：Protobuf
// ────────────────────────────────────────────────────────────
class SignalReceiver {
public:
    using OnSignalBatch = std::function<void(const proto::SignalBatch&)>;

    explicit SignalReceiver(const std::string& endpoint = "ipc:///tmp/quant_signals")
        : endpoint_(endpoint), ctx_(1), socket_(ctx_, zmq::socket_type::pull)
    {}

    ~SignalReceiver() { stop(); }

    // ── 启动接收线程 ────────────────────────────────────────

    void start(OnSignalBatch callback) {
        callback_ = std::move(callback);
        running_  = true;

        socket_.bind(endpoint_);
        socket_.set(zmq::sockopt::rcvtimeo, 100);  // 100ms 超时，便于检查停止标志

        recv_thread_ = std::thread([this]() { recv_loop(); });
        LOG_INFO("SignalReceiver: listening on {}", endpoint_);
    }

    void stop() {
        running_ = false;
        if (recv_thread_.joinable()) recv_thread_.join();
        socket_.close();
        LOG_INFO("SignalReceiver: stopped");
    }

    [[nodiscard]] uint64_t messages_received() const noexcept {
        return msg_count_.load(std::memory_order_relaxed);
    }

private:
    void recv_loop() {
        while (running_) {
            zmq::message_t msg;
            const auto result = socket_.recv(msg, zmq::recv_flags::none);

            if (!result) {
                // rcvtimeo 超时，检查停止标志
                continue;
            }

            const int64_t recv_ns = infra::Clock::now_ns();

            proto::SignalBatch batch;
            if (!batch.ParseFromArray(msg.data(), static_cast<int>(msg.size()))) {
                LOG_WARN("SignalReceiver: failed to parse message ({} bytes)", msg.size());
                continue;
            }

            msg_count_.fetch_add(1, std::memory_order_relaxed);

            // 计算信号延迟（Python 生成时间 → C++ 接收时间）
            const int64_t latency_us = (recv_ns - batch.batch_time_ns()) / 1000;
            LOG_DEBUG("SignalReceiver: batch strategy={} signals={} latency={}μs",
                      batch.strategy_id(),
                      batch.signals_size(),
                      latency_us);

            if (latency_us > 5000) {
                LOG_WARN("SignalReceiver: high latency {}μs (>5ms), batch may be stale",
                         latency_us);
            }

            if (callback_) {
                callback_(batch);
            }
        }
    }

    std::string    endpoint_;
    zmq::context_t ctx_;
    zmq::socket_t  socket_;
    OnSignalBatch  callback_;
    std::thread    recv_thread_;
    std::atomic<bool>     running_{false};
    std::atomic<uint64_t> msg_count_{0};
};

// ────────────────────────────────────────────────────────────
// 将 proto::Signal 转换为引擎内 OrderRequest
// ────────────────────────────────────────────────────────────
inline OrderRequest signal_to_order_request(const proto::Signal& sig,
                                             double nav,
                                             Price  last_price) {
    OrderRequest req;
    req.symbol      = make_symbol(sig.symbol());
    req.strategy_id = static_cast<StrategyId>(std::hash<std::string>{}(sig.strategy_id()) & 0x7FFFFFFF);
    req.signal_ns   = sig.signal_time_ns();

    if (sig.close_position()) {
        req.side = Side::Sell;
        req.qty  = 0;  // PositionManager 实际会填入全部持仓
        return req;
    }

    const double target_amount = sig.target_weight() * nav;
    const Price  price         = (last_price > 0) ? last_price : sig.ref_price();

    if (price <= 0) {
        req.qty = 0;
        return req;
    }

    // 计算目标手数（A 股最小单位 100 股）
    const Quantity raw_qty = static_cast<Quantity>(target_amount / price);
    req.qty   = (raw_qty / 100) * 100;
    req.price = price;
    req.type  = OrderType::Limit;
    req.side  = Side::Buy;

    return req;
}

} // namespace quant
