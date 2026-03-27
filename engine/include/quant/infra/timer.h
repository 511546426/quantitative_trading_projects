#pragma once
#include <cstdint>
#include <algorithm>
#include <chrono>
#include <ctime>

namespace quant::infra {

// ────────────────────────────────────────────────────────────
// Clock - 高精度时间工具
// ────────────────────────────────────────────────────────────
struct Clock {
    // 当前纳秒时间戳（CLOCK_REALTIME，与市场时间对齐）
    [[nodiscard]] static int64_t now_ns() noexcept {
        struct timespec ts{};
        ::clock_gettime(CLOCK_REALTIME, &ts);
        return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;
    }

    // 单调时钟（用于测量延迟，不受 NTP 影响）
    [[nodiscard]] static int64_t mono_ns() noexcept {
        struct timespec ts{};
        ::clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;
    }

};

// ────────────────────────────────────────────────────────────
// Stopwatch - 测量代码段执行时间
// ────────────────────────────────────────────────────────────
class Stopwatch {
public:
    Stopwatch() : start_(Clock::mono_ns()) {}

    void reset() noexcept { start_ = Clock::mono_ns(); }

    [[nodiscard]] int64_t elapsed_ns() const noexcept {
        return Clock::mono_ns() - start_;
    }

    [[nodiscard]] double elapsed_us() const noexcept {
        return static_cast<double>(elapsed_ns()) / 1e3;
    }

    [[nodiscard]] double elapsed_ms() const noexcept {
        return static_cast<double>(elapsed_ns()) / 1e6;
    }

private:
    int64_t start_;
};

// ────────────────────────────────────────────────────────────
// RateLimit - 简单令牌桶（用于 API 调用限速）
// ────────────────────────────────────────────────────────────
class RateLimit {
public:
    // max_per_second: 每秒最多调用次数
    explicit RateLimit(double max_per_second) noexcept
        : interval_ns_(static_cast<int64_t>(1e9 / max_per_second))
        , last_ns_(Clock::mono_ns()) {}

    // 返回是否允许（不阻塞，调用方决定是否等待）
    [[nodiscard]] bool try_acquire() noexcept {
        const int64_t now = Clock::mono_ns();
        if (now - last_ns_ >= interval_ns_) {
            last_ns_ = now;
            return true;
        }
        return false;
    }

    // 返回距离下次可用的等待纳秒数（0 表示立即可用）
    [[nodiscard]] int64_t wait_ns() const noexcept {
        const int64_t elapsed = Clock::mono_ns() - last_ns_;
        return std::max(int64_t{0}, interval_ns_ - elapsed);
    }

private:
    int64_t interval_ns_;
    int64_t last_ns_;
};

} // namespace quant::infra
