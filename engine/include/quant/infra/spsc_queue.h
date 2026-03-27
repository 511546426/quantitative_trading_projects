#pragma once
#include <atomic>
#include <array>
#include <optional>
#include <cstddef>

namespace quant::infra {

// ────────────────────────────────────────────────────────────
// SPSCQueue<T, N> - 单生产者单消费者无锁队列
//
// 典型用途：
//   线程 A（生产者）→ 线程 B（消费者）的单向数据通道
//   策略线程 → 风控/OMS 线程
//   OMS 线程 → Gateway 线程
//
// 性能特点：
//   - push/pop 均为 O(1)，无锁，无等待
//   - 两端分别独占一个 cache line，彻底消除 false sharing
//   - N 必须是 2 的幂，mod 操作变成 bit AND
//
// 使用约束：
//   - 同一时刻只能有 1 个线程调用 push
//   - 同一时刻只能有 1 个线程调用 pop
// ────────────────────────────────────────────────────────────
template<typename T, size_t N>
class SPSCQueue {
    static_assert((N & (N - 1)) == 0, "N must be a power of 2");
    static_assert(N >= 2, "N must be at least 2");

public:
    SPSCQueue() = default;

    // 禁止拷贝和移动：队列对象通常是全局/线程局部的
    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;

    // ── 生产者接口 ──────────────────────────────────────────

    // 入队（非阻塞）。返回 false 表示队列已满。
    [[nodiscard]] bool push(const T& item) noexcept {
        return emplace(item);
    }

    [[nodiscard]] bool push(T&& item) noexcept {
        return emplace(std::move(item));
    }

    template<typename... Args>
    [[nodiscard]] bool emplace(Args&&... args) noexcept {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & kMask;

        // 队满：next 追上了 tail
        if (next == tail_.load(std::memory_order_acquire))
            return false;

        new (&buffer_[head]) T(std::forward<Args>(args)...);
        head_.store(next, std::memory_order_release);
        return true;
    }

    // ── 消费者接口 ──────────────────────────────────────────

    // 出队（非阻塞）。返回 false 表示队列为空。
    [[nodiscard]] bool pop(T& item) noexcept {
        const size_t tail = tail_.load(std::memory_order_relaxed);

        // 队空：tail 追上了 head
        if (tail == head_.load(std::memory_order_acquire))
            return false;

        item = std::move(*reinterpret_cast<T*>(&buffer_[tail]));
        reinterpret_cast<T*>(&buffer_[tail])->~T();
        tail_.store((tail + 1) & kMask, std::memory_order_release);
        return true;
    }

    // 返回 optional，无元素时返回 nullopt
    [[nodiscard]] std::optional<T> try_pop() noexcept {
        T item;
        if (pop(item)) return item;
        return std::nullopt;
    }

    // ── 状态查询 ────────────────────────────────────────────

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire)
            == tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] size_t size_approx() const noexcept {
        const size_t h = head_.load(std::memory_order_acquire);
        const size_t t = tail_.load(std::memory_order_acquire);
        return (h - t) & kMask;
    }

    static constexpr size_t capacity() noexcept { return N - 1; }

private:
    static constexpr size_t kMask = N - 1;

    // 生产者端独占一个 cache line
    alignas(64) std::atomic<size_t> head_{0};

    // 消费者端独占一个 cache line
    alignas(64) std::atomic<size_t> tail_{0};

    // 环形缓冲区
    alignas(alignof(T)) std::array<std::byte[sizeof(T)], N> buffer_;
};

// ────────────────────────────────────────────────────────────
// 常用队列容量别名（2 的幂次）
// ────────────────────────────────────────────────────────────
template<typename T> using SignalQueue  = SPSCQueue<T, 1024>;
template<typename T> using OrderQueue   = SPSCQueue<T, 2048>;
template<typename T> using FillQueue    = SPSCQueue<T, 4096>;

} // namespace quant::infra
