#pragma once
#include <array>
#include <cstddef>
#include <cassert>
#include <stdexcept>
#include <new>

namespace quant::infra {

// ────────────────────────────────────────────────────────────
// FixedPool<T, N> - 编译期固定容量的对象池
//
// 设计目标：在热路径（decision loop）中避免 malloc/free。
// 所有 T 对象从预分配的数组块中取出，释放后放回空闲链表。
//
// 约束：
//   - 非线程安全。若多线程使用，调用方需加锁或每线程一个池。
//   - 单次分配大小固定为 sizeof(T)。
// ────────────────────────────────────────────────────────────
template<typename T, size_t N>
class FixedPool {
public:
    FixedPool() {
        // 初始化空闲链表：所有节点依次相连
        for (size_t i = 0; i < N - 1; ++i) {
            free_list_[i] = i + 1;
        }
        free_list_[N - 1] = kNone;
        head_ = 0;
        available_ = N;
    }

    // 禁止拷贝
    FixedPool(const FixedPool&) = delete;
    FixedPool& operator=(const FixedPool&) = delete;

    // 分配一个 T 对象槽（不调用构造函数）
    // 返回 nullptr 表示池已耗尽
    [[nodiscard]] void* allocate() noexcept {
        if (head_ == kNone) return nullptr;
        const size_t idx = head_;
        head_ = free_list_[idx];
        --available_;
        return &storage_[idx];
    }

    // 构造 + 分配
    template<typename... Args>
    [[nodiscard]] T* construct(Args&&... args) {
        void* p = allocate();
        if (!p) throw std::bad_alloc{};
        return new (p) T(std::forward<Args>(args)...);
    }

    // 析构 + 回收
    void destroy(T* ptr) noexcept {
        if (!ptr) return;
        ptr->~T();
        deallocate(ptr);
    }

    // 仅回收（调用方已析构）
    void deallocate(void* ptr) noexcept {
        if (!ptr) return;
        const size_t idx = static_cast<std::byte*>(ptr) - reinterpret_cast<std::byte*>(&storage_[0]);
        const size_t slot = idx / sizeof(T);
        assert(slot < N);
        free_list_[slot] = head_;
        head_ = slot;
        ++available_;
    }

    [[nodiscard]] size_t available() const noexcept { return available_; }
    [[nodiscard]] size_t capacity()  const noexcept { return N; }
    [[nodiscard]] bool   full()      const noexcept { return available_ == N; }
    [[nodiscard]] bool   empty()     const noexcept { return available_ == 0; }

private:
    static constexpr size_t kNone = SIZE_MAX;

    // 对象存储（原始字节，按 T 对齐）
    alignas(T) std::array<std::byte[sizeof(T)], N> storage_;

    // 空闲链表：free_list_[i] 存储下一个空闲槽的索引
    std::array<size_t, N> free_list_;

    size_t head_      = kNone;
    size_t available_ = 0;
};

} // namespace quant::infra
