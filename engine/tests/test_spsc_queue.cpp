#include <gtest/gtest.h>
#include "quant/infra/spsc_queue.h"
#include <thread>
#include <vector>
#include <numeric>

using namespace quant::infra;

// ────────────────────────────────────────────────────────────
// 基础功能测试
// ────────────────────────────────────────────────────────────
TEST(SPSCQueue, BasicPushPop) {
    SPSCQueue<int, 8> q;
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size_approx(), 0u);

    EXPECT_TRUE(q.push(42));
    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.size_approx(), 1u);

    int val = 0;
    EXPECT_TRUE(q.pop(val));
    EXPECT_EQ(val, 42);
    EXPECT_TRUE(q.empty());
}

TEST(SPSCQueue, QueueFull) {
    SPSCQueue<int, 4> q;  // capacity = 3
    EXPECT_EQ(q.capacity(), 3u);

    EXPECT_TRUE(q.push(1));
    EXPECT_TRUE(q.push(2));
    EXPECT_TRUE(q.push(3));
    EXPECT_FALSE(q.push(4));  // 满了
}

TEST(SPSCQueue, PopEmpty) {
    SPSCQueue<int, 4> q;
    int val;
    EXPECT_FALSE(q.pop(val));
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(SPSCQueue, TryPop) {
    SPSCQueue<int, 4> q;
    q.push(99);
    auto val = q.try_pop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 99);
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(SPSCQueue, FIFOOrder) {
    SPSCQueue<int, 16> q;
    for (int i = 0; i < 10; ++i) q.push(i);

    for (int i = 0; i < 10; ++i) {
        int v;
        ASSERT_TRUE(q.pop(v));
        EXPECT_EQ(v, i);
    }
}

TEST(SPSCQueue, WrapAround) {
    SPSCQueue<int, 8> q;
    // 填满再消费，循环多次
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 5; ++i) q.push(i * 10);
        for (int i = 0; i < 5; ++i) {
            int v; q.pop(v);
            EXPECT_EQ(v, i * 10);
        }
    }
}

// ────────────────────────────────────────────────────────────
// 多线程正确性测试（SPSC：1 生产者 + 1 消费者）
// ────────────────────────────────────────────────────────────
TEST(SPSCQueue, MultiThreaded_SPSC) {
    static constexpr int N = 100'000;
    SPSCQueue<int, 1024> q;
    std::atomic<int> consumed{0};
    int64_t checksum_produce = 0, checksum_consume = 0;

    for (int i = 0; i < N; ++i) checksum_produce += i;

    std::thread producer([&]() {
        for (int i = 0; i < N; ++i) {
            while (!q.push(i)) std::this_thread::yield();
        }
    });

    std::thread consumer([&]() {
        int val;
        while (consumed.load(std::memory_order_relaxed) < N) {
            if (q.pop(val)) {
                checksum_consume += val;
                consumed.fetch_add(1, std::memory_order_relaxed);
            } else {
                std::this_thread::yield();
            }
        }
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(consumed.load(), N);
    EXPECT_EQ(checksum_produce, checksum_consume);
}

// ────────────────────────────────────────────────────────────
// 结构体类型
// ────────────────────────────────────────────────────────────
TEST(SPSCQueue, StructType) {
    struct Msg { int id; double val; };
    SPSCQueue<Msg, 8> q;

    q.push({1, 3.14});
    q.push({2, 2.71});

    Msg m;
    ASSERT_TRUE(q.pop(m));
    EXPECT_EQ(m.id, 1);
    EXPECT_DOUBLE_EQ(m.val, 3.14);

    ASSERT_TRUE(q.pop(m));
    EXPECT_EQ(m.id, 2);
}
