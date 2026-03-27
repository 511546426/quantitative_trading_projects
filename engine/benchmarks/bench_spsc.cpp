#include <benchmark/benchmark.h>
#include "quant/infra/spsc_queue.h"
#include <thread>
#include <atomic>

using namespace quant::infra;

// ────────────────────────────────────────────────────────────
// 单线程 push/pop 吞吐量
// ────────────────────────────────────────────────────────────
static void BM_SPSC_SingleThread(benchmark::State& state) {
    SPSCQueue<int, 1024> q;
    for (auto _ : state) {
        q.push(42);
        int v;
        q.pop(v);
        benchmark::DoNotOptimize(v);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SPSC_SingleThread)->ThreadRange(1, 1);

// ────────────────────────────────────────────────────────────
// 双线程端到端延迟（生产者发 N 条，消费者全收完的时间）
// ────────────────────────────────────────────────────────────
static void BM_SPSC_TwoThread_Throughput(benchmark::State& state) {
    SPSCQueue<int64_t, 4096> q;
    std::atomic<bool> ready{false};
    std::atomic<int64_t> total_consumed{0};
    const int64_t N = state.range(0);

    std::thread consumer([&]() {
        int64_t val, cnt = 0;
        while (!ready.load(std::memory_order_relaxed)) std::this_thread::yield();
        while (cnt < N) {
            if (q.pop(val)) ++cnt;
            else std::this_thread::yield();
        }
        total_consumed = cnt;
    });

    for (auto _ : state) {
        total_consumed = 0;
        ready = false;
        ready = true;
        for (int64_t i = 0; i < N; ++i) {
            while (!q.push(i)) std::this_thread::yield();
        }
        // 等待消费者消费完
        while (total_consumed.load() < N) std::this_thread::yield();
    }

    consumer.join();
    state.SetItemsProcessed(state.iterations() * N);
    state.SetBytesProcessed(state.iterations() * N * sizeof(int64_t));
}
BENCHMARK(BM_SPSC_TwoThread_Throughput)->Arg(10000)->Arg(100000);

BENCHMARK_MAIN();
