# C++ 交易引擎设计文档

> 这是系统最核心的技术模块，也是私募技术壁垒所在。
> 设计目标：低延迟（μs 级）、高可靠、可扩展。

---

## 设计哲学

```
1. 数据驱动设计（Data-Oriented Design）
   └── 结构体成员按访问模式排列，最大化缓存命中率

2. 无锁优先（Lock-free First）
   └── 热路径上不用互斥锁，用 SPSC / MPSC 无锁队列

3. 零拷贝通信（Zero-copy）
   └── 共享内存 / 引用语义，避免不必要的数据复制

4. 确定性（Determinism）
   └── 避免动态内存分配（hot path），使用内存池
```

---

## 项目目录结构

```
engine/
├── CMakeLists.txt
├── conanfile.txt                    # 依赖管理
├── include/
│   └── quant/
│       ├── core/
│       │   ├── types.h              # 基础类型定义
│       │   ├── order.h              # 订单结构
│       │   ├── trade.h              # 成交结构
│       │   ├── position.h           # 持仓结构
│       │   ├── bar.h                # K 线结构
│       │   └── event.h              # 事件定义
│       ├── infra/
│       │   ├── spsc_queue.h         # 无锁单生产者单消费者队列
│       │   ├── memory_pool.h        # 内存池
│       │   ├── timer.h              # 高精度定时器
│       │   └── logger.h             # 日志接口
│       ├── oms/
│       │   ├── order_manager.h
│       │   ├── position_manager.h
│       │   └── pnl_calculator.h
│       ├── risk/
│       │   ├── pre_trade_check.h
│       │   └── real_time_monitor.h
│       └── gateway/
│           ├── base_gateway.h
│           └── gateway_factory.h
└── src/
    ├── oms/
    ├── risk/
    ├── gateway/
    │   ├── xtp_gateway.cpp
    │   └── mock_gateway.cpp         # 模拟撮合，用于测试
    ├── signal_receiver.cpp          # 接收 Python 策略信号
    └── main.cpp
```

---

## 核心数据结构设计

### 订单结构（cache-line 对齐）

```cpp
// order.h
#pragma once
#include <cstdint>
#include <array>

namespace quant {

// 方向
enum class Side : uint8_t { Buy = 0, Sell = 1 };

// 订单类型
enum class OrderType : uint8_t { Market = 0, Limit = 1, FAK = 2, FOK = 3 };

// 订单状态
enum class OrderStatus : uint8_t {
    New       = 0,
    Submitted = 1,
    Partial   = 2,
    Filled    = 3,
    Cancelled = 4,
    Rejected  = 5,
};

// 设计要点：
// 1. 使用固定大小数组存储股票代码，避免 std::string 的堆分配
// 2. 成员按大小从大到小排列，减少 padding
// 3. alignas(64) 确保不跨 cache line
struct alignas(64) Order {
    // 8 字节
    int64_t  order_id;
    int64_t  strategy_id;
    
    // 8 字节
    int64_t  created_ns;        // 创建时间（纳秒时间戳）
    int64_t  submitted_ns;      // 发单时间
    
    // 8 字节
    double   limit_price;       // 限价单价格，市价单为 0
    double   avg_fill_price;    // 平均成交价
    
    // 8 字节
    int64_t  target_qty;        // 目标数量（股）
    int64_t  filled_qty;        // 已成交数量
    
    // 12 字节
    std::array<char, 12> symbol; // "000001.SZ\0\0\0"
    
    // 3 字节
    Side       side;
    OrderType  type;
    OrderStatus status;
    
    // padding 由编译器自动添加到 64 字节
};

static_assert(sizeof(Order) <= 64, "Order should fit in one cache line");

} // namespace quant
```

### SPSC 无锁队列

```cpp
// spsc_queue.h - 单生产者单消费者无锁队列
// 典型场景：策略线程（生产者）→ 风控/OMS 线程（消费者）

template<typename T, size_t N>
class SPSCQueue {
    static_assert((N & (N - 1)) == 0, "N must be power of 2");
    
public:
    // 生产者调用（只有一个线程可以调用）
    bool push(const T& item) noexcept {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & mask_;
        
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;  // 队列满
        }
        
        buffer_[head] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }
    
    // 消费者调用（只有一个线程可以调用）
    bool pop(T& item) noexcept {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        
        if (tail == head_.load(std::memory_order_acquire)) {
            return false;  // 队列空
        }
        
        item = buffer_[tail];
        tail_.store((tail + 1) & mask_, std::memory_order_release);
        return true;
    }

private:
    static constexpr size_t mask_ = N - 1;
    
    alignas(64) std::atomic<size_t> head_{0};  // 生产者端，独占 cache line
    alignas(64) std::atomic<size_t> tail_{0};  // 消费者端，独占 cache line
    std::array<T, N> buffer_;
};
```

---

## 线程模型

```
┌─────────────────────────────────────────────────────────┐
│ 线程 1：信号接收线程（Signal Receiver）                   │
│   │  接收 ZMQ 消息，解析信号，投入信号队列               │
│   └─→ [SPSC Queue: SignalQueue]                          │
└─────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│ 线程 2：核心决策线程（Decision Engine）- 最重要！        │
│   │  从信号队列取信号 → 风控校验 → 生成订单 → 投入订单队列 │
│   │  【热路径，禁止任何系统调用/内存分配】               │
│   └─→ [SPSC Queue: OrderQueue]                          │
└─────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│ 线程 3：Gateway 线程（Order Sender）                     │
│   │  从订单队列取订单 → 调用券商 API → 发单              │
│   └─→ [SPSC Queue: FillQueue]                           │
└─────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│ 线程 4：成交处理线程（Fill Processor）                   │
│   ・更新持仓、计算 PnL、持久化到数据库                   │
│   ・此线程可以有 IO，不在关键路径                        │
└─────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│ 线程 5：监控线程（Monitor）                              │
│   ・定期检查回撤、持仓暴露、策略状态                     │
│   ・触发风控熔断信号                                     │
└─────────────────────────────────────────────────────────┘
```

---

## Gateway 抽象接口设计

```cpp
// base_gateway.h - 支持多券商，面向接口编程

namespace quant {

// 回调接口：券商事件回调到引擎
class IGatewayCallback {
public:
    virtual ~IGatewayCallback() = default;
    virtual void on_order_accepted(const Order& order) = 0;
    virtual void on_order_rejected(int64_t order_id, const std::string& reason) = 0;
    virtual void on_trade(const Trade& trade) = 0;
    virtual void on_position_update(const Position& pos) = 0;
    virtual void on_error(int error_code, const std::string& msg) = 0;
};

// 抽象 Gateway 接口
class IBaseGateway {
public:
    virtual ~IBaseGateway() = default;
    
    // 连接管理
    virtual bool connect() = 0;
    virtual void disconnect() = 0;
    virtual bool is_connected() const = 0;
    
    // 订单操作
    virtual int64_t send_order(const Order& order) = 0;  // 返回本地 order_id
    virtual bool cancel_order(int64_t order_id) = 0;
    
    // 查询（同步）
    virtual std::vector<Position> query_positions() = 0;
    virtual double query_cash() = 0;
    
    // 注册回调
    void set_callback(IGatewayCallback* cb) { callback_ = cb; }
    
protected:
    IGatewayCallback* callback_ = nullptr;
};

} // namespace quant
```

---

## 风控引擎（Pre-trade Check）

```cpp
// pre_trade_check.h

namespace quant::risk {

struct RiskConfig {
    double max_single_position_ratio = 0.10;   // 单股最大仓位比例
    double max_total_position_ratio  = 0.80;   // 最大总仓位比例
    double max_sector_position_ratio = 0.30;   // 单行业最大仓位
    double max_order_amount          = 500000; // 单笔最大金额（元）
    double max_daily_loss_ratio      = 0.03;   // 日内最大亏损
    double max_drawdown_ratio        = 0.15;   // 最大回撤触发熔断
};

enum class RiskResult : uint8_t {
    Pass     = 0,
    Reject   = 1,   // 直接拒绝
    Reduce   = 2,   // 降低数量后通过
};

class PreTradeChecker {
public:
    explicit PreTradeChecker(const RiskConfig& config) : cfg_(config) {}
    
    // 核心方法：检查订单是否合规
    // 返回 Pass/Reject/Reduce
    RiskResult check(Order& order,                    // 可能被修改（降量）
                     const PortfolioSnapshot& snap,   // 当前持仓快照
                     double total_capital) const noexcept;
    
private:
    bool check_position_limit(const Order& order, 
                              const PortfolioSnapshot& snap,
                              double total_capital) const noexcept;
    
    bool check_daily_loss(const PortfolioSnapshot& snap,
                          double total_capital) const noexcept;
    
    bool check_circuit_breaker() const noexcept; // 全局熔断开关
    
    RiskConfig cfg_;
    std::atomic<bool> circuit_breaker_{false};  // 熔断标志
};

} // namespace quant::risk
```

---

## CMake 项目配置

```cmake
# CMakeLists.txt
cmake_minimum_required(VERSION 3.20)
project(QuantEngine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 编译选项
add_compile_options(
    -O3                    # 最高优化级别
    -march=native          # 针对当前 CPU 指令集优化
    -Wall -Wextra          # 开启所有警告
    -fno-omit-frame-pointer  # 保留帧指针，方便 perf 分析
)

# 依赖（通过 Conan 管理）
find_package(spdlog REQUIRED)     # 日志
find_package(GTest REQUIRED)      # 单元测试
find_package(benchmark REQUIRED)  # 性能测试
find_package(cppzmq REQUIRED)     # ZMQ 通信
find_package(protobuf REQUIRED)   # 序列化

# 核心引擎库
add_library(quant_core STATIC
    src/oms/order_manager.cpp
    src/oms/position_manager.cpp
    src/risk/pre_trade_check.cpp
    src/gateway/mock_gateway.cpp
)
target_include_directories(quant_core PUBLIC include/)
target_link_libraries(quant_core PRIVATE spdlog::spdlog)

# 主程序
add_executable(quant_engine src/main.cpp src/signal_receiver.cpp)
target_link_libraries(quant_engine PRIVATE quant_core cppzmq protobuf)

# 单元测试
add_subdirectory(tests)

# 性能测试
add_subdirectory(benchmarks)
```

---

## Python-C++ 通信设计（ZeroMQ）

```
Python 策略进程                    C++ 引擎进程
───────────────────                ──────────────────
strategy.py                        signal_receiver.cpp
    │                                      │
    │  ZMQ PUB/SUB 或 PUSH/PULL           │
    │  (IPC socket: unix:///tmp/signals)   │
    │──────────────────────────────────────▶│
    │                                      │
    │  消息格式（Protobuf）                │
    │  message Signal {                    │
    │    string symbol = 1;                │
    │    double target_weight = 2;         │
    │    int64 signal_time_ns = 3;         │
    │    string strategy_id = 4;           │
    │  }                                   │
```

```python
# Python 端发信号示例
import zmq
import signal_pb2  # protobuf 生成的 Python 类

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("ipc:///tmp/quant_signals")

def send_signal(symbol: str, target_weight: float, strategy_id: str):
    msg = signal_pb2.Signal()
    msg.symbol = symbol
    msg.target_weight = target_weight
    msg.signal_time_ns = time.time_ns()
    msg.strategy_id = strategy_id
    socket.send(msg.SerializeToString())
```

---

## 性能目标

| 指标 | 目标值 | 测量方法 |
|------|--------|----------|
| 信号到发单延迟 | < 1ms（软实时） | `submitted_ns - signal_time_ns` |
| OMS 处理延迟 | < 100μs | Google Benchmark |
| 风控检查延迟 | < 10μs | Google Benchmark |
| 每秒处理订单数 | > 10,000 | 压力测试 |
| 内存占用 | < 500MB | `valgrind massif` |

> 注：A 股日频策略不需要极限低延迟，目标是工程完整性和可扩展性，
> 真正的 HFT 需要 FPGA + 内核旁路，超出个人实盘范围。

---

## 开发顺序建议

```
Week 1-2: 基础设施
  └── types.h / order.h / trade.h / position.h
  └── spsc_queue.h（带完整单元测试）
  └── 日志集成（spdlog）
  └── CMake 构建系统

Week 3-4: Mock 闭环
  └── mock_gateway.cpp（模拟撮合引擎）
  └── order_manager.cpp
  └── position_manager.cpp
  └── 完整流程测试（下单→成交→持仓更新）

Week 5-6: 风控 + 实际 Gateway
  └── pre_trade_check.cpp
  └── XTP 或 QMT Gateway 封装

Week 7-8: Python 对接
  └── Protobuf 定义 + 代码生成
  └── ZMQ 信号接收器
  └── 端到端联调测试
```
