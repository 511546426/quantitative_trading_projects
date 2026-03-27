cmake_minimum_required(VERSION 3.20)
project(QuantEngine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ────────────────────────────────────────────────────────────
# 编译选项
# ────────────────────────────────────────────────────────────
add_compile_options(
    -O3
    -march=native
    -Wall -Wextra -Wpedantic
    -Wno-unused-parameter
    -fno-omit-frame-pointer
)

# Debug 模式启用 Address/UB Sanitizer
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-fsanitize=address,undefined -g)
    add_link_options(-fsanitize=address,undefined)
endif()

# ────────────────────────────────────────────────────────────
# 依赖（Conan 管理）
# ────────────────────────────────────────────────────────────
find_package(spdlog REQUIRED)
find_package(GTest  REQUIRED)
find_package(benchmark REQUIRED)
find_package(cppzmq REQUIRED)
find_package(yaml-cpp REQUIRED)

# ────────────────────────────────────────────────────────────
# Protobuf 代码生成
# ────────────────────────────────────────────────────────────
set(PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/proto/signal.proto")
set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/signal.pb.cc")
set(PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/signal.pb.h")
find_program(PROTOC_EXECUTABLE NAMES protoc REQUIRED)

add_custom_command(
    OUTPUT ${PROTO_SRC} ${PROTO_HDR}
    COMMAND ${PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
            ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating Protobuf C++ files"
)

add_library(quant_proto STATIC ${PROTO_SRC})
target_include_directories(quant_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(quant_proto PUBLIC protobuf)

# ────────────────────────────────────────────────────────────
# 核心引擎静态库
# ────────────────────────────────────────────────────────────
add_library(quant_core STATIC
    src/oms/order_manager.cpp
    src/oms/position_manager.cpp
    src/risk/pre_trade_check.cpp
    src/gateway/mock_gateway.cpp
)
target_include_directories(quant_core PUBLIC
    include/
    ${CMAKE_CURRENT_BINARY_DIR}   # protobuf 生成头文件
)
target_link_libraries(quant_core PUBLIC
    spdlog::spdlog
    yaml-cpp::yaml-cpp
)

# ────────────────────────────────────────────────────────────
# 主可执行文件
# ────────────────────────────────────────────────────────────
add_executable(quant_engine
    src/main.cpp
    src/signal_receiver.cpp
)
target_link_libraries(quant_engine PRIVATE
    quant_core
    quant_proto
    cppzmq
    spdlog::spdlog
)

# ────────────────────────────────────────────────────────────
# 单元测试
# ────────────────────────────────────────────────────────────
enable_testing()
add_subdirectory(tests)

# ────────────────────────────────────────────────────────────
# 性能基准测试
# ────────────────────────────────────────────────────────────
add_subdirectory(benchmarks)
