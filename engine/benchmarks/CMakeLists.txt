find_package(benchmark REQUIRED)

add_executable(quant_benchmarks
    bench_spsc.cpp
    bench_risk.cpp
)

target_link_libraries(quant_benchmarks PRIVATE
    quant_core
    benchmark::benchmark
    benchmark::benchmark_main
)
