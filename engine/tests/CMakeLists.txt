find_package(GTest REQUIRED)

add_executable(quant_tests
    test_spsc_queue.cpp
    test_order_manager.cpp
    test_risk.cpp
    test_position_manager.cpp
)

target_link_libraries(quant_tests PRIVATE
    quant_core
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(quant_tests)
