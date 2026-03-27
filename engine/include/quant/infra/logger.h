#pragma once
#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <memory>
#include <string>

namespace quant::infra {

// ────────────────────────────────────────────────────────────
// Logger - spdlog 的单例封装
//
// 日志等级（从低到高）：
//   TRACE < DEBUG < INFO < WARN < ERROR < CRITICAL
//
// 用法：
//   Logger::init("logs/engine.log");
//   LOG_INFO("Order submitted: {}", order_id);
// ────────────────────────────────────────────────────────────
class Logger {
public:
    static void init(const std::string& log_file = "logs/engine.log",
                     size_t max_bytes       = 100 * 1024 * 1024,  // 100 MB
                     size_t max_files       = 3,
                     spdlog::level::level_enum level = spdlog::level::info)
    {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_level(level);

        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            log_file, max_bytes, max_files);
        file_sink->set_level(spdlog::level::debug);

        auto logger = std::make_shared<spdlog::logger>(
            "quant",
            spdlog::sinks_init_list{console_sink, file_sink}
        );
        logger->set_level(spdlog::level::trace);
        logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
        logger->flush_on(spdlog::level::warn);

        spdlog::register_logger(logger);
        spdlog::set_default_logger(logger);
    }

    static std::shared_ptr<spdlog::logger> get() {
        return spdlog::default_logger();
    }
};

} // namespace quant::infra

// ────────────────────────────────────────────────────────────
// 全局日志宏（减少调用冗长度）
// ────────────────────────────────────────────────────────────
#define LOG_TRACE(...)    SPDLOG_TRACE(__VA_ARGS__)
#define LOG_DEBUG(...)    SPDLOG_DEBUG(__VA_ARGS__)
#define LOG_INFO(...)     SPDLOG_INFO(__VA_ARGS__)
#define LOG_WARN(...)     SPDLOG_WARN(__VA_ARGS__)
#define LOG_ERROR(...)    SPDLOG_ERROR(__VA_ARGS__)
#define LOG_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)
