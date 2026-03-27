#pragma once
#include "base_gateway.h"
#include <memory>
#include <string>
#include <stdexcept>

namespace quant::gateway {

// ────────────────────────────────────────────────────────────
// GatewayFactory - 根据配置创建对应 Gateway 实例
//
// 支持的类型：
//   "mock"  - MockGateway（模拟撮合，用于测试/回测）
//   "xtp"   - XtpGateway（券商 XTP 接口，需要 XTP SDK）
//   "qmt"   - QmtGateway（迅投 miniQMT 接口）
// ────────────────────────────────────────────────────────────
class GatewayFactory {
public:
    static std::unique_ptr<IBaseGateway> create(const std::string& type,
                                                 const std::string& config_path = "");
};

} // namespace quant::gateway
