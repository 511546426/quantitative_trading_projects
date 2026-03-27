"""
config.py - 统一配置管理

从 YAML 文件加载所有模块配置，支持环境变量覆盖。
"""
from __future__ import annotations
import os
import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class GatewayConfig:
    type: str = "mock"                  # mock / qmt
    qmt_path: str = ""                  # QMT 客户端安装路径
    account_id: str = ""                # 资金账号
    session_id: int = 0                 # 会话 ID（随机即可）

@dataclass
class RiskConfig:
    max_single_position_ratio: float = 0.10
    max_total_position_ratio: float = 0.95
    max_order_amount: float = 500_000.0
    max_daily_loss_ratio: float = 0.03
    max_drawdown_ratio: float = 0.15
    min_order_amount: float = 100.0
    max_active_orders_per_sym: int = 3
    allow_short: bool = False

@dataclass
class AlgoConfig:
    default_algo: str = "twap"          # twap / vwap / direct
    twap_slices: int = 5                # TWAP 分几笔
    twap_interval_sec: int = 60         # TWAP 间隔（秒）
    vwap_participation: float = 0.10    # VWAP 参与率
    price_tolerance: float = 0.005      # 相对于参考价的滑点容忍度

@dataclass
class PersistConfig:
    pg_host: str = "127.0.0.1"
    pg_port: int = 5432
    pg_user: str = "postgres"
    pg_password: str = ""
    pg_database: str = "quant"

@dataclass
class MonitorConfig:
    enable_wechat: bool = False
    wechat_webhook: str = ""
    enable_email: bool = False
    email_smtp: str = ""
    email_from: str = ""
    email_to: str = ""
    email_password: str = ""
    log_dir: str = "logs"

@dataclass
class TradingConfig:
    """交易时间与 A 股规则"""
    morning_start: str = "09:30"
    morning_end: str = "11:30"
    afternoon_start: str = "13:00"
    afternoon_end: str = "15:00"
    lot_size: int = 100                 # A 股最小交易单位
    commission_rate: float = 0.00025    # 佣金（万 2.5）
    commission_min: float = 5.0         # 最低佣金 5 元
    stamp_duty_rate: float = 0.001      # 印花税（千分之一，仅卖出）
    slippage_rate: float = 0.001        # 滑点估算

@dataclass
class EngineConfig:
    initial_cash: float = 20_000.0
    strategy_id: str = "reversal_v1"
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    algo: AlgoConfig = field(default_factory=AlgoConfig)
    persist: PersistConfig = field(default_factory=PersistConfig)
    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)


def _merge(dc_class, data: dict):
    """将 dict 合并到 dataclass（忽略未知字段）"""
    valid = {f.name for f in dc_class.__dataclass_fields__.values()}
    return dc_class(**{k: v for k, v in data.items() if k in valid})


def load_config(path: str | Path = "execution/config.yaml") -> EngineConfig:
    """加载配置，支持环境变量覆盖"""
    cfg = EngineConfig()
    p = Path(path)
    if p.exists():
        with open(p) as f:
            raw = yaml.safe_load(f) or {}
        if "initial_cash" in raw:
            cfg.initial_cash = raw["initial_cash"]
        if "strategy_id" in raw:
            cfg.strategy_id = raw["strategy_id"]
        for section, dc_cls in [
            ("gateway", GatewayConfig),
            ("risk", RiskConfig),
            ("algo", AlgoConfig),
            ("persist", PersistConfig),
            ("monitor", MonitorConfig),
            ("trading", TradingConfig),
        ]:
            if section in raw and isinstance(raw[section], dict):
                setattr(cfg, section, _merge(dc_cls, raw[section]))

    # 环境变量覆盖（敏感信息不入配置文件）
    if v := os.getenv("QMT_ACCOUNT"):
        cfg.gateway.account_id = v
    if v := os.getenv("QMT_PATH"):
        cfg.gateway.qmt_path = v
    if v := os.getenv("PG_PASSWORD"):
        cfg.persist.pg_password = v
    if v := os.getenv("WECHAT_WEBHOOK"):
        cfg.monitor.wechat_webhook = v
        cfg.monitor.enable_wechat = True

    return cfg
