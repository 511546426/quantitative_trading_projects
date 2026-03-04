"""
配置管理器 — 加载 YAML + 环境变量，提供点号路径访问。

加载优先级（高 → 低）：
  1. 环境变量
  2. .env 文件
  3. YAML 配置文件（默认值）
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)(?::-(.*?))?\}")

_SENSITIVE_KEYS = {"password", "token", "secret", "webhook_url"}


class Config:
    """
    配置管理器。

    用法::

        cfg = Config.load("data/config/settings.yaml", "data/config/sources.yaml")
        host = cfg.get("database.clickhouse.host", "localhost")
    """

    def __init__(self, data: dict):
        self._data = data

    # ---- 构造 --------------------------------------------------------

    @classmethod
    def load(cls, *yaml_paths: str, env_file: str | None = None) -> Config:
        """
        合并多个 YAML 并替换 ``${VAR:-default}`` 占位符。

        Parameters
        ----------
        yaml_paths : str
            一个或多个 YAML 文件路径（相对于项目根目录）。
        env_file : str, optional
            .env 文件路径，默认自动检测项目根目录下的 ``.env``。
        """
        project_root = Path(__file__).resolve().parents[2]

        dotenv_path = Path(env_file) if env_file else project_root / ".env"
        if dotenv_path.exists():
            load_dotenv(dotenv_path)
            logger.info("已加载 .env: %s", dotenv_path)

        merged: dict = {}
        for rel in yaml_paths:
            p = project_root / rel if not Path(rel).is_absolute() else Path(rel)
            if not p.exists():
                logger.warning("配置文件不存在，跳过: %s", p)
                continue
            with open(p, encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            merged = _deep_merge(merged, raw)

        resolved = _resolve_env_vars(merged)
        return cls(resolved)

    # ---- 访问 --------------------------------------------------------

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        通过点号路径获取值::

            cfg.get("database.clickhouse.host")
        """
        keys = key_path.split(".")
        node = self._data
        for k in keys:
            if isinstance(node, dict) and k in node:
                node = node[k]
            else:
                return default
        return node

    def section(self, key_path: str) -> dict:
        """获取子字典（用于传给子模块初始化）"""
        val = self.get(key_path, {})
        return val if isinstance(val, dict) else {}

    @property
    def raw(self) -> dict:
        return self._data

    # ---- 展示 --------------------------------------------------------

    def masked_dump(self) -> dict:
        """返回遮蔽敏感字段的配置副本（用于日志打印）"""
        return _mask_sensitive(self._data)

    def __repr__(self) -> str:
        return f"Config({list(self._data.keys())})"


# ================================================================
# 内部工具函数
# ================================================================

def _deep_merge(base: dict, override: dict) -> dict:
    """递归合并字典，override 覆盖 base"""
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _resolve_env_vars(obj: Any) -> Any:
    """递归替换 ``${VAR:-default}`` 为环境变量值"""
    if isinstance(obj, str):
        def _replace(m: re.Match) -> str:
            var_name = m.group(1)
            default_val = m.group(2) if m.group(2) is not None else ""
            return os.environ.get(var_name, default_val)
        return _ENV_VAR_PATTERN.sub(_replace, obj)
    if isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_vars(item) for item in obj]
    return obj


def _mask_sensitive(obj: Any, _parent_key: str = "") -> Any:
    """将敏感字段值替换为 ****"""
    if isinstance(obj, dict):
        return {
            k: "****" if k in _SENSITIVE_KEYS and isinstance(v, str) and v
            else _mask_sensitive(v, k)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_mask_sensitive(item) for item in obj]
    return obj
