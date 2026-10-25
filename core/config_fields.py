"""YAML 标量字段的保守解析：单字段坏值回默认，顶档/schema 异常由上层处理。"""
from __future__ import annotations

from typing import Any

from nonebot import logger

# 非法枚举按“键 + 原值”去重；重载反复失败不会每次刷同一条 WARN。
_WARNED_CHOICES: set[tuple[str, str]] = set()


def bool_field(doc: dict, key: str, default: bool) -> bool:
    value = doc.get(key, default)
    return value if type(value) is bool else default


def int_field(doc: dict, key: str, default: int, *, min_v: int = 0) -> int:
    value = doc.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < min_v:
        return default
    return value


def float_field(doc: dict, key: str, default: float, *, min_v: float = 0.0) -> float:
    value = doc.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < min_v:
        return default
    return float(value)


def str_field(doc: dict, key: str, default: str = "") -> str:
    value = doc.get(key, default)
    return value if isinstance(value, str) else default


def str_set_field(doc: dict, key: str, default: set[str]) -> set[str]:
    value: Any = doc.get(key, default)
    if isinstance(value, (list, tuple, set)) and all(isinstance(item, str) for item in value):
        return set(value)
    return set(default)


def choice_field(doc: dict, key: str, default: str, choices: tuple[str, ...]) -> str:
    value = doc.get(key, default)
    if isinstance(value, str) and value in choices:
        return value
    marker = (key, repr(value))
    if marker not in _WARNED_CHOICES:
        _WARNED_CHOICES.add(marker)
        logger.warning(f"[HERMES config] {key}={value!r} 非法，回退 {default!r}；可选值={choices}")
    return default
