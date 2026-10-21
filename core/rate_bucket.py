"""唤起限频:per-target 令牌桶 + 独立配置文件 `<hermes_custom_dir>/wake_limits.yaml`。

设计定案:每 target 容量 3 格、每 300s 回 1 格(连发上限 3 条,持续 ≤12 条/小时);
超限直接拒+报错文案,不排队不延后。容量/速率可调(人改 YAML,现读现用);
文件缺失/坏档 → 缺省值(WARN 按内容去重,同 media_config)。
桶状态纯内存(重启即清,与限频语义兼容);key 由调用方按
`{adapter}:{scope}:{scope_id}` 形态构造,统一走单一来源键形。
"""

from __future__ import annotations

import math
import time

import yaml
from dataclasses import dataclass
from typing import Any

from nonebot import logger

from ..config import resolve_custom_dir

_CONFIG_FILENAME = "wake_limits.yaml"

_DEFAULT_CAPACITY = 3
_DEFAULT_REFILL_SECONDS = 300


@dataclass(frozen=True)
class WakeLimits:
    """唤起限频快照。frozen:单次判定内一份,改文件下一发生效。"""

    capacity: int = _DEFAULT_CAPACITY
    """每 target 令牌容量(连发上限)。"""
    refill_seconds: int = _DEFAULT_REFILL_SECONDS
    """每多少秒回 1 格。"""


def _int_field(doc: dict, key: str, *, default: int, min_v: int = 1) -> int:
    """取 int 字段,缺失/类型错/越界回退默认。"""
    node: Any = doc.get(key)
    if isinstance(node, bool) or not isinstance(node, int) or node < min_v:
        return default
    return node


_last_warn: str = ""


def load_wake_limits() -> WakeLimits:
    """加载 wake_limits.yaml。文件缺失/坏档 → 缺省(WARN 按内容去重)。

    现读现用:每次调用现读文件,保存即生效,不做缓存。
    """
    global _last_warn
    custom_dir = resolve_custom_dir()
    path = custom_dir / _CONFIG_FILENAME if custom_dir else None
    if path is None or not path.is_file():
        return WakeLimits()
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            raise ValueError("顶层必须是映射(YAML mapping)")
    except Exception as exc:
        warn = f"[HERMES wake-limits] 配置文件不可用,回退缺省: {exc}"
        if warn != _last_warn:
            logger.warning(warn)
            _last_warn = warn
        return WakeLimits()
    return WakeLimits(
        capacity=_int_field(doc, "capacity", default=_DEFAULT_CAPACITY),
        refill_seconds=_int_field(doc, "refill_seconds", default=_DEFAULT_REFILL_SECONDS),
    )


# key → (当前格数(float,允许小数累积), 上次结算时刻)。模块级状态 = 重启即清。
_BUCKETS: dict[str, tuple[float, float]] = {}


def try_acquire(
    key: str, *, capacity: int, refill_seconds: int, now: float | None = None
) -> bool:
    """取 1 格;取到 → True 并扣格,桶空 → False。

    离线回补:距上次结算每满 refill_seconds 回 1 格,封顶 capacity。
    now 可注入(测试/定时结算用);缺省 time.monotonic()。
    """
    t = time.monotonic() if now is None else now
    tokens, updated = _BUCKETS.get(key, (float(capacity), t))
    if refill_seconds > 0 and t > updated:
        tokens = min(float(capacity), tokens + (t - updated) / float(refill_seconds))
    if tokens >= 1.0:
        _BUCKETS[key] = (tokens - 1.0, t)
        return True
    _BUCKETS[key] = (tokens, t)
    return False


def seconds_until_next(key: str, *, refill_seconds: int) -> int:
    """距回满 1 格的等待秒数(向上取整);桶满或无记录 → 0。

    只读,不改状态;用于超限报错文案让调用方知道多久后能再试。
    """
    state = _BUCKETS.get(key)
    if state is None or refill_seconds <= 0:
        return 0
    tokens, _updated = state
    if tokens >= 1.0:
        return 0
    return max(1, math.ceil((1.0 - tokens) * refill_seconds))
