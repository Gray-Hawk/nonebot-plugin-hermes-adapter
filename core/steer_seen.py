"""steer 呈现去重(alive/103 §14.3)。

已 steer 成功的消息在下一轮又会从 recent 历史里冒出来重复呈现。按 scope 记
短期名单(插件 m:id),recent 渲染前过滤;限额 200 条防无界增长。名单只是
呈现层去重,不影响入库/对账/寻址。

独立小模块而非 relay 内部:turn 渲染侧也要过滤(relay→turn 单向依赖,
放 relay 会循环 import)。
"""

from __future__ import annotations

from collections import deque
from typing import Sequence

from .message_buffer import BufferedMessage

_STEER_SEEN_MAX = 200
_steer_seen_order: dict[tuple[str, str], deque[int]] = {}
_steer_seen_ids: dict[tuple[str, str], set[int]] = {}


def _steer_seen_mark(scope: tuple[str, str], plugin_id: int) -> None:
    order = _steer_seen_order.setdefault(scope, deque())
    ids = _steer_seen_ids.setdefault(scope, set())
    if plugin_id in ids:
        return
    order.append(plugin_id)
    ids.add(plugin_id)
    while len(order) > _STEER_SEEN_MAX:
        ids.discard(order.popleft())


def _steer_seen_filter(
    scope: tuple[str, str], rows: Sequence[BufferedMessage],
) -> list[BufferedMessage]:
    ids = _steer_seen_ids.get(scope)
    if not ids:
        return list(rows)
    return [row for row in rows if row.id is None or row.id not in ids]


def _steer_seen_reset_for_test() -> None:
    _steer_seen_order.clear()
    _steer_seen_ids.clear()
