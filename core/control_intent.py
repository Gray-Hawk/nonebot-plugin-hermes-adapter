"""Runs 控制意图的进程内队列（控制输入从信封控制行改为可选 MCP 工具）。

主人定案的超简单语义：控制只是插件功能，工具只登记、不直接发消息/退窗/钉锚；
一份控制配给接下来的一份明确正文决策（空 body 也占一份），重叠按登记先后顺序
依次消费；没有控制就一律缺省。工具失败、断流、NB 重启导致控制丢失时，正文仍按
缺省控制发送——控制是增强，绝不吞正文。

实现刻意只有一张进程内表：
- 键与 inflight 同形（make_inflight_key）：群 `(adapter, "group:<id>")`、
  私聊 `(adapter, "private:<id>")`；
- 每键一个 deque，登记入队、决策到达时弹出；
- 不做持久化、不做跨重启恢复、不建新表——重启即丢，正文照发。

决策侧只挂私有键（`_control` / `_control_bound`）：`runs_terminal.normalize_decision`
与流式折叠只比较非 `_` 前缀字段，所以控制不进计划、不改变同文折叠与 E2/E3 台账口径。
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any

from .decision_contract import parse_reply_to

# 控制字段 = 现有真实字段，一个不多一个不少。not_final 不在此列：它仍是信封
# 旧兼容字段（completions 正常用法、Runs 旧信封继续认），不进控制队列。
CONTROL_FIELDS: tuple[str, ...] = (
    "reply_to",
    "should_exit_active",
    "pin_anchor",
    "release_anchor",
    "answer_anchor",
    "anchor_merge",
)
CONTROL_BOOL_FIELDS: tuple[str, ...] = ("should_exit_active", "pin_anchor")
CONTROL_STR_FIELDS: tuple[str, ...] = ("reply_to", "release_anchor", "answer_anchor", "anchor_merge")

# MCP 工具名。runs 流里该工具的 tool.started/tool.completed 不算「普通工作工具」
# 的封批边界（见 hermes_client.RunsEventParser），所以这个名字是唯一的跨模块契约。
CONTROL_TOOL_NAME = "submit_control"

# 队列上限：控制只是增强，积压就丢最旧的（丢控制不丢正文）。
MAX_PENDING_CONTROLS = 8


class ControlIntentError(ValueError):
    """要登记的控制不合法：调用方按「控制丢失」处理，正文照发。"""


def build_control(
    *,
    reply_to: Any = None,
    should_exit_active: Any = None,
    pin_anchor: Any = None,
    release_anchor: Any = None,
    answer_anchor: Any = None,
    anchor_merge: Any = None,
) -> dict[str, Any]:
    """把一次工具调用的入参收敛成一份控制（只保留真正给了值的字段）。

    - 布尔字段只接受 bool；false 等于缺省，不登记（避免空控制占掉一份正文的配额）；
    - 字符串字段去空白后为空即不登记；
    - reply_to 只接受 `m:<正整数>`（与信封同一份判定），别的形态直接拒——
      引用挂不上是可以接受的降级，但把裸数字/QQ id 当 m 号发出去不行。
    """
    control: dict[str, Any] = {}
    if reply_to not in (None, ""):
        if not isinstance(reply_to, str) or parse_reply_to(reply_to) is None:
            raise ControlIntentError("reply_to 只接受本会话消息编号 `m:<正整数>`")
        control["reply_to"] = reply_to
    for name, value in (("should_exit_active", should_exit_active), ("pin_anchor", pin_anchor)):
        if value is None:
            continue
        if not isinstance(value, bool):
            raise ControlIntentError(f"{name} 必须是布尔值")
        if value:
            control[name] = True
    for name, value in (
        ("release_anchor", release_anchor),
        ("answer_anchor", answer_anchor),
        ("anchor_merge", anchor_merge),
    ):
        if value is None:
            continue
        if isinstance(value, bool):
            # 旧信封容忍 true 视同 "all"；控制工具只收字符串，别把布尔悄悄翻译。
            raise ControlIntentError(f"{name} 必须是字符串（user_id 或 \"all\"）")
        text = str(value).strip()
        if text:
            control[name] = text
    return control


class ControlQueue:
    """按会话键维护的简单 FIFO；只登记、只按决策顺序弹出。"""

    def __init__(self, *, max_pending: int = MAX_PENDING_CONTROLS) -> None:
        self._queues: dict[tuple[str, str], deque[dict[str, Any]]] = {}
        self._lock = threading.Lock()
        self.max_pending = max_pending

    def register(self, key: tuple[str, str], control: dict[str, Any]) -> int:
        """登记一份控制，返回登记后队列深度。空控制不占位。"""
        if not control:
            return self.pending(key)
        with self._lock:
            queue = self._queues.setdefault(key, deque())
            if len(queue) >= self.max_pending:
                queue.popleft()
            queue.append(dict(control))
            return len(queue)

    def pop(self, key: tuple[str, str]) -> dict[str, Any] | None:
        with self._lock:
            queue = self._queues.get(key)
            if not queue:
                return None
            control = queue.popleft()
            if not queue:
                self._queues.pop(key, None)
            return control

    def pending(self, key: tuple[str, str]) -> int:
        with self._lock:
            queue = self._queues.get(key)
            return len(queue) if queue else 0

    def discard(self, key: tuple[str, str]) -> int:
        """丢弃该会话剩余控制（一轮结束时的收尾）：不跨轮误配给下一份正文。"""
        with self._lock:
            queue = self._queues.pop(key, None)
            return len(queue) if queue else 0


CONTROL_QUEUE = ControlQueue()


def bind_control(
    decision: Any, key: tuple[str, str], *, queue: ControlQueue = CONTROL_QUEUE
) -> dict[str, Any] | None:
    """给一份决策配控制：一份决策弹一份控制，空 body 同样占一份。

    只挂私有键，计划器与流式折叠的比较口径不受影响。已经配过的决策（同一批里
    即时发送与终态各过一次）不再弹第二份。返回该决策绑到的控制（可能为 None）。
    """
    if not isinstance(decision, dict) or decision.get("_control_bound"):
        return decision.get("_control") if isinstance(decision, dict) else None
    decision["_control_bound"] = True
    control = queue.pop(key)
    if control:
        decision["_control"] = control
    return control


def resolve_control(decision: Any) -> dict[str, Any]:
    """决策最终生效的控制：工具登记值兜底缺省，旧信封控制行原样优先。

    旧控制行是「在途兼容」：模型若仍按老两行写字段，那些字段照常生效；工具登记的
    值只补旧行为空/False 的字段。返回值恒含全部六个字段（字符串 ""、布尔 False）。
    """
    effective: dict[str, Any] = {}
    control = (decision or {}).get("_control") if isinstance(decision, dict) else None
    control = control if isinstance(control, dict) else {}
    for name in CONTROL_FIELDS:
        default: Any = False if name in CONTROL_BOOL_FIELDS else ""
        legacy = (decision or {}).get(name) if isinstance(decision, dict) else None
        effective[name] = legacy or control.get(name) or default
    return effective


def control_value(decision: Any, name: str) -> Any:
    """单字段便捷读：`control_value(decision, "reply_to")`。"""
    return resolve_control(decision)[name]
