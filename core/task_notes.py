"""插件提示便签(错误分级反馈+离场确认轮方案)。

插件在检测到文书/信封错误时写入这里,下一轮构造 user content 时弹出并贴到
current_message 附近——不拦截不当轮打断;错误已发出覆水难收,反馈回路让模型
下轮自知。

Key 与 inflight 同形(构造统一走 core/inflight.make_inflight_key):
(adapter, "group:<id>" / "private:<id>")。

离场任务便签的登记点已删除:旧 `background_task` 登记只有写、没有弹出消费者,
保留只会积内存而不产生任何行为。
"""

from __future__ import annotations

import time

# scope_key -> [提示便签, ...](FIFO,下一轮一次性弹出)
_plugin_notes: dict[tuple[str, str], list[str]] = {}
# (scope_key, 提示种类) -> 上次提示时间戳(同类提示 24h 内不重复,防提醒通胀)
_hint_marks: dict[tuple[tuple[str, str], str], float] = {}

_HINT_COOLDOWN_SEC = 24 * 3600

# 内存兜底(本部署补丁):_hint_marks 是纯内存态只增不减,长跑缓慢泄漏。
# 淘汰时长刻意远超实用冷却,只作内存边界,不改变去重语义。
_HINT_MARK_KEEP_SEC = 7 * 24 * 3600
_TABLE_MAX = 512


def _sweep_expired(table: dict, cutoff: float, *, ts_of) -> None:
    """淘汰 ts 早于 cutoff 的条目;仍超容量再按 ts 从旧到新削到半容量。"""
    if len(table) <= _TABLE_MAX:
        return
    dead = [k for k, v in table.items() if ts_of(v) < cutoff]
    if len(table) - len(dead) > _TABLE_MAX:
        rest = sorted((k for k in table if k not in dead), key=lambda k: ts_of(table[k]))
        dead += rest[: len(table) - len(dead) - _TABLE_MAX // 2]
    for k in dead:
        table.pop(k, None)


def push_plugin_note(key: tuple[str, str], kind: str, text: str, cooldown_sec: float = _HINT_COOLDOWN_SEC) -> bool:
    """写入一条插件提示便签。kind 用于去重;重复期内返回 False 不写。"""
    now = time.time()
    mark = (key, kind)
    if now - _hint_marks.get(mark, 0) < cooldown_sec:
        return False
    _hint_marks[mark] = now
    _sweep_expired(_hint_marks, now - _HINT_MARK_KEEP_SEC, ts_of=float)
    _plugin_notes.setdefault(key, []).append(text)
    return True


def pop_plugin_notes(key: tuple[str, str]) -> list[str]:
    """取出并清空该 scope 的提示便签(构造下一轮 content 时消费)。"""
    return _plugin_notes.pop(key, [])
