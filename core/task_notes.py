"""离场任务便签 + 插件提示便签(2026-10-13,错误分级反馈+离场确认轮方案)。

两个用途,都是 per-scope 的纯内存态(重启即清,丢失代价=少一条提示,可接受):

1. **离场任务便签**:模型派后台任务时在 submit_decision 里填 `background_task`
   便签;delivery poll 发现该会话任务完成、唤起汇报轮时把便签拼进引导语
   (防上下文压缩后模型忘记自己派过什么活),汇报轮点火后清除。
2. **插件提示便签**:文书错误暗示(如直角引号检出)写入这里,下一轮构造
   user content 时弹出并贴到 current_message 附近——不拦截不当轮打断,
   错误已发出覆水难收,反馈回路让模型下轮自知。

Key 与 inflight 同形(构造统一走 core/inflight.make_inflight_key):
(adapter, "group:<id>" / "private:<id>")。
"""

from __future__ import annotations

import time

# scope_key -> (任务便签, 登记时间戳)
_task_notes: dict[tuple[str, str], tuple[str, float]] = {}
# scope_key -> [提示便签, ...](FIFO,下一轮一次性弹出)
_plugin_notes: dict[tuple[str, str], list[str]] = {}
# (scope_key, 提示种类) -> 上次提示时间戳(同类提示 24h 内不重复,防提醒通胀)
_hint_marks: dict[tuple[tuple[str, str], str], float] = {}

_HINT_COOLDOWN_SEC = 24 * 3600

# 内存兜底(本部署补丁):_task_notes/_hint_marks 是纯内存态只增不减,长跑缓慢
# 泄漏。淘汰时长刻意远超实用冷却/便签寿命,只作内存边界,不改变去重与便签语义。
_TASK_NOTE_KEEP_SEC = 72 * 3600
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


def set_task_note(key: tuple[str, str], note: str) -> None:
    _task_notes[key] = (note.strip(), time.time())
    _sweep_expired(_task_notes, time.time() - _TASK_NOTE_KEEP_SEC, ts_of=lambda v: v[1])


def pop_task_note(key: tuple[str, str]) -> str | None:
    """取出并清除该 scope 的任务便签(汇报轮点火即消费)。无则 None。"""
    entry = _task_notes.pop(key, None)
    return entry[0] if entry else None


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
