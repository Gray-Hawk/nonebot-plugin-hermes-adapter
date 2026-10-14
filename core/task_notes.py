"""离场任务便签 + 插件提示便签(2026-10-13,错误分级反馈+离场确认轮方案)。

两个用途,都是 per-scope 的纯内存态(重启即清,丢失代价=少一条提示,可接受):

1. **离场任务便签**:模型派后台任务时在 submit_decision 里填 `background_task`
   便签;delivery poll 发现该会话任务完成、唤起汇报轮时把便签拼进引导语
   (防上下文压缩后模型忘记自己派过什么活),汇报轮点火后清除。
2. **插件提示便签**:文书错误暗示(如直角引号检出)写入这里,下一轮构造
   user content 时弹出并贴到 current_message 附近——不拦截不当轮打断,
   错误已发出覆水难收,反馈回路让模型下轮自知。

Key 与 inflight 同形:(adapter, "group:<id>" / "private:<id>")。
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


def set_task_note(key: tuple[str, str], note: str) -> None:
    _task_notes[key] = (note.strip(), time.time())


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
    _plugin_notes.setdefault(key, []).append(text)
    return True


def pop_plugin_notes(key: tuple[str, str]) -> list[str]:
    """取出并清空该 scope 的提示便签(构造下一轮 content 时消费)。"""
    return _plugin_notes.pop(key, [])
