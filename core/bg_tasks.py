"""fire-and-forget 后台任务统一出口(本部署补丁)。

裸 asyncio.create_task 不持引用有两个后果：task 可能被 GC 提前回收；
协程在进 try 之前炸掉时只剩一句 "Task exception was never retrieved"，
应用日志里什么都看不到。这里统一收口：持引用到 done（防 GC）+
done 回调把异常打进 ERROR 日志。
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

from nonebot import logger

_bg_tasks: set[asyncio.Task] = set()


def spawn_bg_task(coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> asyncio.Task:
    """起一发 fire-and-forget 任务：持引用防 GC，异常结束补 ERROR 日志。"""
    task = asyncio.create_task(coro, name=name)
    _bg_tasks.add(task)
    task.add_done_callback(_on_bg_task_done)
    return task


def _on_bg_task_done(task: asyncio.Task) -> None:
    _bg_tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(f"[HERMES bg] 后台任务 {task.get_name()!r} 异常结束: {exc!r}")
