"""活跃态过期清理。"""

from __future__ import annotations

import time

from nonebot import logger, require

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..config import plugin_config


def register_expire_active_sessions() -> None:
    interval = plugin_config.hermes_active_sweep_interval_sec
    # H5 修复(2026-10-17):interval≤0 不注册——其余 interval 任务均有此守卫
    # (session_idle_reset/proactive_activation/delivery_poll),本任务原是全 tasks/
    # 唯一例外,配 0/负值会在启动期被 apscheduler 直接抛 ValueError。
    if interval <= 0:
        logger.warning(f"[HERMES] 活跃窗口清扫已停用: sweep_interval={interval}s")
        return

    @scheduler.scheduled_job("interval", seconds=interval, id="hermes_expire_active_sessions")
    async def _sweep() -> None:
        if _mcp.active_sessions is None:
            return
        now_ms = int(time.time() * 1000)
        expired = _mcp.active_sessions.sweep_expired(now_ms)
        if expired:
            logger.info(
                f"[HERMES] swept {len(expired)} expired active session(s): "
                + ", ".join(f"({s.adapter}, {s.group_id})" for s in expired)
            )
