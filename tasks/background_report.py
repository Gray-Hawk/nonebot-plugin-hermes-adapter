"""后台完成事件的持久汇报轮询（alive/92，替代旧转录水位轮询）。"""

from __future__ import annotations

import time

from nonebot import logger
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp
from ..core.misc_config import load_misc_config


def register_background_report() -> None:
    interval = load_misc_config().delivery_poll_interval_sec
    if interval <= 0:
        return

    @scheduler.scheduled_job(
        "interval", seconds=interval, id="hermes_background_report",
        coalesce=True, max_instances=1,
    )
    async def _tick() -> None:
        consumer = _mcp.background_report_consumer
        if consumer is None:
            return
        await consumer.tick(time.time())

    logger.info(f"[HERMES] 后台完成持久汇报已启用: 间隔 {interval}s")
