"""后台完成事件的持久汇报轮询（alive/92，替代旧转录水位轮询）。

一拍里做两件事，顺序固定：

1. **登记**：先从看板（Kanban 终态事件）把新事件写进同一张持久信箱；
2. **消费**：再跑信箱的发现／恢复／唤起。

登记先于消费，新事件当拍就能被唤醒；两个来源共用同一份去重、租约与唤醒闸，
差别只在「上游有没有认领对象」（子代理有，看板没有，销账只落本地台账）。
"""

from __future__ import annotations

import time

from nonebot import logger
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp
from ..core.kanban_report_source import ingest_into
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
        now = time.time()
        try:
            ingest_into(_mcp.background_report_outbox, _mcp.kanban_report_source)
        except Exception as exc:  # 看板源故障不得拖垮子代理汇报链
            logger.warning(f"[HERMES] 看板事件登记失败: {type(exc).__name__}")
        await consumer.tick(now)

    logger.info(f"[HERMES] 后台完成持久汇报已启用: 间隔 {interval}s")
