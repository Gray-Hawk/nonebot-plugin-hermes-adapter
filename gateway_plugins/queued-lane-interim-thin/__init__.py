"""queued-lane-interim-thin: 两处网关窄补丁 + 一处 Kanban 订阅围栏，全部是用户插件。

* ``patches``：queued-lane 前序答复在本 run 的 SSE 流上以 ``message.interim`` 出现
  （stock 的 ``ApiServerAdapter.send`` 必然失败的那一份），且同文的 ``run.completed``
  不再重复；
* ``kanban_notify_guard``：新卡不再产生 ``api_server`` 自投订阅（NB 汇报信箱照旧从
  ``tasks.session_id`` 路由）。

不改 Hermes 上游文件；``register`` 与 ``api_server.connect()`` 两处都调用 install（幂等，
重连不叠 wrapper）。"""
from __future__ import annotations

import logging

from . import kanban_notify_guard, patches

logger = logging.getLogger(__name__)


def register(ctx):
    logger.info("queued-lane-interim-thin: %s", patches.install().describe())
    logger.info("queued-lane-interim-thin: kanban guard %s", kanban_notify_guard.install().describe())

    def wire(native, adapter):
        # api_server.connect(), before its first run is admitted: re-assert both installs so a
        # reload path that imported the gateway modules late still gets the wrappers.
        logger.info("queued-lane-interim-thin: verified at api_server connect: %s | %s",
                    patches.install().describe(), kanban_notify_guard.install().describe())

    ctx.register_platform_handler("api_server", wire)
