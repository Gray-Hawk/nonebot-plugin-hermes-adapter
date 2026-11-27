"""steer-presentation-thin: Runs steer 呈递收口（用户插件，不改 Hermes 上游文件）。

* ``patches``：``/v1/runs/{id}/steer`` 请求体带 NB 的结构化呈递标记时，本批文本投递
  时不再套 ``[OUT-OF-BAND …]`` 外壳（原样落成那条 user 行）；系统提示的 steer 说明
  追加一段，让模型认识零包装 user 行的可信身份。

``register`` 与 ``api_server.connect()`` 两处都调用 install（幂等，重连不叠 wrapper）。
"""
from __future__ import annotations

import logging

from . import patches

logger = logging.getLogger(__name__)


def register(ctx):
    logger.info("steer-presentation-thin: %s", patches.install().describe())

    def wire(native, adapter):
        # api_server.connect()：首个 run 被准入前再确认一次，防止晚导入的模块
        # 把包装函数换回上游版本。
        logger.info(
            "steer-presentation-thin: verified at api_server connect: %s | %s",
            patches.install().describe(), patches.describe_registry(),
        )

    ctx.register_platform_handler("api_server", wire)
