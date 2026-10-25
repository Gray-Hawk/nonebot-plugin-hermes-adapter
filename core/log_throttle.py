"""同因节流日志工具。

多个设计定案的静默闸(静默丢弃 / 顺延 / 吞没 / 清会话)补观测时共用:同一原因键
在节流间隔内只发一条 INFO 日志,防故障期每条触发刷一条。观测归观测,不扰用户,
不改任何闸的放行 / 拦截 / 丢弃 / 顺延语义。

节流状态为模块级、进程内有效,重启即清空(与各闸的既有节流状态同寿命)。
"""

from __future__ import annotations

import time

from nonebot import logger

# 同因节流间隔(秒)。同类观测 1 小时只发一条。
_THROTTLE_SEC = 3600.0

# 原因键 -> 上次实际发出日志的时间(单调秒)。字典按发声顺序排列。
_last_emit: dict[str, float] = {}
_MAX_CAUSES = 4096


def throttled_info(cause: str, message: str, *, interval_sec: float = _THROTTLE_SEC) -> bool:
    """同因节流 INFO 日志。

    cause 相同且距上次发出不足 interval_sec 时跳过(返回 False);否则记录当前
    时刻并发一条 INFO(返回 True)。不同 cause 互不影响,各自独立节流。
    使用单调时钟,避免系统校时改变节流间隔;原因表有硬上限,满员时淘汰
    最久未实际发声的条目,防止动态命令名等键在长期运行中累积。

    cause 应是稳定、足以定位 scope/原因 的短键(如 "gate.queue_full:adapter:scope"),
    不得含正文或敏感值;message 同理,只带 scope/原因,不带正文与凭据。
    """
    now = time.monotonic()
    last = _last_emit.get(cause)
    if last is not None and now - last < interval_sec:
        return False
    if cause not in _last_emit and len(_last_emit) >= _MAX_CAUSES:
        _last_emit.pop(next(iter(_last_emit)))
    # 重发后移到末尾,让满员淘汰偏向最久未发声的原因。
    _last_emit.pop(cause, None)
    _last_emit[cause] = now
    logger.info(message)
    return True
