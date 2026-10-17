"""T1: 会话空闲自动重置(本部署新增)。

以最后一次 chat() 完成 / push_message 成功为活动基准(session_manager._last_activity),
T1 秒内无任何显式/隐式激活或 LLM 主动行为,则静默重置会话(等价 /clear 的 session
效果,但不发任何群提示——自动路径不调 commands.py 的 handler)。

硬性保护:正在执行 Agent 任务的会话绝不清——inflight slot 与 active session 租约
双查。/delay 暂停、/resume 恢复由 SessionManager._idle_paused 承载。
"""

from __future__ import annotations

import time

from nonebot import logger, require

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..config import plugin_config
from ..core.inflight import make_inflight_key
from ..core.session import session_manager

# 检查节奏:固定 60s。T1 本身是秒级可配,检查粒度不需要跟着细——
# 最坏情况晚 60s 清,对「空闲太久换个新会话」的语义毫无影响。
_SWEEP_SEC = 60


def register_session_idle_reset() -> None:
    idle_sec = plugin_config.hermes_session_idle_reset_sec
    if idle_sec <= 0:
        return

    @scheduler.scheduled_job("interval", seconds=_SWEEP_SEC, id="hermes_session_idle_reset")
    async def _sweep() -> None:
        if _mcp.inflight is None:
            return
        now = time.time()
        for internal_id, (adapter, is_private, user_id, group_id) in session_manager.idle_expired(
            idle_sec, now
        ):
            # 保护 1:inflight slot 在 = 有 turn 正在跑
            key = make_inflight_key(adapter, is_private, user_id, group_id)
            if _mcp.inflight.is_inflight(key):
                logger.debug(f"[HERMES T1] skip {internal_id}: turn in-flight")
                continue
            # 保护 2:reactive turn 的 active session 租约兜底
            # (inflight 覆盖 reactive/passive 主路径,租约覆盖窗口维度的在途状态)
            if not is_private and group_id and _mcp.active_sessions is not None:
                # 本部署补丁: 租约改 key 级,直接查计数,
                # 不再经过实例(turn 跑期间实例可能已被 trigger() 换过)。
                if _mcp.active_sessions.lease_count(adapter, group_id) > 0:
                    logger.debug(f"[HERMES T1] skip {internal_id}: active session leased")
                    continue
            session_manager.clear_internal(internal_id)

    logger.info(f"[HERMES] T1 空闲自动重置已启用: idle>{idle_sec}s, 每 {_SWEEP_SEC}s 检查")
