"""T2±T3: 定时隐性激活(本部署新增)。

会话超时后(无活跃窗口、无 in-flight turn),每隔 T2 秒 + 0~T3 随机偏移检查一次:
若 max(now-T4, 上次检查时刻) 以来群里有新消息,跑一发无窗口的临时 reactive turn
(ephemeral),最多推 N2 条,由 LLM 自决是否发言;无新消息则不打扰 LLM。

窗口语义(定案):定时激活本身不开窗;LLM 真发言则由 _run_reactive_turn 的
「发言即开窗」逻辑开窗;沉默则群保持失聪,等下一个 @/引用/关键词/定时命中。

per-group 状态是内存态:重启丢失的后果只是首轮把上次检查点当成 0,
即按 now-T4 取窗口,多推一点旧消息,可接受。
"""

from __future__ import annotations

import asyncio
import random
import time

from nonebot import logger, require

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..config import plugin_config

# 调度 tick 周期:实际触发节奏由 per-group 的 _next_check_at 控制(T2+T3 随机),
# tick 只是驱动节拍,30s 粒度对分钟级 T2 足够。
_TICK_SEC = 30

_next_check_at: dict[tuple[str, str], float] = {}
_last_check_at: dict[tuple[str, str], float] = {}

_PROACTIVE_PROMPT = (
    "(定时巡查:以上是群里最近的新消息,请自行判断是否值得发言。"
    "本模式 push_message 不可用,想说话就直接在 submit_decision 里给 reply_text;"
    "没什么可说的就 should_reply=false)"
)


def register_proactive_activation() -> None:
    interval = plugin_config.hermes_proactive_interval_sec
    if interval <= 0:
        return

    @scheduler.scheduled_job("interval", seconds=_TICK_SEC, id="hermes_proactive_activation")
    async def _tick() -> None:
        if (
            _mcp.bot_registry is None
            or _mcp.message_buffer is None
            or _mcp.active_sessions is None
            or _mcp.inflight is None
        ):
            return
        # 延迟导入:handlers 依赖 mcp 运行时单例,模块级互import 会成环
        from ..handlers.message import _handle_reactive_path

        now = time.time()
        now_ms = int(now * 1000)
        cfg = plugin_config

        for adapter, scope, scope_id in _mcp.bot_registry.known():
            if scope != "group":
                continue
            if cfg.hermes_proactive_groups and scope_id not in cfg.hermes_proactive_groups:
                continue
            gkey = (adapter, scope_id)
            if now < _next_check_at.get(gkey, 0):
                continue

            # 无论本次是否触发,都先排下一次:T2 + random(0, T3)
            jitter = random.uniform(0, cfg.hermes_proactive_jitter_sec) if cfg.hermes_proactive_jitter_sec > 0 else 0.0
            _next_check_at[gkey] = now + interval + jitter

            # 触发条件三查:无活跃窗口、无 in-flight turn、窗口内有新消息
            if _mcp.active_sessions.is_active(adapter, scope_id, now_ms):
                continue
            if _mcp.inflight.is_inflight((adapter, f"group:{scope_id}")):
                continue

            since_ms = max(
                now_ms - cfg.hermes_proactive_window_sec * 1000,
                int(_last_check_at.get(gkey, 0) * 1000),
            )
            _last_check_at[gkey] = now
            # get_recent 返回新→旧;过滤保留顺序,条数上限 N2 天然满足(超了取最新)
            msgs = [
                m
                for m in _mcp.message_buffer.get_recent(adapter, scope_id, cfg.hermes_proactive_max_messages)
                if m.ts >= since_ms and not m.is_bot
            ]
            if not msgs:
                continue

            entry = _mcp.bot_registry.get(adapter, "group", scope_id)
            if entry is None:
                continue
            from nonebot import get_bot

            try:
                bot = get_bot(entry.bot_self_id)
            except (KeyError, ValueError) as exc:
                logger.warning(f"[HERMES proactive] bot offline, skip {adapter}/{scope_id}: {exc}")
                continue

            logger.info(
                f"[HERMES proactive] firing ephemeral turn: {adapter}/{scope_id} "
                f"msgs={len(msgs)} window_sec={cfg.hermes_proactive_window_sec}"
            )
            # fire-and-forget:turn 可能跑几分钟(工具链/压缩),不能堵住调度 tick
            # 里其他群的检查;并发安全由 inflight registry 保证。
            asyncio.create_task(
                _handle_reactive_path(
                    bot=bot,
                    target=entry.target,
                    adapter_name=adapter,
                    user_id="timer",
                    nickname="定时巡查",
                    group_id=scope_id,
                    text=_PROACTIVE_PROMPT,
                    image_urls=[],
                    is_explicit_trigger=False,
                    addressed_to_bot=False,
                    now_ms=now_ms,
                    ephemeral=True,
                    recent_override=msgs,
                )
            )

    logger.info(
        f"[HERMES] T2 定时隐性激活已启用: 间隔 {interval}s"
        f" + 0~{plugin_config.hermes_proactive_jitter_sec}s 随机,"
        f" 窗口 {plugin_config.hermes_proactive_window_sec}s, 上限 {plugin_config.hermes_proactive_max_messages} 条"
    )
