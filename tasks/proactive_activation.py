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
from collections import deque

from nonebot import logger, require

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..config import plugin_config
from ..core.bg_tasks import spawn_bg_task
from ..core.inflight import make_inflight_key

# 调度 tick 周期:实际触发节奏由 per-group 的 _next_check_at 控制(T2+T3 随机),
# tick 只是驱动节拍,30s 粒度对分钟级 T2 足够。
_TICK_SEC = 30

_next_check_at: dict[tuple[str, str], float] = {}
_last_check_at: dict[tuple[str, str], float] = {}

# 阶段2c(维护者 2026-10-13 定案,原话「巡查必须异步了,同步模式会一瞬间挤占好几个
# 队列」):巡查改全局串行队列——到龄群进队列去重,全局同一时刻最多 1 发巡查
# turn 在跑;失败(含 429)即弃不重试,下轮 T2 自然再查。
# 旧形态是同 tick 多群到龄就 create_task 并发点火,每发捏着一个 gateway 全局槽
# (max_concurrent_runs=10)几分钟,扎堆即挤占用户 turn 的槽位。
_patrol_queue: deque[tuple[str, str, dict]] = deque()
_patrol_queued: set[tuple[str, str]] = set()
_patrol_running = False

_PROACTIVE_PROMPT = (
    "(定时巡查:以上是群里最近的新消息,请自行判断是否值得发言。"
    "本模式 push_message 不可用,想说话就直接在 submit_decision 里给 reply_text;"
    "没什么可说的就 should_reply=false)"
)


def _enqueue_patrol(adapter: str, scope_id: str, kwargs: dict) -> bool:
    """到龄群进巡查队列(按群去重)。返回是否真入队(已排队= False)。"""
    if (adapter, scope_id) in _patrol_queued:
        return False
    _patrol_queue.append((adapter, scope_id, kwargs))
    _patrol_queued.add((adapter, scope_id))
    return True


async def _drain_patrol_queue(run_one) -> None:
    """串行消费巡查队列(全局同一时刻最多 1 发巡查 turn)。

    run_one 失败(含 429/传输错误/异常)只记日志即弃——巡查 turn 天生可弃,
    不重试不贴表情(与用户 turn 的「丢了就贴」刻意分流),下轮 T2 自然再查。
    finally 复位 _patrol_running:中途异常也不能把队列永久锁死。
    """
    global _patrol_running
    try:
        while _patrol_queue:
            adapter, scope_id, kwargs = _patrol_queue.popleft()
            _patrol_queued.discard((adapter, scope_id))
            # 2026-10-13 审核修复(巡查迟到闸门,维护者定案): 入队到点火可能隔几分钟
            # (串行队列前车未走),期间窗口转活跃/有 turn 在跑 → 本轮直接弃。
            # tick 入口的检查只是便宜前置过滤,这道才是执行时的真实闸门。
            recheck_ms = int(time.time() * 1000)
            if _mcp.active_sessions is not None and _mcp.active_sessions.is_alive_for_trigger(adapter, scope_id, recheck_ms):
                logger.info(f"[HERMES proactive] 点火前复查: 窗口已活跃,本轮弃 {adapter}/{scope_id}")
                continue
            if _mcp.inflight is not None and _mcp.inflight.is_inflight(make_inflight_key(adapter, False, "", scope_id)):
                logger.info(f"[HERMES proactive] 点火前复查: turn 在跑,本轮弃 {adapter}/{scope_id}")
                continue
            logger.info(f"[HERMES proactive] 串行点火: {adapter}/{scope_id} (队列余 {len(_patrol_queue)})")
            try:
                await run_one(**kwargs)
            except Exception as exc:
                logger.warning(f"[HERMES proactive] 巡查 turn 异常即弃 {adapter}/{scope_id}: {exc}")
    finally:
        _patrol_running = False


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
        from ..handlers.message_relay import _handle_reactive_path

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
            if _mcp.active_sessions.is_alive_for_trigger(adapter, scope_id, now_ms):
                continue
            if _mcp.inflight.is_inflight(make_inflight_key(adapter, False, "", scope_id)):
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
                f"[HERMES proactive] due,进串行队列: {adapter}/{scope_id} "
                f"msgs={len(msgs)} window_sec={cfg.hermes_proactive_window_sec}"
            )
            # 阶段2c:并发点火 → 全局串行队列。kwargs 与旧 create_task 调用一致,
            # 由 worker 逐个 await(全局同一时刻最多 1 发巡查 turn 占槽)。
            _enqueue_patrol(
                adapter,
                scope_id,
                dict(
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
                    droppable=True,  # 巡查天生可弃:撞在跑 turn 不占 followup 槽
                    recent_override=msgs,
                ),
            )

        # 队列非空且 worker 未跑 → 点火 worker(单事件循环内 check-then-set 无竞态)
        global _patrol_running
        if _patrol_queue and not _patrol_running:
            _patrol_running = True
            spawn_bg_task(_drain_patrol_queue(_handle_reactive_path))

    logger.info(
        f"[HERMES] T2 定时隐性激活已启用: 间隔 {interval}s"
        f" + 0~{plugin_config.hermes_proactive_jitter_sec}s 随机,"
        f" 窗口 {plugin_config.hermes_proactive_window_sec}s, 上限 {plugin_config.hermes_proactive_max_messages} 条"
    )
