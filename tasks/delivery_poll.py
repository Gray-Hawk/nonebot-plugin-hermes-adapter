"""后台任务完成投递轮询(本部署新增)。

上游行为(api_server 无状态会话):异步委派(delegate_task 后台结果)完成时不跑
wake turn——客户端拥有下一轮,只把结果作为 display_kind=async_delegation_complete
的 user 角色投递记录写进会话转录(wake.py persist_delegation_delivery)。TUI/桌面端
轮询 GET /api/sessions/{id}/messages 看到它;本插件原先没有等价通道,结果只能干等
下一条真实消息才被折叠进上下文——群里永远收不到「任务完成」推送。

本任务补上这条通道:每拍轮询全部已知会话的 messages 端点,发现新投递记录就跑一发
LLM turn 让 agent 自己汇报(群=reactive 临时 turn,私聊=passive turn)。

为什么走 LLM turn 而不是直接转发记录原文:投递记录 content 含完整任务简报(内网
地址、凭据取用方式、部署路径等内部细节),原样进群等于泄密;走 turn 让 agent 按人设
口径和保密判断自己组织语言。成本是每次完成一发额外 turn,完成属低频事件,可接受。

去重与播种:_last_seen 按 session_key 记已播报的最大记录 id(上游 DB 主键,单调)。
首轮轮询只播种不播报——nb 停机期间的完成不补播(会随下一条真实消息自然折叠进上下
文),避免重启后给群刷一堆过期通知。播种/播报状态是内存态,重启即清,语义同上。

顺延不丢:目标不在 BotRegistry(重启后群里还没人说话)、bot 掉线、或有 turn 在跑时,
本拍不推进 _last_seen,下一拍重试。
"""

from __future__ import annotations

import asyncio
import time

import httpx
from nonebot import get_bot, logger, require

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..config import plugin_config
from ..core import task_notes
from ..core.routing import resolve_target
from ..core.session import session_manager

# 每拍每会话拉最新一页,过滤出投递记录;一页内的投递记录密度远低于此上限
_PAGE_LIMIT = 15
# 轮询请求自身的超时;端点是本地小查询,10s 已经很宽
_HTTP_TIMEOUT = 10

# session_key -> 已播报(或已播种)的最大投递记录 id
_last_seen: dict[str, int] = {}

_DELIVERY_ROW_KIND = "async_delegation_complete"

# 群(reactive 临时 turn)与私聊(passive turn)共用的引导语核心,差异只在末尾的
# 沉默通道:reactive 可以 should_reply=false,passive 没有静默语义、必须说点什么。
_PROMPT_CORE = (
    "(系统通知:你之前派出的后台任务刚刚完成,完整结果已经在上面的会话记录里"
    "([ASYNC DELEGATION BATCH COMPLETE] 那条)。请用自己的语气向大家简要汇报结果要点"
    "和后续动作;不要复读任务简报原文,不要暴露服务器路径、内网地址、凭据等内部细节。"
)
_PROMPT_GROUP = _PROMPT_CORE + "若结果没什么值得说的,就 should_reply=false)"
_PROMPT_PRIVATE = _PROMPT_CORE + "请直接给出汇报内容)"


def extract_new_delivery_rows(
    messages: list[dict], last_seen: int
) -> tuple[list[dict], int]:
    """从一页消息里挑出 id > last_seen 的投递记录,返回 (新记录按 id 升序, 新的 last_seen)。

    纯函数,方便离线测试。messages 形状即端点 data 数组元素(只需 id / display_kind)。
    """
    rows = [m for m in messages if m.get("display_kind") == _DELIVERY_ROW_KIND]
    if not rows:
        return [], last_seen
    newest = max(int(m["id"]) for m in rows)
    fresh = sorted((m for m in rows if int(m["id"]) > last_seen), key=lambda m: int(m["id"]))
    return fresh, max(last_seen, newest)


async def _poll_session(
    *, internal_id: str, adapter: str, is_private: bool, user_id: str, group_id: str | None, session_key: str
) -> None:
    tgt = resolve_target(adapter, is_private, group_id)
    url = f"{tgt.base_url}/api/sessions/{session_key}/messages"
    headers = {"Authorization": f"Bearer {tgt.api_key}"} if tgt.api_key else {}
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.get(
                url, params={"order": "latest", "limit": _PAGE_LIMIT}, headers=headers
            )
    except (httpx.TimeoutException, httpx.ConnectError) as exc:
        logger.debug(f"[HERMES delivery-poll] {internal_id} 请求失败,本拍跳过: {exc}")
        return
    if resp.status_code == 404:
        # 会话在上游还没有转录(典型:/clear 后的新 generation 还没聊过)——正常,不播
        return
    if resp.status_code != 200:
        logger.warning(f"[HERMES delivery-poll] {internal_id} 端点返回 {resp.status_code},本拍跳过")
        return
    try:
        messages = resp.json().get("data") or []
    except ValueError:
        logger.warning(f"[HERMES delivery-poll] {internal_id} 响应不是 JSON,本拍跳过")
        return

    seen = _last_seen.get(session_key)
    if seen is None:
        # 首轮:基线播种,不播报存量(见模块 docstring)
        _, _last_seen[session_key] = extract_new_delivery_rows(messages, 0)
        return

    fresh, new_seen = extract_new_delivery_rows(messages, seen)
    if not fresh:
        return

    # 投递目标与并发检查:任一不满足就顺延下一拍(不推进 _last_seen,不丢)
    if _mcp.bot_registry is None or _mcp.inflight is None:
        return
    scope = "private" if is_private else "group"
    scope_id = user_id if is_private else (group_id or "")
    entry = _mcp.bot_registry.get(adapter, scope, scope_id)
    if entry is None:
        return
    if _mcp.inflight.is_inflight((adapter, f"{scope}:{scope_id}")):
        return
    try:
        bot = get_bot(entry.bot_self_id)
    except (KeyError, ValueError) as exc:
        logger.warning(f"[HERMES delivery-poll] bot offline, 顺延 {internal_id}: {exc}")
        return

    # 确认能发才推进水位:fire-and-forget 的 turn 失败与否不再回捞(与 T2 同一约定)
    _last_seen[session_key] = new_seen
    now_ms = int(time.time() * 1000)
    # 2026-10-13 分级反馈:模型离场时登记的任务便签随汇报轮回投(防压缩后忘记任务)。
    # 便签点火即消费——汇报失败也不还,下轮完成行(若有)用通用引导语兜底。
    note_key = (adapter, f"private:{user_id}" if is_private else f"group:{group_id or ''}")
    task_note = task_notes.pop_task_note(note_key)
    note_suffix = f'你离场时登记的任务便签:“{task_note}”。' if task_note else ""
    logger.info(
        f"[HERMES delivery-poll] 发现 {len(fresh)} 条新投递记录: {internal_id},唤起汇报 turn"
    )
    # 延迟导入:handlers 依赖 mcp 运行时单例,模块级互 import 会成环
    if is_private:
        from ..handlers.message import _handle_passive_path

        asyncio.create_task(
            _handle_passive_path(
                bot=bot,
                target=entry.target,
                adapter_name=adapter,
                user_id=user_id,
                nickname="任务投递",
                group_id=None,
                text=note_suffix + _PROMPT_PRIVATE,
                image_urls=[],
                is_private=True,
                now_ms=now_ms,
            )
        )
    else:
        from ..handlers.message import _handle_reactive_path

        asyncio.create_task(
            _handle_reactive_path(
                bot=bot,
                target=entry.target,
                adapter_name=adapter,
                user_id="delivery",
                nickname="任务投递",
                group_id=scope_id,
                text=note_suffix + _PROMPT_GROUP,
                image_urls=[],
                is_explicit_trigger=False,
                addressed_to_bot=False,
                now_ms=now_ms,
                ephemeral=True,
            )
        )


def register_delivery_poll() -> None:
    interval = plugin_config.hermes_delivery_poll_interval_sec
    if interval <= 0:
        return

    @scheduler.scheduled_job("interval", seconds=interval, id="hermes_delivery_poll")
    async def _tick() -> None:
        known_keys = set()
        for internal_id, adapter, is_private, user_id, group_id, session_key in session_manager.known_sessions():
            known_keys.add(session_key)
            await _poll_session(
                internal_id=internal_id,
                adapter=adapter,
                is_private=is_private,
                user_id=user_id,
                group_id=group_id,
                session_key=session_key,
            )
        # 清掉已不存在会话的水位条目(/clear 换代、T1 重置后旧 key 不再出现)
        for key in list(_last_seen):
            if key not in known_keys:
                del _last_seen[key]

    logger.info(f"[HERMES] 后台任务完成投递轮询已启用: 间隔 {interval}s")
