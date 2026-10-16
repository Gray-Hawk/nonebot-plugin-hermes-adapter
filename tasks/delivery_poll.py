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

2026-10-14 窗口冲掉修复(维护者报障「总是我先发消息,子代理才到」):旧实现每拍只拉
最新 15 行找投递记录,而转录行的大头是 assistant/tool 行——活跃会话一轮工具链就
几十行,完成行写入后 30 秒内就被顶出窗口,轮询永远看不到,只能靠下一条真实消息
把 agent 唤起后顺上下文汇报。修复:水位改为「已扫描的最大行 id」(不再只盯投递行),
每拍先拉 15 行小页;整页都比水位新(=行生产速度超过窗口)才加大页往回翻,
直到盖住水位为止。空闲期开销与旧版一致,活跃期自动扩容。

去重与播种:水位按 session_key 记。首轮轮询只播种不播报——nb 停机期间的完成不补播
(会随下一条真实消息自然折叠进上下文),避免重启后给群刷一堆过期通知。水位是内存态,
重启即清,语义同上。

顺延不丢:目标不在 BotRegistry(重启后群里还没人说话)、bot 掉线、或有 turn 在跑时,
本拍不推进水位,下一拍重扫重试。
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
from ..core.inflight import make_inflight_key
from ..core.routing import resolve_target
from ..core.session import session_manager

# 每拍先拉小页探窗口;仅当整页都比水位新(行生产速度超过窗口)才加大页回翻
_PROBE_LIMIT = 15
_ESCALATION_LIMIT = 200
# 回翻页数上限:超过即「水位盖不住」——不播报不推进,WARN 后下拍重试
_MAX_ESCALATION_PAGES = 5
# 轮询请求自身的超时;端点是本地小查询,10s 已经很宽
_HTTP_TIMEOUT = 10

# session_key -> 已扫描的最大行 id(含非投递行)。投递行的「已播报」由水位天然
# 表达:水位只在成功唤起汇报 turn 后推进,未盖住的投递行必然 id > 水位。
_watermark: dict[str, int] = {}

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


def _row_id(m: dict) -> int:
    return int(m.get("id") or 0)


async def collect_fresh_rows(
    pages_fn, watermark: int
) -> tuple[list[dict], int, bool]:
    """收集 id > watermark 的全部新行。返回 (新行按 id 升序, 本拍页尖 id, 是否盖住水位)。

    pages_fn(offset, limit) -> awaitable,该页行(latest 序,新→旧)。先探 15 行小页:
    页内最小 id 已 <= watermark 说明一页盖全;整页都比水位新才加大页回翻
    (每页 _ESCALATION_LIMIT,至多 _MAX_ESCALATION_PAGES 页),直到盖住水位或
    翻到底(短页)。回翻超限仍盖不住 → covered=False,调用方不播报不推进水位。

    数据源靠回调注入,离线可用假页测试。
    """
    probe = await pages_fn(0, _PROBE_LIMIT)
    if not probe:
        return [], watermark, True
    tip = max(_row_id(m) for m in probe)
    collected = {_row_id(m): m for m in probe}
    # 短页=历史已到底,新行必然全在页内,无需回翻;满页且整页比水位新才回翻
    if len(probe) == _PROBE_LIMIT and min(_row_id(m) for m in probe) > watermark:
        # 窗口被冲:小页全比水位新,回翻
        covered = False
        offset = _PROBE_LIMIT
        for _ in range(_MAX_ESCALATION_PAGES):
            page = await pages_fn(offset, _ESCALATION_LIMIT)
            if not page:
                covered = True  # 翻到底了,剩余行不足一页
                break
            collected.update({_row_id(m): m for m in page})
            offset += _ESCALATION_LIMIT
            if len(page) < _ESCALATION_LIMIT or min(_row_id(m) for m in page) <= watermark:
                covered = True
                break
        if not covered:
            return [], tip, False
    fresh = sorted((m for m in collected.values() if _row_id(m) > watermark), key=_row_id)
    return fresh, tip, True


async def _poll_session(
    *, internal_id: str, adapter: str, is_private: bool, user_id: str, group_id: str | None, session_key: str
) -> None:
    tgt = resolve_target(adapter, is_private, group_id)
    url = f"{tgt.base_url}/api/sessions/{session_key}/messages"
    headers = {"Authorization": f"Bearer {tgt.api_key}"} if tgt.api_key else {}

    async def fetch_page(client: httpx.AsyncClient, offset: int, limit: int) -> list[dict]:
        resp = await client.get(
            url, params={"order": "latest", "limit": limit, "offset": offset}, headers=headers
        )
        if resp.status_code == 404:
            # 会话在上游还没有转录(典型:/clear 后的新 generation 还没聊过)——正常,当空页
            return []
        if resp.status_code != 200:
            raise RuntimeError(f"端点返回 {resp.status_code}")
        try:
            return resp.json().get("data") or []
        except ValueError:
            raise RuntimeError("响应不是 JSON")

    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            seen = _watermark.get(session_key)
            if seen is None:
                # 首轮:水位置当前页尖,存量不播报(见模块 docstring)
                probe = await fetch_page(client, 0, _PROBE_LIMIT)
                if probe:
                    _watermark[session_key] = max(_row_id(m) for m in probe)
                return
            fresh, tip, covered = await collect_fresh_rows(
                lambda off, lim: fetch_page(client, off, lim), seen
            )
    except (httpx.TimeoutException, httpx.ConnectError) as exc:
        logger.debug(f"[HERMES delivery-poll] {internal_id} 请求失败,本拍跳过: {exc}")
        return
    except RuntimeError as exc:
        logger.warning(f"[HERMES delivery-poll] {internal_id} {exc},本拍跳过")
        return

    if not covered:
        logger.warning(
            f"[HERMES delivery-poll] {internal_id} 回翻 {_MAX_ESCALATION_PAGES} 页仍盖不住水位"
            f"(行生产速度异常?),本拍不播报不推进"
        )
        return
    deliveries = [m for m in fresh if m.get("display_kind") == _DELIVERY_ROW_KIND]
    if not deliveries:
        # 无新投递(含无新行):扫描水位照常推进到页尖
        _watermark[session_key] = tip
        return

    # 投递目标与并发检查:任一不满足就顺延下一拍(不推进水位,下拍重扫重试,不丢)
    if _mcp.bot_registry is None or _mcp.inflight is None:
        return
    scope = "private" if is_private else "group"
    scope_id = user_id if is_private else (group_id or "")
    entry = _mcp.bot_registry.get(adapter, scope, scope_id)
    if entry is None:
        return
    if _mcp.inflight.is_inflight(make_inflight_key(adapter, is_private, user_id, group_id)):
        return
    try:
        bot = get_bot(entry.bot_self_id)
    except (KeyError, ValueError) as exc:
        logger.warning(f"[HERMES delivery-poll] bot offline, 顺延 {internal_id}: {exc}")
        return

    # 确认能发才推进水位:fire-and-forget 的 turn 失败与否不再回捞(与 T2 同一约定)
    _watermark[session_key] = tip
    now_ms = int(time.time() * 1000)
    # 2026-10-13 分级反馈:模型离场时登记的任务便签随汇报轮回投(防压缩后忘记任务)。
    # 便签点火即消费——汇报失败也不还,下轮完成行(若有)用通用引导语兜底。
    note_key = make_inflight_key(adapter, is_private, user_id, group_id)
    task_note = task_notes.pop_task_note(note_key)
    note_suffix = f'你离场时登记的任务便签:“{task_note}”。' if task_note else ""
    logger.info(
        f"[HERMES delivery-poll] 发现 {len(deliveries)} 条新投递记录: {internal_id},唤起汇报 turn"
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
        for key in list(_watermark):
            if key not in known_keys:
                del _watermark[key]

    logger.info(f"[HERMES] 后台任务完成投递轮询已启用: 间隔 {interval}s")
