"""nb 重启后 reattach 在跑的 runs turn(本部署补丁:阶段2a)。

旧路 nb 重启 = 在跑 turn 全灭(SSE 断连 interrupt,最终回复必丢);runs 的 turn
活在 gateway,插件重启后按 run_tracker 的行 reattach:

- run 仍在跑 → 按 last_seq 续订事件流,新旁白照发,终态后走极简送达补发回复;
- 重启期间已 completed → 直接补发 output(回复不丢,这是本补丁的核心收益);
- failed/cancelled/interrupted → 记 WARN 删行不补发(与现网失败静默语义一致)。

极简送达与正常 turn 的差异(刻意收窄,防半个状态机):
- reactive/结构化: output 按 submit_decision JSON 解析,失败即静默;at_user_id
  用 message_buffer 当前 recent 窗口校验(重启后 recent 按新 buffer 算);
- pin/release anchor、should_exit_active、ack emoji、防抖合并这些 turn 生命周期
  特性在续跑轮一律跳过(记 INFO);mark_bot_replied 照打(窗口若还活着);
- 续跑期间到达的旁白帧按 interim_echo 同款转发(受 hermes_interim_echo_enabled 控制)。

送达窗口:BotRegistry 重启即清、scope 内可能已有新 turn 在跑——等 entry 出现且
scope 空闲才送,15s 一拍共 40 拍(10 分钟);超时保留行。保留行两条出路
(2026-10-16 维护者拍板 A+B,替代旧「只等下次启动」):A=同 scope 任意 turn 收尾即
触发 retry_scope_deliveries 重试;B=已终态回复逾 15 分钟未送达直接丢弃
(_DELIVERY_CAP_SEC,在 _resume_row_impl 查得 status 后生效;未终态长任务不受
cap 影响)。reattach 过程自身的网络失败同样保留行(runs 在 gateway 侧还活着)。
真僵尸行(群组沉寂/bot 离线超 24h)仍由启动时 TTL 清扫兜底。
"""

from __future__ import annotations

import asyncio

import httpx
from nonebot import get_bot, logger

from .. import mcp as _mcp
from ..config import plugin_config
from ..core.hermes_client import (
    RunsEventParser,
    _try_parse_first_json_block,
    extract_response_media,
)
from ..core.inflight import make_inflight_key
from ..core.outbound import send_local_files, send_text_with_media
from ..core.routing import resolve_target
from ..core.run_tracker import TrackedRun

_RESUME_RETRY_SEC = 15
_RESUME_RETRY_MAX = 40  # 40 拍 × 15s = 10 分钟送达窗口
_TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
# 送达时效 cap(2026-10-16 维护者拍板 B):已终态(completed)的回复若 15 分钟仍未送达,
# 直接丢弃不再保留——维护者原话「15 分钟内没送到,送到概率基本就是 0」,且时过境迁的
# 旧回复裸发出来比丢失更糟(当日实证:13:59 的部署汇报在 scope 抢不到空闲窗被保留,
# 2 小时后第二次部署重启时被补送,维护者当成凭空冒出的古董消息)。
# 只作用于已终态行:仍在跑的 run(未终态)不受 cap 影响,长任务不会被误丢。
_DELIVERY_CAP_SEC = 15 * 60
# per-run 恢复去重:启动恢复(resume_active_runs)与 turn 收尾重试
# (retry_scope_deliveries)可能重叠触发同一行,集合挡一次。
_resume_inflight: set[str] = set()
# 僵尸行 TTL(低危⑫):送达失败/网络失败保留的行若永远送不出去(群组沉寂、bot 离线、
# 网关已遗忘该 run),启动时按龄期清扫。单 turn 迭代数有硬顶,真在跑的 run 到不了
# 24h 龄期,误伤不到;网关终态状态只留 1h,更老的行本来也送不出去。
_ZOMBIE_TTL_SEC = 24 * 3600


def _inflight_key(row: TrackedRun) -> tuple[str, str]:
    return make_inflight_key(row.adapter, row.is_private, row.user_id, row.group_id)


def _build_delivery(output: str, expect_structured: bool, recent_speakers: set[str]) -> tuple[str, str | None] | None:
    """终态 output → (reply_text, at_user_id)。纯函数,离线可测。

    结构化模式:JSON 解析失败或 should_reply=false → None(静默);
    at_user_id 不在 recent 窗口 → 不 @(与正常 turn 同口径)。
    非结构化:output 原文即回复;恒不 @(没有信封,无处取 at 依据)。
    """
    if not expect_structured:
        return (output, None) if output.strip() else None
    decision = _try_parse_first_json_block(output)
    if not isinstance(decision, dict):
        return None
    if not decision.get("should_reply"):
        return None
    reply_text = str(decision.get("reply_text") or "").strip()
    if not reply_text:
        return None
    at = decision.get("at_user_id")
    at_user = str(at) if at is not None and str(at) in recent_speakers else None
    return reply_text, at_user


async def resume_active_runs() -> None:
    """启动钩子:扫 tracker,每行一个恢复任务,各管各的 scope 互不阻塞。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    # 低危⑫:先扫僵尸行(龄期超 TTL 的行——网关早已遗忘那些 run,送不出去了)
    pruned = tracker.prune_zombies(_ZOMBIE_TTL_SEC)
    if pruned:
        logger.warning(f"[HERMES run-resume] 清扫僵尸行 {len(pruned)} 条: {pruned}")
    rows = tracker.list_running()
    if not rows:
        return
    logger.info(f"[HERMES run-resume] 启动恢复: {len(rows)} 个在跑 run 待 reattach")
    for row in rows:
        asyncio.create_task(_resume_row(row))


async def _resume_row(row: TrackedRun) -> None:
    """per-run 去重包装:启动恢复与 turn 收尾重试(A)可能重叠触发同一行。"""
    if row.run_id in _resume_inflight:
        return
    _resume_inflight.add(row.run_id)
    try:
        await _resume_row_impl(row)
    finally:
        _resume_inflight.discard(row.run_id)


async def _drop_if_stale_completed(row: TrackedRun, tracker) -> bool:
    """已终态(completed)且逾 _DELIVERY_CAP_SEC 的行直接丢弃删行(B 时效闸)。

    2026-10-17 维护者拍板:B 闸原本只排在送达窗口之后(_resume_row_impl 主路径),
    scope 长忙(窗口超时)或 bot 离线时永远轮不到执行,已终态超龄行会躺到 24h
    僵尸清扫才走。抽成 helper 后在各保留分支前统一补闸。查询失败/未终态/
    未超龄一律 False,调用方维持原保留语义不变。
    """
    try:
        tgt = resolve_target(row.adapter, row.is_private, row.group_id)
        headers = {"Authorization": f"Bearer {tgt.api_key}"} if tgt.api_key else {}
        async with httpx.AsyncClient(timeout=tgt.timeout) as client:
            resp = await client.get(f"{tgt.base_url}/v1/runs/{row.run_id}", headers=headers)
        if resp.status_code != 200:
            return False
        status = str(resp.json().get("status") or "")
        if status != "completed":
            return False
        age = int(_now_ms() / 1000 - row.started_at)
        if age <= _DELIVERY_CAP_SEC:
            return False
        logger.warning(
            f"[HERMES run-resume] 回复已逾送达时效({_DELIVERY_CAP_SEC}s),丢弃 "
            f"run_id={row.run_id} scope={row.scope_key} age={age}s"
        )
        tracker.finish(row.run_id)
        return True
    except Exception:
        return False


async def _resume_row_impl(row: TrackedRun) -> None:
    tracker = _mcp.run_tracker
    assert tracker is not None
    scope = "private" if row.is_private else "group"
    scope_id = row.user_id if row.is_private else (row.group_id or "")

    # 送达窗口:等 registry entry(bot 路由)出现 + scope 空闲(防新旧回复乱序)
    entry = None
    for _ in range(_RESUME_RETRY_MAX):
        registry = _mcp.bot_registry
        inflight = _mcp.inflight
        if registry is not None and inflight is not None:
            entry = registry.get(row.adapter, scope, scope_id)
            if entry is not None and not inflight.is_inflight(_inflight_key(row)):
                break
            entry = None
        await asyncio.sleep(_RESUME_RETRY_SEC)
    if entry is None:
        # 低危⑫(2026-10-14):送达窗口超时不再弃行——删行=completed 回复永久丢。
        # 保留行等下次启动再送;永远送不出去的真僵尸由启动 TTL 清扫兜底。
        # 2026-10-17:保留前先过 B 时效闸(原只排在窗口之后,scope 长忙时轮不到)。
        if await _drop_if_stale_completed(row, tracker):
            return
        logger.warning(
            f"[HERMES run-resume] 送达窗口超时,保留行待下次启动 run_id={row.run_id} scope={row.scope_key}"
        )
        return
    try:
        bot = get_bot(entry.bot_self_id)
    except (KeyError, ValueError) as exc:
        # 2026-10-17:bot 离线保留前同样先过 B 时效闸。
        if await _drop_if_stale_completed(row, tracker):
            return
        logger.warning(f"[HERMES run-resume] bot 离线,保留行待下次启动 run_id={row.run_id}: {exc}")
        return

    tgt = resolve_target(row.adapter, row.is_private, row.group_id)
    headers = {"Authorization": f"Bearer {tgt.api_key}"} if tgt.api_key else {}
    parser = RunsEventParser()
    status_body: dict = {}
    streamed = False  # 是否真订阅过事件流(终态 run 跳过订阅,下面的终态闸要区分)
    try:
        async with httpx.AsyncClient(timeout=tgt.timeout) as client:
            resp = await client.get(f"{tgt.base_url}/v1/runs/{row.run_id}", headers=headers)
            if resp.status_code != 200:
                raise RuntimeError(f"status HTTP {resp.status_code}")
            status_body = resp.json()
            status = str(status_body.get("status") or "")
            # B 时效闸(2026-10-16 维护者拍板):已终态回复逾 _DELIVERY_CAP_SEC 未送达 →
            # 直接丢弃删行,不再保留等下次启动。未终态(仍在跑)的行不查 cap。
            if status == "completed" and (_now_ms() / 1000 - row.started_at) > _DELIVERY_CAP_SEC:
                logger.warning(
                    f"[HERMES run-resume] 回复已逾送达时效({_DELIVERY_CAP_SEC}s),丢弃 "
                    f"run_id={row.run_id} scope={row.scope_key} age={int(_now_ms() / 1000 - row.started_at)}s"
                )
                tracker.finish(row.run_id)
                return
            if status not in _TERMINAL_STATUSES:
                # 仍在跑:按 last_seq 续订(帧从 last_seq+1 起,天然不重复)
                streamed = True
                async with client.stream(
                    "GET",
                    f"{tgt.base_url}/v1/runs/{row.run_id}/events?last_seq={row.last_seq}",
                    headers=headers,
                ) as ev:
                    if ev.status_code != 200:
                        raise RuntimeError(f"events HTTP {ev.status_code}")
                    async for line in ev.aiter_lines():
                        interim = parser.feed(line)
                        if (
                            interim
                            and plugin_config.hermes_interim_echo_enabled
                        ):
                            cleaned, _, _ = extract_response_media(interim)
                            if cleaned:
                                await send_text_with_media(
                                    bot=bot, target=entry.target, text=cleaned,
                                    media_urls=[], at_user_id=None, adapter_name=row.adapter,
                                )
                        if tracker and parser.last_seq >= 0 and (interim or parser.terminal):
                            tracker.update_seq(row.run_id, parser.last_seq)
                        if parser.terminal:
                            break
    except Exception as exc:
        # 网络类失败:保留行,下次 nb 启动再试(run 在 gateway 侧还活着,行零成本)
        logger.warning(
            f"[HERMES run-resume] reattach 失败,保留行待下次启动 run_id={row.run_id}: {exc}"
        )
        return

    # 终态归并:失败/取消/中断不补发(与现网失败静默语义一致)
    if parser.interrupted or parser.error is not None:
        logger.warning(
            f"[HERMES run-resume] run 终态不可送达 run_id={row.run_id} "
            f"interrupted={parser.interrupted} error={str(parser.error)[:120]!r}"
        )
        tracker.finish(row.run_id)
        return
    output = parser.output
    if output is None:
        status = str(status_body.get("status") or "")
        if status != "completed":
            logger.warning(f"[HERMES run-resume] run 终态={status} 不补发 run_id={row.run_id}")
            tracker.finish(row.run_id)
            return
        output = str(status_body.get("output") or "")
    # 2026-10-13 审核修复(BUG1): 终态闸只对「真订阅过事件流」的路径生效——
    # 停机期间已终态的 run 从未喂过帧,parser.terminal 恒 False,旧代码在这里
    # 把已从 status_body 捞出的 output 拦死:不补发、不删行,行变僵尸每次启动重演。
    if streamed and not parser.terminal:
        logger.warning(f"[HERMES run-resume] 事件流未见终态帧 run_id={row.run_id},保留行待下次启动")
        return

    # 极简送达
    recent_speakers: set[str] = set()
    if _mcp.message_buffer is not None and not row.is_private and row.group_id:
        recent_speakers = {
            str(m.user_id)
            for m in _mcp.message_buffer.get_recent(row.adapter, row.group_id, 20)
        }
    delivery = _build_delivery(output, row.expect_structured, recent_speakers)
    if delivery is None:
        logger.info(f"[HERMES run-resume] 续跑轮静默(解析失败/决定不回) run_id={row.run_id}")
        tracker.finish(row.run_id)
        return
    reply_text, at_user = delivery
    cleaned, media_urls, file_paths = extract_response_media(reply_text)
    if not cleaned and not media_urls and not file_paths:
        logger.info(f"[HERMES run-resume] 续跑轮回复为空 run_id={row.run_id}")
        tracker.finish(row.run_id)
        return
    sent = await send_text_with_media(
        bot=bot, target=entry.target, text=cleaned, media_urls=media_urls,
        at_user_id=at_user, adapter_name=row.adapter,
    )
    await send_local_files(bot=bot, target=entry.target, file_paths=file_paths, adapter_name=row.adapter)
    if sent and not row.is_private and row.group_id and _mcp.active_sessions is not None:
        if _mcp.active_sessions.get_if_active(row.adapter, row.group_id, _now_ms()) is not None:
            _mcp.active_sessions.mark_bot_replied(
                row.adapter, row.group_id, now_ms=_now_ms(),
                media_count=len(media_urls) + len(file_paths), text=cleaned,
            )
    logger.info(
        f"[HERMES run-resume] 续跑送达完成 run_id={row.run_id} scope={row.scope_key} "
        f"sent={sent} text_len={len(cleaned)} media={len(media_urls)} files={len(file_paths)}"
        "(turn 生命周期特性:pin/exit/ack/防抖 跳过)"
    )
    tracker.finish(row.run_id)


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


async def retry_scope_deliveries(adapter_name: str, is_private: bool, user_id: str, group_id) -> None:
    """同 scope turn 收尾触发的送达重试(2026-10-16 维护者拍板 A)。

    旧路:送达窗口超时后保留行只等下次 nb 启动,一等就是几小时(当日实证 2h)。
    新径:同 scope 任意 turn 收尾(slot 释放)即对本 scope 的待送达行逐行起恢复,
    窗口外再无死等。与启动恢复共用 _resume_inflight 去重,重叠触发自动跳过;
    B 的 15 分钟时效闸在 _resume_row_impl 内统一生效,过期行到此自然丢弃。
    调用方 fire-and-forget(asyncio.create_task),勿 await 阻塞收尾主路。
    """
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    rows = [
        r
        for r in tracker.list_running()
        if r.adapter == adapter_name
        and r.is_private == is_private
        and str(r.user_id) == str(user_id)
        and (r.group_id or None) == (group_id or None)
    ]
    if not rows:
        return
    logger.info(
        f"[HERMES run-resume] turn 收尾触发送达重试: adapter={adapter_name} "
        f"private={is_private} user={user_id} group={group_id} rows={len(rows)}"
    )
    for row in rows:
        asyncio.create_task(_resume_row(row))
