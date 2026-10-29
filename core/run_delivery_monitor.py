"""runs 半接管交付台账的规划与业务销账（本部署补丁，alive/108）。

提交 B 只提供纯函数/helper：由 message_turns 与 run_resume 在既有发送
路径上调用。监控/plan 自身异常绝不阻断 legacy 发送；未入账 run 一律安全
返回 None／无操作，不伪造台账。observe/repair 的 reconcile 在提交 C 接入。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from nonebot import logger

from .outbound import DeliveryContext


def _sha256_json(value) -> str:
    canonical = json.dumps(value, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _file_plan_entry(raw: str) -> dict:
    """有序文件计划：路径、文件名、大小与内容哈希（取不到时显式 None）。"""
    entry = {"path": str(raw), "name": Path(raw).name, "size": None, "sha256": None}
    try:
        path = Path(raw)
        data = path.read_bytes()
        entry["size"] = len(data)
        entry["sha256"] = hashlib.sha256(data).hexdigest()
    except OSError:
        pass
    return entry


def prepare_outbound_delivery(
    *,
    tracker,
    run_id: str | None,
    cleaned_text: str,
    media_urls,
    file_paths,
    reply_to: str | None = None,
    reply_qq_message_id: str | int | None = None,
    reply_at_user_id: str | int | None = None,
) -> DeliveryContext | None:
    """在最终输出确定后、出站前生成投递计划与组件，返回 primary context。

    文件组件按原列表位置派生 file:<index>，由 send_local_files 逐个认领。
    计划首次形成后不得变形（tracker.upsert_delivery_plan 失败关闭）。
    任何监控侧异常只记告警并返回 None，legacy 发送照常进行。
    """
    if tracker is None or not run_id:
        return None
    try:
        if tracker.get_delivery(run_id) is None:
            return None
        plan = {
            "cleaned_text": cleaned_text,
            "media_urls": [str(u) for u in (media_urls or [])],
            "files": [_file_plan_entry(p) for p in (file_paths or [])],
            "reply_to": reply_to,
            "reply_qq_message_id": (
                str(reply_qq_message_id) if reply_qq_message_id is not None else None
            ),
            "reply_at_user_id": (
                str(reply_at_user_id) if reply_at_user_id is not None else None
            ),
        }
        components = [{
            "component_id": "primary",
            "kind": "primary",
            "payload_sha256": _sha256_json({
                "cleaned_text": plan["cleaned_text"],
                "media_urls": plan["media_urls"],
                "reply_qq_message_id": plan["reply_qq_message_id"],
                "reply_at_user_id": plan["reply_at_user_id"],
            }),
        }, {
            # history 只补历史不重发 primary;正常路由 legacy 写库后回执。
            "component_id": "history",
            "kind": "history",
            "payload_sha256": _sha256_json({"cleaned_text": plan["cleaned_text"]}),
        }]
        for idx, entry in enumerate(plan["files"]):
            components.append({
                "component_id": f"file:{idx}",
                "kind": "file",
                "payload_sha256": _sha256_json(entry),
            })
        tracker.upsert_delivery_plan(run_id=run_id, plan=plan, components=components)
        return DeliveryContext(
            tracker=tracker, run_id=run_id,
            generation_guard=lambda: tracker.is_delivery_current(run_id),
        )
    except Exception as exc:
        logger.warning(f"run={run_id} 投递计划生成失败, 本轮按 legacy 无台账发送: {exc}")
        return None


def settle_expected(
    *, tracker, run_id: str | None, expected_state: str, reason: str | None = None
) -> None:
    """业务销账：静默／抑制／取消／超期。缺身份或无台账行均安全无操作。"""
    if tracker is None or not run_id:
        return
    try:
        tracker.set_expected_state(run_id, expected_state, cancel_reason=reason)
    except Exception as exc:
        logger.warning(f"run={run_id} 业务销账失败({expected_state}/{reason}): {exc}")


# --- 统一 reconcile 与短暂顺序门(alive/108 提交 C)。 ---
# 正常终态、顺序门与启动恢复共用 reconcile_run 一个实现;补发只经 outbound
# 的 DeliveryContext,不在本模块直接 call_api。observe 只记录不动平台。

import asyncio
import os
import time

from nonebot import get_bot

from . import outbound as _ob
from .bg_tasks import spawn_bg_task
from .message_buffer import BufferedMessage
from .message_origin import MessageOrigin
from .outbound import DeliveryClaimBusy
from .runs_delivery_config import effective_mode, load_runs_delivery_config

_mcp = None  # 测试可注入;生产由 _runtime() 惰性取真实单例
_send_text = None  # 测试缝:缺省用 core.outbound 真实现
_send_files = None

_scope_locks: dict[str, asyncio.Lock] = {}
_scope_events: dict[str, asyncio.Event] = {}


def _runtime():
    global _mcp
    if _mcp is None:
        from .. import mcp as runtime_mcp

        _mcp = runtime_mcp
    return _mcp


def _scope_lock(scope_key: str) -> asyncio.Lock:
    return _scope_locks.setdefault(scope_key, asyncio.Lock())


def _scope_event(scope_key: str) -> asyncio.Event:
    return _scope_events.setdefault(scope_key, asyncio.Event())


def notify_scope(scope_key: str) -> None:
    """本进程内有交付进展时唤醒顺序门等待者(跨进程由超时与租约兜底)。"""
    _scope_event(scope_key).set()


def _plan_of(delivery: dict) -> dict:
    return json.loads(delivery["plan_json"]) if delivery.get("plan_json") else {}


def _ensure_plan_from_output(tracker, delivery: dict) -> None:
    """恢复路径没有 plan 时,用 legacy 同款解析从 output 生成并落 plan。"""
    from ..tasks.run_resume import _build_delivery, resolve_reply
    from .hermes_client import extract_response_media

    built = _build_delivery(delivery["output"], delivery["expect_structured"])
    if built is None:
        settle_expected(tracker=tracker, run_id=delivery["run_id"],
                        expected_state="silent", reason="parse_failed_or_no_reply")
        return
    reply_text, reply_to = built
    cleaned, media_urls, file_paths = extract_response_media(reply_text)
    if not cleaned and not media_urls and not file_paths:
        settle_expected(tracker=tracker, run_id=delivery["run_id"],
                        expected_state="silent", reason="empty_content")
        return
    resolved = resolve_reply(
        _runtime().message_buffer,
        adapter=delivery["adapter"],
        group_id=None if delivery["is_private"] else delivery["group_id"],
        owner_user_id=delivery["user_id"] if delivery["is_private"] else None,
        reply_to=reply_to,
        log_tag="runs-reconcile",
    )
    prepare_outbound_delivery(
        tracker=tracker, run_id=delivery["run_id"], cleaned_text=cleaned,
        media_urls=media_urls, file_paths=file_paths, reply_to=reply_to,
        reply_qq_message_id=resolved.qq_message_id if resolved else None,
        reply_at_user_id=resolved.reply_at_user_id if resolved else None,
    )


def _resolve_entry(delivery: dict):
    """经既有 bot_registry 解析 target 与 bot;未就绪返回 (None, None)。"""
    registry = _runtime().bot_registry
    if registry is None or not delivery.get("adapter"):
        return None, None
    scope = "private" if delivery["is_private"] else "group"
    scope_id = delivery["user_id"] if delivery["is_private"] else (delivery["group_id"] or "")
    entry = registry.get(delivery["adapter"], scope, scope_id)
    if entry is None:
        return None, None
    try:
        return entry, get_bot(entry.bot_self_id)
    except (KeyError, ValueError):
        return None, None


def _component_terminal(c: dict, cfg) -> bool:
    """组件是否已达终态(不再值得补);unknown/failed 未耗尽重试不算。"""
    state = c["state"]
    if state in ("sent", "cancelled", "not_required"):
        return True
    if state == "unknown":
        return c["attempt_count"] >= cfg.unknown_max_attempts
    if state == "failed":
        return c["attempt_count"] >= 3
    return False


async def _repair_components(tracker, delivery: dict, cfg, stats: dict) -> None:
    """repair 模式逐组件补发;只经 outbound 交付上下文,绝不绕过认领。"""
    run_id = delivery["run_id"]
    owner = f"reconcile-{os.getpid()}"
    plan = _plan_of(delivery)
    entry, bot = _resolve_entry(delivery)
    if entry is None:
        stats["deferred"] = "bot_not_ready"
        return
    comps = tracker.list_components(run_id)
    primary = next((c for c in comps if c["component_id"] == "primary"), None)
    files = [c for c in comps if c["kind"] == "file"]

    if primary is not None and not _component_terminal(primary, cfg):
        if primary["state"] == "unknown":
            outcome = tracker.claim_component(
                run_id=run_id, component_id="primary", owner=owner,
                unknown_backoff_sec=cfg.unknown_backoff_sec,
                unknown_max_attempts=cfg.unknown_max_attempts,
            )
            if outcome != "send":
                stats["unknown"] = stats.get("unknown", 0) + 1
                primary = None
        if primary is not None:
            ctx = DeliveryContext(
                tracker=tracker, run_id=run_id, component_id="primary", owner=owner,
                generation_guard=lambda: tracker.is_delivery_current(run_id),
            )
            try:
                await (_send_text or _ob.send_text_with_media)(
                    bot=bot, target=entry.target, text=plan.get("cleaned_text", ""),
                    media_urls=plan.get("media_urls", []),
                    reply_qq_message_id=plan.get("reply_qq_message_id"),
                    reply_at_user_id=plan.get("reply_at_user_id"),
                    adapter_name=delivery["adapter"], delivery=ctx,
                )
            except DeliveryClaimBusy:
                stats["busy"] = stats.get("busy", 0) + 1
            except Exception as exc:
                logger.warning(f"run={run_id} reconcile primary 补发异常: {exc}")
                stats["error"] = stats.get("error", 0) + 1

    unresolved_files = []
    for comp in files:
        if _component_terminal(comp, cfg):
            continue
        if comp["state"] == "unknown":
            outcome = tracker.claim_component(
                run_id=run_id, component_id=comp["component_id"], owner=owner,
                unknown_backoff_sec=cfg.unknown_backoff_sec,
                unknown_max_attempts=cfg.unknown_max_attempts,
            )
            if outcome != "send":
                stats["unknown"] = stats.get("unknown", 0) + 1
                continue
        unresolved_files.append(comp)
    if unresolved_files:
        paths = [f["path"] for f in plan.get("files", [])]
        ctx = DeliveryContext(
            tracker=tracker, run_id=run_id, owner=owner,
            generation_guard=lambda: tracker.is_delivery_current(run_id),
        )
        try:
            await (_send_files or _ob.send_local_files)(
                bot=bot, target=entry.target, file_paths=paths,
                adapter_name=delivery["adapter"], delivery=ctx,
            )
        except DeliveryClaimBusy:
            stats["busy"] = stats.get("busy", 0) + 1
        except Exception as exc:
            logger.warning(f"run={run_id} reconcile 文件补发异常: {exc}")
            stats["error"] = stats.get("error", 0) + 1


async def _repair_history(tracker, delivery: dict, stats: dict) -> None:
    """primary 已 sent 而 history 缺失时只补历史,不重发 primary。"""
    run_id = delivery["run_id"]
    primary = tracker.get_component(run_id=run_id, component_id="primary")
    if primary is None or primary["state"] != "sent":
        return
    plan = _plan_of(delivery)
    buffer = _runtime().message_buffer
    for comp in tracker.list_components(run_id):
        if comp["kind"] != "history" or comp["state"] != "pending":
            continue
        if buffer is None:
            tracker.mark_component_not_required(run_id=run_id, component_id=comp["component_id"])
            continue
        msg_ids = list(primary["msg_ids"])
        content = plan.get("cleaned_text", "")
        if plan.get("media_urls"):
            content = f"{content} [图片]".strip()
        entry, bot = _resolve_entry(delivery)
        nickname = ""
        if bot is not None:
            try:
                nickname = await _ob.get_bot_nickname(bot)
            except Exception:
                nickname = ""
        buffer.append(
            BufferedMessage(
                ts=int(time.time() * 1000),
                adapter=delivery["adapter"],
                group_id=None if delivery["is_private"] else delivery["group_id"],
                user_id=entry.bot_self_id if entry is not None else "",
                nickname=nickname,
                content=content,
                origin=MessageOrigin.bot_output(),
                image_urls=[],
                is_bot=True,
                qq_message_id=msg_ids[0] if msg_ids else None,
                private_owner_id=delivery["user_id"] if delivery["is_private"] else None,
            )
        )
        tracker.mark_component_sent(run_id=run_id, component_id=comp["component_id"], msg_ids=msg_ids)
        stats["history"] = stats.get("history", 0) + 1


async def reconcile_run(run_id: str, *, reason: str = "") -> dict:
    """唯一对账补发器:正常终态、顺序门与启动恢复共用。"""
    stats: dict = {"run_id": run_id, "reason": reason}
    tracker = _runtime().run_tracker
    if tracker is None:
        return stats
    delivery = tracker.get_delivery(run_id)
    if delivery is None or delivery["expected_state"] != "ready":
        stats["skipped"] = "settled_or_missing"
        return stats
    if not tracker.is_delivery_current(run_id):
        tracker.set_expected_state(run_id, "cancelled", cancel_reason="generation_cleared")
        stats["skipped"] = "generation_cleared"
        return stats
    cfg = load_runs_delivery_config()
    mode = effective_mode(
        cfg, adapter=delivery["adapter"] or "onebotv11",
        is_private=bool(delivery["is_private"]),
        user_id=delivery["user_id"], group_id=delivery["group_id"],
    )
    stats["mode"] = mode
    if mode == "off":
        stats["skipped"] = "off"
        return stats
    if not tracker.list_components(run_id):
        _ensure_plan_from_output(tracker, delivery)
        delivery = tracker.get_delivery(run_id)
        if delivery["expected_state"] != "ready":
            stats["skipped"] = "settled_by_plan"
            return stats
    comps = tracker.list_components(run_id)
    if not comps:
        stats["deferred"] = "no_plan"
        return stats
    if mode == "observe":
        missing = [c for c in comps if not _component_terminal(c, cfg)]
        stats["would_repair"] = len(missing)
        if missing:
            logger.info(
                f"[runs-reconcile] observe 记录缺件 run={run_id} reason={reason} "
                f"components={[c['component_id'] for c in missing]}"
            )
        return stats
    # repair
    if reason == "turn_end" and time.time() - delivery["updated_at"] < cfg.grace_sec:
        stats["deferred"] = "grace"
        return stats
    await _repair_components(tracker, delivery, cfg, stats)
    await _repair_history(tracker, delivery, stats)
    final = tracker.list_components(run_id)
    if all(_component_terminal(c, cfg) for c in final):
        tracker.mark_delivery_terminal(run_id)
        stats["terminal"] = True
    notify_scope(delivery["scope_key"])
    return stats


async def reconcile_scope(scope_key: str, *, reason: str = "") -> None:
    """按提交序对账一个 hermes scope 的全部未终态 deliveries。"""
    tracker = _runtime().run_tracker
    if tracker is None:
        return
    for delivery in tracker.list_ready_deliveries(scope_key):
        try:
            await reconcile_run(delivery["run_id"], reason=reason)
        except Exception as exc:
            logger.warning(f"[runs-reconcile] scope 对账异常 run={delivery['run_id']}: {exc}")
    notify_scope(scope_key)


async def reconcile_pending_deliveries() -> None:
    """启动入口:扫全部 expected 已落账但未终态的 deliveries(含已 finish 的)。"""
    tracker = _runtime().run_tracker
    if tracker is None:
        return
    rows = tracker.list_ready_deliveries()
    if rows:
        logger.info(f"[runs-reconcile] 启动对账 {len(rows)} 条未终态 deliveries")
    for delivery in rows:
        try:
            await reconcile_run(delivery["run_id"], reason="startup")
        except Exception as exc:
            logger.warning(f"[runs-reconcile] 启动对账异常 run={delivery['run_id']}: {exc}")


def schedule_reconcile_for_run(run_id: str, *, reason: str = "turn_end") -> None:
    """turn 收尾 fire-and-forget 调度本 scope 对账;未入账/已终态不调度。"""
    tracker = _runtime().run_tracker
    if tracker is None:
        return
    delivery = tracker.get_delivery(run_id)
    if delivery is None or delivery["expected_state"] != "ready":
        return
    spawn_bg_task(
        reconcile_scope(delivery["scope_key"], reason=reason),
        name=f"delivery-reconcile-{run_id[:12]}",
    )


async def order_gate(run_id: str) -> dict:
    """短暂顺序门:最终答复出站前检查同 scope 前序,缺件先补,超时放行。

    只管理最终答复组件;无前序异常时仅一次本地查询,不等待、不调补发器。
    observe 只记录"本可等待";off 直接放行。
    """
    stats: dict = {"run_id": run_id}
    tracker = _runtime().run_tracker
    if tracker is None:
        return stats
    delivery = tracker.get_delivery(run_id)
    if delivery is None or delivery["expected_state"] != "ready":
        return stats
    cfg = load_runs_delivery_config()
    mode = effective_mode(
        cfg, adapter=delivery["adapter"] or "onebotv11",
        is_private=bool(delivery["is_private"]),
        user_id=delivery["user_id"], group_id=delivery["group_id"],
    )
    stats["mode"] = mode
    predecessors = tracker.list_predecessors(
        scope_key=delivery["scope_key"], submission_seq=delivery["submission_seq"] or 0
    )
    if not predecessors:
        stats["fast"] = True
        return stats
    if mode != "repair":
        stats["would_wait"] = len(predecessors)
        logger.info(
            f"[order-gate] {mode} 记录前序未终态 scope={delivery['scope_key']} "
            f"run={run_id} blockers={[p['run_id'] for p in predecessors]}"
        )
        return stats
    loop = asyncio.get_running_loop()
    deadline = loop.time() + cfg.order_gate_timeout_sec
    scope_key = delivery["scope_key"]
    async with _scope_lock(scope_key):
        waited = 0
        while True:
            predecessors = tracker.list_predecessors(
                scope_key=scope_key, submission_seq=delivery["submission_seq"] or 0
            )
            if not predecessors:
                stats["waited"] = waited
                return stats
            fresh = any(
                time.time() - p["updated_at"] < cfg.grace_sec for p in predecessors
            )
            if not fresh:
                # 前序超过 legacy 宽限仍未终态:只补前序,当前 run 仍由 legacy 主发。
                waited += 1
                for predecessor in predecessors:
                    await reconcile_run(predecessor["run_id"], reason="order_gate")
                notify_scope(scope_key)
                predecessors = tracker.list_predecessors(
                    scope_key=scope_key, submission_seq=delivery["submission_seq"] or 0
                )
                if not predecessors:
                    stats["waited"] = waited
                    return stats
            remaining = deadline - loop.time()
            if remaining <= 0:
                stats["timeout"] = True
                stats["blocker_runs"] = [p["run_id"] for p in predecessors]
                logger.warning(
                    f"[order-gate] order_gate_timeout scope={scope_key} run={run_id} "
                    f"blockers={stats['blocker_runs']} waited={waited}"
                )
                return stats
            event = _scope_event(scope_key)
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), remaining)
            except (asyncio.TimeoutError, TimeoutError):
                pass

