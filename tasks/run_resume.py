"""nb 重启后 reattach 在跑的 runs turn(本部署补丁:阶段2a)。

旧路 nb 重启 = 在跑 turn 全灭(SSE 断连 interrupt,最终回复必丢);runs 的 turn
活在 gateway,插件重启后按 run_tracker 的行 reattach:

- run 仍在跑 → 按 last_seq 续订事件流,新旁白照发,终态后走极简送达补发回复;
- 重启期间已 completed → 直接补发 output(回复不丢,这是本补丁的核心收益);
- failed/cancelled/interrupted → 记 WARN 删行不补发(与现网失败静默语义一致)。

极简送达与正常 turn 的差异(刻意收窄,防半个状态机):
- reactive/结构化: output 按 submit_decision JSON 解析,失败即静默;reply_to=m:id
  通过消息库解析成当前会话的平台 message_id,内联 @ 由统一出站构造消息段;
- pin/release anchor、should_exit_active、防抖合并这些 turn 生命周期
  特性在续跑轮一律跳过(记 INFO);mark_bot_replied 照打(窗口若还活着);
- 续跑期间到达的旁白帧按 interim_echo 同款转发(受 hermes_interim_echo_enabled 控制)。

送达窗口:BotRegistry 重启即清、scope 内可能已有新 turn 在跑——等 entry 出现且
scope 空闲才送,15s 一拍共 40 拍(10 分钟);超时保留行。保留行两条出路
:A=同 scope 任意 turn 收尾即
触发 retry_scope_deliveries 重试;B=已终态回复逾 15 分钟(自 gateway 报告的
完成时刻起算,非 run 开始时刻——长任务不被误杀)未送达直接丢弃
(_DELIVERY_CAP_SEC,在 _resume_row_impl 查得 status 后生效;未终态长任务不受
cap 影响)。reattach 过程自身的网络失败同样保留行(runs 在 gateway 侧还活着)。
真僵尸行(群组沉寂/bot 离线超 24h)仍由启动时 TTL 清扫兜底。
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import nonebot_plugin_alconna as alconna
from nonebot import get_bot, logger

from .. import mcp as _mcp
from ..adapter_norm import normalize_adapter_name
from ..config import plugin_config
from ..core.bg_tasks import spawn_bg_task
from ..core.hermes_client import (
    RunsEventParser,
    extract_response_media,
)
from ..core.inflight import make_inflight_key
from ..core.message_buffer import BufferedMessage
from ..core.message_origin import MessageOrigin
from ..core.companion_runtime import deliver_companion_run_output
from ..core.misc_config import load_misc_config
from ..core.outbound import (
    DeliveryContext,
    get_bot_nickname,
    send_local_files,
    send_result_history,
    send_result_sticker_count,
    send_text_with_media,
)
from ..core.reply_resolution import resolve_reply
from ..core.reply_recovery import (
    deliver_recovered_replies,
    recover_run_overwritten_replies,
)
from ..core.routing import resolve_target
from ..core.run_delivery_monitor import (
    reconcile_pending_deliveries,
    reconcile_run,
    settle_expected,
)
from ..core.runs_terminal import (
    TerminalPlanError,
    append_terminal_batch,
    fold_plan_units,
    prepare_terminal_batch,
)
from ..core.run_reclaim import reclaim_pending_submissions
from ..core.run_tracker import TrackedRun

_RESUME_RETRY_SEC = 15
_RESUME_RETRY_MAX = 40  # 40 拍 × 15s = 10 分钟送达窗口
_TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
# 送达时效 cap:已终态(completed)的回复若 15 分钟仍未送达,
# 直接丢弃不再保留——「15 分钟内没送到,送到概率基本就是 0」,且时过境迁的
# 旧回复裸发出来比丢失更糟(当日实证:13:59 的部署汇报在 scope 抢不到空闲窗被保留,
# 2 小时后第二次部署重启时被补送,被当成凭空冒出的古董消息)。
# 只作用于已终态行:仍在跑的 run(未终态)不受 cap 影响,长任务不会被误丢。
# 基准=gateway status 的 updated_at(_set_run_status 每次状态转换刷新,终态落定
# 时刻即真实完成时刻),非 run 开始时刻——旧实现用 started_at 起算,run 时长超
# cap 的长任务在完成瞬间即被误判超龄丢弃(alive/86 普查发现)。
_DELIVERY_CAP_SEC = 15 * 60
# per-run 恢复去重:启动恢复(resume_active_runs)与 turn 收尾重试
# (retry_scope_deliveries)可能重叠触发同一行,集合挡一次。
_resume_inflight: set[str] = set()
# 僵尸行 TTL:送达失败/网络失败保留的行若永远送不出去(群组沉寂、bot 离线、
# 网关已遗忘该 run),启动时按龄期清扫。单 turn 迭代数有硬顶,真在跑的 run 到不了
# 24h 龄期,误伤不到;网关终态状态只留 1h,更老的行本来也送不出去。
_ZOMBIE_TTL_SEC = 24 * 3600


def _inflight_key(row: TrackedRun) -> tuple[str, str]:
    return make_inflight_key(row.adapter, row.is_private, row.user_id, row.group_id)


def _bot_adapter_name(bot: Any) -> str:
    """Bot 实例 → 归一化 adapter 名(与本插件路由键同一来源)。"""
    get_name = getattr(getattr(bot, "adapter", None), "get_name", None)
    raw = str(get_name() or "") if callable(get_name) else ""
    return normalize_adapter_name(raw)


def _scope_of(adapter: str, is_private: bool, user_id: Any, group_id: Any):
    """台账行 → (adapter, scope, scope_id);缺目标标识返回 None。"""
    scope = "private" if is_private else "group"
    scope_id = str(user_id or "") if is_private else str(group_id or "")
    return (adapter, scope, scope_id) if scope_id else None


def restore_bot_routes(bot, *, now_ms: int | None = None) -> int:
    """Bot 连接后按既有交付台账补回内存送达路由,不伪造入站消息。

    路由来源只有两处持久事实:未结案的 active_runs(恢复 reattach 需要 target)
    与 expected_state='ready' 的 deliveries(启动对账需要 target)。Target 由
    scope_id 现场合成(与 notices/wake 的兜底构造同一形状),只存内存,不落库。

    写入的 entry 标记 ``synthetic=True``:真实感知事件到达时
    ``BotRegistry.upsert`` 会用真实 Target 覆盖它(真实感知优先)。本进程的
    bot 连接钩子只会为当前连接 bot 的 adapter 补路由——多 bot 同目标属 P-11
    多实例场景,明确不做。

    只补路由;"要不要补投"仍由恢复链与组件认领的实发回执裁决。
    """
    registry = _mcp.bot_registry
    tracker = _mcp.run_tracker
    if registry is None or tracker is None:
        return 0
    adapter_name = _bot_adapter_name(bot)
    bot_self_id = str(getattr(bot, "self_id", "") or "")
    if not bot_self_id:
        return 0
    keys: dict[tuple[str, str, str], None] = {}
    for row in tracker.list_running():
        if row.adapter == adapter_name:
            key = _scope_of(row.adapter, row.is_private, row.user_id, row.group_id)
            if key is not None:
                keys[key] = None
    for delivery in tracker.list_ready_deliveries():
        if (delivery.get("adapter") or "") != adapter_name:
            continue
        key = _scope_of(
            adapter_name,
            bool(delivery.get("is_private")),
            delivery.get("user_id"),
            delivery.get("group_id"),
        )
        if key is not None:
            keys[key] = None

    ts = _now_ms() if now_ms is None else now_ms
    restored = 0
    for adapter, scope, scope_id in keys:
        existing = registry.get(adapter, scope, scope_id)  # type: ignore[arg-type]
        if existing is not None and (
            not existing.synthetic or existing.bot_self_id == bot_self_id
        ):
            continue
        registry.upsert(
            adapter=adapter,
            scope=scope,  # type: ignore[arg-type]
            scope_id=scope_id,
            bot_self_id=bot_self_id,
            target=alconna.Target(
                id=scope_id, private=(scope == "private"), adapter=adapter
            ),
            ts=ts,
            synthetic=True,
        )
        restored += 1
    if restored:
        logger.info(
            f"[HERMES run-resume] Bot 连接后恢复送达路由 {restored} 条: "
            f"adapter={adapter_name} self_id={bot_self_id}"
        )
    return restored


def recover_bot_deliveries(bot) -> None:
    """真实 Bot 每次连接后完整恢复送达:补路由 → 唤醒 active reattach → ready 对账。

    不以"新增路由数"作为后续扫描条件:断线保留的 entry 在重连时仍然有效,
    已过等待窗口退出的 active 恢复任务也由同一钩子重新唤醒。``_resume_row``
    自带 per-run 去重,重复连接不会并行恢复同一条 run;ready 对账走既有
    ``reconcile_pending_deliveries`` 与组件认领,已 sent 的件不重发。
    """
    restore_bot_routes(bot)
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    adapter_name = _bot_adapter_name(bot)
    for row in tracker.list_running():
        if row.adapter != adapter_name:
            continue
        spawn_bg_task(
            _resume_row(row),
            name=f"resume-bot-connect-{row.run_id[:12]}",
        )
    spawn_bg_task(
        reconcile_pending_deliveries(),
        name="delivery-reconcile-bot-connect",
    )


async def _recover_overwritten_replies(row: TrackedRun, output: str, bot, target) -> None:
    """断流恢复与正常终态共用前序答复回收，末轮仍由旧极简出口处理。"""
    if not row.expect_structured:
        return
    try:
        recovered = await recover_run_overwritten_replies(row, output)
        await deliver_recovered_replies(
            recovered,
            bot=bot,
            target=target,
            adapter_name=row.adapter,
            user_id=row.user_id,
            group_id=row.group_id,
            is_private=row.is_private,
        )
    except Exception:
        # 前序补投失败不能拖垮末轮原恢复路径；缺证据时 recover 自身已静默跳过。
        logger.warning(
            f"[HERMES reply-recovery] 断流恢复前序回收异常 run_id={row.run_id}",
            exc_info=True,
        )


def _maybe_spawn_steer_reconcile(row: TrackedRun) -> None:
    """run 终态对账补投钩子(本部署补丁: 弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75/84)。

    reattach 终态与正常 turn 终态同样需要对账: nb 重启期间被接受的 steer 台账是持久化的。
    finish 删行前确认该 run 台账有 pending steer 才 spawn 对账(常态零开销)。任何异常只
    WARN, 不阻塞删行。
    """
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    try:
        if not tracker.pending_steers(row.run_id):
            return
        from ..core.steer_reconcile import reconcile_run_finished

        spawn_bg_task(
            reconcile_run_finished(row),
            name=f"steer-reconcile-{row.run_id[:12]}",
        )
    except Exception:
        logger.warning("[HERMES steer对账] 终态钩子异常(不影响删行)", exc_info=True)


def _settle(row: TrackedRun, expected_state: str, reason: str) -> None:
    """runs 半接管业务销账(alive/108);未入账 run 安全无操作。"""
    settle_expected(
        tracker=_mcp.run_tracker, run_id=row.run_id,
        expected_state=expected_state, reason=reason,
    )


def _build_units(output: str, expect_structured: bool) -> list[dict] | None:
    """终态 output → 有序 unit 列表（恢复链适配器，纯函数，离线可测）。

    Runs 终态允许连续多份两行决策(E2)，与正常链／对账链共用
    ``core.runs_terminal.units_from_output`` 的同一个解码器、同一份份过滤与空字段
    口径：同一 output 必须得到逐字相同的 plan_json，恢复才能被 30 号 upsert 接受。
    返回 None 表示解析失败／无正文（整批静默），[] 表示结构有效但所有份都无内容。
    """
    from ..core.runs_terminal import units_from_output

    return units_from_output(output, expect_structured)


def _pending_plan_indexes(tracker, run_id: str) -> list[int]:
    """既有持久计划里仍允许补发的份序号（sent/cancelled/not_required 不复活）。

    R3：流式份可能在 append_stream_unit 落账之后、实际发送之前进程中断，
    此时 run 后续以静默份结束。终态没有新增正文**不等于**既有 pending 计划为空，
    恢复链必须先看这份计划，而不是直接 silent 销账删行。

    R4：一份正文的发送是 primary → file:* → history 的组件序列，绝不能只看
    primary。正文 ack 后、文件上传/历史落盘前的中断窗口里，primary 已 sent 而
    file/history 仍 pending；此时若整份当完成销账，文件与历史会永久退出对账扫描。
    因此按**整份组件**判断：任一组件不在终态集合即算 pending，交给既有补发/
    对账路径完成（已 sent 的 primary 不重发）。
    """
    from ..core.runs_terminal import delivery_plan_units

    if tracker is None or not run_id:
        return []
    delivery = tracker.get_delivery(run_id)
    units = delivery_plan_units(delivery) if delivery else None
    if not units:
        return []
    states = {
        c["component_id"]: c["state"] for c in tracker.list_components(run_id)
    }
    terminal = {"sent", "cancelled", "not_required"}
    pending: list[int] = []
    for index, unit in enumerate(units):
        primary_state = states.get(f"primary:{index}")
        if primary_state in ("cancelled", "not_required"):
            # 业务免发/被取消的份整份不复活，不因残留组件重新进入补发。
            continue
        if primary_state != "sent":
            # 正文未成功（pending/failed/缺组件）：整份仍可补发。
            pending.append(index)
            continue
        # 正文已 sent：只把仍 pending 的 file/history 交给既有对账路径。
        component_ids = [f"history:{index}"]
        component_ids.extend(
            f"file:{index}:{file_index}"
            for file_index in range(len(unit.get("files") or ()))
        )
        if any(states.get(component_id) not in terminal for component_id in component_ids):
            pending.append(index)
    return pending


async def resume_active_runs() -> None:
    """启动钩子:扫 tracker,每行一个恢复任务,各管各的 scope 互不阻塞。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    # alive/108:先认领“已提交但尚未登记 run_id”的崩溃窗口,再统一恢复 active_runs。
    try:
        await reclaim_pending_submissions()
    except Exception:
        logger.warning("[HERMES run-reclaim] 启动认领异常,继续既有 active_runs 恢复", exc_info=True)
    # 先扫僵尸行(龄期超 TTL 的行——网关早已遗忘那些 run,送不出去了)
    pruned = tracker.prune_zombies(_ZOMBIE_TTL_SEC)
    if pruned:
        logger.warning(f"[HERMES run-resume] 清扫僵尸行 {len(pruned)} 条: {pruned}")
    # steer 对账台账清理(本部署补丁: 弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75/84):
    # 启动时清 accepted_at 超龄的台账行(24h, 对齐僵尸行 TTL), 防台账无限累积。
    try:
        pruned_steer = tracker.prune_steer_ledger(_ZOMBIE_TTL_SEC)
        if pruned_steer:
            logger.info(f"[HERMES steer对账] 启动清理超龄台账 {pruned_steer} 条")
    except Exception:
        logger.warning("[HERMES steer对账] 启动清理台账失败", exc_info=True)
    try:
        pruned_replies = tracker.prune_recovered_reply_ledger(_ZOMBIE_TTL_SEC)
        if pruned_replies:
            logger.info(f"[HERMES reply-recovery] 启动清理超龄台账 {pruned_replies} 条")
    except Exception:
        logger.warning("[HERMES reply-recovery] 启动清理台账失败", exc_info=True)
    rows = tracker.list_running()
    if rows:
        logger.info(f"[HERMES run-resume] 启动恢复: {len(rows)} 个在跑 run 待 reattach")
        for row in rows:
            spawn_bg_task(_resume_row(row))
    # 提交 C:启动再扫 expected 已落账但未终态的 deliveries(含 active_runs 已
    # finish 的),与正常终态共用同一对账器;observe 模式只记录不补发。
    spawn_bg_task(reconcile_pending_deliveries(), name="delivery-reconcile-startup")


async def _resume_row(row: TrackedRun) -> None:
    """per-run 去重包装:启动恢复与 turn 收尾重试(A)可能重叠触发同一行。"""
    if row.run_id in _resume_inflight:
        return
    _resume_inflight.add(row.run_id)
    try:
        await _resume_row_impl(row)
    finally:
        _resume_inflight.discard(row.run_id)


def _completed_overdue_age(status_body: dict, now_sec: float) -> int | None:
    """B 时效闸统一判定:已终态且完成时刻逾 cap → 超龄秒数,否则 None。

    基准取 status 的 updated_at(gateway 每次状态转换刷新,completed 落定时刻
    即真实完成时刻):长任务(run 时长>cap)完成瞬间 age≈0 不误丢;停机期间
    完成的古董回复 updated_at 早已超 cap 照丢。updated_at 缺失/非法 → None
    不丢(宁留待 24h 僵尸清扫,不误丢)。helper 与主路径内联两处共用本判定,
    口径单点。
    """
    if str(status_body.get("status") or "") != "completed":
        return None
    try:
        completed_ts = float(status_body.get("updated_at") or 0.0)
    except (TypeError, ValueError):
        return None
    if completed_ts <= 0:
        return None
    age = int(now_sec - completed_ts)
    return age if age > _DELIVERY_CAP_SEC else None


def _drop_overdue_status(row: TrackedRun, tracker, status_body: dict) -> bool:
    """按同一时效口径结算已完成且超龄的 status；未超龄返回 False。"""
    age = _completed_overdue_age(status_body, _now_ms() / 1000)
    if age is None:
        return False
    logger.warning(
        f"[HERMES run-resume] 回复已逾送达时效({_DELIVERY_CAP_SEC}s),丢弃 "
        f"run_id={row.run_id} scope={row.scope_key} age={age}s"
    )
    _settle(row, "expired", "delivery_cap")
    _maybe_spawn_steer_reconcile(row)
    tracker.finish(row.run_id)
    return True


async def _drop_if_stale_completed(row: TrackedRun, tracker) -> bool:
    """已终态(completed)且逾 _DELIVERY_CAP_SEC 的行直接丢弃删行(B 时效闸)。

    B 闸原本只排在送达窗口之后(_resume_row_impl 主路径),
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
        return _drop_overdue_status(row, tracker, resp.json())
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
        # 送达窗口超时不再弃行——删行=completed 回复永久丢。
        # 保留行等下次启动再送;永远送不出去的真僵尸由启动 TTL 清扫兜底。
        # 保留前先过 B 时效闸(原只排在窗口之后,scope 长忙时轮不到)。
        if await _drop_if_stale_completed(row, tracker):
            return
        logger.warning(
            f"[HERMES run-resume] 送达窗口超时,保留行待下次启动 run_id={row.run_id} scope={row.scope_key}"
        )
        return
    try:
        bot = get_bot(entry.bot_self_id)
    except (KeyError, ValueError) as exc:
        # bot 离线保留前同样先过 B 时效闸。
        if await _drop_if_stale_completed(row, tracker):
            return
        logger.warning(f"[HERMES run-resume] bot 离线,保留行待下次启动 run_id={row.run_id}: {exc}")
        return

    tgt = resolve_target(row.adapter, row.is_private, row.group_id)
    headers = {"Authorization": f"Bearer {tgt.api_key}"} if tgt.api_key else {}
    parser = RunsEventParser()
    status_body: dict = {}
    client = None
    streamed = False  # 是否真订阅过事件流(终态 run 跳过订阅,下面的终态闸要区分)
    try:
        async with httpx.AsyncClient(timeout=tgt.timeout) as client:
            resp = await client.get(f"{tgt.base_url}/v1/runs/{row.run_id}", headers=headers)
            if resp.status_code != 200:
                raise RuntimeError(f"status HTTP {resp.status_code}")
            status_body = resp.json()
            status = str(status_body.get("status") or "")
            # B 时效闸:已终态回复逾 _DELIVERY_CAP_SEC(自 gateway 报告的
            # 完成时刻起算)未送达 → 直接丢弃删行,不再保留等下次启动。
            # 未终态(仍在跑)的行不查 cap;判定与 helper 共用 _completed_overdue_age。
            if _drop_overdue_status(row, tracker, status_body):
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
                            and load_misc_config().interim_echo_enabled
                        ):
                            cleaned, _, _ = extract_response_media(interim)
                            if cleaned:
                                await send_text_with_media(
                                    bot=bot, target=entry.target, text=cleaned,
                                    media_urls=[], adapter_name=row.adapter,
                                )
                        if tracker and parser.last_seq >= 0 and (interim or parser.terminal):
                            tracker.update_seq(row.run_id, parser.last_seq)
                        if parser.terminal:
                            break
    except Exception as exc:
        # 首次 status 为 running 后，run 可能在订阅 events 前刚好完成；此时事件
        # 入口可已失效。只在这条订阅路径复查一次 status，终态仍归并到下方既有
        # 交付链，仍 running 或复查失败才保留行。重新拿终态后丢弃部分事件帧，
        # 以 status 的完整 output 为准，避免残片覆盖最终答复。
        if streamed:
            assert client is not None  # streamed 只在成功进入 async with 后置真。
            try:
                resp = await client.get(f"{tgt.base_url}/v1/runs/{row.run_id}", headers=headers)
                if resp.status_code != 200:
                    raise RuntimeError(f"status 复查 HTTP {resp.status_code}")
                refreshed = resp.json()
                refreshed_status = str(refreshed.get("status") or "")
                if refreshed_status not in _TERMINAL_STATUSES:
                    raise RuntimeError(f"status 复查仍为 {refreshed_status or 'unknown'}")
                if _drop_overdue_status(row, tracker, refreshed):
                    return
                status_body = refreshed
                streamed = False
                parser = RunsEventParser()
                logger.info(
                    f"[HERMES run-resume] events 失败后以 status 终态恢复 "
                    f"run_id={row.run_id} status={refreshed_status}"
                )
            except Exception as refresh_exc:
                logger.warning(
                    f"[HERMES run-resume] reattach 失败,保留行待下次启动 "
                    f"run_id={row.run_id}: {exc}; status 复查: {refresh_exc}"
                )
                return
        else:
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
        _maybe_spawn_steer_reconcile(row)
        tracker.finish(row.run_id)
        return
    output = parser.output
    if output is None:
        status = str(status_body.get("status") or "")
        if status != "completed":
            logger.warning(f"[HERMES run-resume] run 终态={status} 不补发 run_id={row.run_id}")
            _maybe_spawn_steer_reconcile(row)
            tracker.finish(row.run_id)
            return
        output = str(status_body.get("output") or "")
    # 终态闸只对「真订阅过事件流」的路径生效——
    # 停机期间已终态的 run 从未喂过帧,parser.terminal 恒 False,旧代码在这里
    # 把已从 status_body 捞出的 output 拦死:不补发、不删行,行变僵尸每次启动重演。
    if streamed and not parser.terminal:
        logger.warning(f"[HERMES run-resume] 事件流未见终态帧 run_id={row.run_id},保留行待下次启动")
        return

    if await deliver_companion_run_output(
        session_id=row.session_key,
        bot=bot,
        target=entry.target,
        adapter_name=row.adapter,
        user_id=row.user_id,
        group_id=row.group_id,
        is_private=row.is_private,
    ):
        logger.info(f"[HERMES run-resume] companion 接管终态交付 run_id={row.run_id}")
        _maybe_spawn_steer_reconcile(row)
        tracker.finish(row.run_id)
        return

    # expected 必须先于 tracker.finish 落账(alive/108):恢复发送同样要有交付
    # 事实;写库失败只记告警,不阻断既有恢复发送(legacy 行为不回归)。
    # P-16:恢复链是 failed／stream_after_partial 的唯一合法重臂入口,显式 rearm;
    # 业务终态(silent／suppressed／cancelled／expired)即使走到这里也不回臂。
    accepted = True
    if tracker is not None:
        try:
            accepted = tracker.record_expected_delivery(
                run_id=row.run_id, output=output, expect_structured=row.expect_structured,
                rearm=True,
            )
        except Exception as exc:
            logger.warning(f"[HERMES run-resume] expected 落账失败 run_id={row.run_id}: {exc}")
    if not accepted:
        # P-16:业务终态拒绝落账(不回臂 ready)必须贯穿恢复出站——交付台账已由
        # 正常终态链销账,恢复不得再读 pending 计划补发正文/文件;重复终态因此
        # 零新增发送。这里只清 active_runs 僵尸行,不写交付台账任何字段。
        state = None
        try:
            delivery = tracker.get_delivery(row.run_id) if tracker is not None else None
            state = delivery.get("expected_state") if delivery else None
        except Exception:
            pass
        logger.warning(
            f"[HERMES run-resume] 交付已处业务终态,恢复零发送 run_id={row.run_id} "
            f"state={state!r}"
        )
        tracker.finish(row.run_id)
        return
    # 前序答复回收与正常终态同步紧急禁用（alive/105 生产回归）：
    # 未按 run 边界截断转录前，恢复路径同样可能补投历史答复。
    # 极简送达。reply_to 的 m:id 在发送前按 row 中的会话归属解析，重启后也不
    # 改用“最近发言者”猜测目标；查不到映射只降级普通正文。
    units = _build_units(output, row.expect_structured)
    # E3/R3: 终态没有新增正文（静默份/解析失败）不等于既有持久计划为空。
    # 流式份可能已 append_stream_unit 落账、但在实际发送前中断；这里先看计划里
    # 还有没有允许补发的 pending 份，有就按既有计划补发，绝不提前 silent 销账删行。
    pending_existing = _pending_plan_indexes(tracker, row.run_id)
    if not units:
        if not pending_existing:
            if units is None:
                logger.info(f"[HERMES run-resume] 续跑轮静默(解析失败/决定不回) run_id={row.run_id}")
                _settle(row, "silent", "parse_failed_or_no_reply")
            else:
                logger.info(f"[HERMES run-resume] 续跑轮回复为空 run_id={row.run_id}")
                _settle(row, "silent", "empty_content")
            _maybe_spawn_steer_reconcile(row)
            tracker.finish(row.run_id)
            return
        logger.info(
            f"[HERMES run-resume] 终态无新增正文,按既有持久计划补发 pending 份 "
            f"run_id={row.run_id} pending={pending_existing}"
        )
    else:
        # 先整批落计划再发送：登记失败显式失败、零发送，绝不回落 legacy 无台账发送。
        # 组件命名与正常终态链一致(primary:i／history:i／file:i:j)，已 sent 的份不重发。
        # E3: 断流前已即时发送的份在计划里是前缀；终态份若只是计划后缀 → 折掉，
        # 只补未发送/失败重试份。计划仍只允许尾部追加，不改写旧份。
        append_units, existing_units = fold_plan_units(
            tracker=tracker, run_id=row.run_id, units=units
        )
        try:
            if existing_units:
                append_terminal_batch(
                    tracker=tracker, run_id=row.run_id, units=append_units
                )
            else:
                prepare_terminal_batch(tracker=tracker, run_id=row.run_id, units=units)
        except TerminalPlanError as exc:
            logger.error(
                f"[HERMES run-resume] 终态计划落库失败,本轮零发送 run_id={row.run_id}: {exc}"
            )
            return
    from ..core.runs_terminal import delivery_plan_units

    assert tracker is not None
    delivery = tracker.get_delivery(row.run_id)
    plan_units = (delivery_plan_units(delivery) if delivery else None) or []
    comp_states = {
        c["component_id"]: c["state"] for c in tracker.list_components(row.run_id)
    }
    contexts = [
        DeliveryContext(
            tracker=tracker, run_id=row.run_id, component_id=f"primary:{index}",
            generation_guard=lambda run_id=row.run_id: tracker.is_delivery_current(run_id),
        )
        for index in range(len(plan_units))
    ]
    for index, plan_unit in enumerate(plan_units):
        primary_state = comp_states.get(f"primary:{index}")
        if primary_state in ("sent", "cancelled", "not_required"):
            # 已即时发送/已免发：恢复不得重发正文。R4：正文已 ack 不代表整份完成，
            # file/history 可能仍 pending（正文回写 sent 后、文件上传前中断）；这类
            # 未完成组件交给收尾的 reconcile 组件补发，绝不在这里当整份完成跳过。
            leftover = [
                component_id
                for component_id in (
                    [f"history:{index}"]
                    + [
                        f"file:{index}:{file_index}"
                        for file_index in range(len(plan_unit.get("files") or ()))
                    ]
                )
                if comp_states.get(component_id) not in ("sent", "cancelled", "not_required")
            ]
            if leftover:
                logger.info(
                    f"[HERMES run-resume] 正文已发,剩余组件交对账补发 "
                    f"run_id={row.run_id} part={index} pending={leftover}"
                )
            continue  # 已即时发送/已免发：恢复不得重发
        cleaned = plan_unit.get("cleaned_text") or ""
        media_urls = list(plan_unit.get("media_urls") or [])
        file_paths = [
            f.get("path") for f in plan_unit.get("files") or [] if f.get("path")
        ]
        reply_to = plan_unit.get("reply_to") or ""
        resolved_reply = resolve_reply(
            _mcp.message_buffer,
            adapter=row.adapter,
            group_id=None if row.is_private else row.group_id,
            owner_user_id=row.user_id if row.is_private else None,
            reply_to=reply_to,
            log_tag="HERMES run-resume",
        )
        ctx = contexts[index]
        sent = await send_text_with_media(
            bot=bot, target=entry.target, text=cleaned, media_urls=media_urls,
            reply_qq_message_id=resolved_reply.qq_message_id if resolved_reply else None,
            reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
            adapter_name=row.adapter, delivery=ctx,
        )
        await send_local_files(
            bot=bot, target=entry.target, file_paths=file_paths, adapter_name=row.adapter,
            component_prefix=f"{index}:", delivery=ctx,
        )
        if sent and _mcp.message_buffer is not None:
            # 补发与正常 turn 一样留下可寻址的 bot 行；平台回执缺失时只把
            # qq_message_id 留 NULL，不把“无 ID”误当发送失败。
            buffered_content = send_result_history(sent, cleaned)
            if media_urls:
                buffered_content = f"{buffered_content} [图片]".strip()
            _mcp.message_buffer.append(
                BufferedMessage(
                    ts=_now_ms(),
                    adapter=row.adapter,
                    group_id=None if row.is_private else row.group_id,
                    user_id=row.bot_self_id,
                    nickname=await get_bot_nickname(bot),
                    content=buffered_content,
                    origin=MessageOrigin.bot_output(),
                    image_urls=[],
                    is_bot=True,
                    qq_message_id=getattr(sent, "primary_qq_message_id", None),
                    private_owner_id=row.user_id if row.is_private else None,
                )
            )
        if sent and not row.is_private and row.group_id and _mcp.active_sessions is not None:
            if _mcp.active_sessions.get_if_trigger_alive(row.adapter, row.group_id, _now_ms()) is not None:
                _mcp.active_sessions.mark_bot_replied(
                    row.adapter, row.group_id, now_ms=_now_ms(),
                    media_count=len(media_urls) + len(file_paths) + send_result_sticker_count(sent),
                    text=cleaned,
                )
        logger.info(
            f"[HERMES run-resume] 续跑送达完成 run_id={row.run_id} scope={row.scope_key} "
            f"part={index}/{len(plan_units)} sent={sent} text_len={len(cleaned)} "
            f"media={len(media_urls)} files={len(file_paths)}"
            "(turn 生命周期特性:pin/exit/防抖 跳过)"
        )
        try:
            if sent and _mcp.message_buffer is not None:
                tracker.mark_component_sent(
                    run_id=row.run_id, component_id=f"history:{index}",
                    msg_ids=list(getattr(sent, "msg_ids", []) or []),
                )
            elif sent:
                tracker.mark_component_not_required(
                    run_id=row.run_id, component_id=f"history:{index}"
                )
        except Exception as exc:
            logger.warning(f"[runs-resume] history 回执失败 run={row.run_id}: {exc}")
    try:
        await reconcile_run(row.run_id, reason="resume")
    except Exception as exc:
        logger.warning(f"[runs-resume] reconcile 异常 run={row.run_id}: {exc}")
    _maybe_spawn_steer_reconcile(row)
    tracker.finish(row.run_id)


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


async def retry_scope_deliveries(adapter_name: str, is_private: bool, user_id: str, group_id) -> None:
    """同 scope turn 收尾触发的送达重试。

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
        spawn_bg_task(_resume_row(row))
