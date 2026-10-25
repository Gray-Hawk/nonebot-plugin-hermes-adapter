"""接力/steer 域:链条计数、后续轮点火、pending 聚合出队、slot 释放、steer 直注/followup、_refire 接力与 route_synthesized_input 入口。

从 handlers/message.py 拆出;纯搬运,零逻辑改动。
"""

from __future__ import annotations

import asyncio
import time

import nonebot_plugin_alconna as alconna
from nonebot import get_bot, get_bots, logger
from nonebot.adapters import Bot

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..adapter_norm import normalize_adapter_name
from ..config import plugin_config, steer_reply_push_active
from ..core import rate_bucket, user_lists
from ..core.chat_config import load_chat_config
from ..core.hermes_client import (
    hermes_client,
)
from ..core.inflight import (
    PendingEntry,
    make_inflight_key,
    _SYNTHETIC_NICKNAME,
)
from ..core.message_buffer import (
    BufferedMessage,
)
from ..core.run_tracker import make_scope_key
from ..core.bg_tasks import spawn_bg_task
from ..core.prompt_builder import (
    _format_speaker_tag,
)
from ..core.routing import resolve_target
from .message_turns import (
    _run_passive_turn,
    _run_reactive_turn,
)
from .message_ingress import (
    _now_ms,
)
from .message_perception import (
    _claim_event_db_id,
)
from .message_feedback import (
    _sweep_ts_marks,
)

_chain_counts: dict[tuple[tuple[str, str], str], int] = {}
# (scope_key, kind) → 最近一次点火时刻(内存兜底淘汰用,不参与计数语义;
# 值保持 int 不变,既有回归按整数断言)
_chain_touched: dict[tuple[tuple[str, str], str], float] = {}

# 私聊排队汇总成批(规划书章二,topic: 项目文档库 §4):
# turn 收尾→接力的边界才是链式连发的真凶——slot 一释放,尾随消息就各自 try_enter
# 成功、各自开一轮;而每个接力 turn 本身又是一整轮 LLM,期间新消息继续补货。
# 修法:收尾发现 pending 非空时不立即 pop+refire,而是**先不释放 slot**、等一个聚合窗,
# 窗内新到消息经 try_enter 自然入队并同委托人合并(现成机制),窗后一次性 pop 开轮。
# **全量启用**(不限长 turn);窗长按候选区间 3-5s 定稿 4s
# (对齐应答防抖量级:防抖 2.5s 只覆盖同 turn 内的碎片,治不了跨 turn 边界)。
_QUEUE_AGGREGATE_WINDOW_SEC = 4.0

# 本部署补丁: steer 注入文本尾部挂的一次性约定行(运行时文本)——模型侧协议:
# 本条为对话中途插入的消息; 若模型之后想回应它, 约定调用 push_message 工具
# 发送回应(推送通道已泛化私聊), 其余内容照常。回应不占用正文、不挂任何标记段。
# 挂接条件 = steer_reply_push_active()(推送开关开且旁白关,注入时现读):
# 旁白开启时约定行不挂(流式全推送生效,模型写在旁白位的回应自然透出,
# 不留协议噪音)。
# 全角字符按转义写法,防写入管道把全角字面量规范化回半角。
_STEER_REPLY_CONVENTION_LINE = (
    "\uff08系统提示\uff1a本条为对话中途插入的消息\u3002若你之后回应它\uff0c"
    "请调用 push_message 工具发送回应\uff0c其余内容照常\u3002\uff09"
)


def _with_steer_reply_convention(text: str) -> str:
    """按生效判定给 steer 注入文本挂回应约定行(不生效原样返回)。"""
    if not steer_reply_push_active():
        return text
    return f"{text}\n{_STEER_REPLY_CONVENTION_LINE}"

def _chain_allow(key: tuple[str, str], kind: str, limit: int) -> bool:
    """同 scope 连续计数未达上限 → 允许点火。"""
    return _chain_counts.get((key, kind), 0) < limit

def _chain_bump(key: tuple[str, str], kind: str) -> None:
    _chain_counts[(key, kind)] = _chain_counts.get((key, kind), 0) + 1
    _chain_touched[(key, kind)] = time.time()
    # 链条计数只在「连续点火」期间有意义,对端不再说话的残留按闲置淘汰
    _sweep_ts_marks(_chain_touched, time.time() - 3600, companions=(_chain_counts,))

def _chain_reset(key: tuple[str, str], kind: str) -> None:
    _chain_counts.pop((key, kind), None)
    _chain_touched.pop((key, kind), None)

def _fire_followup_turn(
    *,
    key: tuple[str, str],
    tag: str,
    prompt: str,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
) -> None:
    """追加一发合成后续轮(create_task 排队,经 inflight 接力在本 turn 后跑,不并发)。

    全族唯一出口(Q1 合族):离场确认轮 / 类①继续执行轮 / 类②重交轮都走这里,
    只换 tag 与 prompt。合成身份由 is_synthetic=True 单一事实标记;昵称沿用现有
    约定(群聊 "system-check"、私聊真实 user_id + 昵称「系统核对」)只承担显示。
    """
    logger.info(f"[HERMES feedback] 追加后续轮({tag}) {key}")
    if is_private:
        spawn_bg_task(
            _handle_passive_path(
                bot=bot, target=target, adapter_name=adapter_name,
                user_id=user_id, nickname=_SYNTHETIC_NICKNAME, group_id=None,
                text=prompt, image_urls=[], is_private=True,
                now_ms=_now_ms(), is_synthetic=True,
            )
        )
    else:
        spawn_bg_task(
            _handle_reactive_path(
                bot=bot, target=target, adapter_name=adapter_name,
                user_id="system-check", nickname=_SYNTHETIC_NICKNAME, group_id=group_id,
                text=prompt, image_urls=[], is_direct=False,
                addressed_to_bot=False, now_ms=_now_ms(), ephemeral=True,
                is_synthetic=True,
            )
        )

async def _pop_pending_after_aggregate_window(
    key: tuple[str, str], *, after_ts: int, aggregate: bool
) -> PendingEntry | None:
    """turn 收尾取下一发:私聊被动路径先等一个聚合窗,把尾随消息并进同一条目。

    实现点(规划书 §4.2)。设计要点:
      - 还有 pending 时**不释放 slot**,先 await 聚合窗——窗内新到消息经 try_enter
        自然落队、并入同委托人条目(_merge_pending_message);窗满再一次性 pop 开轮。
        slot 一旦释放,尾随消息就会各自 try_enter 成功、各自开一轮,这正是要治的
        「跨 turn 边界连发」——所以合并只能靠「不释放 slot + 窗内等待」达成。
      - pending 为空(含只有过期条目)时立即返回,秒回场景零额外延迟。
      - aggregate 只在私聊被动路径为 True:reactive 路径与群聊被动路径(active
        session 关闭时的兜底)行为完全不变。
      - pending_steer 回收条目在本 helper 之前已入队,天然并入,无需特判。

    取队判据走 peek_next_pending(非破坏读,与 pop 同 stale 规则),确认有货才等窗。
    """
    assert _mcp.inflight is not None
    if aggregate and _mcp.inflight.peek_next_pending(key, after_ts=after_ts) is not None:
        logger.debug(
            f"[HERMES queue-aggregate] 等聚合窗 {_QUEUE_AGGREGATE_WINDOW_SEC}s key={key}"
        )
        await asyncio.sleep(_QUEUE_AGGREGATE_WINDOW_SEC)
    return _mcp.inflight.pop_next_pending(key, after_ts=after_ts)

def _release_slot_and_retry(key, adapter_name: str, is_private: bool, user_id: str, group_id) -> None:
    """slot 释放 + 同 scope 保留行送达重试。

    旧路:送达窗口超时的保留行只等下次 nb 启动,一等几小时(当日实证 2h 古董消息)。
    新径:任何 turn 收尾(slot 释放)即触发本 scope 的重试;fire-and-forget 不阻塞
    收尾主路,重试内部的 15 分钟时效闸(B)让过期行到此自然丢弃。"""
    _mcp.inflight.exit(key)
    if _mcp.run_tracker is not None:
        from ..tasks.run_resume import retry_scope_deliveries

        spawn_bg_task(retry_scope_deliveries(adapter_name, is_private, user_id, group_id))

async def _handle_passive_path(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    text: str,
    image_urls: list[str],
    sticker_urls: list[str] | None = None,
    is_private: bool,
    now_ms: int,
    nickname: str | None = None,
    event_msg_id: str | int | None = None,
    is_synthetic: bool = False,
):
    """Passive 外壳:inflight 占位 → _run_passive_turn → 合并重燃。

    与 reactive 同形,key 含 private/group 前缀区分。
    is_synthetic=合成轮身份事实（对账补投/任务投递/主动唤起等系统补投），
    落到 BufferedMessage 供合并函数做身份分界。
    """
    assert _mcp.inflight is not None

    key = make_inflight_key(adapter_name, is_private, user_id, group_id)

    current_buffered = BufferedMessage(
        ts=now_ms,
        adapter=adapter_name,
        group_id=group_id,
        user_id=user_id,
        nickname=nickname or user_id,
        content=text,
        image_urls=list(image_urls),
        sticker_urls=list(sticker_urls or []),
        reply_to_ts=None,
        is_bot=False,
        is_synthetic=is_synthetic,
    )
    # B 方案:凭本事件 message_id 领 perception 挂号的 db 真 id;合成消息领不到=None
    current_buffered.id = _claim_event_db_id(adapter_name, event_msg_id)

    if (
        _mcp.inflight.try_enter(
            key,
            current_buffered,
            is_direct=True,  # passive 路径只在 (private OR active_session=off) 触发, 二者都需要 user 显式说话
            original_msg_id=event_msg_id,
            now_ms=now_ms,
            commission=True,  # passive/私聊直说都按委托排队,不做闲聊 followup 合并
        )
        != "entered"
    ):
        return

    should_refire = False
    try:
        result = await _run_passive_turn(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            group_id=group_id,
            text=text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_private=is_private,
            now_ms=now_ms,
            current_buffered=current_buffered,
        )
        should_refire = not (result is not None and result.is_transport_error)
    except Exception:
        logger.exception(f"[HERMES] passive turn raised; dropping pending for {key}")
        should_refire = False
        raise
    finally:
        if not should_refire:
            _release_slot_and_retry(key, adapter_name, is_private, user_id, group_id)
        else:
            # 私聊被动路径的收尾边界先过聚合窗——窗内尾随消息
            # 并入同一条目后一次性开轮;窗内 slot 不释放(见 helper docstring)。
            pending_entry = await _pop_pending_after_aggregate_window(
                # 私聊聚合窗回退止血——当日三连 bug(m:id 错标/
                # 剥皮丢图/延迟送达)均与「合并批无单一 m:id」相关,深层拆批渲染单独立项
                # (见 planned-mods 私聊 m:id 专项);立项落地前私聊不等聚合窗,批规模压低。
                key, after_ts=current_buffered.ts, aggregate=False
            )
            if pending_entry is None:
                _release_slot_and_retry(key, adapter_name, is_private, user_id, group_id)
            else:
                spawn_bg_task(
                    _refire(
                        key=key,
                        trigger_msg=pending_entry.msg,
                        is_direct=pending_entry.is_direct,
                        addressed_to_bot=pending_entry.addressed_to_bot,
                        original_msg_id=pending_entry.original_msg_id,
                        depth=1,
                        mode="passive",
                        steer_recovered=pending_entry.steer_recovered,
                        bot=bot,
                        target=target,
                        adapter_name=adapter_name,
                        group_id=group_id,
                    )
                )

def _in_post_reply_cooldown(adapter_name: str, group_id: str, now_ms: int) -> bool:
    """B: 判断 (adapter, group_id) 是否处于 bot 上次回复后的冷却窗内。

    输入路径 (`_handle_reactive_path`) 和重燃路径 (`_refire`) 共用,确保:
      - 通过 reactive submit_decision 发出的回复 (_run_reactive_turn 末段写 mark_bot_replied)
      - 通过 MCP push_message 发出的回复 (push_message_impl 写 mark_bot_replied)
    两条路径都把后续非显式触发的旁观消息压在窗内,避免「同主题二次答复」。

    冷却仅对**非显式触发**生效——显式 @bot 必须立刻进 chat;调用方自行判断
    `is_direct` 后再询问本 helper。

    冷却窗禁用 (`reactive_post_reply_cooldown_sec == 0`) 或 session 不存在
    /已过期、未记录过 last_bot_reply_at → 返回 False。
    """
    assert _mcp.active_sessions is not None

    cooldown_sec = load_chat_config().reactive_post_reply_cooldown_sec
    if cooldown_sec <= 0:
        return False
    sess = _mcp.active_sessions.get_if_trigger_alive(adapter_name, group_id, now_ms)
    if sess is None or not sess.last_bot_reply_at:
        return False
    elapsed_ms = now_ms - sess.last_bot_reply_at
    return 0 <= elapsed_ms < cooldown_sec * 1000

def _steer_eligible_ids(
    adapter_name: str, is_private: bool, user_id: str, group_id: str | None, now_ms: int
) -> set[str]:
    """steer 资格集合(任务锚+steer 规划书§3.1):
    在跑 turn 触发者 ∪ 在钉锚全体参与者。私聊锚不存在,集合={live 触发者}。"""
    ids: set[str] = set()
    tracker = _mcp.run_tracker
    if tracker is not None:
        live = tracker.get_by_scope(make_scope_key(adapter_name, is_private, user_id, group_id))
        if live is not None:
            ids.add(live.user_id)
    if not is_private and group_id is not None and _mcp.active_sessions is not None:
        ids.update(
            _mcp.active_sessions.pinned_participant_ids(
                adapter_name, group_id, now_ms,
                max_age_ms=load_chat_config().anchor_max_age_sec * 1000,
            )
        )
    return ids

async def _steer_text_into_live_run(
    adapter_name: str, is_private: bool, user_id: str, group_id: str | None, text: str,
    *, m_id: str | int | None = None,
) -> bool:
    """把纯文本 steer 注入本 scope 在跑的 run。无 live run / 已收尾(409) /
    网络异常 → False(调用方落回正常排队,零丢失)。scope 感知版发送原语
    (规划书§3.2/3.3);群聊 followup 槽的 _try_steer_followup 也走它。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return False
    live = tracker.get_by_scope(make_scope_key(adapter_name, is_private, user_id, group_id))
    if live is None:
        return False
    ok = await hermes_client.steer_run(
        tgt=resolve_target(adapter_name, is_private, group_id), run_id=live.run_id, text=text
    )
    if ok:
        logger.info(
            f"[HERMES steer] 直注在跑 turn (scope={'private' if is_private else 'group'}:"
            f"{user_id if is_private else group_id} run_id={live.run_id} len={len(text)})"
        )
        # 本部署补丁(弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75 根因、alive/84 方案):
        # steer 被上游接受(HTTP 200 且 accepted=True)即记台账 pending, 供 run 终态
        # 对账:拉转录按 m:id 精确搜, 搜不到即被静默丢弃, 批 2 补投成新 run。仅开关开
        # 且 m_id 非 None 才入账;记台账失败只 WARN, 不影响 steer 主流程。
        if m_id is not None:
            try:
                tracker.record_steer(
                    run_id=live.run_id, m_id=str(m_id), scope_key=live.scope_key,
                    session_key=live.session_key, sender_id=str(user_id),
                    steer_text=text, accepted_at=time.time(),
                )
            except Exception:
                logger.warning(
                    f"[HERMES steer] 记台账失败(不影响 steer 主流程) "
                    f"run_id={live.run_id} m_id={m_id}"
                )
    return ok

async def _maybe_steer_direct(
    *,
    target,
    adapter_name: str,
    user_id: str,
    nickname: str | None,
    group_id: str | None,
    msg_text: str,
    is_direct: bool,
    now_ms: int,
    event_msg_id: str | int | None = None,
) -> bool:
    """资格内纯文本 steer 直注(规划书§3.2/3.3)。True=已注入,调用方不再 dispatch。

    私聊:本 scope 有 live run 即资格成立(在跑 run 必属本人)。
    群聊:仅委托(explicit=addressed)且发送者 ∈ 资格集合才直试;闲聊不直注,
    维持走 reactive 队尾的 followup 槽 steer(_try_steer_followup,现状)。
    """
    if not msg_text.strip():
        return False
    if not target.private:
        if not is_direct:
            return False
        if user_id not in _steer_eligible_ids(adapter_name, False, user_id, group_id, now_ms):
            return False
    # 本部署补丁(弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75 根因、alive/84 方案):
    # 有 m:id 时在发言人标签后挂 [m:xxx], 供批 2 对账按 m:id 精确搜转录;无 id 维持旧形态。
    speaker_tag = _format_speaker_tag(nickname, user_id)
    if event_msg_id is not None:
        text = _with_steer_reply_convention(
            f"{speaker_tag} [m:{event_msg_id}]: {msg_text.strip()}"
        )
    else:
        text = _with_steer_reply_convention(
            f"{speaker_tag}: {msg_text.strip()}"
        )
    return await _steer_text_into_live_run(
        adapter_name, target.private, user_id, group_id, text, m_id=event_msg_id
    )

async def _try_steer_followup(
    key: tuple[str, str],
    adapter_name: str,
    user_id: str,
    group_id: str,
) -> bool:
    """阶段2b:把 followup 槽里的跟帖 steer 注入在跑的 run,成功=True(不再排队)。

    只接纯文本跟帖:带图的走不了 steer(文本通道),留在槽里走 pending。
    表情包自 入队即剥(队列不承载),下方 sticker_urls 检查是
    防御性保留,正常路径不会再命中。注入文本前还会再剥一次 [表情包] 占位——
    占位对在跑 turn 是纯干扰,剥完无有效文本的条目不注入,
    留槽走接力。
    run 已收尾(409)/无 live run/网络异常 → 留在槽里走 pending,语义与旧路完全一致。

    改 peek/settle——条目在 steer 全程不离槽,
    期间 turn 收尾由 pop_next_pending 捡走接力,不再存在「离槽-恢复」的静默吞消息路径。
    注入文本是 await 前的快照:期间若有新跟帖并入(合并发生在同一个 entry 上),
    结算时内容对不上就不摘——并入内容留在槽里走 pending 接力(注入过的前缀会重复
    一次,但零丢失;丢失与重复的代价不对称,选重复)。
    """
    # 常备探针:入口与各早退口
    # 全部 INFO 级留痕——现网 INFO 级别下原 DEBUG 分支不可见,无法区分
    # 「未被调用」与「调用后静默早退」。日志标记 [HERMES steer探针]。
    logger.info(
        f"[HERMES steer探针] 进入 followup steer 尝试 (group={group_id} user={user_id})"
    )
    tracker = _mcp.run_tracker
    if tracker is None:
        logger.info(f"[HERMES steer探针] 早退: run_tracker 为 None (group={group_id})")
        return False
    entry = _mcp.inflight.peek_followup(key)
    if entry is None:
        logger.info(f"[HERMES steer探针] 早退: followup 槽为空 (group={group_id} key={key})")
        return False
    if entry.msg.image_urls or entry.msg.sticker_urls:
        logger.info(
            f"[HERMES steer探针] 早退: 条目带媒体 (group={group_id} "
            f"imgs={len(entry.msg.image_urls)} stickers={len(entry.msg.sticker_urls)})"
        )
        return False
    live = tracker.get_by_scope(make_scope_key(adapter_name, False, user_id, group_id))
    if live is None:
        logger.info(
            f"[HERMES steer探针] 早退: 查无 live run (group={group_id} "
            f"scope={make_scope_key(adapter_name, False, user_id, group_id)})"
        )
        return False
    content_snapshot = entry.msg.content
    # 注入前剥掉 [表情包] 占位:占位对在跑 turn 是纯干扰,
    # 轮内不需要知道「有人发了表情包」。剥完只剩空壳(纯表情包条目,或跨人并入
    # 留下的「昵称: 」空行)→ 不注入,条目留槽走 pending 接力,语义不变。
    steer_lines = []
    for ln in content_snapshot.split("\n"):
        ln = ln.replace("[表情包]", "").strip()
        if ln and not ln.endswith(":"):
            steer_lines.append(ln)
    steer_text = "\n".join(steer_lines).strip()
    if not steer_text:
        logger.info(
            f"[HERMES steer探针] 早退: 剥占位后无有效文本(纯表情包条目) "
            f"(group={group_id})"
        )
        return False
    # 本部署补丁(弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75 根因、alive/84 方案):
    # 跟帖槽条目是入库回填过 id 的 BufferedMessage, 有 id 时在发言人标签后挂 [m:xxx]
    # 供批 2 对账按 m:id 精确搜转录;无 id 维持旧形态。m:id 挂在最终 steer_text 的
    # 发言人标签行上(上方剥表情包逻辑保持原样)。
    _m_id = entry.msg.id
    speaker_tag = _format_speaker_tag(entry.msg.nickname, entry.msg.user_id)
    if _m_id is not None:
        text = _with_steer_reply_convention(
            f"{speaker_tag} [m:{_m_id}]: {steer_text}"
        )
    else:
        text = _with_steer_reply_convention(
            f"{speaker_tag}: {steer_text}"
        )
    ok = await hermes_client.steer_run(
        tgt=resolve_target(adapter_name, False, group_id), run_id=live.run_id, text=text
    )
    if not ok:
        logger.info(
            f"[HERMES steer探针] 早退: steer_run 返回 False (group={group_id} run_id={live.run_id})"
        )
        return False
    # 本部署补丁(弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75 根因、alive/84 方案):
    # 跟帖 steer 被上游接受即记台账 pending(供批 2 对账按 m:id 搜转录)。本路径不经过
    # _steer_text_into_live_run(直连 hermes_client.steer_run), 故在此内联记账, 条件与
    # 直注路径一致:仅开关开且 m_id 非 None;记台账失败只 WARN, 不影响 steer 主流程。
    if _m_id is not None:
        try:
            tracker.record_steer(
                run_id=live.run_id, m_id=str(_m_id), scope_key=live.scope_key,
                session_key=live.session_key, sender_id=str(entry.msg.user_id),
                steer_text=text, accepted_at=time.time(),
            )
        except Exception:
            logger.warning(
                f"[HERMES steer] 记台账失败(不影响 steer 主流程) "
                f"run_id={live.run_id} m_id={_m_id}"
            )
    if _mcp.inflight.peek_followup(key) is entry and entry.msg.content == content_snapshot:
        _mcp.inflight.settle_followup(key, entry)
    else:
        # await 期间并入了新内容(或被接力取走):留槽走 pending,不摘。
        logger.debug(f"[HERMES reactive] steer 期间 followup 有并入,留槽接力 (group={group_id})")
    logger.info(
        f"[HERMES reactive] 跟帖已 steer 注入在跑 turn (group={group_id} "
        f"user={entry.msg.user_id} run_id={live.run_id} len={len(text)})"
    )
    return True

async def _handle_reactive_path(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str,
    text: str,
    image_urls: list[str],
    sticker_urls: list[str] | None = None,
    is_direct: bool,
    addressed_to_bot: bool = False,
    now_ms: int,
    nickname: str | None = None,
    event_msg_id: str | int | None = None,
    ephemeral: bool = False,
    force_skip_cooldown: bool = False,
    droppable: bool = False,
    recent_override: list[BufferedMessage] | None = None,
    replied_msg_id=None,
    context_limit: int | None = None,
    is_synthetic: bool = False,
):
    """Reactive 外壳:inflight 占位 → 调 _run_reactive_turn → finally 合并重燃。

    coalesce 语义:in-flight 期间到来的新触发不并发跑;委托 @ 进按委托人分键的
    FIFO 队列(总 2/每人 1 由 3 调 2),旁观闲聊只留一个最新 followup。本发完成后
    pop_next_pending 取下一发,用 create_task 起 _refire 接力,本 task 立即 return,
    不阻塞 NoneBot 事件循环。

    ephemeral / recent_override / context_limit(本部署新增)原样透传给
    _run_reactive_turn,语义见该函数 docstring。ephemeral turn 没有窗口,
    cooldown 与租约本来就是 None 安全路径,照常走。
    """
    assert _mcp.inflight is not None and _mcp.active_sessions is not None

    # inflight key、current_buffered、db id 领号三件上移到门控之前：窄化门控要在此处
    # 探测 is_inflight 并可能直接 try_enter 合并，主流程下方复用同一份（领 id 是缓存
    # 一次性 pop，重复领会领空，故原调用点已删）。
    key = make_inflight_key(adapter_name, False, user_id, group_id)
    current_buffered = BufferedMessage(
        ts=now_ms,
        adapter=adapter_name,
        group_id=group_id,
        user_id=user_id,
        nickname=nickname or user_id,
        content=text,
        image_urls=list(image_urls),
        sticker_urls=list(sticker_urls or []),
        reply_to_ts=None,
        is_bot=False,
        is_synthetic=is_synthetic,
    )
    # B 方案：凭本事件 message_id 领 perception 挂号的 db 真 id；合成消息领不到=None
    current_buffered.id = _claim_event_db_id(adapter_name, event_msg_id)

    is_commission = addressed_to_bot and not user_lists.is_known_bot(user_id)

    # 图片门控(窄化)：active window + 非直说 + 纯图/纯表情包无文本。
    # 有在跑 turn → 照常 try_enter 入队合并 followup，消息到场但不点火；无在跑 turn
    # → 躺 buffer 等下个文本触发，语义同窄化前。直说（is_direct，如纯图+@）不受门控，
    # 照常走主流程点火（纯图+@ 照常点火的既有形态不退化）。
    # 合并分支绕开下方 post-reply cooldown 检查是刻意设计：合并进在跑 turn 不是新点火，
    # cooldown 闸只管新点火，越权代拦会把 turn 进行中的合法跟帖一并吞掉。
    # 附带留痕:纯图合并进槽时条目带 image_urls，会触发 steer 探针
    # 带媒体早退（_try_steer_followup），此为已知毒化面，待后续专项处置；表情包入队
    # 时会被入队剥占位实现 _strip_stickers_for_queue 剥成“[表情包]”占位，不毒化。
    in_active = _mcp.active_sessions.is_alive_for_trigger(adapter_name, group_id, now_ms)
    if in_active and not is_direct and (image_urls or sticker_urls) and not text.strip():
        if _mcp.inflight.is_inflight(key):
            # slot 已被占，try_enter 只排队不点火，天然满足“不点火但到场”。
            inflight_result = _mcp.inflight.try_enter(
                key,
                current_buffered,
                is_direct=is_direct,
                original_msg_id=event_msg_id,
                now_ms=now_ms,
                addressed_to_bot=addressed_to_bot,
                ephemeral=ephemeral,
                commission=is_commission,
            )
            logger.debug(
                f"[HERMES reactive] merged media-only into running turn "
                f"(group={group_id} user={user_id} result={inflight_result})"
            )
            return
        logger.debug(
            f"[HERMES reactive] media-only passive in-window msg stays buffered "
            f"(group={group_id} user={user_id}); awaiting next text trigger"
        )
        return

    # B: post-reply cooldown — bot 刚在本群发出 reactive 回复 N 秒内,
    # 新消息直接静默。压「我刚说完别人接话→我又凑一句」模式 2。
    # 本部署补丁: 显式 @bot 不再旁路——双 bot 互 @ 穿透冷却等于没有限速器,
    # 互聊循环全速助燃;砍后冷却窗对一切触发生效(含委托 @,窗内静默、
    # 内容留 buffer 不丢,窗口短可接受)。
    # 写在 inflight try_enter 之前,避免占用 slot 又立刻退出造成 pending 抖动。
    # steer 对账补投(alive/84)专用豁免:补投必在最终回复后的 cooldown 窗内,
    # 不豁免则被静默吞;其余调用方维持 False 不变。
    if in_active and not force_skip_cooldown:
        # debug 日志保留入口处,便于运维排查;helper 自己不打日志(refire 路径也会用)
        sess = _mcp.active_sessions.get_if_trigger_alive(adapter_name, group_id, now_ms)
        cooldown_sec = load_chat_config().reactive_post_reply_cooldown_sec
        logger.debug(
            f"[HERMES reactive] cooldown_check group={group_id} user={user_id} "
            f"sess_exists={sess is not None} "
            f"last_bot_reply_at={sess.last_bot_reply_at if sess else 'n/a'} "
            f"now_ms={now_ms} window_ms={cooldown_sec * 1000}"
        )
        if _in_post_reply_cooldown(adapter_name, group_id, now_ms):
            elapsed_ms = now_ms - (sess.last_bot_reply_at if sess else 0)
            logger.debug(
                f"[HERMES reactive] skip: post-reply cooldown "
                f"(group={group_id} elapsed_ms={elapsed_ms} window_ms={cooldown_sec * 1000})"
            )
            return

    # droppable=True 的合成轮(巡查)
    # 撞在跑 turn 直接丢弃——「巡查天生可弃」,不占 followup 槽不接力,否则活跃期
    # 巡查会在用户 turn 结束后不请自来。投递轮询等不可弃合成轮不传此标记。
    if droppable and _mcp.inflight.is_inflight(key):
        logger.debug(f"[HERMES reactive] droppable turn discarded (inflight busy): group={group_id}")
        return
    inflight_result = _mcp.inflight.try_enter(
        key,
        current_buffered,
        is_direct=is_direct,
        original_msg_id=event_msg_id,
        now_ms=now_ms,
        addressed_to_bot=addressed_to_bot,
        # 条目自带 ephemeral——接力时按
        # 「这条排队消息自己的无窗判定」跑,不再被发包 turn 的值污染。
        ephemeral=ephemeral,
        commission=is_commission,  # 群聊只有平台确认冲 bot 的人类 @ 才进委托队列
    )
    if inflight_result != "entered":
        # 阶段2b:非委托跟帖在 runs 通道下先尝 steer 注入在跑 turn(成功即不排队);
        # 委托/带媒体/旧路/run 已收尾被拒 → 一律维持原 pending 语义。
        # 排除 ephemeral——定时巡查(user=timer)、
        # 任务投递(user=delivery)这类合成输入也是非委托纯文本,此前会被原样注入
        # 用户进行中的 turn(合成提示词污染对话),且被消费后不再走 pending。
        if (
            inflight_result == "queued"
            and not is_commission
            and not ephemeral
            and plugin_config.hermes_api_interface == "runs"
            and await _try_steer_followup(key, adapter_name, user_id, group_id)
        ):
            return
        # 入队是 coalesce 的正常语义,但必须可观测:这条分支早先对 bystander 完全静默,
        # 一串消息集体没反应时日志里只剩 cooldown_check,查不出去向。
        # 常备探针:DEBUG→INFO 并
        # 补 steer 门各条件取值,定位「queued 后未走 steer」时卡在哪一项。
        logger.info(
            f"[HERMES steer探针] queued behind in-flight turn "
            f"(group={group_id} user={user_id} direct={is_direct} "
            f"commission={is_commission} ephemeral={ephemeral} "
            f"runs_channel={plugin_config.hermes_api_interface == 'runs'} result={inflight_result})"
        )
        return

    # should_refire_pending=False 仅在 _run_reactive_turn 抛异常时:那是 plugin
    # 自己的 bug,不该把 pending 回放(可能立刻再炸)。transport_error 是上游
    # 故障(Hermes 超时 / 5xx),pending 里的下一发(特别是 explicit @)仍要
    # refire,不能被静默吞掉。
    should_refire_pending = False
    # 租约:本 turn 跑多久,活跃窗就至少活多久。慢 turn(生图 / 长工具链)跨过 TTL 后,
    # 收尾的 touch / mark_bot_replied 和 pending 的 refire 全都依赖窗口还在。
    lease = _mcp.active_sessions.begin_turn(adapter_name, group_id)
    try:
        await _run_reactive_turn(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            text=text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_direct=is_direct,
            addressed_to_bot=addressed_to_bot,
            now_ms=now_ms,
            trigger_msg_id=event_msg_id,
            ephemeral=ephemeral,
            recent_override=recent_override,
            context_limit=context_limit,
            replied_msg_id=replied_msg_id,
            current_buffered=current_buffered,
        )
        should_refire_pending = True
    except Exception:
        logger.exception(f"[HERMES] reactive turn raised; dropping pending for {key}")
        raise
    finally:
        # 先还租约(剩余窗口不足时在这里垫到下限),再决定要不要接力 —— 顺序反了
        # 就等于让 refire 去撞一个刚死的窗口。
        _mcp.active_sessions.end_turn(lease, now_ms=_now_ms())
        if not should_refire_pending:
            _release_slot_and_retry(key, adapter_name, False, user_id, group_id)
        else:
            # 群聊 reactive 路径**不启用**聚合窗(有群级防抖合并先例,
            # 另行核实再说)——aggregate=False 走同一 helper 保持出队语义单点。
            pending_entry = await _pop_pending_after_aggregate_window(
                key, after_ts=current_buffered.ts, aggregate=False
            )
            if pending_entry is None:
                _release_slot_and_retry(key, adapter_name, False, user_id, group_id)
            else:
                spawn_bg_task(
                    _refire(
                        key=key,
                        trigger_msg=pending_entry.msg,
                        is_direct=pending_entry.is_direct,
                        original_msg_id=pending_entry.original_msg_id,
                        # 补上接力透传——此前这条 finally 链路
                        # 丢 addressed_to_bot,排队委托 refire 起跑后 addressed 门控全失效
                        # (图片不内联/归属标记缺行);passive 侧与递归链本来就传。
                        addressed_to_bot=pending_entry.addressed_to_bot,
                        depth=1,
                        mode="reactive",
                        # 读条目自己的 ephemeral——
                        # 此前透传发包 turn 的值,冷窗关键词 turn 里排队的 addressed
                        # 委托会被以 ephemeral=True 跑掉(不读钉锚 + 二次 trigger 窗口)。
                        ephemeral=pending_entry.ephemeral,
                        # 迟到 steer 回收标记透传(cooldown 豁免)。
                        steer_recovered=pending_entry.steer_recovered,
                        bot=bot,
                        target=target,
                        adapter_name=adapter_name,
                        group_id=group_id,
                    )
                )

async def _refire(
    *,
    key,
    trigger_msg: BufferedMessage,
    is_direct: bool,
    original_msg_id: str | int | None,
    depth: int,
    mode: str,
    addressed_to_bot: bool = False,
    ephemeral: bool = False,
    steer_recovered: bool = False,
    bot: Bot,
    target,
    adapter_name: str,
    group_id,
):
    """链式重燃。fire-and-forget,深度上限 MAX_REFIRE_DEPTH。

    ephemeral(本部署新增)沿重燃链透传:临时 turn 期间排进 pending 的
    keyword-soft 命中需要以同样的无窗口方式续跑,否则会在 _run_reactive_turn
    入口的 get_if_trigger_alive 处被当「窗口没了」误吞。显式 pending 不受影响——
    显式触发在 dispatch 时已 trigger() 开窗,get_if_trigger_alive 必然命中。"""
    from ..core.inflight import MAX_REFIRE_DEPTH

    assert _mcp.inflight is not None

    # 表情包无需在此降级:队列不承载表情包——入队时点
    # 已在 inflight 层剥离成 [表情包] 文本占位(见 core/inflight.py
    # _strip_stickers_for_queue),接力出来的条目 sticker_urls 恒为空。
    # 字节在到达时已入库,召回路径不受影响。

    if depth > MAX_REFIRE_DEPTH:
        _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)  # release slot first to avoid race with concurrent arrivals
        logger.warning(f"[HERMES] refire depth exceeded ({depth}); dropping pending {key}")
        return

    # now_ms 用 wall-clock 而不是 trigger_msg.ts:_run_*_turn 内部用它做
    # get_if_trigger_alive 的 TTL 校验、active_sessions.touch 的滑动续期、以及 bot
    # 自己回复的 BufferedMessage.ts。如果用 trigger 时间会导致 touch 后窗口
    # 比预期早 N 秒过期、bot 回复时间戳倒退。trigger_msg.ts 只在 finally 的
    # pending.ts 比对里用,那是消息到达时序而非「当前是几点」。
    refire_now_ms = _now_ms()

    # B: refire 路径同款 post-reply cooldown 闸门。
    # 本部署补丁: explicit @ 穿透已砍除(与入口闸同批)——接力轮同样吃冷却,
    # 委托 @ 在窗内被静默(内容留 buffer),steer_recovered 仍单列豁免:回收条目
    # 的 ts 是回收时刻(网关只还回文本,拿不到原到达时刻),按到达时刻判仍会误拦。
    # 判定时刻用「跟帖到达」而非「接力起跑」——旧口径下
    # 本 turn 一发回复 last_bot_reply_at 即刷新,refire 必撞 8s 窗,等于把全部
    # 「turn 在跑期间到达的合法跟帖」接力一律静默吞掉(当日受控群两轮实测实锤;
    # 跟帖到达时已过入口闸,是合法排队,该不该再答应交接力轮的 submit_decision
    # 自决,而不是被闸门越权替模型决定)。
    if (
        mode == "reactive"
        and group_id is not None
        and not steer_recovered
        and _in_post_reply_cooldown(adapter_name, str(group_id), trigger_msg.ts)
    ):
        logger.info(f"[HERMES reactive] refire skipped by post-reply cooldown (key={key} depth={depth})")
        _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)
        return

    lease = None
    if mode == "reactive" and group_id is not None and _mcp.active_sessions is not None:
        lease = _mcp.active_sessions.begin_turn(adapter_name, str(group_id))

    should_refire = False
    try:
        if mode == "reactive":
            assert group_id is not None
            result = await _run_reactive_turn(
                bot=bot,
                target=target,
                adapter_name=adapter_name,
                user_id=trigger_msg.user_id,
                nickname=trigger_msg.nickname,
                group_id=group_id,
                text=trigger_msg.content,
                image_urls=list(trigger_msg.image_urls),
                sticker_urls=list(trigger_msg.sticker_urls),
                is_direct=is_direct,
                addressed_to_bot=addressed_to_bot,
                now_ms=refire_now_ms,
                trigger_msg_id=original_msg_id,
                ephemeral=ephemeral,
                current_buffered=trigger_msg,
            )
        else:
            result = await _run_passive_turn(
                bot=bot,
                target=target,
                adapter_name=adapter_name,
                user_id=trigger_msg.user_id,
                group_id=trigger_msg.group_id,
                text=trigger_msg.content,
                image_urls=list(trigger_msg.image_urls),
                sticker_urls=list(trigger_msg.sticker_urls),
                is_private=trigger_msg.group_id is None,
                now_ms=refire_now_ms,
                current_buffered=trigger_msg,
            )
        should_refire = not (result is not None and result.is_transport_error)
        if result is None and is_direct:
            # 兜底挂点(保留留痕,后续可能移除):排过队的直说触发被吞必须留痕。
            # 具体原因由 _run_*_turn 内部那条 drop 日志给出,这条负责点出「被吞的是
            # 一个排过队的 direct @」,两条合起来才够定位。
            logger.warning(
                f"[HERMES] queued direct trigger produced no turn (key={key} depth={depth} msg_id={original_msg_id})"
            )
    except Exception:
        logger.exception(f"[HERMES] refire raised at depth {depth}; dropping pending for {key}")
        should_refire = False
    finally:
        if lease is not None:
            _mcp.active_sessions.end_turn(lease, now_ms=_now_ms())
        if not should_refire:
            _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)
            # B012 豁免: try 侧异常已被上方 except 捕获记录,不存在被吞的活异常;
            # refire 是独立 create_task 的旁路接力,CancelledError 窗口仅停机时。
            return  # noqa: B012
        pending_entry = await _pop_pending_after_aggregate_window(
            key,
            after_ts=trigger_msg.ts,
            # 接力轮的收尾边界是链式连发的另一半——私聊被动接力同样过聚合窗
            # (mode="passive" 且 group_id is None = 私聊);群聊接力/群聊被动保持不变。
            # 私聊聚合窗回退止血(同 _handle_passive_path 处注释),
            # 深层拆批渲染立项落地前统一不等窗。
            aggregate=False,
        )
        if pending_entry:
            spawn_bg_task(
                _refire(
                    key=key,
                    trigger_msg=pending_entry.msg,
                    is_direct=pending_entry.is_direct,
                    addressed_to_bot=pending_entry.addressed_to_bot,
                    original_msg_id=pending_entry.original_msg_id,
                    depth=depth + 1,
                    mode=mode,
                    # 链上每一跳都按「当前条目
                    # 自己的 ephemeral」接力,不再把链首外层值一路透传下去。
                    ephemeral=pending_entry.ephemeral,
                    # 迟到 steer 回收标记沿链透传。
                    steer_recovered=pending_entry.steer_recovered,
                    bot=bot,
                    target=target,
                    adapter_name=adapter_name,
                    group_id=group_id,
                )
            )
        else:
            _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)

async def route_synthesized_input(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    nickname: str | None,
    text: str,
    allow_passive: bool,
    addressed_to_bot: bool,
    now_ms: int,
):
    """合成消息的统一入口,供 notice handler 复用既有 message routing。

    派发规则(active_session 固定开启):
      - private (target.private=True) → 仅 allow_passive=True 才走 passive,否则跳过
      - group → 触发 active session 并走 reactive(synth 始终算 is_direct=True)

    `addressed_to_bot` 由合成方按事件语义给:平台确认事件指向 bot 本体(如戳 bot)
    才是 True;群级事件(如入群)虽然 explicit 触发,但不是「冲 bot 说的」,
    断言了就是对模型撒谎,还会让「被点名却静默」的 WARNING 变成噪音。

    `allow_passive` 只控制私聊兜底:
      - 戳一戳: True(私聊也开口)
      - 入群: False(群侧经 reactive 由 Hermes 自决)
    """
    if target.private:
        if not allow_passive:
            return
        await _handle_passive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=None,
            text=text,
            image_urls=[],
            is_private=True,
            now_ms=now_ms,
            is_synthetic=True,
        )
        return

    # 群聊(active_session 固定开启)→ 显式触发 + reactive
    # (与 handle_message 显式触发同语义: 先 trigger,再 _handle_reactive_path)
    assert _mcp.active_sessions is not None
    _mcp.active_sessions.trigger(adapter_name, group_id or "", user_id, now_ms=now_ms)
    logger.info(f"[HERMES notice] synthesized reactive trigger: {adapter_name}/{group_id} by {user_id}")
    await _handle_reactive_path(
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
        text=text,
        image_urls=[],
        is_direct=True,
        addressed_to_bot=addressed_to_bot,
        now_ms=now_ms,
        is_synthetic=True,
    )


# --- 主动唤起(本部署补丁) ---
#
# 唤起 = 合成输入注入的泛化:任意内容唤起一轮 LLM turn,复用 _handle_passive_path /
# _handle_reactive_path(ephemeral) 全套管线(信封/反馈/限流/留痕),发言天然进
# 会话上下文。三道闸(按序):白名单硬闸(私侧) → per-target 令牌桶(独立 YAML
# 现读现用) → 路由解析(registry 优先、无 entry 合成兜底)。
# 防重入:唤起轮里模型再调 wake 工具时由令牌桶挡下。
# 内部 API 保持干净函数签名,供 MCP 工具与插件侧 apscheduler 定时任务共用
# (定时提醒类点火的落点;为未来提示系统演进预留的调用面)。

_DEFAULT_WAKE_SOURCE = "定时提醒"


def _resolve_wake_route(adapter_name: str, scope: str, scope_id: str):
    """解析 (bot, target):BotRegistry 优先,无 entry 时合成兜底。

    兜底先例:适配器过滤选 bot(同 core/approvals.py 的 get_bots 过滤) +
    alconna Target 合成构造(同 handlers/notices.py 的 _build_target)。
    返回 (bot, target);失败返回给调用方看的错误串(str)。
    """
    if _mcp.bot_registry is not None:
        entry = _mcp.bot_registry.get(adapter_name, scope, scope_id)  # type: ignore[arg-type]
        if entry is not None:
            try:
                return get_bot(entry.bot_self_id), entry.target
            except (KeyError, ValueError) as exc:
                return f"bot 离线(self_id={entry.bot_self_id}): {exc}"
    matched = sorted(
        (
            b
            for b in get_bots().values()
            if normalize_adapter_name(getattr(b.adapter, "get_name", lambda: "")()) == adapter_name
        ),
        key=lambda b: str(getattr(b, "self_id", "")),
    )
    if not matched:
        return (
            f"registry 无 entry 且无 {adapter_name} 适配器 bot 在线——"
            "目标重启后需先发言重建路由,或等 bot 上线后再试"
        )
    return (
        matched[0],
        alconna.Target(id=scope_id, private=(scope == "private"), adapter=adapter_name),
    )


def _wake_rate_limited(adapter_name: str, scope: str, scope_id: str) -> str | None:
    """令牌桶闸:取格成功 → None;超限 → 给调用方看的报错文案。"""
    limits = rate_bucket.load_wake_limits()
    bucket_key = f"{adapter_name}:{scope}:{scope_id}"
    if rate_bucket.try_acquire(bucket_key, capacity=limits.capacity, refill_seconds=limits.refill_seconds):
        return None
    wait = rate_bucket.seconds_until_next(bucket_key, refill_seconds=limits.refill_seconds)
    return (
        f"唤起频率超限(每 target 容量 {limits.capacity} 格、每 {limits.refill_seconds}s 回 1 格),"
        f"约 {wait}s 后再试;不排队、不延后,请勿重试撞桶"
    )


async def wake_private_turn(
    *, adapter_name: str, user_id: str, text: str, source: str = _DEFAULT_WAKE_SOURCE
) -> str | None:
    """唤起一条私聊 turn。返回 None = 已点火;返回 str = 拒绝原因(原样回给调用方)。

    闸序:私聊主动下行白名单硬闸(user_lists.yaml,未配置回落管理员集合) → 令牌桶 →
    路由解析(registry 优先/合成兜底)→ _handle_passive_path。
    """
    if not user_lists.private_allowed(adapter_name, user_id):
        return f"目标不在私聊主动下行白名单(user_lists.yaml 的 private_allow 或管理员集合): {user_id}"
    denied = _wake_rate_limited(adapter_name, "private", user_id)
    if denied is not None:
        return denied
    route = _resolve_wake_route(adapter_name, "private", user_id)
    if isinstance(route, str):
        return route
    bot, target = route
    spawn_bg_task(
        _handle_passive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=source,
            group_id=None,
            text=text,
            image_urls=[],
            is_private=True,
            now_ms=_now_ms(),
            is_synthetic=True,
        )
    )
    logger.info(f"[HERMES wake] 私聊唤起已点火: {adapter_name}/{user_id} source={source}")
    return None


async def wake_group_turn(
    *, adapter_name: str, group_id: str, text: str, source: str = _DEFAULT_WAKE_SOURCE
) -> str | None:
    """唤起一条群聊 ephemeral turn(无窗口依赖)。返回 None = 已点火;str = 拒绝原因。

    闸序:令牌桶 → 路由解析(registry 优先/合成兜底)→ _handle_reactive_path。
    """
    denied = _wake_rate_limited(adapter_name, "group", group_id)
    if denied is not None:
        return denied
    route = _resolve_wake_route(adapter_name, "group", group_id)
    if isinstance(route, str):
        return route
    bot, target = route
    spawn_bg_task(
        _handle_reactive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id="wake",
            nickname=source,
            group_id=group_id,
            text=text,
            image_urls=[],
            is_direct=False,
            addressed_to_bot=False,
            now_ms=_now_ms(),
            ephemeral=True,
            is_synthetic=True,
        )
    )
    logger.info(f"[HERMES wake] 群聊唤起已点火: {adapter_name}/{group_id} source={source}")
    return None