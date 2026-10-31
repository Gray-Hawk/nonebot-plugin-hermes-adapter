"""接力/steer 域:链条计数、后续轮点火、pending 聚合出队、slot 释放、steer 直注/followup、_refire 接力与 route_synthesized_input 入口。

从 handlers/message.py 拆出;纯搬运,零逻辑改动。
"""

from __future__ import annotations

import asyncio
import copy
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
)
from ..core.message_origin import MessageOrigin, MessageSource
from ..core.message_buffer import (
    BufferedMessage,
    iter_message_media,
)
from ..core.run_tracker import make_scope_key
from ..core.bg_tasks import spawn_bg_task
from ..core.prompt_builder import (
    render_steer_rows,
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
    _peek_event_db_id,
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
    只换 tag 与 prompt。tag 只用于选择来源枚举；路由 id 不再兼任身份判据。
    """
    logger.info(f"[HERMES feedback] 追加后续轮({tag}) {key}")
    source = {
        "confirm": MessageSource.FEEDBACK_CONFIRM,
        "relay": MessageSource.FEEDBACK_RELAY,
        "bounce": MessageSource.FEEDBACK_BOUNCE,
    }[tag.split(":", 1)[0]]
    origin = MessageOrigin.synthetic(source)
    if is_private:
        spawn_bg_task(
            _handle_passive_path(
                bot=bot, target=target, adapter_name=adapter_name,
                user_id=user_id, nickname=origin.display_label, group_id=None,
                text=prompt, image_urls=[], is_private=True,
                now_ms=_now_ms(), origin=origin,
            )
        )
    else:
        spawn_bg_task(
            _handle_reactive_path(
                bot=bot, target=target, adapter_name=adapter_name,
                user_id="system-check", nickname=origin.display_label, group_id=group_id,
                text=prompt, image_urls=[], is_direct=False,
                addressed_to_bot=False, now_ms=_now_ms(), ephemeral=True,
                origin=origin,
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
    origin: MessageOrigin,
    rows=None,
    delivery_generation: int | None = None,
    source_event_id: str | None = None,
):
    """Passive 外壳:inflight 占位 → _run_passive_turn → 合并重燃。

    与 reactive 同形,key 含 private/group 前缀区分。origin 是文本来源唯一事实，
    路由 user_id、昵称和 m:id 不再参与来源推断。
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
        origin=origin,
        image_urls=list(image_urls),
        sticker_urls=list(sticker_urls or []),
        reply_to_ts=None,
        is_bot=False,
        delivery_generation=delivery_generation,
        source_event_id=source_event_id,
    )
    # 只有可入库来源才能领 perception 挂号的真 id；来源与事件号各管各的。
    current_buffered.id = _claim_event_db_id(adapter_name, event_msg_id) if origin.allows_store else None
    render_rows = list(rows or [current_buffered])

    if (
        _mcp.inflight.try_enter(
            key,
            current_buffered,
            is_direct=True,  # passive 路径只在 (private OR active_session=off) 触发, 二者都需要 user 显式说话
            original_msg_id=event_msg_id,
            now_ms=now_ms,
            commission=True,  # passive/私聊直说都按委托排队,不做闲聊 followup 合并
            fragments=render_rows,
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
            current_rows=render_rows,
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
                # 私聊聚合窗恢复(alive/103 设计定案):拆批渲染落地后
                # 批内每条碎片各自持真实 m:id,当年「合并批无单一 m:id」的三连
                # 故障根因已除,回退止血撤销,恢复私聊被动路径聚合窗。
                key, after_ts=current_buffered.ts, aggregate=is_private
            )
            if pending_entry is None:
                _release_slot_and_retry(key, adapter_name, is_private, user_id, group_id)
            else:
                spawn_bg_task(
                    _refire(
                        key=key,
                        trigger_msg=pending_entry.msg,
                        current_rows=pending_entry.fragments,
                        is_direct=pending_entry.is_direct,
                        addressed_to_bot=pending_entry.addressed_to_bot,
                        original_msg_id=pending_entry.original_msg_id,
                        depth=1,
                        mode="passive",
                        bot=bot,
                        target=target,
                        adapter_name=adapter_name,
                        group_id=group_id,
                    )
                )

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
    adapter_name: str, is_private: bool, user_id: str, group_id: str | None, *,
    presentation,
) -> bool:
    """把 steer 整批(presentation, alive/103 §17.9.2)注入本 scope 在跑的 run。

    无 live run / 已收尾(409) / 网络异常 → False(调用方落回正常排队,零丢失)。
    scope 感知版发送原语(规划书§3.2/3.3);直注与群聊 followup 槽共用。
    发送文本=presentation.text＋末尾一次约定行(若开关开);逐行台账与呈现去重
    只吃 presentation.rows——发送、台账、seen 是同一份身份事实,无第二条轨。
    """
    tracker = _mcp.run_tracker
    if tracker is None:
        return False
    live = tracker.get_by_scope(make_scope_key(adapter_name, is_private, user_id, group_id))
    if live is None:
        return False
    # 台账/seen 在 HTTP 前从同一份 presentation 投影：同号同值保序合并；
    # 同号异 sender/正文是身份事实矛盾，整批回退，绝不先发送无法可靠对账的文本。
    items: list[dict] = []
    seen_mids: list = []
    by_mid: dict[str, dict] = {}
    for item in presentation.rows:
        mid = item.source_row.id
        if mid is None:
            continue
        key = str(mid)
        existing = by_mid.get(key)
        if existing is not None:
            if (existing["sender_id"] != str(item.source_row.user_id)
                    or existing["steer_text"] != item.rendered_line):
                logger.warning(
                    f"[HERMES steer] 同号身份/正文冲突,发送前整批回退 m:id={key} "
                    f"(run_id={live.run_id} 既有 sender={existing['sender_id']} "
                    f"冲突 sender={item.source_row.user_id})"
                )
                return False
            continue
        entry = {
            "m_id": key,
            "sender_id": str(item.source_row.user_id),
            "steer_text": item.rendered_line,
        }
        by_mid[key] = entry
        items.append(entry)
        seen_mids.append(mid)
    text = presentation.text
    if steer_reply_push_active():
        text = _with_steer_reply_convention(text)
    ok = await hermes_client.steer_run(
        tgt=resolve_target(adapter_name, is_private, group_id), run_id=live.run_id, text=text
    )
    if ok:
        logger.info(
            f"[HERMES steer] 直注在跑 turn (scope={'private' if is_private else 'group'}:"
            f"{user_id if is_private else group_id} run_id={live.run_id} len={len(text)})"
        )
        # 本部署补丁(alive/75 根因、alive/84 方案):steer 被上游接受即按发送前
        # 已去重/校验的 presentation 投影逐行记 pending 台账。无号行不入账；
        # 记台账失败只 WARN, 不影响主流程。
        if items:
            try:
                tracker.record_steer(
                    run_id=live.run_id, scope_key=live.scope_key,
                    session_key=live.session_key, accepted_at=time.time(),
                    items=items,
                )
            except Exception:
                logger.warning(
                    f"[HERMES steer] 记台账失败(不影响 steer 主流程) run_id={live.run_id}"
                )
        # 呈现去重(alive/103 §14.3):本批已随 steer 给模型看过,recent 不再重复呈现。
        scope = make_inflight_key(adapter_name, is_private, user_id, group_id)
        for mid in seen_mids:
            _steer_seen_mark(scope, mid)
    return ok


def _stable_unique(urls: list[str]) -> list[str]:
    """保序去重(alive/103 §17.9.6):防抖等合法路径会合并掉重复 URL,
    媒体校验必须投影到同一空间再比,否则合法合批被误杀。"""
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _is_subsequence(observed: list[str], aggregate: list[str]) -> bool:
    """observed 是否为 aggregate 的子序列(同序、可缺不可多)。"""
    it = iter(aggregate)
    return all(any(u == a for a in it) for u in observed)

# --- steer 呈现去重(alive/103 §14.3):实现迁 core/steer_seen.py(turn 渲染侧
# 也要过滤,relay→turn 单向依赖防循环 import);此处 re-export 保持既有调用面。
from ..core.steer_seen import (
    _steer_seen_filter,
    _steer_seen_mark,
    _steer_seen_reset_for_test,
)


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
    origin: MessageOrigin,
    event_msg_id: str | int | None = None,
    image_urls: list[str] | None = None,
    sticker_urls: list[str] | None = None,
    # 拆批(alive/103 §17.9.1):触发载荷 fragments 序列——steer 逐行身份/媒体/
    # 台账/去重的唯一事实源。None=旧调用点,按聚合参数规范化构造单条。
    rows=None,
) -> bool:
    """资格内 steer 直注(规划书§3.2/3.3)。True=已注入,调用方不再 dispatch。

    私聊:本 scope 有 live run 即资格成立(在跑 run 必属本人)。
    群聊:仅委托(explicit=addressed)且发送者 ∈ 资格集合才直试;闲聊不直注,
    维持走 reactive 队尾的 followup 槽 steer(_try_steer_followup,现状)。

    多行合批(alive/103 §17.9):行装配与 current 共用 render_steer_rows——
    每行 rendered_line 是实发整批的连续子串,媒体寻址 [图 m:id#idx] 只贴
    该行自己的 m:id,从根上消灭 S1 真机缺陷(媒体错贴前一条 m:id)。
    媒体整批校验走稳定去重投影的子序列(alive/103 §17.9.6):合法去重(同 URL
    连发/同人追加)与跨人替换(旧 fragments 保留)放行,真正缺件/顺序矛盾失败
    关闭(落回正常 dispatch 走内联帧),不注断链引用。任一行来源闸不通过或
    渲染拒绝(ValueError)整批返回 False——半贴号/丢媒体/改写原始消息都不如
    安全回退。平台 event.message_id 只作 perception 桥的查找键(peek 不 claim),
    绝不进模型可见文本。
    """
    if not origin.allows_steer or not msg_text.strip():
        return False
    if not target.private:
        if not is_direct:
            return False
        if user_id not in _steer_eligible_ids(adapter_name, False, user_id, group_id, now_ms):
            return False
    if rows is not None:
        steer_rows = [r for r in rows if r is not None]
    else:
        fragment = BufferedMessage(
            ts=now_ms, adapter=adapter_name, group_id=group_id, user_id=user_id,
            nickname=nickname or user_id, content=msg_text, origin=origin,
            image_urls=list(image_urls or []), sticker_urls=list(sticker_urls or []),
            reply_to_ts=None, is_bot=False,
        )
        # steer 挂载只认插件 m:id(alive/103 三套编号边界):平台 event.message_id
        # 仅作查找键;取不到维持无号形态,绝不拿平台 ID 顶替。
        fragment.id = _peek_event_db_id(adapter_name, event_msg_id)
        steer_rows = [fragment]
    if not steer_rows:
        return False
    # 聚合载荷是 rows 的外壳视图,可能少观测(跨人替换:旧 fragments 保留、
    # 外壳换成新触发)也可能稳定去重(同人连发)。校验方向:聚合(去重后、保序)
    # 必须是行观测投影(去重后、保序)的子序列——外壳说的每件事行都交代得到,
    # 顺序一致;行多说的部分(旧碎片)放行。真正缺件/顺序矛盾失败关闭(alive/103
    # §17.9.6)。
    observed_images = _stable_unique([
        url for row in steer_rows for kind, url in iter_message_media(row)
        if kind in ("image", "quote")  # 聚合 image_urls=own+quote,与入库分型对齐
    ])
    observed_stickers = _stable_unique([
        url for row in steer_rows for kind, url in iter_message_media(row)
        if kind == "sticker"
    ])
    if not _is_subsequence(_stable_unique(list(image_urls or [])), observed_images):
        return False
    if not _is_subsequence(_stable_unique(list(sticker_urls or [])), observed_stickers):
        return False
    try:
        presentation = render_steer_rows(steer_rows)
    except ValueError:
        # 任一行来源闸(SYSTEM/MIXED/BOT)或媒体寻址不合法 → 整批安全回退。
        return False
    return await _steer_text_into_live_run(
        adapter_name, target.private, user_id, group_id, presentation=presentation
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
    防御性保留,正常路径不会再命中。注入前对每行做独立副本投影(rework-1 F1/F2):
    按正文子行剥 [表情包] 占位、去空白、剔冒号空壳后重连,保留有效子行及原行
    作者/m:id;媒体检查先于空壳丢弃——投影行任一仍带 own/quote/forward 媒体
    即整批留 pending,剥完无有效文本的纯表情空壳才不注入、留槽走接力。
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
    if not entry.msg.origin.allows_steer:
        logger.info(f"[HERMES steer探针] 早退: 来源不允许 steer (group={group_id})")
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
    # 冻结发送集(alive/103 §17.9.4):await 前独立投影 followup fragments;
    # 发送/台账/seen 只吃这份冻结 presentation。结算时同时比较 fragments
    # 对象身份与 content——任何新 fragment 并入、或槽被 pop/替换,都不得摘槽。
    frozen = list(entry.fragments or [])
    fragment_identity_snapshot = tuple(id(row) for row in frozen)
    render_rows = frozen or [entry.msg]
    # 逐行独立副本投影(rework-1 终审 F1/F2,替代旧的整行 endswith(":") 过滤):
    #  F1: 冒号空壳/表情占位的清理落到正文子行级——「有效指令\n作者:」这类多行
    #      内容不再因整行以冒号收尾被整段误删;每行先在独立副本内按子行剥
    #      [表情包] 占位、去空白、剔冒号空壳(仅尾部冒号、无正文的子行),
    #      有效子行重连后仍归原 row 的作者与 m:id。
    #  F2: 媒体检查先于空壳丢弃——表情剥离后的投影行只要还剩 own/quote/forward
    #      任一媒体,整批留 pending(steer 文本通道注不了断链引用)。空正文图片行
    #      不再先被内容过滤整行丢掉而让媒体检查永远看不到它——否则剩余文本行
    #      steer 成功即摘整槽,图被静默吞掉。
    projected: list[BufferedMessage] = []
    for row in render_rows:
        prow = copy.copy(row)
        if prow.sticker_urls or "[表情包]" in (prow.content or ""):
            prow.sticker_urls = []
        kept = [
            seg
            for seg in (
                seg.replace("[表情包]", "").strip()
                for seg in (prow.content or "").split("\n")
            )
            if seg and not seg.endswith((":", "："))
        ]
        prow.content = "\n".join(kept)
        projected.append(prow)
    if any(iter_message_media(prow) for prow in projected):
        logger.info(
            f"[HERMES steer探针] 早退: 投影行仍带 own/quote/forward 媒体,整批留 pending "
            f"(group={group_id})"
        )
        return False
    cleaned = [prow for prow in projected if (prow.content or "").strip()]
    if not cleaned:
        logger.info(
            f"[HERMES steer探针] 早退: 剥占位后无有效文本(纯表情包条目) "
            f"(group={group_id})"
        )
        return False
    try:
        presentation = render_steer_rows(cleaned)
    except ValueError:
        logger.info(
            f"[HERMES steer探针] 早退: 行来源闸/渲染拒绝 (group={group_id})"
        )
        return False
    ok = await _steer_text_into_live_run(
        adapter_name, False, user_id, group_id, presentation=presentation
    )
    if not ok:
        logger.info(
            f"[HERMES steer探针] 早退: steer_run 返回 False (group={group_id} run_id={live.run_id})"
        )
        return False
    # 结算冻结:发送过的冻结集随成功路径记账/去重完毕;槽是否摘下只看
    # 「槽仍是这个槽且 fragments 仍是那批对象且正文没变」。必须比较对象身份，
    # 不能用 dataclass 值相等——同内容的新 fragment 也代表 await 期间有新并入。
    if (
        _mcp.inflight.peek_followup(key) is entry
        and tuple(id(row) for row in entry.fragments) == fragment_identity_snapshot
        and entry.msg.content == content_snapshot
    ):
        _mcp.inflight.settle_followup(key, entry)
    else:
        # await 期间并入了新 fragment(或被接力取走):留槽走 pending,不摘。
        logger.debug(f"[HERMES reactive] steer 期间 followup 有并入,留槽接力 (group={group_id})")
    logger.info(
        f"[HERMES reactive] 跟帖已 steer 注入在跑 turn (group={group_id} "
        f"user={entry.msg.user_id} run_id={live.run_id} len={len(presentation.text)})"
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
    droppable: bool = False,
    recent_override: list[BufferedMessage] | None = None,
    replied_msg_id=None,
    context_limit: int | None = None,
    origin: MessageOrigin,
    rows=None,
    delivery_generation: int | None = None,
    source_event_id: str | None = None,
    commission_override: bool | None = None,
):
    """Reactive 外壳:inflight 占位 → 调 _run_reactive_turn → finally 合并重燃。

    coalesce 语义:in-flight 期间到来的新触发不并发跑;委托 @ 进按委托人分键的
    FIFO 队列(总 2/每人 1 由 3 调 2),旁观闲聊只留一个最新 followup。本发完成后
    pop_next_pending 取下一发,用 create_task 起 _refire 接力,本 task 立即 return,
    不阻塞 NoneBot 事件循环。

    ephemeral / recent_override / context_limit(本部署新增)原样透传给
    _run_reactive_turn,语义见该函数 docstring。ephemeral turn 没有窗口,
    租约本来就是 None 安全路径,照常走。
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
        origin=origin,
        image_urls=list(image_urls),
        sticker_urls=list(sticker_urls or []),
        reply_to_ts=None,
        is_bot=False,
        delivery_generation=delivery_generation,
        source_event_id=source_event_id,
    )
    # 只有可入库来源才能领 perception 挂号的真 id；来源与事件号各管各的。
    current_buffered.id = _claim_event_db_id(adapter_name, event_msg_id) if origin.allows_store else None
    render_rows = list(rows or [current_buffered])

    is_commission = addressed_to_bot and not user_lists.is_known_bot(user_id) if commission_override is None else commission_override

    # 图片门控(窄化)：active window + 非直说 + 纯图/纯表情包无文本。
    # 有在跑 turn → 照常 try_enter 入队合并 followup，消息到场但不点火；无在跑 turn
    # → 躺 buffer 等下个文本触发，语义同窄化前。直说（is_direct，如纯图+@）不受门控，
    # 照常走主流程点火（纯图+@ 照常点火的既有形态不退化）。
    # 合并分支排在 droppable/try_enter 之前是刻意设计：合并进在跑 turn 不是新点火，
    # 排队语义只管「能不能立刻开轮」，越权代拦会把 turn 进行中的合法跟帖一并吞掉。
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
                fragments=render_rows,
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

    # droppable=True 的合成轮(巡查)
    # 撞在跑 turn 直接丢弃——「巡查天生可弃」,不占 followup 槽不接力,否则活跃期
    # 巡查会在用户 turn 结束后不请自来。投递轮询等不可弃合成轮不传此标记。
    if droppable and _mcp.inflight.is_inflight(key):
        logger.debug(f"[HERMES reactive] droppable turn discarded (inflight busy): group={group_id}")
        return
    # 调度观测(规划书 §11):只记本次入口的最终动作,不记正文。
    # 防抖等待仍由入口防抖的 DEBUG 记录;这里补上「立即开轮 / inflight 合流」的
    # INFO 级区分,填掉旧冷却 DEBUG 丢弃日志消失后的定位空档。
    logger.info(
        f"[HERMES sched] reactive entry group={group_id} user={user_id} "
        f"direct={is_direct} addressed={addressed_to_bot} commission={is_commission} "
        f"ephemeral={ephemeral} inflight_busy={_mcp.inflight.is_inflight(key)}"
    )
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
        fragments=render_rows,
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
        # 一串消息集体没反应时日志里查不出去向。
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
            current_rows=render_rows,
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
                        current_rows=pending_entry.fragments,
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
    # 拆批 fragments 序列(alive/103 §14.2):来自 pending_entry.fragments,
    # None=无拆批信息走单行旧路径。
    current_rows=None,
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
    refire_rows = list(current_rows or [trigger_msg])
    refire_text = "\n".join(row.content for row in refire_rows if row.content)
    refire_images = []
    for row in refire_rows:
        refire_images.extend(u for u in row.image_urls if u not in refire_images)

    # 接力不再有 post-reply 冷却闸:排队条目在到达时已过入口资格与防抖,
    # 该不该再答应由接力轮的 submit_decision 自决,而不是被时间窗越权替模型决定。
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
                text=refire_text,
                image_urls=refire_images,
                sticker_urls=list(trigger_msg.sticker_urls),
                is_direct=is_direct,
                addressed_to_bot=addressed_to_bot,
                now_ms=refire_now_ms,
                trigger_msg_id=original_msg_id,
                ephemeral=ephemeral,
                current_buffered=trigger_msg,
                current_rows=refire_rows,
            )
        else:
            result = await _run_passive_turn(
                bot=bot,
                target=target,
                adapter_name=adapter_name,
                user_id=trigger_msg.user_id,
                group_id=trigger_msg.group_id,
                text=refire_text,
                image_urls=refire_images,
                sticker_urls=list(trigger_msg.sticker_urls),
                is_private=trigger_msg.group_id is None,
                now_ms=refire_now_ms,
                current_buffered=trigger_msg,
                current_rows=refire_rows,
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
            # 私聊聚合窗随 alive/103 恢复(拆批落地,批内碎片各有真实 m:id)。
            aggregate=(mode == "passive" and group_id is None),
        )
        if pending_entry:
            spawn_bg_task(
                _refire(
                    key=key,
                    trigger_msg=pending_entry.msg,
                    current_rows=pending_entry.fragments,
                    is_direct=pending_entry.is_direct,
                    addressed_to_bot=pending_entry.addressed_to_bot,
                    original_msg_id=pending_entry.original_msg_id,
                    depth=depth + 1,
                    mode=mode,
                    # 链上每一跳都按「当前条目
                    # 自己的 ephemeral」接力,不再把链首外层值一路透传下去。
                    ephemeral=pending_entry.ephemeral,
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
    source: MessageSource,
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
    origin = MessageOrigin.synthetic(source)
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
            origin=origin,
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
        origin=origin,
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
    *, adapter_name: str, user_id: str, text: str, source: str = _DEFAULT_WAKE_SOURCE,
    source_event_id: str | None = None,
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
            origin=MessageOrigin.synthetic(MessageSource.WAKE_MANUAL, label=source),
            source_event_id=source_event_id,
        )
    )
    logger.info(f"[HERMES wake] 私聊唤起已点火: {adapter_name}/{user_id} source={source}")
    return None


async def wake_group_turn(
    *, adapter_name: str, group_id: str, text: str, source: str = _DEFAULT_WAKE_SOURCE,
    source_event_id: str | None = None,
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
            origin=MessageOrigin.synthetic(MessageSource.WAKE_MANUAL, label=source),
            source_event_id=source_event_id,
            commission_override=source_event_id is not None,
        )
    )
    logger.info(f"[HERMES wake] 群聊唤起已点火: {adapter_name}/{group_id} source={source}")
    return None