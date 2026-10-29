"""turn 执行域:dispatch 装配与 passive/reactive 两路 turn 执行、chat 重试/溢出恢复、同 turn 复述抑制。

从 handlers/message.py 拆出;纯搬运,零逻辑改动。
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import replace
from functools import partial

from nonebot import logger
from nonebot.adapters import (
    Bot,
    Event,
)

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..config import plugin_config
from ..core.active_session import (
    ActiveSession,
    AnchorRecord,
    Participant,
)
from ..core.approvals import admin_user_ids
from ..core.group_profile import build_group_profile_block
from ..core import identity
from ..config import resolve_custom_dir
from ..core.hermes_client import (
    ChatResult,
    extract_response_media,
    hermes_client,
    maybe_extract_decision_reply_text,
    preview_raw,
)
from ..core.image_inline import (
    inline_gif_frames,
    inline_image_frames,
)
from ..core.media_config import load_media_config
from ..core.inflight import (
    PendingEntry,
    make_inflight_key,
)
from ..core.log_throttle import throttled_info
from ..core.run_delivery_monitor import prepare_outbound_delivery, settle_expected
from ..core.steer_seen import _steer_seen_filter
from ..core.message_buffer import (
    BufferedMessage,
    build_media_address_index,
)
from ..core.message_origin import MessageOrigin
from ..core import task_notes
from ..core import user_lists
from ..core.chat_config import load_chat_config
from ..core.misc_config import load_misc_config
from ..core.outbound import (
    get_bot_nickname,
    send_local_files,
    send_result_history,
    send_result_sticker_count,
    send_text_with_media,
)
from ..core.reply_resolution import resolve_reply
from ..core.reply_recovery import deliver_recovered_replies
from ..core.prompt_builder import (
    build_passive_system_prompt,
    build_passive_user_content,
    build_reactive_system_prompt,
    build_reactive_user_content,
    split_unpushed_messages,
)
from ..core.routing import resolve_target
from ..core.session import session_manager
from .message_media import (
    _absent_media_note,
    _collect_inline_image_urls,
    _label_groups,
)
from .message_ingress import (
    _is_blacklisted,
    _now_ms,
)

def _inline_media_window(window_msgs: list[BufferedMessage]) -> list[BufferedMessage]:
    """纯文本后端只让当前消息媒体进入载荷，历史媒体保留文本占位。

    AstrBot 的历史图片不随本轮重复上传，只留文本占位；但不能把窗口清空——
    `_collect_inline_image_urls` 还要用窗口内容判 semi 门（窗内是否 @bot）与逐行
    有效模式，清空会让半内联门恒判未命中、连当前消息图一起丢掉。故保留同长浅拷贝、
    仅清 image_urls，原消息对象一律不动（缓冲里的历史仍可召回）。
    """
    if plugin_config.hermes_api_interface == "astrbot":
        return [replace(m, image_urls=[]) for m in window_msgs]
    return window_msgs


def _observe_silence(path: str, adapter_name: str, group_id: str | None, is_private: bool) -> None:
    """403/暂不可用硬静默的节流观测(抽出以便离线行为测试)。

    硬静默分支已有一条 error 级日志(每条触发);这里补同因节流 INFO 观测,
    故障期 1 小时只留一条,不与 error 逐条刷屏重复,也不改静默语义。
    """
    scope = "private" if is_private else f"group:{group_id}"
    throttled_info(
        f"gate.403:{path}:{adapter_name}:{scope}",
        f"[HERMES {path}] 403/暂不可用硬静默观测 (group={group_id} is_private={is_private})",
    )

def _filter_current_and_steered(
    recent: list[BufferedMessage], *, current_rows, scope: tuple[str, str],
) -> list[BufferedMessage]:
    """recent 呈现去重(alive/103 §14.2/§14.3)。

    - 已 steer 成功呈现的行不再随 recent 重复(§14.3);
    - current 拆批序列覆盖的行不再随 recent 重复——拆批后它们已在
      current_message 逐行各贴真号呈现,recent 再出一次就是双份(§14.2)。
    current 不过滤自身:留槽接力等场景 current 必须完整呈现。
    """
    rows = _steer_seen_filter(scope, recent)
    if current_rows:
        ids = {r.id for r in current_rows if r.id is not None}
        if ids:
            rows = [r for r in rows if r.id not in ids]
    return rows


async def _dispatch_chat(
    *,
    bot: Bot,
    event: Event,
    target,
    adapter_name: str,
    user_id: str,
    nickname: str | None,
    group_id: str | None,
    msg_text: str,
    image_urls: list[str],
    sticker_urls: list[str],
    is_direct: bool,
    addressed_to_bot: bool,
    now_ms: int,
    ephemeral: bool = False,
    context_limit: int | None = None,
    replied_msg_id=None,
    # 拆批 fragments 序列(alive/103 §14.2):防抖合并后的有序完整碎片,
    # None=无拆批信息(旧路径单行)。
    fragments=None,
    # 载荷携带的媒体寻址序列(alive/103 A4):dispatch 层不消费,但必须签收——
    # _dispatch_chat(**st.payload) 整包展开,缺形参会在入口直接 TypeError。
    fragment_db_ids=None,
):
    """passive/reactive 分叉(原 handle_message 末段,防抖批抽出以便
    防抖到期后与防抖关闭路径共用同一 dispatch)。"""
    # 任务锚+steer(规划书§3):资格内直注在跑 turn——私聊恒资格,
    # 群聊仅委托且发送者在资格集合(在跑 turn 触发者 ∪ 在钉锚全体参与者)。
    # 直注成功即 return;注不进去落回正常 dispatch,零丢失。
    # 媒体不再一票否决(alive/103):文字＋图/表情走降级注入(寻址引用＋读图提示),
    # 无插件 m:id 的带媒体消息在 _maybe_steer_direct 内失败关闭,落回内联帧路径。
    if (
        plugin_config.hermes_api_interface == "runs"
        and not ephemeral
        and await _maybe_steer_direct(
            target=target, adapter_name=adapter_name, user_id=user_id, nickname=nickname,
            group_id=group_id, msg_text=msg_text, is_direct=is_direct,
            now_ms=now_ms, origin=MessageOrigin.human(),
            event_msg_id=getattr(event, "message_id", None),
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            batch_ids=[f.id for f in (fragments or []) if f.id is not None] or None,
        )
    ):
        return
    if target.private:
        # 原 v0.1.6 等价路径:passive 模式,raw_text 直接当回复
        await _handle_passive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            text=msg_text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_private=target.private,
            now_ms=now_ms,
            event_msg_id=getattr(event, "message_id", None),
            origin=MessageOrigin.human(),
            rows=fragments,
        )
        return

    # 群聊 + 活跃态启用 → reactive 决策
    await _handle_reactive_path(
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
        text=msg_text,
        image_urls=image_urls,
        sticker_urls=sticker_urls,
        is_direct=is_direct,
        addressed_to_bot=addressed_to_bot,
        now_ms=now_ms,
        event_msg_id=getattr(event, "message_id", None),
        origin=MessageOrigin.human(),
        # keyword-soft 窗口外命中:跑无窗口临时 turn(不开窗);窗口内则正常跑,
        # 两种都推 N1 条上下文(不用续发轮裁剪)。
        ephemeral=ephemeral,
        context_limit=context_limit,
        replied_msg_id=replied_msg_id,
        rows=fragments,
    )

# 上游 locked 类持久化失败 = 别的 Hermes 进程正在写 state.db,上游要求「过一会再发一次」。
# 等一拍再重试,让持有写锁的一方先落库;不做指数退避,单次补偿够覆盖锁窗口,
# 拖长只会把群里的回复延迟摊得更明显。
_PERSISTENCE_RETRY_DELAY_S = 1.0

# 同 turn 去重的判据。刻意很窄:只认「近乎逐字复述」。
#
# 早期规则是"本 turn 内 push 过就把 submit_decision 整条抑制",它把最自然的用法
# ——push 一句「在查」、decision 给真答案——也一起打死了:答案被静默吞掉,而 agent
# 以为自己发出去了,还会向用户汇报已发送。包含关系不能当判据,「引用刚说的话 + 补上
# 新内容」同样是包含。所以失败方向明确选:宁可群里多一条啰嗦的,不可吞掉答案。
_RESTATEMENT_NOISE = str.maketrans(
    "",
    "",
    " \t\r\n，。！？、；：~～…!?,.;:「」『』\"'“”‘’()()【】[]",
)

def _is_restatement(pushed: str, reply: str) -> bool:
    """reply 是否只是把刚发出去的 pushed 又说了一遍。

    归一化(去空白/标点/大小写)后完全相同,或一方包含另一方且多出来的部分几乎为零,
    才算复述。pushed 为空(不知道原文)一律返回 False。
    """
    a = pushed.translate(_RESTATEMENT_NOISE).casefold()
    b = reply.translate(_RESTATEMENT_NOISE).casefold()
    if not a or not b:
        return False
    if a == b:
        return True
    longer, shorter = (b, a) if len(b) >= len(a) else (a, b)
    if shorter not in longer:
        return False
    return len(longer) - len(shorter) <= max(4, len(longer) // 10)

async def _chat_and_adopt_rotation(**chat_kwargs) -> ChatResult:
    """调 chat();上游本轮换过 session(自动压缩触发)就立刻采纳新 id。

    采纳必须发生在每一次 chat 之后而不只是最后一次:重试那轮同样可能碰上轮换,
    漏掉就又把会话钉回已关闭的父会话。
    """
    result = await hermes_client.chat(**chat_kwargs)
    if result.effective_session_key:
        session_manager.adopt_session_key(chat_kwargs["session_key"], result.effective_session_key)
    # T1 活动基准:无论成败,chat() 走完就算本会话有 LLM 活动。
    # chat_kwargs 四元组在 passive / reactive 两条路径上都齐全。
    session_manager.mark_activity(
        chat_kwargs["adapter_name"],
        chat_kwargs["is_private"],
        chat_kwargs["user_id"],
        chat_kwargs.get("group_id"),
    )
    return result

async def _chat_with_persistence_retry(label: str, /, **chat_kwargs) -> ChatResult:
    """调 chat();命中瞬时(locked)持久化失败时用**同一个** session_key 重试一次。

    label 走 positional-only,余下 kwargs 原样转给 hermes_client.chat,
    调用点因此和直接调 chat() 读起来一样。

    不主动换 session_key:上游三类 cause(locked / disk / unknown)都指向同一个 state.db,
    换 key 只是新开一个会话写同一个库,治不了锁也治不了满盘,代价是整个群的
    Hermes 侧上下文当场清零。disk / unknown 直接不重试 —— 重发也写不进去,而持久化
    失败是在 turn 收尾阶段判定的(工具此时已经跑过),白重试一次等于让副作用再来一遍。

    **例外**:上游自己轮换了会话(同一轮里既压缩又撞锁)时,重试要跟上采纳后的新 key。
    那不是"我们换 key",而是上游告知"活的会话已经是这个了";继续用旧 key 是往一个
    已关闭的父会话里写,必然失败。
    """
    result = await _chat_and_adopt_rotation(**chat_kwargs)
    if not result.is_persistence_error:
        return result

    if result.persistence_cause != "locked":
        # disk / unknown 要人介入(清盘、修 state.db 权限、hermes doctor),
        # 拉到 error 级别让运维扫日志能看见;回复本身走 transport_error 兜底屏蔽。
        logger.error(
            f"[HERMES {label}] 上游 Session 持久化失败且不可自动恢复 "
            f"(cause={result.persistence_cause}),不重试;请检查 Hermes 侧磁盘 / state.db"
        )
        return result

    if result.effective_session_key:
        # 采纳只更新了 SessionManager 的映射,本轮的 kwargs 还攥着旧 key;
        # 不同步过来,重试就把已关闭的父会话又钉回去了。
        chat_kwargs["session_key"] = result.effective_session_key

    logger.warning(f"[HERMES {label}] 上游 state.db 写锁冲突,{_PERSISTENCE_RETRY_DELAY_S}s 后重试一次")
    await asyncio.sleep(_PERSISTENCE_RETRY_DELAY_S)
    retried = await _chat_and_adopt_rotation(**chat_kwargs)
    if retried.is_persistence_error:
        logger.error(f"[HERMES {label}] 重试后仍持久化失败 (cause={retried.persistence_cause}),放弃本轮")
    return retried

# 重置丢掉的是 Hermes 侧的 transcript,不是全部上下文:群聊每轮仍会注入插件自己的
# `<recent_messages>` 窗口(私聊不注入),近处的对话照样看得见,接不上的是窗口之外、
# 更早的那部分。措辞因此只说"再往前的",写成"什么都不记得了"在群聊里就是假的。
_SESSION_RESET_NOTICE = "(上下文满了,我重开了一段对话 —— 再往前的就接不上了)"

async def _chat_with_overflow_recovery(label: str, /, *, reply_bot: Bot, reply_target, **chat_kwargs) -> ChatResult:
    """调 chat();撞上「上下文溢出 + 上游自动压缩已关」时重置会话并重发一次。

    上游在 compression.enabled: false 下禁掉了包括会话轮换在内的全部自动恢复,只回一段
    提示文本。而窗口只涨不缩:不管它的话下一轮更大,这条会话从此每轮都失败。

    换 session key 是对症的唯一手段 —— 那段历史窗口本身就是病因。这与持久化失败**正好
    相反**(见 _chat_with_persistence_retry:那一类刻意不换 key,因为换了既治不了写锁也
    治不了满盘,却白白清空整群上下文)。两处的判断不要互相抄。

    只重发一次:新会话仍然溢出说明病因不在历史,而在单轮本身(system prompt + 历史窗口
    就已经超了),再转一圈只会连着烧 generation 且每次都失败。

    reply_bot / reply_target 刻意不叫 bot / target:chat_kwargs 里的 `target` 是出向接入点
    (HermesTarget),与发消息用的会话目标同名不同物。
    """
    # 阶段2a:bot_self_id 顺 kwargs 注入 chat()(runs 路 run_tracker 登记用),
    # 单点注入覆盖全部调用点;completions 路忽略此参数。
    result = await _chat_with_persistence_retry(
        label, bot_self_id=str(reply_bot.self_id), **chat_kwargs
    )
    if not result.is_context_overflow:
        return result

    adapter_name = chat_kwargs["adapter_name"]
    is_private = chat_kwargs["is_private"]
    user_id = chat_kwargs["user_id"]
    group_id = chat_kwargs["group_id"]
    logger.error(
        f"[HERMES {label}] 上游上下文溢出且自动压缩已关,重置会话后重发一次 "
        f"(group={group_id} stale_key={chat_kwargs.get('session_key')})"
    )

    # 先说再答:重发可能很慢,而且这条提示即使重发失败也依然属实(会话确实已经重置)。
    await send_text_with_media(
        bot=reply_bot,
        target=reply_target,
        text=_SESSION_RESET_NOTICE,
        media_urls=[],
        adapter_name=adapter_name,
    )

    session_manager.clear_session(adapter_name, is_private, user_id, group_id)
    chat_kwargs["session_key"] = session_manager.get_session_key(adapter_name, is_private, user_id, group_id)

    retried = await _chat_with_persistence_retry(
        label, bot_self_id=str(reply_bot.self_id), **chat_kwargs
    )
    if retried.is_context_overflow:
        logger.error(
            f"[HERMES {label}] 重置后仍然溢出,放弃本轮(group={group_id})—— 病因不在历史而在单轮本身:"
            "system prompt 加历史窗口就已经超了,请缩小窗口或换更大上下文的模型"
        )
    return retried

def _delivery_identity(result) -> tuple[object, str | None]:
    """runs 半接管交付身份(alive/108):旧结果对象/非 runs 通道安全返回 None。"""
    return getattr(_mcp, "run_tracker", None), getattr(result, "run_id", None)


def _settle(result, expected_state: str, reason: str) -> None:
    """业务销账:把本轮不发送的事实落到台账;未入账 run 安全无操作。"""
    tracker, run_id = _delivery_identity(result)
    settle_expected(tracker=tracker, run_id=run_id, expected_state=expected_state, reason=reason)


def _prepare_delivery(result, *, cleaned_text: str, media_urls, file_paths,
                      reply_to=None, reply_qq_message_id=None, reply_at_user_id=None):
    """按最终输出构造投递计划;失败/未入账返回 None,发送按 legacy 照常。"""
    tracker, run_id = _delivery_identity(result)
    return prepare_outbound_delivery(
        tracker=tracker, run_id=run_id, cleaned_text=cleaned_text,
        media_urls=media_urls, file_paths=file_paths, reply_to=reply_to,
        reply_qq_message_id=reply_qq_message_id, reply_at_user_id=reply_at_user_id,
    )


async def _run_passive_turn(
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
    current_buffered: BufferedMessage | None = None,
    # 拆批 fragments 序列(alive/103 §14.2):有序完整碎片;None=单行旧路径。
    current_rows=None,
):
    """跑一发 passive turn,返回 ChatResult 或 None(被 submit_decision 静默兜底等情况)。"""
    misc_cfg = load_misc_config()
    session_key = session_manager.get_session_key(
        adapter_name=adapter_name,
        is_private=is_private,
        user_id=user_id,
        group_id=group_id,
    )
    hermes_target = resolve_target(adapter_name, is_private, group_id)

    # 群聊 + 默认配置(active_session=false)+ perception_enabled:补回 0.1.6
    # 「@bot 时让 LLM 看到群里旁观历史」。before_ts=now_ms 排除 perception 在
    # 同一事件 priority=1 时刚写入的当前消息,避免历史里出现重复。
    # 私聊不注入(0.1.6 起 perception 在私聊就是 no-op,Hermes session 已覆盖)。
    # 历史从 0.2.x 起放进 user content 而非 system,以维持 system 字节稳定。
    recent: list[BufferedMessage] = []
    if not is_private and group_id and _mcp.message_buffer is not None:
        recent = list(
            _mcp.message_buffer.get_recent(
                adapter=adapter_name,
                group_id=group_id,
                limit=load_chat_config().history_message_limit,
                before_ts=now_ms,
            )
        )

    # 拆批/呈现去重(alive/103 §14.2/§14.3):current 序列覆盖的行与已 steer
    # 呈现的行不再随 recent 重复出现。私聊 recent 恒空,过滤为 no-op。
    recent = _filter_current_and_steered(
        recent,
        current_rows=current_rows,
        scope=make_inflight_key(adapter_name, is_private, user_id, group_id),
    )

    # 本部署补丁: 私聊挂 submit_decision 决策信封
    # (全隐式化:私聊不再「硬必回」,模型按私聊版协议自决,无实质内容可静默)。
    # 群聊 passive(active_session=off 的旧部署形态)维持 raw_text 直发不动。
    use_private_decision = is_private
    # 本部署补丁:计算记忆根路径,用于路由提示动态跟随配置
    _custom_dir = resolve_custom_dir()
    _memory_root = str(_custom_dir / "memory" / adapter_name) if _custom_dir else None
    # 身份表：发言者标签正主名注释，custom_dir 不可用/查不到都静默 None
    _name_resolver = (
        (lambda uid, _cd=_custom_dir, _ad=adapter_name: identity.resolve_display_name(_cd, _ad, uid))
        if _custom_dir else None
    )
    system_prompt = build_passive_system_prompt(
        adapter=adapter_name,
        is_private=is_private,
        user_id=user_id,
        group_id=group_id,
        private_decision=use_private_decision,
        # 群记忆外挂:群档案注入块(私聊 group_id=None 内部静默跳过)
        profile_block=build_group_profile_block(adapter_name, group_id),
        # 本部署补丁:传入功能开关,私聊时启用则注入简化版路由提示
        group_profile_enabled=misc_cfg.group_profile_enabled,
        memory_root=_memory_root,
    )
    # 图片内联成 data: URL 再出站:上游对 http(s) URL 是纯透传,交出去就把可达性
    # 赌在 provider 出网与 URL 时效上,且部分 provider 形态会静默丢图。
    # 三模式改造:私聊维持无条件内联;群聊 passive 与 reactive 同走
    # media_config 三模式(窗口=recent;passive 无 addressed 概念,按 True 传——
    # 群聊 passive 只在显式触发时被调用,等价于已被 @)。
    media_cfg = load_media_config()
    # 媒体寻址:窗口行 + 当前行建 url→(kind,m:id,idx) 反向映射,内联收集照旧走 url,
    # 收集完按 url 贴号(规划书 §8)。passive 的 recent 排除了当前消息,故单独拼当前行;
    # B 方案:当前行直接吃触发条目(构造时已凭 event 领得 db 真 id),不再偷看 buffer
    # 尾部(接力场景尾部≠当前消息必错标)。无号合成条目也保留给统一呈递器；
    # 只有媒体寻址索引在下方过滤无号行，不再把整条身份与媒体清单一起丢掉。
    current_row = current_buffered
    render_rows = list(current_rows or ([current_row] if current_row is not None else []))
    addressable_current_rows = [r for r in render_rows if r.id is not None]
    label_index = build_media_address_index(list(recent) + addressable_current_rows)
    if is_private:
        inline_urls = list(image_urls)
    else:
        inline_urls = _collect_inline_image_urls(
            media_cfg,
            window_msgs=_inline_media_window(recent),
            current_image_urls=image_urls,
            current_user_id=user_id,
            addressed_to_bot=True,
            self_id=str(bot.self_id),
        )
    inlined = await inline_image_frames(inline_urls, cfg=media_cfg) if inline_urls else []
    # 动图抽帧:表情走专用通道,返回 (url, 帧组)(动图多帧/静态单帧),
    # 帧组结构自配对防张冠李戴(见 image_inline.inline_gif_frames docstring)。
    sticker_inlined = (
        await inline_gif_frames(sticker_urls or [], cfg=media_cfg)
        if media_cfg.sticker_vision_enabled
        else []
    )
    # 缺席补占位:请求内联但整组缺席的当前消息媒体,在其号上补「(图#k 加载失败)」(§8)。
    absent_note = _absent_media_note(
        label_index,
        requested=list(image_urls) + list(sticker_urls or []),
        succeeded=[u for u, _ in inlined] + [u for u, _ in sticker_inlined],
    )
    # 呈递正文只从 rows 派生；text 形参保留给日志/兼容调用，不再成为第二事实源。
    passive_text = "\n".join(row.content for row in render_rows if row.content) or " "
    if absent_note:
        passive_text = f"{passive_text} {absent_note}".strip()
    if len(render_rows) == 1:
        render_rows[0] = replace(render_rows[0], content=passive_text)
    elif absent_note and render_rows:
        render_rows[-1] = replace(
            render_rows[-1], content=f"{render_rows[-1].content} {absent_note}".strip(),
        )
    user_content = build_passive_user_content(
        recent_messages=recent,
        rows=render_rows,
        current_image_urls=_label_groups(label_index, inlined),
        current_sticker_urls=_label_groups(label_index, sticker_inlined),
        is_private=is_private,
        # 分级反馈:弹出本 scope 的插件提示便签贴进本轮(见即焚)
        plugin_note="\n".join(
            task_notes.pop_plugin_notes(_feedback_scope_key(adapter_name, is_private, user_id, group_id))
        ) or None,
        name_resolver=_name_resolver,
    )

    # 本部署补丁: 长任务旁白实时转发。旁白是本轮 turn 的附属
    # 输出,用 handler 现成的 bot/target 直发,不走 push_message(那是带窗口校验的
    # 独立发话通道);不 @ 人(进度广播不针对特定个体)。extract_response_media 剥掉
    # MEDIA:/md 图片标签——旁白里的本地路径原样发出会泄主机路径;媒体最终回复还会带。
    async def _interim_echo(segment: str) -> None:
        # 分级反馈:旁白段过 where=interim/all 的 note 规则(确认轮不跟旁白)
        _check_interim_feedback(
            _feedback_scope_key(adapter_name, is_private, user_id, group_id), segment
        )
        cleaned, _, _ = extract_response_media(segment)
        if cleaned:
            await send_text_with_media(
                bot=bot,
                target=target,
                text=cleaned,
                media_urls=[],
                adapter_name=adapter_name,
            )

    result = await _chat_with_overflow_recovery(
        "passive",
        reply_bot=bot,
        reply_target=target,
        text="",
        image_urls=[],
        session_key=session_key,
        user_id=user_id,
        group_id=group_id,
        adapter_name=adapter_name,
        is_private=is_private,
        mode="passive",
        expect_structured=use_private_decision,
        structured_tool_name="submit_decision" if use_private_decision else None,
        system_prompt=system_prompt,
        user_content_override=user_content,
        target=hermes_target,
        interim_callback=_interim_echo if misc_cfg.interim_echo_enabled else None,
        approval_callback=(
            _make_approval_notify(
                bot, adapter_name,
                f"私聊 {user_id}" if is_private else f"群 {group_id}",
                is_private=is_private, group_id=group_id,
            )
            if plugin_config.hermes_api_interface == "runs" else None
        ),
        overwritten_reply_callback=partial(
            deliver_recovered_replies,
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            group_id=group_id,
            is_private=is_private,
        ),
    )

    # 阶段2b:runs 路迟到 steer 回收(被动路径)——私聊/passive 直说按委托排队,
    # 与既有 passive pending 语义一致;turn 结束后的 refire 自然接力。
    # 台账核销注记(本部署补丁: 弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75/84):
    # 本点把该 run 的 pending 台账置 recovered(native 路终态 pending_steer 上浮=
    # 上游已还回这批 steer, 对账不得再补, 防双重投递)。run_id 不可得——ChatResult
    # 不携带 run_id, 且 run 终态时 run_tracker 行已由 finish 删除——故按文本子串
    # 核销(recover_by_text): pending_steer 是各条 steer_text 的拼接, 必含其为子串。
    if result is not None and result.pending_steer:
        from ..core.steer_reconcile import strip_steer_presentation
        _scope_key = make_inflight_key(adapter_name, is_private, user_id, group_id)
        _mcp.inflight.try_enter(
            _scope_key,
            BufferedMessage(
                ts=_now_ms(), adapter=adapter_name, group_id=group_id,
                user_id=user_id, nickname=(current_buffered.nickname if current_buffered is not None else user_id),
                content=strip_steer_presentation(result.pending_steer), origin=MessageOrigin.human_replay(),
                image_urls=[], sticker_urls=[],
                reply_to_ts=None, is_bot=False,
            ),
            is_direct=True,
            original_msg_id=None,
            now_ms=_now_ms(),
            commission=True,
        )
        logger.info(
            f"[HERMES passive] 回收 pending_steer 进委托队列 "
            f"(key={_scope_key} len={len(result.pending_steer)})"
        )
        # 排回队列成功后按文本子串核销 pending 台账(见上方注记)。核销失败只 WARN,
        # 不影响回收主流程——对账仍以转录 m:id 搜索为最终判据双保险。
        if _mcp.run_tracker is not None:
            try:
                _mcp.run_tracker.recover_by_text(result.pending_steer)
            except Exception:
                logger.warning("[HERMES steer对账] 回收核销失败(不影响回收)", exc_info=True)

    # 本部署补丁: 403 鉴权类错误硬静默——不发 fallback、
    # 不发 raw_text,不依赖 fallback 配置。403 是运维事件(key/额度),群里通知无意义,
    # 上游持续故障时每条触发一条通知就是刷屏。记 error 级日志让运维能看见。
    # 扩展: provider 暂不可用/重试耗尽引导文案同口径静默。
    if result.is_forbidden_error or result.is_outage_error:
        _silence_kind = (
            "403 鉴权类错误" if result.is_forbidden_error
            else "provider 暂不可用(重试耗尽)引导文案"
        )
        logger.error(
            f"[HERMES passive] {_silence_kind},已静默丢弃 (group={group_id} is_private={is_private})"
            "——检查接入点 key / provider 额度与可用性"
        )
        _observe_silence("passive", adapter_name, group_id, is_private)
        _settle(result, "cancelled", "forbidden" if result.is_forbidden_error else "outage")
        return result

    if getattr(result, "is_run_detached", False):
        # 与 reactive 同语义：回复仍归原 run 的恢复链，当前轮保持静默。
        logger.info(
            f"[HERMES passive] runs 订阅断开,等待原 run 恢复 "
            f"(group={group_id} is_private={is_private})"
        )
        return result

    # 阶段3b:turn 被主动终止,硬静默(同 reactive 分支注释)。
    if result.is_interrupted:
        logger.info(f"[HERMES passive] turn 被终止,硬静默 (group={group_id} is_private={is_private})")
        _settle(result, "cancelled", "interrupted")
        return result

    # 上游 transport_error 同款保护(见 _run_reactive_turn 同名分支注释)。
    # passive 路径下私聊总是显式对话,群聊已通过触发判断进得来,两边都该有可见反馈;
    # 配空 fallback_text 时静默,保留逃生口。
    if result.is_transport_error:
        fallback_text = misc_cfg.transport_error_fallback_text
        logger.warning(
            f"[HERMES passive] transport error fallback "
            f"(group={group_id}, is_private={is_private}, "
            f"fallback={'silent' if not fallback_text else 'friendly_text'}); "
            f"upstream raw_text suppressed (len={len(result.raw_text or '')})"
        )
        if fallback_text:
            await send_text_with_media(
                bot=bot,
                target=target,
                text=fallback_text,
                media_urls=[],
                adapter_name=adapter_name,
            )
        return result

    # 本部署补丁: 私聊决策信封解析成功时按信封走——
    # should_reply=false 即静默(私聊不再硬必回)。解析失败回 raw_text:私聊是显式
    # 对话,静默更像故障;并保留既有防御性抠取(模型可能吐了破信封)。
    # 分级反馈:background_task 便签登记(静默轮也登记)。
    _fb_key = _feedback_scope_key(adapter_name, is_private, user_id, group_id)
    if use_private_decision and result.structured is not None and not result.parse_failed:
        _record_background_note(_fb_key, result.structured, log_tag="passive")
        if not result.structured.get("should_reply"):
            logger.info(f"[HERMES passive] 私聊自决静默 (user={user_id})")
            _settle(result, "silent", "should_reply_false")
            return result
        reply_text = str(result.structured.get("reply_text") or "").strip()
    else:
        # 防御:同一 Hermes session 之前跑过 reactive 时学到 submit_decision 契约,
        # 切回 passive 后仍可能吐 JSON。检测并抠 reply_text;不命中则用原 raw_text。
        # 闭环提醒:决策轮信封缺失(模型写了散文忘包 JSON,
        # 区别于 transport 错误)→ 留便签下轮提醒,不靠人介入纠偏。
        if use_private_decision and result.parse_failed and not result.is_transport_error:
            task_notes.push_plugin_note(
                _fb_key, "envelope_missing",
                "系统提示:你上一轮的最终回复没有包 submit_decision JSON 信封(走了散文兜底)。"
                "最终答复必须输出单个 JSON 对象,正文放进 reply_text 字段;过程想法走旁白即可。",
                cooldown_sec=3600.0,
            )
        reply_text = result.raw_text
        extracted = maybe_extract_decision_reply_text(reply_text)
        if extracted is not None:
            if extracted == "":
                logger.info(f"[HERMES passive] LLM 返回 should_reply=false 结构,静默(group={group_id})")
                _settle(result, "silent", "should_reply_false")
                return result
            logger.warning(f"[HERMES passive] 检测到 submit_decision 形 JSON 残留,抠 reply_text 后发送(group={group_id})")
            reply_text = extracted

    # 类②拦截点(passive,规划书 §3.3):reply_text 定稿后、extract_response_media 前。
    # 命中 action=bounce 规则 → 不投递,立刻追加重交轮;缺省(无 bounce 规则)恒放行。
    # 这里不套合成轮身份豁免:重交轮自己再吐残片正是「第二次检出放行」的那一发。
    if await _maybe_intercept_bounce(
        key=_fb_key, reply_text=reply_text, bot=bot, target=target,
        adapter_name=adapter_name, user_id=user_id, group_id=group_id,
        is_private=is_private,
    ):
        _settle(result, "suppressed", "bounce")
        return result

    cleaned_text, extracted_media_urls, extracted_files = extract_response_media(reply_text)
    media_urls = list(result.media_urls) + [u for u in extracted_media_urls if u not in result.media_urls]
    # 本部署补丁:本地文件上传。client 层(普通文本路径)已收过一轮
    # file_paths; handler 层对 reply_text 的二次 extract 覆盖 JSON 残留场景。
    file_paths = list(result.file_paths) + [p for p in extracted_files if p not in result.file_paths]

    if not cleaned_text and not media_urls and not file_paths:
        _settle(result, "silent", "empty_content")
        return result
    reply_to = (
        (result.structured or {}).get("reply_to")
        if use_private_decision and result.structured is not None and not result.parse_failed
        else None
    )
    resolved_reply = resolve_reply(
        _mcp.message_buffer,
        adapter=adapter_name,
        group_id=group_id,
        owner_user_id=user_id if group_id is None else None,
        reply_to=reply_to,
        log_tag="HERMES passive",
    )
    reply_qq_message_id = resolved_reply.qq_message_id if resolved_reply else None
    delivery = _prepare_delivery(
        result, cleaned_text=cleaned_text, media_urls=media_urls, file_paths=file_paths,
        reply_to=reply_to, reply_qq_message_id=reply_qq_message_id,
        reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
    )
    sent = await send_text_with_media(
        bot=bot,
        target=target,
        text=cleaned_text,
        media_urls=media_urls,
        reply_qq_message_id=reply_qq_message_id,
        reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
        adapter_name=adapter_name,
        **({"delivery": delivery} if delivery is not None else {}),
    )
    if is_private and sent and _mcp.message_buffer is not None:
        # 私聊 bot 行保留真实发送者，同时单独记录对端归属；不能用 bot 的
        # user_id 当私聊桶键，也不能靠 is_bot 对所有私聊全局放行。
        buffered_content = send_result_history(sent, cleaned_text)
        if media_urls:
            buffered_content = f"{buffered_content} [图片]".strip()
        _mcp.message_buffer.append(
            BufferedMessage(
                ts=_now_ms(),
                adapter=adapter_name,
                group_id=None,
                user_id=str(bot.self_id),
                nickname=await get_bot_nickname(bot),
                content=buffered_content,
                origin=MessageOrigin.bot_output(),
                image_urls=[],
                is_bot=True,
                qq_message_id=getattr(sent, "primary_qq_message_id", None),
                private_owner_id=user_id,
            )
        )
    # 分级反馈:发送成功后的文书暗示/离场确认检查(合成轮豁免)
    await _post_send_feedback(
        key=_fb_key, reply_text=cleaned_text, bot=bot, target=target,
        adapter_name=adapter_name, user_id=user_id, group_id=group_id,
        is_private=is_private,
        suppress_recursion=(current_buffered is not None and current_buffered.origin.suppresses_feedback),
        not_final=bool((result.structured or {}).get("not_final")),
    )
    # 文件跟在文本消息之后上传(占位符在前作说明, 文件随后到达)。
    await send_local_files(bot=bot, target=target, file_paths=file_paths, adapter_name=adapter_name, **({"delivery": delivery} if delivery is not None else {}))
    return result

async def _run_reactive_turn(
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
    trigger_msg_id: str | int | None = None,
    ephemeral: bool = False,
    recent_override: list[BufferedMessage] | None = None,
    context_limit: int | None = None,
    replied_msg_id=None,
    current_buffered: BufferedMessage | None = None,
    # 拆批 fragments 序列(alive/103 §14.2):有序完整碎片;None=单行旧路径。
    current_rows=None,
):
    """跑一发 reactive turn,返回 hermes_client.chat() 的 ChatResult,或 None 表示提前 return。

    外壳 _handle_reactive_path 负责 inflight + 图片门控,这里只管:
    拉 recent → 组 prompt → 调 chat → 解析 decision → 发出向 → 回写 buffer。

    ephemeral=True(本部署新增,隐式激活用):无活跃窗口也跑,现场造一个不注册进
    管理器的临时 session;发言即开窗(见发送成功段),沉默不开窗。
    recent_override: 定时巡查(T2)传入的 T4 窗口消息(新→旧),替代默认拉取。
    context_limit: 覆盖 history_message_limit 的拉取条数(keyword-soft 的 N1),
    设置时不再做续发轮裁剪。
    """
    assert _mcp.message_buffer is not None and _mcp.active_sessions is not None
    chat_cfg = load_chat_config()
    misc_cfg = load_misc_config()

    # 用 get_if_trigger_alive 而非 get():get() 是 debug-only 裸访问,可能返回已过期 session;
    # get_if_trigger_alive 与 is_alive_for_trigger(handle_message 入口处用过)同口径。
    session = _mcp.active_sessions.get_if_trigger_alive(adapter_name, group_id, now_ms)
    if session is None:
        if not ephemeral:
            # turn 生命期内的 TTL 过期已由 begin_turn/end_turn 租约挡住。剩下两条来路:
            # 上一发 turn 返回了 should_exit_active(唯一会 end() 窗口的地方),或者窗口
            # 恰好在「turn 结束 → refire 接力」的交接瞬间过期且被 cron sweep 扫掉。
            # 两种都意味着有人的触发被吞了 —— 必须留痕,不能静默 return。
            logger.warning(
                f"[HERMES reactive] drop: no active window at turn start "
                f"(group={group_id} user={user_id} direct={is_direct} now_ms={now_ms})"
            )
            return None
        # 隐式激活临时 turn:造一个不注册进管理器的 session 实例。下游读操作
        # (triggered_by / topic_hint / last_bot_reply_at)天然兼容;写操作
        # (touch / mark_bot_replied / end_if_current_generation)按 key 查管理器,全是安全 no-op。
        session = ActiveSession(
            adapter=adapter_name,
            group_id=group_id,
            triggered_by=user_id,
            started_at=now_ms,
            last_active_at=now_ms,
            expires_at=now_ms,  # 已过期:仅作数据载体,不代表有窗口
        )
        logger.debug(f"[HERMES reactive] ephemeral turn start (group={group_id} user={user_id})")

    # 世代号快照:turn 收尾 should_exit_active 收窗时做双重比对(实例同一性+世代号)。
    # turn 期间窗口被别人的显式触发续期过 → 世代号 +1,本 turn 的收窗请求自然失效。
    # ephemeral 临时实例未注册进管理器,快照 0 即可——收窗对它本就 no-op。
    turn_generation = session.generation

    # B.3: 快照本 turn 入口时的 last_bot_reply_at, 供 chat() 返回后判定 agent loop
    # 期间是否有外部(MCP push_message)推过 bot 自己的回复。 若发生, 即使 LLM 返
    # should_reply=True 也必须抑制本路 send, 否则同 turn 内双答。
    last_bot_reply_at_at_entry = session.last_bot_reply_at

    if recent_override is not None:
        # 定时巡查(T2):T4 窗口消息由调用方过滤好,新→旧顺序与 get_recent 一致。
        recent_full = recent_override
    else:
        recent_full = _mcp.message_buffer.get_recent(
            adapter=adapter_name,
            group_id=group_id,
            limit=context_limit or chat_cfg.history_message_limit,
        )
    # 本部署补丁(增量推送): 只推「没推过」的新消息,
    # 旧历史由 Hermes 服务端转录承担——此前每轮全量重推历史块,块随 user 消息落进
    # 转录、后续每轮重发,复利占输入账单 ~10%(生产实证)。拉取上限即推送上限:
    # 水位过老导致新消息超上限时,更老的永久跳过(与旧窗口外不可见语义一致)。
    # 旧续发轮 OR 双闸/select_followup_window 随之废弃(定:配置削减)。
    watermark = session_manager.get_push_watermark(adapter_name, user_id, group_id)
    recent, new_watermark = split_unpushed_messages(recent_full, watermark)
    # 拆批/呈现去重(alive/103 §14.2/§14.3):current 序列覆盖的行与已 steer
    # 呈现的行不再随 recent 重复出现;寻址索引仍用 recent_full 全集不受影响。
    recent = _filter_current_and_steered(
        recent,
        current_rows=current_rows,
        scope=make_inflight_key(adapter_name, False, user_id, group_id),
    )

    # 平台账号名 + self_id 一起进 prompt:模型判归属只能靠这两样把消息里回填的
    # 裸 `@<id>` 和 [bot] 历史行绑回自己。取值走进程内缓存,首轮一次 RPC。
    self_nickname = await get_bot_nickname(bot)

    # 本部署补丁:计算记忆根路径,用于路由提示动态跟随配置
    _custom_dir = resolve_custom_dir()
    _memory_root = str(_custom_dir / "memory" / adapter_name) if _custom_dir else None
    # 身份表：发言者标签正主名注释，custom_dir 不可用/查不到都静默 None
    _name_resolver = (
        (lambda uid, _cd=_custom_dir, _ad=adapter_name: identity.resolve_display_name(_cd, _ad, uid))
        if _custom_dir else None
    )
    system_prompt = build_reactive_system_prompt(
        # 群记忆外挂:群档案注入块(私聊/无档案返回 None,函数内静默跳过)
        profile_block=build_group_profile_block(adapter_name, group_id),
        # 本部署补丁:传入功能开关,私聊时启用则注入简化版路由提示
        group_profile_enabled=misc_cfg.group_profile_enabled,
        memory_root=_memory_root,
        # 本部署补丁:会话上下文挪入系统提示,吃缓存
        adapter=adapter_name,
        group_id=group_id,
        self_id=str(bot.self_id),
        self_nickname=self_nickname,
    )
    # 本部署补丁: 黑名单标记进 runtime_state。
    # 在 turn 内按 (group_id, user_id) 重算而非从 handler 透传——refire/pending
    # 接力链不用为此加字段。
    sender_blacklisted = _is_blacklisted(group_id, user_id)
    # 本部署补丁: 取在钉委托锚(懒式超龄剔除)。
    # 改手动单轨后,入口不再自动钉锚;这里只会读到「之前判断轮已确认
    # 并 pin 成功」的锚。ephemeral 临时 session 不注册进管理器,天然无锚。
    pinned_anchors: list[AnchorRecord] = []
    if not ephemeral:
        pinned_anchors = [
            a
            for a in _mcp.active_sessions.get_anchors(
                adapter_name,
                group_id,
                now_ms,
                max_age_ms=chat_cfg.anchor_max_age_sec * 1000,
            )
            if not (a.ts == now_ms and a.commissioner_id == user_id)
        ]
    # 见 _run_passive_turn 同名调用注释:出站一律内联,不发平台 URL。
    # 本部署补丁(图片投送管线三模式): 取代 9-29 addressed
    # 单点门控,三模式(full/semi/off)+例外名单由 media_config.yaml 现读现用。
    # 判定逻辑收口在 _collect_inline_image_urls;内联走帧组(静图单帧,动图抽帧)。
    media_cfg = load_media_config()
    # 媒体寻址:窗口行 + 当前行建 url→(kind,m:id,idx) 反向映射(规划书 §8 渲染时反查)。
    # B 方案:当前行直接吃触发条目(构造时已凭 event 领得 db 真 id),不再取
    # recent_full[0]——接力场景尾部≠当前消息;旧 user_id 同源护栏只挡跨用户,
    # 同用户连发接力照样错标,随桥一起拆。条目拼在尾部:与 recent_full 内同一
    # db 行撞 url 时后者覆盖前者(id 相同,无害)。无号合成条目仍交给统一呈递器，
    # 媒体索引仅收可寻址行，不为系统消息伪造编号。
    current_row = current_buffered
    render_rows = list(current_rows or ([current_row] if current_row is not None else []))
    addressable_current_rows = [r for r in render_rows if r.id is not None]
    label_index = build_media_address_index(
        list(recent_full) + addressable_current_rows
    )
    sticker_inlined = (
        await inline_gif_frames(sticker_urls or [], cfg=media_cfg)
        if media_cfg.sticker_vision_enabled
        else []
    )
    inline_urls = _collect_inline_image_urls(
        media_cfg,
        # full 用增量窗口(recent=未推送过的新消息,其图从未进过视野);
        # semi 用全量窗口(recent_full,判定前后文需要看到已推送的紧邻消息)
        window_msgs=_inline_media_window(recent if media_cfg.mode == "full" else recent_full),
        current_image_urls=image_urls,
        current_user_id=user_id,
        addressed_to_bot=addressed_to_bot,
        self_id=str(bot.self_id),
    )
    inlined_frames = await inline_image_frames(inline_urls, cfg=media_cfg) if inline_urls else []
    # 缺席补占位:请求内联但整组缺席的当前消息媒体,在其号上补「(图#k 加载失败)」(§8);
    # 反查不到号(未入库/例外)的缺席仍走下面的粗占位兜底。
    absent_note = _absent_media_note(
        label_index,
        requested=list(image_urls) + list(sticker_urls or []),
        succeeded=[u for u, _ in inlined_frames] + [u for u, _ in sticker_inlined],
    )
    # 呈递正文只从 rows 派生；text 形参不再覆盖行内容。媒体补白也只是
    # 对同一行做 replace，单元素与多元素不会走两套正文来源。
    current_text = "\n".join(row.content for row in render_rows if row.content)
    if absent_note:
        current_text = (current_text + " " + absent_note).strip()
    elif image_urls and not inlined_frames and "[图片]" not in current_text:
        current_text = (current_text + " [图片]").strip()
    current_text = current_text or "[图片]"
    if len(render_rows) == 1:
        render_rows[0] = replace(render_rows[0], content=current_text)
    elif absent_note and render_rows:
        render_rows[-1] = replace(
            render_rows[-1], content=f"{render_rows[-1].content} {absent_note}".strip(),
        )
    elif image_urls and not inlined_frames and render_rows and "[图片]" not in render_rows[-1].content:
        render_rows[-1] = replace(
            render_rows[-1], content=f"{render_rows[-1].content} [图片]".strip(),
        )
    user_content = build_reactive_user_content(
        addressed_to_bot=addressed_to_bot,
        triggered_by=session.triggered_by,
        triggered_by_nickname=None,
        topic_hint=session.topic_hint,
        recent_messages=recent,
        rows=render_rows,
        current_origin=current_buffered.origin if current_buffered is not None else None,
        current_image_urls=_label_groups(label_index, inlined_frames),
        current_sticker_urls=_label_groups(label_index, sticker_inlined),
        sender_blacklisted=sender_blacklisted,
        pinned_anchors=pinned_anchors,
        # 分级反馈:弹出本 scope 的插件提示便签贴进本轮(见即焚)
        plugin_note="\n".join(
            task_notes.pop_plugin_notes(_feedback_scope_key(adapter_name, False, user_id, group_id))
        ) or None,
        now_ms=now_ms,
        name_resolver=_name_resolver,
    )

    # 出站 prompt 体量:模型在 explicit @ 下静默时,第一件要排除的就是「上下文被
    # 某条历史撑爆」。只记尺寸不记内容(历史可能几十万字符,也含用户隐私)。
    # oversized_history 记的是 sanitize **之前** 的原始长度 —— 渲染端已经会截断,
    # 但一条超长旧行说明 DB 里躺着脏数据,值得单独告警。
    prompt_chars = len(user_content) if isinstance(user_content, str) else sum(len(str(p)) for p in user_content)
    oversized = [(m.id, len(m.content)) for m in recent_full if len(m.content) > 2000]
    logger.debug(
        f"[HERMES reactive] prompt built group={group_id} user_content_chars={prompt_chars} "
        f"window={len(recent)}/{len(recent_full)} direct={is_direct} "
        f"mode={media_cfg.mode} imgs={len(inlined_frames)}/{len(image_urls)}"
    )
    if oversized:
        logger.warning(
            f"[HERMES reactive] <recent_messages> 含超长历史行(已按渲染上限截断,但 DB 里是脏数据): "
            f"group={group_id} rows={oversized}"
        )

    session_key = session_manager.get_session_key(
        adapter_name=adapter_name,
        is_private=False,
        user_id=user_id,
        group_id=group_id,
    )
    hermes_target = resolve_target(adapter_name, False, group_id)
    # 注:user_content_override 已携带 user message 的全部内容(text + 多模态);
    # text/image_urls 在 chat() 中会被忽略(见 hermes_client.chat 文档),此处显式传 ""
    # /[] 让契约清晰,避免被读者误以为 image_urls 也参与了构造。
    # 本部署补丁: 旁白实时转发,同 passive 路径注释。
    async def _interim_echo(segment: str) -> None:
        # 分级反馈:旁白段过 where=interim/all 的 note 规则(确认轮不跟旁白)
        _check_interim_feedback(
            _feedback_scope_key(adapter_name, False, user_id, group_id), segment
        )
        cleaned, _, _ = extract_response_media(segment)
        if cleaned:
            await send_text_with_media(
                bot=bot,
                target=target,
                text=cleaned,
                media_urls=[],
                adapter_name=adapter_name,
            )

    result = await _chat_with_overflow_recovery(
        "reactive",
        reply_bot=bot,
        reply_target=target,
        text="",
        image_urls=[],
        session_key=session_key,
        user_id=user_id,
        group_id=group_id,
        adapter_name=adapter_name,
        is_private=False,
        mode="reactive",
        expect_structured=True,
        structured_tool_name="submit_decision",
        system_prompt=system_prompt,
        user_content_override=user_content,
        target=hermes_target,
        interim_callback=_interim_echo if misc_cfg.interim_echo_enabled else None,
        approval_callback=(
            _make_approval_notify(bot, adapter_name, f"群 {group_id}", is_private=False, group_id=group_id)
            if plugin_config.hermes_api_interface == "runs" else None
        ),
        overwritten_reply_callback=partial(
            deliver_recovered_replies,
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            group_id=group_id,
            is_private=False,
        ),
    )

    # 阶段2b:runs 路 turn 收尾时,上游漏接的迟到 steer 随 completed 上浮——排回
    # pending refire,不能丢(那是用户在 turn 边界上说的话)。add_followup 合并
    # 语义,不顶掉 turn 期间新来的真实跟帖。
    # 台账核销注记(本部署补丁: 弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75/84):
    # 本点把该 run 的 pending 台账置 recovered(native 路终态 pending_steer 上浮=
    # 上游已还回这批 steer, 对账不得再补, 防双重投递)。run_id 不可得——ChatResult
    # 不携带 run_id, 且 run 终态时 run_tracker 行已由 finish 删除——故按文本子串
    # 核销(recover_by_text): pending_steer 是各条 steer_text 的拼接, 必含其为子串。
    if result is not None and result.pending_steer:
        from ..core.steer_reconcile import strip_steer_presentation
        _mcp.inflight.add_followup(
            make_inflight_key(adapter_name, False, user_id, group_id),
            PendingEntry(
                msg=BufferedMessage(
                    ts=_now_ms(), adapter=adapter_name, group_id=group_id,
                    user_id=user_id, nickname=nickname or user_id,
                    content=strip_steer_presentation(result.pending_steer), origin=MessageOrigin.human_replay(),
                    image_urls=[], sticker_urls=[],
                    reply_to_ts=None, is_bot=False,
                ),
                is_direct=False,
                # 迟到 steer 的出生轮语义:随条目走(接力改读
                # 条目自己的值后,这里同步盖出生轮的 ephemeral——冷窗关键词轮里上浮的
                # steer 仍按无窗跑,不因接力方有窗而切换跑法,避免交接到无窗轮被吞)。
                ephemeral=ephemeral,
            ),
        )
        logger.info(
            f"[HERMES reactive] 回收 pending_steer 进 pending "
            f"(group={group_id} len={len(result.pending_steer)})"
        )
        # 排回队列成功后按文本子串核销 pending 台账(见上方注记)。核销失败只 WARN,
        # 不影响回收主流程——对账仍以转录 m:id 搜索为最终判据双保险。
        if _mcp.run_tracker is not None:
            try:
                _mcp.run_tracker.recover_by_text(result.pending_steer)
            except Exception:
                logger.warning("[HERMES steer对账] 回收核销失败(不影响回收)", exc_info=True)

    # 结果对象可能来自尚未带“本地能力拒绝”标记的后端；缺省 False 保持该后端的
    # 原有水位与冷却行为，不能让一个可选标记在发送前炸掉整个 reactive turn。
    is_local_refusal = getattr(result, "is_local_refusal", False)

    # 增量推送水位推进:仅在内容确定落进上游转录时推进(非 transport 错误、
    # 非 403/暂不可用等 provider 报错家族);失败/本 turn 内发生会话轮换则不推进,
    # 下轮从旧水位(或全量)重推——宁重复,不丢失。
    if (
        new_watermark is not None
        and not result.is_transport_error
        and not is_local_refusal
        and not result.is_forbidden_error
        and not result.is_outage_error
    ):
        session_manager.advance_push_watermark(
            adapter_name, user_id, group_id, new_watermark, expected_key=session_key
        )

    if getattr(result, "is_run_detached", False):
        # 原 run 已登记且仍在恢复链中；当前轮不发错误兜底或破信封提醒。
        # 上方水位可推进：网关已接纳本轮内容，slot 释放后由 retry 续订原 run。
        logger.info(f"[HERMES reactive] runs 订阅断开,等待原 run 恢复 (group={group_id})")
        return result

    if result.parse_failed or result.structured is None:
        # 本部署补丁: 403 鉴权类错误硬静默(同 passive 路径注释)。
        # 扩展: provider 暂不可用/重试耗尽引导文案同口径静默。
        if result.is_forbidden_error or result.is_outage_error:
            _silence_kind = (
                "403 鉴权类错误" if result.is_forbidden_error
                else "provider 暂不可用(重试耗尽)引导文案"
            )
            logger.error(
                f"[HERMES reactive] {_silence_kind},已静默丢弃 (group={group_id})"
                "——检查接入点 key / provider 额度与可用性"
            )
            _observe_silence("reactive", adapter_name, group_id, False)
            _settle(result, "cancelled", "forbidden" if result.is_forbidden_error else "outage")
            return result
        # 阶段3b:turn 被主动终止(/run-stop 或 gateway 关停)——硬静默。
        # 终止是管理员的主动动作,命令回执已经说明,不再发「被中断」兜底文本。
        if result.is_interrupted:
            logger.info(f"[HERMES reactive] turn 被终止,硬静默 (group={group_id})")
            _settle(result, "cancelled", "interrupted")
            return result
        # 闭环提醒:决策轮信封缺失(模型写散文忘包 JSON;
        # 403/outage/interrupted/transport 均已提前分流,到这里是模型自己的形状问题)
        # → 留便签下轮提醒,不靠人介入纠偏。
        if result.parse_failed and not result.is_transport_error:
            task_notes.push_plugin_note(
                _feedback_scope_key(adapter_name, False, user_id, group_id),
                "envelope_missing",
                "系统提示:你上一轮的最终回复没有包 submit_decision JSON 信封(走了散文兜底)。"
                "最终答复必须输出单个 JSON 对象,正文放进 reply_text 字段;过程想法走旁白即可。",
                cooldown_sec=3600.0,
            )
        # 上游 transport_error(5xx / 网络断 / 流被掐):raw_text 是服务端错误信息原文
        # (如 "Model generated invalid tool call: ..."),原文转发会把内部错误丢到群里
        # 既泄密又难看。换成 config 里的友好兜底文本;空串则静默。
        # parse_failed 但非 transport(LLM 真的回了点啥但结构错):原样转发 raw_text,
        # 仍可能是用户想要的回答。
        # 兜底挂点(保留留痕,后续可能移除):transport 错误兜底——
        # 仅直说触发发 fallback 文本 / 贴 429 emoji,隐式静默防刷屏。
        if result.is_transport_error and is_direct:
            # 阶段2d(「丢了就贴」):429 满额丢单(自动重试一次仍 429)不发
            # 文本,在触发消息上贴 emoji 表「忙,稍等」;无 msg id 时退回文本兜底。
            if result.is_rate_limited and trigger_msg_id is not None:
                logger.info(
                    f"[HERMES reactive] 429 满额丢单,贴表情 (group={group_id} msg={trigger_msg_id})"
                )
                await _emoji_like_quiet(bot, trigger_msg_id, chat_cfg.rate_limit_emoji_id)
                return result
            fallback_text = misc_cfg.transport_error_fallback_text
            logger.warning(
                f"[HERMES reactive] transport error fallback "
                f"(group={group_id}, fallback={'silent' if not fallback_text else 'friendly_text'}); "
                f"upstream raw_text suppressed (len={len(result.raw_text or '')})"
            )
            if fallback_text:
                await send_text_with_media(
                    bot=bot,
                    target=target,
                    text=fallback_text,
                    media_urls=[],
                    # transport 错误没有可靠决策信封，不额外生成 @ 或 Reply。
                    adapter_name=adapter_name,
                )
            return result

        # raw_text 预览:用来分类失败模式(裸文本 / fenced ```json / 多 JSON 块 /
        # nested object 等)。换行折叠成 \n 字面量,300 字够覆盖单 turn submit_decision。
        raw = result.raw_text or ""
        # 纯散文兜底:模型完全没进 submit_decision 信封(连 "should_reply" 键都没出现),
        # 说明它抛开协议直接自然作答 —— 这段散文本身就是它想说的话,非显式 turn 也照发。
        # 关键区分:信封在场但破了(截断 / 语法脏)可能正带着「这条不归我」的
        # should_reply=false 决定,而那信号只以 JSON 形式存在,拿一段可能不该说的话去撞群
        # 比丢一条更糟,所以那种破法非显式 turn 仍静默。transport / persistence 错误的 raw
        # 是服务端报文,绝不当散文转发,故排除。
        # 信封在场的嗅探从「含 "should_reply" 字面值」收紧为
        # 「以 { 开头或存在 "should_reply": 键值形态」——旧口径下模型正文讨论协议
        # (提及字段名)会被误判破信封,非显式 turn 静默吞话。
        _looks_envelope = raw.strip().startswith("{") or bool(re.search(r'"should_reply"\s*:', raw))
        is_pure_prose = bool(raw) and not result.is_transport_error and not _looks_envelope
        # 兜底挂点(保留留痕,后续可能移除):破信封必达兜底——
        # 直说触发必发 raw_text;隐式仅在纯散文时发,其余静默(信封破可能带
        # should_reply=false,不敢代发)。
        forward_raw = bool(raw) and (is_direct or is_pure_prose)
        raw_preview = preview_raw(raw)
        logger.warning(
            f"[HERMES reactive] structured parse failed (group={group_id}, "
            f"transport_error={result.is_transport_error}); fallback="
            f"{('raw_text' if is_direct else 'prose') if forward_raw else 'silent'}; "
            f"raw_len={len(raw)} raw_preview={raw_preview!r}"
        )
        # 显式触发降级发 raw_text;非显式触发仅在纯散文时发,其余静默
        if forward_raw:
            fallback_text = raw
            extracted = maybe_extract_decision_reply_text(fallback_text)
            if extracted is not None:
                fallback_text = extracted
            cleaned_text, extracted_media_urls, extracted_files = extract_response_media(fallback_text)
            media_urls = list(result.media_urls) + [u for u in extracted_media_urls if u not in result.media_urls]
            fallback_files = list(result.file_paths) + [p for p in extracted_files if p not in result.file_paths]
            delivery = _prepare_delivery(
                result, cleaned_text=cleaned_text, media_urls=media_urls, file_paths=fallback_files
            )
            # 本部署补丁(问题5): 散文兜底恒不 @——
            # 信封破了拿不到决策,没有任何 @ 依据,顺带消掉「兜底答错」。
            await send_text_with_media(
                bot=bot,
                target=target,
                text=cleaned_text,
                media_urls=media_urls,
                adapter_name=adapter_name,
                **({"delivery": delivery} if delivery is not None else {}),
            )
            # 本部署补丁:散文兜底同样可能带本地文件引用。
            await send_local_files(
                bot=bot, target=target,
                file_paths=fallback_files,
                adapter_name=adapter_name,
                **({"delivery": delivery} if delivery is not None else {}),
            )
        else:
            # 破信封且现有语义为静默:销账,监控不得把它当丢失补发。
            _settle(result, "silent", "parse_failed")
        return result

    logger.info(
        f"[HERMES reactive] decision adapter={adapter_name} group={group_id} user={user_id} "
        f"direct={is_direct} should_reply={result.structured.get('should_reply')} "
        f"should_exit_active={result.structured.get('should_exit_active')} "
        f"reply_text_len={len(str(result.structured.get('reply_text') or ''))} "
        f"topic_hint={result.structured.get('topic_hint')!r} "
        f"salvaged={result.structured.get('_salvaged', False)}"
    )
    # 解析成功时也留一份 raw 预览:decision 摘要看不出模型到底吐了什么形状
    # (是完整 submit_decision,还是只有 {"should_reply": false} 这种退化输出),
    # 而「该回却静默」的排查恰恰全靠这个区分。DEBUG 级,正常运行不刷日志。
    raw_preview = preview_raw(result.raw_text or "")
    logger.debug(f"[HERMES reactive] decision raw (len={len(result.raw_text or '')}): {raw_preview!r}")

    decision = result.structured
    # 分级反馈:离场任务便签登记(静默轮也登记)
    _fb_key = _feedback_scope_key(adapter_name, False, user_id, group_id)
    _record_background_note(_fb_key, decision, log_tag="reactive")
    if decision.get("topic_hint"):
        _mcp.active_sessions.update_topic(adapter_name, group_id, str(decision["topic_hint"]))
    # release_anchor 处理移在 answer_anchor 之后(alive/103):同轮 answer+release
    # 必须先让合法 answer 标记生效再松锚,否则锚被提前 release 后 answer 校验
    # 落空,模型反收「非法值」便签。见 turn 尾部。
    if decision.get("should_exit_active") and not _mcp.active_sessions.end_if_current_generation(session, turn_generation):
        # 收窗只对本 turn 入口快照的那个世代生效:期间若有别人 @bot 续期过窗口,
        # 世代号已 +1,旧世代的收窗请求不再匹配,新窗口留在字典里等它的 refire。
        logger.info(
            f"[HERMES reactive] should_exit_active not applied: window was re-triggered "
            f"during this turn (group={group_id})"
        )

    # 同一事实两处要用:静默分支判「是不是 push 已答」、send 分支做同 turn 去重。
    # chat() 返回后到这里没有 await,快照一次即可。
    mid_turn_push = session.last_bot_reply_at > last_bot_reply_at_at_entry

    if not decision.get("should_reply"):
        # 显式触发 + LLM 选择沉默是「看起来该回但没回」最常见的来源,
        # 静默是设计功能不是异常:被点名(addressed_to_you: true
        # 已进 prompt)却静默只是模型否掉了插件给的既定事实,不构成需要排查的形状,
        # 与「本 turn 已由 MCP push 答过的防重复静默」同级,统一 INFO 留痕即可。
        emit = logger.info
        emit(
            f"[HERMES reactive] silent: LLM decided should_reply=false "
            f"(group={group_id} user={user_id} direct={is_direct} "
            f"addressed={addressed_to_bot} answered_mid_turn={mid_turn_push} "
            f"topic_hint={result.structured.get('topic_hint')!r})"
        )
        _settle(result, "silent", "should_reply_false")
        return result

    reply_text = str(decision.get("reply_text") or "").strip()
    cleaned_reply_text, extracted_media_urls, file_paths = extract_response_media(reply_text)
    # 结构化出口与 passive 同口径:模型本轮附件由 client 层下载进 result.media_urls,
    # reply_text 里通常只有一个占位或完全没有媒体引用——只从 reply_text 抽会把已经
    # 下载好的附件整批丢掉(纯图回复更会被下面的空闸门直接吞掉)。合并顺序与去重
    # 规则对齐 passive 路径。
    media_urls = list(result.media_urls) + [
        u for u in extracted_media_urls if u not in result.media_urls
    ]
    if not cleaned_reply_text and not media_urls and not file_paths:
        _settle(result, "silent", "empty_content")
        return result

    # B.3: 同 turn 内防重复闸门 — 若 chat() agent loop 期间 last_bot_reply_at
    # 已被推进(即 push_message 在中途答过一次), 抑制本路 submit_decision 的 send。
    # 该闸对显式触发同样生效:同 turn 双答属纯重复,与触发性质无关。
    # 注: mid_turn_push 直接读 `session.last_bot_reply_at` 而非重新查 active_sessions ──
    # mark_bot_replied 是对同一 dataclass 实例原地写, session 变量持有的就是那个实例。
    # 不查 active_sessions 也回避了 TTL 边界判定与 ended-and-retriggered 罕见竞态。
    if mid_turn_push and not _is_restatement(session.last_bot_reply_text, cleaned_reply_text):
        # 中途 push 过,但这条带的是新内容(典型:push 一句「在查」、decision 给真答案)。
        # 两条都发 —— 抑制它等于把答案吞掉,而 agent 会以为已经发出去了。
        logger.info(
            f"[HERMES reactive] mid-turn push detected but submit_decision adds new content; "
            f"sending both (group={group_id} push_len={len(session.last_bot_reply_text)} "
            f"reply_text_len={len(cleaned_reply_text)})"
        )
    elif mid_turn_push:
        # 复述。中途那发**没投出任何媒体**、而本发带着能投的图 → 不是纯重复,是
        # 「文本已答、图还没出去」。典型来路:agent 用 push_message 发文本,图给的是
        # Hermes 主机本地路径(投不出去),随后 submit_decision 里带着网关内联好的
        # data URL。整条抑制会把那张图彻底丢掉,所以只补媒体、不重复文本。
        if media_urls and session.last_bot_reply_media == 0:
            delivery = _prepare_delivery(
                result, cleaned_text="", media_urls=media_urls, file_paths=[],
                reply_to=decision.get("reply_to"),
            )
            media_sent = await send_text_with_media(
                bot=bot,
                target=target,
                text="",
                media_urls=media_urls,
                adapter_name=adapter_name,
                **({"delivery": delivery} if delivery is not None else {}),
            )
            logger.info(
                f"[HERMES reactive] mid-turn push had no media; delivered submit_decision media only "
                f"(group={group_id} media={len(media_urls)} ok={media_sent})"
            )
            if media_sent:
                _mcp.active_sessions.mark_bot_replied(
                    adapter_name, group_id, now_ms=_now_ms(), media_count=len(media_urls), text=""
                )
            return result
        logger.info(
            f"[HERMES reactive] suppress submit_decision reply: it restates the mid-turn push "
            f"(group={group_id} user={user_id} direct={is_direct} "
            f"reply_text_len={len(reply_text)} mid_turn_media={session.last_bot_reply_media})"
        )
        _settle(result, "suppressed", "external_delivery")
        return result

    # 正文内联 @ 保持原顺序；Reply 另从同一条 m:id 记录取平台 ID 和真实发送者，
    # 群聊按 QQ 原生段序在 Reply 后补被引用者 At，私聊不补。无映射仍降级普通正文。
    resolved_reply = resolve_reply(
        _mcp.message_buffer,
        adapter=adapter_name,
        group_id=group_id,
        owner_user_id=None,
        reply_to=decision.get("reply_to"),
        log_tag="HERMES reactive",
    )
    reply_qq_message_id = resolved_reply.qq_message_id if resolved_reply else None
    # 类②拦截点(reactive,规划书 §3.3):cleaned_reply_text 定稿后、send_text_with_media 前。
    # 命中 action=bounce 规则 → 不投递该回复,立刻追加重交轮(不 mark_bot_replied、不入库)。
    if await _maybe_intercept_bounce(
        key=_fb_key, reply_text=cleaned_reply_text, bot=bot, target=target,
        adapter_name=adapter_name, user_id=user_id, group_id=group_id,
        is_private=False,
    ):
        _settle(result, "suppressed", "bounce")
        return result

    delivery = _prepare_delivery(
        result, cleaned_text=cleaned_reply_text, media_urls=media_urls, file_paths=file_paths,
        reply_to=decision.get("reply_to"), reply_qq_message_id=reply_qq_message_id,
        reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
    )
    sent = await send_text_with_media(
        bot=bot,
        target=target,
        text=cleaned_reply_text,
        media_urls=media_urls,
        reply_qq_message_id=reply_qq_message_id,
        reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
        adapter_name=adapter_name,
        **({"delivery": delivery} if delivery is not None else {}),
    )
    primary_qq_message_id = getattr(sent, "primary_qq_message_id", None)
    # 本部署补丁:本地文件跟在文本消息之后上传。
    await send_local_files(bot=bot, target=target, file_paths=file_paths, adapter_name=adapter_name, **({"delivery": delivery} if delivery is not None else {}))
    logger.debug(
        f"[HERMES reactive] sent adapter={adapter_name} group={group_id} "
        f"ok={bool(sent)} text_len={len(cleaned_reply_text)} media={len(media_urls)} "
        f"reply_to={decision.get('reply_to')!r} reply_resolved={reply_qq_message_id is not None}"
    )
    # 分级反馈:发送成功后的文书暗示/离场确认检查(合成轮豁免)
    if sent:
        await _post_send_feedback(
            key=_fb_key, reply_text=cleaned_reply_text, bot=bot, target=target,
            adapter_name=adapter_name, user_id=user_id, group_id=group_id,
            is_private=False,
            suppress_recursion=(current_buffered is not None and current_buffered.origin.suppresses_feedback),
            not_final=bool(decision.get("not_final")),
        )

    # 用 send 完成时的 wall clock,不复用入参 now_ms。
    # 入参 now_ms 是 _handle_reactive_path 进函数那一刻抓的, chat() + send 可能耗时
    # 任意长(上游重试 / 上下文压缩 / 工具调用累加),复用入参会让 last_bot_reply_at
    # 远早于真实 send 时间,同 turn 防重与活跃窗续期都按错时刻判定。
    # 调一次 _now_ms() 拿到 reply_now_ms,三个写操作复用这一个快照,既校正 stale
    # 入参,又避免多次读时钟在三个字段间引入毫秒级偏差。
    if sent and _mcp.message_buffer is not None:
        reply_now_ms = _now_ms()
        # 发言即开窗(窗口生命周期定案):隐式激活的临时 turn 真说了话 → 开启活跃
        # 窗口,群内跟帖可被听见。should_exit_active 优先——LLM 明说了「这轮谈完了」,
        # 不得借发言把窗又顶开。开窗后 touch / mark_bot_replied 都有了落点。
        if ephemeral and not decision.get("should_exit_active"):
            _mcp.active_sessions.trigger(adapter_name, group_id, user_id, now_ms=reply_now_ms)
            logger.info(f"[HERMES reactive] ephemeral turn spoke; active window opened (group={group_id})")
        # 回写清洗后的文本 + [图片] 占位,绝不写 reply_text 原文:媒体在回复里是内联
        # data:image;base64 形态,单张图就是几十万字符,原文入库后会被逐 turn 渲染进
        # <recent_messages>,几轮把上下文顶满,模型开始引用错历史。入站侧本来就只存
        # [图片] 占位(见 handle_perception),出站对齐同一约定。
        buffered_content = send_result_history(sent, cleaned_reply_text)
        if media_urls:
            buffered_content = f"{buffered_content} [图片]".strip()
        _mcp.message_buffer.append(
            BufferedMessage(
                ts=reply_now_ms,
                adapter=adapter_name,
                group_id=group_id,
                user_id=str(bot.self_id),
                nickname=self_nickname,
                content=buffered_content,
                origin=MessageOrigin.bot_output(),
                image_urls=[],
                is_bot=True,
                qq_message_id=primary_qq_message_id,
            )
        )
        # 注:若 should_exit_active=True,session 已在上方 end(),touch / mark_bot_replied
        # 都是安全 no-op(两者文档统一:session 缺失则 no-op)。
        _mcp.active_sessions.touch(adapter_name, group_id, now_ms=reply_now_ms)
        # B.2: 记下「bot 刚回过」时间戳/文本/媒体数,供同 turn 防重与活跃窗状态使用。
        # 本地能力拒绝没有模型回复，不写回复时间戳；or 短路同时避免误报落空。
        marked = is_local_refusal or _mcp.active_sessions.mark_bot_replied(
            adapter_name,
            group_id,
            now_ms=reply_now_ms,
            media_count=len(media_urls) + send_result_sticker_count(sent),
            text=cleaned_reply_text,
        )
        if not marked and not decision.get("should_exit_active"):
            # 排除 should_exit_active:那是本 turn 自己刚 end() 掉窗口,mark 落空是预期的。
            # 剩下的只能是别人的 turn 在本 turn 期间 end() 掉了这个 key(跨用户场景),
            # 日志里 last_bot_reply_at 会一直是 0,同 turn 防重也失去依据 —— 没有这条就查不出为什么。
            logger.warning(
                f"[HERMES reactive] mark_bot_replied landed nowhere: active window gone "
                f"(group={group_id}); same-turn dedupe loses this reply"
            )
        # 队列化定案:委托锚改手动单轨——入口不再自动钉,只有本轮
        # submit_decision 显式 pin_anchor=true 且回复真发出去了才钉。放在 mark_bot_replied
        # 之后,避免新锚立刻被自己的发言标成 answered;should_exit_active 不钉。
        # 去掉 ephemeral 排除——ephemeral 在 handler 入口
        # 按「开窗前」的 in_active_window 算,冷窗下「@bot+关键词」的委托轮恒 ephemeral=True,
        # 但入口 trigger() 已开真窗口,旧门把这支永远挡在钉锚外且无日志。真正的闸门是
        # 下面的 get_if_trigger_alive:没窗(ephemeral 沉默/窗已死)自然钉不上,有窗就该能钉。
        # 任务锚重构(规划书§2.2):merge 双通道优先于新钉——
        # ①信封 anchor_merge=发起者 id(模型判同任务;校验在钉发起者集合,非法走
        #   便签反馈通道,下轮贴回模型自修正);
        # ②quote 自动归并:pin_anchor=true 且当前消息引用的正是某锚的首条委托消息。
        # 闲聊引用不归并(没报 pin/merge 就不评估)。refire 接力出的委托没有
        # replied_msg_id(队列不存引用 id),只能靠信封通道——已知边界(§5.1)。
        merge_starter: str | None = None
        anchor_merge_raw = decision.get("anchor_merge")
        if anchor_merge_raw and not decision.get("should_exit_active"):
            candidate = str(anchor_merge_raw)
            if candidate in _mcp.active_sessions.pinned_commissioners(adapter_name, group_id):
                merge_starter = candidate
            else:
                task_notes.push_plugin_note(
                    _feedback_scope_key(adapter_name, False, user_id, group_id),
                    "anchor-merge-invalid",
                    f"你上一轮填的 anchor_merge={candidate} 不在在钉锚发起者集合,已忽略;"
                    "要归并请填在钉任务的发起者 id",
                    cooldown_sec=0,
                )
                logger.info(
                    f"[HERMES reactive] anchor_merge invalid: {candidate!r} (group={group_id})"
                )
        if (
            merge_starter is None
            and decision.get("pin_anchor") is True
            and not decision.get("should_exit_active")
            and replied_msg_id is not None
        ):
            merge_starter = _mcp.active_sessions.find_anchor_starter_by_msg_id(
                adapter_name, group_id, replied_msg_id
            )
        if merge_starter is not None:
            merge_session = _mcp.active_sessions.get_if_trigger_alive(adapter_name, group_id, reply_now_ms)
            merged = merge_session is not None and _mcp.active_sessions.merge_into_anchor(
                merge_session, starter_id=merge_starter,
                user_id=user_id, nickname=nickname, now_ms=now_ms,
            )
            logger.info(
                f"[HERMES reactive] anchor merge {'merged' if merged else 'failed'} "
                f"(group={group_id} starter={merge_starter} user={user_id})"
            )
        elif decision.get("pin_anchor") is True and not decision.get("should_exit_active"):
            # 本部署补丁: known_bots 挡钉——bot 账号的委托轮不许起任务锚
            # (bot 派的活不是真人任务,双 bot 互 @ 下 bot 钉锚会占住双槽)。
            # 门控放调用点不放 pin_anchor 内部:manager 是纯状态机,「谁配钉」
            # 是 handler 的策略判断。merge 归并(加入别人的锚)不在此闸内。
            if user_lists.is_known_bot(user_id):
                logger.info(
                    f"[HERMES reactive] pin_anchor skipped: known_bot (group={group_id} user={user_id})"
                )
            elif current_buffered is not None and not current_buffered.origin.allows_anchor:
                # 来源闸：系统指令或混合聚合体没有单一真人委托身份，失败关闭。
                logger.info(
                    f"[HERMES reactive] pin_anchor skipped: origin not anchorable "
                    f"(group={group_id} user={user_id} source={current_buffered.origin.source.value})"
                )
            elif current_buffered is None or current_buffered.id is None:
                # 拿不到合法插件 m:id 同样失败关闭——锚必须绑定本轮真实、
                # 可寻址的来源消息,不允许拿 ambient trigger_msg_id 或平台 ID
                # 冒充模型可见编号。steer 新任务也因此钉不上:锚来源只认
                # 当前真实 trigger,steer 内容无从继承原 turn 身份。
                logger.info(
                    f"[HERMES reactive] pin_anchor skipped: no plugin m_id on source "
                    f"(group={group_id} user={user_id})"
                )
            else:
                current_session = _mcp.active_sessions.get_if_trigger_alive(adapter_name, group_id, reply_now_ms)
                if current_session is None:
                    logger.info(f"[HERMES reactive] pin_anchor skipped: no active window (group={group_id} user={user_id})")
                else:
                    pinned = _mcp.active_sessions.pin_anchor(
                        current_session,
                        AnchorRecord(
                            participants=[
                                Participant(user_id=user_id, nickname=nickname or user_id, joined_ts=now_ms)
                            ],
                            # 双字段分型:模型可见 m_id 取来源消息的插件编号,
                            # 平台 quote ID 只作内部归并匹配,不进模型可见文本。
                            m_id=current_buffered.id,
                            platform_message_id=(
                                str(trigger_msg_id) if trigger_msg_id is not None else None
                            ),
                            text=(text or "")[:200],
                            ts=now_ms,
                        ),
                        # master_ids 必须拆前缀——
                        # admin_users(user_lists.yaml)是「adapter:id」复合键,
                        # 而 commissioner_id 是裸 user_id,直接比恒不命中,该槽形同虚设。
                        # 复用 approvals 同款拆法:只取当前适配器名下的裸 id,跨平台不串。
                        master_ids=set(admin_user_ids(set(user_lists.load_user_lists().admin_users), adapter_name)),
                        max_age_ms=chat_cfg.anchor_max_age_sec * 1000,
                        now_ms=now_ms,
                    )
                    logger.info(
                        f"[HERMES reactive] pin_anchor {'pinned' if pinned else 'declined'} "
                        f"(group={group_id} user={user_id} msg_id={trigger_msg_id})"
                    )

        # answered 改模型自答自标——
        # 信封 answer_anchor=委托人 id|"all",回复真发出才标记(与 pin 同门槛,
        # 收窗轮不标记)。错填不静默丢:值校验在钉锚委托人集合,非法值走插件
        # 便签反馈通道,下一轮贴回模型自修正(解挂不差一轮聊天)。
        answer_anchor = decision.get("answer_anchor")
        if answer_anchor and not decision.get("should_exit_active"):
            who = "all" if answer_anchor is True else str(answer_anchor)
            valid_ids = _mcp.active_sessions.pinned_commissioners(adapter_name, group_id)
            if who != "all" and who not in valid_ids:
                task_notes.push_plugin_note(
                    _feedback_scope_key(adapter_name, False, user_id, group_id),
                    "answer-anchor-invalid",
                    f"你上一轮填的 answer_anchor={who} 不在当前在钉锚委托人集合"
                    f"({sorted(valid_ids) or '空'}),已忽略;要标应答请填在钉委托人的 id 或 all",
                    cooldown_sec=0,
                )
                logger.info(
                    f"[HERMES reactive] answer_anchor invalid: who={who!r} "
                    f"pinned={sorted(valid_ids)} (group={group_id})"
                )
            else:
                marked_anchors = _mcp.active_sessions.mark_anchor_answered(adapter_name, group_id, who)
                if marked_anchors:
                    logger.info(
                        f"[HERMES reactive] anchor answered: who={who} count={marked_anchors} (group={group_id})"
                    )

        # 本部署补丁: 模型可控松锚。值=委托人
        # user_id 或 "all"(bool true 宽容视同 all)。ephemeral 无注册窗口,
        # release 按 key 查管理器,自然 no-op。位置在 answer 之后(alive/103):
        # 同轮 answer+release 是「标记完结并松锚」的合法组合,不可颠倒。
        release_anchor = decision.get("release_anchor")
        if release_anchor:
            who = "all" if release_anchor is True else str(release_anchor)
            released = _mcp.active_sessions.release_anchor(adapter_name, group_id, who)
            if released:
                logger.info(
                    f"[HERMES reactive] anchor released: who={who} count={released} (group={group_id})"
                )

    return result


# 同层互调(turns↔relay)与 turns→feedback 的顶部 import 会构成
# feedback→relay→turns 加载环;统一置底,由入口层 message.py 首载本域、
# 全部 def 就位后再解析。
from .message_feedback import (
    _check_interim_feedback,
    _emoji_like_quiet,
    _feedback_scope_key,
    _make_approval_notify,
    _maybe_intercept_bounce,
    _post_send_feedback,
    _record_background_note,
)
from .message_relay import (
    _handle_passive_path,
    _handle_reactive_path,
    _maybe_steer_direct,
)