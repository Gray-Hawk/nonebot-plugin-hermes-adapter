"""
消息处理器

priority=1 perception:写 MessageBuffer + BotRegistry,非阻塞
priority=98 main:触发判断 → reactive 决策 → 出向
"""

from __future__ import annotations

import asyncio

import nonebot_plugin_alconna as alconna
from nonebot import logger, on_message
from nonebot.adapters import Bot, Event
from nonebot.matcher import Matcher
from nonebot.rule import Rule

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..core import user_lists
from ..core.chat_config import load_chat_config
from ..core.inflight import make_inflight_key
from ..utils import check_isolation, get_adapter_name

from .message_media import (
    _collect_at_placeholders,
    _collect_nontext_placeholders,
    _compose_quote_prefix,
    _extract_forward_full,
    _extract_image_urls,
    _extract_replied_media,
    _extract_sticker_urls,
)

from .message_ingress import (
    _extract_sender_nickname,
    _has_at_bot_in_original,
    _ignore_rule,
    _is_blacklisted,
    _is_bot_at,
    _msg_at_only_other_users,
    _now_ms,
    _warn_unassembled_throttled,
)

from .message_perception import _merge_fragment_payload, _peek_event_db_id
from ..core.message_buffer import BufferedMessage
from ..core.message_origin import MessageOrigin
from ..core.session import session_manager

from .message_turns import _dispatch_chat
from .message_relay import route_synthesized_input


receive_message = on_message(rule=Rule(_ignore_rule), priority=98, block=True)


# 本部署补丁: 应答防抖。
# 设计见 项目文档库「方案:应答防抖」。
# 要点:perception 在 priority=1 无条件写 buffer,turn 上下文从 buffer 拉,
# 所以延迟起跑的 turn 自动看到防抖期内到达的所有碎片,触发载荷只需管归属。
class _DebounceState:
    """per-target 防抖状态。首条消息的 handler 常驻(睡满防抖窗口后 dispatch),
    期内后续消息只更新本状态并立即返回——合并语义:同一个人的碎片「拼接」
    (文本换行拼、媒体追加),跨人碎片才「替换」载荷。
    群聊首轮固定窗,其他真人提前到期;私聊仍按原规则顺延。"""

    __slots__ = ("first_mono", "deadline_mono", "payload", "merged", "first_human_id")

    def __init__(self, first_mono: float, deadline_mono: float, payload: dict):
        self.first_mono = first_mono          # 首条消息到达时刻(monotonic),硬上限基准
        self.deadline_mono = deadline_mono    # 群聊只可提前;私聊期内可顺延
        self.payload = payload                # 最新触发的 dispatch 载荷(kwargs)
        self.merged = 0                       # 被合并掉的碎片条数(观测用)
        self.first_human_id: str | None = None  # 不随 bot 替换触发载荷而丢失真人归属


_debounce_states: dict[tuple[str, str], _DebounceState] = {}
_GROUP_FIRST_WINDOW_SEC = 6.0  # 群聊首轮固定窗；同人不重置，其他真人插队立即到期


def _keyword_soft_hit(keywords, text: str) -> bool:
    """keyword-soft 点火判定(判定与呈递分离)。

    只在「用户当下手打的本体快照」上匹配关键词。调用方必须传拼接信封前的
    本体文本——引用块/非文本占位/@回填/合并转发展开都照常进 msg_text 给
    LLM 看,但不参与点火(旧文本/转发日志里的关键词不再误触发)。
    """
    return any(kw and kw in text for kw in keywords)


def _build_current_fragment(
    *,
    now_ms: int, adapter_name: str, group_id: str | None, user_id: str,
    nickname: str, msg_text: str,
    own_image_urls: list[str], sticker_urls: list[str],
    quote_image_urls: list[str], forward_image_urls: list[str],
    delivery_generation: int | None, event_msg_id,
) -> BufferedMessage:
    """构造本事件的触发 fragment(P-08:生产唯一构造点)。

    媒体分型与 perception 入库同源:own／sticker／quote／forward 四个字段分开填,
    iter_message_media 枚举序(own→sticker→quote→forward)与 message_images 表
    的 idx 一致——此前 own＋引用图混塞进 image_urls、quote／forward 缺填,同一张
    引用图在感知入库里是 quote 项、在 steer 行渲染里却挤进 image 项,导致
    `m:id#idx` 错位。dispatch 的聚合 own＋quote image_urls 参数仍由调用方另算供
    内联帧用,不再冒充行内寻址事实。插件 m:id 用 peek 读 perception 挂号,不 claim。
    """
    fragment = BufferedMessage(
        ts=now_ms, adapter=adapter_name, group_id=group_id, user_id=user_id,
        nickname=nickname, content=msg_text, origin=MessageOrigin.human(),
        image_urls=list(own_image_urls), sticker_urls=list(sticker_urls),
        quote_image_urls=list(quote_image_urls),
        forward_image_urls=list(forward_image_urls),
        reply_to_ts=None, is_bot=False,
        delivery_generation=delivery_generation,
    )
    fragment.id = _peek_event_db_id(adapter_name, event_msg_id)
    return fragment


def _is_reply_delayed(user_id: str) -> bool:
    """强制回复延迟名单判定:仅已知 bot(user_lists.yaml 的 known_bots)延迟。

    旧 .env 额外名单(hermes_reply_delay_users)已删除——延迟只保留给已知 bot。
    """
    return user_lists.is_known_bot(user_id)


def _debounce_arrive(
    key: tuple[str, str],
    payload: dict,
    *,
    debounce_sec: float,
    max_wait_sec: float,
    now_mono: float,
    replaceable: bool = True,
    fixed_window_sec: float | None = None,
    fire_on_cross_human: bool = False,
    trigger_sender_is_bot: bool = False,
) -> tuple[_DebounceState, bool]:
    """一条触发到达防抖闸。返回 (state, is_new)。

    is_new=True: 调用方是首条触发,负责睡满窗口后 dispatch(state.payload)。
    is_new=False: 本条被合并,调用方应立即返回。合并规则(坑2修复):
    同一个发送者的碎片一律拼接进在窗载荷(_merge_fragment_payload);
    跨发送者碎片(群聊)按 replaceable 决定是否整体替换。
    fixed_window_sec 非空时保持首条截止,第二个不同真人使截止立即到期;
    否则沿用私聊顺延(受 first+max_wait 硬上限)。
    replaceable=False 用于「纯图/纯表情包非显式碎片」:不替换(该形态
    会在 reactive 图片门控被整 turn 吞掉,替换会弄丢先前的文本触发载荷)。
    get-or-create 与状态更新之间无 await,asyncio 单线程下原子。
    """
    st = _debounce_states.get(key)
    if st is None:
        if fixed_window_sec is not None:
            deadline = now_mono + fixed_window_sec
        else:
            deadline = now_mono + debounce_sec
            if max_wait_sec > 0:
                # 首条也套 max_wait 硬上限——debounce_sec 配得比
                # max_wait 大时,旧代码首条 deadline 会越过 first+max_wait 才发车。
                deadline = min(deadline, now_mono + max_wait_sec)
        st = _DebounceState(
            first_mono=now_mono,
            deadline_mono=deadline,
            payload=payload,
        )
        if not trigger_sender_is_bot:
            st.first_human_id = payload.get("user_id")
        _debounce_states[key] = st
        return st, True
    if st.payload.get("delivery_generation") is not None and payload.get("delivery_generation") is not None \
            and st.payload["delivery_generation"] != payload["delivery_generation"]:
        # clear 分代：旧批不得吞掉新代消息，新批也不得被迟到的旧碎片反向污染。
        # 新代到达时整批替换，旧 fragments 不 prepend；旧代随后才到则直接丢弃。
        if payload["delivery_generation"] > st.payload["delivery_generation"]:
            st.payload = payload
            st.first_human_id = None if trigger_sender_is_bot else payload.get("user_id")
        elif fixed_window_sec is not None:
            # 群聊固定窗：迟到旧代不是有效插话，也不能影响发车时间。
            return st, False
    elif st.payload.get("user_id") == payload.get("user_id"):
        _merge_fragment_payload(st.payload, payload)
    elif replaceable:
        # 拆批:跨人替换只换触发归属与外壳,fragments 序列前置保留旧人碎片——
        # 旧人文本不再只靠 buffer 兜底,current/steer 拆批后逐行各贴真号、各自
        # 发言人。fragments 是唯一身份轨(旧的平行碎片 ID 轨已删)。
        prev_frags = list(st.payload.get("fragments") or [])
        if prev_frags:
            payload["fragments"] = prev_frags + list(payload.get("fragments") or [])
        st.payload = payload
    st.merged += 1
    if fixed_window_sec is not None:
        if (
            fire_on_cross_human
            and st.first_human_id is not None
            and st.first_human_id != payload.get("user_id")
            and not trigger_sender_is_bot
        ):
            # 其他真人插队：首条任务每 0.5s 重读 deadline，下一次循环即发车。
            st.deadline_mono = min(st.deadline_mono, now_mono)
        # 否则保持首条截止：后来的 bot 延迟配置不缩短也不抬高本窗。
        if st.first_human_id is None and not trigger_sender_is_bot:
            st.first_human_id = payload.get("user_id")
        return st, False
    new_deadline = now_mono + debounce_sec
    if max_wait_sec > 0:
        new_deadline = min(new_deadline, st.first_mono + max_wait_sec)
    st.deadline_mono = max(st.deadline_mono, new_deadline)
    return st, False


@receive_message.handle()
async def handle_message(bot: Bot, event: Event, matcher: Matcher):
    # 单例未装配(启动窗口期)原静默吞消息无日志——
    # 排障时无法区分「没收到」与「装配未完成」。补 WARN,60s 节流防刷屏。
    if _mcp.message_buffer is None or _mcp.active_sessions is None:
        _warn_unassembled_throttled("message_buffer/active_sessions")
        return

    # 函数级快照:本 handler 单次调用内全部聊天配置键读同一份 chat_config 快照。
    chat_cfg = load_chat_config()

    try:
        target = alconna.get_target()
    except Exception:
        matcher.skip()

    adapter_name = get_adapter_name(target)
    # 取不到 user_id 的匿名/异形事件(群匿名消息、
    # 无发送者平台事件)不再折叠进公共 "user" 桶——共享 session/inflight/防抖键
    # 有理论串话风险。QQ 正常消息恒有 id,取不到即不处理+WARN。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] 事件取不到 user_id,不处理 (adapter={adapter_name})")
        return
    if user_id == str(bot.self_id):
        matcher.skip()

    if not check_isolation(event, target):
        logger.debug(
            f"[HERMES skip] isolation_denied adapter={get_adapter_name(target)} "
            f"target={target.id} private={target.private} user={user_id}"
        )
        matcher.skip()

    # 引用消息提取(与 perception 共用同一 helper:引用图入库/呈递必须同源)
    replied = await _extract_replied_media(event, bot, adapter_name=adapter_name)
    replied_image_urls: list[str] = list(replied.image_urls)
    replied_msg_id = replied.msg_id  # 任务锚 quote 自动归并用(规划书§2.2)

    try:
        uni_msg = alconna.UniMessage.generate_without_reply(event=event, bot=bot)
    except Exception:
        matcher.skip()

    msg_text = uni_msg.extract_plain_text().strip()
    # keyword-soft 判定的快照要在任何拼接之前取——
    # 引用前缀/占位/@回填/转发展开随后都会拼进 msg_text,那些文本不参与点火。
    keyword_match_text = msg_text
    # 本条消息自身媒体先抽(own/sticker):引用图的号接在其后,故先算 base。
    own_image_urls = await _extract_image_urls(uni_msg, bot, adapter_name)
    # QQ 大表情包单独抽,不进普通 image_urls;是否喂模型由 media_config.sticker_vision_enabled 门控
    sticker_urls = await _extract_sticker_urls(uni_msg, bot, adapter_name)
    quote_base = len(own_image_urls) + len(sticker_urls)
    # 引用前缀拼装收口到 _compose_quote_prefix(审计修复批,疑似 B):
    # 与 perception(入库 content)同源调用——出入站同文本,历史行的引用前缀与
    # 当轮 current_message 形态一致。
    quote_prefix = _compose_quote_prefix(replied, quote_base)
    if quote_prefix:
        msg_text = f"{quote_prefix} {msg_text}".strip()

    # 内联载荷:当前消息自己的图 + 引用图(转发图恒不内联,Q3 定案);URL 去重交给内联入口。
    image_urls = own_image_urls + replied_image_urls
    nickname = _extract_sender_nickname(event, adapter_name) or user_id

    # B-0: 非文本段占位拼到 msg_text 末尾。注意这里**不重复**对 replied_message 做
    # collect: 引用消息已通过引用前缀((引用:...) 形态)进来,
    # 引用里的 voice/video 用户感知较低,且会让占位重复堆叠。
    event_db_id = _peek_event_db_id(adapter_name, getattr(event, "message_id", None))
    nontext_placeholders = _collect_nontext_placeholders(uni_msg, file_message_id=event_db_id)
    if nontext_placeholders:
        suffix = " ".join(nontext_placeholders)
        msg_text = (msg_text + " " + suffix).strip() if msg_text else suffix

    # At 段回填:让 Hermes 在 decision_protocol 里能看到当前消息 @ 了谁;keyword
    # 模式的 startswith 检测不受影响(at_placeholders 拼在末尾,不在前缀)
    # 传 event + bot_self_id 让 helper 检测「OneBot v11 _check_at_me 剥走的 @bot」并补回
    # 本部署补丁: at_placeholders 计算上移到 has_real_content
    # 快照之前,并把「含 At 段」计入 has_real_content——纯 @bot 消息(先发图再补 @
    # 的常见操作)不再被空消息闸门吞掉,按正常显式触发进 chat。完全无内容的空消息
    # (无文本/无图/无 At/无转发)仍走原 skip。仅 @ 他人的消息由下游闸门兜底:
    # 窗口外被 not_active_not_explicit 挡,窗口内被 @s-only-other-users 挡。
    at_placeholders = _collect_at_placeholders(uni_msg, event=event, bot_self_id=str(bot.self_id))
    has_real_content = bool(msg_text) or bool(image_urls) or bool(at_placeholders)
    if at_placeholders:
        suffix = " ".join(at_placeholders)
        msg_text = (msg_text + " " + suffix).strip() if msg_text else suffix

    # B-1.1: 合并转发提取 (main path 用 full 版,LLM 当前 turn 看到展开后的转发内容)。
    # 同一次 get_forward_msg 响应内兼收转发图 URL 发号(kind='forward_image',只注册
    # 不内联);base = own + sticker + quote,与 perception 侧同一编号。在 keyword-stripping
    # 之前追加:keyword 前缀作用于 msg_text 开头的手打文本,转发块拼在末尾。
    forward_base = quote_base + len(replied_image_urls)
    forward_full, _forward_image_urls = await _extract_forward_full(
        uni_msg, bot, adapter_name=adapter_name, number_base=forward_base
    )
    if forward_full is not None:
        msg_text = f"{msg_text}\n{forward_full}" if msg_text else forward_full
        has_real_content = True

    logger.debug(
        f"[HERMES recv] adapter={adapter_name} target={target.id} private={target.private} "
        f"user={user_id} nick={nickname!r} is_tome={event.is_tome()} self_id={bot.self_id!r} "
        f"at_targets={[str(s.target) for s in uni_msg[alconna.At]] if uni_msg.has(alconna.At) else []} "
        f"text_len={len(msg_text)} imgs={len(image_urls)}"
    )

    if not has_real_content:
        logger.debug(f"[HERMES skip] empty adapter={adapter_name} user={user_id}")
        matcher.skip()

    group_id = None if target.private else target.id
    now = _now_ms()

    # --- 触发判断 ---
    # addressed_to_bot 是与触发正交的寻址事实:平台确认「这条冲 bot 说的」
    # (真 @ / 引用 bot 的消息)。它随消息进 prompt 的 runtime_state,
    # 让归属判断不必从回填的裸 `@<id>` 去猜。
    #
    # 本部署补丁:hermes_group_trigger 收敛 at / keyword 两档,
    # keyword-soft 不做兼容保留(当它没存在过),all 等其它值废弃 WARN 落 keyword
    # (模式解析见下方群聊分支)。
    # is_direct = 直说标记:私聊消息恒 True,群聊 = addressed_to_bot,合成事件
    # 按各注入点给值;它不再是“显式激活触发事件”,只是“这条消息指名/直说”的
    # 事实标签(下游开窗 / 解析兜底转发 / 图片门控旁路 / 裁剪豁免
    # 全部挂在这个事实上;cooldown 与防抖的直说旁路已砍除,修改 56)。addressed_to_bot 是寻址事实,与 is_direct 并存但在
    # 私聊脱钩:私聊 addressed_to_bot 恒 False、is_direct 恒 True。
    is_direct = False
    addressed_to_bot = False
    keyword_soft_hit = False  # keyword-soft 全文命中(隐式激活)
    # 黑名单:名单账号(按群配置)的消息不置 direct/addressed——
    # 它们的 @ 降级为普通隐式信号,从入口消解 AI 互聊循环的燃料;消息照常入
    # buffer、窗口内跟帖照常可见(人类委托「去跟 X 说」的会话能继续)。
    sender_blacklisted = _is_blacklisted(group_id, user_id)
    if target.private:
        is_direct = True
    else:
        # event.is_tome() 在用户引用 bot 旧消息时也会被置 True;若当前消息含 @ 段但
        # 全部指向他人,视作"用户在跟别人讲话,只是顺手引用了 bot 那条",不算显式 @bot。
        #
        # has_bot_at 要兜底翻 event.original_message:OneBot v11 adapter 在 _check_at_me
        # 里会把消息开头/结尾的 @bot 段从 event.message 中剥掉(并置 to_me=True),下游
        # uni_msg 里看不到 @bot,但用户其实 @ 了。否则「@bot @other 怎么看」会因
        # _msg_at_only_other_users=True 触发 quoted-only 抑制,完全不响应(连 emoji 都没)。
        has_bot_at = _is_bot_at(uni_msg, str(bot.self_id)) or _has_at_bot_in_original(event, str(bot.self_id))
        is_mentioned = has_bot_at or (event.is_tome() and not _msg_at_only_other_users(uni_msg, str(bot.self_id)))
        # choice_field 已在配置加载层完成非法值回退与同值 WARN 去重。
        effective_trigger_mode = chat_cfg.group_trigger
        addressed_to_bot = is_mentioned and not sender_blacklisted
        is_direct = addressed_to_bot
        # keyword 档:关键词命中 = 隐式激活,不剥前缀、不升级 addressed_to_bot,
        # 由 LLM 拿 N1 条上下文自判是否点名。at 档关键词通道关闭:keyword_soft_hit
        # 恒 False,不调用检测(入口初始化值直通)。
        # 判定只跑本体快照(keyword_match_text),引用块/
        # 转发等拼接文本不参与点火;内容本身仍在 msg_text 里完整呈递给 LLM。
        if effective_trigger_mode == "keyword":
            keyword_soft_hit = _keyword_soft_hit(chat_cfg.keywords, keyword_match_text)

    # --- 活跃态分支(固定开启) ---
    in_active_window = (
        not target.private
        and group_id is not None
        and _mcp.active_sessions.is_alive_for_trigger(adapter_name, group_id, now)
    )

    # keyword 首条不打开活跃窗，但其防抖期内的普通续发/真人插话仍须入闸。
    in_debounce_window = bool(group_id) and make_inflight_key(
        adapter_name, target.private, user_id, group_id
    ) in _debounce_states
    if not is_direct and not in_active_window and not keyword_soft_hit and not in_debounce_window:
        logger.debug(f"[HERMES skip] not_active_not_explicit adapter={adapter_name} group={group_id} user={user_id}")
        matcher.skip()

    # 隐式激活只延时不开窗(窗口生命周期定案):窗口内的非显式消息滑动续期,
    # 受 T5 封顶(touch 内部判)。窗口外的隐式激活(keyword 档命中 / 定时巡查)
    # 走 ephemeral 临时 turn,不调 trigger()。
    if in_active_window and not is_direct:
        _mcp.active_sessions.touch(adapter_name, group_id, now)

    # C: 活跃窗口内,若消息只 @ 他人未点名 bot,视作非本路径触发,只让 perception
    # 写 buffer,不进 chat() 决策。修「跨目标 @ 误抢」模式 1。
    # 显式触发(at-bot)早已在上面计入 is_direct,不会到这里被过滤。
    if in_active_window and not is_direct and _msg_at_only_other_users(uni_msg, str(bot.self_id)):
        logger.debug(
            f"[HERMES reactive] skip: msg @s only other users (adapter={adapter_name} group={group_id} user={user_id})"
        )
        matcher.skip()

    # 显式触发(全隐式化后 = addressed):进入 / 续期活跃态(群聊场景)
    if is_direct and not target.private and group_id:
        _mcp.active_sessions.trigger(adapter_name, group_id, user_id, now_ms=now)
        logger.info(f"[HERMES] active_session triggered/renewed: {adapter_name}/{group_id} by {user_id}")
        # 队列化定案:入口不再自动钉锚。@ 只保证进委托队列并拿到一次
        # 主动判断轮;是否钉锚由该轮 submit_decision 显式给 pin_anchor(见 _run_reactive_turn)。
        # 这样玩笑/围观 @ 不会污染双槽,正常任务也不因锚满被挤掉。

    if not target.private:
        logger.info(
            f"[HERMES] dispatch: group={group_id} direct={is_direct} "
            f"in_active={in_active_window} mode=reactive"
        )

    # --- 应答防抖(本部署补丁) ---
    # 首条触发:登记状态 → 睡满防抖窗 → 拿合并后载荷 dispatch。
    # 期内碎片:同人拼接/跨人替换后返回,不另起 turn;群聊固定窗,私聊沿用顺延。
    # 防抖期间不占 inflight slot;T2 定时/投递轮询不经过本 handler,天然不防抖。
    # 显式/隐式一视同仁——委托 @ 的直连豁免已砍除(本部署补丁):双 bot 场景下
    # 豁免让互 @ 零延迟直连、无限制器助燃互聊;砍后防抖窗即限速。跨人替换只换
    # 触发归属,turn 上下文从 buffer 拉,被顶掉的碎片内容不丢。
    # 拆批 fragments 序列(P-08):本条完整对象经生产唯一构造点产出(真实入库
    # m:id 用 peek 读 perception 挂号,不 claim——claim 留给 path 一次性消费)
    # 随载荷暂存;防抖合并按序追加,steer/current 逐行渲染按号寻址,不查库。
    # 媒体分型 own/sticker/quote/forward 与 perception 入库同源。
    delivery_generation = session_manager.get_delivery_generation(
        adapter_name, target.private, user_id, group_id
    )
    fragment = _build_current_fragment(
        now_ms=now, adapter_name=adapter_name, group_id=group_id, user_id=user_id,
        nickname=nickname, msg_text=msg_text,
        own_image_urls=list(own_image_urls), sticker_urls=list(sticker_urls),
        quote_image_urls=list(replied_image_urls),
        forward_image_urls=list(_forward_image_urls or []),
        delivery_generation=delivery_generation,
        event_msg_id=getattr(event, "message_id", None),
    )
    debounce_sec = (
        chat_cfg.reply_debounce_private_sec
        if target.private
        else _GROUP_FIRST_WINDOW_SEC
    )
    max_wait_sec = chat_cfg.reply_debounce_max_wait_sec if target.private else 0.0
    sender_is_bot = _is_reply_delayed(user_id)
    # 本部署补丁: 强制延迟名单(仅 known_bots)——名单内用户的触发防抖窗提升到
    # reply_delay_for_bot_sec(防抖关闭也强制开窗,bot 交互限速器),
    # 顺延硬上限同步抬到不低于窗长,否则长窗会被默认 max_wait 钳回去。
    if chat_cfg.reply_delay_for_bot_sec > 0 and sender_is_bot:
        debounce_sec = max(debounce_sec, chat_cfg.reply_delay_for_bot_sec)
        # max_wait 的 0=不限语义优先:仅原本有上限时才抬升,0 严格保持不限
        if max_wait_sec > 0:
            max_wait_sec = max(max_wait_sec, chat_cfg.reply_delay_for_bot_sec)
    # 群聊首轮固定 6 秒，不再读取 reply_debounce_group_sec／共用 max_wait；
    # 仅保留已知 bot 首条专用延迟作为例外。群聊即使旧配置写 0 也走固定窗。
    if group_id or debounce_sec > 0:
        keyword_context = keyword_soft_hit or (
            in_debounce_window and not is_direct and not in_active_window
        )
        debounce_key = make_inflight_key(adapter_name, target.private, user_id, group_id)
        payload = dict(
            bot=bot,
            event=event,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            delivery_generation=delivery_generation,
            msg_text=msg_text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_direct=is_direct,
            addressed_to_bot=addressed_to_bot,
            now_ms=now,
            replied_msg_id=replied_msg_id,
            ephemeral=keyword_context and not in_active_window,
            context_limit=chat_cfg.history_message_limit if keyword_context else None,
            fragments=[fragment],
        )
        # get-or-create 与状态更新之间无 await,asyncio 单线程下原子。
        loop = asyncio.get_running_loop()
        # 纯图/纯表情包非显式碎片不替换跨人载荷:它是 reactive 图片门控会整 turn
        # 吞掉的形态,替换会把先前的文本触发载荷弄丢;真人仍可提前发车(图已在 buffer
        # 里,最终 turn 看得到)。同人碎片不走替换——_merge_fragment_payload 拼接,
        # 本保护只对群聊跨人碎片有意义。
        # [表情包] 是程序占位,不是用户正文;不能让纯贴纸顶掉文本触发。
        media_only = bool(image_urls or sticker_urls) and not (
            keyword_match_text or quote_prefix or at_placeholders or forward_full
            or any(p != "[表情包]" for p in nontext_placeholders)
        )
        st, is_new = _debounce_arrive(
            debounce_key,
            payload,
            debounce_sec=debounce_sec,
            max_wait_sec=max_wait_sec,
            now_mono=loop.time(),
            replaceable=not (media_only and not is_direct),
            fixed_window_sec=debounce_sec if group_id else None,
            fire_on_cross_human=bool(group_id),
            trigger_sender_is_bot=sender_is_bot,
        )
        if not is_new:
            logger.debug(
                f"[HERMES debounce] merged fragment (key={debounce_key} user={user_id} "
                f"merged={st.merged} fire_in={st.deadline_mono - loop.time():.1f}s)"
            )
            return
        try:
            while True:
                remaining = st.deadline_mono - loop.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(remaining, 0.5))
            logger.debug(
                f"[HERMES debounce] fire (key={debounce_key} merged={st.merged} "
                f"waited={loop.time() - st.first_mono:.1f}s)"
            )
            # 坑1修复:发车前先释放状态——此前状态活到
            # 整个 turn 结束才 pop,turn 期间到达的消息被当作「期内碎片」合并进
            # 已过发车点的死状态,随 pop 一并丢弃(私聊文本/@ 委托被无声吞掉,
            # 生产实证三连丢)。提前 pop 后,turn 期间的新消息开新防抖窗正常发车,
            # inflight 占线则进 pending 走现成 refire 接力。
            _debounce_states.pop(debounce_key, None)
            await _dispatch_chat(**st.payload)
        finally:
            _debounce_states.pop(debounce_key, None)  # 睡眠期异常兜底;正常路径已 pop,no-op
        return

    # --- 调用 Hermes (防抖关闭时走原路径直发) ---
    await _dispatch_chat(
        bot=bot,
        event=event,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
        delivery_generation=delivery_generation,
        msg_text=msg_text,
        image_urls=image_urls,
        sticker_urls=sticker_urls,
        is_direct=is_direct,
        addressed_to_bot=addressed_to_bot,
        now_ms=now,
        replied_msg_id=replied_msg_id,
        ephemeral=keyword_soft_hit and not in_active_window,
        context_limit=chat_cfg.history_message_limit if keyword_soft_hit else None,
        fragments=[fragment],
    )


