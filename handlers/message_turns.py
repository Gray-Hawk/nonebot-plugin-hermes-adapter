"""turn 执行域:dispatch 装配与 passive/reactive 两路 turn 执行、chat 重试/溢出恢复、同 turn 复述抑制。

从 handlers/message.py 拆出;纯搬运,零逻辑改动。
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Sequence
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
from ..core.bg_tasks import spawn_bg_task
from ..core.group_profile import build_group_profile_block
from ..core import identity
from ..config import resolve_custom_dir
from ..core.companion_runtime import (
    deliver_companion_run_output,
    prepare_companion_run,
)
from ..core.hermes_client import (
    ChatResult,
    extract_response_media,
    hermes_client,
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
from ..core.run_tracker import make_scope_key
from ..core.run_delivery_monitor import (
    order_gate,
    prepare_outbound_delivery,
    schedule_reconcile_for_run,
    settle_expected,
)
from ..core.runs_terminal import TerminalPlanError
from ..core.steer_seen import _steer_seen_filter
from ..core.message_buffer import (
    BufferedMessage,
    build_media_address_index,
)
from ..core.message_origin import MessageOrigin
from ..core import task_notes
from ..core import user_lists
from ..core import control_intent, continuation
from ..core.chat_config import load_chat_config
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
from ..core.reply_recovery import deliver_recovered_replies
from ..core.prompt_builder import (
    build_passive_system_prompt,
    build_passive_user_content,
    build_reactive_system_prompt,
    build_reactive_user_content,
    split_unpushed_messages,
    steer_region_decorations,
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
    if plugin_config.hermes_api_interface in ("astrbot", "antigravity"):
        return [replace(m, image_urls=[]) for m in window_msgs]
    return window_msgs


def _passive_decision_enabled(is_private: bool) -> bool:
    """passive 是否走正文决策通道。

    私聊天然要决策；antigravity 群 passive 也走同一通道——系统提示只教 § 协议，
    不解析就会把 § 标记原样发进群里。其余后端的群 passive 仍是 raw_text 直发。
    """
    return is_private or plugin_config.hermes_api_interface == "antigravity"


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

def _speaker_name_resolver(adapter_name: str):
    """发言者正主名 resolver(普通/历史/steer 行唯一接线)。

    身份事实只有一份:custom_dir 下的 memory/<adapter>/identities.csv。配置目录
    优先,而且只要配置目录里的这张表**已经有条目**,它就是唯一权威源——表里缺某个
    用户就是「没有这条事实」,返回 None,绝不拿别的目录里同名账号的旧名字补上
    (那会把上一个目录/上一个配置的身份带进当前呈递)。

    只有整张表都还不存在/没有条目时(定制目录搬迁、预览环境未重新初始化配置),
    才沿用本进程刚加载过的同一适配器身份表:身份事实仍是同一份表,不新建档案、
    不猜身份。steer 入口(直注/跟帖)与普通/历史行共用这一份接线,避免同一个用户
    在不同呈递通道里一段有正主名、一段没有。
    """
    custom_dir = resolve_custom_dir()

    def resolve(user_id: str) -> str | None:
        name = identity.resolve_display_name(custom_dir, adapter_name, user_id)
        if name:
            return name
        if identity.has_identity_entries(custom_dir, adapter_name):
            # 当前权威表已经有条目:缺这一行就是没有事实,不跨目录借。
            return None
        for alt in identity.loaded_identity_dirs(adapter_name):
            if custom_dir is not None and str(alt) == str(custom_dir):
                continue
            name = identity.resolve_display_name(alt, adapter_name, user_id)
            if name:
                return name
        return None

    return resolve


def _match_pending_steer_rows(
    pending_steer: str, candidates: list[dict], *, convention_line: str = "",
    decorations: Sequence[str] = (),
) -> list[dict] | None:
    """按 pending 原文的出现位置与次数匹配台账行(台账只提供身份,不提供顺序)。

    上游把本 run 被吞的 steer 文本按**接受顺序**拼成 pending_steer;台账行是客户端
    HTTP 回应后落的库,两个合资格入口的回应可以乱序到达(B 先回、A 后回),台账序
    因此是 B/A,与原文相反。所以顺序与出现次数只认原文:从左到右扫,每个位置必须
    正好被某条台账 steer_text 覆盖;同一文本出现几次就还原几行(旧版本在生产库里
    留下的重复账项,也照原文次数还原——既不会凭空多一行,也不会少还原被吞的行)。

    **先按原位置精确匹配,匹配不到才把空白当分隔符跳过**:私聊无号行的呈递就是正文
    本身,正文完全可能以空白/换行开头(用户原文逐字节保全)。先跳空白再 startswith
    会让这类账项永远匹配不上,整批退回渲染文本,把多条合成一条、丢掉尾换行。

    空串正文(用户发了一条空行消息)在原文里没有任何字符可锚定:它在「当前位置没有
    任何非空账项命中」时就地按台账批次顺序取出,条数与相对顺序同样保留;整批以空行
    结尾时,尾部剩余的空串账项一次补齐。

    返回 None = 原文里存在台账覆盖不到的正文(部分命中/台账缺失):调用方整段
    fallback,宁可作者信息回退,也不让未命中正文从呈递里消失。同一起点能匹配多条
    时取最长:更长账项完整覆盖后续行,避免把一条多行账项拆成「前缀 + 残段」。

    convention_line 是发送原语在整批末尾追加一次的系统约定行(不属于任何账项
    正文,多条各自成批时会出现多次):只有在**当前位置没有任何账项命中**时才把它
    当分隔符跳过一次。绝不能先对整段文本做全局删除——用户正文里引用同一行字面量
    时会被删掉,匹配随之失败,原文被永久改写。

    decorations 同理:steer 整批在聊天区区域块里发送,开标签行/固定说明行/闭标签
    是块框架(`prompt_builder.steer_region_decorations`),不是任何账项正文。同样
    只在没有账项命中处跳过。
    """
    remaining = list(candidates)
    matched: list[dict] = []
    pos = 0
    total = len(pending_steer)
    while pos < total:
        # 1) 当前位置精确命中的账项优先(正文可以以空白开头,不能先跳空白)。
        hits = [
            c for c in remaining
            if c.get("steer_text") is not None
            and c["steer_text"] != ""
            and pending_steer.startswith(c["steer_text"], pos)
        ]
        if hits:
            hit = max(hits, key=lambda c: len(c["steer_text"]))
            matched.append(hit)
            remaining.remove(hit)
            pos += len(hit["steer_text"])
            continue
        # 2) 发送侧装饰/批次分隔:只在没有账项命中时跳过(先于空串账项,约定行本身
        #    是整行字面量,不能被当成"这里可以插一条空行消息"的位置)。
        if convention_line and pending_steer.startswith(convention_line, pos):
            pos += len(convention_line)
            continue
        decoration = next(
            (d for d in decorations if d and pending_steer.startswith(d, pos)), None
        )
        if decoration is not None:
            pos += len(decoration)
            continue
        # 3) 空串账项:原文里没有可锚定的字符,只能在「此处没有非空账项」时就地取出。
        empty = next(
            (c for c in remaining if c.get("steer_text") == ""), None
        )
        if empty is not None:
            matched.append(empty)
            remaining.remove(empty)
            continue
        # 4) 批次内分隔空白。
        if pending_steer[pos] in " \t\r\n":
            pos += 1
            continue
        return None
    # 批次以空行结尾(空串行在末尾):尾部剩余的全是空串账项时一次补齐。
    if all(c.get("steer_text") == "" for c in remaining):
        matched.extend(remaining)
    return matched or None


def _pending_steer_recovery_rows(
    pending_steer: str, *,
    adapter_name: str, is_private: bool, user_id: str, group_id: str | None,
    now_ms: int,
) -> list[tuple[str | None, BufferedMessage]] | None:
    """native 回收的逐行身份还原(P-09),每行带上台账 m_id 供交会去重。

    pending_steer 是上游把本 run 被吞 steer 还回时的拼接文本,speaker 信息已在
    上游剥壳时全丢——直接整段回收会把多作者内容整体归给 run 发起者。steer_ledger
    逐行存了 scope+m_id+sender+rendered_line:按 scope 拉候选台账,**顺序与出现
    次数只认 pending 原文的实际位置**(`_match_pending_steer_rows`),台账仅提供
    每行的 sender/正文身份,不按 ACK／SQLite 插入序重排。命中行构造独立
    human_replay fragments,各 sender 与原文顺序原样保留。带号行的 m:id 是既有
    原始行(库里那一行),随 fragments 一起带上:正文逐字节取自原文,媒体/引用
    寻址按既有 message_images 事实单独恢复(空媒体表就不贴标签,不伪造)。
    返回值的 m_id 与 fragment 并排,给「原行是否还在接力槽」的交接判断用
    (见 `InflightRegistry.add_recovered_followup`),不用于新造身份。
    human_replay 的 allows_store=False 保证原行不重复入库。原文有台账覆盖不到的
    正文(部分命中/无台账)返回 None,调用方走原文整段回收 fallback 并记明确
    日志——不为修作者另造脆弱的模型文本解析侧链。
    """
    from ..core.steer_reconcile import original_text_of
    tracker = _mcp.run_tracker
    if tracker is None:
        return None
    try:
        scope_key = make_scope_key(adapter_name, is_private, user_id, group_id)
        candidates = tracker.pending_steers_by_scope(scope_key)
    except Exception:
        logger.warning(
            "[HERMES steer回收] 台账查询失败, native 回收走原文 fallback",
            exc_info=True,
        )
        return None
    # 约定行是发送侧装饰(每批一次),不属于任何账项正文:只在「当前位置没有任何
    # 账项命中」时当分隔符跳过。绝不先做全局删除——用户正文引用同一行字面量时会被
    # 删掉,匹配失败后整批退回渲染文本,原文就被永久改写了。
    from .message_relay import _STEER_REPLY_CONVENTION_LINE

    matched = _match_pending_steer_rows(
        pending_steer, candidates, convention_line=_STEER_REPLY_CONVENTION_LINE,
        decorations=steer_region_decorations(),
    )
    if matched is None:
        if candidates:
            logger.warning(
                "[HERMES steer回收] 台账只覆盖部分 pending_steer, "
                "为避免漏呈递改走原文整段 fallback"
            )
        return None
    buffer = getattr(_mcp, "message_buffer", None)
    rows = []
    for c in matched:
        # 正文只取原始来源(台账 raw_text / 按 m:id 回查 messages / 旧账项兼容还原),
        # 新账项不从渲染行剥壳。
        content = original_text_of(c)
        if content is None:
            logger.warning(
                f"[HERMES steer回收] 台账行取不到原始正文,改走原文整段 fallback "
                f"(m_id={c.get('m_id')})"
            )
            return None
        m_id = str(c["m_id"])
        # 真实带号行:按既有 messages/message_images 事实恢复 m:id、发送者昵称与
        # 结构化 @(下一轮普通 handler 的 [有人@我]/user= 就靠这些原始事实),媒体
        # 寻址同样回查既有表。正文仍逐字节取自原文,不把旧渲染尾缀塞回正文,也
        # 不从标签反推 At;库里没有这一行(或无该列事实)就保持未知,不猜。
        fragment_id = int(m_id) if m_id.isdigit() else None
        media: dict[str, list[str]] = {}
        facts: dict = {}
        if fragment_id is not None and buffer is not None:
            try:
                media = buffer.get_media_urls(fragment_id)
            except Exception:
                logger.warning(
                    f"[HERMES steer回收] 媒体寻址回查失败 (m_id={m_id})", exc_info=True
                )
                media = {}
            try:
                facts = buffer.get_message_facts(fragment_id) or {}
            except Exception:
                logger.warning(
                    f"[HERMES steer回收] 身份事实回查失败 (m_id={m_id})", exc_info=True
                )
                facts = {}
            if not isinstance(facts, dict):
                facts = {}
        rows.append((
            m_id,
            BufferedMessage(
                ts=now_ms, adapter=adapter_name, group_id=group_id,
                user_id=str(facts.get("user_id") or c["sender_id"]),
                nickname=str(facts.get("nickname") or c["sender_id"]),
                content=content,
                origin=MessageOrigin.human_replay(),
                image_urls=list(media.get("image", [])),
                sticker_urls=list(media.get("sticker", [])),
                quote_image_urls=list(media.get("quote", [])),
                forward_image_urls=list(media.get("forward_image", [])),
                id=fragment_id,
                at_user_ids=tuple(facts.get("at_user_ids") or ()),
                reply_to_ts=None, is_bot=False,
            ),
        ))
    return rows


def _pending_steer_recovery_fragments(
    pending_steer: str, *,
    adapter_name: str, is_private: bool, user_id: str, group_id: str | None,
    now_ms: int,
) -> list[BufferedMessage] | None:
    """`_pending_steer_recovery_rows` 的 fragments 视图(既有调用面/探针沿用)。"""
    rows = _pending_steer_recovery_rows(
        pending_steer, adapter_name=adapter_name, is_private=is_private,
        user_id=user_id, group_id=group_id, now_ms=now_ms,
    )
    return None if rows is None else [fragment for _m_id, fragment in rows]


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


def _current_delivery_generation(adapter_name, is_private, user_id, group_id):
    """旧测试/外部包装可替换 session_manager；缺新 getter 时保持无 token 兼容。"""
    getter = getattr(session_manager, "get_delivery_generation", None)
    if getter is None:
        return None
    return getter(adapter_name, is_private, user_id, group_id)


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
    # 拆批 fragments 序列:防抖合并后的有序完整碎片,
    # None=无拆批信息(旧路径单行)。
    fragments=None,
    delivery_generation: int | None = None,
):
    """passive/reactive 分叉(原 handle_message 末段,防抖批抽出以便
    防抖到期后与防抖关闭路径共用同一 dispatch)。"""
    if delivery_generation is None:
        # 合成/旧调用没有入场章时在 dispatch 补盖；真人消息在 message.py 更早盖章。
        delivery_generation = _current_delivery_generation(
            adapter_name, target.private, user_id, group_id
        )
    # 任务锚+steer(规划书§3):资格内直注已前移到 `message.handle_message` 的
    # 防抖等待之前(定案 118,同 scope 有真实在跑轮/ACP 轮时直注)。dispatch 阶段
    # 不再重复尝试,避免同一消息被处理两次;未直注的消息在这里照常开 passive/reactive 轮。
    if target.private:
        # 原 v0.1.6 等价路径:passive 模式,raw_text 直接当回复
        await _handle_passive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            delivery_generation=delivery_generation,
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
        delivery_generation=delivery_generation,
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

# 段内重复吞掉的可见回执：给本轮触发消息贴的 emoji id（NapCat 群聊实测
# set_msg_emoji_like 可用；私聊内部 result=65011，不调用）。不新增配置项。
_SEGMENT_DUPLICATE_EMOJI_ID = 32

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

def _decision_body(decision: dict) -> str:
    """决策正文：纯文本协议下正文就是 ``body``。

    AstrBot 旧信封的 ``reply_text`` 已在 client 解析分支归一为 ``body``，
    这里不按接口分叉。
    """
    return str((decision or {}).get("body") or "")


def _merge_reply_media(result_media, extracted_media):
    """媒体补发：客户端已下载的在前，正文里抽到的按出现顺序补，同 URL 去重。"""
    merged = list(result_media)
    merged.extend(u for u in extracted_media if u not in result_media)
    return merged


def _mid_turn_action(mid_turn_push: bool, pushed_text: str, pushed_media: int,
                     reply_text: str, media_urls) -> str:
    """同 turn 内 push 与终态回复的处置：send / send_both / media_only / suppress。

    顺序即副作用顺序的判定：先看是否复述，再决定只补媒体还是整条抑制；
    不复述时两条都发（答案不能被吞）。
    """
    if not mid_turn_push:
        return "send"
    if not _is_restatement(pushed_text, reply_text):
        return "send_both"
    if media_urls and pushed_media == 0:
        return "media_only"
    return "suppress"


async def _handle_mid_turn_push(*, action: str, send_text, prepare_delivery, mark_replied,
                                finish_history, schedule_reconcile, settle_suppressed) -> bool:
    """轮内 push 冲突的唯一处置入口；True = 本轮已收口，调用方直接 return。

    顺序与改造前一致：media_only 先备投递再只发媒体、成功后 mark、收尾与对账；
    suppress 只落外部投递口径，不做任何发送。
    """
    if action == "media_only":
        delivery = prepare_delivery()
        sent = await send_text(delivery)
        if sent:
            mark_replied()
        finish_history(delivery, sent)
        schedule_reconcile()
        return True
    if action == "suppress":
        settle_suppressed()
        return True
    return False


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

    if _live_callback_has_sent(chat_kwargs.get("decision_callback")):
        # E3:整轮重跑会重新开 run 并把已即时发出的正文再发一遍；宁可不重试，
        # 由本轮失败面收口（已发送的事实保留在台账里）。
        logger.error(
            f"[HERMES {label}] 持久化失败但本轮已有即时发送,不重试以免重复投递 "
            f"(cause={result.persistence_cause})"
        )
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

    if _live_callback_has_sent(chat_kwargs.get("decision_callback")):
        logger.error(
            f"[HERMES {label}] 上下文溢出但本轮已有即时发送,放弃重发以免重复投递"
            f"(group={chat_kwargs.get('group_id')})"
        )
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
    chat_kwargs["delivery_generation"] = _current_delivery_generation(
        adapter_name, is_private, user_id, group_id
    )

    retried = await _chat_with_persistence_retry(
        label, bot_self_id=str(reply_bot.self_id), **chat_kwargs
    )
    if retried.is_context_overflow:
        logger.error(
            f"[HERMES {label}] 重置后仍然溢出,放弃本轮(group={group_id})—— 病因不在历史而在单轮本身:"
            "system prompt 加历史窗口就已经超了,请缩小窗口或换更大上下文的模型"
        )
    return retried

def _segment_duplicate_emoji_hook(
    *, bot: Bot, trigger_msg_id, group_id: str, adapter_name: str, interface: str
) -> Callable[[str, str, str], None] | None:
    """段内重复吞掉时给触发消息贴一次表情的观察口；不适用时返回 None。

    只接 onebotv11 群聊 reactive（调用方是 _run_reactive_turn）＋ runs 通道＋
    拿得到 trigger_msg_id 的路径。私聊不在调用点：NapCat 私聊
    set_msg_emoji_like 外层 retcode=0 但内部 result=65011，是能力边界，
    不能把外层成功当贴上。

    每轮最多贴一次（首次命中即置位，失败也不重试）；后台任务自身吞异常，
    只有 debug/warning 日志，绝不影响正文发送。
    """
    if interface != "runs" or adapter_name != "onebotv11" or trigger_msg_id is None:
        return None
    fired = False

    def _hook(reason: str, text: str, source: str) -> None:
        nonlocal fired
        if reason != "segment_duplicate" or fired:
            return
        fired = True
        logger.info(
            f"[HERMES reactive] 段内重复吞掉,给触发消息贴表情 "
            f"(group={group_id} msg={trigger_msg_id} len={len(text)})"
        )
        spawn_bg_task(
            _emoji_like_quiet(bot, trigger_msg_id, _SEGMENT_DUPLICATE_EMOJI_ID),
            name="segment-duplicate-emoji",
        )

    return _hook


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


def _terminal_delivery(result, index: int, **kwargs):
    """Runs 终态多份:取整批计划里第 index 份的投递上下文。

    只有 runs 终态(带 terminal_decisions)会挂 terminal_batch_contexts;
    其余路径(completions／散文兜底／旧接口)保持原有 _prepare_delivery 行为。
    """
    contexts = getattr(result, "terminal_batch_contexts", None)
    if contexts is not None:
        return contexts[index] if index < len(contexts) else None
    return _prepare_delivery(result, **kwargs)


def _generation_guard_context(generation, *, adapter_name, is_private, user_id, group_id):
    """只带代际栅栏的出站上下文（无 Runs 台账）；generation=None 时无上下文。"""
    if generation is None:
        return None
    getter = getattr(session_manager, "get_delivery_generation", None)
    if getter is None:
        return None
    return DeliveryContext(
        tracker=None,
        run_id="",
        generation_guard=lambda: getter(adapter_name, is_private, user_id, group_id) <= generation,
    )


def _generation_fenced_delivery(result, *, adapter_name, is_private, user_id, group_id):
    """无 Runs 台账的终态(antigravity ACP)沿用会话代际做旧代出站围栏。

    /clear(T1 空闲重置同理)已推进 generation 时,在途轮次即使拿到回复也不得再调
    平台。判定点与失败语义和 runs 路 generation_guard 相同;没有 run 身份,
    因此不做组件认领与台账回执,只核一次既有 SessionManager 代际。
    """
    return _generation_guard_context(
        getattr(result, "delivery_generation", None),
        adapter_name=adapter_name, is_private=is_private,
        user_id=user_id, group_id=group_id,
    )


def _history_component(index: int) -> str:
    return f"history:{index}"


def _batch_file_prefix(index: int) -> str:
    return f"{index}:"


def _cancel_batch_unit_for(
    tracker, run_id: str | None, index: int, *, reason: str
) -> None:
    """按 run_id/index 把一份记免发终态（流式即时发送与终态链共用）。"""
    if tracker is None or not run_id:
        return
    wanted = {f"primary:{index}", f"history:{index}"}
    try:
        comps = tracker.list_components(run_id)
    except Exception as exc:
        logger.warning(
            f"[HERMES terminal] 第{index}份免发记账读取失败({reason}) run={run_id}: {exc}"
        )
        return
    for comp in comps:
        component_id = comp.get("component_id", "")
        if component_id not in wanted and not component_id.startswith(f"file:{index}:"):
            continue
        if comp.get("state") in ("sent", "cancelled", "not_required"):
            continue
        try:
            tracker.mark_component_not_required(run_id=run_id, component_id=component_id)
        except Exception as exc:
            logger.warning(
                f"[HERMES terminal] 第{index}份免发记账失败({reason}) "
                f"run={run_id} component={component_id}: {exc}"
            )


def _cancel_batch_unit(result, index: int, *, reason: str) -> None:
    """Runs 终态多份:被业务拦截(bounce／轮内去重)的份记既有免发终态。

    该份的 primary／history／file 组件都不再投递，后续 repair 对账不得把它当漏件
    补发；已 sent 的组件保持原回执不动，其他正常份不受影响。
    """
    tracker, run_id = _delivery_identity(result)
    _cancel_batch_unit_for(tracker, run_id, index, reason=reason)


def _prepare_terminal_batch_contexts(result, units):
    """Runs 终态整批计划:units 由共享计划器派生，一次落库；失败抛 TerminalPlanError。

    计划只由 output 派生(body／媒体／文件／reply_to)，send 时的引用解析结果不进
    计划——恢复路径重复调用才能得到逐字相同的 plan_json(30 号 upsert 拒绝任何变化)。
    """
    from ..core.runs_terminal import prepare_terminal_batch

    tracker, run_id = _delivery_identity(result)
    contexts = prepare_terminal_batch(tracker=tracker, run_id=run_id, units=units)
    return contexts, units


async def _await_order_gate(result) -> None:
    """最终答复出站前的短暂顺序门;异常不阻断 legacy 发送。"""
    _, run_id = _delivery_identity(result)
    if not run_id:
        return
    try:
        await order_gate(run_id)
    except Exception as exc:
        logger.warning(f"[runs-delivery] 顺序门异常(按 legacy 直接发送): {exc}")


def _mark_history_component(
    tracker, run_id: str | None, sent, *, wrote_history: bool, component_id: str
) -> None:
    """history 组件回执：写库的记 sent，本就不写历史的记免发。"""
    if tracker is None or not run_id or not sent:
        return
    try:
        if wrote_history:
            tracker.mark_component_sent(
                run_id=run_id, component_id=component_id,
                msg_ids=list(getattr(sent, "msg_ids", []) or []),
            )
        else:
            tracker.mark_component_not_required(run_id=run_id, component_id=component_id)
    except Exception as exc:
        logger.warning(f"[runs-delivery] history 组件回执失败 run={run_id}: {exc}")


def _finish_history(result, delivery, sent, *, wrote_history: bool, component_id: str = "history") -> None:
    """legacy 历史写库后回执 history 组件;该路本就不写历史的记免发。

    component_id 只给 Runs 终态多份用(E2):每份各自 history:<i>,旧路径不传即 history。
    """
    if delivery is None or not sent:
        return
    tracker, run_id = _delivery_identity(result)
    _mark_history_component(
        tracker, run_id, sent, wrote_history=wrote_history, component_id=component_id,
    )


def _schedule_reconcile(result) -> None:
    """turn 收尾调度统一对账;异常不影响既有收尾。"""
    _, run_id = _delivery_identity(result)
    if not run_id:
        return
    try:
        schedule_reconcile_for_run(run_id)
    except Exception as exc:
        logger.warning(f"[runs-delivery] 对账调度失败 run={run_id}: {exc}")


def _make_runs_live_decisions(
    *,
    bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
    fb_key,
    nickname: str,
    session=None,
    mark_replied: bool = False,
    suppresses_feedback: bool = False,
):
    """E3:runs 轮内 delta 决策的即时发送器（正文即时发，控制副作用留给终态）。

    返回 (deliver, state)：
    - deliver(decision, run_id)：一份完整决策到达即落台账追加、按 E2 同一套
      bounce／媒体／文件／history／buffer 语义发送，不执行收窗/锚等
      轮级控制（终态按持久决策顺序统一执行）。
    - state：sent_any／suppressed_any／last_reply_at，供终态链判定同 turn 去重与收口。

    台账追加（append_stream_unit）先于发送；失败直接抛给 client，client 停用本轮
    即时发送并由终态 E2 路径补发。发送段异常只记日志，不让 SSE 消费循环崩。
    """
    state = {"sent_any": False, "suppressed_any": False, "last_reply_at": 0, "outcomes": {}}
    prepared = {}
    key = make_inflight_key(adapter_name, is_private, user_id, group_id)

    def prepare(decision: dict, run_id: str):
        from ..core.runs_terminal import append_stream_unit, units_from_decisions

        # 控制按「完整正文决策到达／入队」的确定性时点消费，而不是等到实际发送：
        # 发送受 3 秒条间节奏约束，若把绑定推迟到 deliver，等待期间新登记的控制会
        # 被已经到达的旧正文抢走（甲/乙/丙 见证）。prepare 在 _RunsReplyQueue.put
        # 同步调用一次；bind_control 对同一决策幂等，deliver/终态再绑不会弹第二份。
        control_intent.bind_control(decision, key)
        pairs = units_from_decisions([decision])
        if pairs:
            prepared[id(decision)] = append_stream_unit(
                tracker=_mcp.run_tracker, run_id=run_id, unit=pairs[0][1],
            )

    def set_busy(busy: bool):
        inflight = getattr(_mcp, "inflight", None)
        if inflight is not None:
            inflight.set_reply_batch_busy(key, busy)

    async def on_drained():
        from .message_relay import _release_runs_batch_inputs

        try:
            await _release_runs_batch_inputs(key, adapter_name, is_private, user_id, group_id)
        except Exception:
            logger.warning("[HERMES] 发送批后 steer 释放失败，输入仍留原接力槽", exc_info=True)

    async def deliver(decision: dict, run_id: str) -> None:
        from ..core.runs_terminal import append_stream_unit, units_from_decisions

        # 控制队列：一份决策消费一份登记控制（空 body 也占一份）。正常 Runs 流式
        # 路径已在 prepare（决策入队）绑定过；这里的再次绑定是幂等兜底（非流式/终态
        # 直投不经 prepare）。控制只登记不执行，这一份照旧走原 delivery/outbound。
        control_intent.bind_control(decision, key)
        pairs = units_from_decisions([decision])
        if not pairs:
            return  # 空 body/静默份：只占决策序，不占发送序，控制留给终态
        _, unit = pairs[0]
        tracker = _mcp.run_tracker
        ctx = prepared.pop(id(decision), None)
        if ctx is None:
            ctx = append_stream_unit(tracker=tracker, run_id=run_id, unit=unit)
        index = int(str(ctx.component_id).split(":")[-1])
        body = unit["body"]
        media_urls = list(unit["media_urls"])
        file_paths = list(unit["file_paths"])
        if await _maybe_intercept_bounce(
            key=fb_key, reply_text=body, bot=bot, target=target,
            adapter_name=adapter_name, user_id=user_id, group_id=group_id,
            is_private=is_private,
        ):
            _cancel_batch_unit_for(tracker, run_id, index, reason="bounce")
            state["suppressed_any"] = True
            state["outcomes"][id(decision)] = ("suppressed", None)
            return
        resolved_reply = resolve_reply(
            _mcp.message_buffer,
            adapter=adapter_name,
            group_id=None if is_private else group_id,
            owner_user_id=user_id if is_private else None,
            reply_to=control_intent.control_value(decision, "reply_to"),
            log_tag="HERMES live",
        )
        state["sent_any"] = True  # 与 E2 同口径：尝试过发送就不在收口时记 silent
        if decision.get("_runs_terminal_only"):
            from types import SimpleNamespace

            await _await_order_gate(SimpleNamespace(run_id=run_id))
        sent = await send_text_with_media(
            bot=bot, target=target, text=body, media_urls=media_urls,
            reply_qq_message_id=resolved_reply.qq_message_id if resolved_reply else None,
            reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
            adapter_name=adapter_name, delivery=ctx,
        )
        await send_local_files(
            bot=bot, target=target, file_paths=file_paths, adapter_name=adapter_name,
            component_prefix=f"{index}:", delivery=ctx,
        )
        wrote_history = False
        if sent and _mcp.message_buffer is not None:
            buffered_content = send_result_history(sent, body)
            if media_urls:
                buffered_content = f"{buffered_content} [图片]".strip()
            _mcp.message_buffer.append(
                BufferedMessage(
                    ts=_now_ms(), adapter=adapter_name,
                    group_id=None if is_private else group_id,
                    user_id=str(bot.self_id), nickname=nickname,
                    content=buffered_content, origin=MessageOrigin.bot_output(),
                    image_urls=[], is_bot=True,
                    qq_message_id=getattr(sent, "primary_qq_message_id", None),
                    private_owner_id=user_id if is_private else None,
                )
            )
            wrote_history = True
        _mark_history_component(
            tracker, run_id, sent, wrote_history=wrote_history,
            component_id=_history_component(index),
        )
        if sent and mark_replied and session is not None and not is_private \
                and group_id is not None and _mcp.active_sessions is not None:
            _mcp.active_sessions.mark_bot_replied(
                adapter_name, group_id, now_ms=_now_ms(),
                media_count=len(media_urls) + len(file_paths) + send_result_sticker_count(sent),
                text=body,
            )
            state["last_reply_at"] = getattr(session, "last_bot_reply_at", 0)
        if sent:
            await _post_send_feedback(
                key=fb_key, reply_text=body, bot=bot, target=target,
                adapter_name=adapter_name, user_id=user_id, group_id=group_id,
                is_private=is_private, suppress_recursion=suppresses_feedback,
                not_final=bool(decision.get("not_final")),
            )
        # E3/R5:终态控制只对真正发送成功的即时份执行(与 terminal-only 同口径)；
        # bounce／发送失败的份记下结果，终态不得误施开窗/锚控制。
        state["outcomes"][id(decision)] = ("sent" if sent else "failed", sent)
        logger.info(
            f"[HERMES live] 即时发送第{index}份 adapter={adapter_name} "
            f"group={group_id} private={is_private} ok={bool(sent)} "
            f"text_len={len(body)} media={len(media_urls)} files={len(file_paths)}"
        )

    setattr(deliver, "has_sent", lambda: bool(state["sent_any"]))
    setattr(deliver, "prepare", prepare)
    setattr(deliver, "set_busy", set_busy)
    setattr(deliver, "on_drained", on_drained)
    return deliver, state


def _make_acp_live_decisions(
    *,
    bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
    fb_key,
    nickname: str,
    session=None,
    mark_replied: bool = False,
    delivery_generation: int | None = None,
    suppresses_feedback: bool = False,
):
    """ACP 流式正文的即时发送器（无 Runs 台账，不伪造 run_id）。

    与 runs 版共用同一套 outbound／历史／反馈原语，只去掉台账组件认领与回执；
    clear 代际仍由 DeliveryContext.generation_guard 在每次出站前复核。这里不设置
    reply_batch_busy：ACP steer 的实测语义允许生成中再发 prompt，闸门由 turn 状态
    （active_turn / finishing）负责。
    """
    state = {"sent_any": False, "suppressed_any": False, "last_reply_at": 0, "outcomes": {}}
    key = make_inflight_key(adapter_name, is_private, user_id, group_id)

    def prepare(decision: dict, run_id: str) -> None:
        control_intent.bind_control(decision, key)

    async def deliver(decision: dict, run_id: str = "") -> None:
        body = str(decision.get("body") or "").strip()
        if not body:
            return
        # 与 runs 版同口径：一份决策消费一份登记控制（空 body 也占一份）。
        control_intent.bind_control(decision, key)
        if await _maybe_intercept_bounce(
            key=fb_key, reply_text=body, bot=bot, target=target,
            adapter_name=adapter_name, user_id=user_id, group_id=group_id,
            is_private=is_private,
        ):
            state["suppressed_any"] = True
            state["outcomes"][id(decision)] = ("suppressed", None)
            return
        resolved_reply = resolve_reply(
            _mcp.message_buffer,
            adapter=adapter_name,
            group_id=None if is_private else group_id,
            owner_user_id=user_id if is_private else None,
            reply_to=control_intent.control_value(decision, "reply_to"),
            log_tag="HERMES acp live",
        )
        state["sent_any"] = True  # 与 E2 同口径：尝试过发送就不在收口时记 silent
        delivery = _generation_guard_context(
            delivery_generation,
            adapter_name=adapter_name, is_private=is_private,
            user_id=user_id, group_id=group_id,
        )
        sent = await send_text_with_media(
            bot=bot, target=target, text=body,
            reply_qq_message_id=resolved_reply.qq_message_id if resolved_reply else None,
            reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
            adapter_name=adapter_name,
            **({"delivery": delivery} if delivery is not None else {}),
        )
        wrote_history = False
        if sent and _mcp.message_buffer is not None:
            _mcp.message_buffer.append(
                BufferedMessage(
                    ts=_now_ms(), adapter=adapter_name,
                    group_id=None if is_private else group_id,
                    user_id=str(bot.self_id), nickname=nickname,
                    content=send_result_history(sent, body), origin=MessageOrigin.bot_output(),
                    image_urls=[], is_bot=True,
                    qq_message_id=getattr(sent, "primary_qq_message_id", None),
                    private_owner_id=user_id if is_private else None,
                )
            )
            wrote_history = True
        if sent and mark_replied and session is not None and not is_private \
                and group_id is not None and _mcp.active_sessions is not None:
            _mcp.active_sessions.mark_bot_replied(
                adapter_name, group_id, now_ms=_now_ms(),
                media_count=send_result_sticker_count(sent), text=body,
            )
            state["last_reply_at"] = getattr(session, "last_bot_reply_at", 0)
        if sent:
            await _post_send_feedback(
                key=fb_key, reply_text=body, bot=bot, target=target,
                adapter_name=adapter_name, user_id=user_id, group_id=group_id,
                is_private=is_private, suppress_recursion=suppresses_feedback,
                not_final=bool(decision.get("not_final")),
            )
        state["outcomes"][id(decision)] = ("sent" if sent else "failed", sent)
        logger.info(
            f"[HERMES acp live] 即时发送 adapter={adapter_name} "
            f"group={group_id} private={is_private} ok={bool(sent)} "
            f"text_len={len(body)} wrote_history={wrote_history}"
        )

    setattr(deliver, "has_sent", lambda: bool(state["sent_any"]))
    setattr(deliver, "prepare", prepare)
    return deliver, state


def _live_callback_has_sent(callback) -> bool:
    """E3: 本轮即时发送器是否已经真正尝试过发送（重试前防重复投递的闸门）。"""
    checker = getattr(callback, "has_sent", None)
    if not callable(checker):
        return False
    try:
        return bool(checker())
    except Exception:
        return False


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
    if not await prepare_companion_run(
        session_id=session_key,
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        group_id=group_id,
        is_private=is_private,
    ):
        logger.warning("[HERMES passive] companion 旧事件未恢复，本轮不启动新 run")
        return None
    delivery_generation = (
        current_buffered.delivery_generation
        if current_buffered is not None and current_buffered.delivery_generation is not None
        else _current_delivery_generation(adapter_name, is_private, user_id, group_id)
    )
    source_event_id = current_buffered.source_event_id if current_buffered is not None else None
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
    use_private_decision = _passive_decision_enabled(is_private)
    # 本部署补丁:计算记忆根路径,用于路由提示动态跟随配置
    _custom_dir = resolve_custom_dir()
    _memory_root = str(_custom_dir / "memory" / adapter_name) if _custom_dir else None
    # 身份表：发言者标签正主名注释，custom_dir 不可用/查不到都静默 None
    _name_resolver = _speaker_name_resolver(adapter_name)
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
        # 任务锚总开关关闭:上一轮(开启时)排队的两类非法锚纠错便签不再组装进
        # user content——关闭后不继续教模型用锚;其余便签(引用/退窗/媒体等)照常。
        plugin_note="\n".join(
            task_notes.pop_plugin_notes(
                _feedback_scope_key(adapter_name, is_private, user_id, group_id),
                drop_kinds=(
                    () if load_chat_config().task_anchor_enabled
                    else task_notes.ANCHOR_NOTE_KINDS
                ),
            )
        ) or None,
        name_resolver=_name_resolver,
        # 身份栏与 [有人@我]:当前 bot 的 self_id 是「我」的唯一事实源;
        # adapter 用于 admin_users/master_users 复合键归属。
        self_id=str(bot.self_id),
        adapter=adapter_name,
    )

    # 纯文本正文协议：正文（含工具前的进度）走 Runs 决策通道，
    # 不再有独立旁白通道——interim echo 整路停用，避免同文发两遍。
    # E3: 私聊 passive 的 runs 决策同样即时发送；runs 的群 passive 是普通文本路径，
    # 不接。antigravity 的私聊/群 passive 都走 § 决策通道（见 _live_enabled）。
    _live_fb_key = _feedback_scope_key(adapter_name, is_private, user_id, group_id)
    # 续跑请求按会话键登记：轮开始先清掉上一轮残留（本轮工具会重新登记）。
    continuation.discard_continuation(_live_fb_key)
    _live_enabled = use_private_decision and plugin_config.hermes_api_interface in (
        "runs", "antigravity",
    )
    _passive_nickname = await get_bot_nickname(bot) if _live_enabled else ""
    if plugin_config.hermes_api_interface == "antigravity":
        _live_deliver, _live_state = _make_acp_live_decisions(
            bot=bot, target=target, adapter_name=adapter_name, user_id=user_id,
            group_id=group_id, is_private=is_private, fb_key=_live_fb_key,
            nickname=_passive_nickname, session=None, mark_replied=False,
            delivery_generation=delivery_generation,
            suppresses_feedback=(
                current_buffered is not None and current_buffered.origin.suppresses_feedback
            ),
        )
    else:
        _live_deliver, _live_state = _make_runs_live_decisions(
            bot=bot, target=target, adapter_name=adapter_name, user_id=user_id,
            group_id=group_id, is_private=is_private, fb_key=_live_fb_key,
            nickname=_passive_nickname, session=None, mark_replied=False,
            suppresses_feedback=(
                current_buffered is not None and current_buffered.origin.suppresses_feedback
            ),
        )

    result = await _chat_with_overflow_recovery(
        "passive",
        reply_bot=bot,
        reply_target=target,
        text="",
        image_urls=[],
        session_key=session_key,
        **({"delivery_generation": delivery_generation} if delivery_generation is not None else {}),
        **({"source_event_id": source_event_id} if source_event_id is not None else {}),
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
        decision_callback=(
            _live_deliver if _live_enabled else None
        ),
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
    # 多作者还原(P-09): pending_steer 剥壳后 sender 全丢, 优先按 steer_ledger
    # (scope+m_id+sender+逐行投影)找回实际回收行, 构造独立 human_replay fragments
    # 传入队列(各 sender 与顺序保留);无可靠台账时保留原文整段回收 fallback 并记
    # 明确日志。
    if result is not None and result.pending_steer:
        from ..core.steer_reconcile import _strip_convention
        _scope_key = make_inflight_key(adapter_name, is_private, user_id, group_id)
        _recovered_frags = _pending_steer_recovery_fragments(
            result.pending_steer, adapter_name=adapter_name, is_private=is_private,
            user_id=user_id, group_id=group_id, now_ms=_now_ms(),
        )
        if _recovered_frags is None:
            logger.info(
                f"[HERMES passive] pending_steer 回收无可靠台账,按原文整段回收 "
                f"(key={_scope_key} len={len(result.pending_steer)})"
            )
        _mcp.inflight.try_enter(
            _scope_key,
            BufferedMessage(
                ts=_now_ms(), adapter=adapter_name, group_id=group_id,
                user_id=user_id, nickname=(current_buffered.nickname if current_buffered is not None else user_id),
                content=_strip_convention(result.pending_steer), origin=MessageOrigin.human_replay(),
                image_urls=[], sticker_urls=[],
                reply_to_ts=None, is_bot=False,
            ),
            is_direct=True,
            original_msg_id=None,
            now_ms=_now_ms(),
            commission=True,
            fragments=_recovered_frags,
        )
        logger.info(
            f"[HERMES passive] 回收 pending_steer 进委托队列 "
            f"(key={_scope_key} len={len(result.pending_steer)})"
        )
        # 排回队列成功后按文本子串核销 pending 台账(见上方注记)。核销失败只 WARN,
        # 不影响回收主流程——对账仍以转录 m:id 搜索为最终判据双保险。限定本会话
        # scope:子串(尤其空串正文的 %%)不得核销别的会话里还等着回收的账项。
        if _mcp.run_tracker is not None:
            try:
                _mcp.run_tracker.recover_by_text(
                    result.pending_steer,
                    scope_key=make_scope_key(adapter_name, is_private, user_id, group_id),
                )
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

    if await deliver_companion_run_output(
        session_id=session_key,
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        group_id=group_id,
        is_private=is_private,
    ):
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
        if getattr(result, "streamed_decisions", None):
            # E3: 已有正文即时发出，失败兜底不得再发一条。
            logger.warning(
                f"[HERMES passive] 传输失败兜底跳过:本轮已有 "
                f"{len(getattr(result, 'streamed_decisions', None) or [])} 份即时发送 "
                f"(group={group_id} is_private={is_private})"
            )
            _settle(result, "failed", "stream_after_partial")
            return result
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

    # 纯文本正文协议：决策的 body 就是正文；空正文即静默，不合成空决策。
    # 私聊/完成态走决策通道，普通文本（群 passive）单份直发，不再从散文里抠 JSON。
    _fb_key = _feedback_scope_key(adapter_name, is_private, user_id, group_id)
    passive_batch_units = None
    streamed_unit_count = 0
    _passive_reply_text = ""
    passive_decisions: list[dict] = []
    if use_private_decision and result.structured is not None and not result.parse_failed:
        terminal_decisions = [
            item for item in (getattr(result, "terminal_decisions", None) or [])
            if isinstance(item, dict)
        ]
        streamed_decisions = [
            item for item in (getattr(result, "streamed_decisions", None) or [])
            if isinstance(item, dict)
        ]
        streamed_ids = {id(item) for item in streamed_decisions}
        # 已即时发送的份 + 终态份按到达顺序拼接；不做内容级折叠（跨段同文是两条）。
        passive_decisions = streamed_decisions or terminal_decisions or [result.structured]
        # R5:连续多份里每份的控制都要按顺序消费；静默份不消费控制。
        for _passive_decision in passive_decisions:
            if _passive_decision.get("should_reply"):
                control_intent.bind_control(_passive_decision, _fb_key)
        passive_deliverable = [
            item for item in passive_decisions
            if item.get("should_reply") and id(item) not in streamed_ids
        ]
        if not passive_deliverable and not streamed_decisions:
            logger.info(f"[HERMES passive] 终态没有正文 (user={user_id})")
            _settle(result, "silent", "empty_output")
            # 只登记了续跑、没输出正文：仍按工具语义追加继续执行轮（reactive 同口径）。
            if continuation.take_continuation(_fb_key):
                await _fire_continuation(
                    key=_fb_key, bot=bot, target=target, adapter_name=adapter_name,
                    user_id=user_id, group_id=group_id, is_private=is_private,
                )
            return result
        if getattr(result, "run_id", None):
            # E2/E3:runs 计划先落库;失败本轮零发送,不回落 legacy。
            # 份过滤/空字段口径与恢复链共用 units_from_decisions。
            from ..core.runs_terminal import (
                append_terminal_batch,
                fold_plan_units,
                units_from_decisions,
            )

            passive_pairs = units_from_decisions(passive_decisions)
            streamed_unit_count = len(units_from_decisions(streamed_decisions))
            if not passive_pairs:
                logger.info(f"[HERMES passive] 终态所有份内容为空 (user={user_id})")
                _settle(result, "silent", "empty_content")
                # 零正文轮同样消费续跑请求（reactive 同口径），否则任务静默停住。
                if continuation.take_continuation(_fb_key):
                    await _fire_continuation(
                        key=_fb_key, bot=bot, target=target, adapter_name=adapter_name,
                        user_id=user_id, group_id=group_id, is_private=is_private,
                    )
                return result
            all_units = [unit for _, unit in passive_pairs]
            try:
                if streamed_decisions:
                    append_units, _existing = fold_plan_units(
                        tracker=_mcp.run_tracker, run_id=getattr(result, "run_id", None),
                        units=all_units,
                    )
                    _batch_contexts = append_terminal_batch(
                        tracker=_mcp.run_tracker, run_id=getattr(result, "run_id", None),
                        units=append_units,
                    )
                else:
                    _batch_contexts, _passive_units = _prepare_terminal_batch_contexts(
                        result, all_units
                    )
            except TerminalPlanError as exc:
                logger.error(
                    f"[HERMES passive] runs 终态计划落库失败,本轮零发送 "
                    f"(group={group_id} run={getattr(result, 'run_id', None)}): {exc}"
                )
                _settle(result, "failed", "terminal_plan_failed")
                return result
            result.terminal_batch_contexts = _batch_contexts
            passive_batch_units = all_units
            passive_items = list(enumerate([decision for decision, _ in passive_pairs]))
        else:
            # completions：多份顺序直发，不经 Runs 台账（没有计划/对账语义）。
            passive_items = list(enumerate(passive_deliverable))
    else:
        if use_private_decision and result.parse_failed and not result.is_transport_error:
            # 正文协议失败（单份超长等）：fail-closed，本轮零发送，不猜补发。
            logger.error(
                f"[HERMES passive] 正文协议失败,本轮零发送 "
                f"reason={getattr(result, 'parse_failed_reason', '')!r} (group={group_id})"
            )
            _settle(result, "failed", getattr(result, "parse_failed_reason", "") or "parse_failed")
            return result
        _passive_reply_text = result.raw_text
        passive_items = [(0, None)]

    passive_attempted = bool(_live_state.get("sent_any"))
    passive_suppressed = bool(_live_state.get("suppressed_any"))

    async def _deliver_passive_once(reply_text: str, decision, index: int) -> None:
        nonlocal passive_attempted, passive_suppressed
        outcome = (_live_state.get("outcomes") or {}).get(id(decision))
        if passive_batch_units is not None and index < streamed_unit_count \
                and ((outcome and outcome[0] in ("sent", "suppressed"))
                     or (not outcome and not (decision or {}).get("_live_delivery_failed"))):
            # E3:该份正文已在 run 内即时发送；控制已按顺序消费，这里不重发。
            return
        # 类②拦截点(passive,规划书 §3.3):reply_text 定稿后、extract_response_media 前。
        # 命中 action=bounce 规则 → 不投递,立刻追加重交轮;缺省(无 bounce 规则)恒放行。
        # 这里不套合成轮身份豁免:重交轮自己再吐残片正是「第二次检出放行」的那一发。
        if await _maybe_intercept_bounce(
            key=_fb_key, reply_text=reply_text, bot=bot, target=target,
            adapter_name=adapter_name, user_id=user_id, group_id=group_id,
            is_private=is_private,
        ):
            if passive_batch_units is not None:
                # 业务主动禁止投递：该份记免发终态，repair 对账不得当漏件补发。
                _cancel_batch_unit(result, index, reason="bounce")
                passive_suppressed = True
                return
            _settle(result, "suppressed", "bounce")
            return

        if passive_batch_units is not None:
            # Runs 终态多份(E2):发送只消费整批计划里这一份的 body／媒体／文件。
            _unit = passive_batch_units[index]
            cleaned_text = _unit["body"]
            media_urls = list(_unit["media_urls"])
            file_paths = list(_unit["file_paths"])
        else:
            cleaned_text, extracted_media_urls, extracted_files = extract_response_media(reply_text)
            # 本轮附件只并入第一份（主答复）；后续份按正文自带的媒体引用发送。
            base_media = list(result.media_urls) if index == 0 else []
            base_files = list(result.file_paths) if index == 0 else []
            media_urls = base_media + [u for u in extracted_media_urls if u not in base_media]
            file_paths = base_files + [p for p in extracted_files if p not in base_files]

        if not cleaned_text and not media_urls and not file_paths:
            if passive_batch_units is not None:
                return
            _settle(result, "silent", "empty_content")
            return
        passive_attempted = True
        reply_to = (
            control_intent.control_value(decision, "reply_to") if decision is not None else None
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
        delivery = _terminal_delivery(
            result, index,
            cleaned_text=cleaned_text, media_urls=media_urls, file_paths=file_paths,
            reply_to=reply_to, reply_qq_message_id=reply_qq_message_id,
            reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
        )
        if delivery is None:
            # ACP 等无 Runs 台账的终态:沿用会话代际,clear 后旧代正文不得出站。
            delivery = _generation_fenced_delivery(
                result,
                adapter_name=adapter_name, is_private=is_private,
                user_id=user_id, group_id=group_id,
            )
        if passive_batch_units is not None and delivery is None:
            # 整批计划已落库却拿不到该份上下文：宁可失败也不做无台账发送。
            logger.error(
                f"[HERMES passive] 第{index}份缺少交付上下文,停止本批剩余发送 "
                f"(run={getattr(result, 'run_id', None)})"
            )
            return
        await _await_order_gate(result)
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
        _finish_history(
            result, delivery, sent,
            wrote_history=bool(is_private and sent and _mcp.message_buffer is not None),
            component_id=_history_component(index) if passive_batch_units is not None else "history",
        )
        # 分级反馈:发送成功后的文书暗示/离场确认检查(合成轮豁免)
        await _post_send_feedback(
            key=_fb_key, reply_text=cleaned_text, bot=bot, target=target,
            adapter_name=adapter_name, user_id=user_id, group_id=group_id,
            is_private=is_private,
            suppress_recursion=(current_buffered is not None and current_buffered.origin.suppresses_feedback),
            not_final=bool((decision or {}).get("not_final")),
        )
        # 文件跟在文本消息之后上传(占位符在前作说明, 文件随后到达)。
        # 带索引前缀只给 E2 批量份；非批量 legacy 计划仍是 file:<idx>。
        await send_local_files(
            bot=bot, target=target, file_paths=file_paths, adapter_name=adapter_name,
            component_prefix=_batch_file_prefix(index) if passive_batch_units is not None else "",
            **({"delivery": delivery} if delivery is not None else {}),
        )
    for _passive_index, _passive_decision in passive_items:
        if _passive_decision is not None:
            _passive_body = _decision_body(_passive_decision).strip()
        else:
            _passive_body = _passive_reply_text
        await _deliver_passive_once(_passive_body, _passive_decision, _passive_index)
    if continuation.take_continuation(_fb_key):
        # request_continuation：工具只登记，这里消费一次并追加继续执行轮；
        # 与旧 not_final 同一条 relay 链（未发送正文时同样续跑，链上限兜底）。
        await _fire_continuation(
            key=_fb_key, bot=bot, target=target, adapter_name=adapter_name,
            user_id=user_id, group_id=group_id, is_private=is_private,
        )
    if passive_batch_units is not None and not passive_attempted:
        # 整批都被业务拦截：按既有销账语义记 suppressed，禁止对账补投。
        _settle(result, "suppressed" if passive_suppressed else "silent",
                "bounce" if passive_suppressed else "should_reply_false")
    _schedule_reconcile(result)
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
        # (triggered_by / last_bot_reply_at)天然兼容;写操作
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
    _name_resolver = _speaker_name_resolver(adapter_name)
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
    if not ephemeral and chat_cfg.task_anchor_enabled:
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
        recent_messages=recent,
        rows=render_rows,
        current_image_urls=_label_groups(label_index, inlined_frames),
        current_sticker_urls=_label_groups(label_index, sticker_inlined),
        sender_blacklisted=sender_blacklisted,
        pinned_anchors=pinned_anchors,
        # 分级反馈:弹出本 scope 的插件提示便签贴进本轮(见即焚)
        # 任务锚总开关关闭:上一轮(开启时)排队的两类非法锚纠错便签不再组装进
        # user content——关闭后不继续教模型用锚;其余便签照常。
        plugin_note="\n".join(
            task_notes.pop_plugin_notes(
                _feedback_scope_key(adapter_name, False, user_id, group_id),
                drop_kinds=(
                    () if chat_cfg.task_anchor_enabled
                    else task_notes.ANCHOR_NOTE_KINDS
                ),
            )
        ) or None,
        now_ms=now_ms,
        name_resolver=_name_resolver,
        # 身份栏与 [有人@我]:与 passive 同一事实源,群/私两条真实路径一致。
        self_id=str(bot.self_id),
        adapter=adapter_name,
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
    if not await prepare_companion_run(
        session_id=session_key,
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        group_id=group_id,
        is_private=False,
    ):
        logger.warning(f"[HERMES reactive] companion 旧事件未恢复，本轮不启动新 run (group={group_id})")
        return None
    delivery_generation = (
        current_buffered.delivery_generation
        if current_buffered is not None and current_buffered.delivery_generation is not None
        else _current_delivery_generation(adapter_name, False, user_id, group_id)
    )
    source_event_id = current_buffered.source_event_id if current_buffered is not None else None
    hermes_target = resolve_target(adapter_name, False, group_id)
    # E3:轮内即时决策发送器。runs 走台账；antigravity 走无台账的 ACP 版（同一
    # outbound／队列语义，delivery_generation 栅栏照旧）；completions 保持 E1 原路。
    _live_fb_key = _feedback_scope_key(adapter_name, False, user_id, group_id)
    # 续跑请求是本轮工具调用时登记的：轮开始先清掉上一轮残留，避免陈旧标记
    # 让本轮的静默/早退分支凭空续跑一次。
    continuation.discard_continuation(_live_fb_key)
    _live_deliver, _live_state = (
        _make_acp_live_decisions(
            bot=bot, target=target, adapter_name=adapter_name, user_id=user_id,
            group_id=group_id, is_private=False, fb_key=_live_fb_key,
            nickname=self_nickname or user_id, session=session, mark_replied=True,
            delivery_generation=delivery_generation,
            suppresses_feedback=(
                current_buffered is not None and current_buffered.origin.suppresses_feedback
            ),
        )
        if plugin_config.hermes_api_interface == "antigravity"
        else _make_runs_live_decisions(
            bot=bot, target=target, adapter_name=adapter_name, user_id=user_id,
            group_id=group_id, is_private=False, fb_key=_live_fb_key,
            nickname=self_nickname or user_id, session=session, mark_replied=True,
            suppresses_feedback=(
                current_buffered is not None and current_buffered.origin.suppresses_feedback
            ),
        )
    )
    # 注:user_content_override 已携带 user message 的全部内容(text + 多模态);
    # text/image_urls 在 chat() 中会被忽略(见 hermes_client.chat 文档),此处显式传 ""
    # /[] 让契约清晰,避免被读者误以为 image_urls 也参与了构造。
    # 段内重复吞掉的可观察回执（主人 2026-11-09 批准）：Runs 同一 assistant 段的
    # interim／delta 完整单元全文匹配被吞时，给「本轮应该回复的用户触发消息」贴一次
    # 表情，作为真实使用中的快速可见验证。只接 onebotv11 群聊 reactive（本函数）
    # 且有 trigger_msg_id 的路径；私聊不调用（NapCat 私聊 set_msg_emoji_like 外层
    # retcode=0 但内部 result=65011，属能力边界，不把外层 0 当贴上）。
    # 不是给 bot 自己消息贴，也不另发表情消息；unsealed_delta_tail／跨段同文／
    # 终态重复不贴。每轮最多一次，失败只记日志，不影响正文。
    _suppressed_hook = _segment_duplicate_emoji_hook(
        bot=bot,
        trigger_msg_id=trigger_msg_id,
        group_id=group_id,
        adapter_name=adapter_name,
        interface=plugin_config.hermes_api_interface,
    )
    # 纯文本正文协议：进度就是正文（Runs 决策通道），独立旁白通道整路停用。
    result = await _chat_with_overflow_recovery(
        "reactive",
        reply_bot=bot,
        reply_target=target,
        on_suppressed=_suppressed_hook,
        text="",
        image_urls=[],
        session_key=session_key,
        **({"delivery_generation": delivery_generation} if delivery_generation is not None else {}),
        **({"source_event_id": source_event_id} if source_event_id is not None else {}),
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
        decision_callback=(
            _live_deliver
            if plugin_config.hermes_api_interface in ("runs", "antigravity")
            else None
        ),
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
    # 多作者还原(P-09): 与 passive 路径同规——按 steer_ledger 找回实际回收行
    # 构造 fragments 传入 PendingEntry(各 sender 与顺序保留);无可靠台账走原文
    # 整段 fallback。
    # 交会去重(第 3 轮评审 R4): 原行可能还在 followup 槽里没被 pop(它的 steer
    # HTTP 还没回来)。这时回收副本和原行都会进下一轮 → 同一原行两次。按台账
    # m_id 把仍在槽里的原行认出来(见 add_recovered_followup),不追加副本。
    if result is not None and result.pending_steer:
        from ..core.steer_reconcile import _strip_convention
        _recovered_rows = _pending_steer_recovery_rows(
            result.pending_steer, adapter_name=adapter_name, is_private=False,
            user_id=user_id, group_id=group_id, now_ms=_now_ms(),
        )
        _recovered_frags = (
            None if _recovered_rows is None
            else [fragment for _m_id, fragment in _recovered_rows]
        )
        if _recovered_frags is None:
            logger.info(
                f"[HERMES reactive] pending_steer 回收无可靠台账,按原文整段回收 "
                f"(group={group_id} len={len(result.pending_steer)})"
            )
        _mcp.inflight.add_recovered_followup(
            make_inflight_key(adapter_name, False, user_id, group_id),
            PendingEntry(
                msg=BufferedMessage(
                    ts=_now_ms(), adapter=adapter_name, group_id=group_id,
                    user_id=user_id, nickname=nickname or user_id,
                    content=_strip_convention(result.pending_steer), origin=MessageOrigin.human_replay(),
                    image_urls=[], sticker_urls=[],
                    reply_to_ts=None, is_bot=False,
                ),
                is_direct=False,
                # 迟到 steer 的出生轮语义:随条目走(接力改读
                # 条目自己的值后,这里同步盖出生轮的 ephemeral——冷窗关键词轮里上浮的
                # steer 仍按无窗跑,不因接力方有窗而切换跑法,避免交接到无窗轮被吞)。
                ephemeral=ephemeral,
                # None 由 __post_init__ 播种成 [msg 独立副本](旧行为不变)。
                fragments=_recovered_frags,  # type: ignore[arg-type]
            ),
            known_m_ids=(
                None if _recovered_rows is None
                else [m_id for m_id, _fragment in _recovered_rows]
            ),
        )
        logger.info(
            f"[HERMES reactive] 回收 pending_steer 进 pending "
            f"(group={group_id} len={len(result.pending_steer)})"
        )
        # 排回队列成功后按文本子串核销 pending 台账(见上方注记)。核销失败只 WARN,
        # 不影响回收主流程——对账仍以转录 m:id 搜索为最终判据双保险。限定本会话
        # scope:子串(尤其空串正文的 %%)不得核销别的会话里还等着回收的账项。
        if _mcp.run_tracker is not None:
            try:
                _mcp.run_tracker.recover_by_text(
                    result.pending_steer,
                    scope_key=make_scope_key(adapter_name, False, user_id, group_id),
                )
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
        # 原 run 已登记且仍在恢复链中；当前轮不发错误兜底提醒。
        # 上方水位可推进：网关已接纳本轮内容，slot 释放后由 retry 续订原 run。
        logger.info(f"[HERMES reactive] runs 订阅断开,等待原 run 恢复 (group={group_id})")
        return result

    if await deliver_companion_run_output(
        session_id=session_key,
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        group_id=group_id,
        is_private=False,
    ):
        return result

    if result.parse_failed or result.structured is None:
        if getattr(result, "streamed_decisions", None):
            # E3: 已有正文在 run 内即时发出；失败/残段不得再走 raw_text 兜底
            # （那会把同一段话说第二遍）。只销账，不再发送。
            logger.warning(
                f"[HERMES reactive] 失败兜底跳过:本轮已有 {len(getattr(result, 'streamed_decisions', None) or [])} 份"
                f"即时发送 (group={group_id} reason={getattr(result, 'parse_failed_reason', '')!r})"
            )
            _settle(result, "failed", "stream_after_partial")
            return result
        if getattr(result, "parse_failed_reason", ""):
            # 正文协议失败（单份超长等）：fail-closed，本轮零发送，不猜补发。
            logger.error(
                f"[HERMES reactive] 正文协议失败,本轮零发送 "
                f"reason={getattr(result, 'parse_failed_reason', '')!r} (group={group_id})"
            )
            _settle(result, "failed", getattr(result, "parse_failed_reason", "") or "parse_failed")
            return result

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

        # raw_text 预览:用来分类失败模式(裸文本 / fenced / 旧 JSON 残骸等)。
        raw = result.raw_text or ""
        # 纯散文兜底（非结构化接口，例如群 completions／astrbot 旧协议）：
        # 这段文本本身就是模型要对用户说的话，原样发送，不从中抠 JSON。
        # transport / persistence 错误的 raw 是服务端报文，绝不当散文转发。
        is_pure_prose = bool(raw) and not result.is_transport_error
        forward_raw = is_pure_prose and (is_direct or not raw.strip().startswith("{"))
        raw_preview = preview_raw(raw)
        logger.warning(
            f"[HERMES reactive] structured parse failed (group={group_id}, "
            f"transport_error={result.is_transport_error}); fallback="
            f"{('raw_text' if is_direct else 'prose') if forward_raw else 'silent'}; "
            f"raw_len={len(raw)} raw_preview={raw_preview!r}"
        )
        # 显式触发降级发 raw_text;非显式触发仅在纯散文时发,其余静默
        if forward_raw:
            cleaned_text, extracted_media_urls, extracted_files = extract_response_media(raw)
            media_urls = list(result.media_urls) + [u for u in extracted_media_urls if u not in result.media_urls]
            fallback_files = list(result.file_paths) + [p for p in extracted_files if p not in result.file_paths]
            delivery = _prepare_delivery(
                result, cleaned_text=cleaned_text, media_urls=media_urls, file_paths=fallback_files
            )
            # 本部署补丁(问题5): 散文兜底恒不 @——
            # 信封破了拿不到决策,没有任何 @ 依据,顺带消掉「兜底答错」。
            await _await_order_gate(result)
            prose_sent = await send_text_with_media(
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
            _finish_history(result, delivery, prose_sent, wrote_history=False)
            _schedule_reconcile(result)
        else:
            # 无正文可发（空输出）:销账,监控不得把它当丢失补发。
            _settle(result, "silent", "parse_failed")
        return result

    logger.info(
        f"[HERMES reactive] decision adapter={adapter_name} group={group_id} user={user_id} "
        f"direct={is_direct} should_reply={result.structured.get('should_reply')} "
        f"should_exit_active={result.structured.get('should_exit_active')} "
        f"reply_text_len={len(str(result.structured.get('body') or ''))} "
        f"salvaged={result.structured.get('_salvaged', False)}"
    )
    # 解析成功时也留一份 raw 预览:decision 摘要看不出模型到底吐了什么形状
    # (是完整信封,还是只有 {"body": ""} 这种静默份,或旧单对象 {"should_reply": false} 退化输出),
    # 而「该回却静默」的排查恰恰全靠这个区分。DEBUG 级,正常运行不刷日志。
    raw_preview = preview_raw(result.raw_text or "")
    logger.debug(f"[HERMES reactive] decision raw (len={len(result.raw_text or '')}): {raw_preview!r}")

    # 同一事实两处要用:静默分支判「是不是 push 已答」、send 分支做同 turn 去重。
    # chat() 返回后到这里没有 await,快照一次即可。多份终态按整批入口快照判定,
    # 不把本轮自己发出的前几份当成本轮的 mid-turn push。
    # E3: 流式即时发送会推进 last_bot_reply_at；以最后一次即时发送为基线，
    # 只有其后的外部 push 才算 mid-turn push，不把本轮自己的前几份当外部答复。
    mid_turn_push = session.last_bot_reply_at > max(
        last_bot_reply_at_at_entry, int(_live_state.get("last_reply_at") or 0)
    )

    terminal_decisions = [
        item for item in (getattr(result, "terminal_decisions", None) or [])
        if isinstance(item, dict)
    ]
    streamed_decisions = [
        item for item in (getattr(result, "streamed_decisions", None) or [])
        if isinstance(item, dict)
    ]
    streamed_ids = {id(item) for item in streamed_decisions}
    # 已即时发送的份 + 终态份按到达顺序就是本轮完整决策序列；
    # 不做内容级折叠（跨段同文是两条独立消息）。
    decisions = streamed_decisions or terminal_decisions
    if not decisions and isinstance(result.structured, dict):
        decisions = [result.structured]
    # 只有 runs 有交付台账：其它接口按单份 legacy 发送，不伪造批次计划。
    batch_mode = bool(getattr(result, "run_id", None)) and bool(
        streamed_decisions or terminal_decisions
    )
    if not any(decision.get("should_reply") for decision in decisions):
        # 纯文本协议：没有正文就是静默，不合成空决策、不消费控制。
        logger.info(f"[HERMES reactive] 无正文,静默 (group={group_id} user={user_id})")
        _settle(result, "silent", "empty_output")
        if continuation.take_continuation(_live_fb_key):
            # 只登记了续跑、没输出正文：仍按工具语义追加继续执行轮。
            await _fire_continuation(
                key=_live_fb_key, bot=bot, target=target, adapter_name=adapter_name,
                user_id=user_id, group_id=group_id, is_private=False,
            )
        return result
    # 控制队列：终态/无即时发送器时在这里按份补齐绑定（即时发送过的份已在发送器里
    # 按同一顺序消费过，bind_control 幂等，不会重弹）。一份决策弹一份控制。
    _control_key = make_inflight_key(adapter_name, False, user_id, group_id)
    for _decision in decisions:
        control_intent.bind_control(_decision, _control_key)
    # 轮级退窗：本组任一明确决策带了 should_exit_active，就整组生效一次，且必须在
    # 组内**最后一条发出之后**才真正收窗。逐份立即收窗会被后续份的「发言即开窗」
    # 重新顶开（世代号 +1 后旧世代收窗也会失效），用户侧表现为「说要走却没走」。
    # 只延后执行时机，不把该字段继承给其他份；reply_to/锚仍各按原份一次性生效。
    group_exit_requested = any(
        control_intent.control_value(decision, "should_exit_active")
        for decision in decisions
    )
    exit_window_closed = False

    def _close_group_exit_window(branch: str) -> None:
        """轮级退窗的唯一执行点：整组末条之后，或早退分支收口时。

        世代保护不变：只对本 turn 入口快照的那个世代生效；期间若有别人 @bot
        续期过窗口，世代号已 +1，旧世代的收窗请求不再匹配，新窗口留给自己的
        pending/refire。幂等：同一 turn 只尝试一次，早退分支与正常尾部共用。
        """
        nonlocal exit_window_closed
        if exit_window_closed or not group_exit_requested:
            return
        exit_window_closed = True
        manager = _mcp.active_sessions
        if manager is None:  # 活跃窗管理器未就绪:没有可收的窗
            return
        if not manager.end_if_current_generation(session, turn_generation):
            logger.info(
                f"[HERMES reactive] should_exit_active not applied ({branch}): window was "
                f"re-triggered or already gone (group={group_id})"
            )

    # R1:控制副作用与正文发送分离——所有有效决策都先做锚/收窗等轮级控制，
    # 空 body 只跳过发送；不得因过滤空份把本来在发送判断前执行的控制一起丢掉。
    unit_of: dict[int, dict] = {}
    batch_contexts = None
    batch_units = None
    if batch_mode:
        from ..core.runs_terminal import (
            append_terminal_batch,
            fold_plan_units,
            units_from_decisions,
        )

        # 份过滤／空字段口径与恢复链、对账链共用同一计划器。
        pairs = units_from_decisions(decisions)
        unit_of = {id(decision): unit for decision, unit in pairs}
        if pairs:
            all_units = [unit for _, unit in pairs]
            try:
                if streamed_decisions:
                    # E3: 已即时发送的前缀份不重落计划。以**已落计划**为准折叠
                    # 终态份，只追加计划尾部没有的份：即时发送中途失败留下的
                    # pending 份会被复用，不会因重算序号被追加成重复份。
                    append_units, _existing = fold_plan_units(
                        tracker=_mcp.run_tracker, run_id=getattr(result, "run_id", None),
                        units=all_units,
                    )
                    batch_contexts = append_terminal_batch(
                        tracker=_mcp.run_tracker, run_id=getattr(result, "run_id", None),
                        units=append_units,
                    )
                    batch_units = all_units
                else:
                    # E2:runs 终态先把整批计划(全部主消息／历史／文件组件)落库,
                    # 失败本轮零发送。
                    batch_contexts, batch_units = _prepare_terminal_batch_contexts(
                        result, all_units
                    )
            except TerminalPlanError as exc:
                logger.error(
                    f"[HERMES reactive] runs 终态计划落库失败,本轮零发送 "
                    f"(group={group_id} run={getattr(result, 'run_id', None)}): {exc}"
                )
                _settle(result, "failed", "terminal_plan_failed")
                return result
            result.terminal_batch_contexts = batch_contexts
    sent_any = bool(_live_state.get("sent_any"))
    suppressed_any = bool(_live_state.get("suppressed_any"))

    async def _apply_sent_controls(
        decision: dict, sent, cleaned_reply_text: str, media_urls: list,
        reply_now_ms: int, *, write_history: bool,
    ) -> None:
        """正文成功发出后的轮级控制(E1/E2 既定语义,delta/终态共用)。

        E3/R5/R6:流式即时发送只提前了正文与历史落账;发言开窗/touch/
        mark_bot_replied 与锚 pin/merge/answer/release 仍按终态时机执行一次,
        与 terminal-only 完全同口径(should_exit_active 优先、来源/权限/世代闸门
        不变)。live 份 write_history=False:历史已在即时发送时落账,不重复写。
        """
        if not sent or _mcp.message_buffer is None:
            return
        # 生效控制：submit_control 登记值兜底，旧信封控制行原样优先。
        ctl = control_intent.resolve_control(decision)
        # 任务锚总开关关闭：锚字段在本轮消费点直接丢弃，不产生任何锚副作用
        # （不 merge、不钉、不标应答、不松）；reply_to / 退窗等非锚控制照常。
        # 存量锚数据不动，重新开启后按原规则恢复。
        if not chat_cfg.task_anchor_enabled:
            ctl, dropped_anchor_fields = control_intent.neutralize_anchor_fields(ctl)
            if dropped_anchor_fields:
                logger.info(
                    f"[HERMES reactive] task anchors disabled: ignored {dropped_anchor_fields} "
                    f"(group={group_id} user={user_id})"
                )
        # 发言即开窗(窗口生命周期定案):隐式激活的临时 turn 真说了话 → 开启活跃
        # 窗口,群内跟帖可被听见。should_exit_active 优先——LLM 明说了「这轮谈完了」,
        # 不得借发言把窗又顶开。开窗后 touch / mark_bot_replied 都有了落点。
        # 本组带退窗时同样不开:否则后续份的 trigger() 会把世代号 +1,轮末按入口
        # 世代收窗就落空,整组退窗被自己的连发重新顶开。
        if ephemeral and not ctl["should_exit_active"] and not group_exit_requested:
            _mcp.active_sessions.trigger(adapter_name, group_id, user_id, now_ms=reply_now_ms)
            logger.info(f"[HERMES reactive] ephemeral turn spoke; active window opened (group={group_id})")
        if write_history:
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
                    qq_message_id=getattr(sent, "primary_qq_message_id", None),
                )
            )
        # 注:本轮带 should_exit_active 时,窗口在整组末条发出后的统一收尾里 end();
        # touch / mark_bot_replied 在此之前仍有落点(收窗已延后到组末)。
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
        if not marked and not ctl["should_exit_active"] and not group_exit_requested:
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
        anchor_merge_raw = ctl["anchor_merge"]
        if anchor_merge_raw and not ctl["should_exit_active"]:
            candidate = str(anchor_merge_raw)
            if candidate in _mcp.active_sessions.pinned_commissioners(adapter_name, group_id):
                merge_starter = candidate
            else:
                task_notes.push_plugin_note(
                    _feedback_scope_key(adapter_name, False, user_id, group_id),
                    "anchor-merge-invalid",
                    f"你上一轮填的 anchor_merge={candidate} 不在在钉锚发起者集合，已忽略；"
                    "要归并请填在钉任务的发起者 id",
                    cooldown_sec=0,
                )
                logger.info(
                    f"[HERMES reactive] anchor_merge invalid: {candidate!r} (group={group_id})"
                )
        if (
            merge_starter is None
            and ctl["pin_anchor"] is True
            and not ctl["should_exit_active"]
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
        elif ctl["pin_anchor"] is True and not ctl["should_exit_active"]:
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
        answer_anchor = ctl["answer_anchor"]
        if answer_anchor and not ctl["should_exit_active"]:
            who = "all" if answer_anchor is True else str(answer_anchor)
            valid_ids = _mcp.active_sessions.pinned_commissioners(adapter_name, group_id)
            if who != "all" and who not in valid_ids:
                task_notes.push_plugin_note(
                    _feedback_scope_key(adapter_name, False, user_id, group_id),
                    "answer-anchor-invalid",
                    f"你上一轮填的 answer_anchor={who} 不在当前在钉锚委托人集合"
                    f"（{sorted(valid_ids) or '空'}），已忽略；要标应答请填在钉委托人的 id 或 all",
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
        release_anchor = ctl["release_anchor"]
        if release_anchor:
            who = "all" if release_anchor is True else str(release_anchor)
            released = _mcp.active_sessions.release_anchor(adapter_name, group_id, who)
            if released:
                logger.info(
                    f"[HERMES reactive] anchor released: who={who} count={released} (group={group_id})"
                )

    send_index = 0
    live_sent = False
    for decision in decisions:
        _fb_key = _feedback_scope_key(adapter_name, False, user_id, group_id)
        # E3(无台账后端, antigravity ACP):正文已在轮内即时发送；终态只补轮级控制
        # 与历史/收口，不得重发同一份正文。即时发送失败的份落到正常发送路径重试。
        if not batch_mode and id(decision) in streamed_ids:
            outcome = (_live_state.get("outcomes") or {}).get(id(decision))
            if outcome and outcome[0] == "sent" and outcome[1]:
                await _apply_sent_controls(
                    decision, outcome[1], _decision_body(decision).strip(), [],
                    _now_ms(), write_history=False,
                )
                continue
            if not decision.get("_live_delivery_failed"):
                continue
        # release_anchor 处理移在 answer_anchor 之后(alive/103):同轮 answer+release
        # 必须先让合法 answer 标记生效再松锚,否则锚被提前 release 后 answer 校验
        # 落空,模型反收「非法值」便签。见 turn 尾部。
        # should_exit_active 是轮级控制:这里只记录(见 group_exit_requested),
        # 真正收窗在整组最后一条发出后的统一收尾执行——逐份立即收窗会被后续份
        # 的「发言即开窗」顶开。

        # chat() 返回后到这里没有 await,快照一次即可(见循环入口的整批快照)。

        if batch_mode:
            _unit = unit_of.get(id(decision))
            if _unit is None:
                # 静默／空 body 份:只做上面的控制,不占用发送序号。
                if not decision.get("should_reply"):
                    logger.info(
                        f"[HERMES reactive] silent: LLM decided should_reply=false "
                        f"(group={group_id} user={user_id} direct={is_direct} "
                        f"addressed={addressed_to_bot} answered_mid_turn={mid_turn_push})"
                    )
                continue
            unit_index = send_index
            send_index += 1
            outcome = (_live_state.get("outcomes") or {}).get(id(decision))
            live_sent = id(decision) in streamed_ids and bool(
                (outcome and outcome[0] in ("sent", "suppressed"))
                or (not outcome and not decision.get("_live_delivery_failed"))
            )
            if live_sent:
                # E3/R5/R6:正文已在 run 内即时发送，终态只跳过重复正文与历史落账；
                # 既定轮级控制(发言开窗/touch/mark/锚 answer/release)仍按终态
                # 时机执行一次。被 bounce／发送失败的即时份不施控制(与终态被拦截同口径)。
                outcome = (_live_state.get("outcomes") or {}).get(id(decision))
                sent = outcome[1] if outcome and outcome[0] == "sent" else None
                if not sent:
                    continue
                reply_text = _unit["body"]
                cleaned_reply_text = _unit["body"]
                media_urls = list(_unit["media_urls"])
                file_paths = list(_unit["file_paths"])
                delivery = None
                await _apply_sent_controls(
                    decision, sent, cleaned_reply_text, media_urls, _now_ms(),
                    write_history=False,
                )
                continue
            # Runs 终态多份(E2):发送只消费整批计划里这一份的 body／媒体／文件。
            reply_text = _unit["body"]
            cleaned_reply_text = _unit["body"]
            media_urls = list(_unit["media_urls"])
            file_paths = list(_unit["file_paths"])
        else:
            if not decision.get("should_reply"):
                # 显式触发 + LLM 选择沉默是「看起来该回但没回」最常见的来源,
                # 静默是设计功能不是异常:被点名(addressed_to_you: true
                # 已进 prompt)却静默只是模型否掉了插件给的既定事实,不构成需要排查的形状,
                # 与「本 turn 已由 MCP push 答过的防重复静默」同级,统一 INFO 留痕即可。
                logger.info(
                    f"[HERMES reactive] silent: LLM decided should_reply=false "
                    f"(group={group_id} user={user_id} direct={is_direct} "
                    f"addressed={addressed_to_bot} answered_mid_turn={mid_turn_push})"
                )
                continue
            unit_index = 0
            reply_text = _decision_body(decision).strip()
            cleaned_reply_text, extracted_media_urls, file_paths = extract_response_media(reply_text)
            # 结构化出口与 passive 同口径:模型本轮附件由 client 层下载进 result.media_urls,
            # reply_text 里通常只有一个占位或完全没有媒体引用——只从 reply_text 抽会把已经
            # 下载好的附件整批丢掉(纯图回复更会被下面的空闸门直接吞掉)。合并顺序与去重
            # 规则对齐 passive 路径。多份时本轮附件只并入第一份(主答复)。
            media_urls = _merge_reply_media(result.media_urls, extracted_media_urls)
        if not cleaned_reply_text and not media_urls and not file_paths:
            if batch_mode:
                _cancel_batch_unit(result, unit_index, reason="empty_content")
                continue
            # 旧 completions 单份早退也要收窗:父版本本份进循环即收,
            # 若这里直接 return,should_exit_active 会静默丢失。
            _close_group_exit_window("empty_content")
            _settle(result, "silent", "empty_content")
            return result

        # B.3: 同 turn 内防重复闸门 — 若 chat() agent loop 期间 last_bot_reply_at
        # 已被推进(即 push_message 在中途答过一次), 抑制本路 submit_decision 的 send。
        # 该闸对显式触发同样生效:同 turn 双答属纯重复,与触发性质无关。
        # 注: mid_turn_push 直接读 `session.last_bot_reply_at` 而非重新查 active_sessions ──
        # mark_bot_replied 是对同一 dataclass 实例原地写, session 变量持有的就是那个实例。
        # 不查 active_sessions 也回避了 TTL 边界判定与 ended-and-retriggered 罕见竞态。
        mid_turn_action = _mid_turn_action(
            mid_turn_push, session.last_bot_reply_text, session.last_bot_reply_media,
            cleaned_reply_text, media_urls,
        )
        if mid_turn_action == "send_both":
            # 中途 push 过,但这条带的是新内容(典型:push 一句「在查」、decision 给真答案)。
            # 两条都发 —— 抑制它等于把答案吞掉,而 agent 会以为已经发出去了。
            logger.info(
                f"[HERMES reactive] mid-turn push detected but submit_decision adds new content; "
                f"sending both (group={group_id} push_len={len(session.last_bot_reply_text)} "
                f"reply_text_len={len(cleaned_reply_text)})"
            )
        elif mid_turn_action == "suppress":
            logger.info(
                f"[HERMES reactive] suppress submit_decision reply: it restates the mid-turn push "
                f"(group={group_id} user={user_id} direct={is_direct} "
                f"reply_text_len={len(reply_text)} mid_turn_media={session.last_bot_reply_media})"
            )
        elif mid_turn_action == "media_only":
            logger.info(
                f"[HERMES reactive] mid-turn push had no media; deliver submit_decision media only "
                f"(group={group_id} media={len(media_urls)})"
            )
        if unit_index == 0 and await _handle_mid_turn_push(
            action=mid_turn_action,
            send_text=lambda delivery: send_text_with_media(
                bot=bot, target=target, text="", media_urls=media_urls, adapter_name=adapter_name,
                **({"delivery": delivery} if delivery is not None else {}),
            ),
            prepare_delivery=lambda: _terminal_delivery(
                result, unit_index, cleaned_text="", media_urls=media_urls,
                file_paths=[], reply_to=control_intent.control_value(decision, "reply_to"),
            ),
            mark_replied=lambda: _mcp.active_sessions.mark_bot_replied(
                adapter_name, group_id, now_ms=_now_ms(), media_count=len(media_urls), text=""
            ),
            finish_history=lambda delivery, sent: _finish_history(
                result, delivery, sent, wrote_history=False,
                component_id=(
                    _history_component(unit_index) if batch_mode else "history"
                ),
            ),
            schedule_reconcile=lambda: _schedule_reconcile(result),
            settle_suppressed=lambda: (
                _cancel_batch_unit(result, unit_index, reason="external_delivery")
                if batch_mode else _settle(result, "suppressed", "external_delivery")
            ),
        ):
            if batch_mode:
                if mid_turn_action == "media_only":
                    # 媒体已按该份组件发出；不算被拦截，也不额外销账。
                    sent_any = True
                else:
                    suppressed_any = True
                continue
            # 单份早退同样要收窗(见 empty_content 分支)。
            _close_group_exit_window("mid_turn_push")
            return result

        # 正文内联 @ 保持原顺序；Reply 另从同一条 m:id 记录取平台 ID 和真实发送者，
        # 群聊按 QQ 原生段序在 Reply 后补被引用者 At，私聊不补。无映射仍降级普通正文。
        resolved_reply = resolve_reply(
            _mcp.message_buffer,
            adapter=adapter_name,
            group_id=group_id,
            owner_user_id=None,
            reply_to=control_intent.control_value(decision, "reply_to"),
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
            if batch_mode:
                # 业务主动禁止投递：该份记免发终态，repair 对账不得当漏件补发。
                _cancel_batch_unit(result, unit_index, reason="bounce")
                suppressed_any = True
                continue
            # 旧 completions 单份 bounce 早退:收窗语义同父版本(不可丢 should_exit_active)。
            _close_group_exit_window("bounce")
            _settle(result, "suppressed", "bounce")
            return result

        delivery = _terminal_delivery(
            result, unit_index,
            cleaned_text=cleaned_reply_text, media_urls=media_urls, file_paths=file_paths,
            reply_to=control_intent.control_value(decision, "reply_to"), reply_qq_message_id=reply_qq_message_id,
            reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
        )
        if delivery is None:
            # ACP 等无 Runs 台账的终态:沿用会话代际,clear 后旧代正文不得出站。
            delivery = _generation_fenced_delivery(
                result,
                adapter_name=adapter_name, is_private=False,
                user_id=user_id, group_id=group_id,
            )
        if batch_mode and delivery is None:
            # 整批计划已落库却拿不到该份上下文：宁可失败也不做无台账发送。
            logger.error(
                f"[HERMES reactive] 第{unit_index}份缺少交付上下文,停止本批剩余发送 "
                f"(run={getattr(result, 'run_id', None)})"
            )
            _close_group_exit_window("delivery_context_missing")
            return result
        sent_any = True
        await _await_order_gate(result)
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
        # 带索引前缀只给 E2 批量份；非批量 legacy 计划仍是 file:<idx>。
        await send_local_files(
            bot=bot, target=target, file_paths=file_paths, adapter_name=adapter_name,
            component_prefix=_batch_file_prefix(unit_index) if batch_mode else "",
            **({"delivery": delivery} if delivery is not None else {}),
        )
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
        # 既有控制(发言开窗/touch/mark)在共享 helper 内复用同一个 reply_now_ms 快照,
        # 既校正 stale 入参,又避免多次读时钟在字段间引入毫秒级偏差。
        await _apply_sent_controls(
            decision, sent, cleaned_reply_text, media_urls, _now_ms(),
            write_history=not live_sent,
        )
        if not live_sent:
            _finish_history(
                result, delivery, sent,
                wrote_history=bool(sent and _mcp.message_buffer is not None),
                component_id=_history_component(unit_index) if batch_mode else "history",
            )
    # 轮级退窗的统一收尾：整组所有份都发完（含 live 已发份与空 body 份）之后才收。
    # 早退分支已在各自出口调用同一收窗点；世代保护见 _close_group_exit_window。
    _close_group_exit_window("group_done")
    if continuation.take_continuation(_live_fb_key):
        # request_continuation：本轮正文发完后消费一次，追加继续执行轮（同 relay 链）。
        await _fire_continuation(
            key=_live_fb_key, bot=bot, target=target, adapter_name=adapter_name,
            user_id=user_id, group_id=group_id, is_private=False,
        )
    if not sent_any:
        # 全部份都没有发送(静默／全部被业务拦截)：按既有销账语义收口，禁止对账补投。
        _settle(
            result,
            "suppressed" if suppressed_any else "silent",
            "bounce" if suppressed_any else "should_reply_false",
        )
    _schedule_reconcile(result)
    return result


# 同层互调(turns↔relay)与 turns→feedback 的顶部 import 会构成
# feedback→relay→turns 加载环;统一置底,由入口层 message.py 首载本域、
# 全部 def 就位后再解析。
from .message_feedback import (
    _emoji_like_quiet,
    _feedback_scope_key,
    _fire_continuation,
    _make_approval_notify,
    _fire_followup_turn,
    _maybe_intercept_bounce,
    _post_send_feedback,
)
from .message_relay import (
    _handle_passive_path,
    _handle_reactive_path,
)