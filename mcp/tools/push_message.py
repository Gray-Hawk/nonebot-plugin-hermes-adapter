"""push_message: Hermes 反向调用,nonebot 主动发送一条消息到群或私聊。

群私双路(group_id/user_id 互斥二选一):
  - 群侧(行为不变):(adapter, group_id) 必须有活跃 reactive session
    + BotRegistry 必须有该 (adapter, group_id) 的 Target;
  - 私侧(无窗体系):不验窗;目标必须在私聊主动下行白名单(private_allow,
    唤起与直发共用),路由走「registry 优先、无 entry 合成兜底」单路径
    (与 wake_turn 唤起通道同一个解析函数),per-target 令牌桶与唤起共用同桶。
不满足条件返回 422 等价错误(由 FastMCP 序列化为 isError=true)。

成功路径的副作用, 与 reactive submit_decision 回复路径(_run_reactive_turn 末段)等价:
  1. mark_bot_replied(仅群侧) — 写 ActiveSession.last_bot_reply_at/text/media, 供同 turn
     防重与活跃窗状态在后续 reactive turn / refire 判定
  2. message_buffer.append(is_bot=True) — 让后续 turn 拉到的消息历史里能看见 bot
     这条 push 出去的话, LLM 不会"以为自己没说"
私侧无窗体系,只回写 buffer(形态与群侧同构,group_id=None 的自已条目);
两件都做才能避免 Hermes 用 push_message 当主回复后, 后续 refire / 同 turn submit_decision
又答一遍同主题。
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from nonebot import get_bot, logger
from pydantic import BaseModel, Field, field_validator, model_validator

from ...core.decision_contract import parse_reply_to
from ...core.message_buffer import BufferedMessage
from ...core.message_origin import MessageOrigin
from ...core.outbound import (
    get_bot_nickname,
    send_result_history,
    send_result_sticker_count,
    send_text_with_media,
)
from ...core.reply_resolution import resolve_reply
from ...core.routing import CallerScope
from ...core.session import session_manager
from ..auth import PushContextError, validate_push_context

if TYPE_CHECKING:
    from ...core.message_buffer import MessageBuffer


class PushMessageInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    group_id: str | None = Field(default=None, description="Group ID (mutually exclusive with user_id)")
    user_id: str | None = Field(
        default=None,
        description="Private-chat target user id (mutually exclusive with group_id; "
        "must be listed in private_allow / the admin set)",
    )
    text: str = Field(..., description="Reply text. Empty allowed only if image_urls non-empty. Inline [CQ:at,qq=数字] supports ordered multi-mention.")
    image_urls: list[str] = Field(default_factory=list, description="Image URLs")
    reply_to: str | None = Field(default=None, description="Optional message to quote, exactly m:<positive integer> from this chat")
    task_id: str | None = Field(default=None, description="(M1: 不使用,M2 bg_tasks 接入)")

    @field_validator("reply_to")
    @classmethod
    def _reply_to_uses_unified_mid(cls, value: str | None) -> str | None:
        if value is not None and parse_reply_to(value) is None:
            raise ValueError("reply_to must be exactly m:<positive integer>")
        return value

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "PushMessageInput":
        if bool(self.group_id) == bool(self.user_id):
            raise ValueError("exactly one of group_id / user_id must be given")
        return self


class PushMessageResult(BaseModel):
    ok: bool
    error: str | None = None
    warning: str | None = None
    """部分投递:文本发出去了,但有 image_urls 无法投递(见 skipped_images)。"""

    skipped_images: list[str] = Field(default_factory=list)
    """被跳过的图片引用。bot 侧只能投递 http(s) 与 data: URL;主机本地路径取不到字节。"""

    note: str | None = None
    """行为提示(非错误)。告知调用方"这一轮你已经在群里说过话了",让它在写
    submit_decision 之前就知道再回一条会是第二条消息 —— 这是唯一能在决策**之前**
    把这件事告诉 agent 的时机。"""


# push 成功后回给调用方的行为提示。写在返回值里而不是只写进 SKILL.md:agent 是在拿到
# 这个结果之后才写 submit_decision 的,这里是唯一能在它决策**之前**提醒到它的地方。
# 群侧措辞一字不动(历史行为);私聊是同语义的路数复述。
_SPOKEN_NOTE = (
    "You have now spoken in this group for this turn. If your submit_decision also carries "
    "reply_text, it will ALSO be sent as a second message. Restating what you just pushed is "
    "dropped as a duplicate — set should_reply=false unless you have something new to add."
)
_PRIVATE_SPOKEN_NOTE = (
    "You have now spoken to this user in private for this turn. If your submit_decision also "
    "carries reply_text, it will ALSO be sent as a second private message. Restating what you "
    "just pushed is dropped as a duplicate — set should_reply=false unless you have something "
    "new to add."
)

# bot 侧能真正投递的 scheme。本地路径不在其中:MCP 调用方(Hermes)与 bot 可能不同机,
# 即使同机,按调用方给的任意路径去读文件也是一条不该开的洞。
_DELIVERABLE_PREFIXES = ("http://", "https://", "data:")


def _partition_image_urls(urls: list[str]) -> tuple[list[str], list[str]]:
    """拆成 (可投递, 需跳过)。"""
    ok: list[str] = []
    skipped: list[str] = []
    for u in urls:
        (ok if u.startswith(_DELIVERABLE_PREFIXES) else skipped).append(u)
    return ok, skipped


def _resolve_private_push(adapter: str, user_id: str):
    """私侧 push 的节流 + 路由:整条复用唤起通道的单一路径,不自造同款。

    令牌桶与唤起共用同桶(键形 {adapter}:{scope}:{scope_id} 一致,同 target 唤起+
    直发合计受容量约束);路由直调 message_relay._resolve_wake_route
    (registry 优先、无 entry 合成兜底,与 wake_turn 同一个解析函数)。

    返回 (bot, target, bot_self_id);失败返回给调用方看的错误串(str)。
    """
    # 延迟导入:handlers 依赖 mcp 运行时单例,模块级互 import 会成环(wake_turn 同款)
    from ...handlers import message_relay as _relay

    denied = _relay._wake_rate_limited(adapter, "private", user_id)
    if denied is not None:
        logger.warning(f"[MCP push_message] 私聊直发被限频: {adapter}/{user_id}")
        return denied
    route = _relay._resolve_wake_route(adapter, "private", user_id)
    if isinstance(route, str):
        logger.warning(f"[MCP push_message] 私聊直发路由失败: {route}")
        return route
    bot, target = route
    return bot, target, str(getattr(bot, "self_id", "") or "")


async def push_message_impl(
    inp: PushMessageInput,
    *,
    active_sessions,
    bot_registry,
    scope: CallerScope | None,
    message_buffer: MessageBuffer | None = None,
) -> PushMessageResult:
    """scope 是调用方的可操作范围,**没有默认值**:漏传会是 TypeError 而不是静默放行。
    None = 认不出调用方,一律拒(见 auth.assert_scope_allows)。"""
    if not inp.text and not inp.image_urls:
        return PushMessageResult(ok=False, error="text and image_urls both empty")

    is_private = inp.user_id is not None
    # 互斥校验(schema model_validator + validate_push_context 双层保证)确保
    # 私侧 user_id / 群侧 group_id 恰好一个非 None,这里收窄成 str。
    target_id: str = inp.user_id or inp.group_id or ""
    gid: str = "" if is_private else (inp.group_id or "")

    now_ms = int(time.time() * 1000)
    try:
        validate_push_context(
            adapter=inp.adapter,
            group_id=inp.group_id,
            user_id=inp.user_id,
            active_sessions=active_sessions,
            bot_registry=bot_registry,
            now_ms=now_ms,
            scope=scope,
        )
    except PushContextError as exc:
        logger.warning(f"[MCP push_message] context invalid: {exc}")
        return PushMessageResult(ok=False, error=str(exc))

    if is_private:
        route = _resolve_private_push(inp.adapter, target_id)
        if isinstance(route, str):
            return PushMessageResult(ok=False, error=route)
        bot, target, bot_self_id = route
    else:
        # 防御:即使 validate 通过了,这里二次 get 之间存在理论 TOCTOU 窗口
        # (registry 没 TTL,短期内不会自动 evict,但 python -O 下 assert 会被剥除,
        # 走 if 而非 assert 保 push_message 整体错误面收敛在 PushMessageResult 里)
        entry = bot_registry.get(inp.adapter, "group", gid)
        if entry is None:
            logger.warning(
                f"[MCP push_message] registry entry disappeared after context check: {inp.adapter}/{gid}"
            )
            return PushMessageResult(
                ok=False,
                error=f"bot registry entry not found: ({inp.adapter}, {gid})",
            )

        try:
            bot = get_bot(entry.bot_self_id)
        except (KeyError, ValueError) as exc:
            logger.warning(f"[MCP push_message] bot offline self_id={entry.bot_self_id}: {exc}")
            return PushMessageResult(ok=False, error=f"bot offline: {entry.bot_self_id}")
        target = entry.target
        bot_self_id = entry.bot_self_id

    # 无法投递的引用要在这里就摘掉并如实回报:直接交给 outbound 只会被静默跳过,
    # 调用方拿到 ok=true 以为图发了,用户却什么也没看到 —— agent 通常会因此重发一遍,
    # 于是群里出现两条一样的文本。
    deliverable, skipped = _partition_image_urls(inp.image_urls)
    warning: str | None = None
    if skipped:
        logger.warning(
            f"[MCP push_message] {len(skipped)} 个 image_urls 无法投递(需要 http(s)/data:): {[u[:80] for u in skipped]}"
        )
        warning = (
            f"{len(skipped)} image(s) were NOT delivered: only http(s):// and data: URLs can be sent. "
            "A path on the Hermes host is not reachable from the bot. To send a locally generated "
            "image, put a MEDIA:<absolute path> tag in your submit_decision reply_text instead — "
            "whitelisted local images are inlined and shipped as real image messages. "
            "Do not retry this push with the same path."
        )
    if not inp.text and not deliverable:
        return PushMessageResult(
            ok=False,
            error="nothing deliverable: text empty and all image_urls unsupported",
            warning=warning,
            skipped_images=skipped,
        )

    resolved_reply = resolve_reply(
        message_buffer,
        adapter=inp.adapter,
        group_id=None if is_private else gid,
        owner_user_id=target_id if is_private else None,
        reply_to=inp.reply_to,
        log_tag="MCP push_message",
    )
    receipt_ids = await send_text_with_media(
        bot=bot,
        target=target,
        text=inp.text,
        media_urls=deliverable,
        reply_qq_message_id=resolved_reply.qq_message_id if resolved_reply else None,
        reply_at_user_id=resolved_reply.reply_at_user_id if resolved_reply else None,
        adapter_name=inp.adapter,
    )
    if not receipt_ids:
        return PushMessageResult(ok=False, error="send failed (see nonebot log)", skipped_images=skipped)
    # 一个逻辑 m:id 首版只保存主回执；普通消息恰好只有一个。forward 多分片的
    # 精确逐片寻址不伪装成已支持，后续如确有需要再另建一对多映射。
    primary_qq_message_id = getattr(receipt_ids, "primary_qq_message_id", None)

    if is_private:
        # 私侧无窗体系:无 mark_activity/touch/mark_bot_replied(都是群窗机制);
        # 只回写 buffer,形态与群侧同构(group_id=None 的自已条目)——
        # 防后续 turn 拉到历史时 LLM「以为自己没说」而重复作答。
        if message_buffer is not None:
            message_buffer.append(
                BufferedMessage(
                    ts=now_ms,
                    adapter=inp.adapter,
                    group_id=None,
                    user_id=bot_self_id,
                    # 与群侧回写同一约定:用平台账号名,别写字面 "Bot"。
                    nickname=await get_bot_nickname(bot),
                    content=send_result_history(receipt_ids, inp.text),
                    origin=MessageOrigin.bot_output(),
                    image_urls=list(deliverable),
                    is_bot=True,
                    qq_message_id=primary_qq_message_id,
                    private_owner_id=inp.user_id,
                )
            )
        return PushMessageResult(
            ok=True, warning=warning, skipped_images=skipped, note=_PRIVATE_SPOKEN_NOTE
        )

    # T1 活动基准:LLM 主动发话也算活动,防止「agent 长任务期间只 push 不说话」
    # 被空闲重置误判。群级标记,share_group / per-user 两种会话形态一起覆盖。
    session_manager.mark_activity_group(inp.adapter, gid)

    # 滑动续期。注:用 send 前的 now_ms 而非 send 后的 wall clock,
    # 慢 send(图片上传等)情况下 TTL 续期会比 wall clock 略短(<10s 量级,
    # 300s TTL 下可忽略)。如未来需要精确续期,在此重新读 time.time()。
    active_sessions.touch(inp.adapter, gid, now_ms=now_ms)

    # 与 reactive 回复路径对齐 — 写 last_bot_reply_at/text/media(供同 turn 防重),
    # 把 push 的内容注入 buffer(供后续 turn 的 <recent_messages> 看见 bot 已答)
    # media_count 记**实际投出去的**张数(不含 skipped):同 turn 去重闸门据此判断
    # 「文本已答但图还没出去」,把 submit_decision 里能投的图补发出去。
    # 带上原文:同 turn 去重据此判断 submit_decision 是不是在复述这一条。
    if not active_sessions.mark_bot_replied(
        inp.adapter, gid, now_ms=now_ms, media_count=len(deliverable) + send_result_sticker_count(receipt_ids), text=inp.text
    ):
        # push 已经发出去了,但窗口在这一刻没了(某一发 turn 返回了 should_exit_active)。
        # 后续 turn 的同 turn 去重看不到这一发,可能出现重复答复。
        logger.warning(
            f"[MCP push_message] mark_bot_replied landed nowhere: no active session "
            f"for {inp.adapter}/{gid}; same-turn dedupe loses this reply"
        )
    if message_buffer is not None:
        message_buffer.append(
            BufferedMessage(
                ts=now_ms,
                adapter=inp.adapter,
                group_id=gid,
                user_id=bot_self_id,
                # 与 reactive 回写同一约定:用平台账号名,别写字面 "Bot" —— 那行会
                # 进下一轮 <recent_messages>,角色名不叫 Bot 时读起来像另一个 bot。
                nickname=await get_bot_nickname(bot),
                content=send_result_history(receipt_ids, inp.text),
                origin=MessageOrigin.bot_output(),
                image_urls=list(deliverable),
                is_bot=True,
                qq_message_id=primary_qq_message_id,
            )
        )
    return PushMessageResult(ok=True, warning=warning, skipped_images=skipped, note=_SPOKEN_NOTE)
