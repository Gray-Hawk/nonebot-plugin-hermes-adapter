"""显式撤回当前 bot 已发送的一条 QQ 消息。

模型只提供逻辑 ``m:id`` 与当前聊天；平台消息号由既有身份映射在内部解析。
不做内容搜索、缺省最新或跨聊天补猜，也不在结果未知时自动重试。
"""

from __future__ import annotations

import time
from typing import Literal

from nonebot import get_bot, logger
from nonebot.adapters.onebot.v11.exception import ActionFailed, NetworkError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ...core.decision_contract import parse_reply_to
from ...core.message_buffer import MessageBuffer
from ...core.routing import CallerScope
from ..auth import PushContextError, validate_push_context

ErrorKind = Literal["local_rejected", "platform_rejected", "unconfirmed"]


class RecallMessageInput(BaseModel):
    """撤回输入：逻辑消息号必填，群／私目标二选一。"""

    model_config = ConfigDict(extra="forbid")

    adapter: str = Field(..., min_length=1)
    message_id: str = Field(..., min_length=1)
    group_id: str | None = None
    user_id: str | None = None

    @field_validator("message_id")
    @classmethod
    def validate_message_id(cls, value: str) -> str:
        if parse_reply_to(value) is None:
            raise ValueError("message_id must be 'm:<positive integer>'")
        return value

    @model_validator(mode="after")
    def validate_target(self) -> "RecallMessageInput":
        if (self.group_id is None) == (self.user_id is None):
            raise ValueError("exactly one of group_id or user_id is required")
        target = self.group_id if self.group_id is not None else self.user_id
        if target is not None and not target.strip():
            raise ValueError("target id must not be blank")
        return self


class RecallMessageResult(BaseModel):
    """模型可见结果；不包含平台消息号或原始平台异常。"""

    ok: bool
    message_id: str
    error: str | None = None
    error_kind: ErrorKind | None = None
    note: str | None = None


def _failure(inp: RecallMessageInput, kind: ErrorKind, error: str) -> RecallMessageResult:
    return RecallMessageResult(
        ok=False,
        message_id=inp.message_id,
        error=error,
        error_kind=kind,
    )


async def recall_message_impl(
    inp: RecallMessageInput,
    *,
    active_sessions,
    bot_registry,
    scope: CallerScope | None,
    message_buffer: MessageBuffer,
) -> RecallMessageResult:
    """校验权限、聊天归属和真实发送者后，执行一次平台撤回。"""

    try:
        validate_push_context(
            adapter=inp.adapter,
            group_id=inp.group_id,
            user_id=inp.user_id,
            active_sessions=active_sessions,
            bot_registry=bot_registry,
            now_ms=int(time.time() * 1000),
            scope=scope,
        )
    except PushContextError as exc:
        return _failure(inp, "local_rejected", str(exc))

    if inp.group_id is not None:
        entry = bot_registry.get(inp.adapter, "group", inp.group_id)
        if entry is None:  # validate 后防御并发路由变化，失败即闭。
            return _failure(inp, "local_rejected", "unknown target or no active bot route")
        bot_self_id = entry.bot_self_id
        try:
            bot = get_bot(bot_self_id)
        except (KeyError, ValueError):
            return _failure(inp, "local_rejected", "current bot is offline")
    else:
        # 与 push_message 一样延迟导入，避开 handlers → mcp → handlers 启动环。
        from ...handlers import message_relay

        route = message_relay._resolve_wake_route(inp.adapter, "private", inp.user_id or "")
        if isinstance(route, str):
            logger.warning(
                "recall_message private route unavailable: adapter={} user={} reason={}",
                inp.adapter,
                inp.user_id,
                route,
            )
            return _failure(inp, "local_rejected", "no active bot route for this private chat")
        bot, _target = route
        bot_self_id = str(getattr(bot, "self_id", "") or "")
        if not bot_self_id:
            return _failure(inp, "local_rejected", "current bot identity is unavailable")

    logical_id = parse_reply_to(inp.message_id)
    if logical_id is None:  # 防御直接构造 impl 或 python -O，仍失败即闭。
        return _failure(inp, "local_rejected", "message_id must be 'm:<positive integer>'")
    target = message_buffer.get_reply_target(
        inp.adapter,
        inp.group_id,
        logical_id,
        owner_user_id=inp.user_id,
    )
    if target is None:
        return _failure(
            inp,
            "local_rejected",
            "no recallable platform receipt for this m:id in the specified chat; "
            "it may be missing, cross-chat, or from an older unmapped record",
        )
    qq_message_id, sender_id = target
    if sender_id != bot_self_id:
        return _failure(inp, "local_rejected", "only messages sent by the current bot can be recalled")
    try:
        platform_message_id = int(qq_message_id)
    except (TypeError, ValueError):
        return _failure(inp, "local_rejected", "stored platform receipt is invalid; recall refused")
    if not hasattr(bot, "delete_msg"):
        return _failure(inp, "local_rejected", "current bot does not support OneBot message recall")

    try:
        await bot.delete_msg(message_id=platform_message_id)
    except ActionFailed as exc:
        logger.warning(
            "recall_message platform rejection: logical_id={} adapter={} error={}",
            logical_id,
            inp.adapter,
            type(exc).__name__,
        )
        return _failure(
            inp,
            "platform_rejected",
            "platform refused the recall; time limit, permission, or message state may apply",
        )
    except (NetworkError, TimeoutError) as exc:
        logger.warning(
            "recall_message result unconfirmed: logical_id={} adapter={} error={}",
            logical_id,
            inp.adapter,
            type(exc).__name__,
        )
        return _failure(
            inp,
            "unconfirmed",
            "recall result is unconfirmed because the platform response was not received; "
            "do not retry blindly",
        )
    return RecallMessageResult(
        ok=True,
        message_id=inp.message_id,
        note="platform accepted one recall request for the specified message",
    )
