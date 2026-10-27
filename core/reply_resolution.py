"""把 ``reply_to=m:id`` 安全解析为当前会话的平台 ID 与被引用者。

模型只接触插件自增 m:id；平台 message_id 始终留在插件内部。解析失败返回
None，由调用方按普通正文发送，绝不把模型给的字符串直接交给 OneBot。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from nonebot import logger

from .decision_contract import parse_reply_to

if TYPE_CHECKING:
    from .message_buffer import MessageBuffer


@dataclass(frozen=True)
class ResolvedReply:
    qq_message_id: str
    reply_at_user_id: str | None
    """群聊被引用消息的真实发送者；私聊恒 None，不生成自动 @。"""


def resolve_reply(
    message_buffer: MessageBuffer | None,
    *,
    adapter: str,
    group_id: str | None,
    owner_user_id: str | None,
    reply_to: object,
    log_tag: str,
) -> ResolvedReply | None:
    """返回平台 ID 与群聊首位 @ 目标；无引用或校验失败均返回 None。

    平台 ID 和发送者由同一条数据库查询取出，避免跨行拼接。群聊按
    ``adapter + group_id`` 隔离，私聊额外要求 ``owner_user_id``，且私聊即便
    解析成功也不产生自动 @。旧消息、合成消息、跨会话和归属未知的老 bot
    私聊行均失败即闭。
    """
    if reply_to is None or reply_to == "":
        return None
    message_id = parse_reply_to(reply_to)
    if message_id is None:
        logger.warning(f"[{log_tag}] reply_to 形态非法，按普通正文发送: {reply_to!r}")
        return None
    if message_buffer is None:
        logger.warning(f"[{log_tag}] reply_to=m:{message_id} 无法解析：消息库未就绪")
        return None
    target = message_buffer.get_reply_target(
        adapter,
        group_id,
        message_id,
        owner_user_id=owner_user_id if group_id is None else None,
    )
    if target is None:
        logger.warning(
            f"[{log_tag}] reply_to=m:{message_id} 无当前会话平台映射，按普通正文发送 "
            f"(adapter={adapter} group={group_id} owner={owner_user_id})"
        )
        return None
    qq_message_id, sender_user_id = target
    return ResolvedReply(
        qq_message_id=qq_message_id,
        reply_at_user_id=(sender_user_id or None) if group_id is not None else None,
    )


def resolve_reply_qq_message_id(
    message_buffer: MessageBuffer | None,
    *,
    adapter: str,
    group_id: str | None,
    owner_user_id: str | None,
    reply_to: object,
    log_tag: str,
) -> str | None:
    """兼容旧调用方：只取平台 ID；新投递链路应使用 resolve_reply。"""
    resolved = resolve_reply(
        message_buffer,
        adapter=adapter,
        group_id=group_id,
        owner_user_id=owner_user_id,
        reply_to=reply_to,
        log_tag=log_tag,
    )
    return resolved.qq_message_id if resolved is not None else None
