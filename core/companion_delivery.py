"""Runs Companion 与现有 outbound、回复解析、历史和会话窗口的薄适配层。"""

from __future__ import annotations

import json
import time
from typing import Any

from .. import mcp as _mcp
from .message_buffer import BufferedMessage
from .message_origin import MessageOrigin
from .outbound import (
    get_bot_nickname,
    send_local_files,
    send_result_history,
    send_result_sticker_count,
    send_text_with_media,
)
from .reply_resolution import resolve_reply
from .runs_companion import ComponentResult, DeliveryPlan


class OneBotComponentSender:
    """只适配投递，不实现决策语法；各组件共享现有统一 outbound。"""

    def __init__(self, *, bot, target, adapter_name: str, user_id: str,
                 group_id: str | None, is_private: bool):
        self.bot = bot
        self.target = target
        self.adapter_name = adapter_name
        self.user_id = user_id
        self.group_id = group_id
        self.is_private = is_private

    @staticmethod
    def _message_receipt(result: Any, original_text: str) -> dict[str, Any]:
        return {
            "msg_ids": list(getattr(result, "msg_ids", []) or []),
            "history": send_result_history(result, original_text),
            "sticker_media_count": send_result_sticker_count(result),
        }

    async def __call__(self, component: str, plan: DeliveryPlan) -> ComponentResult:
        if component == "file":
            sent_count = await send_local_files(
                bot=self.bot,
                target=self.target,
                file_paths=list(plan.file_paths),
                adapter_name=self.adapter_name,
            )
            receipt = {"file_count": sent_count}
            if sent_count == len(plan.file_paths):
                return ComponentResult("sent", receipt)
            # 多文件部分成功无法从聚合计数反推出已发项，禁止盲目整批重试。
            return ComponentResult("failed" if sent_count == 0 else "unknown", receipt)

        resolved = None
        if component == "text":
            resolved = resolve_reply(
                _mcp.message_buffer,
                adapter=self.adapter_name,
                group_id=self.group_id,
                owner_user_id=self.user_id if self.is_private else None,
                reply_to=plan.decision.get("reply_to"),
                log_tag="HERMES runs-companion",
            )
        original_text = plan.cleaned_text if component == "text" else ""
        result = await send_text_with_media(
            bot=self.bot,
            target=self.target,
            text=original_text,
            media_urls=list(plan.media_urls) if component == "media" else [],
            reply_qq_message_id=resolved.qq_message_id if resolved else None,
            reply_at_user_id=resolved.reply_at_user_id if resolved else None,
            adapter_name=self.adapter_name,
        )
        receipt = self._message_receipt(result, original_text)
        return ComponentResult("sent" if bool(result) else "failed", receipt)


class CompanionHistoryWriter:
    """消费端提交发送事实后，复用与旧补投一致的历史和窗口语义。"""

    def __init__(self, *, ledger, bot, adapter_name: str, user_id: str,
                 group_id: str | None, is_private: bool, message_buffer,
                 active_sessions):
        self.ledger = ledger
        self.bot = bot
        self.adapter_name = adapter_name
        self.user_id = user_id
        self.group_id = group_id
        self.is_private = is_private
        self.message_buffer = message_buffer
        self.active_sessions = active_sessions

    async def __call__(self, plan: DeliveryPlan) -> bool:
        row = self.ledger.row(plan.event.event_id)
        receipts = json.loads(row["receipts_json"])
        text_receipt = receipts.get("text", {})
        media_receipt = receipts.get("media", {})
        file_receipt = receipts.get("file", {})
        now_ms = int(time.time() * 1000)
        has_message = bool(plan.cleaned_text or plan.media_urls)
        history = str(text_receipt.get("history") or plan.cleaned_text)
        if plan.media_urls:
            history = f"{history} [图片]".strip()
        msg_ids = list(text_receipt.get("msg_ids") or media_receipt.get("msg_ids") or [])
        if has_message and self.message_buffer is not None:
            self.message_buffer.append(
                BufferedMessage(
                    ts=now_ms,
                    adapter=self.adapter_name,
                    group_id=self.group_id,
                    user_id=str(self.bot.self_id),
                    nickname=await get_bot_nickname(self.bot),
                    content=history,
                    origin=MessageOrigin.bot_output(),
                    image_urls=[],
                    is_bot=True,
                    qq_message_id=msg_ids[0] if msg_ids else None,
                    private_owner_id=self.user_id if self.is_private else None,
                )
            )
        if not self.is_private and self.group_id is not None and self.active_sessions is not None:
            self.active_sessions.mark_bot_replied(
                self.adapter_name,
                self.group_id,
                now_ms=now_ms,
                media_count=(
                    len(plan.media_urls)
                    + int(file_receipt.get("file_count") or 0)
                    + int(text_receipt.get("sticker_media_count") or 0)
                ),
                text=plan.cleaned_text,
            )
        return True


class CompanionFeedback:
    """真实投递提交后，复用旧出口的反馈、接力与后续轮语义。"""

    def __init__(self, *, bot, target, adapter_name: str, user_id: str,
                 group_id: str | None, is_private: bool):
        self.bot = bot
        self.target = target
        self.adapter_name = adapter_name
        self.user_id = user_id
        self.group_id = group_id
        self.is_private = is_private

    async def __call__(self, plan: DeliveryPlan) -> bool:
        # 延迟到真实投递时导入，避免 handlers 启动期的循环依赖。
        from ..handlers.message_feedback import _feedback_scope_key, _post_send_feedback

        await _post_send_feedback(
            key=_feedback_scope_key(
                self.adapter_name, self.is_private, self.user_id, self.group_id
            ),
            reply_text=plan.cleaned_text,
            bot=self.bot,
            target=self.target,
            adapter_name=self.adapter_name,
            user_id=self.user_id,
            group_id=self.group_id,
            is_private=self.is_private,
            not_final=bool(plan.decision.get("not_final")),
        )
        return True
