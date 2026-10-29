"""run 内被后续答复覆盖的结构化回复回收。"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any

from nonebot import logger

from .. import mcp as _mcp
from .hermes_client import _try_parse_first_json_block, extract_response_media
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
from .steer_reconcile import _fetch_transcript


@dataclass(frozen=True, slots=True)
class RecoveredReply:
    """一条只补用户可见投递、不重放决策副作用的前序回复。"""

    transcript_row_id: int
    raw_text: str
    run_id: str = ""


def extract_overwritten_replies(
    rows: list[dict[str, Any]], final_output: str
) -> list[RecoveredReply]:
    """从一次完整 run 转录中取回终态 output 之前的有效回复。

    只认以 JSON 对象开头的结构化 assistant 行，避免把讨论 JSON 的旁白
    当成决策。终态 output 必须与最后一个结构化答复原文一致，否则证据
    不足，整批不补。没有转录行号就无法持久去重，同样不补。
    """
    decisions: list[tuple[int | None, str, dict[str, Any]]] = []
    for row in rows:
        if row.get("role") != "assistant":
            continue
        content = row.get("content")
        if not isinstance(content, str) or not content.lstrip().startswith("{"):
            continue
        parsed = _try_parse_first_json_block(content)
        if not isinstance(parsed, dict) or not isinstance(
            parsed.get("should_reply"), bool
        ):
            continue
        row_id = row.get("id")
        decisions.append(
            (row_id if isinstance(row_id, int) else None, content, parsed)
        )
    if not decisions or decisions[-1][1] != final_output:
        return []

    recovered: list[RecoveredReply] = []
    for row_id, raw_text, decision in decisions[:-1]:
        if not decision.get("should_reply"):
            continue
        reply_text = decision.get("reply_text")
        if not isinstance(reply_text, str) or not reply_text.strip():
            continue
        if row_id is None:
            logger.warning("[HERMES reply-recovery] 前序答复缺转录行号，跳过补投")
            continue
        recovered.append(RecoveredReply(row_id, raw_text))
    return recovered


async def recover_run_overwritten_replies(
    tracked: Any, final_output: str
) -> list[RecoveredReply]:
    """读取完整本轮回转录；证据不完整时宁可不补，也不猜测。"""
    fetched = await _fetch_transcript(tracked)
    if fetched is None:
        logger.warning(
            f"[HERMES reply-recovery] 转录读取失败，不补投 run_id={tracked.run_id}"
        )
        return []
    rows, complete = fetched
    if not complete:
        logger.warning(
            f"[HERMES reply-recovery] 转录未覆盖本轮，不补投 run_id={tracked.run_id}"
        )
        return []
    ordered = sorted(rows, key=lambda row: row.get("id") or 0)
    recovered = extract_overwritten_replies(ordered, final_output)
    return [
        RecoveredReply(item.transcript_row_id, item.raw_text, tracked.run_id)
        for item in recovered
    ]


async def deliver_recovered_replies(
    replies: list[RecoveredReply],
    *,
    run_id: str | None = None,
    bot: Any,
    target: Any,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
) -> int:
    """按原顺序补发前序答复，只执行可见投递与投递所需的历史回写。"""
    delivered = 0
    for item in replies:
        decision = _try_parse_first_json_block(item.raw_text)
        if not isinstance(decision, dict) or not decision.get("should_reply"):
            continue
        reply_text = decision.get("reply_text")
        if not isinstance(reply_text, str) or not reply_text.strip():
            continue
        effective_run_id = item.run_id or run_id
        if not effective_run_id:
            logger.warning("[HERMES reply-recovery] 缺 run_id，不补投")
            continue
        tracker = _mcp.run_tracker
        if tracker is None:
            logger.warning(
                f"[HERMES reply-recovery] 去重台账未就绪，不补投 run_id={effective_run_id}"
            )
            continue
        if not tracker.claim_recovered_reply(
            effective_run_id, item.transcript_row_id
        ):
            continue
        try:
            cleaned, media_urls, file_paths = extract_response_media(reply_text)
            resolved = resolve_reply(
                _mcp.message_buffer,
                adapter=adapter_name,
                group_id=group_id,
                owner_user_id=user_id if is_private else None,
                reply_to=decision.get("reply_to"),
                log_tag="HERMES reply-recovery",
            )
            sent = await send_text_with_media(
                bot=bot,
                target=target,
                text=cleaned,
                media_urls=media_urls,
                reply_qq_message_id=resolved.qq_message_id if resolved else None,
                reply_at_user_id=resolved.reply_at_user_id if resolved else None,
                adapter_name=adapter_name,
            )
            files_sent = await send_local_files(
                bot=bot,
                target=target,
                file_paths=file_paths,
                adapter_name=adapter_name,
            )
            if not sent and not files_sent:
                tracker.release_recovered_reply(
                    effective_run_id, item.transcript_row_id
                )
                continue
            delivered += 1
            now_ms = int(time.time() * 1000)
            if sent and _mcp.message_buffer is not None:
                history = send_result_history(sent, cleaned)
                if media_urls:
                    history = f"{history} [图片]".strip()
                _mcp.message_buffer.append(
                    BufferedMessage(
                        ts=now_ms,
                        adapter=adapter_name,
                        group_id=group_id,
                        user_id=str(bot.self_id),
                        nickname=await get_bot_nickname(bot),
                        content=history,
                        origin=MessageOrigin.bot_output(),
                        image_urls=[],
                        is_bot=True,
                        qq_message_id=getattr(sent, "primary_qq_message_id", None),
                        private_owner_id=user_id if is_private else None,
                    )
                )
            if (
                not is_private
                and group_id is not None
                and _mcp.active_sessions is not None
            ):
                _mcp.active_sessions.mark_bot_replied(
                    adapter_name,
                    group_id,
                    now_ms=now_ms,
                    media_count=(
                        len(media_urls)
                        + files_sent
                        + send_result_sticker_count(sent)
                    ),
                    text=cleaned,
                )
            logger.info(
                f"[HERMES reply-recovery] 前序答复已补投 run_id={effective_run_id} "
                f"row_id={item.transcript_row_id}"
            )
        except Exception:
            tracker.release_recovered_reply(effective_run_id, item.transcript_row_id)
            logger.warning(
                f"[HERMES reply-recovery] 补投异常 run_id={effective_run_id} "
                f"row_id={item.transcript_row_id}",
                exc_info=True,
            )
    return delivered
