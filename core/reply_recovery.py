"""run 内被后续答复覆盖的结构化回复纯取证。

本模块只回答「当前 run 有哪些前序答复有完整证据」。它不持有平台发送、
历史回写或活跃窗副作用；可见交付统一进入 alive/108 的交付计划。
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from nonebot import logger

from .. import mcp as _mcp
from .steer_reconcile import _fetch_transcript
from .hermes_client import extract_response_media
from .reply_resolution import resolve_reply


@dataclass(frozen=True, slots=True)
class RecoveredReply:
    """一条通过完整性与归属校验的前序回复。"""

    reply_seq: int
    raw_text: str
    run_id: str = ""


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _strict_decision(raw: Any) -> dict[str, Any] | None:
    """只接受单 JSON object；不做全角归一、截断救补或首尾旁白兼容。"""
    if not isinstance(raw, str):
        return None
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    if type(value.get("should_reply")) is not bool:
        return None
    if type(value.get("reply_seq")) is not int or value["reply_seq"] <= 0:
        return None
    if not isinstance(value.get("reply_context"), str):
        return None
    reply_text = value.get("reply_text")
    if value["should_reply"] and (
        not isinstance(reply_text, str) or not reply_text.strip()
    ):
        return None
    if reply_text is not None and not isinstance(reply_text, str):
        return None
    return value


def _business_value(value: dict[str, Any]) -> tuple[Any, ...]:
    """同 seq 折叠只比较回收相关业务值，不比较回合副作用字段。"""
    return (
        value["should_reply"],
        value.get("reply_text"),
        value.get("reply_to"),
        value["reply_context"],
        value["reply_seq"],
    )


def extract_overwritten_replies(
    rows: list[dict[str, Any]],
    final_output: str,
    *,
    reply_context: str,
    reply_protocol_version: int,
) -> list[RecoveredReply]:
    """提取当前 context 的 seq 1..N-1；任何证据冲突都整批失败关闭。"""
    if reply_protocol_version != 1 or not reply_context:
        return []
    final = _strict_decision(final_output)
    if final is None or final["reply_context"] != reply_context:
        return []
    final_seq = final["reply_seq"]
    if final_seq == 1:
        return []

    buckets: dict[int, list[dict[str, Any]]] = {}
    raw_by_business: dict[tuple[Any, ...], str] = {}
    for row in rows:
        if row.get("role") != "assistant":
            continue
        raw = row.get("content")
        parsed = _strict_decision(raw)
        if parsed is None:
            # 只有当前票号的污染才影响本 run；历史普通文本与旧协议行无关。
            if isinstance(raw, str) and reply_context in raw:
                logger.warning("[HERMES reply-recovery] 当前 context 存在严格解析污染，整批放弃")
                return []
            continue
        if parsed["reply_context"] != reply_context:
            continue
        seq = parsed["reply_seq"]
        bucket = buckets.setdefault(seq, [])
        if any(_business_value(item) != _business_value(parsed) for item in bucket):
            logger.warning(f"[HERMES reply-recovery] 同 seq 业务值冲突 seq={seq}，整批放弃")
            return []
        bucket.append(parsed)
        raw_by_business.setdefault(_business_value(parsed), str(raw))

    seqs = sorted(buckets)
    if any(seq > final_seq for seq in seqs):
        return []
    if seqs not in (list(range(1, final_seq + 1)), list(range(1, final_seq))):
        logger.warning(
            f"[HERMES reply-recovery] 决策序列不完整 seqs={seqs} final_seq={final_seq}，整批放弃"
        )
        return []
    if final_seq in buckets and any(
        _business_value(item) != _business_value(final)
        for item in buckets[final_seq]
    ):
        return []

    recovered: list[RecoveredReply] = []
    for seq in range(1, final_seq):
        item = buckets[seq][0]
        if item["should_reply"]:
            recovered.append(RecoveredReply(
                seq, raw_by_business[_business_value(item)]
            ))
    return recovered


async def recover_run_overwritten_replies(
    tracked: Any, final_output: str
) -> list[RecoveredReply]:
    """读取本 run 的权威身份并取证；缺身份、缺页或冲突时不产出候选。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return []
    delivery = tracker.get_delivery(tracked.run_id)
    if not delivery:
        return []
    reply_context = delivery.get("idempotency_key") or ""
    protocol_version = delivery.get("reply_protocol_version") or 0
    final = _strict_decision(final_output)
    if (
        protocol_version != 1
        or final is None
        or final["reply_context"] != reply_context
        or final["reply_seq"] == 1
    ):
        return []

    def saw_seq1(candidate_rows: list[dict[str, Any]]) -> bool:
        for row in candidate_rows:
            if row.get("role") != "assistant":
                continue
            parsed = _strict_decision(row.get("content"))
            if (
                parsed is not None
                and parsed["reply_context"] == reply_context
                and parsed["reply_seq"] == 1
            ):
                return True
        return False

    fetched = await _fetch_transcript(
        tracked,
        stop_at_user=False,
        stop_check=saw_seq1,
    )
    if fetched is None:
        logger.warning(f"[HERMES reply-recovery] 转录读取失败 run_id={tracked.run_id}")
        return []
    rows, complete = fetched
    if not complete:
        logger.warning(f"[HERMES reply-recovery] 转录分页不完整 run_id={tracked.run_id}")
        return []
    recovered = extract_overwritten_replies(
        rows,
        final_output,
        reply_context=reply_context,
        reply_protocol_version=protocol_version,
    )
    return [
        RecoveredReply(item.reply_seq, item.raw_text, tracked.run_id)
        for item in recovered
    ]


def recovery_plan_entries(
    replies: list[RecoveredReply],
    *,
    adapter: str,
    group_id: str | None,
    user_id: str,
    is_private: bool,
) -> list[dict[str, Any]]:
    """把已验证答复转成统一计划载荷；只解析本地内容，不执行投递。"""
    from .run_delivery_monitor import _file_plan_entry

    entries: list[dict[str, Any]] = []
    for item in replies:
        decision = _strict_decision(item.raw_text)
        if decision is None or not decision["should_reply"]:
            raise ValueError("unverified recovery decision")
        cleaned, media_urls, file_paths = extract_response_media(
            decision["reply_text"]
        )
        resolved = resolve_reply(
            _mcp.message_buffer,
            adapter=adapter,
            group_id=group_id,
            owner_user_id=user_id if is_private else None,
            reply_to=decision.get("reply_to"),
            log_tag="HERMES reply-recovery",
        )
        entries.append({
            "reply_seq": item.reply_seq,
            "cleaned_text": cleaned,
            "media_urls": media_urls,
            "files": [_file_plan_entry(path) for path in file_paths],
            "reply_to": decision.get("reply_to"),
            "reply_qq_message_id": resolved.qq_message_id if resolved else None,
            "reply_at_user_id": resolved.reply_at_user_id if resolved else None,
        })
    return entries
