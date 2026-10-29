"""runs 半接管交付台账的规划与业务销账（本部署补丁，alive/108）。

提交 B 只提供纯函数/helper：由 message_turns 与 run_resume 在既有发送
路径上调用。监控/plan 自身异常绝不阻断 legacy 发送；未入账 run 一律安全
返回 None／无操作，不伪造台账。observe/repair 的 reconcile 在提交 C 接入。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from nonebot import logger

from .outbound import DeliveryContext


def _sha256_json(value) -> str:
    canonical = json.dumps(value, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _file_plan_entry(raw: str) -> dict:
    """有序文件计划：路径、文件名、大小与内容哈希（取不到时显式 None）。"""
    entry = {"path": str(raw), "name": Path(raw).name, "size": None, "sha256": None}
    try:
        path = Path(raw)
        data = path.read_bytes()
        entry["size"] = len(data)
        entry["sha256"] = hashlib.sha256(data).hexdigest()
    except OSError:
        pass
    return entry


def prepare_outbound_delivery(
    *,
    tracker,
    run_id: str | None,
    cleaned_text: str,
    media_urls,
    file_paths,
    reply_to: str | None = None,
    reply_qq_message_id: str | int | None = None,
    reply_at_user_id: str | int | None = None,
) -> DeliveryContext | None:
    """在最终输出确定后、出站前生成投递计划与组件，返回 primary context。

    文件组件按原列表位置派生 file:<index>，由 send_local_files 逐个认领。
    计划首次形成后不得变形（tracker.upsert_delivery_plan 失败关闭）。
    任何监控侧异常只记告警并返回 None，legacy 发送照常进行。
    """
    if tracker is None or not run_id:
        return None
    try:
        if tracker.get_delivery(run_id) is None:
            return None
        plan = {
            "cleaned_text": cleaned_text,
            "media_urls": [str(u) for u in (media_urls or [])],
            "files": [_file_plan_entry(p) for p in (file_paths or [])],
            "reply_to": reply_to,
            "reply_qq_message_id": (
                str(reply_qq_message_id) if reply_qq_message_id is not None else None
            ),
            "reply_at_user_id": (
                str(reply_at_user_id) if reply_at_user_id is not None else None
            ),
        }
        components = [{
            "component_id": "primary",
            "kind": "primary",
            "payload_sha256": _sha256_json({
                "cleaned_text": plan["cleaned_text"],
                "media_urls": plan["media_urls"],
                "reply_qq_message_id": plan["reply_qq_message_id"],
                "reply_at_user_id": plan["reply_at_user_id"],
            }),
        }]
        for idx, entry in enumerate(plan["files"]):
            components.append({
                "component_id": f"file:{idx}",
                "kind": "file",
                "payload_sha256": _sha256_json(entry),
            })
        tracker.upsert_delivery_plan(run_id=run_id, plan=plan, components=components)
        return DeliveryContext(tracker=tracker, run_id=run_id)
    except Exception as exc:
        logger.warning(f"run={run_id} 投递计划生成失败, 本轮按 legacy 无台账发送: {exc}")
        return None


def settle_expected(
    *, tracker, run_id: str | None, expected_state: str, reason: str | None = None
) -> None:
    """业务销账：静默／抑制／取消／超期。缺身份或无台账行均安全无操作。"""
    if tracker is None or not run_id:
        return
    try:
        tracker.set_expected_state(run_id, expected_state, cancel_reason=reason)
    except Exception as exc:
        logger.warning(f"run={run_id} 业务销账失败({expected_state}/{reason}): {exc}")
