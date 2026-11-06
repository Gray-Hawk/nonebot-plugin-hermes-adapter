"""Runs 终态多份交付（E2）。

`core.npha_terminal.decode_terminal_batch()` 把 `run.completed.output` 解成 N 份
control/body；本模块只负责把整批**一次形成完整终态计划**并落库，再按份返回各自的
投递上下文。

两条硬约束（E2 卡片）：
1. 计划只由 output 派生（正文／媒体／文件／reply_to），不含发送时才解析到的
   `reply_qq_message_id` 等易变值——恢复路径重复调用必须得到逐字相同的 plan_json，
   否则 30 号 `upsert_delivery_plan` 会拒绝，旧 decisions 不得被重排。
2. 任何落库失败都抛 `TerminalPlanError`，调用方必须放弃本轮发送，绝不回落
   legacy 无台账发送（`run_delivery_monitor.prepare_outbound_delivery` 返回 None
   即放行的旧问题不得带进本链）。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .outbound import DeliveryContext


class TerminalPlanError(RuntimeError):
    """终态批量计划未落库：本轮零发送，不得回落 legacy。"""


def _sha256_json(value) -> str:
    canonical = json.dumps(value, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _file_plan_entry(raw: str) -> dict:
    """有序文件计划：路径、文件名、大小与内容哈希（取不到时显式 None）。"""
    entry = {"path": str(raw), "name": Path(raw).name, "size": None, "sha256": None}
    try:
        data = Path(raw).read_bytes()
        entry["size"] = len(data)
        entry["sha256"] = hashlib.sha256(data).hexdigest()
    except OSError:
        pass
    return entry


def _text_unit(text: str) -> dict | None:
    """纯正文 output(旧接口／散文) → unit；空内容返回 None。"""
    from .hermes_client import extract_response_media

    cleaned, media_urls, file_paths = extract_response_media(text)
    if not cleaned and not media_urls and not file_paths:
        return None
    return {
        "body": cleaned,
        "media_urls": list(media_urls),
        "file_paths": list(file_paths),
        # 空 reply_to 统一用 ""：正常链、恢复链、对账链必须落逐字相同的计划。
        "reply_to": "",
    }


def _decision_unit(decision) -> dict | None:
    """单份结构化终态 → unit；静默／空 body／无内容返回 None。

    正常链(handler)、恢复链(run_resume)、对账链(run_delivery_monitor)共用这一份
    过滤与空字段口径，避免同一 output 生成不同 plan_json 被 upsert 拒绝。
    """
    if not isinstance(decision, dict) or not decision.get("should_reply"):
        return None
    body = str(decision.get("body") or "").strip()
    if not body:
        return None
    unit = _text_unit(body)
    if unit is None:
        return None
    unit["reply_to"] = str(decision.get("reply_to") or "")
    return unit


def units_from_decisions(decisions) -> list[tuple[dict, dict]]:
    """有序 ``(decision, unit)``：保留决策身份，handler 控制副作用按份对齐。"""
    pairs: list[tuple[dict, dict]] = []
    for decision in decisions or ():
        unit = _decision_unit(decision)
        if unit is not None:
            pairs.append((decision, unit))
    return pairs


def units_from_output(output: str, expect_structured: bool) -> list[dict] | None:
    """终态 output → 有序 units；None=解析失败／无正文，[]=结构有效但全空内容。"""
    text = output or ""
    if not text.strip():
        return None
    if not expect_structured:
        unit = _text_unit(text)
        return [unit] if unit is not None else []
    from .npha_terminal import (
        TerminalFormatError,
        decode_terminal_batch,
        structured_batch_from_terminal,
    )

    try:
        decisions = structured_batch_from_terminal(decode_terminal_batch(text))
    except TerminalFormatError:
        return None
    return [unit for _, unit in units_from_decisions(decisions)]


def build_batch_plan(units: list[dict]) -> tuple[dict, list[dict]]:
    """(plan, components)：只允许 output 派生字段，保证恢复路径可重复落同一计划。

    units 每项：body / media_urls / file_paths / reply_to（均已清洗，不含附件回执）。
    """
    plan_units: list[dict] = []
    components: list[dict] = []
    for unit in units:
        entry, entry_components = _unit_plan_entry(unit, len(plan_units))
        plan_units.append(entry)
        components.extend(entry_components)
    return {"batch": True, "units": plan_units}, components


def _unit_plan_entry(unit: dict, index: int) -> tuple[dict, list[dict]]:
    """单份 unit → (plan unit, 该份的 primary/history/file 组件)。"""
    files = [_file_plan_entry(path) for path in unit.get("file_paths") or ()]
    media_urls = [str(url) for url in unit.get("media_urls") or ()]
    entry = {
        "component_id": f"primary:{index}",
        "history_component_id": f"history:{index}",
        "cleaned_text": unit.get("body") or "",
        "media_urls": media_urls,
        "files": files,
        "reply_to": str(unit.get("reply_to") or ""),
    }
    components = [
        {
            "component_id": f"primary:{index}",
            "kind": "primary",
            "payload_sha256": _sha256_json({
                "cleaned_text": entry["cleaned_text"],
                "media_urls": media_urls,
                "reply_to": entry["reply_to"],
            }),
        },
        {
            "component_id": f"history:{index}",
            "kind": "history",
            "payload_sha256": _sha256_json({"cleaned_text": entry["cleaned_text"]}),
        },
    ]
    for file_index, file_entry in enumerate(files):
        components.append({
            "component_id": f"file:{index}:{file_index}",
            "kind": "file",
            "payload_sha256": _sha256_json(file_entry),
        })
    return entry, components


def prepare_terminal_batch(
    *, tracker, run_id: str | None, units: list[dict]
) -> list[DeliveryContext]:
    """整批计划落库成功后返回逐份上下文；失败抛 TerminalPlanError（零发送）。"""
    if tracker is None or not run_id:
        raise TerminalPlanError("no delivery ledger identity (tracker/run_id missing)")
    if not units:
        raise TerminalPlanError("empty terminal batch")
    plan, components = build_batch_plan(units)
    try:
        tracker.upsert_delivery_plan(run_id=run_id, plan=plan, components=components)
    except Exception as exc:  # 计划冲突／写库失败一律显式失败
        raise TerminalPlanError(f"terminal plan rejected for run={run_id}: {exc}") from exc
    return [
        DeliveryContext(
            tracker=tracker, run_id=run_id, component_id=f"primary:{index}",
            generation_guard=lambda run_id=run_id: tracker.is_delivery_current(run_id),
        )
        for index in range(len(units))
    ]


# ---------------------------------------------------------------------------
# E3：流式即时决策的台账追加与终态折叠（本部署补丁）
#
# 30 号台账的约束不变：已落的 decisions 必须是新 decisions 的逐字前缀，已 sent
# 组件不重置。E3 只允许「在尾部追加新份」，不允许改写/重排旧份；终态 output
# 解出的份若是已流式份的后缀，就折掉不重发。
# ---------------------------------------------------------------------------


def _media_identity(url: str) -> str:
    """媒体身份摘要：比较用定长表示，避免把整段 base64 塞进比较键。"""
    return hashlib.sha256(str(url).encode("utf-8")).hexdigest()


def normalize_decision(decision: dict) -> dict:
    """比较用决策规范表示：去掉插件内部 `_` 前缀键，正文按**计划器同款**归一。

    delta 与终态是同一段文本的两种切帧：同一张本地图片在流式 body 里可能是
    本地路径，终态却已被真实 inliner 换成 data URL（R2）。直接比较原始 body
    会把同一份判成两份，终态因此再发一遍。这里复用 `_decision_unit` 的清洗
    与媒体摘取（`extract_response_media` 内部就是同一份 inliner）得到
    （正文, 媒体身份, 文件, reply_to），两侧落在同一规范表示上。

    仍然只做**后缀逐份**等价比较，不按正文全局去重：同文不同段是两条独立决策。
    """
    payload = {
        str(key): value
        for key, value in (decision or {}).items()
        if not str(key).startswith("_")
    }
    unit = _decision_unit(decision)
    if unit is None:
        payload["body"] = ("raw", str((decision or {}).get("body") or "").strip())
    else:
        payload["body"] = (
            "unit",
            unit["body"],
            tuple(_media_identity(url) for url in unit["media_urls"]),
            tuple(unit["file_paths"]),
            unit["reply_to"],
        )
    return payload


def fold_stream_decisions(
    streamed: list[dict], terminal: list[dict] | None
) -> tuple[list[dict], int]:
    """把终态决策折回已即时发送的决策序列；返回 (合并后决策, 已折叠的终态份数)。

    delta 流的终态段与 run.completed.output 是同一段文本的两种切帧，正常情况
    下终态份是已流式决策的**后缀**。只按后缀逐份等价比较（正文+控制），
    不按文本全局去重——同文不同段是两条独立决策。
    终态里多出来的份（流式没收到）原样追加，由调用方补发。
    """
    stream = [normalize_decision(item) for item in (streamed or []) if isinstance(item, dict)]
    term = [normalize_decision(item) for item in (terminal or []) if isinstance(item, dict)]
    folded = 0
    for size in range(min(len(stream), len(term)), 0, -1):
        if stream[len(stream) - size:] == term[:size]:
            folded = size
            break
    merged = list(streamed or [])
    if folded < len(term):
        merged.extend((terminal or [])[folded:])
    return merged, folded


def append_terminal_batch(
    *, tracker, run_id: str | None, units: list[dict]
) -> list[DeliveryContext]:
    """在既有计划尾部追加新份；返回**全量**份上下文（含已流式发送的前缀）。

    只追加、不改写：tracker 侧用 allow_prefix_growth 校验旧 units 逐字不变。
    没有既有计划时等价于首次落计划（供流式首份与无流式终态共用）。
    """
    if tracker is None or not run_id:
        raise TerminalPlanError("no delivery ledger identity (tracker/run_id missing)")
    delivery = tracker.get_delivery(run_id)
    if delivery is None:
        raise TerminalPlanError(f"no delivery for run={run_id}")
    plan_units = delivery_plan_units(delivery)
    if not isinstance(plan_units, list):
        plan_units = []
    components: list[dict] = []
    new_units = list(units or [])
    for unit in new_units:
        entry, entry_components = _unit_plan_entry(unit, len(plan_units))
        plan_units.append(entry)
        components.extend(entry_components)
    if new_units:
        try:
            tracker.upsert_delivery_plan(
                run_id=run_id,
                plan={"batch": True, "units": plan_units},
                components=components,
                allow_prefix_growth=True,
            )
        except Exception as exc:
            raise TerminalPlanError(
                f"stream plan append rejected for run={run_id}: {exc}"
            ) from exc
    return [
        DeliveryContext(
            tracker=tracker, run_id=run_id, component_id=f"primary:{index}",
            generation_guard=lambda run_id=run_id: tracker.is_delivery_current(run_id),
        )
        for index in range(len(plan_units))
    ]


def append_stream_unit(*, tracker, run_id: str | None, unit: dict) -> DeliveryContext:
    """即时决策的第一/后续份：必要时开流式台账行，再追加一份计划并返回上下文。"""
    if tracker is None or not run_id:
        raise TerminalPlanError("no delivery ledger identity (tracker/run_id missing)")
    if tracker.get_delivery(run_id) is None:
        tracker.open_stream_delivery(run_id)
    contexts = append_terminal_batch(tracker=tracker, run_id=run_id, units=[unit])
    return contexts[-1]


def delivery_plan_units(delivery: dict) -> list[dict] | None:
    """已落库计划里的份列表；无计划/legacy 计划返回 None。"""
    raw = delivery.get("plan_json")
    if not raw:
        return None
    try:
        plan = json.loads(raw)
    except ValueError:
        return None
    units = plan.get("units") if isinstance(plan, dict) else None
    return units if isinstance(units, list) else None


def fold_plan_units(*, tracker, run_id: str | None, units: list[dict]) -> tuple[list[dict], int]:
    """终态 units 折到既有计划尾部；返回 (需新增的尾部 units, 既有份数)。

    无既有计划 → (units, 0)，调用方走 E2 首次落计划。既有计划只接受「终态份
    是计划后缀」的折叠；对不上就按新份追加（不推翻已落账的旧份）。
    """
    delivery = tracker.get_delivery(run_id) if tracker is not None and run_id else None
    old_units = delivery_plan_units(delivery) if delivery else None
    if not old_units:
        return list(units), 0
    new_plan, _ = build_batch_plan(units)
    new_units = new_plan["units"]

    def payload(entry: dict) -> dict:
        return {
            key: entry.get(key)
            for key in ("cleaned_text", "media_urls", "files", "reply_to")
        }

    folded = 0
    for size in range(min(len(old_units), len(new_units)), 0, -1):
        if [payload(u) for u in old_units[len(old_units) - size:]] == [
            payload(u) for u in new_units[:size]
        ]:
            folded = size
            break
    return list(units[folded:]), len(old_units)
