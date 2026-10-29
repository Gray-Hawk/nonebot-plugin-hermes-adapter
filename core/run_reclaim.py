"""runs 提交应答丢失后的启动 reclaim（alive/108 提交 D）。

只处理 run_submissions 中 run_id 为空且未被 clear 取消的记录：用原
Idempotency-Key 与原始请求体重放，网关归还同一 run 后绑定＋登记，交给既有
run_resume。409、超 24h、路由漂移或应答不可判定一律 needs_manual，绝不换 key
偷跑新 run；网络未知保留 submitted，下次启动再试。
"""
from __future__ import annotations

import time

import httpx
from nonebot import logger

from .routing import resolve_target

_mcp = None  # 测试可注入；生产由 _runtime() 取真实单例
_http_post = None  # 测试缝；生产走 httpx
_RECLAIM_RETENTION_SEC = 24 * 3600
_RECLAIM_TIMEOUT_SEC = 15


def _runtime():
    global _mcp
    if _mcp is None:
        from .. import mcp as runtime_mcp

        _mcp = runtime_mcp
    return _mcp


def _scope_components(scope_key: str) -> tuple[str, bool, str, str | None]:
    """make_scope_key 的逆变换:adapter:private:<QQ> / adapter:group:<群号>。"""
    adapter, kind, scope_id = scope_key.split(":", 2)
    if kind == "private":
        return adapter, True, scope_id, None
    if kind == "group":
        return adapter, False, "", scope_id
    raise ValueError(f"unknown scope kind: {kind!r}")


async def _post(tgt, submission: dict):
    if _http_post is not None:
        return await _http_post(tgt, submission)
    headers = {"Idempotency-Key": submission["idempotency_key"]}
    if tgt.api_key:
        headers["Authorization"] = f"Bearer {tgt.api_key}"
    timeout = min(float(tgt.timeout), _RECLAIM_TIMEOUT_SEC)
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.post(
            f"{tgt.base_url}/v1/runs", json=submission["request_body"], headers=headers
        )


async def reclaim_pending_submissions() -> dict:
    """启动恢复第一步；返回各类结果计数，不让单条失败挡住后续 run。"""
    tracker = _runtime().run_tracker
    stats = {"reclaimed": 0, "needs_manual": 0, "deferred": 0}
    if tracker is None:
        return stats
    pending = tracker.list_unbound_submissions()
    if pending:
        logger.info(f"[HERMES run-reclaim] 启动发现 {len(pending)} 条未绑定 submission")
    for submission in pending:
        key = submission["idempotency_key"]
        if time.time() - submission["created_at"] > _RECLAIM_RETENTION_SEC:
            # 网关只清理终态幂等记录；超期重放可能变成新 run，必须人工核对。
            tracker.mark_submission_state(key, "needs_manual")
            stats["needs_manual"] += 1
            logger.warning(f"[HERMES run-reclaim] 超 24h 保留期，转人工核对 key={key}")
            continue
        try:
            adapter, is_private, user_id, group_id = _scope_components(submission["scope_key"])
            tgt = resolve_target(adapter, is_private, group_id)
            if submission["target_fingerprint"] != f"{adapter}|{tgt.base_url}":
                tracker.mark_submission_state(key, "needs_manual")
                stats["needs_manual"] += 1
                logger.warning(f"[HERMES run-reclaim] 上游路由已变化，转人工核对 key={key}")
                continue
            resp = await _post(tgt, submission)
        except Exception as exc:
            # 网络／解析未知不代表网关未接受；保留原记录，不换 key。
            stats["deferred"] += 1
            logger.warning(f"[HERMES run-reclaim] 重放结果未知，保留下次启动 key={key}: {exc}")
            continue
        if resp.status_code not in (200, 202):
            tracker.mark_submission_state(key, "needs_manual")
            stats["needs_manual"] += 1
            logger.warning(
                f"[HERMES run-reclaim] 重放被拒绝，转人工核对 key={key} "
                f"status={resp.status_code} body={resp.text[:200]!r}"
            )
            continue
        try:
            payload = resp.json()
            run_id = str(payload.get("run_id") or "")
            if not run_id:
                raise ValueError("missing run_id")
        except Exception as exc:
            tracker.mark_submission_state(key, "needs_manual")
            stats["needs_manual"] += 1
            logger.warning(f"[HERMES run-reclaim] 应答无 run 身份，转人工核对 key={key}: {exc}")
            continue
        try:
            tracker.bind_submission_and_register(
                idempotency_key=key,
                run_id=run_id,
                scope_key=submission["scope_key"],
                adapter=adapter,
                is_private=is_private,
                user_id=user_id,
                group_id=group_id,
                bot_self_id=submission["bot_self_id"],
                session_key=submission["session_key"],
                mode=submission["mode"],
                expect_structured=submission["expect_structured"],
            )
        except Exception as exc:
            # 已拿到 run 身份但本地绑定失败：保留原 submission 下次重试，网关仍会同键归还。
            stats["deferred"] += 1
            logger.warning(f"[HERMES run-reclaim] run={run_id} 本地绑定失败，保留重试: {exc}")
            continue
        stats["reclaimed"] += 1
        logger.warning(
            f"[HERMES run-reclaim] 已认领原 run run_id={run_id} "
            f"replayed={bool(payload.get('replayed'))}"
        )
    return stats
