"""run 终态 steer 对账补投引擎(本部署补丁)。

本部署补丁(弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75 根因、alive/84 方案):
steer 被上游接口接受不等于模型真的看到那条消息——上游在 turn 边界可能把注入静默丢弃
(被吞 steer 不上模型上下文, pending_steer 上浮是唯一还回通道)。批 1 已把每条被接受的
steer 记进台账(steer_ledger, status=pending), 本模块在 run 终态后:
  1. 拉会话转录按 m:id 精确搜, 搜得到置 reconciled;
  2. 搜不到 = 被吞, 按情形 A/B 构造补投消息发新 turn(置 replayed);
  3. 查询失败/会话 404 = 全部置 replay_failed, 不补(故障面归 run_resume 层)。
全链每步局部捕获, 任何失败只 WARN 不抛, 不炸主 turn(对账是终态钩子 fire-and-forget
起的后台任务)。补投是正常消息, 走 message_relay 的 passive/reactive 外壳, 复用防抖/
合并/排队语义, 不新建旁路。
"""

from __future__ import annotations

import json
import time

import httpx
from nonebot import get_bot, logger

from .. import mcp as _mcp
from ..core.bg_tasks import spawn_bg_task
from ..core.hermes_client import _try_parse_first_json_block
from ..core.inflight import _SYNTHETIC_NICKNAME
from ..core.routing import resolve_target
from ..core.run_tracker import TrackedRun

_QUERY_LIMIT = 500
_QUERY_TIMEOUT = 10

# 转录里 steer 行的 OUT-OF-BAND 包裹(upstream 写入形态): 开标记一整行(含 em dash)、
# 关标记单独一行。剥皮只锚关标记 + 首个换行, 不锚开标记全文, 免随上游措辞漂移。
_OOB_CLOSE = "[/OUT-OF-BAND USER MESSAGE]"
_OOB_OPEN_PREFIX = "[OUT-OF-BAND USER MESSAGE"


async def reconcile_run_finished(tracked) -> None:
    """run 终态对账入口(终态钩子调用)。tracked = core.run_tracker.TrackedRun。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    pending = tracker.pending_steers(tracked.run_id)
    if not pending:
        return  # 常态零开销路径到此为止: 该 run 没有 pending steer。
    logger.info(
        f"[HERMES steer对账] 触发: run_id={tracked.run_id} pending={len(pending)}"
    )

    rows = await _fetch_transcript(tracked)
    if rows is None:
        _mark_status(tracked, [p["m_id"] for p in pending], "replay_failed")
        logger.warning(
            f"[HERMES steer对账] 转录查询失败, 全部条目置 replay_failed "
            f"(run_id={tracked.run_id} count={len(pending)})"
        )
        return

    # 逐条判定: 任一转录行 content 含 "[m:{id}]:"(带冒号)即落账。冒号是关键判据
    # (双 id 空间问题): 直注挂的 m:id 是平台 message_id、跟帖挂的是感知库 db id, 两
    # 空间可能撞号; 挂载形态是 "标签 [m:xxx]: 内容"(带冒号), 正常消息信封是
    # "[m:xxx] 内容"(无冒号)——带冒号搜索天然区分。不加 display_kind 过滤: 上游
    # _merge_leftover_steer 会把 leftover steer 恢复为下一轮正常 user 行、回收路径落账
    # 后也是正常 user 行, 过滤会把这两类恢复误判成缺件导致双重投递。
    reconciled: list[str] = []
    missing: list[dict] = []
    for p in pending:
        needle = f"[m:{p['m_id']}]:"
        if any(needle in (r.get("content") or "") for r in rows):
            reconciled.append(p["m_id"])
        else:
            missing.append(p)
    if reconciled:
        _mark_status(tracked, reconciled, "reconciled")
        logger.info(f"[HERMES steer对账] 命中 {len(reconciled)} 条 (run_id={tracked.run_id})")
    if not missing:
        return
    logger.info(f"[HERMES steer对账] 缺件 {len(missing)} 条 (run_id={tracked.run_id}), 补投")
    await _replay_missing(tracked, rows, missing)


async def _fetch_transcript(tracked) -> list[dict] | None:
    """GET 会话转录(latest 序, limit=500)。任何失败/404/非 JSON 返回 None。"""
    tgt = resolve_target(tracked.adapter, tracked.is_private, tracked.group_id)
    url = f"{tgt.base_url}/api/sessions/{tracked.session_key}/messages"
    headers = {"Authorization": f"Bearer {tgt.api_key}"} if tgt.api_key else {}
    try:
        async with httpx.AsyncClient(timeout=_QUERY_TIMEOUT) as client:
            resp = await client.get(
                url, params={"order": "latest", "limit": _QUERY_LIMIT}, headers=headers
            )
    except Exception as exc:
        logger.warning(
            f"[HERMES steer对账] 转录查询异常 (run_id={tracked.run_id}): {exc}",
            exc_info=True,
        )
        return None
    if resp.status_code != 200:
        # 404(会话换代/clear 后无转录)与非 200 同归「查询失败」。
        logger.warning(
            f"[HERMES steer对账] 转录查询非 200: {resp.status_code} "
            f"(run_id={tracked.run_id})"
        )
        return None
    try:
        return resp.json().get("data") or []
    except ValueError:
        logger.warning(
            f"[HERMES steer对账] 转录响应不是 JSON (run_id={tracked.run_id})",
            exc_info=True,
        )
        return None


async def _replay_missing(tracked, rows, missing) -> None:
    """对缺件按情形判定补投。多条目按 accepted_at 升序。"""
    missing = sorted(missing, key=lambda p: p["accepted_at"])
    now_ms = int(time.time() * 1000)
    if tracked.is_private:
        await _replay_case_a(tracked, rows, missing, now_ms)  # 私聊恒情形 A。
        return
    # 群聊: sender ∈ 资格集合 → A, 否则 → B。函数内延迟 import(handlers 依赖 mcp
    # 运行时单例, 模块级互 import 会成环)。
    from ..handlers.message_relay import _steer_eligible_ids

    case_a: list[dict] = []
    case_b: list[dict] = []
    for p in missing:
        try:
            eligible = _steer_eligible_ids(
                tracked.adapter, False, p["sender_id"], tracked.group_id, now_ms
            )
        except Exception:
            logger.warning("[HERMES steer对账] _steer_eligible_ids 异常, 按情形 B 处理", exc_info=True)
            eligible = set()
        # 对账时 live run 已 finish 删行, _steer_eligible_ids 只剩钉锚参与者;
        # 触发者本人用 tracked.user_id 补判(TrackedRun.user_id 即 turn 触发者)。
        is_a = p["sender_id"] in eligible or p["sender_id"] == tracked.user_id
        (case_a if is_a else case_b).append(p)
    if case_a:
        await _replay_case_a(tracked, rows, case_a, now_ms)
    for p in case_b:
        await _replay_direct(tracked, p, now_ms)


async def _replay_case_a(tracked, rows, missing, now_ms) -> None:
    """情形 A 补投(时间线重构)。多条被吞在 ★ 位置并列。失败降级情形 B 原文直插。"""
    m_ids = [p["m_id"] for p in missing]
    text: str | None = None
    degraded = False
    try:
        text, degraded = _build_case_a_text(rows, missing)
    except Exception as exc:
        logger.warning(
            f"[HERMES steer对账] 情形 A 文案构建失败, 降级情形 B "
            f"(run_id={tracked.run_id}): {exc}",
            exc_info=True,
        )
        degraded = True
    if degraded or text is None:
        # 500 行盖不住 run 链(收不到触发批)或构建失败 → 降级为情形 B 原文直插。
        logger.warning(
            f"[HERMES steer对账] 情形 A 降级为情形 B 原文直插 "
            f"(run_id={tracked.run_id} count={len(m_ids)})"
        )
        for p in missing:
            await _replay_direct(tracked, p, now_ms)
        return
    try:
        # 情形 A 时间线重构是系统指令文案,合成身份(is_synthetic=True)——真人消息
        # 并入时须带身份分界;昵称用 inflight 单点常量(只承担显示),
        # 手抄字面量会漂移(alive/86 普查收口项)。
        _spawn_replay_turn(
            tracked, text, tracked.user_id, _SYNTHETIC_NICKNAME, now_ms, is_synthetic=True
        )
        _mark_status(tracked, m_ids, "replayed")
        logger.info(f"[HERMES steer对账] 情形 A 补投派发成功 (run_id={tracked.run_id} count={len(m_ids)})")
    except Exception as exc:
        logger.warning(
            f"[HERMES steer对账] 情形 A 补投失败, 置 replay_failed "
            f"(run_id={tracked.run_id}): {exc}",
            exc_info=True,
        )
        _mark_status(tracked, m_ids, "replay_failed")


async def _replay_direct(tracked, row, now_ms) -> None:
    """原文直插补投(情形 B 与情形 A 降级共用)。成功置 replayed, 失败置 replay_failed。

    台账 steer_text 剥尾部约定行后作为正常消息重投; [m:xxx] 标记保留(对账可追溯,
    被吞条目在转录无重复)。bot 取不到/不在线 → replay_failed(不重试)。
    """
    m_id = row["m_id"]
    try:
        text = _strip_convention(row["steer_text"])
        # 私聊 sender 恒为本人; 群聊按原 sender 归因(昵称不可得, 回落 sender_id)。
        user_id = tracked.user_id if tracked.is_private else row["sender_id"]
        # 情形 B 合理例外(alive/84):原文直插是用户的原始话语、非系统指令文案,
        # 按正常消息处理——is_synthetic 走缺省 False,不标合成身份,真人消息并入
        # 不会被合并身份分界误标。
        _spawn_replay_turn(tracked, text, user_id, row["sender_id"], now_ms)
        _mark_status(tracked, [m_id], "replayed")
        logger.info(f"[HERMES steer对账] 原文直插派发成功 (run_id={tracked.run_id} m_id={m_id})")
    except Exception as exc:
        logger.warning(
            f"[HERMES steer对账] 原文直插补投失败, 置 replay_failed "
            f"(run_id={tracked.run_id} m_id={m_id}): {exc}",
            exc_info=True,
        )
        _mark_status(tracked, [m_id], "replay_failed")


def _spawn_replay_turn(
    tracked, text: str, user_id: str, nickname: str, now_ms: int, *, is_synthetic: bool = False
) -> None:
    """把补投文案作为正常消息走 passive/reactive 外壳派发(不传 ephemeral, 复用防抖/
    合并/排队语义)。函数内延迟 import(handlers 依赖 mcp 运行时单例)。

    is_synthetic 区分两类补投身份:情形 A 的时间线重构是系统指令文案(is_synthetic=True),
    真人消息并入时须带身份分界;情形 B 原文直插是用户的原始话语,按正常消息处理
    (is_synthetic=False)——这是 alive/84 情形 B 的合理例外,文案自带头、不是系统指令,
    不该被合并身份分界误标。
    """
    bot, entry = _resolve_bot(tracked)
    if tracked.is_private:
        from ..handlers.message_relay import _handle_passive_path

        spawn_bg_task(
            _handle_passive_path(
                bot=bot, target=entry.target, adapter_name=tracked.adapter,
                user_id=user_id, nickname=nickname, group_id=None,
                text=text, image_urls=[], is_private=True, now_ms=now_ms,
                is_synthetic=is_synthetic,
            )
        )
    else:
        from ..handlers.message_relay import _handle_reactive_path

        spawn_bg_task(
            _handle_reactive_path(
                bot=bot, target=entry.target, adapter_name=tracked.adapter,
                user_id=user_id, nickname=nickname,
                group_id=tracked.group_id, text=text, image_urls=[],
                is_direct=False, addressed_to_bot=False, now_ms=now_ms,
                force_skip_cooldown=True,
                is_synthetic=is_synthetic,
            )
        )


def _resolve_bot(tracked):
    """取该 scope 的 bot 与 registry entry。取不到抛异常(调用方按失败处理)。"""
    if _mcp.bot_registry is None:
        raise RuntimeError("bot_registry 未就绪")
    scope = "private" if tracked.is_private else "group"
    scope_id = tracked.user_id if tracked.is_private else (tracked.group_id or "")
    entry = _mcp.bot_registry.get(tracked.adapter, scope, scope_id)
    if entry is None:
        raise RuntimeError(f"bot_registry 无 {tracked.adapter}/{scope}/{scope_id} entry")
    return get_bot(entry.bot_self_id), entry


def _build_case_a_text(rows, missing) -> tuple[str | None, bool]:
    """从 rows(latest 序)重构时间线文案。返回 (文案, 是否降级)。

    区间截取: rows[0] = 本 run 末行(终态后立刻对账, 页尖=最终回复)。从 rows[0] 往前
    收: assistant/tool 行照收; display_kind='steer' 的 user 行照收(本链已落账的兄弟
    steer); 第一个普通 user 行(display_kind 空)= 触发批, 收进后停止。收不到触发批
    (500 行盖不住)→ 返回 (None, True) 降级。
    """
    mid_replies: list[str] = []       # [我的中途回应] 条目(逆时序收, 拼装时倒序)
    mid_pushes: list[str] = []        # [我的中途推送] 条目
    delivered_steers: list[str] = []  # [已送达的中途插话] 条目
    tool_count = 0
    trigger_text: str | None = None
    final_reply: str | None = None
    got_final = False

    for row in rows:
        role = row.get("role")
        if role == "assistant":
            content = row.get("content") or ""
            tcs = row.get("tool_calls") or []
            if tcs:
                tool_count += len(tcs)
                mid_pushes.extend(_extract_push_texts(tcs))
            if not content.strip() and tcs:
                # 纯工具调用行(空 content 且带 tool_calls): 不进入文案, 仅计数与提取推送。
                if not got_final:  # 页尖为纯工具调用行时占位最终回复为空。
                    final_reply, got_final = "", True
                continue
            text = _assistant_text(content)
            if not got_final:
                final_reply, got_final = text, True
            elif text.strip():
                mid_replies.append(text.strip())
            continue
        if role == "tool":
            continue  # 工具结果行: 不进入文案。
        if role == "user":
            if row.get("display_kind") == "steer":
                stripped = _strip_out_of_band(row.get("content") or "")
                if stripped:
                    delivered_steers.append(stripped)
                continue
            trigger_text = (row.get("content") or "")[:500]  # 普通 user 行 = 触发批。
            break

    if trigger_text is None:
        return None, True

    parts: list[str] = [
        "(系统核对:补投一条你被吞的中途插话。刚才那个 turn 里,你在模型调用飞行途中"
        "插入的指示因上游缺陷(v0.22.1 引入的 steer 丢弃)没能送达。原时间线重构如下:",
    ]
    if trigger_text.strip():
        parts.append(f"[你当时的触发消息] {trigger_text}")
    for t in reversed(mid_replies):
        parts.append(f"[我的中途回应] {t}")
    for t in reversed(mid_pushes):
        parts.append(f"[我的中途推送] {t}")
    for t in reversed(delivered_steers):
        parts.append(f"[已送达的中途插话] {t}")
    for p in missing:  # 已按 accepted_at 升序
        parts.append(f"[★被吞的插话,本应在这里生效] {_strip_convention(p['steer_text'])}")
    parts.append(f"[我的最终回复] {final_reply[:500] if final_reply else '(空)'}")
    if tool_count:
        parts.append(f"(其间我还调用了 {tool_count} 次工具)")
    parts.append(
        "请把 ★ 处插话按它本来的位置与意图理解并回应;若其内容与最终回复冲突,"
        "以插话为准重新执行。)"
    )
    return "\n".join(parts), False


def _assistant_text(content: str) -> str:
    """assistant 行 content → 文案: JSON 信封提取 reply_text, 否则原文(旁白段)。"""
    if not content:
        return ""
    if content.lstrip().startswith("{"):
        parsed = _try_parse_first_json_block(content)
        if isinstance(parsed, dict) and "reply_text" in parsed:
            rt = parsed.get("reply_text")
            if isinstance(rt, str):
                return rt
    return content


def _extract_push_texts(tool_calls) -> list[str]:
    """从 assistant 行的 tool_calls 提取 name 含 push_message 的 arguments.text。"""
    texts: list[str] = []
    for tc in tool_calls or []:
        if "push_message" not in str(tc.get("name") or ""):
            continue
        args = tc.get("arguments")
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except Exception:
                args = None
        if isinstance(args, dict) and isinstance(args.get("text"), str):
            texts.append(args["text"].strip())
    return texts


def _strip_convention(text: str) -> str:
    """剥掉 steer 注入文本尾部的一次性约定行(补投是正常消息, 不该带)。"""
    from ..handlers.message_relay import _STEER_REPLY_CONVENTION_LINE

    line = _STEER_REPLY_CONVENTION_LINE
    if text.endswith(line):
        text = text[: -len(line)]
    return text.rstrip("\n")


def _strip_out_of_band(content: str) -> str:
    """剥掉 gateway 写入转录的 OUT-OF-BAND 包裹, 还原 steer 注入文本原文。"""
    text = content
    idx = text.find(_OOB_CLOSE)
    if idx >= 0:
        text = text[:idx]
    if text.startswith(_OOB_OPEN_PREFIX):
        nl = text.find("\n")
        if nl >= 0:
            text = text[nl + 1:]
    return text.strip()


def _mark_status(tracked, m_ids: list[str], status: str) -> None:
    """按 run_id+m_id 批量置台账状态(核销/补投结果)。失败只 WARN。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    try:
        tracker.mark_steers(tracked.run_id, m_ids, status)
    except Exception:
        logger.warning(
            f"[HERMES steer对账] mark_steers 失败 ({status}) (run_id={tracked.run_id})",
            exc_info=True,
        )
