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
import re
import time

import httpx
from nonebot import get_bot, logger

from .. import mcp as _mcp
from ..core.bg_tasks import spawn_bg_task
from ..core.hermes_client import _try_parse_first_json_block
from ..core.message_origin import MessageOrigin, MessageSource
from ..core.routing import resolve_target
from ..core.run_tracker import TrackedRun, is_virtual_ledger_mid

_QUERY_LIMIT = 500
_QUERY_TIMEOUT = 10
# 分页安全上限(alive/103):10 页=5000 行,覆盖超长 run;达上限仍未覆盖
# 本 run 按证据不足处理(缺件不补投),不无限翻。
_MAX_PAGES = 10

# 转录里 steer 行的 OUT-OF-BAND 包裹(upstream 写入形态): 开标记一整行(含 em dash)、
# 关标记单独一行。剥皮只锚关标记 + 首个换行, 不锚开标记全文, 免随上游措辞漂移。
_OOB_CLOSE = "[/OUT-OF-BAND USER MESSAGE]"
_OOB_OPEN_PREFIX = "[OUT-OF-BAND USER MESSAGE"


async def reconcile_run_finished(tracked) -> None:
    """run 终态对账入口(终态钩子调用)。tracked = core.run_tracker.TrackedRun。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return
    try:
        pending = tracker.claim_steers_for_reconcile(tracked.run_id)
    except Exception:
        logger.warning(
            f"[HERMES steer对账] 台账认领失败,跳过本次对账 (run_id={tracked.run_id})",
            exc_info=True,
        )
        return
    if not pending:
        return  # 常态零开销路径到此为止: 该 run 没有已接受待对账的 steer。
    # 无号行的虚拟锚只承载原文供 native 回收,没有可搜的转录锚:按 m:id 搜必然落空,
    # 会被误判成「被吞」而重复补投,所以不做转录核验——run 已正常收尾即结算。
    # 真被吞的时候,回收点(pending_steer 上浮)已经先按原文把这些账项核销成
    # recovered;剩下的虚拟锚留在 live 只会被后来的 run 按 scope 拉走(旧原文污染)。
    virtual = [p for p in pending if is_virtual_ledger_mid(p.get("m_id"))]
    if virtual:
        _mark_status(tracked, [p["m_id"] for p in virtual], "reconciled")
        logger.info(
            f"[HERMES steer对账] 无号行虚拟锚 {len(virtual)} 条随 run 收尾结算 "
            f"(run_id={tracked.run_id})"
        )
        pending = [p for p in pending if not is_virtual_ledger_mid(p.get("m_id"))]
    if not pending:
        return
    logger.info(
        f"[HERMES steer对账] 触发: run_id={tracked.run_id} pending={len(pending)}"
    )

    result = await _fetch_transcript(tracked, pending)
    if result is None:
        _mark_status(tracked, [p["m_id"] for p in pending], "replay_failed")
        logger.warning(
            f"[HERMES steer对账] 转录查询失败, 全部条目置 replay_failed "
            f"(run_id={tracked.run_id} count={len(pending)})"
        )
        return
    rows, complete = result

    # 逐条判定:任一转录行里出现「这一行消息的聊天行」即落账。统一呈递后 steer
    # 行与普通聊天行同文法(m:id 在行首),不再有 `标签 [m:id]: 内容` 这一带冒号的
    # 独占形态;判据因此改为「行首出现该行 m:id」(强提醒/身份栏可前置),并继续
    # 认旧文法 `[id=…] [m:id]:`——升级前已送达的旧台账行照样核销,不会因为改了
    # 呈递文法就被误判成「被吞」而重复补投。语义未变:仍是「这条原始消息的聊天行
    # 是否真的出现在模型可见转录里」。
    # 不加 display_kind 过滤: 上游
    # _merge_leftover_steer 会把 leftover steer 恢复为下一轮正常 user 行、回收路径落账
    # 后也是正常 user 行, 过滤会把这两类恢复误判成缺件导致双重投递。
    reconciled: list[str] = []
    missing: list[dict] = []
    for p in pending:
        hit = _steer_line_hit_pattern(p["m_id"])
        if any(hit.search(r.get("content") or "") for r in rows):
            reconciled.append(p["m_id"])
        else:
            missing.append(p)
    if reconciled:
        _mark_status(tracked, reconciled, "reconciled")
        logger.info(f"[HERMES steer对账] 命中 {len(reconciled)} 条 (run_id={tracked.run_id})")
    if not missing:
        return
    if not complete:
        # 证据不足(alive/103 §8):命中条目已正常核销,但未覆盖本 run 范围前
        # 不得把未命中当「被吞」补投——置 replay_failed 留待人工/下轮排查。
        _mark_status(tracked, [p["m_id"] for p in missing], "replay_failed")
        logger.warning(
            f"[HERMES steer对账] 证据不完整(分页未覆盖本 run), {len(missing)} 条置 "
            f"replay_failed 不补投 (run_id={tracked.run_id})"
        )
        return
    logger.info(f"[HERMES steer对账] 缺件 {len(missing)} 条 (run_id={tracked.run_id}), 补投")
    await _replay_missing(tracked, rows, missing)


async def _fetch_transcript(tracked, pending: list[dict] | None = None):
    """分页 GET 会话完整持久转录(latest 序, limit=500/页)。

    返回 (rows, complete):rows=按转录 id 去重的累计行(latest 序);
    complete=True=证据完整(全部 pending 命中/遇到本 run 触发批/页尽),
    缺件可补投;False=证据不足(中途页失败或超页上限),缺件不得补投。
    首页失败返回 None(调用方按全部 replay_failed 处理,与旧行为一致)。

    必须显式 include_compacted=true(alive/103):上游缺省 false 只给当前活跃
    上下文,已压缩的 steer 行会被误判成「被吞」而重复补投;对账要核实的是
    完整持久转录,不是模型当前可见窗口。
    分页用上游 limit/offset(latest 序)。查询期间新行只落在页尖,后续页旧行
    相对后移→页间可能重复(按 id 去重)但不会漏行。停止条件:全部 pending
    行的聊天行已在累计行里出现(`_steer_line_hit_pattern`,无需更多证据)、
    遇到本 run 触发批(首个普通 user 行,display_kind 空=覆盖本 run 范围)、
    页返回不足 limit(没有更多)。
    """
    tgt = resolve_target(tracked.adapter, tracked.is_private, tracked.group_id)
    url = f"{tgt.base_url}/api/sessions/{tracked.session_key}/messages"
    headers = {"Authorization": f"Bearer {tgt.api_key}"} if tgt.api_key else {}
    needles = [_steer_line_hit_pattern(p["m_id"]) for p in (pending or [])]
    rows: list[dict] = []
    seen: set = set()
    offset = 0

    def _page_failed(exc_info: bool = False) -> None:
        logger.warning(
            f"[HERMES steer对账] 转录分页失败 (run_id={tracked.run_id} offset={offset})",
            exc_info=exc_info,
        )

    try:
        async with httpx.AsyncClient(timeout=_QUERY_TIMEOUT) as client:
            for _ in range(_MAX_PAGES):
                try:
                    resp = await client.get(
                        url,
                        params={
                            "order": "latest",
                            "limit": _QUERY_LIMIT,
                            "offset": offset,
                            "include_compacted": "true",
                        },
                        headers=headers,
                    )
                except Exception:
                    _page_failed(exc_info=True)
                    return None if offset == 0 else (rows, False)
                if resp.status_code != 200:
                    # 404(会话换代/clear 后无转录)与非 200 同归「查询失败」。
                    logger.warning(
                        f"[HERMES steer对账] 转录查询非 200: {resp.status_code} "
                        f"(run_id={tracked.run_id} offset={offset})"
                    )
                    return None if offset == 0 else (rows, False)
                try:
                    page = resp.json().get("data") or []
                except ValueError:
                    logger.warning(
                        f"[HERMES steer对账] 转录响应不是 JSON (run_id={tracked.run_id})",
                        exc_info=True,
                    )
                    return None if offset == 0 else (rows, False)
                for r in page:
                    rid = r.get("id")
                    if rid is not None:
                        if rid in seen:
                            continue
                        seen.add(rid)
                    rows.append(r)
                if needles and all(
                    any(n.search(r.get("content") or "") for r in rows)
                    for n in needles
                ):
                    return rows, True
                if any(
                    r.get("role") == "user" and not r.get("display_kind") for r in page
                ):
                    return rows, True
                if len(page) < _QUERY_LIMIT:
                    return rows, True
                offset += _QUERY_LIMIT
    except Exception:
        _page_failed(exc_info=True)
        return None if not rows else (rows, False)
    logger.warning(
        f"[HERMES steer对账] 分页达上限 {_MAX_PAGES} 仍未覆盖本 run, 证据按不足处理 "
        f"(run_id={tracked.run_id})"
    )
    return rows, False


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
        # 情形 A 是系统重构指令；情形 B 的真人原文回投走另一来源，不能混用身份。
        origin = MessageOrigin.synthetic(MessageSource.RECONCILE_TIMELINE)
        _spawn_replay_turn(
            tracked, text, tracked.user_id, origin.display_label, now_ms, origin=origin
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

    用户正文只从既有原始来源取:台账 raw_text(发送时随行入账),或按 m:id 回查
    messages 表的原始 content。两者都拿不到就置 replay_failed 留待人工——**不从
    渲染后的文本反向剥壳**:壳与正文按字面不可区分,剥错就是吞用户原文。
    原始 m:id 仍留在台账供对账追溯,不回填成新消息的可寻址身份。
    """
    m_id = row["m_id"]
    try:
        text = original_text_of(row)
        if text is None:
            logger.warning(
                f"[HERMES steer对账] 取不到原始正文,置 replay_failed 不补投 "
                f"(run_id={tracked.run_id} m_id={m_id})"
            )
            _mark_status(tracked, [m_id], "replay_failed")
            return
        # 私聊 sender 恒为本人; 群聊按原 sender 归因(昵称不可得, 回落 sender_id)。
        user_id = tracked.user_id if tracked.is_private else row["sender_id"]
        # 情形 B 是真人原文回投：作者是真人，通道与平台入站分开记录，不生成系统署名。
        _spawn_replay_turn(
            tracked, text, user_id, row["sender_id"], now_ms, origin=MessageOrigin.human_replay()
        )
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
    tracked, text: str, user_id: str, nickname: str, now_ms: int, *, origin: MessageOrigin
) -> None:
    """把补投文案走 passive/reactive 外壳派发，复用既有排队与生命周期。

    origin 由情形 A/B 的调用点显式选择；本函数不根据文案或昵称猜来源。
    """
    bot, entry = _resolve_bot(tracked)
    if tracked.is_private:
        from ..handlers.message_relay import _handle_passive_path

        spawn_bg_task(
            _handle_passive_path(
                bot=bot, target=entry.target, adapter_name=tracked.adapter,
                user_id=user_id, nickname=nickname, group_id=None,
                text=text, image_urls=[], is_private=True, now_ms=now_ms,
                origin=origin,
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
                origin=origin,
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
        "（系统核对：补投一条你被吞的中途插话。刚才那个 turn 里，你在模型调用飞行途中"
        "插入的指示因上游缺陷（v0.22.1 引入的 steer 丢弃）没能送达。原时间线重构如下：",
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
        # 空串是合法原文(用户发了一条空行消息):只有 None 才算取不到,
        # 用 `or` 会把它当成缺失,把渲染壳 `[m:302]:` 当正文补投。
        text = original_text_of(p)
        if text is None:
            text = _strip_convention(p["steer_text"])
        parts.append(f"[★被吞的插话，本应在这里生效] {text}")
    parts.append(f"[我的最终回复] {final_reply[:500] if final_reply else '（空）'}")
    if tool_count:
        parts.append(f"（其间我还调用了 {tool_count} 次工具）")
    parts.append(
        "请把 ★ 处插话按它本来的位置与意图理解并回应；若其内容与最终回复冲突，"
        "以插话为准重新执行。）"
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
    """剥掉 steer 注入文本尾部的一次性约定行(补投是正常消息, 不该带)。

    发送原语挂的是 `text + "\\n" + 约定行`:只还原它插入的那一个分隔换行,正文
    自己的尾随换行(用户原文可能以空行结尾)逐字节保留。旧实现 rstrip("\\n")
    把正文尾空白一起吃掉,「全路径逐字节保全」就不成立。
    """
    from ..handlers.message_relay import _STEER_REPLY_CONVENTION_LINE

    line = _STEER_REPLY_CONVENTION_LINE
    if text.endswith(line):
        text = text[: -len(line)]
        if text.endswith("\n"):
            text = text[:-1]
    return text


def original_text_of(row: dict) -> str | None:
    """台账行 → 原始正文(恢复/对账改投唯一正文来源)。

    先看台账 raw_text(发送时与渲染行一并入账),再按 m:id 回查 messages 表的原始
    content——都是既有数据源,不新建表、不做第二套恢复通道。两样都拿不到返回
    None,由调用方置 replay_failed 留待人工:**不会**退回去从渲染文本反向剥壳,
    壳与正文按字面不可区分,剥错就是吞用户原文。
    """
    raw = row.get("raw_text")
    if raw is not None:
        # 本列随行入账(新账项):正文就是原始 content,哪怕空串也不再往别处找。
        return str(raw)
    m_id = row.get("m_id")
    buffer = getattr(_mcp, "message_buffer", None)
    if m_id is not None and buffer is not None:
        try:
            found = buffer.get_content(int(m_id))
        except Exception:
            found = None
            logger.warning(
                f"[HERMES steer] 按 m:id 回查原始正文失败 (m_id={m_id})", exc_info=True
            )
        if found is not None:
            return found
    # 旧账项(raw_text 列引入前落库,该列为 NULL):按既有兼容边界处理——旧行没有
    # 任何原文来源,剥壳是唯一可用的还原方式。新账项永不走到这里。
    legacy = row.get("steer_text")
    if legacy:
        return strip_steer_presentation(legacy)
    return None


_STEER_STRIP_RE = None


def _steer_strip_pattern() -> re.Pattern:
    """旧账项的呈递外壳剥壳正则(只服务 raw_text 为 NULL 的历史行)。

    新账项一律直接读原始正文(台账 raw_text / messages.content),不经过本正则;
    这里保留升级前落库的老行兼容边界,不再为新文法扩充规则——新版消息头是单独
    一行 `[m:id] [身份] [id=…]:`,本正则认不出它,也就不可能误吞新正文。
    """
    global _STEER_STRIP_RE
    if _STEER_STRIP_RE is None:
        from ..core.prompt_builder import _AT_ME_MARK, _IDENTITY_LABELS

        labels = "|".join(re.escape(x) for x in _IDENTITY_LABELS)
        at = rf"(?:{re.escape(_AT_ME_MARK.strip())}[ \t]+)?"
        idb = rf"(?:\[(?:{labels})\][ \t]+)?"
        tag = r"\[id=[^\]\n]+\]"
        mid = r"\[m:\d+\]"
        _STEER_STRIP_RE = re.compile(
            rf"^{at}(?:"
            # 旧 speaker 形:`[m:id]? [身份]? [id=…]( [m:id])?: `
            rf"(?:{mid}[ \t]+)?{idb}{tag}(?:[ \t]+{mid})?[ \t]*:[ \t]*"
            rf"|"
            # 旧无 speaker 形:`[m:id]: 正文`(身份栏可选)
            rf"{idb}{mid}[ \t]*:[ \t]*"
            rf")",
            flags=re.MULTILINE,
        )
    return _STEER_STRIP_RE


def strip_steer_presentation(text: str) -> str:
    """旧账项还原:剥掉升级前的 steer 呈递外壳。

    只给 `original_text_of` 的旧行兼容分支用(台账 raw_text 为 NULL 的历史账项),
    新账项一律读原始正文。正文里同样字样的文本不受影响(只锚行首)。
    """
    text = _strip_convention(text)
    return _steer_strip_pattern().sub("", text)


def _steer_line_hit_pattern(m_id) -> re.Pattern:
    """对账命中判据:转录里是否出现这一行消息的聊天行。

    统一呈递后 steer 行的 m:id 在行首(强提醒可前置),旧文法把 m:id 挂在
    speaker 之后/冒号之前。两种形态都认,判据只回答一件事:这条原始消息的
    聊天行有没有真的进模型可见转录——与「用哪种文法渲染」无关。
    """
    from ..core.prompt_builder import _AT_ME_MARK, _IDENTITY_LABELS

    labels = "|".join(re.escape(x) for x in _IDENTITY_LABELS)
    mid = rf"\[m:{re.escape(str(m_id))}\]"
    return re.compile(
        rf"^(?:{re.escape(_AT_ME_MARK.strip())}[ \t]+)?"
        rf"(?:\[(?:{labels})\][ \t]+)?"
        rf"(?:\[id=[^\]\n]+\][ \t]+)?{mid}",
        flags=re.MULTILINE,
    )


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
