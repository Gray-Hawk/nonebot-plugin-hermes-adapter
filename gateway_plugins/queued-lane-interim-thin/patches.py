"""queued-lane 前序答复 → 本 run 的 ``message.interim``（窄补丁，不改上游）。

问题：``/v1/runs`` 的回合跑在网关消息链路上时（``platform=api_server``），adapter 的
``send()`` 是 HTTP 请求/响应桩，必然失败。轮末若还排着 steer 跟进，stock 的 queued lane
（``GatewayTurnMixin._run_agent_deliver_first_response``，唯一调用点
``run_turn.py:1713``）把这份 first_response 交给 ``ApiServerAdapter.send`` → 明确失败 →
文本写进 state.db 后原地丢弃；``run.completed`` 只带最后一份。

做法：跑完 stock 主体后看它做了什么——`already_sent` 已置位（真投递成功/已流式确认）就
什么也不做；否则把这份文本按 ``_execute_run._interim_cb`` 同一个事件形状、投到**本回合
admission 绑定的那个 run** 自己的 ``_RunStream``。SSE 客户端（NB）用它既有的分段发送
逻辑发出去，终态仍只由 ``run.completed.output`` 负责。

绑定只用既有事实，不猜 active run：

1. ``api_execution`` 里**本次 admission 的 adapter**（``prepare_api_execution`` 登记）；
2. ``active_authority(adapter.gateway_runner)`` ＋ ``adapter._ensure_session_db()`` 必须
   同一 ``SessionDB``（上游准入同一条 profile 围栏，``adapter._ensure_session_db`` 与
   authority.db 不同即别的 profile 的 adapter）；
3. ``authority.logical_owner(turn_ctx.session_id)``——压缩续写时 admission 身份在
   lineage 根上，物理 tip 不参与查表；
4. ``live.event_stream.execution`` 的 ``admission_id`` / ``authority_epoch`` /
   ``execution_generation``（``_stamp_claim`` 写入）；
5. ``get_session_admission(db, admission_id=...)`` 的 ``status == started``、
   ``request_id``（== run id）、``target_session_id``（== logical owner）、
   ``owner_epoch`` / ``generation`` 与 (4) 一致；
6. ``adapter._run_streams[run_id]`` 仍在（run 仍拥有 transport）。

任何一条不成立：**不发布**，并留一条可见 warning（除"这根本不是 /v1/runs 回合"这种
正常情形，它只记 debug）；stock 的结果对象一个字段都不动，所以回退结果与今天完全一致
（也就不会错绑）。

同文终态不重复：发布过的 interim 文本记在**该 run 自己的 _RunStream 上**（不是全局表、
没有恢复窗口）；``run.completed`` 帧若与已发布 interim 同文，只把这一帧的 ``output``
清空——``GET /v1/runs/{id}`` 与 admission 结果里的 output 不动。NB 的 SSE 客户端因此
不会再发第二遍；终态文本与 interim 不同时一字不改。

明确不做：不改 ``_interim_cb`` / ``_execute_run``、不新增表、队列、恢复窗口、run 映射
或身份体系；不把 SSE 入队记作 QQ 已投递（``result["already_sent"]`` 绝不置位）。
"""
from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

PATCH_ID = "queued-lane-interim-thin/1"
_MARK = "__queued_lane_interim_thin__"
_INTERIM_TEXTS_ATTR = "_queued_lane_interim_texts"
_RUN_PREFIX = "run_"


@dataclass(frozen=True)
class InstallState:
    first_response: str = "unchanged"
    terminal_dedupe: str = "unchanged"

    def describe(self) -> str:
        return f"first_response={self.first_response} terminal_dedupe={self.terminal_dedupe}"


@dataclass(frozen=True)
class Binding:
    """``run_id``/``stream`` when this turn's admission is exactly one live run; else ``reason``.

    ``noisy`` marks a *break* between the frame the turn carries and the run it should reach
    (a visible failure), as opposed to a turn that simply is not a ``/v1/runs`` execution.
    """

    run_id: str | None = None
    stream: Any = None
    reason: str = ""
    noisy: bool = False
    detail: dict = field(default_factory=dict)


def _adapter_authority(adapter):
    """The authority that owns *adapter*, or ``(None, reason)``.

    ``active_authority`` is the same scope resolution upstream admission uses
    (``session_api_turn.admit_api_steps``); the SessionDB identity check is that door's
    profile fence — the same one ``require_active_authority(runner, store=...)`` enforces:
    the store must be **provably** the authority's. An adapter that cannot show its store
    (attribute missing, ``None``, or a raising call) is not proof of anything, so it is
    refused instead of being let through.
    """
    runner = getattr(adapter, "gateway_runner", None)
    if runner is None:
        return None, "adapter has no gateway runner"
    from gateway.session_authorities import active_authority

    authority = active_authority(runner)
    if authority is None:
        return None, "no active session authority"
    ensure = getattr(adapter, "_ensure_session_db", None)
    if not callable(ensure):
        return None, ("adapter cannot prove its SessionDB (no callable _ensure_session_db); "
                      "upstream demand is store identity")
    try:
        db = ensure()
    except Exception as exc:
        return None, (f"adapter cannot prove its SessionDB (_ensure_session_db raised "
                      f"{type(exc).__name__})")
    if db is None:
        return None, "adapter cannot prove its SessionDB (_ensure_session_db returned None)"
    if db is not authority.db:
        return None, "adapter is bound to another profile's SessionDB"
    return authority, ""


def bound_run(adapter, session_id) -> Binding:
    """The run this turn executes, proven from the admission/generation facts already on disk."""
    from hermes_state_runtime import get_session_admission

    authority, why = _adapter_authority(adapter)
    if authority is None:
        return Binding(reason=why, noisy=True)
    if not session_id:
        return Binding(reason="turn has no session id", noisy=True)
    owner = authority.logical_owner(session_id)  # compression: the admission identity is the root
    live = (getattr(authority, "sessions", None) or {}).get(owner)
    if live is None:
        return Binding(reason=f"no live session for {session_id} (logical owner {owner})",
                       noisy=True, detail={"logical_owner": owner})
    execution = getattr(getattr(live, "event_stream", None), "execution", None) or {}
    admission_id = execution.get("admission_id")
    if not admission_id:
        # A plain api_server turn with no admitted /v1/runs execution has nothing to bind to.
        return Binding(reason=f"no executing admission on {owner}")
    detail = {"admission_id": str(admission_id), "logical_owner": owner}
    row = get_session_admission(authority.db, admission_id=admission_id) or {}
    if not row:
        return Binding(reason=f"admission {admission_id} missing from the store", noisy=True,
                       detail=detail)
    run_id = str(row.get("request_id") or "")
    detail["run_id"] = run_id
    detail["status"] = str(row.get("status") or "")
    if row.get("status") != "started":
        return Binding(reason=f"admission {admission_id} is {row.get('status')!r}, not started",
                       noisy=True, detail=detail)
    if not run_id.startswith(_RUN_PREFIX):
        return Binding(reason=f"admission request_id {run_id!r} is not a /v1/runs run id",
                       noisy=True, detail=detail)
    if str(row.get("target_session_id") or "") != owner:
        return Binding(reason=(f"admission target {row.get('target_session_id')!r} is not this "
                               f"turn's logical owner {owner!r}"), noisy=True, detail=detail)
    epoch = getattr(authority, "epoch", None)
    if row.get("owner_epoch") != epoch or execution.get("authority_epoch") != epoch:
        return Binding(reason=(f"authority epoch mismatch (row={row.get('owner_epoch')!r} "
                               f"stamp={execution.get('authority_epoch')!r} live={epoch!r})"),
                       noisy=True, detail=detail)
    if row.get("generation") != execution.get("execution_generation"):
        return Binding(reason=(f"generation mismatch (row={row.get('generation')!r} "
                               f"stamp={execution.get('execution_generation')!r})"),
                       noisy=True, detail=detail)
    stream = (getattr(adapter, "_run_streams", None) or {}).get(run_id)
    if stream is None:
        return Binding(reason=f"run {run_id} no longer owns transport state", noisy=True,
                       detail=detail)
    return Binding(run_id=run_id, stream=stream, detail=detail)


def _remember_interim(stream, text: str) -> None:
    texts = getattr(stream, _INTERIM_TEXTS_ATTR, None)
    if texts is None:
        texts = set()
        try:
            setattr(stream, _INTERIM_TEXTS_ATTR, texts)
        except Exception:  # a slotted/foreign stream: the frame still goes out, no dedupe memo
            return
    texts.add(text.strip())


def publish_interim(adapter, session_id, text: str) -> Binding:
    """Push one ``message.interim`` frame onto this turn's run stream; never raises."""
    binding = bound_run(adapter, session_id)
    if binding.run_id is None:
        (logger.warning if binding.noisy else logger.debug)(
            "queued-lane interim NOT published for session %s: %s", session_id or "?", binding.reason)
        return binding
    from gateway.platforms.api_server_runs import _run_event

    event = _run_event(binding.run_id, "message.interim", text=text, already_streamed=False)
    # On the gateway loop (the delivery seam is a coroutine), the same synchronous push
    # ``_RunLaunch.put_event`` does; a finished run's stream ignores it (terminal).
    binding.stream.put_nowait(event)
    _remember_interim(binding.stream, text)
    return binding


def _publish_if_undelivered(runner, turn_ctx, adapter, response, result) -> str:
    """Publish the queued-lane first response when the stock lane did not deliver it."""
    if turn_ctx is None or getattr(turn_ctx, "mute_notification_reply", False):
        return ""
    from gateway.config import Platform

    if getattr(getattr(turn_ctx, "source", None), "platform", None) is not Platform.API_SERVER:
        return ""
    if not isinstance(result, dict) or result.get("interrupted"):
        return ""
    if result.get("already_sent"):
        # The stock queued-lane lane DID deliver (or the stream had already confirmed it):
        # publishing again would duplicate what the client already has.
        return ""
    delivery_result = response if isinstance(response, dict) else result
    text = str(delivery_result.get("final_response") or "")
    if runner._is_intentional_silence(delivery_result, text):
        # Same predicate pair the stock body used; an allowed silence is already blank there.
        if runner._silence_allowed_for_source(
            turn_ctx.source, turn_ctx.persist_user_display_kind, turn_ctx.reply_expected,
            delivery_result,
        ):
            return ""
        from gateway.run_turn import _unexpected_silence_reply

        text = str(_unexpected_silence_reply())
    if not text.strip():
        return ""
    # The adapter that ADMITTED this turn owns the run streams; the delivery adapter passed to
    # the seam can be a different (per-source) instance in a multiplexed gateway.
    from gateway.session_api_turn import api_execution

    prepared = api_execution.get() or {}
    owner = prepared.get("adapter") or adapter
    binding = publish_interim(owner, getattr(turn_ctx, "session_id", None), text)
    if binding.run_id:
        logger.info("queued-lane interim published for session %s run %s (%d chars)",
                    getattr(turn_ctx, "session_key", "?") or "?", binding.run_id, len(text))
    return binding.run_id


def _shape_first_response(fn) -> bool:
    if not callable(fn) or not inspect.iscoroutinefunction(fn):
        return False
    try:
        return len(inspect.signature(fn).parameters) == 6  # self, turn_ctx, adapter, response, result, stream_task
    except (TypeError, ValueError):
        return False


def _install_first_response(turn_module) -> str:
    target = turn_module.GatewayTurnMixin
    current = getattr(target, "_run_agent_deliver_first_response", None)
    mark = getattr(current, _MARK, None)
    if mark == PATCH_ID:
        return "already-installed"
    if mark is not None:
        raise RuntimeError(f"_run_agent_deliver_first_response already carries {mark!r}")
    if not _shape_first_response(current):
        raise RuntimeError(
            "GatewayTurnMixin._run_agent_deliver_first_response is not the "
            "(self, turn_ctx, adapter, response, result, stream_task) coroutine")
    original = current

    async def _run_agent_deliver_first_response(self, turn_ctx, adapter, response, result, stream_task):
        await original(self, turn_ctx, adapter, response, result, stream_task)
        try:
            _publish_if_undelivered(self, turn_ctx, adapter, response, result)
        except Exception:  # a publish failure must never change turn semantics
            logger.warning("queued-lane interim publish failed", exc_info=True)

    setattr(_run_agent_deliver_first_response, _MARK, PATCH_ID)
    target._run_agent_deliver_first_response = _run_agent_deliver_first_response
    return "installed"


def _install_terminal_dedupe(runs_module) -> str:
    target = runs_module._RunLaunch
    current = target.put_event
    mark = getattr(current, _MARK, None)
    if mark == PATCH_ID:
        return "already-installed"
    if mark is not None:
        raise RuntimeError(f"_RunLaunch.put_event already carries {mark!r}")
    try:
        arity = len(inspect.signature(current).parameters)
    except (TypeError, ValueError):
        arity = 0
    if arity != 2:  # self, event
        raise RuntimeError("_RunLaunch.put_event is not the (self, event) method")
    original = current

    def put_event(self, event):
        if isinstance(event, dict) and event.get("event") == "run.completed":
            output = event.get("output")
            published = getattr(self.queue, _INTERIM_TEXTS_ATTR, None) or ()
            if isinstance(output, str) and output.strip() and output.strip() in published:
                # The client already received this text as an interim frame on this same run:
                # the terminal frame must not replay it. The run status / admission result keep
                # the full output for polling clients.
                logger.info("queued-lane interim: run %s terminal output already published as an "
                            "interim; not repeating it on the SSE terminal frame", self.run_id)
                event = {**event, "output": ""}
        return original(self, event)

    setattr(put_event, _MARK, PATCH_ID)
    target.put_event = put_event
    return "installed"


def install(turn_module=None, runs_module=None) -> InstallState:
    """Wrap the two seams once. A shape it cannot recognise raises (never a silent no-op)."""
    if turn_module is None:
        from gateway import run_turn as turn_module
    if runs_module is None:
        from gateway.platforms import api_server_runs as runs_module
    return InstallState(
        first_response=_install_first_response(turn_module),
        terminal_dedupe=_install_terminal_dedupe(runs_module),
    )
