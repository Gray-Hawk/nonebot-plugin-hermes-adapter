"""Session-addressed bridge onto the gateway's ONE manual-compression implementation.

The messaging ``/compress`` command (``gateway/slash_commands_session.py``) resolves a
``SessionSource`` from the incoming message, loads that session's transcript, and calls
``_run_manual_compression``. A Runs client holds only a session id, so this module supplies
exactly the two things the chat path gets from its message:

* the session's durable continuation tip (a client may still hold a pre-rotation id), and
* a per-conversation (never global) busy fence.

Everything substantive stays in ``_run_manual_compression``: the temporary agent build, the
shared ``compress_now`` core, and ``_persist_manual_compression`` (rotation included). This
file adds no compressor and no transcript of its own.

``--preview`` is the one other branch the chat command has, and it is reused verbatim
(``_compress_preview_reply``): a pure report over the loaded transcript. It performs no
agent build, takes no turn lease and no compression lock, and writes nothing — so it stays
available while a turn is running, exactly like the messaging command. Its numbers are also
returned structurally (current-context token estimate, message count, the session model's
``context_length``), so a client renders "占用/上限" without scraping the reply text.

Busy fencing reuses the SAME cross-process session turn lease every agent turn takes
(``agent/turn_facade_lease.py`` -> ``SessionDB.acquire_session_turn_lease``), so a turn
running in this or another process makes the request a retryable ``session_busy`` refusal
instead of interleaving a compression with it. Holding it also parks the next turn (the
existing behavior for any writer) rather than letting the two interleave.
"""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# The summariser is an LLM call; the lease must comfortably outlive one compression so no
# turn starts while the replacement transcript is still being written. A crashed holder is
# still reclaimed by the lease's dead-PID / expiry rules.
_MANUAL_COMPRESS_LEASE_TTL_S = 1800.0

#: Roles the messaging preview counts as "currently in context"
#: (``gateway/slash_commands_session._compress_preview_reply``) — kept as a module constant so the
#: structured figures and the reply text cannot drift apart.
_PREVIEW_ROLES = frozenset({"user", "assistant"})
#: Roles the real compression request carries (tool results included); the full-history estimate.
_HISTORY_ROLES = frozenset({"user", "assistant", "tool"})
#: A preview must answer promptly; an endpoint probe that hangs reports "上限未知" instead.
_WINDOW_RESOLVE_TIMEOUT_S = 10.0


class CompressError(Exception):
    """A refusal/error with the HTTP mapping the control endpoint returns."""

    def __init__(self, code: str, message: str, *, status: int = 500, retry_after: str = ""):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.retry_after = retry_after


def _rough_tokens(messages) -> int:
    """Rough request-size estimate over the same message set the compressor sees."""
    try:
        from agent.model_metadata import estimate_request_tokens_rough
        msgs = [{"role": m.get("role"), "content": m.get("content")}
                for m in messages if m.get("role") in _HISTORY_ROLES]
        return int(estimate_request_tokens_rough(msgs))
    except Exception:
        return 0


def _preview_messages(history) -> list[dict]:
    """The messaging preview's own message selection: user/assistant rows that carry content.

    Deliberately the same comprehension ``gateway.slash_commands_session._compress_preview_reply``
    applies, so the structured ``context_tokens`` equals the "~N tokens currently in context" number
    the chat command prints (a test pins that equality against the real reply text).
    """
    return [{"role": m.get("role"), "content": m.get("content")} for m in history
            if m.get("role") in _PREVIEW_ROLES and m.get("content")]


def _resolve_model_window(model: str) -> dict:
    """The session model's context window through the gateway's own resolver (sync; may probe).

    ``gateway.run._resolve_gateway_model_context`` is the helper ``/context`` uses: it honours the
    ``model.context_length`` config pin, custom providers and endpoint/catalog lookups for the
    *current* route, so the number follows a model switch instead of being fixed here. Its
    ``context_source == "default"`` means the model is unknown to the catalog and it fell through to
    ``DEFAULT_FALLBACK_CONTEXT``: that fallback is NOT a known window, so the limit is ``None`` and
    the client prints 上限未知 rather than a made-up figure.
    """
    from gateway.run import _resolve_gateway_model_context

    resolved = _resolve_gateway_model_context(model or None)
    limit = int(getattr(resolved, "context_length", 0) or 0)
    known = str(getattr(resolved, "context_source", "") or "") != "default"
    return {
        "model": str(getattr(resolved, "model", "") or model or ""),
        "context_length": limit if (known and limit > 0) else None,
        "context_source": str(getattr(resolved, "context_source", "") or ""),
    }


async def _context_window(runner: Any, source: Any, model: str) -> dict:
    """Resolve the window off the event loop, in the profile serving ``source`` (multiplex)."""
    def _resolve() -> dict:
        from gateway.run import _profile_runtime_scope
        multiplex = bool(getattr(getattr(runner, "config", None), "multiplex_profiles", False))
        home_for = getattr(runner, "_resolve_profile_home_for_source", None)
        if multiplex and callable(home_for):
            with _profile_runtime_scope(Path(home_for(source))):
                return _resolve_model_window(model)
        return _resolve_model_window(model)

    try:
        return await asyncio.wait_for(asyncio.to_thread(_resolve), timeout=_WINDOW_RESOLVE_TIMEOUT_S)
    except Exception:  # unresolved/timeout: report "上限未知", never a guessed window
        logger.info("session-compress: context window unresolved for %r", model, exc_info=True)
        return {"model": model, "context_length": None, "context_source": "unresolved"}


async def _lookup_source(runner, tip: str):
    """``SessionSource`` this session is routed from, or the synthesized API-server one."""
    from gateway.config import Platform
    from gateway.session import SessionSource

    entry = await runner.async_session_store.lookup_by_session_id(tip)
    source = getattr(entry, "origin", None) if entry is not None else None
    if source is not None:
        return source
    # A Runs/API session is addressed by id: mirror the source the api_server lane carries.
    return SessionSource(platform=Platform.API_SERVER, chat_id=tip, chat_type="dm")


def _request_from_payload(payload: dict):
    """Payload → the shared ``CompressRequest`` (same parser every native surface uses).

    A focus-only payload goes through ``compress_request``, which feeds the raw argument string to
    the one parser (so ``here N`` stays a boundary). A client that sends the preview flag as its own
    field keeps that parsing too: the flag is merged into the parsed request instead of bypassing
    the parser, and ``--aggressive`` keeps the same refusal.
    """
    from gateway.session_mutation_compress import compress_request

    if payload.get("preview") and set(payload) <= {"focus", "preview"}:
        from agent.conversation_compression_manual import parse_compress_args
        from hermes_state_runtime import RuntimeStoreError

        request = parse_compress_args(str(payload.get("focus") or ""))
        if request.aggressive:
            raise RuntimeStoreError("unsupported_compress_options")
        request.preview = True
        return request
    return compress_request(payload)


def _validate_session_id(session_id: str) -> None:
    from gateway.session import _is_path_unsafe
    if (not session_id or len(session_id) > 512 or any(ch in session_id for ch in "\r\n\x00")
            or _is_path_unsafe(session_id)):
        raise CompressError("invalid_session_id", "Invalid session ID", status=400)


async def _preview_outcome(runner, row: Any, history, tip: str, request, before_tokens: int) -> dict:
    """``/compress --preview``: the messaging command's own read-only report, plus its numbers.

    Reuses ``_compress_preview_reply``/``summarize_compress_preview`` so the numbers a Runs client
    sees are the numbers the real run would use, and returns them as structured fields instead of
    letting the client scrape the reply text. Nothing is built, leased or written: the transcript is
    unchanged, both session ids are the input id, and the window comes from the session's own model.
    """
    from agent.model_metadata import estimate_request_tokens_rough
    from gateway.slash_commands_session import _compress_preview_reply
    from hermes_cli.partial_compress import summarize_compress_preview

    messages = _preview_messages(history)
    context_tokens = int(estimate_request_tokens_rough(messages))
    report = summarize_compress_preview(
        messages, request.partial, request.keep_last, request.focus_topic, context_tokens)
    window = await _context_window(
        runner, await _lookup_source(runner, tip),
        str(row.get("model") or "") if isinstance(row, dict) else "")
    return _outcome(
        status="preview", before=tip, after=tip, before_messages=len(history),
        after_messages=len(history), before_tokens=before_tokens, after_tokens=before_tokens,
        # The reply text stays the messaging surface's own rendering (same selection, same
        # estimator); the structured fields above are what a client must use for numbers.
        message=_compress_preview_reply(
            history, request.partial, request.keep_last, request.focus_topic, ""),
        focus_topic=request.focus_topic,
        preview={
            # What actually goes into the context (messaging preview's selection: user/assistant
            # with content) — the figure the QQ receipt shows.
            "context_tokens": context_tokens,
            "context_messages": len(messages),
            # The session model's window; None means genuinely unknown (never a guessed 1M).
            "context_limit": window["context_length"],
            "context_model": window["model"],
            "context_source": window["context_source"],
            # The whole stored transcript, tool results included: same estimator, wider selection.
            "history_tokens": before_tokens,
            "history_messages": len(history),
            # The shared structured report the reply text is rendered from.
            "head_count": int(report["head_count"]),
            "tail_count": int(report["tail_count"]),
            "partial": bool(report["partial"]),
        })


async def compress_session(runner, session_id: str, payload: dict) -> dict:
    """Compress ONE session through the messaging implementation.

    Returns the JSON-safe outcome (message counts, both session ids, rotation/in-place flags,
    rough token estimates and the surface-neutral reply text). Raises :class:`CompressError`
    for the endpoint's 4xx/5xx mapping.
    """
    from agent.conversation_compression_manual import MIN_MESSAGES
    from gateway.config import Platform
    from gateway.session import SessionEntry, SessionSource
    from gateway.session_transcript import TranscriptReadError
    from hermes_state_errors import SessionCompressionInProgressError, SessionTurnLeaseLostError
    from hermes_state_pidns import holder_namespace_token
    from hermes_state_runtime import RuntimeStoreError

    _validate_session_id(session_id)
    db = getattr(runner, "_session_db", None)
    sync_db = getattr(db, "_db", db)
    if sync_db is None or not callable(getattr(sync_db, "get_session", None)):
        raise CompressError("storage_unavailable", "Session storage unavailable", status=503)

    row = await asyncio.to_thread(sync_db.get_session, session_id)
    if not row:
        raise CompressError("session_not_found", "Session not found", status=404)
    # The durable continuation tip: a client still holding a pre-rotation id compresses and
    # reports the CURRENT transcript, exactly like resume does.
    tip = await asyncio.to_thread(sync_db.get_compression_tip, session_id) or session_id

    try:
        request = _request_from_payload(payload if isinstance(payload, dict) else {})
    except RuntimeStoreError as exc:
        raise CompressError(exc.reason, f"Invalid compression request: {exc.reason}", status=400) from None

    # One transcript read feeds both the read-only preview and the real run.
    try:
        history = await runner.async_session_store.load_transcript(tip)
    except TranscriptReadError as exc:
        raise CompressError("transcript_unreadable", "Session transcript unreadable",
                            status=500) from exc
    before_messages, before_tokens = len(history), _rough_tokens(history)
    if before_messages < MIN_MESSAGES:
        # Same refusal the messaging command gives; no provider call, no write.
        from agent.i18n import t
        return _outcome(
            status="not_enough", before=tip, after=tip, before_messages=before_messages,
            after_messages=before_messages, before_tokens=before_tokens,
            after_tokens=before_tokens, message=t("gateway.compress.not_enough"),
            focus_topic=request.focus_topic)

    # ``--preview`` short-circuits BEFORE the compression lock and the turn lease: it is a
    # read-only report, so it must never write history and must not be refused merely because
    # a turn is in flight (the messaging command does not gate it either).
    if request.preview:
        return await _preview_outcome(runner, row, history, tip, request, before_tokens)

    # A compression already in flight holds the per-session compression lock; say so
    # distinctly instead of reporting a turn as busy. (Diagnostic read only.)
    lock_reader = getattr(sync_db, "get_compression_lock_holder", None)
    if callable(lock_reader):
        try:
            if await asyncio.to_thread(lock_reader, tip):
                raise CompressError(
                    "session_compressing", "Session compression already in progress",
                    status=409, retry_after="1")
        except CompressError:
            raise
        except Exception:
            logger.debug("session-compress: compression-lock probe failed", exc_info=True)

    holder = f"pid={os.getpid()}{holder_namespace_token()}:manual-compress={uuid.uuid4().hex}"
    try:
        acquired = await asyncio.to_thread(
            sync_db.try_acquire_session_turn_lease, tip, holder, ttl_seconds=_MANUAL_COMPRESS_LEASE_TTL_S)
    except Exception as exc:
        logger.warning("session-compress: turn lease acquisition failed for %s: %s", tip, exc)
        raise CompressError("storage_unavailable", "Session storage unavailable", status=503) from None
    if not acquired:
        # Per conversation, not global: another session compresses/starts turns freely.
        raise CompressError(
            "session_busy", "Session is running a turn; retry when it settles",
            status=409, retry_after="1")

    try:
        entry = await runner.async_session_store.lookup_by_session_id(tip)
        source = entry.origin if entry is not None else None
        if source is None:
            # No routing entry (a Runs/API session is addressed by id): synthesize the source
            # the api_server lane itself would carry, so session-keyed resolution stays stable.
            source = SessionSource(platform=Platform.API_SERVER, chat_id=tip, chat_type="dm")
        if entry is None:
            now = datetime.now()
            entry = SessionEntry(
                session_key=runner._session_key_for_source(source), session_id=tip,
                created_at=now, updated_at=now)

        try:
            reply = await runner._run_manual_compression(source, entry, history, request)
        except (SessionTurnLeaseLostError, SessionCompressionInProgressError) as exc:
            raise CompressError(
                "session_busy", "Session is running a turn; retry when it settles",
                status=409, retry_after="1") from exc
        except TranscriptReadError as exc:
            raise CompressError("transcript_unreadable", "Session transcript unreadable",
                                status=500) from exc

        after_id = str(getattr(entry, "session_id", "") or tip)
        after_history = await runner.async_session_store.load_transcript(after_id)
        after_messages, after_tokens = len(after_history), _rough_tokens(after_history)
        rotated = bool(after_id) and after_id != tip

        status = "compressed" if after_messages < before_messages else "no_change"
        # ("no_change" covers an aborted/refused summary or nothing summarizable.)

        # The runs lane keys its cached agent by the session id (its gateway_session_key), not
        # by a synthesized source key, so mirror the messaging path's post-compression eviction
        # on the id the client used: the next turn rebuilds its prompt from the current files.
        if status == "compressed" and not rotated:
            evict = getattr(runner, "_evict_cached_agent", None)
            if callable(evict):
                try:
                    evict(tip)
                except Exception:
                    logger.debug("session-compress: cached-agent eviction failed", exc_info=True)

        return _outcome(
            status=status, before=tip, after=after_id, before_messages=before_messages,
            after_messages=after_messages, before_tokens=before_tokens, after_tokens=after_tokens,
            message=str(getattr(reply, "reply", reply) or ""), focus_topic=request.focus_topic,
            in_place=(status == "compressed" and not rotated))
    finally:
        try:
            await asyncio.to_thread(sync_db.release_session_turn_lease, tip, holder)
        except Exception:
            logger.warning("session-compress: turn lease release failed for %s", tip, exc_info=True)


def _outcome(*, status: str, before: str, after: str, before_messages: int, after_messages: int,
             before_tokens: int, after_tokens: int, message: str, focus_topic, in_place: bool = False,
             preview: dict | None = None) -> dict:
    """JSON-safe projection (no message bodies, no credentials).

    ``preview`` carries the read-only measurements (context tokens/messages, the session model's
    window, the full-history estimate, the shared report's head/tail counts) only on the preview
    branch; every other status keeps the fields it always had.
    """
    out = {
        "object": "hermes.session.compression",
        "status": status,
        "session_id_before": before,
        "session_id_after": after,
        "rotated": bool(after) and after != before,
        "in_place": bool(in_place),
        "before_messages": int(before_messages),
        "after_messages": int(after_messages),
        # Rough request-size estimates (same estimator the compressor reports with), not exact
        # provider counts: the shared implementation returns its own line in ``message``.
        "before_tokens": int(before_tokens),
        "after_tokens": int(after_tokens),
        "focus_topic": focus_topic,
        "message": message,
    }
    if preview is not None:
        out.update(preview)
    return out
