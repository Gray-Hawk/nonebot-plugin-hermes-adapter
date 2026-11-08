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
available while a turn is running, exactly like the messaging command.

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

logger = logging.getLogger(__name__)

# The summariser is an LLM call; the lease must comfortably outlive one compression so no
# turn starts while the replacement transcript is still being written. A crashed holder is
# still reclaimed by the lease's dead-PID / expiry rules.
_MANUAL_COMPRESS_LEASE_TTL_S = 1800.0


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
                for m in messages if m.get("role") in {"user", "assistant", "tool"}]
        return int(estimate_request_tokens_rough(msgs))
    except Exception:
        return 0


def _validate_session_id(session_id: str) -> None:
    from gateway.session import _is_path_unsafe
    if (not session_id or len(session_id) > 512 or any(ch in session_id for ch in "\r\n\x00")
            or _is_path_unsafe(session_id)):
        raise CompressError("invalid_session_id", "Invalid session ID", status=400)


def _preview_outcome(history, tip: str, request, before_messages: int, before_tokens: int) -> dict:
    """``/compress --preview``: the messaging command's own read-only report.

    Reuses ``_compress_preview_reply`` so the numbers a Runs client sees are the numbers the
    real run would use. Nothing is built, leased or written: the transcript is unchanged and
    both session ids are the input id.
    """
    from gateway.slash_commands_session import _compress_preview_reply

    report = _compress_preview_reply(
        history, request.partial, request.keep_last, request.focus_topic, "")
    return _outcome(
        status="preview", before=tip, after=tip, before_messages=before_messages,
        after_messages=before_messages, before_tokens=before_tokens,
        after_tokens=before_tokens, message=report, focus_topic=request.focus_topic)


async def compress_session(runner, session_id: str, payload: dict) -> dict:
    """Compress ONE session through the messaging implementation.

    Returns the JSON-safe outcome (message counts, both session ids, rotation/in-place flags,
    rough token estimates and the surface-neutral reply text). Raises :class:`CompressError`
    for the endpoint's 4xx/5xx mapping.
    """
    from agent.conversation_compression_manual import MIN_MESSAGES
    from gateway.config import Platform
    from gateway.session import SessionEntry, SessionSource
    from gateway.session_mutation_compress import compress_request
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
        request = compress_request(payload if isinstance(payload, dict) else {})
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
        return _preview_outcome(history, tip, request, before_messages, before_tokens)

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
             before_tokens: int, after_tokens: int, message: str, focus_topic, in_place: bool = False) -> dict:
    """JSON-safe projection (no message bodies, no credentials)."""
    return {
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
