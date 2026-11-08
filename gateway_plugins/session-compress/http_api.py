"""Authenticated control route for the session-addressed manual compression path.

Auth reuses the gateway's own control-plane credential (``API_SERVER_KEY``, Bearer scheme),
exactly like the ``background-completion`` user plugin. Fail closed: unset/weak key refuses
every request with 503, a wrong/missing bearer is 401, and the key is never logged or
echoed. No key is read from (or written to) plugin config.
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import os

from aiohttp import web

from .bridge import CompressError, compress_session

logger = logging.getLogger(__name__)

ROUTE_PREFIX = "/api/plugins/session-compress"
_KEY_ENV = "API_SERVER_KEY"
_MAX_BODY_BYTES = 65536


def _configured_key() -> str:
    return os.environ.get(_KEY_ENV, "").strip()


def _authorized(request) -> bool:
    key = _configured_key()
    if len(key.encode("utf-8")) < 16:
        return False  # fail closed: missing/weak key refuses everything
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return False
    return hmac.compare_digest(header[7:].encode("utf-8"), key.encode("utf-8"))


def _error(status: int, code: str, message: str, *, retry_after: str = "") -> web.Response:
    headers = {"Retry-After": retry_after} if retry_after else None
    return web.json_response({"error": {"code": code, "message": message}}, status=status, headers=headers)


def register_routes(app) -> None:
    """Attach the control routes to the live api_server aiohttp application."""
    app.router.add_get(f"{ROUTE_PREFIX}/status", _guarded(_status))
    app.router.add_post(f"{ROUTE_PREFIX}/sessions/{{session_id}}/compress", _guarded(_compress))


def _guarded(handler):
    async def wrapped(request: web.Request):
        if not _configured_key():
            return _error(503, "authentication_not_configured", "Authentication not configured")
        if not _authorized(request):
            return _error(401, "unauthorized", "Unauthorized")
        try:
            return await asyncio.wait_for(handler(request), timeout=900)
        except TimeoutError:
            return _error(504, "timeout", "Compression did not finish in time")

    return wrapped


def _runner(request: web.Request):
    runner = request.app.get("gateway_runner")
    if runner is None:
        adapter = request.app.get("api_server_adapter")
        runner = getattr(adapter, "gateway_runner", None)
    return runner


async def _status(request: web.Request) -> web.Response:
    """Liveness probe: the route exists and a runner is resolvable."""
    return web.json_response({
        "plugin": "session-compress",
        "version": "0.1.0",
        "runner_available": _runner(request) is not None,
    })


async def _compress(request: web.Request) -> web.Response:
    runner = _runner(request)
    if runner is None:
        return _error(503, "runtime_unavailable", "Gateway runner unavailable")

    session_id = request.match_info.get("session_id", "")
    payload: dict = {}
    if request.body_exists:
        if request.content_length is not None and request.content_length > _MAX_BODY_BYTES:
            return _error(413, "body_too_large", "Request body too large")
        try:
            raw = await request.content.read(_MAX_BODY_BYTES + 1)
            if len(raw) > _MAX_BODY_BYTES:
                return _error(413, "body_too_large", "Request body too large")
            if raw.strip():
                import json
                payload = json.loads(raw)
        except Exception:
            return _error(400, "invalid_body", "Body must be a JSON object")
        if not isinstance(payload, dict):
            return _error(400, "invalid_body", "Body must be a JSON object")

    try:
        outcome = await compress_session(runner, session_id, payload)
    except CompressError as exc:
        if exc.status >= 500:
            logger.warning("session-compress refused %s: %s", session_id, exc.code)
        return _error(exc.status, exc.code, exc.message, retry_after=exc.retry_after)
    except Exception as exc:  # noqa: BLE001 - the endpoint must answer, never leak a traceback
        logger.warning("session-compress failed for %s: %s", session_id, type(exc).__name__, exc_info=True)
        return _error(500, "compress_failed", f"Manual compression failed: {exc}")

    headers = {"X-Hermes-Session-Id": outcome["session_id_after"]} if outcome.get("session_id_after") else None
    return web.json_response(outcome, headers=headers)
