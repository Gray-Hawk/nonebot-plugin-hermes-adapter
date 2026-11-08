"""session-compress: the Runs-lane manual compression control path, as a user plugin.

The messaging ``/compress`` command owns the one manual-compression implementation
(``GatewaySessionCommandsMixin._run_manual_compression``: temporary agent + shared
``compress_now`` + rotation persistence). The Runs lane addresses sessions by id and has
no chat source, so it cannot reach that command. This plugin adds only the missing
transport: an authenticated HTTP route on the existing ``api_server`` control plane that
resolves the session, takes the session's own (cross-process) turn lease, and calls the
same method. No compressor, session history, or state machine is duplicated here.
"""
from __future__ import annotations

import logging

from .http_api import register_routes

logger = logging.getLogger(__name__)


def register(ctx):
    """Register the control route on the api_server platform's aiohttp app."""

    def wire(native, adapter):
        # ``native`` is the live aiohttp ``Application``; the runner rides on the app and on
        # the adapter, so the route resolves it per request (the adapter is wired before
        # connect() binds, and a reconnect rebuilds the app).
        register_routes(native)
        logger.info(
            "session-compress: POST /api/plugins/session-compress/sessions/{session_id}/compress registered"
        )

    ctx.register_platform_handler("api_server", wire)
