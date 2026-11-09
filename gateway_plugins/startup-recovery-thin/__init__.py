"""startup-recovery-thin: keep Hermes' startup recovery off the event loop, as a user plugin.

Two narrow wrappers (see ``patches``): the startup retained-API-image sweep is skipped when it
would run on the loop, and the native recovery binding stage moves into a worker thread. No
upstream file is edited; ``recover_native_sessions`` itself stays on the main loop.

``api_server.connect()`` invokes plugin platform factories before it returns, and
``run_adapters`` calls ``recover_api_turns`` only after publishing the adapter -- so ``wire``
runs ahead of startup recovery by construction. ``install`` is idempotent for reconnects.
"""
from __future__ import annotations

import logging

from .patches import install

logger = logging.getLogger(__name__)


def register(ctx):
    logger.info("startup-recovery-thin: %s", install().describe())

    def wire(native, adapter):
        # api_server.connect(), before AppRunner.setup() and before publish -> recover_api_turns.
        logger.info("startup-recovery-thin: verified at api_server connect: %s", install().describe())

    ctx.register_platform_handler("api_server", wire)
