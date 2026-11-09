"""The two narrow startup-recovery patches, as runtime wrappers over stock gateway modules.

Both call sites import their target at CALL time (``from gateway... import ...`` inside the function), so
replacing the module attribute reaches live traffic without editing upstream.

1. ``collect_unheld_api_images``: ``recover_api_turns`` calls it synchronously ON the loop,
   where its full-store image scan parks it; there it is skipped with one explicit log. Every
   off-loop caller (the delete-session path's ``asyncio.to_thread``) reaches the original.
2. ``recover_profile_native_sessions``: PR #148062 moves the synchronous binding stage (pending
   read, routing-index walk, compression lineage) into ``asyncio.to_thread`` and leaves
   ``recover_native_sessions`` on the loop. An already-patched core is left alone; a stock core
   gets that split here. The PR's shutdown-wait change is NOT part of this plugin.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

PATCH_ID = "startup-recovery-thin/1"
_MARK = "__startup_recovery_thin__"
_loop_skip_logged = False


@dataclass(frozen=True)
class InstallState:
    media: str
    native: str

    def describe(self) -> str:
        return f"media={self.media} native={self.native}"


def _shape(fn, *, coroutine: bool, params: int) -> bool:
    if not callable(fn) or inspect.iscoroutinefunction(fn) is not coroutine:
        return False
    try:
        return len(inspect.signature(fn).parameters) == params
    except (TypeError, ValueError):
        return False


def _install_media(media_module) -> str:
    current = getattr(media_module, "collect_unheld_api_images", None)
    mark = getattr(current, _MARK, None)
    if mark == PATCH_ID:
        return "already-installed"
    if mark is not None:
        raise RuntimeError(f"collect_unheld_api_images already carries {mark!r}")
    if current is None or not _shape(current, coroutine=False, params=1):
        raise RuntimeError("collect_unheld_api_images is not a (db) function any more")
    original = current

    def collect_unheld_api_images(db):
        global _loop_skip_logged
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return original(db)  # worker thread (delete-session path): collect as before
        if not _loop_skip_logged:
            _loop_skip_logged = True
            logger.warning(
                "startup-recovery-thin: skipped the startup retained-API-image sweep on the event "
                "loop (%s); off-loop callers (delete-session) still collect", db)
        return 0

    setattr(collect_unheld_api_images, _MARK, PATCH_ID)
    media_module.collect_unheld_api_images = collect_unheld_api_images
    return "installed"


def _bindings(runner, authority):
    """The stock core's binding stage, unchanged, callable off the loop (PR #148062's split)."""
    from gateway.session_authorities import owner_scope
    with owner_scope(authority):
        pending = {row['target_session_id'] for row in authority.db._read_all(
            "SELECT DISTINCT target_session_id FROM session_admissions WHERE status IN ('queued','unknown')")}
        bindings = [(owner, entry.origin, runner._adapter_for_source(entry.origin))
                    for entry in runner.session_store.list_sessions() if entry.origin is not None
                    for owner in [authority.logical_owner(entry.session_id)] if owner in pending]
    return pending, bindings


def _install_native(runtime_module) -> str:
    current = getattr(runtime_module, "recover_profile_native_sessions", None)
    mark = getattr(current, _MARK, None)
    if mark == PATCH_ID:
        return "already-installed"
    if mark is not None:
        raise RuntimeError(f"recover_profile_native_sessions already carries {mark!r}")
    if current is None or not _shape(current, coroutine=True, params=2):
        raise RuntimeError("recover_profile_native_sessions is not a (runner, authority) coroutine")
    try:
        source = inspect.getsource(current)
    except (OSError, TypeError) as exc:
        raise RuntimeError(f"cannot read the core source ({exc}); refusing to shadow it") from exc
    core_bindings = getattr(runtime_module, "_native_recovery_bindings", None)
    bind = _bindings
    if core_bindings is not None:
        if not callable(core_bindings):
            raise RuntimeError("run_runtime._native_recovery_bindings is not callable")
        if "to_thread" in source:
            # This core already carries PR #148062's split: another hop would only stack.
            return "deferred-core-patched"
        bind = core_bindings  # extracted stage, still called on the loop: thread that one
    elif "logical_owner" not in source or "status IN ('queued','unknown')" not in source:
        raise RuntimeError("recover_profile_native_sessions is not the stock shape this patch knows")

    async def recover_profile_native_sessions(runner, authority):
        pending, bindings = await asyncio.to_thread(bind, runner, authority)
        outcome: dict = await authority.recover_native_sessions(bindings)
        from gateway.session_authorities import owner_scope
        from gateway.session_detached import recover_detached_sessions
        with owner_scope(authority):
            outcome.update(dict.fromkeys(recover_detached_sessions(authority, pending - set(outcome)), 'detached'))
        for sid, verdict in outcome.items():
            logger.info('Native session startup recovery %s (%s): %s', sid, authority.profile_id, verdict)
        return outcome

    setattr(recover_profile_native_sessions, _MARK, PATCH_ID)
    runtime_module.recover_profile_native_sessions = recover_profile_native_sessions
    return "installed"


def install(media_module=None, runtime_module=None) -> InstallState:
    """Patch both targets (idempotent). A shape it cannot recognize raises, never no-ops."""
    from gateway import run_runtime, session_ingress_media
    return InstallState(_install_media(media_module or session_ingress_media),
                        _install_native(runtime_module or run_runtime))
