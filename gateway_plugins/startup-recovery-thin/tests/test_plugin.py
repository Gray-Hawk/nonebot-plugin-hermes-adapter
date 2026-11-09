"""Focused tests for the startup-recovery-thin plugin.

Pins the two wrappers and the install contract, not the upstream recoveries themselves:

* an event-loop call of ``collect_unheld_api_images`` reaches ZERO original calls and logs once;
* a worker-thread call reaches the original unchanged (the delete-session path);
* the native binding stage runs in a worker thread, ``recover_native_sessions`` returns to the
  loop, and the outcome matches the stock function;
* a core that already carries PR #148062 is left alone; a shape that cannot be recognized raises;
* repeated installs stack nothing.

Run with the Hermes checkout on PYTHONPATH, from a directory that is NOT this repository's root
(the adapter's own ``utils.py`` there would shadow Hermes' ``utils``)::

    cd /tmp && PYTHONPATH=/path/to/hermes-agent \
        /path/to/hermes-agent/venv/bin/python -m unittest discover \
        -s /path/to/repo/gateway_plugins/startup-recovery-thin/tests -v
"""
from __future__ import annotations

import asyncio
import importlib.util
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

# Load the plugin directory (its manifest name has a hyphen) under an importable name.
_PLUGIN_DIR = Path(__file__).resolve().parents[1]
if "startup_recovery_thin_plugin" not in sys.modules:
    _spec = importlib.util.spec_from_file_location(
        "startup_recovery_thin_plugin", _PLUGIN_DIR / "__init__.py",
        submodule_search_locations=[str(_PLUGIN_DIR)])
    assert _spec is not None and _spec.loader is not None
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["startup_recovery_thin_plugin"] = _mod
    _spec.loader.exec_module(_mod)

from startup_recovery_thin_plugin import patches  # noqa: E402

_SQL_TAIL = "status IN ('queued','unknown')"
LOGGER = "startup_recovery_thin_plugin"


# ---------------------------------------------------------------------------
# Fakes: one per seam the wrappers touch
# ---------------------------------------------------------------------------
class _FakeDB:
    def __init__(self, pending, threads):
        self.pending = list(pending)
        self.threads = threads

    def _read_all(self, sql):
        assert _SQL_TAIL in sql, sql
        self.threads.append(threading.get_ident())
        return [{"target_session_id": sid} for sid in self.pending]


class _FakeStore:
    def __init__(self, rows, threads):
        self.rows = rows
        self.threads = threads

    def list_sessions(self):
        self.threads.append(threading.get_ident())
        return [SimpleNamespace(session_id=sid, origin=origin) for sid, origin in self.rows]


class _FakeRunner:
    def __init__(self, rows, threads):
        self.threads = threads
        self.config = SimpleNamespace(multiplex_profiles=False)
        self.session_store = _FakeStore(rows, threads)

    def _adapter_for_source(self, origin):
        self.threads.append(threading.get_ident())
        return f"adapter:{origin}"


class _FakeAuthority:
    def __init__(self, pending, threads, runner):
        self.threads = threads
        self.profile_id = "home"
        self.sessions: dict = {}
        self.runner = runner  # ``owner_scope`` reads ``runner.config``
        self.db = _FakeDB(pending, threads)
        self.recovery: tuple[list, int] | None = None

    def logical_owner(self, session_id):
        self.threads.append(threading.get_ident())
        return session_id

    async def recover_native_sessions(self, bindings):
        self.recovery = (list(bindings), threading.get_ident())
        return {sid: "ready" for sid, _, _ in bindings}


def _harness(pending=("s1",), rows=(("s1", "qq"),)):
    threads: list[int] = []
    runner = _FakeRunner(rows, threads)
    return runner, _FakeAuthority(pending, threads, runner)


def _media_module(calls):
    module = SimpleNamespace()

    def collect_unheld_api_images(db):
        calls.append(db)
        return 3

    module.collect_unheld_api_images = collect_unheld_api_images
    return module


def _media_module_wrong_shape(calls):
    module = SimpleNamespace()

    async def collect_unheld_api_images(db):  # pragma: no cover - shape mismatch only
        calls.append(db)

    module.collect_unheld_api_images = collect_unheld_api_images
    return module


def _runtime_module():
    """A stock-shaped ``run_runtime`` double: binding inline, recovery under ``owner_scope``."""
    module = SimpleNamespace()

    async def recover_profile_native_sessions(runner, authority):
        from gateway.session_authorities import owner_scope
        with owner_scope(authority):
            pending = {row['target_session_id'] for row in authority.db._read_all(
                "SELECT DISTINCT target_session_id FROM session_admissions WHERE status IN ('queued','unknown')")}
            bindings = [(owner, entry.origin, runner._adapter_for_source(entry.origin))
                        for entry in runner.session_store.list_sessions() if entry.origin is not None
                        for owner in [authority.logical_owner(entry.session_id)] if owner in pending]
            outcome = await authority.recover_native_sessions(bindings)
            from gateway.session_detached import recover_detached_sessions
            outcome.update(dict.fromkeys(recover_detached_sessions(authority, pending - set(outcome)), 'detached'))
        return outcome

    module.recover_profile_native_sessions = recover_profile_native_sessions
    return module


async def _core_patched_recover(runner, authority):
    """A core that already carries PR #148062: binding off-loop, recovery back on the loop."""
    _, bindings = await asyncio.to_thread(lambda r, a: (set(), []), runner, authority)
    return await authority.recover_native_sessions(bindings)


def _core_binding_stage(runner, authority):
    return set(), [(sid, origin, runner._adapter_for_source(origin))
                   for sid, origin in (("s1", "qq"),)]


async def _core_extracted_not_threaded(runner, authority):
    """A core that extracted the binding stage but still calls it on the loop."""
    _, bindings = _core_binding_stage(runner, authority)
    return await authority.recover_native_sessions(bindings)


class MediaGuardTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        patches._loop_skip_logged = False

    async def test_event_loop_call_skips_original_and_logs_once(self):
        calls = []
        module = _media_module(calls)
        self.assertEqual(patches.install(media_module=module, runtime_module=_runtime_module()).media,
                         "installed")
        with self.assertLogs(LOGGER, level="WARNING") as logs:
            self.assertEqual(module.collect_unheld_api_images("db-a"), 0)
            self.assertEqual(module.collect_unheld_api_images("db-b"), 0)
        self.assertEqual(calls, [])
        self.assertEqual(len(logs.records), 1)
        self.assertIn("skipped the startup retained-API-image sweep", logs.records[0].getMessage())

    async def test_worker_thread_call_runs_original(self):
        calls = []
        module = _media_module(calls)
        patches.install(media_module=module, runtime_module=_runtime_module())
        self.assertEqual(await asyncio.to_thread(module.collect_unheld_api_images, "db-c"), 3)
        self.assertEqual(calls, ["db-c"])

    async def test_reinstall_stacks_nothing(self):
        calls = []
        module = _media_module(calls)
        runtime = _runtime_module()
        patches.install(media_module=module, runtime_module=runtime)
        first_media, first_native = module.collect_unheld_api_images, runtime.recover_profile_native_sessions
        state = patches.install(media_module=module, runtime_module=runtime)
        self.assertEqual(state.media, "already-installed")
        self.assertEqual(state.native, "already-installed")
        self.assertIs(module.collect_unheld_api_images, first_media)
        self.assertIs(runtime.recover_profile_native_sessions, first_native)
        self.assertEqual(await asyncio.to_thread(module.collect_unheld_api_images, "db-d"), 3)
        self.assertEqual(calls, ["db-d"])  # one wrapper layer, one original call

    async def test_foreign_patch_is_refused(self):
        module = _media_module([])
        setattr(module.collect_unheld_api_images, patches._MARK, "someone-else/9")
        with self.assertRaises(RuntimeError) as caught:
            patches.install(media_module=module, runtime_module=_runtime_module())
        self.assertIn("someone-else/9", str(caught.exception))

    async def test_unrecognized_shape_raises(self):
        module = _media_module_wrong_shape([])
        with self.assertRaises(RuntimeError):
            patches.install(media_module=module, runtime_module=_runtime_module())


class NativeBindingTest(unittest.IsolatedAsyncioTestCase):
    async def test_binding_on_worker_thread_recovery_on_loop(self):
        runner, authority = _harness()
        module = _runtime_module()
        self.assertEqual(patches.install(media_module=_media_module([]), runtime_module=module).native,
                         "installed")
        loop_thread = threading.get_ident()
        outcome = await module.recover_profile_native_sessions(runner, authority)
        self.assertEqual(outcome, {"s1": "ready"})
        self.assertTrue(authority.threads)
        self.assertTrue(all(ident != loop_thread for ident in authority.threads),
                        f"binding touched the loop thread: {authority.threads}")
        self.assertIsNotNone(authority.recovery)
        assert authority.recovery is not None
        self.assertEqual(authority.recovery[0], [("s1", "qq", "adapter:qq")])
        self.assertEqual(authority.recovery[1], loop_thread)  # recovery is back on the loop

    async def test_outcome_matches_stock_function(self):
        stock_runner, stock_authority = _harness()
        patched_runner, patched_authority = _harness()
        stock = _runtime_module().recover_profile_native_sessions
        module = _runtime_module()
        patches.install(media_module=_media_module([]), runtime_module=module)
        self.assertEqual(
            await module.recover_profile_native_sessions(patched_runner, patched_authority),
            await stock(stock_runner, stock_authority))

    async def test_core_already_patched_is_deferred(self):
        module = SimpleNamespace(recover_profile_native_sessions=_core_patched_recover,
                                 _native_recovery_bindings=_core_binding_stage)
        before = module.recover_profile_native_sessions
        state = patches.install(media_module=_media_module([]), runtime_module=module)
        self.assertEqual(state.native, "deferred-core-patched")
        self.assertIs(module.recover_profile_native_sessions, before)

    async def test_extracted_stage_is_threaded_and_reused(self):
        module = SimpleNamespace(recover_profile_native_sessions=_core_extracted_not_threaded,
                                 _native_recovery_bindings=patches._bindings)
        self.assertEqual(patches.install(media_module=_media_module([]), runtime_module=module).native,
                         "installed")
        runner, authority = _harness()
        loop_thread = threading.get_ident()
        outcome = await module.recover_profile_native_sessions(runner, authority)
        self.assertEqual(outcome, {"s1": "ready"})
        self.assertTrue(all(ident != loop_thread for ident in authority.threads))
        assert authority.recovery is not None
        self.assertEqual(authority.recovery[1], loop_thread)

    async def test_unrecognized_shape_raises(self):
        module = SimpleNamespace(recover_profile_native_sessions=lambda runner: None)
        with self.assertRaises(RuntimeError):
            patches.install(media_module=_media_module([]), runtime_module=module)

    async def test_unknown_body_is_refused(self):
        async def recover_profile_native_sessions(runner, authority):
            return {}

        module = SimpleNamespace(recover_profile_native_sessions=recover_profile_native_sessions)
        with self.assertRaises(RuntimeError):
            patches.install(media_module=_media_module([]), runtime_module=module)


class RealModuleInstallTest(unittest.IsolatedAsyncioTestCase):
    """The install contract against the Hermes checkout on PYTHONPATH."""

    async def test_real_install_is_idempotent_and_recognizes_the_core(self):
        from gateway import run_runtime, session_ingress_media
        original_media = session_ingress_media.collect_unheld_api_images
        original_native = run_runtime.recover_profile_native_sessions
        try:
            first = patches.install()
            media_now = session_ingress_media.collect_unheld_api_images
            native_now = run_runtime.recover_profile_native_sessions
            second = patches.install()
            self.assertEqual(first.media, "installed")
            self.assertEqual(second.describe(), f"media=already-installed native={second.native}")
            self.assertIs(session_ingress_media.collect_unheld_api_images, media_now)
            self.assertIs(run_runtime.recover_profile_native_sessions, native_now)
            # This checkout carries PR #148062 whole-file; a stock core reports "installed".
            self.assertIn(first.native, {"deferred-core-patched", "installed"})
        finally:
            session_ingress_media.collect_unheld_api_images = original_media
            run_runtime.recover_profile_native_sessions = original_native


if __name__ == "__main__":
    unittest.main()
