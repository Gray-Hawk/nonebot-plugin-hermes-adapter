"""Focused tests for the session-compress user plugin.

These pin the thin wiring, not the compressor (the upstream suite owns
``_run_manual_compression``/``compress_now``): auth, 404, per-session busy, in-place vs
rotation reporting, the new-id header the Runs client adopts, and the read-only
``--preview`` branch (never compresses, never leases, never writes).

Run with the Hermes checkout on PYTHONPATH, from a directory that is NOT this repository's
root (the adapter's own ``utils.py`` there would shadow Hermes' ``utils``)::

    cd /tmp && PYTHONPATH=/path/to/hermes-agent \\
        /path/to/hermes-agent/venv/bin/python -m unittest discover \\
        -s /path/to/repo/gateway_plugins/session-compress/tests -v
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

# Load the plugin directory (its manifest name has a hyphen) under an importable name.
_PLUGIN_DIR = Path(__file__).resolve().parents[1]
if "session_compress_plugin" not in sys.modules:
    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        "session_compress_plugin", _PLUGIN_DIR / "__init__.py",
        submodule_search_locations=[str(_PLUGIN_DIR)])
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["session_compress_plugin"] = _mod
    _spec.loader.exec_module(_mod)

from session_compress_plugin.http_api import register_routes  # noqa: E402

KEY = "test-key-0123456789abcdef0123456789abcdef"
AUTH = {"Authorization": f"Bearer {KEY}"}


def _msgs(count: int) -> list[dict]:
    out = []
    for i in range(count):
        out.append({"role": "user" if i % 2 == 0 else "assistant", "content": f"m{i}"})
    return out


class FakeDB:
    def __init__(self, *, exists=True, lock_holder=None, lease_ok=True):
        self.exists = exists
        self.lock_holder = lock_holder
        self.lease_ok = lease_ok
        self.released = []
        self.lease_holder = None
        # Counters let a test assert that a path neither probed the compression lock nor
        # tried to take the turn lease (0 = the branch never ran).
        self.lock_probes = 0
        self.lease_attempts = 0

    def get_session(self, session_id):
        return {"id": session_id, "source": "api_server"} if self.exists else None

    def get_compression_tip(self, session_id):
        return session_id

    def get_compression_lock_holder(self, session_id):
        self.lock_probes += 1
        return self.lock_holder

    def try_acquire_session_turn_lease(self, session_id, holder, *, ttl_seconds=300.0):
        self.lease_attempts += 1
        if self.lease_ok:
            self.lease_holder = holder
        return self.lease_ok

    def release_session_turn_lease(self, session_id, holder):
        self.released.append((session_id, holder))


class FakeStore:
    def __init__(self, transcripts, entry=None):
        self.transcripts = transcripts
        self.entry = entry
        self.loaded = []

    async def lookup_by_session_id(self, session_id):
        return self.entry

    async def load_transcript(self, session_id):
        self.loaded.append(session_id)
        return list(self.transcripts.get(session_id, []))


class FakeRunner:
    def __init__(self, *, db=None, store=None, rotate_to=None, reply="🗜️ Compressed: 6 → 3 messages"):
        self._session_db = SimpleNamespace(_db=db)
        self.async_session_store = store
        self.rotate_to = rotate_to
        self.reply = reply
        self.calls = []
        self.evicted = []
        self.store = store

    def _session_key_for_source(self, source):
        return f"agent:main:{source.platform.value}:{source.chat_type}:{source.chat_id}"

    async def _run_manual_compression(self, source, entry, history, request):
        self.calls.append({"source": source, "entry": entry, "history": history, "request": request})
        target = self.rotate_to or entry.session_id
        if self.rotate_to:
            entry.session_id = self.rotate_to
        self.store.transcripts[target] = _msgs(3)
        return self.reply

    def _evict_cached_agent(self, session_key):
        self.evicted.append(session_key)


class PluginEndpointTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._old_key = os.environ.get("API_SERVER_KEY")
        os.environ["API_SERVER_KEY"] = KEY

    def tearDown(self):
        if self._old_key is None:
            os.environ.pop("API_SERVER_KEY", None)
        else:
            os.environ["API_SERVER_KEY"] = self._old_key

    async def _client(self, runner):
        app = web.Application()
        app["gateway_runner"] = runner
        register_routes(app)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)
        return client

    def _runner(self, *, db=None, transcripts=None, entry=None, rotate_to=None, reply=None):
        db = db or FakeDB()
        store = FakeStore(transcripts or {"sess-1": _msgs(6)}, entry=entry)
        runner = FakeRunner(db=db, store=store, rotate_to=rotate_to, **({"reply": reply} if reply else {}))
        return runner, db, store

    async def test_status_requires_auth(self):
        runner, _, _ = self._runner()
        client = await self._client(runner)
        resp = await client.get("/api/plugins/session-compress/status")
        self.assertEqual(resp.status, 401)
        resp = await client.get("/api/plugins/session-compress/status", headers=AUTH)
        self.assertEqual(resp.status, 200)
        self.assertTrue((await resp.json())["runner_available"])

    async def test_unset_key_fails_closed(self):
        os.environ.pop("API_SERVER_KEY", None)
        runner, _, _ = self._runner()
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress", json={})
        self.assertEqual(resp.status, 503)

    async def test_unknown_session_is_404_and_never_compresses(self):
        runner, db, _ = self._runner(db=FakeDB(exists=False))
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/nope/compress", headers=AUTH, json={})
        self.assertEqual(resp.status, 404)
        self.assertEqual((await resp.json())["error"]["code"], "session_not_found")
        self.assertEqual(runner.calls, [])
        self.assertEqual(db.released, [])

    async def test_turn_busy_is_409(self):
        runner, db, _ = self._runner(db=FakeDB(lease_ok=False))
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress", headers=AUTH, json={})
        self.assertEqual(resp.status, 409)
        self.assertEqual((await resp.json())["error"]["code"], "session_busy")
        self.assertEqual(resp.headers.get("Retry-After"), "1")
        self.assertEqual(runner.calls, [])

    async def test_concurrent_compression_is_409(self):
        """A held per-session compression lock is its own code, not the generic turn-busy."""
        runner, db, _ = self._runner(db=FakeDB(lock_holder="pid=1:turn=x"))
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress", headers=AUTH, json={})
        self.assertEqual(resp.status, 409)
        self.assertEqual((await resp.json())["error"]["code"], "session_compressing")
        self.assertEqual(db.lock_probes, 1)
        self.assertEqual(db.lease_attempts, 0)  # refused on the lock before the lease
        self.assertEqual(runner.calls, [])

    async def test_in_place_outcome_and_lease_release(self):
        runner, db, store = self._runner()
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress",
                                 headers=AUTH, json={"focus": "keep codes"})
        self.assertEqual(resp.status, 200)
        body = await resp.json()
        self.assertEqual(body["status"], "compressed")
        self.assertFalse(body["rotated"])
        self.assertTrue(body["in_place"])
        self.assertEqual(body["session_id_after"], "sess-1")
        self.assertEqual(resp.headers["X-Hermes-Session-Id"], "sess-1")
        self.assertEqual(body["before_messages"], 6)
        self.assertEqual(body["after_messages"], 3)
        self.assertEqual(body["focus_topic"], "keep codes")
        # the shared messaging implementation was called with the session's own history
        self.assertEqual(len(runner.calls), 1)
        self.assertEqual(len(runner.calls[0]["history"]), 6)
        # lease taken then released, and no global lock involved
        self.assertTrue(db.lease_holder and "manual-compress" in db.lease_holder)
        self.assertEqual(db.released and db.released[0][0], "sess-1")
        self.assertEqual(runner.evicted, ["sess-1"])

    async def test_rotation_reports_new_id_and_header(self):
        runner, db, store = self._runner(rotate_to="child-9")
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress", headers=AUTH, json={})
        self.assertEqual(resp.status, 200)
        body = await resp.json()
        self.assertEqual(body["status"], "compressed")
        self.assertTrue(body["rotated"])
        self.assertFalse(body["in_place"])
        self.assertEqual(body["session_id_before"], "sess-1")
        self.assertEqual(body["session_id_after"], "child-9")
        self.assertEqual(resp.headers["X-Hermes-Session-Id"], "child-9")
        self.assertEqual(runner.evicted, [])  # a rotated session needs no same-id eviction

    async def test_routing_entry_origin_is_reused_when_present(self):
        entry = SimpleNamespace(session_id="sess-1", session_key="agent:main:onebotv11:group:42", origin=None)
        runner, _, _ = self._runner(entry=entry)
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress", headers=AUTH, json={})
        self.assertEqual(resp.status, 200)
        source = runner.calls[0]["source"]
        self.assertEqual(source.platform.value, "api_server")  # origin None -> synthesized source
        self.assertEqual(runner.calls[0]["entry"] is entry, True)

    async def test_not_enough_messages_makes_no_call(self):
        runner, db, _ = self._runner(transcripts={"sess-1": _msgs(2)})
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress", headers=AUTH, json={})
        self.assertEqual(resp.status, 200)
        body = await resp.json()
        self.assertEqual(body["status"], "not_enough")
        self.assertEqual(runner.calls, [])
        self.assertEqual(db.lease_attempts, 0)  # refused before any lease/lock work

    async def test_preview_is_read_only_and_never_compresses(self):
        """``--preview`` reports through the messaging helper and mutates nothing."""
        runner, db, store = self._runner()
        snapshot = list(store.transcripts["sess-1"])
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress",
                                 headers=AUTH, json={"preview": True, "focus": "keep codes"})
        self.assertEqual(resp.status, 200)
        body = await resp.json()
        self.assertEqual(body["status"], "preview")
        self.assertEqual(body["session_id_before"], "sess-1")
        self.assertEqual(body["session_id_after"], "sess-1")
        self.assertFalse(body["rotated"])
        self.assertFalse(body["in_place"])
        self.assertEqual(body["before_messages"], 6)
        self.assertEqual(body["after_messages"], 6)  # nothing was compressed
        self.assertEqual(body["focus_topic"], "keep codes")
        self.assertIn("Preview — no changes made.", body["message"])
        self.assertIn("Would compress 6 of 6 message(s)", body["message"])
        # The real compression path was never entered...
        self.assertEqual(runner.calls, [])
        self.assertEqual(runner.evicted, [])
        # ...nor fenced: no compression-lock probe, no turn lease taken or released.
        self.assertEqual(db.lock_probes, 0)
        self.assertEqual(db.lease_attempts, 0)
        self.assertIsNone(db.lease_holder)
        self.assertEqual(db.released, [])
        # ...and the stored transcript is byte-identical to what it was before.
        self.assertEqual(store.transcripts["sess-1"], snapshot)

    async def test_preview_is_not_refused_while_a_turn_is_running(self):
        """Read-only preview deliberately skips the busy fence (the messaging command does too)."""
        runner, db, store = self._runner(db=FakeDB(lease_ok=False))
        snapshot = list(store.transcripts["sess-1"])
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress",
                                 headers=AUTH, json={"preview": True})
        self.assertEqual(resp.status, 200)
        self.assertEqual((await resp.json())["status"], "preview")
        self.assertEqual(runner.calls, [])
        self.assertEqual(db.lease_attempts, 0)
        self.assertEqual(store.transcripts["sess-1"], snapshot)

    async def test_preview_on_unknown_session_is_still_404(self):
        runner, db, _ = self._runner(db=FakeDB(exists=False))
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/nope/compress",
                                 headers=AUTH, json={"preview": True})
        self.assertEqual(resp.status, 404)
        self.assertEqual(runner.calls, [])

    async def test_invalid_body_is_400(self):
        runner, _, _ = self._runner()
        client = await self._client(runner)
        resp = await client.post("/api/plugins/session-compress/sessions/sess-1/compress",
                                 headers=AUTH, data="not json")
        self.assertEqual(resp.status, 400)


if __name__ == "__main__":
    unittest.main()
