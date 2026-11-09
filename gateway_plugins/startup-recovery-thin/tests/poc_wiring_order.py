"""Isolated PoC: the plugin is installed BEFORE startup recovery calls its targets.

Runs in its own process with a throwaway ``HERMES_HOME`` (bundled plugins pointed at an empty
dir), binds an ephemeral loopback port, and never touches the main instance or 肥鱼. It proves,
with the REAL gateway seam code:

1. discovery loads ``startup-recovery-thin`` from ``$HERMES_HOME/plugins`` and registers the
   ``api_server`` factory (``hermes_cli.plugins``);
2. ``APIServerAdapter.connect()`` wires that factory (``BasePlatformAdapter._wire_plugin_handlers``
   is called from inside ``connect``) -- the plugin is installed when connect returns;
3. ``GatewayAdapterLifecycleMixin._publish_primary_adapter`` then calls the real
   ``gateway.session_api_turn.recover_api_turns``, whose call-time import now resolves the
   wrapper: the startup image sweep is skipped on the loop (db untouched, one log line);
4. a genuine STOCK ``run_runtime.py`` (``git show HEAD:gateway/run_runtime.py``) patched by the
   plugin binds in a worker thread while ``recover_native_sessions`` returns to the loop, and the
   outcome equals the unpatched stock function's.

Usage (Hermes checkout on PYTHONPATH, from a directory that is not this repo's root)::

    cd /tmp && PYTHONPATH=/path/to/hermes-agent \
        /path/to/hermes-agent/venv/bin/python \
        /path/to/repo/gateway_plugins/startup-recovery-thin/tests/poc_wiring_order.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

PLUGIN_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = PLUGIN_DIR.parents[1]
HOME = Path(tempfile.mkdtemp(prefix="startup-recovery-thin-poc-"))
os.environ["HERMES_HOME"] = str(HOME)
os.environ.pop("HERMES_PROFILE", None)
empty_plugins = HOME / "empty-bundled"
empty_plugins.mkdir()
os.environ["HERMES_BUNDLED_PLUGINS"] = str(empty_plugins)
os.environ["API_SERVER_KEY"] = secrets.token_hex(32)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logging.getLogger("gateway").setLevel(logging.WARNING)
events: list[str] = []


def note(message: str) -> None:
    events.append(message)
    print(f"[poc] {message}", flush=True)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# ---------------------------------------------------------------------------
# 1. isolated home + plugin discovery through the real plugin manager
# ---------------------------------------------------------------------------
shutil.copytree(PLUGIN_DIR, HOME / "plugins" / "startup-recovery-thin",
                ignore=shutil.ignore_patterns("tests", "__pycache__"))
(HOME / "config.yaml").write_text(
    "plugins:\n  enabled:\n    - startup-recovery-thin\n", encoding="utf-8")

from hermes_constants import get_hermes_home  # noqa: E402
from hermes_cli.plugins import discover_plugins, get_plugin_manager  # noqa: E402


def state_marker(fn):
    return getattr(fn, "__startup_recovery_thin__", None)


assert Path(get_hermes_home()).resolve() == HOME.resolve(), get_hermes_home()
note(f"isolated HERMES_HOME {HOME}")
discover_plugins()
manager = get_plugin_manager()
assert "startup-recovery-thin" in manager._plugins, sorted(manager._plugins)
assert not manager._plugins["startup-recovery-thin"].error, manager._plugins["startup-recovery-thin"].error
factories = manager.get_platform_handler_factories("api_server")
assert [name for _, name in factories] == ["startup-recovery-thin"], factories
note(f"plugin discovered; api_server factories={[name for _, name in factories]}")

# ---------------------------------------------------------------------------
# 2. a real APIServerAdapter.connect() wires the factory before returning
# ---------------------------------------------------------------------------
from gateway.config import Platform, PlatformConfig  # noqa: E402
from gateway.platforms.api_server import APIServerAdapter  # noqa: E402
from gateway.run_adapters import GatewayAdapterLifecycleMixin  # noqa: E402
from gateway import session_api_turn, session_ingress_media  # noqa: E402

adapter = APIServerAdapter(PlatformConfig(
    enabled=True, extra={"host": "127.0.0.1", "port": _free_port()}))

# ``db_path`` is only read by the ORIGINAL collect_unheld_api_images; a skipped call leaves it 0.
class PoCDB:
    def __init__(self):
        self.touches: list[str] = []

    @property
    def db_path(self):
        self.touches.append("db_path")
        return str(HOME / "state.db")

    @contextmanager
    def _read_ctx(self):
        self.touches.append("_read_ctx")
        conn = SimpleNamespace(execute=lambda *a, **k: [])
        yield conn


class PoCAuthority:
    def __init__(self):
        self.profile_id = "launch"
        self.sessions: dict = {}
        self.db = PoCDB()
        self.runner = SimpleNamespace(config=SimpleNamespace(multiplex_profiles=False))


authority = PoCAuthority()
adapter.gateway_runner = SimpleNamespace(session_authority=authority)
marker_at_connect_close = None


async def main() -> None:
    global marker_at_connect_close
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    assert await adapter.connect() is True, "api_server adapter did not connect"
    marker_at_connect_close = state_marker(session_ingress_media.collect_unheld_api_images)
    note(f"connect() returned; install marker on collect_unheld_api_images={marker_at_connect_close}")
    assert marker_at_connect_close is not None, "plugin factory did not install before connect returned"
    events.append("connect")

    # compact_settled_api_payloads is imported at call time by recover_authority_api_turns; record
    # that the real recovery reached it AFTER the install (and after the skipped sweep).
    import gateway.session_api_media as session_api_media

    original_compact = session_api_media.compact_settled_api_payloads

    def compact_probe(db, admission_id=None):
        note(f"recover_api_turns reached compact_settled_api_payloads (install marker={marker_at_connect_close})")
        events.append("compact")
        return 0

    session_api_media.compact_settled_api_payloads = compact_probe
    try:
        stub = SimpleNamespace(adapters={}, _sync_voice_mode_state_to_adapter=lambda a: None,
                               _bind_voice_input_callback=lambda a: None)
        GatewayAdapterLifecycleMixin._publish_primary_adapter(stub, Platform.API_SERVER, adapter)
        await asyncio.sleep(0)  # recover_api_turns is synchronous; let its log flush
        note(f"publish+recover done; events={events[-3:]}; db touches={authority.db.touches}")
        assert "compact" in events, "real recover_api_turns never ran"
        # Only the queued-targets read remains; ``db_path`` is read solely by the ORIGINAL sweep.
        assert "db_path" not in authority.db.touches, f"startup sweep touched the store: {authority.db.touches}"
    finally:
        session_api_media.compact_settled_api_payloads = original_compact
        await adapter.disconnect()

    # -----------------------------------------------------------------------
    # 3. a genuine STOCK run_runtime.py, patched by the plugin, binds off-loop
    # -----------------------------------------------------------------------
    stock_source = subprocess.run(
        ["git", "-C", str(Path(sys.modules["gateway"].__file__).resolve().parents[1]),
         "show", "HEAD:gateway/run_runtime.py"],
        check=True, capture_output=True, text=True).stdout
    stock_dir = HOME / "stock"
    stock_dir.mkdir()
    for name in ("stock_plain", "stock_patched"):
        (stock_dir / f"{name}.py").write_text(stock_source, encoding="utf-8")
    sys.path.insert(0, str(stock_dir))
    import stock_plain  # noqa: E402
    import stock_patched  # noqa: E402
    from hermes_plugins.startup_recovery_thin import patches  # noqa: E402

    note(f"stock run_runtime.py has _native_recovery_bindings="
         f"{hasattr(stock_plain, '_native_recovery_bindings')}")
    assert not hasattr(stock_plain, "_native_recovery_bindings"), "expected the unpatched stock shape"
    state = patches.install(media_module=SimpleNamespace(collect_unheld_api_images=lambda db: 0),
                            runtime_module=stock_patched)
    note(f"stock install state: {state.describe()}")
    assert state.native == "installed", state

    def harness():
        threads: list[int] = []
        runner = SimpleNamespace(
            config=SimpleNamespace(multiplex_profiles=False),
            session_store=SimpleNamespace(list_sessions=lambda: [SimpleNamespace(session_id="s1", origin="qq")]),
            _adapter_for_source=lambda origin: (threads.append(threading.get_ident()), "adapter")[1])
        holder = SimpleNamespace(threads=threads, profile_id="home", sessions={}, runner=runner,
                                 recovery=None, db=SimpleNamespace(
                                     _read_all=lambda sql: (threads.append(threading.get_ident()),
                                                            [{"target_session_id": "s1"}])[1]),
                                 logical_owner=lambda sid: (threads.append(threading.get_ident()), sid)[1])
        async def recover(bindings):
            holder.recovery = (list(bindings), threading.get_ident())
            return {sid: "ready" for sid, _, _ in bindings}
        holder.recover_native_sessions = recover
        return runner, holder

    loop_thread = threading.get_ident()
    stock_runner, stock_authority = harness()
    stock_outcome = await stock_plain.recover_profile_native_sessions(stock_runner, stock_authority)
    note(f"unpatched stock: outcome={stock_outcome} binding threads={stock_authority.threads} "
         f"(loop={loop_thread})")

    patched_runner, patched_authority = harness()
    patched_outcome = await stock_patched.recover_profile_native_sessions(patched_runner, patched_authority)
    note(f"patched stock:   outcome={patched_outcome} binding threads={patched_authority.threads} "
         f"recovery thread={patched_authority.recovery[1]} (loop={loop_thread})")
    assert patched_outcome == stock_outcome, (patched_outcome, stock_outcome)
    assert patched_authority.threads and loop_thread not in patched_authority.threads, patched_authority.threads
    assert patched_authority.recovery[1] == loop_thread, patched_authority.recovery
    note("PASS: stock core binding moved off the loop, outcome unchanged, recovery on the loop")

    # The live core here already carries PR #148062: install() defers instead of stacking.
    live_state = patches.install()
    note(f"live core install: {live_state.describe()}")
    assert live_state.native == "deferred-core-patched", live_state


asyncio.run(main())
print("[poc] ALL PASS", flush=True)
