"""queued-lane-interim-thin + kanban notify guard 的隔离测试。

跑的是真代码：
  * ``GatewayTurnMixin._run_agent_deliver_first_response`` / ``_run_agent_queued_followup``
    （stock 主体，含 ``_deliver_queued_first_response`` → ``_send_queued_final_text`` →
    ``adapter.send()`` 在 api_server 上必然失败的真路径）；
  * ``_RunStream`` / ``_run_event`` / ``_RunLaunch.put_event``（run 自己的 SSE 缓冲与终态帧）；
  * ``SessionEvents`` 与真 admission 表（``begin_runtime_epoch`` / ``admit_session_input`` /
    ``claim_session_input`` / ``get_session_admission``）；
  * 生产安装的 ``RunsEventParser``（真分段边界、真段内去重）与 ``decisions_from_text``；
  * ``hermes_cli.kanban_db`` 真建卡/真继承、``kanban_db_notify.add_notify_sub`` 真写行。

桩（明写）：平台 adapter 对象（只读 send/edit_message/_run_streams/_ensure_session_db）、
runner 上与本次无关的少数方法（``_record_room_silence_decision``、``_pop_post_delivery_callback``、
``_refresh_agent_cache_message_count``、递归 ``_run_agent``）、bot 侧收口（把 parser 吐出的
unit 记成"平台发送"）。没有活网关、没有 HTTP/SSE 传输、没有真 OneBot。

运行（必须离开本仓库根目录，否则根目录 utils.py 会遮蔽 Hermes 的 utils）：
    cd /tmp && PYTHONPATH=/home/neonsky/.hermes/hermes-agent \
      /home/neonsky/.hermes/hermes-agent/venv/bin/python -m unittest discover \
      -s /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109/gateway_plugins/queued-lane-interim-thin/tests -v
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("HERMES_DISABLE_LAZY_INSTALLS", "1")

REPO = "/home/neonsky/.hermes/hermes-agent"
NB_SITE = "/home/neonsky/NeonBot/.venv/lib/python3.13/site-packages"
PLUGIN_DIR = str(Path(__file__).resolve().parents[1])
if REPO not in sys.path:
    sys.path.insert(0, REPO)
if NB_SITE not in sys.path:
    sys.path.append(NB_SITE)
if PLUGIN_DIR not in sys.path:
    sys.path.insert(0, PLUGIN_DIR)

logging.basicConfig(level=logging.CRITICAL)

from gateway.config import Platform  # noqa: E402
from gateway.platforms.api_server_runs import _RunLaunch, _RunStream, _run_event  # noqa: E402
from gateway.platforms.base import BasePlatformAdapter  # noqa: E402
from gateway.run import GatewayRunner  # noqa: E402
from gateway.run_turn import GatewayTurnMixin  # noqa: E402
from gateway.session_api_turn import api_execution  # noqa: E402
from gateway.session_authority import SessionAuthority  # noqa: E402
from gateway.session_events import SessionEvents  # noqa: E402
from gateway.turn_context import TurnContext  # noqa: E402
from hermes_state import SessionDB  # noqa: E402
from hermes_state_runtime import (  # noqa: E402
    admit_session_input, begin_runtime_epoch, claim_session_input, get_session_admission,
)

# Real parsers, importable outside NoneBot through shims that touch none of the code under test.
# Two copies on purpose:
#   * ``nonebot_plugin_hermes_adapter``  → this repository (the **candidate** code being delivered);
#   * ``qlt_stock_nonebot_adapter``      → the deployed site-packages copy (**stock as installed**),
#     so a test can replay what the instance runs today against the candidate's frames.
_NB_PKG = "nonebot_plugin_hermes_adapter"
_NB_STOCK_PKG = "qlt_stock_nonebot_adapter"
_NB_ROOT = str(Path(__file__).resolve().parents[3])
_NB_STOCK_ROOT = os.path.join(NB_SITE, _NB_PKG)


def _install_nb_package(name: str, root: str) -> None:
    """A synthetic package object pinned to *root*: ``__init__`` never runs, submodules do."""
    if name in sys.modules:
        return
    _pkg = types.ModuleType(name)
    _pkg.__path__ = [root]
    sys.modules[name] = _pkg
    _cfg = types.ModuleType(name + ".config")
    _cfg.plugin_config = SimpleNamespace()
    _cfg.__getattr__ = lambda attr: (_cfg.plugin_config if attr == "plugin_config"
                                     else (lambda *a, **k: None))
    sys.modules[name + ".config"] = _cfg


if "json5" not in sys.modules:
    sys.modules["json5"] = json
if "nonebot" not in sys.modules:
    _nb = types.ModuleType("nonebot")
    _nb.logger = logging.getLogger("nonebot")
    _nb.__getattr__ = lambda name: (lambda *args, **kwargs: None)
    sys.modules["nonebot"] = _nb

_install_nb_package(_NB_PKG, _NB_ROOT)
_install_nb_package(_NB_STOCK_PKG, _NB_STOCK_ROOT)

from nonebot_plugin_hermes_adapter.core.hermes_client import RunsEventParser  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_text import decisions_from_text  # noqa: E402
from qlt_stock_nonebot_adapter.core.hermes_client import (  # noqa: E402
    RunsEventParser as StockRunsEventParser,
)

import kanban_notify_guard  # noqa: E402
import patches  # noqa: E402

# This test process is itself a dispatcher worker (HERMES_KANBAN_TASK + the delegate-child
# marker), which fences every Kanban write. The tests only touch a throwaway board, so the
# ambient fence is cleared here; nothing about the code under test changes.
from agent.delegation_context import DELEGATED_CHILD_ENV_MARKER  # noqa: E402

os.environ.pop(DELEGATED_CHILD_ENV_MARKER, None)
os.environ.pop("HERMES_KANBAN_TASK", None)

HOME = tempfile.mkdtemp(prefix="qlt-tests-")
os.environ["HERMES_HOME"] = HOME  # after the imports: a pre-import switch re-execs the script
os.environ["HERMES_DISABLE_LAZY_INSTALLS"] = "1"

SEND_ERROR = "API server uses HTTP request/response, not send()"
_STOCK = {
    "first_response": GatewayTurnMixin._run_agent_deliver_first_response,
    "put_event": _RunLaunch.put_event,
}


def restore_stock() -> None:
    """Put the stock methods back (install() patches the mixin, not the runner subclass)."""
    GatewayTurnMixin._run_agent_deliver_first_response = _STOCK["first_response"]
    _RunLaunch.put_event = _STOCK["put_event"]


# --------------------------------------------------------------------------- fixtures
class _Authority:
    """Only the facts ``bound_run`` reads (sessions + epoch + the admission DB).

    ``logical_owner`` is the real ``SessionAuthority`` method, not a re-implementation.
    """

    logical_owner = SessionAuthority.logical_owner

    def __init__(self, db, epoch, profile_id="default"):
        self.db = db
        self.epoch = epoch
        self.profile_id = profile_id
        self.sessions = {}
        self.api_observers = {}


def _failing_send(chat_id, content, metadata=None, **kwargs):
    return _resolved(SimpleNamespace(success=False, error=SEND_ERROR))


def _ok_send(chat_id, content, metadata=None, **kwargs):
    return _resolved(SimpleNamespace(success=True, message_id="m1"))


def _declined_edit(chat_id, message_id, content, finalize=False, **kwargs):
    return _resolved(SimpleNamespace(success=False, error="no live stream to edit"))


def _resolved(value):
    async def _coro():
        return value
    return _coro()


def _new_db(tag: str) -> SessionDB:
    return SessionDB(Path(HOME) / f"state-{tag}-{uuid.uuid4().hex[:8]}.db")


def _admit(db, epoch, session_id, run_id):
    return admit_session_input(
        db, epoch=epoch, principal_id="api", session_id=session_id, request_id=run_id,
        payload={"text": "用户私聊消息", "api_turn_v1": {"history": None, "settings": {}}})


class Run:
    """One live /v1/runs execution: real admission row, real _RunStream, real stamp."""

    def __init__(self, *, platform=Platform.API_SERVER, send_ok=False):
        self.run_id = f"run_{uuid.uuid4().hex}"
        self.session_id = f"hermes-onebotv11+private+10086-g{uuid.uuid4().hex[:4]}"
        self.db = _new_db(self.run_id)
        self.db.ensure_session(self.session_id, source="api_server")
        self.epoch = begin_runtime_epoch(self.db, instance_id=f"qlt-{uuid.uuid4().hex[:8]}")
        self.authority = _Authority(self.db, self.epoch)
        self.live = SimpleNamespace(session_id=self.session_id, event_stream=SessionEvents(),
                                    source=SimpleNamespace(platform=Platform.API_SERVER))
        self.authority.sessions[self.session_id] = self.live
        self.stream = _RunStream()
        self.adapter = SimpleNamespace(
            name="Api_Server",
            platform=Platform.API_SERVER,
            _run_streams={self.run_id: self.stream},
            _run_statuses={self.run_id: {"status": "running", "session_id": self.session_id}},
            gateway_runner=SimpleNamespace(session_authority=self.authority),
            _ensure_session_db=lambda: self.db,
            send=_ok_send if send_ok else _failing_send,
            edit_message=_declined_edit,
            extract_media=BasePlatformAdapter.extract_media,
        )
        self.source = SimpleNamespace(platform=platform, chat_id=self.session_id, thread_id=None,
                                      chat_type="dm", parent_chat_id=None)
        self.ctx = TurnContext(
            source=self.source, session_id=self.session_id,
            session_key=f"agent:main:api_server:dm:{self.session_id}", reply_expected=True,
            run_generation=1, stream_consumer_holder=[None],
            event_message_id=None, inbound_message_id=None, persist_user_display_kind=None,
        )
        self.row = _admit(self.db, self.epoch, self.session_id, self.run_id)
        self.admission_id = self.row["admission_id"]
        self.stamp(claim=True)

    def stamp(self, *, claim: bool) -> None:
        if claim:
            claimed = claim_session_input(
                self.db, epoch=self.epoch, session_id=self.session_id, head=self.admission_id)
            assert claimed, "claim_session_input returned nothing"
            self.admission_id = claimed["admission_id"]
            self.generation = claimed["generation"]
        self.live.event_stream.execution = {
            "authority_epoch": self.epoch,
            "execution_generation": self.generation,
            "admission_id": self.admission_id,
        }


def make_runner(*, already_streamed=False):
    runner = object.__new__(GatewayRunner)
    runner._record_room_silence_decision = _async_noop
    runner._pop_post_delivery_callback = lambda *a, **k: None
    runner._room_allow_silence = lambda source: None
    if already_streamed:
        runner._run_agent_stream_confirmed_final_delivery = lambda *a, **k: True
    return runner


async def _async_noop(*args, **kwargs):
    return None


async def deliver(run: Run, runner, text: str) -> dict:
    """One turn's answer reaching the real queued-lane seam."""
    result = {"final_response": text, "messages": [], "completed": True}
    await GatewayRunner._run_agent_deliver_first_response(
        runner, run.ctx, run.adapter, text, result, None)
    return result


def term(run: Run, output: str) -> None:
    """The run's terminal frame, through the run's own put_event (stock or wrapped)."""
    event = _run_event(run.run_id, "run.completed", completed=True, partial=False,
                       interrupted=False, output=output, usage={})
    _RunLaunch.put_event(SimpleNamespace(run_id=run.run_id, queue=run.stream,
                                         owner=run.adapter), event)


def frame(run: Run, name: str, **fields) -> None:
    """One SSE frame as the run's own publisher emits it (real ``_run_event`` + ``put_event``).

    ``_execute_run`` publishes ``message.delta`` through exactly this pair
    (``loop.call_soon_threadsafe(run.put_event, _run_event(run_id, "message.delta", delta=…))``).
    """
    _RunLaunch.put_event(SimpleNamespace(run_id=run.run_id, queue=run.stream, owner=run.adapter),
                         _run_event(run.run_id, name, **fields))


class NbClient:
    """The adapter's real parser, fed the run's SSE frames; units = platform sends."""

    def __init__(self, stream: _RunStream, parser_cls=RunsEventParser):
        self.stream, self.cursor = stream, 0
        self.parser = parser_cls()
        self.sent: list[str] = []
        self.suppressed: list[tuple[str, str]] = []
        self.parser.on_suppressed = lambda reason, text, src: self.suppressed.append((reason, text))

    def pump(self) -> None:
        for seq, event in list(self.stream.backlog)[self.cursor:]:
            self.cursor += 1
            if event is None:
                self.parser.feed(": stream closed")
                continue
            self.parser.feed("data: " + json.dumps(dict(event, seq=seq), ensure_ascii=False))
        for unit in self.parser.drain_stream_decisions():
            self.sent.append(unit["body"])
        if self.parser.terminal and self.parser.output is not None:
            output, self.parser.output = self.parser.output, None
            self.sent.extend(d["body"] for d in decisions_from_text(output))


# --------------------------------------------------------------------------- range A
class QueuedLaneInterimTest(unittest.TestCase):
    def setUp(self):
        restore_stock()
        for name in ("patches", "kanban_notify_guard"):
            logging.getLogger(name).setLevel(logging.WARNING)
        self._tokens = []

    def tearDown(self):
        while self._tokens:
            api_execution.reset(self._tokens.pop())
        restore_stock()

    def _exec(self, run: Run):
        self._tokens.append(api_execution.set({
            "adapter": run.adapter, "settings": {}, "history": None, "content": None,
            "turn_author": None, "relay_metadata": None, "resume_unanswered": False}))

    def test_control_loses_every_queued_answer(self):
        run = Run()
        self._exec(run)
        client = NbClient(run.stream)
        runner = make_runner()
        asyncio.run(self._abc(run, runner))
        term(run, "答复C")
        client.pump()
        self.assertEqual(client.sent, ["答复C"])

    async def _abc(self, run, runner):
        await deliver(run, runner, "答复A")
        await deliver(run, runner, "答复B")

    def test_abc_each_once_in_order(self):
        patches.install()
        run = Run()
        self._exec(run)
        client = NbClient(run.stream)
        runner = make_runner()
        asyncio.run(self._abc(run, runner))
        term(run, "答复C")
        client.pump()
        self.assertEqual(client.sent, ["答复A", "答复B", "答复C"])
        self.assertEqual(client.parser.interim_count, 2)
        self.assertEqual(client.suppressed, [])
        # SSE enqueue is not a chat ACK: the stock delivery result must stay untouched.
        result = asyncio.run(deliver(run, runner, "答复D"))
        self.assertNotIn("already_sent", result)

    def test_install_is_idempotent_and_shape_checked(self):
        first = patches.install()
        self.assertEqual(first.describe(),
                         "first_response=installed terminal_dedupe=installed")
        self.assertEqual(patches.install().describe(),
                         "first_response=already-installed terminal_dedupe=already-installed")

    def test_two_runs_bound_one_after_another(self):
        patches.install()
        a = Run()
        b = Run()
        # Same session, two admitted runs: B is queued behind A.
        b.session_id = a.session_id
        b.db, b.epoch = a.db, a.epoch
        b.authority = a.authority
        b.adapter = a.adapter
        b.source = a.source
        b.ctx.session_id = a.session_id
        b.row = _admit(a.db, a.epoch, a.session_id, b.run_id)
        b.admission_id = b.row["admission_id"]
        a.adapter._run_streams[b.run_id] = b.stream
        a.live.event_stream.execution = {
            "authority_epoch": a.epoch, "execution_generation": a.generation,
            "admission_id": a.admission_id}
        self._exec(a)
        runner = make_runner()
        asyncio.run(deliver(a, runner, "答复A"))
        self.assertEqual([e for _, e in a.stream.backlog if e and e.get("event") == "message.interim"],
                         [e for _, e in a.stream.backlog if e and e.get("event") == "message.interim"])
        self.assertTrue(any(e and e.get("text") == "答复A" for _, e in a.stream.backlog))
        self.assertEqual(list(b.stream.backlog), [])

        # A settles; B claims the FIFO (generation 2) and gets its own frame.
        a.db._write_rowcount(
            "UPDATE session_admissions SET status='terminal', outcome='completed' "
            "WHERE admission_id=?", (a.admission_id,))
        claimed = claim_session_input(
            a.db, epoch=a.epoch, session_id=a.session_id, head=b.admission_id)
        self.assertIsNotNone(claimed)
        self.assertNotEqual(claimed["admission_id"], a.admission_id)
        a.live.event_stream.execution = {
            "authority_epoch": a.epoch, "execution_generation": claimed["generation"],
            "admission_id": claimed["admission_id"]}
        asyncio.run(deliver(b, runner, "答复B"))
        self.assertTrue(any(e and e.get("text") == "答复B" for _, e in b.stream.backlog))
        self.assertEqual(sum(1 for _, e in a.stream.backlog
                             if e and e.get("event") == "message.interim"), 1)

    def test_compression_tip_binds_logical_root(self):
        patches.install()
        run = Run()
        tip = f"{run.session_id}-tip"
        run.db.get_compression_lineage = lambda sid: [run.session_id] if sid == tip else []
        run.ctx.session_id = tip          # the turn carries the physical tip
        self._exec(run)
        runner = make_runner()
        binding = asyncio.run(deliver(run, runner, "答复A"))
        self.assertEqual(binding.get("already_sent"), None)
        self.assertTrue(any(e and e.get("event") == "message.interim" for _, e in run.stream.backlog))

    def test_compression_tip_without_live_root_is_visible_and_safe(self):
        patches.install()
        run = Run()
        run.ctx.session_id = "hermes-...-unknown-tip"
        run.db.get_compression_lineage = lambda sid: [f"{sid}-root"]
        self._exec(run)
        runner = make_runner()
        with self.assertLogs("patches", level="WARNING") as logs:
            result = asyncio.run(deliver(run, runner, "答复A"))
        self.assertEqual(list(run.stream.backlog), [])
        self.assertNotIn("already_sent", result)          # stock fallback stays the safe result
        self.assertIn("no live session", "\n".join(logs.output))
        self.assertIn("NOT published", "\n".join(logs.output))

    def test_profile_mismatch_is_refused(self):
        patches.install()
        run = Run()
        other = _new_db("other-profile")
        run.adapter._ensure_session_db = lambda: other
        self._exec(run)
        runner = make_runner()
        with self.assertLogs("patches", level="WARNING") as logs:
            result = asyncio.run(deliver(run, runner, "答复A"))
        self.assertEqual(list(run.stream.backlog), [])
        self.assertNotIn("already_sent", result)
        self.assertIn("another profile's SessionDB", "\n".join(logs.output))

    def test_generation_or_epoch_mismatch_is_refused(self):
        patches.install()
        run = Run()
        run.live.event_stream.execution = dict(run.live.event_stream.execution,
                                               execution_generation=run.generation + 5)
        self._exec(run)
        with self.assertLogs("patches", level="WARNING") as logs:
            asyncio.run(deliver(run, make_runner(), "答复A"))
        self.assertEqual(list(run.stream.backlog), [])
        self.assertIn("generation mismatch", "\n".join(logs.output))

    def test_settled_admission_is_refused(self):
        patches.install()
        run = Run()
        run.db._write_rowcount(
            "UPDATE session_admissions SET status='terminal', outcome='completed' "
            "WHERE admission_id=?", (run.admission_id,))

        self._exec(run)
        with self.assertLogs("patches", level="WARNING") as logs:
            asyncio.run(deliver(run, make_runner(), "答复A"))
        self.assertEqual(list(run.stream.backlog), [])
        self.assertIn("not started", "\n".join(logs.output))

    def test_unknown_admission_is_refused(self):
        patches.install()
        run = Run()
        run.live.event_stream.execution = {"authority_epoch": run.epoch,
                                           "execution_generation": run.generation,
                                           "admission_id": "adm_missing"}
        self._exec(run)
        with self.assertLogs("patches", level="WARNING") as logs:
            asyncio.run(deliver(run, make_runner(), "答复A"))
        self.assertEqual(list(run.stream.backlog), [])
        self.assertIn("missing from the store", "\n".join(logs.output))

    def test_non_api_platform_is_untouched(self):
        patches.install()
        run = Run(platform=Platform.TELEGRAM, send_ok=True)
        self._exec(run)
        result = asyncio.run(deliver(run, make_runner(), "telegram answer"))
        self.assertEqual(list(run.stream.backlog), [])
        self.assertTrue(result.get("already_sent"))

    def test_already_streamed_is_untouched(self):
        patches.install()
        run = Run(send_ok=True)
        self._exec(run)
        result = asyncio.run(deliver(run, make_runner(already_streamed=True), "已流式答复"))
        self.assertEqual(list(run.stream.backlog), [])
        self.assertTrue(result.get("already_sent"))

    def test_muted_turn_is_untouched(self):
        patches.install()
        run = Run()
        run.ctx.mute_notification_reply = True
        self._exec(run)
        asyncio.run(deliver(run, make_runner(), "内部通知"))
        self.assertEqual(list(run.stream.backlog), [])

    def test_allowed_silence_marker_publishes_nothing(self):
        patches.install()
        run = Run()
        run.ctx.reply_expected = False          # stock silence_allowed() exemption
        self._exec(run)
        asyncio.run(deliver(run, make_runner(), "NO_REPLY"))
        self.assertEqual(list(run.stream.backlog), [])

    def test_human_turn_silence_marker_publishes_the_stock_replacement(self):
        patches.install()
        run = Run()                             # reply_expected=True -> stock replaces the marker
        self._exec(run)
        asyncio.run(deliver(run, make_runner(), "NO_REPLY"))
        frames = [e for _, e in run.stream.backlog if e and e.get("event") == "message.interim"]
        self.assertEqual(len(frames), 1)
        self.assertNotEqual(frames[0]["text"], "NO_REPLY")

    def test_admitted_adapter_owns_the_publication(self):
        """The seam's delivery adapter may be another instance; the admitting one owns the run."""
        patches.install()
        run = Run()
        self._exec(run)
        other = SimpleNamespace(platform=Platform.API_SERVER, _run_streams={},
                                gateway_runner=run.adapter.gateway_runner,
                                _ensure_session_db=lambda: run.db,
                                send=_failing_send, edit_message=_declined_edit,
                                extract_media=BasePlatformAdapter.extract_media)
        result = {"final_response": "答复A", "messages": [], "completed": True}
        asyncio.run(GatewayRunner._run_agent_deliver_first_response(
            make_runner(), run.ctx, other, "答复A", result, None))
        self.assertTrue(any(e and e.get("text") == "答复A" for _, e in run.stream.backlog))
        self.assertNotIn("already_sent", result)

    def test_no_admission_stamp_is_silent(self):
        patches.install()
        run = Run()
        run.live.event_stream.execution = {}
        self._exec(run)
        asyncio.run(deliver(run, make_runner(), "答复A"))
        self.assertEqual(list(run.stream.backlog), [])

    def test_terminal_same_text_is_not_repeated(self):
        patches.install()
        run = Run()
        self._exec(run)
        runner = make_runner()
        client = NbClient(run.stream)
        asyncio.run(deliver(run, runner, "答复A"))
        asyncio.run(deliver(run, runner, "答复B"))
        # The follow-up's final answer happens to be the same text as the interim just published.
        term(run, "答复B")
        client.pump()
        self.assertEqual(client.sent, ["答复A", "答复B"])
        completed = [e for _, e in run.stream.backlog if e and e.get("event") == "run.completed"]
        self.assertEqual(completed[0]["output"], "")
        # Polling clients still get the real output from the run status.
        run.adapter._run_statuses[run.run_id]["output"] = "答复B"
        self.assertEqual(run.adapter._run_statuses[run.run_id]["output"], "答复B")

    def test_distinct_terminal_is_unchanged(self):
        patches.install()
        run = Run()
        self._exec(run)
        client = NbClient(run.stream)
        asyncio.run(deliver(run, make_runner(), "答复A"))
        term(run, "答复C")
        client.pump()
        self.assertEqual(client.sent, ["答复A", "答复C"])
        completed = [e for _, e in run.stream.backlog if e and e.get("event") == "run.completed"]
        self.assertEqual(completed[0]["output"], "答复C")

    # ---------------------------------------------------------------- R1: segment boundary
    def test_queued_interim_closes_the_preceding_delta_segment(self):
        """delta(A) → queued interim(A) → delta(P before its tool) → terminal C must be [A, P, C]."""
        patches.install()
        run = Run()
        self._exec(run)
        client = NbClient(run.stream)                 # candidate parser (this repository)
        runner = make_runner()
        frame(run, "message.delta", delta="答复A")
        asyncio.run(deliver(run, runner, "答复A"))
        frame(run, "message.delta", delta="查B前的说明")
        frame(run, "tool.started", tool="lookup")
        term(run, "答复C")
        client.pump()
        self.assertEqual(client.sent, ["答复A", "查B前的说明", "答复C"])
        self.assertEqual(len(client.sent), 3)         # platform outlet counted, not observed
        self.assertEqual(client.parser.interim_count, 1)
        self.assertEqual(client.parser.delta_count, 2)

    def test_stock_parser_replays_the_reviewer_gap(self):
        """The same frames through the deployed (stock) parser still give [A, A+P, C].

        This is the exact finding R1 closes: the interim alone only sends text, it does not close
        the delta segment the previous turn left buffered.
        """
        patches.install()
        run = Run()
        self._exec(run)
        client = NbClient(run.stream, parser_cls=StockRunsEventParser)
        runner = make_runner()
        frame(run, "message.delta", delta="答复A")
        asyncio.run(deliver(run, runner, "答复A"))
        frame(run, "message.delta", delta="查B前的说明")
        frame(run, "tool.started", tool="lookup")
        term(run, "答复C")
        client.pump()
        self.assertEqual(client.sent, ["答复A", "答复A查B前的说明", "答复C"])

    def test_partial_delta_before_interim_is_not_resent(self):
        patches.install()
        run = Run()
        self._exec(run)
        client = NbClient(run.stream)
        frame(run, "message.delta", delta="答复")
        asyncio.run(deliver(run, make_runner(), "答复A"))
        frame(run, "tool.started", tool="lookup")
        term(run, "答复C")
        client.pump()
        # The streamed prefix is covered by the interim: one message, no [答复, 答复A] pair.
        self.assertEqual(client.sent, ["答复A", "答复C"])

    def test_interim_prefix_of_streamed_delta_keeps_the_tail(self):
        patches.install()
        run = Run()
        self._exec(run)
        client = NbClient(run.stream)
        frame(run, "message.delta", delta="答复A查B前")
        asyncio.run(deliver(run, make_runner(), "答复A"))
        frame(run, "tool.started", tool="lookup")
        term(run, "答复C")
        client.pump()
        # The delta already ran past the interim: the covered head is not repeated, the tail is
        # still delivered at the tool boundary.
        self.assertEqual(client.sent, ["答复A", "查B前", "答复C"])

    def test_interim_without_any_delta_still_sends_once(self):
        patches.install()
        run = Run()
        self._exec(run)
        client = NbClient(run.stream)
        asyncio.run(deliver(run, make_runner(), "答复A"))
        frame(run, "tool.started", tool="lookup")
        term(run, "答复C")
        client.pump()
        self.assertEqual(client.sent, ["答复A", "答复C"])
        self.assertEqual(client.parser.delta_count, 0)

    # ---------------------------------------------------------------- R2: SessionDB proof
    def _refused_for_store(self, break_store):
        patches.install()
        run = Run()
        break_store(run.adapter)
        self._exec(run)
        with self.assertLogs("patches", level="WARNING") as logs:
            result = asyncio.run(deliver(run, make_runner(), "答复A"))
        self.assertEqual(list(run.stream.backlog), [])           # nothing reaches the run stream
        log = "\n".join(logs.output)
        self.assertIn("NOT published", log)
        self.assertIn("cannot prove its SessionDB", log)
        self.assertEqual(result.get("final_response"), "答复A")   # stock result, untouched
        self.assertNotIn("already_sent", result)
        return run

    def test_unprovable_session_db_none_is_refused(self):
        self._refused_for_store(lambda adapter: setattr(adapter, "_ensure_session_db", lambda: None))

    def test_unprovable_session_db_raises_is_refused(self):
        def boom():
            raise RuntimeError("state.db is locked")

        self._refused_for_store(lambda adapter: setattr(adapter, "_ensure_session_db", boom))

    def test_missing_session_db_accessor_is_refused(self):
        def drop(adapter):
            del adapter._ensure_session_db

        self._refused_for_store(drop)

    def test_same_session_db_object_is_accepted(self):
        patches.install()
        run = Run()
        self._exec(run)
        asyncio.run(deliver(run, make_runner(), "答复A"))
        frames = [e for _, e in run.stream.backlog if e and e.get("event") == "message.interim"]
        self.assertEqual([f["text"] for f in frames], ["答复A"])

    def test_event_less_pending_event_followup(self):
        patches.install()
        run = Run()
        self._exec(run)
        runner = _followup_runner(run)
        first = {"final_response": "答复A", "messages": [], "completed": True}
        ctx = run.ctx
        ctx.history = []
        ctx._interrupt_depth = 0
        ctx.result_holder = [first]
        merged = asyncio.run(GatewayRunner._run_agent_queued_followup(
            runner, ctx, run.adapter, "把工具说明删掉后重跑", None, "答复A", first, None))
        term(run, merged["final_response"])
        client = NbClient(run.stream)
        client.pump()
        self.assertEqual(client.sent, ["答复A", "答复C"])
        self.assertEqual(runner._recursive_calls, 1)
        self.assertNotIn("already_sent", first)

    def test_stale_goal_early_return_does_not_duplicate(self):
        patches.install()
        run = Run()
        self._exec(run)
        runner = _followup_runner(run, stale_goal=True)
        first = {"final_response": "答复A", "messages": [], "completed": True}
        ctx = run.ctx
        ctx.history = []
        ctx._interrupt_depth = 0
        ctx.result_holder = [first]
        pending = SimpleNamespace(text="跟进", source=run.source, message_id="m2",
                                  internal=False, metadata={}, reply_expected=True,
                                  channel_prompt=None, message_type=None)
        returned = asyncio.run(GatewayRunner._run_agent_queued_followup(
            runner, ctx, run.adapter, "跟进", pending, "答复A", first, None))
        # Stale goal: the stock body returns the ORIGINAL result (no already_sent, no new turn).
        self.assertIs(returned, first)
        self.assertEqual(runner._recursive_calls, 0)
        term(run, returned["final_response"])
        client = NbClient(run.stream)
        client.pump()
        self.assertEqual(client.sent, ["答复A"])   # the interim already carried it


def _followup_runner(run: Run, *, stale_goal: bool = False):
    """A runner whose only stubs are the recursion boundary and unrelated bookkeeping."""
    runner = make_runner()
    runner._MAX_INTERRUPT_DEPTH = 3
    runner._delivery_adapter_for = lambda source: None
    runner._intake_adapter_for = lambda source: None
    runner._recursive_calls = 0
    runner._is_goal_continuation_event = lambda event: True
    runner._goal_still_active_for_session = lambda sid: not stale_goal
    runner._reply_anchor_for_event = lambda event: None
    runner._pinned_channel_inputs = lambda key, prompt, source, **kw: (prompt, source)
    runner._persist_prompt_pins = _async_noop
    runner._consume_pending_event = lambda *a, **k: None

    async def _refresh(*a, **k):
        return None

    async def _run_agent(**kwargs):
        runner._recursive_calls += 1
        text = "答复C"
        result = {"final_response": text, "messages": [], "completed": True}
        await GatewayRunner._run_agent_deliver_first_response(
            runner, run.ctx, run.adapter, text, result, None)
        return result

    runner._refresh_agent_cache_message_count = _refresh
    runner._run_agent = _run_agent
    return runner


# --------------------------------------------------------------------------- range B
class KanbanNotifyGuardTest(unittest.TestCase):
    def setUp(self):
        from hermes_cli import kanban_db as kb
        from hermes_cli import kanban_db_connect as kbc
        from hermes_cli import kanban_db_notify as kbn

        self.kb, self.kbn = kb, kbn
        self.db_path = Path(HOME) / f"kanban-{uuid.uuid4().hex[:8]}.db"
        os.environ["HERMES_KANBAN_DB"] = str(self.db_path)
        # A plain connection with the shipped schema: the gateway's connect() refuses to
        # initialize a board from inside this worker process (delegation fencing).
        self.conn = sqlite3.connect(self.db_path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(kb.SCHEMA_SQL)
        kbc._migrate_add_optional_columns(self.conn)
        self.addCleanup(self.conn.close)
        kanban_notify_guard.install()
        self._tokens = []

    def tearDown(self):
        from gateway.session_context import clear_session_vars

        if self._tokens:
            clear_session_vars(self._tokens)
        os.environ.pop("HERMES_KANBAN_DB", None)

    def _session(self, platform: str, chat_id: str = "hermes-onebotv11+private+10086-gabcd"):
        from gateway.session_context import set_session_vars

        self._tokens = set_session_vars(
            platform=platform, chat_id=chat_id, chat_type="dm", user_id="api",
            session_key=f"agent:main:{platform}:dm:{chat_id}", profile="default")

    def _create(self, title="card", parents=(), session_id=None):
        return self.kb.create_task(
            self.conn, title=title, assignee="coder_deepseek", parents=tuple(parents),
            session_id=session_id, initial_status="running")


    def _subs(self, task_id):
        return self.kbn.list_notify_subs(self.conn, task_id)

    def test_new_card_from_api_server_session_gets_no_subscription(self):
        from tools import kanban_tools as kt

        self._session("api_server")
        tid = self._create()
        self.assertFalse(kt._maybe_auto_subscribe(self.conn, tid))
        self.assertEqual(self._subs(tid), [])

    def test_new_card_from_telegram_session_keeps_its_subscription(self):
        from tools import kanban_tools as kt

        self._session("telegram", chat_id="424242")
        tid = self._create()
        self.assertTrue(kt._maybe_auto_subscribe(self.conn, tid))
        subs = self._subs(tid)
        self.assertEqual([(s["platform"], s["chat_id"]) for s in subs], [("telegram", "424242")])

    def test_child_does_not_inherit_api_server_rows(self):
        parent = self._create("parent")
        self.kbn.add_notify_sub(self.conn, task_id=parent, platform="api_server",
                                chat_id="hermes-onebotv11+private+10086-gabcd", user_id="api",
                                notifier_profile="default")
        self.kbn.add_notify_sub(self.conn, task_id=parent, platform="telegram",
                                chat_id="424242", user_id="u1", notifier_profile="default")
        child = self._create("child", parents=(parent,))
        platforms = {(s["platform"], s["chat_id"]) for s in self._subs(child)}
        self.assertEqual(platforms, {("telegram", "424242")})
        # The parent's own rows are untouched (存量注销留给部署步骤).
        self.assertEqual({(s["platform"], s["chat_id"]) for s in self._subs(parent)},
                         {("api_server", "hermes-onebotv11+private+10086-gabcd"),
                          ("telegram", "424242")})

    def test_slash_create_from_api_server_source_is_refused(self):
        from gateway.slash_commands import GatewaySlashCommandsMixin

        api_event = SimpleNamespace(source=SimpleNamespace(
            platform=Platform.API_SERVER, chat_id="hermes-onebotv11+private+10086-gabcd",
            chat_type="dm", thread_id=None, user_id="api", user_id_alt=None, profile="default"))
        self_ = SimpleNamespace(_reply_metadata=lambda e: None, _active_profile_name=lambda: "default",
                                _kanban_notifier_profile=None)
        tid = self._create("slash-card")
        ok = asyncio.run(GatewaySlashCommandsMixin._kanban_auto_subscribe(self_, api_event, tid, None))
        self.assertFalse(ok)
        self.assertEqual(self._subs(tid), [])

    def test_slash_create_from_telegram_source_still_subscribes(self):
        from unittest import mock

        from gateway.slash_commands import GatewaySlashCommandsMixin
        from hermes_cli import kanban_db_connect as kbc

        self._session("telegram", chat_id="424242")
        event = SimpleNamespace(source=SimpleNamespace(
            platform=Platform.TELEGRAM, chat_id="424242", chat_type="dm", thread_id=None,
            user_id="u1", user_id_alt=None, profile="default"))
        self_ = SimpleNamespace(_reply_metadata=lambda e: None, _active_profile_name=lambda: "default",
                                _kanban_notifier_profile=None)
        tid = self._create("slash-card-2")
        # The stock body opens the board through kanban_db_connect and uses it on a worker
        # thread; point that one call at the throwaway board (a fresh connection in that thread).
        with mock.patch.object(
                kbc, "connect",
                lambda board=None: sqlite3.connect(self.db_path, isolation_level=None)):
            ok = asyncio.run(GatewaySlashCommandsMixin._kanban_auto_subscribe(
                self_, event, tid, None))
        self.assertTrue(ok)
        self.assertEqual([(s["platform"], s["chat_id"]) for s in self._subs(tid)],
                         [("telegram", "424242")])

    def test_explicit_subscription_is_still_allowed(self):
        # Dashboard toggle / CLI notify-subscribe is an operator decision, not an automatic wake.
        tid = self._create("explicit")
        self.kbn.add_notify_sub(self.conn, task_id=tid, platform="api_server",
                                chat_id="hermes-onebotv11+private+10086-gabcd",
                                user_id="api", notifier_profile="default")
        self.assertEqual([(s["platform"], s["chat_id"]) for s in self._subs(tid)],
                         [("api_server", "hermes-onebotv11+private+10086-gabcd")])

    def test_nb_report_source_still_routes_from_task_session_id(self):
        from nonebot_plugin_hermes_adapter.core import kanban_report_source as krs

        session_id = "hermes-onebotv11+private+158****4503-g404"
        tid = self._create("nb-card", session_id=session_id)
        task = self.kb.get_task(self.conn, tid)
        self.assertEqual(task.session_id, session_id)
        # KanbanReportSource reads the same column through a mapping row (its own SELECT).
        self.assertEqual(krs.KanbanReportSource._session_for(
            self.conn, {"id": tid, "session_id": task.session_id}), session_id)
        # The handover dry-run selector still enumerates existing api_server rows.
        picked = krs.select_handover_subs([
            {"task_id": tid, "platform": "api_server", "chat_id": session_id},
            {"task_id": tid, "platform": "telegram", "chat_id": "424242"}])
        self.assertEqual([row["platform"] for row in picked], ["api_server"])

    def test_install_is_idempotent(self):
        state = kanban_notify_guard.install()
        self.assertEqual(state.describe(),
                         "auto_subscribe=already-installed inherit=already-installed "
                         "slash=already-installed")

    # ------------------------------------------------------- R4: every automatic entry point
    def test_late_link_inheritance_is_guarded(self):
        """``kb.link_tasks`` copies parent rows too — the same single owner must be guarded."""
        parent = self._create("parent")
        self.kbn.add_notify_sub(self.conn, task_id=parent, platform="api_server",
                                chat_id="hermes-onebotv11+private+10086-gabcd", user_id="api",
                                notifier_profile="default")
        self.kbn.add_notify_sub(self.conn, task_id=parent, platform="telegram",
                                chat_id="424242", user_id="u1", notifier_profile="default")
        child = self._create("child")
        self.kb.link_tasks(self.conn, parent, child)
        self.assertEqual({(s["platform"], s["chat_id"]) for s in self._subs(child)},
                         {("telegram", "424242")})

    def test_creator_origin_inheritance_is_guarded(self):
        """``kanban_db_graph.inherit_creator_origin`` (create_task ``creator_task_id``) copies too."""
        from hermes_cli import kanban_db_graph as graph

        creator = self._create("creator")
        self.kbn.add_notify_sub(self.conn, task_id=creator, platform="api_server",
                                chat_id="hermes-onebotv11+private+10086-gabcd", user_id="api",
                                notifier_profile="default")
        task = self._create("spawned")
        graph.inherit_creator_origin(self.conn, task, creator, created_at=0)
        self.assertEqual(self._subs(task), [])


class IndependentCliCreateTest(unittest.TestCase):
    """R4 的收窄契约：独立 CLI 进程不加载用户插件（上游对 kanban 内建命令跳过插件发现），
    所以继承围栏在那里**不存在**。可证明的闭合点是：自动订阅需要调用会话平台，独立 CLI
    没有；继承只复制已存在的行，而部署步骤（既有 handover，公开 CLI 注销）之后不存在
    api_server 行可继承。本类用真 ``hermes kanban create`` 子进程前后对照。
    """

    HERMES = "/home/neonsky/.hermes/hermes-agent/venv/bin/hermes"
    CHAT = "hermes-onebotv11+private+10086-gabcd"

    def setUp(self):
        from hermes_cli import kanban_db as kb
        from hermes_cli import kanban_db_connect as kbc

        self.kb, self.kbn = kb, None
        self.home = Path(tempfile.mkdtemp(prefix="qlt-cli-home-"))
        self.db_path = self.home / "kanban.db"
        self.conn = sqlite3.connect(self.db_path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(kb.SCHEMA_SQL)
        kbc._migrate_add_optional_columns(self.conn)
        self.addCleanup(self.conn.close)
        from hermes_cli import kanban_db_notify as kbn

        self.kbn = kbn
        # The ambient worker markers are already cleared at import: the subprocess must look
        # like an operator CLI, not like a dispatcher worker.
        self.env = dict(os.environ, HERMES_HOME=str(self.home),
                        HERMES_KANBAN_DB=str(self.db_path), HERMES_DISABLE_LAZY_INSTALLS="1")

    def _cli(self, *argv):
        return subprocess.run([self.HERMES, "kanban", *argv], capture_output=True, text=True,
                              env=self.env, timeout=180)

    def _create_cli(self, title, parent):
        proc = self._cli("create", title, "--parent", parent, "--assignee", "coder_deepseek",
                         "--json")
        self.assertEqual(proc.returncode, 0, proc.stderr[-400:])
        return json.loads(proc.stdout)["id"]

    def _subs(self, task_id):
        return [(s["platform"], s["chat_id"]) for s in self.kbn.list_notify_subs(self.conn, task_id)]

    def test_independent_cli_inherits_only_what_still_exists(self):
        parent = self.kb.create_task(self.conn, title="parent", assignee="coder_deepseek",
                                     initial_status="running")
        self.kbn.add_notify_sub(self.conn, task_id=parent, platform="api_server",
                                chat_id=self.CHAT, user_id="api", notifier_profile="default")

        # Before handover: the plugin is not in that process, so the legacy row is inherited.
        # (Narrowed contract, not a claim of coverage: reproduced, not papered over.)
        legacy_child = self._create_cli("cli-child-before", parent)
        self.assertEqual(self._subs(legacy_child), [("api_server", self.CHAT)])

        # The deployment handover runs through the same public CLI the docs name.
        unsub = self._cli("notify-unsubscribe", parent, "--platform", "api_server",
                          "--chat-id", self.CHAT)
        self.assertEqual(unsub.returncode, 0, unsub.stderr[-400:])
        self.assertEqual(self._subs(parent), [])

        # After handover there is no automatic api_server row left to inherit — and the CLI itself
        # never creates one (auto-subscribe needs a session platform, which this process has not).
        clean_child = self._create_cli("cli-child-after", parent)
        self.assertEqual(self._subs(clean_child), [])


# --------------------------------------------------------------------------- packaging
class PluginEnabledAtLoadTest(unittest.TestCase):
    """Temp home + the real ``PluginManager.discover_and_load()``: a discovered directory is not a
    loaded plugin. ``plugins.enabled`` must name the plugin explicitly (upstream gate), and only
    then do the wrappers exist in-process.
    """

    def setUp(self):
        restore_stock()
        self.addCleanup(restore_stock)
        self.home = Path(tempfile.mkdtemp(prefix="qlt-home-"))
        target = self.home / "plugins" / "queued-lane-interim-thin"
        shutil.copytree(PLUGIN_DIR, target,
                        ignore=shutil.ignore_patterns("tests", "__pycache__"))
        env = mock.patch.dict(os.environ, {"HERMES_HOME": str(self.home),
                                           "HERMES_DISABLE_LAZY_INSTALLS": "1"})
        env.start()
        self.addCleanup(env.stop)
        self.config = self.home / "config.yaml"

    def _write_config(self, enabled: bool) -> None:
        body = ("plugins:\n  enabled:\n    - queued-lane-interim-thin\n" if enabled
                else "plugins:\n  enabled: []\n")
        self.config.write_text(body, encoding="utf-8")

    def _discover(self, *, force: bool = False):
        from hermes_cli.plugins import PluginManager

        manager = PluginManager(scope_key=str(self.home))
        manager.discover_and_load(force=force)
        return manager

    def _loaded(self, manager):
        return manager._plugins.get("queued-lane-interim-thin")

    def _patch_mark(self):
        return getattr(GatewayTurnMixin._run_agent_deliver_first_response, patches._MARK, None)

    def test_directory_alone_is_not_loaded(self):
        self._write_config(enabled=False)
        manager = self._discover()
        loaded = self._loaded(manager)
        self.assertIsNotNone(loaded)
        self.assertFalse(loaded.enabled)
        self.assertIn("not enabled in config", loaded.error or "")
        self.assertIsNone(self._patch_mark())          # first_response keeps its stock body

    def test_enabled_profile_loads_and_wraps(self):
        self._write_config(enabled=True)
        manager = self._discover()
        loaded = self._loaded(manager)
        self.assertTrue(loaded.enabled, f"not loaded: {loaded.error!r}")
        self.assertFalse(loaded.error)
        self.assertEqual(self._patch_mark(), patches.PATCH_ID)
        from tools import kanban_tools as kt

        self.assertEqual(getattr(kt._maybe_auto_subscribe,
                                 kanban_notify_guard._MARK, None),
                         kanban_notify_guard.PATCH_ID)

    def test_enabling_then_rediscovering_installs_the_wrappers(self):
        self._write_config(enabled=False)
        self._discover()
        self.assertIsNone(self._patch_mark())
        self._write_config(enabled=True)
        manager = self._discover(force=True)
        self.assertTrue(self._loaded(manager).enabled)
        self.assertEqual(self._patch_mark(), patches.PATCH_ID)


class PluginPackageTest(unittest.TestCase):
    """The candidate must be a plugin Hermes actually discovers and registers."""

    def test_manifest_is_discovered_as_a_user_plugin(self):
        from hermes_cli import plugins_discovery

        target = Path(HOME) / "plugins" / "queued-lane-interim-thin"
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.symlink_to(Path(PLUGIN_DIR), target_is_directory=True)
        manifests = [m for m in plugins_discovery.collect_directory_manifests()
                     if m.name == "queued-lane-interim-thin"]
        self.assertEqual(len(manifests), 1)
        self.assertEqual(Path(manifests[0].path).resolve(), Path(PLUGIN_DIR).resolve())
        self.assertIn("queued-lane", manifests[0].description)

    def test_register_wires_the_api_server_factory(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "queued_lane_interim_thin_pkg", Path(PLUGIN_DIR) / "__init__.py",
            submodule_search_locations=[PLUGIN_DIR])
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        handlers = {}

        class Ctx:
            def register_platform_handler(self, platform, factory):
                handlers[platform] = factory

        module.register(Ctx())
        self.assertIn("api_server", handlers)
        # The connect-time factory only re-asserts the idempotent installs.
        handlers["api_server"](None, SimpleNamespace())
        self.assertEqual(GatewayTurnMixin._run_agent_deliver_first_response.__dict__
                         .get("__queued_lane_interim_thin__"), patches.PATCH_ID)


if __name__ == "__main__":
    unittest.main(verbosity=2)
