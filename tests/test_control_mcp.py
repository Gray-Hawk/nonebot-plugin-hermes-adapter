"""Runs 控制段 MCP 化：控制队列、控制工具、后台状态查询与 turn 内落地。

覆盖验收要求：
- 普通 body 零控制工具；
- 每种真实控制（reply_to / should_exit_active / pin_anchor / release_anchor /
  answer_anchor / anchor_merge）都能经工具登记后在原时机生效；
- 控制 → 正文、空 body 也算一份决策；
- 多控制 FIFO、同文两份；
- 控制丢失（未登记／工具拒收）仍按缺省控制发正文；
- 控制工具不是工作工具的封批边界（3 秒条间节奏与输入暂存不退化）。
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from tests import _bootstrap  # noqa: F401

from tests import test_runs_terminal as e2
from tests.test_background_report_chain import build_board
from tests.test_runs_stream import ReactiveControlParityTests, RunsEventParser, sse_lines

from nonebot_plugin_hermes_adapter.core import control_intent
from nonebot_plugin_hermes_adapter.core.active_session import (
    ActiveSessionManager,
    AnchorRecord,
    Participant,
)
from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
from nonebot_plugin_hermes_adapter.core.inflight import make_inflight_key
from nonebot_plugin_hermes_adapter.core.kanban_report_source import KanbanReportSource, ingest_into
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin
from nonebot_plugin_hermes_adapter.core.npha_terminal import (
    decode_terminal_batch,
    structured_batch_from_terminal,
)
from nonebot_plugin_hermes_adapter.core.routing import CallerScope
from nonebot_plugin_hermes_adapter.core.running_tasks import (
    DelegationRunningSource,
    KanbanRunningSource,
)
from nonebot_plugin_hermes_adapter.core.background_report_outbox import BackgroundReportOutbox
from nonebot_plugin_hermes_adapter.mcp.tools.background_status import (
    GetBackgroundTasksInput,
    get_background_tasks_impl,
)
from nonebot_plugin_hermes_adapter.mcp.tools.control import (
    SubmitControlInput,
    submit_control_impl,
)

GROUP_KEY = make_inflight_key("onebotv11", False, "u1", "g1")
PRIVATE_KEY = make_inflight_key("onebotv11", True, "u1", None)


class ControlIntentQueueTests(unittest.TestCase):
    """控制队列的纯语义：一份决策一份控制、FIFO、缺省、丢弃。"""

    def setUp(self) -> None:
        self.key = ("onebotv11", "group:queue-test")
        control_intent.CONTROL_QUEUE.discard(self.key)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, self.key)

    def test_one_control_per_decision_including_empty_body(self) -> None:
        queue = control_intent.ControlQueue()
        queue.register(self.key, {"reply_to": "m:1"})
        first = {}   # 空 body 也是一份决策
        second = {}
        self.assertEqual(control_intent.bind_control(first, self.key, queue=queue), {"reply_to": "m:1"})
        self.assertIsNone(control_intent.bind_control(second, self.key, queue=queue))
        self.assertEqual(control_intent.resolve_control(second)["reply_to"], "")
        self.assertEqual(queue.pending(self.key), 0)

    def test_multiple_controls_consumed_in_registration_order(self) -> None:
        queue = control_intent.ControlQueue()
        queue.register(self.key, {"reply_to": "m:1"})
        queue.register(self.key, {"reply_to": "m:2"})
        bodies = [{}, {}]
        for decision in bodies:
            control_intent.bind_control(decision, self.key, queue=queue)
        self.assertEqual(
            [control_intent.resolve_control(d)["reply_to"] for d in bodies], ["m:1", "m:2"]
        )

    def test_bind_is_idempotent_for_same_decision(self) -> None:
        queue = control_intent.ControlQueue()
        queue.register(self.key, {"reply_to": "m:1"})
        queue.register(self.key, {"reply_to": "m:2"})
        decision = {}
        control_intent.bind_control(decision, self.key, queue=queue)
        control_intent.bind_control(decision, self.key, queue=queue)
        self.assertEqual(control_intent.resolve_control(decision)["reply_to"], "m:1")
        # 第二份控制还留在队列里，等下一份正文。
        self.assertEqual(queue.pending(self.key), 1)

    def test_no_control_means_all_defaults(self) -> None:
        effective = control_intent.resolve_control({})
        self.assertEqual(effective["reply_to"], "")
        self.assertIs(effective["should_exit_active"], False)
        self.assertIs(effective["pin_anchor"], False)
        self.assertEqual(effective["release_anchor"], "")
        self.assertEqual(effective["answer_anchor"], "")
        self.assertEqual(effective["anchor_merge"], "")

    def test_legacy_control_line_wins_over_tool_value(self) -> None:
        decision = {"reply_to": "m:9", "_control": {"reply_to": "m:1"}}
        self.assertEqual(control_intent.resolve_control(decision)["reply_to"], "m:9")

    def test_invalid_reply_to_is_rejected(self) -> None:
        for bad in ("123", "m:0", "M:1", "[m:1]", "m: 1", 123):
            with self.subTest(value=bad), self.assertRaises(control_intent.ControlIntentError):
                control_intent.build_control(reply_to=bad)

    def test_empty_control_is_not_registered(self) -> None:
        self.assertEqual(control_intent.build_control(should_exit_active=False), {})
        queue = control_intent.ControlQueue()
        self.assertEqual(queue.register(self.key, {}), 0)
        self.assertIsNone(queue.pop(self.key))

    def test_queue_cap_drops_oldest(self) -> None:
        queue = control_intent.ControlQueue(max_pending=2)
        for index in (1, 2, 3):
            queue.register(self.key, {"reply_to": f"m:{index}"})
        self.assertEqual(queue.pop(self.key), {"reply_to": "m:2"})
        self.assertEqual(queue.pop(self.key), {"reply_to": "m:3"})

    def test_discard_drops_leftovers(self) -> None:
        queue = control_intent.ControlQueue()
        queue.register(self.key, {"pin_anchor": True})
        self.assertEqual(queue.discard(self.key), 1)
        self.assertIsNone(queue.pop(self.key))


class SubmitControlToolTests(unittest.TestCase):
    """MCP 工具层：只登记、只管 runs、失败不抛。"""

    def setUp(self) -> None:
        for key in (GROUP_KEY, PRIVATE_KEY):
            control_intent.CONTROL_QUEUE.discard(key)
            self.addCleanup(control_intent.CONTROL_QUEUE.discard, key)

    def _call(self, *, interface: str = "runs", **kwargs):
        inp = SubmitControlInput(adapter="onebotv11", group_id="g1", **kwargs)
        return submit_control_impl(
            inp, scope=CallerScope.dev(), interface=interface,
            queue=control_intent.CONTROL_QUEUE,
        )

    def test_registers_one_control_for_next_body(self) -> None:
        result = self._call(reply_to="m:12")
        self.assertTrue(result.ok)
        self.assertTrue(result.registered)
        self.assertEqual(result.fields, ["reply_to"])
        self.assertEqual(result.pending, 1)
        self.assertEqual(
            control_intent.CONTROL_QUEUE.pop(GROUP_KEY), {"reply_to": "m:12"}
        )

    def test_all_real_fields_pass_through(self) -> None:
        result = self._call(
            should_exit_active=True, pin_anchor=True, release_anchor="u2",
            answer_anchor="all", anchor_merge="u2",
        )
        self.assertTrue(result.ok)
        self.assertEqual(
            control_intent.CONTROL_QUEUE.pop(GROUP_KEY),
            {
                "should_exit_active": True, "pin_anchor": True,
                "release_anchor": "u2", "answer_anchor": "all", "anchor_merge": "u2",
            },
        )

    def test_rejected_outside_runs_without_touching_queue(self) -> None:
        result = self._call(interface="completions", reply_to="m:1")
        self.assertFalse(result.ok)
        self.assertFalse(result.registered)
        self.assertIn("Runs-only", result.error or "")
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)

    def test_invalid_value_is_a_soft_failure(self) -> None:
        result = self._call(reply_to="12345")
        self.assertFalse(result.ok)
        self.assertFalse(result.registered)
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)

    def test_empty_call_registers_nothing(self) -> None:
        result = self._call()
        self.assertTrue(result.ok)
        self.assertFalse(result.registered)
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)

    def test_exactly_one_target_required(self) -> None:
        with self.assertRaises(ValueError):
            SubmitControlInput(adapter="onebotv11", group_id="g1", user_id="u1")
        with self.assertRaises(ValueError):
            SubmitControlInput(adapter="onebotv11")

    def test_private_scope_registers_on_private_key(self) -> None:
        inp = SubmitControlInput(adapter="onebotv11", user_id="u1", reply_to="m:3")
        result = submit_control_impl(
            inp, scope=CallerScope.dev(), interface="runs",
            queue=control_intent.CONTROL_QUEUE,
        )
        self.assertTrue(result.ok)
        self.assertEqual(control_intent.CONTROL_QUEUE.pop(PRIVATE_KEY), {"reply_to": "m:3"})


class BackgroundStatusToolTests(unittest.TestCase):
    """后台状态：运行中来自真实运行源，待汇报来自完成信箱，两者不许混读。"""

    SESSION = "hermes-onebotv11+group+g1"

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.outbox = BackgroundReportOutbox(self.dir / "background_reports.db")
        self.addCleanup(self.outbox.close)
        # 完成待汇报事件（真实信箱语义：上游终态才会进信箱）。
        self.outbox.upsert_event({
            "event_id": "e1",
            "origin_session_id": self.SESSION,
            "state": "completed",
            "result": "第一行结果\n第二行细节",
            "completed_at": 123.0,
            "source": "subagent",
        })
        self.outbox.upsert_event({
            "event_id": "e2",
            "origin_session_id": "hermes-onebotv11+group+g2",
            "state": "completed",
            "source": "subagent",
        })
        # 真实看板库：一条执行中卡片 + 一条已完成卡片。
        self.board = self.dir / "board.db"
        build_board(self.board, [
            ("t_run", "spawned", {}, self.SESSION),
            ("t_done", "completed", {"summary": "done"}, self.SESSION),
        ])
        with sqlite3.connect(self.board) as con:
            con.execute("UPDATE tasks SET status='running' WHERE id='t_run'")
            con.execute("UPDATE tasks SET status='done' WHERE id='t_done'")
        # 真实 background-completion 登记库：一条已派发未终态 + 一条已汇报完。
        self.registry = self.dir / "background-completion.db"
        with sqlite3.connect(self.registry) as con:
            con.executescript(
                """
                CREATE TABLE bg_completions (
                    delegation_id TEXT PRIMARY KEY,
                    origin_session_id TEXT NOT NULL,
                    claim_id TEXT,
                    state TEXT NOT NULL DEFAULT 'registered',
                    acked INTEGER NOT NULL DEFAULT 0,
                    dispatched_at REAL,
                    completed_at REAL,
                    drop_reason TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            con.execute(
                "INSERT INTO bg_completions VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("deleg_run", self.SESSION, "", "registered", 0, 111.0, None, None, 1.0, 1.0),
            )
            con.execute(
                "INSERT INTO bg_completions VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("deleg_done", self.SESSION, "", "external_ack", 1, 100.0, 120.0, None, 2.0, 2.0),
            )
        self.sources = [
            KanbanRunningSource(self.board),
            DelegationRunningSource(self.registry),
        ]

    def _query(self, **kwargs):
        return get_background_tasks_impl(
            GetBackgroundTasksInput(adapter="onebotv11", group_id="g1"),
            scope=CallerScope.dev(), **kwargs,
        )

    def test_running_comes_from_real_sources_not_the_report_outbox(self) -> None:
        result = self._query(outbox=self.outbox, running_sources=self.sources)
        self.assertTrue(result.ok)
        self.assertEqual(
            sorted(item.task_id for item in result.running), ["deleg_run", "t_run"],
        )
        self.assertEqual(sorted(item.source for item in result.running), ["delegation", "kanban"])
        self.assertEqual(result.running_count, 2)
        self.assertEqual(sorted(result.running_sources), ["delegation", "kanban"])
        self.assertFalse(result.running_unknown)
        # 完成待汇报只来自信箱；手工伪造的 running 信箱行不是运行态来源。
        self.assertEqual([item.event_id for item in result.reports], ["e1"])
        self.assertEqual(result.reports[0].task_state, "completed")
        self.assertEqual(result.reports[0].report_state, "pending")
        self.assertEqual(result.reports[0].summary, "第一行结果")
        self.assertEqual(result.report_count, 1)
        self.assertEqual(result.count, 3)
        self.assertEqual(result.note, "")

    def test_completed_real_task_moves_from_running_to_pending_report(self) -> None:
        running = self._query(outbox=self.outbox, running_sources=self.sources)
        self.assertIn("t_run", [item.task_id for item in running.running])

        # 真实源变化：看板任务结束（status=done + completed 事件）。
        with sqlite3.connect(self.board) as con:
            con.execute("UPDATE tasks SET status='done' WHERE id='t_run'")
            con.execute(
                "INSERT INTO task_events (task_id, run_id, kind, payload, created_at)"
                " VALUES (?,?,?,?,?)",
                ("t_run", None, "completed", json.dumps({"summary": "finished"}), 2.0),
            )
        self.outbox.put_source_cursor("kanban", "0")
        self.assertGreaterEqual(
            ingest_into(self.outbox, KanbanReportSource(self.board)), 1,
        )

        after = self._query(outbox=self.outbox, running_sources=self.sources)
        # 运行中只剩子代理；结束的看板任务出现在待汇报里。
        self.assertEqual([item.task_id for item in after.running], ["deleg_run"])
        self.assertEqual(after.running_count, 1)
        event_ids = [item.event_id for item in after.reports]
        self.assertTrue(any(item.startswith("kanban:t_run:") for item in event_ids), event_ids)
        states = {item.event_id: item.task_state for item in after.reports}
        self.assertIn("completed", states.values())

    def test_other_conversation_is_empty(self) -> None:
        result = get_background_tasks_impl(
            GetBackgroundTasksInput(adapter="onebotv11", user_id="u1"),
            outbox=self.outbox, scope=CallerScope.dev(), running_sources=self.sources,
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.running, [])
        self.assertEqual(result.reports, [])
        self.assertEqual(result.count, 0)
        # 有可读运行源：空 running 是真实「没有在跑任务」。
        self.assertFalse(result.running_unknown)

    def test_no_running_source_is_unknown_not_empty(self) -> None:
        result = self._query(outbox=self.outbox, running_sources=None)
        self.assertTrue(result.ok)
        self.assertEqual(result.running, [])
        self.assertTrue(result.running_unknown)
        self.assertEqual(result.running_sources, [])
        self.assertTrue(result.note)
        self.assertIn("不能证明", result.note)
        # 信箱里的完成待汇报仍然可读。
        self.assertEqual(result.report_count, 1)

    def test_read_failure_is_reported_as_unknown_source(self) -> None:
        result = self._query(
            outbox=self.outbox,
            running_sources=[KanbanRunningSource(self.dir / "missing.db")],
        )
        self.assertTrue(result.ok)
        self.assertTrue(result.running_unknown)
        self.assertTrue(result.running_source_errors)
        self.assertIn("not found", result.running_source_errors[0])

    def test_missing_registry_and_no_running_source_is_an_error(self) -> None:
        result = self._query(outbox=None)
        self.assertFalse(result.ok)
        self.assertIn("unavailable", result.error or "")


class ControlToolBatchBoundaryTests(unittest.TestCase):
    """控制工具不是普通工作工具的封批边界：不切批、不吐旁白。"""

    def test_control_tool_does_not_bump_decision_batch(self) -> None:
        parser = RunsEventParser()
        for line in sse_lines([
            {"event": "message.delta", "delta": '{"body":"甲"}', "seq": 0},
            {"event": "tool.started", "tool": "submit_control", "seq": 1},
            {"event": "tool.completed", "tool": "submit_control", "seq": 2},
            {"event": "message.delta", "delta": '\n{"body":"乙"}', "seq": 3},
        ]):
            parser.feed(line)
        self.assertEqual(parser.decision_batch, 0)
        self.assertEqual([d.body for d in parser.drain_stream_decisions()], ["甲", "乙"])

    def test_work_tool_still_bumps_decision_batch(self) -> None:
        parser = RunsEventParser()
        for line in sse_lines([
            {"event": "message.delta", "delta": '{"body":"甲"}', "seq": 0},
            {"event": "tool.started", "tool": "lookup", "seq": 1},
            {"event": "tool.completed", "tool": "lookup", "seq": 2},
            {"event": "message.delta", "delta": '\n{"body":"乙"}', "seq": 3},
        ]):
            parser.feed(line)
        self.assertEqual(parser.decision_batch, 1)

    def test_body_only_decision_is_stripped_from_narration(self) -> None:
        from nonebot_plugin_hermes_adapter.core.npha_terminal import strip_complete_decisions

        stripped = strip_complete_decisions('{"body":"甲"}\n这句是旁白\n')
        self.assertNotIn('"body"', stripped)
        self.assertIn("这句是旁白", stripped)

    def test_control_tool_does_not_flush_pending_narration(self) -> None:
        parser = RunsEventParser()
        for line in sse_lines([
            {"event": "message.delta", "delta": "我先看看", "seq": 0},
            {"event": "tool.started", "tool": "submit_control", "seq": 1},
        ]):
            self.assertIsNone(parser.feed(line))
        for line in sse_lines([
            {"event": "tool.completed", "tool": "submit_control", "seq": 2},
        ]):
            self.assertIsNone(parser.feed(line))
        # 工作工具才是旁白边界：之前攒的旁白在这里一次定性发出，没有被控制工具切碎。
        segment = None
        for line in sse_lines([{"event": "tool.started", "tool": "lookup", "seq": 3}]):
            segment = parser.feed(line)
        self.assertEqual(segment, "我先看看")


class _ControlTurnHarness(e2._ReactiveHarness):
    """真实 _run_reactive_turn + 真实控制队列，只换外部 I/O。"""

    def setUp(self) -> None:
        control_intent.CONTROL_QUEUE.discard(GROUP_KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, GROUP_KEY)

    def _register(self, **fields) -> None:
        control = control_intent.build_control(**fields)
        control_intent.CONTROL_QUEUE.register(GROUP_KEY, control)

    def _result(self, output: str):
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        return result

    def _live_window(self, now: int):
        manager = ActiveSessionManager()
        session = manager.trigger("onebotv11", "g1", "u1", now_ms=now)
        return manager, session


class ControlToolTurnTests(_ControlTurnHarness):
    async def test_ordinary_body_needs_no_control_tool(self) -> None:
        calls = await self._run(self._result('{"body":"你好"}'), tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["你好"])
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)
        self.assertEqual(
            calls["anchor"], {"pin": 0, "answer": 0, "release": 0, "merge": 0}
        )
        self.assertEqual(calls["control"]["end_window"], 0)

    async def test_reply_to_control_quotes_next_body(self) -> None:
        self._register(reply_to="m:1")
        recorded = []

        def _resolve(*args, **kwargs):
            recorded.append(kwargs.get("reply_to"))
            return None

        calls = await self._run(
            self._result('{"body":"收到"}'), tracker=self._tracker(),
            resolve_reply=MagicMock(side_effect=_resolve),
        )
        self.assertEqual(recorded, ["m:1"])
        self.assertEqual([c["text"] for c in calls["send"]], ["收到"])

    async def test_should_exit_active_control_closes_window_after_send(self) -> None:
        now = int(time.time() * 1000)
        manager, _session = self._live_window(now)
        self._register(should_exit_active=True)
        calls = await self._run(
            self._result('{"body":"再见"}'), tracker=self._tracker(),
            sessions=manager, now_ms=now,
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["再见"])
        self.assertIsNone(manager.get("onebotv11", "g1"))

    async def test_pin_anchor_control_pins_current_request(self) -> None:
        now = int(time.time() * 1000)
        manager, _session = self._live_window(now)
        buffered = BufferedMessage(
            ts=now, adapter="onebotv11", group_id="g1", user_id="u1", nickname="u1",
            content="帮我跟进这件事", origin=MessageOrigin.human(),
        )
        buffered.id = 5
        self._register(pin_anchor=True)
        calls = await self._run(
            self._result('{"body":"收到"}'), tracker=self._tracker(),
            sessions=manager, current_buffered=buffered, now_ms=now,
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["收到"])
        self.assertEqual(manager.pinned_commissioners("onebotv11", "g1"), {"u1"})

    async def test_answer_and_release_control_clear_pinned_anchor(self) -> None:
        now = int(time.time() * 1000)
        manager, session = self._live_window(now)
        manager.pin_anchor(
            session,
            AnchorRecord(
                participants=[Participant(user_id="u1", nickname="u1", joined_ts=now)],
                m_id=1, platform_message_id="p1", text="旧委托", ts=now,
            ),
            master_ids=set(), max_age_ms=3600_000, now_ms=now,
        )
        self._register(answer_anchor="all", release_anchor="all")
        calls = await self._run(
            self._result('{"body":"都办完了"}'), tracker=self._tracker(),
            sessions=manager, now_ms=now,
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["都办完了"])
        self.assertEqual(manager.pinned_commissioners("onebotv11", "g1"), set())

    async def test_anchor_merge_control_joins_participants(self) -> None:
        now = int(time.time() * 1000)
        manager, session = self._live_window(now)
        manager.pin_anchor(
            session,
            AnchorRecord(
                participants=[Participant(user_id="u2", nickname="u2", joined_ts=now)],
                m_id=1, platform_message_id="p1", text="原委托", ts=now,
            ),
            master_ids=set(), max_age_ms=3600_000, now_ms=now,
        )
        self._register(anchor_merge="u2")
        calls = await self._run(
            self._result('{"body":"我补充一句"}'), tracker=self._tracker(),
            sessions=manager, now_ms=now,
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["我补充一句"])
        record = session.anchor_master or session.anchor_guest
        self.assertIn("u1", {p.user_id for p in record.participants})

    async def test_multi_control_fifo_with_same_text_twice(self) -> None:
        self._register(reply_to="m:1")
        self._register(reply_to="m:2")
        recorded = []

        def _resolve(*args, **kwargs):
            recorded.append(kwargs.get("reply_to"))
            return None

        calls = await self._run(
            self._result('{"body":"同一句"}\n{"body":"同一句"}'),
            tracker=self._tracker(), resolve_reply=MagicMock(side_effect=_resolve),
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["同一句", "同一句"])
        self.assertEqual(recorded, ["m:1", "m:2"])
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)

    async def test_empty_body_still_consumes_one_control(self) -> None:
        self._register(should_exit_active=True)
        calls = await self._run(
            self._result('{"body":""}\n{"body":"随后"}'), tracker=self._tracker(),
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["随后"])
        self.assertEqual(calls["control"]["end_window"], 1)
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)

    async def test_lost_control_still_sends_body_with_defaults(self) -> None:
        # 没有登记（工具失败／断流／重启都长这样）：正文照发，控制全缺省。
        recorded = []

        def _resolve(*args, **kwargs):
            recorded.append(kwargs.get("reply_to"))
            return None

        calls = await self._run(
            self._result('{"body":"正文照发"}'), tracker=self._tracker(),
            resolve_reply=MagicMock(side_effect=_resolve),
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["正文照发"])
        self.assertEqual(recorded, [""])
        self.assertEqual(calls["control"]["end_window"], 0)
        self.assertEqual(calls["anchor"]["pin"], 0)


class ControlPrivateTurnTests(e2._PassiveHarness):
    """私聊 passive 也按同一队列消费控制（键与 inflight 同形）。"""

    def setUp(self) -> None:
        control_intent.CONTROL_QUEUE.discard(PRIVATE_KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, PRIVATE_KEY)

    async def test_private_control_consumed_by_next_body(self) -> None:
        control_intent.CONTROL_QUEUE.register(PRIVATE_KEY, {"reply_to": "m:1"})
        decisions = structured_batch_from_terminal(decode_terminal_batch('{"body":"私聊回复"}'))
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["私聊回复"])
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(PRIVATE_KEY), 0)

    async def test_private_body_without_control_still_sends(self) -> None:
        decisions = structured_batch_from_terminal(decode_terminal_batch('{"body":"照发"}'))
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["照发"])


class ControlLiveChainTests(ReactiveControlParityTests):
    """即时发送路径：绑上的控制不能破坏流式/终态折叠（不能重发正文）。"""

    def setUp(self) -> None:  # noqa: D401 - 复用真实 manager harness 的接口约定
        super().setUp()
        control_intent.CONTROL_QUEUE.discard(GROUP_KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, GROUP_KEY)

    async def test_streamed_body_with_control_is_sent_once(self) -> None:
        control_intent.CONTROL_QUEUE.register(GROUP_KEY, {"reply_to": "m:1"})
        manager, calls = await self._run_with_manager(
            manager=self._manager_with_commission(int(time.time() * 1000)),
            output='{"body":"正文"}', live=True, ephemeral=False,
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["正文"])
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)

    async def test_streamed_empty_body_consumes_control_without_send(self) -> None:
        control_intent.CONTROL_QUEUE.register(GROUP_KEY, {"should_exit_active": True})
        manager, calls = await self._run_with_manager(
            manager=self._manager_with_commission(int(time.time() * 1000)),
            output='{"body":""}', live=True, ephemeral=False,
        )
        self.assertEqual(calls["send"], [])
        self.assertIsNone(manager.get("onebotv11", "g1"))
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(GROUP_KEY), 0)


if __name__ == "__main__":
    unittest.main()
