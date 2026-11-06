"""提醒链定向回归：Kanban 与子代理完成提醒统一到后台汇报信箱。

覆盖验收面（两条链各自独立，不混根因）：

  子代理链
  - 已实发且交付已进终态（expected_state='terminal'）的汇报行必须销账，
    不再永久占住同会话的唤醒闸（现网真实故障形态）；
  - 业务终态且确实没发出去（没 primary 实发）按可重试处理，重试上限后落
    report_failed 并允许上游 drop，不再无限 reporting；
  - NB 离线期间事件只落盘不丢，恢复后每个事件只唤醒一次；
  - 重复登记幂等；唤醒失败按 retry_wait 重排并有界重试。

  Kanban 链
  - 看板终态事件以内部身份登记进同一张信箱（source='kanban'），事件号幂等；
  - 水位持久化：停机期间板上事件不丢，恢复后从水位继续，且不重放历史；
  - kanban 事件没有上游认领对象：销账只落本地台账，不调用 ack/drop；
  - 两个来源互不吞：一条源卡住不会把另一条源的后续事件挡死。
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.background_report_consumer import (  # noqa: E402
    BackgroundReportConsumer,
)
from nonebot_plugin_hermes_adapter.core.background_report_outbox import (  # noqa: E402
    SOURCE_KANBAN,
    SOURCE_SUBAGENT,
    BackgroundReportOutbox,
)
from nonebot_plugin_hermes_adapter.core.kanban_report_source import (  # noqa: E402
    KanbanReportSource,
    WAKE_KINDS,
    ingest_into,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402

SESSION = "hermes-onebotv11+private+158****4503-g404"
SCOPE = "onebotv11:private:1581124503"
USER = "1581124503"


# --- 夹具 -------------------------------------------------------------------


class FakeGateway:
    def __init__(self, events: list[dict] | None = None) -> None:
        self.events = events or []
        self.completed: list[tuple[str, str]] = []
        self.dropped: list[str] = []
        self.claims: list[str] = []
        self.sessions_dropped: list[str] = []

    async def list_events(self):
        return list(self.events)

    async def claim(self, event_id: str) -> bool:
        self.claims.append(event_id)
        return True

    async def complete(self, event_id: str, run_id: str) -> None:
        self.completed.append((event_id, run_id))

    async def drop(self, event_id: str) -> None:
        self.dropped.append(event_id)

    async def drop_session(self, origin_session_id: str) -> None:
        self.sessions_dropped.append(origin_session_id)


class FakeWake:
    """记录唤醒；on_wake(event_id) 用来模拟「正常会话跑出 run 并实发」。"""

    def __init__(self, *, reject: str | None = None, on_wake=None) -> None:
        self.reject = reject
        self.on_wake = on_wake
        self.calls: list[tuple[str, str, str]] = []

    async def __call__(self, route, text, source_event_id, source=SOURCE_SUBAGENT):
        self.calls.append((source, source_event_id, text))
        if self.reject is not None:
            return self.reject
        if self.on_wake is not None:
            self.on_wake(source_event_id)
        return None

    def ids(self) -> list[str]:
        return [call[1] for call in self.calls]


async def _route(_session: str):
    return {"adapter_name": "onebotv11", "is_private": True, "user_id": USER, "group_id": None}


async def _noop(_run_id):
    return None


class Harness:
    """真实 SQLite ＋ 真实 RunTracker／信箱／消费器，只替平台与路由边界。"""

    def __init__(self, tmp: Path, *, wake: FakeWake | None = None, gateway=None,
                 outbox: BackgroundReportOutbox | None = None,
                 tracker: RunTracker | None = None) -> None:
        self.outbox = outbox or BackgroundReportOutbox(tmp / "background_reports.db")
        self.tracker = tracker or RunTracker(db_path=tmp / "active_runs.db")
        self.gateway = gateway or FakeGateway()
        self.wake = wake or FakeWake()
        self.consumer = BackgroundReportConsumer(
            outbox=self.outbox, tracker=self.tracker, gateway=self.gateway,
            resolve_route=_route, wake=self.wake, reconcile_run=_noop,
        )

    def land_report(self, *, event_id: str, run_id: str, sent: bool,
                    state: str = "terminal", cancel_reason: str | None = None,
                    key: str | None = None) -> None:
        key = key or f"background-report:{event_id}:attempt:1"
        self.tracker.record_submission(
            idempotency_key=key, request_body={"event": event_id}, target_fingerprint="onebotv11|x",
            scope_key=SCOPE, session_key=SESSION, mode="passive", expect_structured=True,
            bot_self_id="999", source_event_id=event_id,
        )
        self.tracker.bind_submission_and_register(
            idempotency_key=key, run_id=run_id, scope_key=SCOPE, adapter="onebotv11",
            is_private=True, user_id=USER, group_id=None, bot_self_id="999",
            session_key=SESSION, mode="passive", expect_structured=True,
        )
        self.tracker.record_expected_delivery(run_id=run_id, output="{}", expect_structured=True)
        self.tracker.upsert_delivery_plan(
            run_id=run_id,
            plan={"live": {"1": {"cleaned_text": "汇报", "media_urls": [], "file_paths": [],
                                 "reply_to": "", "reply_context": "", "reply_seq": 1}}},
            components=[{"component_id": "primary", "kind": "primary", "payload_sha256": "x"}],
        )
        if sent:
            claim = self.tracker.claim_component_attempt(run_id=run_id, component_id="primary", owner="probe")
            self.tracker.mark_component_sent(
                run_id=run_id, component_id="primary", msg_ids=["322079804"], token=claim.token,
            )
        if state != "ready":
            self.tracker.set_expected_state(run_id, state, cancel_reason=cancel_reason)

    def add_event(self, event_id: str, *, source: str = SOURCE_SUBAGENT, session: str = SESSION,
                  state: str = "completed", result: str = "结果正文") -> None:
        self.outbox.upsert_event({
            "event_id": event_id, "origin_session_id": session, "state": state,
            "result": result, "source": source,
        })


def build_board(path: Path, rows: list[tuple[str, str, dict | None, str]]) -> None:
    """rows: (task_id, kind, payload, session_id)，任务与事件一并建。"""
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, title TEXT, status TEXT,
                                          assignee TEXT, result TEXT, session_id TEXT);
        CREATE TABLE IF NOT EXISTS task_events (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                task_id TEXT, run_id INTEGER, kind TEXT,
                                                payload TEXT, created_at REAL);
        CREATE TABLE IF NOT EXISTS kanban_notify_subs (task_id TEXT, platform TEXT,
                                                       chat_id TEXT, thread_id TEXT);
        """
    )
    now = time.time()
    seen: set[str] = set()
    for task_id, kind, payload, session_id in rows:
        if task_id not in seen:
            con.execute(
                "INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?)",
                (task_id, f"卡片 {task_id}", "done", "coder", None, session_id),
            )
            seen.add(task_id)
        con.execute(
            "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) VALUES (?,?,?,?,?)",
            (task_id, None, kind, json.dumps(payload or {}, ensure_ascii=False), now),
        )
        now += 0.001
    con.commit()
    con.close()


# --- 子代理链 ---------------------------------------------------------------


class SubagentChainTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.now = time.time()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_sent_terminal_delivery_acks_and_unblocks_session(self) -> None:
        h = Harness(self.tmp)
        # 现网同形：E1 已实发、交付已终态，但汇报行卡在 reporting 且租约过期。
        h.land_report(event_id="deleg_stuck", run_id="run_stuck", sent=True, state="terminal")
        h.add_event("deleg_stuck")
        h.outbox.claim_event("deleg_stuck", idempotency_key="k1", now=self.now - 86400, lease_seconds=60)
        h.add_event("deleg_new")

        import asyncio
        asyncio.run(h.consumer.tick(self.now))

        self.assertEqual(h.outbox.get_event("deleg_stuck")["report_state"], "acked")
        self.assertIn(("deleg_stuck", "run_stuck"), h.gateway.completed)
        self.assertEqual(h.wake.ids(), ["deleg_new"])

    def test_unsendable_delivery_retries_then_fails_and_releases_session(self) -> None:
        h = Harness(self.tmp)
        h.land_report(event_id="deleg_bad", run_id="run_bad", sent=False,
                      state="terminal", cancel_reason="reply_context_mismatch")
        h.add_event("deleg_bad")
        h.outbox.claim_event("deleg_bad", idempotency_key="k1", now=self.now - 86400, lease_seconds=60)
        h.add_event("deleg_after")

        import asyncio
        asyncio.run(h.consumer.tick(self.now))
        row = h.outbox.get_event("deleg_bad")
        self.assertEqual(row["report_state"], "retry_wait")
        self.assertIn("unsendable:terminal", row["last_error"])
        # 同会话的新事件仍被唤醒（迟到的处理不占门）
        self.assertEqual(h.wake.ids(), ["deleg_after"])

        # 重试一次后仍不可发 → 上游 drop ＋ 本地报告失败终态，不再无限 reporting
        later = self.now + 60
        h.outbox.claim_event("deleg_bad", idempotency_key="k2", now=later, lease_seconds=60)
        asyncio.run(h.consumer.tick(later))
        bad = h.outbox.get_event("deleg_bad")
        self.assertEqual(bad["report_state"], "report_failed")
        self.assertEqual(h.gateway.dropped, ["deleg_bad"])
        self.assertNotIn("deleg_bad", h.wake.ids())

    def test_offline_restart_wakes_each_event_once(self) -> None:
        # NB 离线：只登记不消费，然后重开信箱（重启）仍不丢
        outbox = BackgroundReportOutbox(self.tmp / "background_reports.db")
        for event_id in ("deleg_o1", "deleg_o2"):
            outbox.upsert_event({
                "event_id": event_id, "origin_session_id": SESSION, "state": "completed",
                "result": "离线期间完成", "source": SOURCE_SUBAGENT,
            })
        outbox.close()

        tracker = RunTracker(db_path=self.tmp / "active_runs.db")
        outbox = BackgroundReportOutbox(self.tmp / "background_reports.db")
        holder: dict[str, Harness] = {}

        def landed(event_id: str) -> None:
            holder["h"].land_report(
                event_id=event_id, run_id=f"run_{event_id}", sent=True, state="terminal",
            )

        wake = FakeWake(on_wake=landed)
        harness = Harness(self.tmp, wake=wake, outbox=outbox, tracker=tracker)
        holder["h"] = harness

        import asyncio
        for step in range(4):  # 同会话串行：每拍最多一条唤醒
            asyncio.run(harness.consumer.tick(self.now + step))
        self.assertEqual(sorted(wake.ids()), ["deleg_o1", "deleg_o2"])
        self.assertEqual(outbox.get_event("deleg_o1")["report_state"], "acked")
        self.assertEqual(outbox.get_event("deleg_o2")["report_state"], "acked")

    def test_duplicate_registration_is_idempotent(self) -> None:
        h = Harness(self.tmp)
        h.add_event("deleg_dup")
        h.add_event("deleg_dup")

        import asyncio
        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.wake.ids(), ["deleg_dup"])
        self.assertEqual(
            len([e for e in h.outbox.active_events() if e["event_id"] == "deleg_dup"]), 1
        )

    def test_recently_expired_lease_still_serializes_session(self) -> None:
        """长 turn 期间租约已过期（<宽限）时不得并发唤醒同会话的第二条。"""
        h = Harness(self.tmp)
        h.land_report(event_id="deleg_running", run_id="run_running", sent=False, state="ready")
        h.add_event("deleg_running")
        h.outbox.claim_event(
            "deleg_running", idempotency_key="k", now=self.now - 30, lease_seconds=10,
        )  # 租约已过期 20s，但仍在唤醒 turn 的 600s 宽限内
        h.add_event("deleg_next")

        import asyncio
        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.wake.ids(), [])  # 串行闸生效
        # 超出宽限：真·残留，交给恢复核对，不再永久挡路
        asyncio.run(h.consumer.tick(self.now + 601))
        self.assertEqual(h.wake.ids(), ["deleg_next"])

    def test_open_stream_with_live_run_never_acks_or_fails(self) -> None:
        """R1：streaming 计划未定稿且 run 仍在跑 → 不销账、不判失败、不放行下一条。"""
        import asyncio

        h = Harness(self.tmp)
        h.land_report(event_id="deleg_open", run_id="run_open", sent=True, state="ready")
        h.tracker.set_expected_state("run_open", "streaming")
        h.add_event("deleg_open")
        h.outbox.claim_event("deleg_open", idempotency_key="k", now=self.now - 200, lease_seconds=60)
        h.add_event("deleg_after")

        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.outbox.get_event("deleg_open")["report_state"], "reporting")
        self.assertEqual(h.wake.ids(), [])
        self.assertEqual(h.gateway.dropped, [])

        # 计划定稿（run 走完终态落账）后正常销账，并放行下一条
        h.tracker.set_expected_state("run_open", "terminal")
        asyncio.run(h.consumer.tick(self.now + 1))
        self.assertEqual(h.outbox.get_event("deleg_open")["report_state"], "acked")
        self.assertEqual(h.wake.ids(), ["deleg_after"])

    def test_abandoned_stream_retries_then_releases_session(self) -> None:
        """streaming 的产出 run 已消失：不销账，但也不能永久 reporting 头阻塞。"""
        import asyncio

        h = Harness(self.tmp)
        h.land_report(event_id="deleg_gone", run_id="run_gone", sent=True, state="ready")
        h.tracker.set_expected_state("run_gone", "streaming")
        h.tracker.finish("run_gone")  # 进程中断：run 退场但没有终态落账
        h.add_event("deleg_gone")
        h.outbox.claim_event("deleg_gone", idempotency_key="k", now=self.now - 200, lease_seconds=60)
        h.add_event("deleg_after")

        asyncio.run(h.consumer.tick(self.now))
        row = h.outbox.get_event("deleg_gone")
        self.assertEqual(row["report_state"], "retry_wait")
        self.assertIn("stream_abandoned", row["last_error"])
        self.assertEqual(h.wake.ids(), ["deleg_after"])  # 同会话不再被永久挡住

        later = self.now + 60
        h.outbox.claim_event("deleg_gone", idempotency_key="k2", now=later, lease_seconds=60)
        asyncio.run(h.consumer.tick(later + 130))
        self.assertEqual(h.outbox.get_event("deleg_gone")["report_state"], "report_failed")
        self.assertEqual(h.gateway.dropped, ["deleg_gone"])

    def test_unbound_submission_is_not_judged_failed_and_releases_after_grace(self) -> None:
        """R2 覆盖：提交意图已落盘但尚未绑定 run（在途/中断）不判失败。"""
        import asyncio

        h = Harness(self.tmp)
        h.tracker.record_submission(
            idempotency_key="k1", request_body={"event": "deleg_unbound"},
            target_fingerprint="onebotv11|x", scope_key=SCOPE, session_key=SESSION,
            mode="passive", expect_structured=True, bot_self_id="999",
            source_event_id="deleg_unbound",
        )
        h.add_event("deleg_unbound")
        h.outbox.claim_event("deleg_unbound", idempotency_key="k1", now=self.now - 100,
                             lease_seconds=60)

        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.outbox.get_event("deleg_unbound")["report_state"], "reporting")
        self.assertNotIn("deleg_unbound", h.gateway.dropped)

        # 超过一次唤醒 turn 上限仍没绑上 run：认作残留，释放租约重来，不判模型失败
        asyncio.run(h.consumer.tick(self.now + 600))
        row = h.outbox.get_event("deleg_unbound")
        self.assertEqual(row["report_state"], "retry_wait")
        self.assertEqual(row["last_error"], "submission_unbound")
        self.assertEqual(row["attempt_count"], 0)  # 没形成 run，不吃生成预算
        self.assertEqual(h.gateway.dropped, [])

    def test_wake_failure_retries_within_bound(self) -> None:
        wake = FakeWake(reject="目标不在私聊白名单")
        h = Harness(self.tmp, wake=wake)
        h.add_event("deleg_retry")

        import asyncio
        asyncio.run(h.consumer.tick(self.now))
        row = h.outbox.get_event("deleg_retry")
        self.assertEqual(row["report_state"], "retry_wait")
        self.assertIn("wake_rejected", row["last_error"])
        self.assertEqual(row["attempt_count"], 0)  # 未形成提交，不吃生成预算

        asyncio.run(h.consumer.tick(self.now + 16))
        self.assertEqual(wake.ids(), ["deleg_retry", "deleg_retry"])

    def test_old_database_gets_source_column(self) -> None:
        path = self.tmp / "legacy.db"
        con = sqlite3.connect(path)
        con.executescript(
            """
            CREATE TABLE background_report_events (
                event_id TEXT PRIMARY KEY, origin_session_id TEXT NOT NULL,
                task_state TEXT NOT NULL, result TEXT NOT NULL, dispatched_at REAL,
                completed_at REAL, delivery_state TEXT, claim_id TEXT,
                report_state TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0, next_attempt_at REAL NOT NULL DEFAULT 0,
                run_id TEXT, idempotency_key TEXT, lease_expires_at REAL, last_error TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL);
            INSERT INTO background_report_events
                (event_id, origin_session_id, task_state, result, created_at, updated_at)
                VALUES ('deleg_old', 'hermes-onebotv11+private+1-g1', 'completed', '', 1, 1);
            """
        )
        con.commit()
        con.close()

        outbox = BackgroundReportOutbox(path)
        try:
            row = outbox.get_event("deleg_old")
            self.assertEqual(row["source"], SOURCE_SUBAGENT)
            # R2 代际水位列同样逐列补齐；旧行为 NULL＝代际未知，沿用旧语义。
            self.assertIn("attempt_baseline_seq", row)
            self.assertIsNone(row["attempt_baseline_seq"])
        finally:
            outbox.close()


# --- Kanban 链 --------------------------------------------------------------


class KanbanChainTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.board = self.tmp / "kanban.db"
        self.now = time.time()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_board_events_become_internal_outbox_events(self) -> None:
        build_board(self.board, [
            ("t_a", "completed", {"summary": "评审通过", "artifacts": ["/home/x/report.md"]}, SESSION),
            ("t_a", "heartbeat", None, SESSION),
            ("t_b", "review_requested", {"summary": "等待评审"}, SESSION),
        ])
        source = KanbanReportSource(self.board)
        events, cursor = source.collect("0")
        self.assertEqual([e["state"] for e in events], ["completed", "review_requested"])
        self.assertTrue(all(e["source"] == SOURCE_KANBAN for e in events))
        self.assertTrue(all(e["origin_session_id"] == SESSION for e in events))
        self.assertEqual(len({e["event_id"] for e in events}), 2)
        self.assertEqual(cursor, "3")  # 不唤醒的 heartbeat 也推进水位
        self.assertIn("评审通过", events[0]["result"])
        self.assertNotIn("/home/x/report.md", events[0]["result"])  # 本机路径不外发

    def test_ingest_persists_dedupes_and_advances_cursor(self) -> None:
        build_board(self.board, [("t_a", "completed", {"summary": "s1"}, SESSION)])
        outbox = BackgroundReportOutbox(self.tmp / "bg.db")
        source = KanbanReportSource(self.board)
        try:
            # 首次接入：从板尾播种，不回放历史（避免开机刷一屏旧卡）。
            self.assertEqual(ingest_into(outbox, source), 0)
            self.assertEqual(outbox.get_source_cursor(SOURCE_KANBAN), "1")

            # 之后的新事件：登记、去重、水位推进，重启（重开信箱）后仍按水位续读。
            build_board(self.board, [
                ("t_b", "blocked", {"reason": "等确认"}, SESSION),
                ("t_c", "completed", {"summary": "s2"}, SESSION),
            ])
            self.assertEqual(ingest_into(outbox, source), 2)
            self.assertEqual(ingest_into(outbox, source), 0)
            self.assertEqual(outbox.get_source_cursor(SOURCE_KANBAN), "3")
            row = outbox.get_event("kanban:t_b:2")
            self.assertEqual(row["source"], SOURCE_KANBAN)
            self.assertEqual(row["report_state"], "pending")
            self.assertIn("等确认", row["result"])
            outbox.close()

            reopened = BackgroundReportOutbox(self.tmp / "bg.db")
            try:
                self.assertEqual(reopened.get_source_cursor(SOURCE_KANBAN), "3")
                self.assertEqual(ingest_into(reopened, source), 0)  # 不重放
            finally:
                reopened.close()
        finally:
            pass

    def test_explicit_zero_cursor_replays_existing_board_events(self) -> None:
        build_board(self.board, [
            ("t_a", "completed", {"summary": "s1"}, SESSION),
            ("t_b", "blocked", {"reason": "等确认"}, SESSION),
        ])
        outbox = BackgroundReportOutbox(self.tmp / "bg.db")
        try:
            outbox.put_source_cursor(SOURCE_KANBAN, "0")
            self.assertEqual(ingest_into(outbox, KanbanReportSource(self.board)), 2)
            self.assertEqual(outbox.get_source_cursor(SOURCE_KANBAN), "2")
        finally:
            outbox.close()

    def test_first_seen_board_seeds_tail_without_replay(self) -> None:
        build_board(self.board, [("t_a", "completed", {"summary": "历史"}, SESSION)])
        source = KanbanReportSource(self.board)
        events, cursor = source.collect(None)
        self.assertEqual(events, [])
        self.assertEqual(cursor, "1")
        self.assertEqual(source.collect(cursor)[0], [])

    def test_unroutable_or_unknown_task_events_are_skipped_not_wedged(self) -> None:
        build_board(self.board, [
            ("t_local", "completed", {"summary": "worker 会话"}, "local-abc123"),
            ("t_a", "completed", {"summary": "正常会话"}, SESSION),
        ])
        source = KanbanReportSource(self.board)
        events, cursor = source.collect("0")
        self.assertEqual([e["event_id"] for e in events], ["kanban:t_a:2"])
        self.assertEqual(cursor, "2")

    def test_kanban_ack_is_local_only(self) -> None:
        h = Harness(self.tmp)
        h.add_event("kanban:t_a:1", source=SOURCE_KANBAN)

        import asyncio
        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.wake.ids(), ["kanban:t_a:1"])
        self.assertEqual(h.gateway.claims, [])  # 没有上游认领对象

        h.land_report(event_id="kanban:t_a:1", run_id="run_k", sent=True, state="terminal")
        asyncio.run(h.consumer.tick(self.now + 1))
        self.assertEqual(h.outbox.get_event("kanban:t_a:1")["report_state"], "acked")
        self.assertEqual(h.gateway.completed, [])  # 销账只落本地台账

    def test_sources_do_not_swallow_each_other(self) -> None:
        h = Harness(self.tmp)
        # 子代理侧一条卡住的 reporting（业务终态不可发）＋ 同会话的 Kanban 事件
        h.land_report(event_id="deleg_stuck", run_id="run_stuck", sent=False, state="silent")
        h.add_event("deleg_stuck")
        h.outbox.claim_event("deleg_stuck", idempotency_key="k", now=self.now - 86400, lease_seconds=60)
        h.add_event("kanban:t_z:9", source=SOURCE_KANBAN)

        import asyncio
        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.wake.ids(), ["kanban:t_z:9"])
        self.assertEqual(h.outbox.get_event("deleg_stuck")["report_state"], "retry_wait")

    def test_each_source_has_its_own_prompt_identity(self) -> None:
        sub = BackgroundReportConsumer._prompt({
            "event_id": "deleg_x", "state": "completed", "result": "结果", "source": SOURCE_SUBAGENT,
        })
        kan = BackgroundReportConsumer._prompt({
            "event_id": "kanban:t:1", "state": "completed", "result": "卡片", "source": SOURCE_KANBAN,
        })
        self.assertIn("后台子代理任务", sub)
        self.assertIn("系统内部通知·Kanban", kan)
        self.assertIn("不要提及事件号", kan)
        self.assertNotIn("用户说", kan)  # 不伪装真人

    def test_wake_runtime_labels_source_as_internal_identity(self) -> None:
        import asyncio

        from nonebot_plugin_hermes_adapter.core import background_report_runtime as rt

        calls: dict = {}

        async def fake_private(**kwargs):
            calls.update(kwargs)
            return None

        import nonebot_plugin_hermes_adapter.handlers.message_relay as relay

        relay_original = relay.wake_private_turn
        relay.wake_private_turn = fake_private
        try:
            asyncio.run(rt._wake(
                {"adapter_name": "onebotv11", "is_private": True, "user_id": USER, "group_id": None},
                "通知正文", "kanban:t_z:9", "kanban",
            ))
        finally:
            relay.wake_private_turn = relay_original
        self.assertEqual(calls["source"], "Kanban通知")     # 内部身份，不伪装真人
        self.assertEqual(calls["source_event_id"], "kanban:t_z:9")
        self.assertEqual(calls["text"], "通知正文")

    def test_wake_kinds_cover_terminal_decision_events(self) -> None:
        for kind in ("completed", "blocked", "review_requested", "changes_requested",
                     "gave_up", "crashed", "timed_out"):
            self.assertIn(kind, WAKE_KINDS)
        for noisy in ("heartbeat", "spawned", "claimed", "status", "archived", "unblocked"):
            self.assertNotIn(noisy, WAKE_KINDS)

    def test_handover_selects_only_stateless_nb_destinations(self) -> None:
        from nonebot_plugin_hermes_adapter.core.kanban_report_source import select_handover_subs

        subs = [
            {"task_id": "t1", "platform": "api_server", "chat_id": SESSION},
            {"task_id": "t2", "platform": "api_server", "chat_id": "cli:local-abc"},
            {"task_id": "t3", "platform": "telegram", "chat_id": "42"},
            "garbage",
        ]
        picked = select_handover_subs(subs)
        self.assertEqual([s["task_id"] for s in picked], ["t1"])

    def test_duplicate_api_server_sub_is_flagged_once(self) -> None:
        build_board(self.board, [("t_dup", "completed", {"summary": "x"}, SESSION)])
        con = sqlite3.connect(self.board)
        con.execute(
            "INSERT INTO kanban_notify_subs (task_id, platform, chat_id, thread_id) VALUES (?,?,?,?)",
            ("t_dup", "api_server", SESSION, ""),
        )
        con.commit()
        con.close()

        import nonebot_plugin_hermes_adapter.core.kanban_report_source as mod

        mod._DUPLICATE_WARNED.clear()
        source = KanbanReportSource(self.board)
        with self.assertLogs("nonebot_plugin_hermes_adapter.core.kanban_report_source", "WARNING") as logs:
            source.collect("0")
            source.collect("0")
        self.assertEqual(len(logs.output), 1)  # 每卡只提示一次


class AttemptGenerationTests(unittest.TestCase):
    """R2 排队窗口：认领时记下的提交水位把上一代交付挡在本次判定之外。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.now = time.time()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_claim_records_watermark_and_drops_previous_run_attribution(self) -> None:
        h = Harness(self.tmp)
        h.land_report(event_id="deleg_gen", run_id="run_old", sent=True, state="terminal")
        seq = h.tracker.get_latest_submission_by_source_event("deleg_gen")["submission_seq"]
        h.add_event("deleg_gen")

        claimed = h.outbox.claim_event(
            "deleg_gen", idempotency_key="background-report:deleg_gen:attempt:2",
            now=self.now, lease_seconds=120, baseline_seq=seq,
        )
        self.assertEqual(claimed["attempt_baseline_seq"], seq)
        self.assertIsNone(claimed["run_id"])  # 上一代的 run 归属不再挂着
        h.tracker.close()
        h.outbox.close()

    def test_queued_generation_is_not_judged_by_previous_suppressed_delivery(self) -> None:
        """已接受但排队的本次尝试：不判旧失败、不 drop，提交成功后正常销账。"""
        import asyncio

        h = Harness(self.tmp)
        event_id = "deleg_queued"
        h.land_report(event_id=event_id, run_id="run_old", sent=False,
                      state="suppressed", cancel_reason="bounce")
        h.add_event(event_id)
        h.outbox.claim_event(
            event_id, idempotency_key=f"background-report:{event_id}:attempt:1",
            now=self.now - 100, lease_seconds=120, baseline_seq=0,
        )
        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.outbox.get_event(event_id)["report_state"], "retry_wait")

        latest = h.tracker.get_latest_submission_by_source_event(event_id)
        # 第二次 wake 已被接受（本次尝试 claim），但同会话占槽、尚无本次 submission。
        claimed = h.outbox.claim_event(
            event_id, idempotency_key=f"background-report:{event_id}:attempt:2",
            now=self.now + 15, lease_seconds=120, baseline_seq=latest["submission_seq"],
        )
        self.assertEqual(claimed["attempt_count"], 2)
        asyncio.run(h.consumer.tick(self.now + 16))
        row = h.outbox.get_event(event_id)
        self.assertEqual(row["report_state"], "reporting")
        self.assertIsNone(row["run_id"])
        self.assertEqual(h.gateway.dropped, [])

        # 排队 turn 真正跑起来：本次尝试自己的 submission/run 与实发交付。
        h.land_report(event_id=event_id, run_id="run_new", sent=True, state="terminal",
                      key=f"background-report:{event_id}:attempt:2")
        asyncio.run(h.consumer.tick(self.now + 17))
        final = h.outbox.get_event(event_id)
        self.assertEqual(final["report_state"], "acked")
        self.assertEqual(final["run_id"], "run_new")
        self.assertEqual(h.gateway.completed, [(event_id, "run_new")])
        h.tracker.close()
        h.outbox.close()


    def test_watermark_read_failure_never_claims_with_unknown_generation(self) -> None:
        """R3：水位读失败必须 fail closed —— 不认领、不唤醒、不耗预算、不 drop。"""
        import asyncio

        h = Harness(self.tmp)
        event_id = "deleg_wm_read_fail"
        h.land_report(event_id=event_id, run_id="run_old", sent=False,
                      state="suppressed", cancel_reason="bounce")
        h.add_event(event_id)
        h.outbox.claim_event(
            event_id, idempotency_key=f"background-report:{event_id}:attempt:1",
            now=self.now - 100, lease_seconds=120, baseline_seq=0,
        )
        asyncio.run(h.consumer.tick(self.now))
        self.assertEqual(h.outbox.get_event(event_id)["report_state"], "retry_wait")
        wakes = list(h.wake.calls)

        # 临时让 tracker 连接上的下一次读取被真实 SQLite 中止，随后立即恢复。
        h.tracker._conn.set_progress_handler(lambda: 1, 1)
        try:
            asyncio.run(h.consumer.tick(self.now + 16))
        finally:
            h.tracker._conn.set_progress_handler(None, 0)
        blocked = h.outbox.get_event(event_id)
        self.assertEqual(blocked["report_state"], "retry_wait")
        self.assertEqual(blocked["attempt_count"], 1)          # 没消耗生成预算
        self.assertEqual(blocked["attempt_baseline_seq"], 0)   # 没有用 NULL 认领
        self.assertEqual(h.wake.calls, wakes)                  # 没有唤醒
        self.assertEqual(h.gateway.dropped, [])

        # 读恢复后按当前水位认领：旧 suppressed 不参与本次判定。
        asyncio.run(h.consumer.tick(self.now + 17))
        claimed = h.outbox.get_event(event_id)
        self.assertEqual(claimed["attempt_count"], 2)
        self.assertEqual(
            claimed["attempt_baseline_seq"],
            h.tracker.get_latest_submission_by_source_event(event_id)["submission_seq"],
        )
        self.assertEqual(len(h.wake.calls), len(wakes) + 1)
        asyncio.run(h.consumer.tick(self.now + 18))
        self.assertEqual(h.outbox.get_event(event_id)["report_state"], "reporting")
        h.tracker.close()
        h.outbox.close()


if __name__ == "__main__":
    unittest.main()
