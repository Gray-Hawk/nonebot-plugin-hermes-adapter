"""证据工具安全回归（R3）：现网取证探针不得打开可写台账。

``compare_ack_predicate.py`` 原来在生产 ``active_runs.db`` 上构造普通
``RunTracker``；该构造器会开可写连接、跑 DDL/PRAGMA，并把 ``steer_ledger``
里 ``sending``／``reconciling`` 的行提交回 ``pending``。在现网台账上用它是写
生产库，不是只读取证。

本用例锁三件事：

1. 探针的 ``ReadOnlyLedger`` 每条连接都是 ``mode=ro`` ＋ ``query_only=ON``，
   写入语句直接被 SQLite 拒绝；
2. 读一遍真实台账后，``sending`` 行、交付/组件状态与 schema 逐字不变；
3. 探针源码不再构造 ``RunTracker``（AST 级检查，防止悄悄退回去），且所有
   ``sqlite3.connect`` 都带 ``mode=ro``。

控制器只在临时库上演示旧写法确实会改 ``sending`` → ``pending``，作为这条
安全约束的成因证据；不触碰生产。
"""

from __future__ import annotations

import ast
import importlib.util
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402

PROBE = ROOT / "EVIDENCE" / "notifications" / "probes" / "compare_ack_predicate.py"
SCOPE = "onebotv11:private:1581124503"
SESSION = "hermes-onebotv11+private+158****4503-g404"
RUN = "run_probe_ro"
EVENT = "deleg_probe_ro"


def _load_probe():
    spec = importlib.util.spec_from_file_location("compare_ack_predicate_probe", PROBE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _snapshot(path: Path) -> dict:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        schema = conn.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
        return {
            "schema": schema,
            "steer": conn.execute(
                "SELECT m_id, status FROM steer_ledger ORDER BY m_id"
            ).fetchall(),
            "delivery": conn.execute(
                "SELECT run_id, expected_state, output FROM run_deliveries ORDER BY run_id"
            ).fetchall(),
            "components": conn.execute(
                "SELECT run_id, component_id, state FROM delivery_components"
                " ORDER BY run_id, component_id"
            ).fetchall(),
            "steer_count": conn.execute("SELECT COUNT(*) FROM steer_ledger").fetchone()[0],
        }
    finally:
        conn.close()


class ReadOnlyEvidenceProbeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "active_runs.db"
        tracker = RunTracker(self.path)
        # 一份「live-style」的在途 steer 认领：旧构造器会把它提交回 pending。
        tracker.claim_steer(
            run_id="run_inflight", scope_key=SCOPE, session_key=SESSION,
            accepted_at=time.time(),
            items=[{"m_id": "m:1", "sender_id": "u1", "steer_text": "在途 steer"}],
        )
        tracker._conn.commit()
        # 一份已实发、已终态的报告交付（现网同形：reply:<seq>:primary）。
        tracker.record_submission(
            idempotency_key="background-report:deleg_probe_ro:attempt:1",
            request_body={"event": EVENT}, target_fingerprint="onebotv11|x",
            scope_key=SCOPE, session_key=SESSION, mode="passive", expect_structured=True,
            bot_self_id="999", source_event_id=EVENT,
        )
        tracker.bind_submission_and_register(
            idempotency_key="background-report:deleg_probe_ro:attempt:1", run_id=RUN,
            scope_key=SCOPE, adapter="onebotv11", is_private=True, user_id="1581124503",
            group_id=None, bot_self_id="999", session_key=SESSION, mode="passive",
            expect_structured=True,
        )
        tracker.record_expected_delivery(run_id=RUN, output="{}", expect_structured=True)
        tracker.upsert_delivery_plan(
            run_id=RUN,
            plan={"live": {"1": {"cleaned_text": "汇报", "media_urls": [], "file_paths": [],
                                 "reply_to": "", "reply_context": "", "reply_seq": 1}}},
            components=[
                {"component_id": "reply:1:primary", "kind": "primary", "payload_sha256": "x"},
                {"component_id": "reply:1:history", "kind": "history", "payload_sha256": "y"},
            ],
        )
        for component_id in ("reply:1:primary", "reply:1:history"):
            token = tracker.claim_component_attempt(
                run_id=RUN, component_id=component_id, owner="probe",
            ).token
            tracker.mark_component_sent(
                run_id=RUN, component_id=component_id, msg_ids=["322079804"], token=token,
            )
        tracker.set_expected_state(RUN, "terminal")
        tracker.close()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_readonly_ledger_cannot_write_and_leaves_state_untouched(self) -> None:
        before = _snapshot(self.path)
        module = _load_probe()
        ledger = module.ReadOnlyLedger(self.path)
        try:
            self.assertEqual(ledger._conn.execute("PRAGMA query_only").fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                ledger._conn.execute("UPDATE steer_ledger SET status='pending'")
            delivery = ledger.get_delivery(RUN)
            self.assertEqual(delivery["expected_state"], "terminal")
            components = ledger.list_components(RUN)
            self.assertEqual(
                sorted((c["component_id"], c["state"]) for c in components),
                [("reply:1:history", "sent"), ("reply:1:primary", "sent")],
            )
            self.assertTrue(module.legacy_is_sent(ledger, RUN) is False)  # 旧判定不认多份命名
            from nonebot_plugin_hermes_adapter.core.background_report_consumer import (
                BackgroundReportConsumer,
            )

            consumer = BackgroundReportConsumer(
                outbox=None, tracker=ledger, gateway=None,
                resolve_route=None, wake=None, reconcile_run=None,
            )
            self.assertEqual(consumer._delivery_verdict(RUN), "sent")  # 新判定认实发事实
        finally:
            ledger.close()
        self.assertEqual(_snapshot(self.path), before)

    def test_probe_source_never_constructs_run_tracker(self) -> None:
        source = PROBE.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
                self.assertNotEqual(name, "RunTracker", "证据探针不得构造可写 RunTracker")
                if name == "connect":
                    literal = " ".join(
                        part.value for part in ast.walk(node)
                        if isinstance(part, ast.Constant) and isinstance(part.value, str)
                    )
                    self.assertIn("mode=ro", literal, "证据探针只能开 mode=ro 连接")
        self.assertIn("query_only=ON", source)
        self.assertIn("PRAGMA query_only", source)

    def test_control_old_constructor_would_reset_sending_row(self) -> None:
        """成因控制（仅临时库）：旧写法第二次构造会把 sending 提交回 pending。"""
        before = _snapshot(self.path)
        self.assertEqual(before["steer"], [("m:1", "sending")])
        reopened = RunTracker(self.path)
        try:
            status = reopened._conn.execute(
                "SELECT status FROM steer_ledger WHERE m_id='m:1'"
            ).fetchone()[0]
        finally:
            reopened.close()
        self.assertEqual(status, "pending")  # 证明这条安全约束不是纸面要求


if __name__ == "__main__":
    unittest.main()
