"""P-16 重复终态落账回归：业务终态不得被同 run 的再次落账回臂成 ready。

真实 SQLite ＋ 真实 `reconcile_run`（repair）＋ 真实 `core.outbound` 发送链，
只替换平台边界与配置读取。覆盖：

  - silent／suppressed／cancelled／expired 上重复 `record_expected_delivery`：
    状态保持、完全重复幂等（零字段变化）、对账零平台调用；
  - 已 terminal 的交付重复落账同样不回臂；
  - 首次落账（无交付行）仍 ready 且正常发送一次；
  - E3 `streaming→ready` 合法提升不受影响；
  - `failed→ready` 只允许恢复链显式 rearm；
  - 既有拒绝语义不变：无绑定 submission 仍 KeyError，已封存计划冲突仍拒绝。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor  # noqa: E402
from nonebot_plugin_hermes_adapter.core.bot_registry import BotRegistry  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_delivery_config import (  # noqa: E402
    RunsDeliveryConfig,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    TerminalPlanError,
    prepare_terminal_batch,
)

BOT = "999"
GROUP = "g1"
SCOPE = f"onebotv11:group:{GROUP}"
OUTPUT = '{}\n{"body":"终态正文"}'
UNITS = [{"body": "终态正文", "media_urls": [], "file_paths": [], "reply_to": ""}]
BUSINESS_TERMINALS = ("silent", "suppressed", "cancelled", "expired")


def land_run(
    tracker: RunTracker, *, run_id: str, with_plan: bool = True, expected: bool = True
) -> None:
    """落一条已绑定 submission 的 run（可留空交付行给 E3 流式入口）。"""
    tracker.upsert(
        run_id=run_id, scope_key=SCOPE, adapter="onebotv11", is_private=False,
        user_id="u1", group_id=GROUP, bot_self_id=BOT, session_key="s",
        mode="reactive", expect_structured=True,
    )
    tracker.record_submission(
        idempotency_key=f"idem-{run_id}", request_body={}, target_fingerprint="x|y",
        scope_key=SCOPE, session_key="s", mode="reactive",
        expect_structured=True, bot_self_id=BOT,
    )
    tracker.bind_submission_and_register(
        idempotency_key=f"idem-{run_id}", run_id=run_id, scope_key=SCOPE,
        adapter="onebotv11", is_private=False, user_id="u1", group_id=GROUP,
        bot_self_id=BOT, session_key="s", mode="reactive", expect_structured=True,
    )
    if not expected:
        return
    tracker.record_expected_delivery(run_id=run_id, output=OUTPUT, expect_structured=True)
    if with_plan:
        prepare_terminal_batch(tracker=tracker, run_id=run_id, units=UNITS)


class RepeatTerminalTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.registry = BotRegistry()
        self.registry.upsert(
            adapter="onebotv11", scope="group", scope_id=GROUP,
            bot_self_id=BOT, target=SimpleNamespace(private=False, id=GROUP), ts=0,
        )
        self.runtime = SimpleNamespace(
            run_tracker=self.tracker, bot_registry=self.registry, message_buffer=MagicMock(),
        )
        self.platform_calls: list[str] = []
        self._stack: list = []

    def _enter(self, *, cfg: RunsDeliveryConfig | None = None) -> None:
        cfg = cfg or RunsDeliveryConfig(mode="repair", grace_sec=0)
        ob = monitor._ob

        async def fake_send(_msg_self, *, target=None, bot=None, **kwargs):
            self.platform_calls.append(str(getattr(target, "id", "?")))
            return SimpleNamespace(msg_ids=[f"ack-{len(self.platform_calls)}"])

        self._stack = [
            patch.object(monitor, "_runtime", lambda: self.runtime),
            patch.object(monitor, "load_runs_delivery_config", lambda: cfg),
            patch.object(monitor, "get_bot", lambda self_id: MagicMock(self_id=self_id)),
            patch.object(monitor, "_maybe_prune_delivery_ledger", lambda t: None),
            patch.object(ob, "load_misc_config", lambda: SimpleNamespace(max_length=4000)),
            patch.object(ob, "load_chat_config",
                         lambda: SimpleNamespace(long_reply_forward=False)),
            patch.object(ob, "get_bot_nickname", AsyncMock(return_value="repeat-bot")),
            patch.object(ob.alconna.UniMessage, "send", fake_send),
        ]
        for item in self._stack:
            item.start()

    def tearDown(self) -> None:
        self._exit()

    def _exit(self) -> None:
        for item in reversed(self._stack):
            item.stop()
        self._stack = []

    def _state(self, run_id: str) -> str:
        return self.tracker.get_delivery(run_id)["expected_state"]

    async def test_business_terminal_survives_repeat_record_and_sends_nothing(self) -> None:
        for terminal in BUSINESS_TERMINALS:
            with self.subTest(terminal=terminal):
                run_id = f"run-{terminal}"
                land_run(self.tracker, run_id=run_id)
                self.tracker.set_expected_state(run_id, terminal, cancel_reason="probe")
                before = self.tracker.get_delivery(run_id)

                self.assertFalse(self.tracker.record_expected_delivery(
                    run_id=run_id, output=OUTPUT, expect_structured=True,
                ))
                self.assertFalse(self.tracker.record_expected_delivery(
                    run_id=run_id, output=OUTPUT, expect_structured=True,
                ))
                after = self.tracker.get_delivery(run_id)

                self.assertEqual(after["expected_state"], terminal)
                self.assertEqual(after["cancel_reason"], before["cancel_reason"])
                self.assertEqual(after["updated_at"], before["updated_at"])
                # 已销账的交付不再进对账扫描，平台零调用。
                self._enter()
                stats = await monitor.reconcile_run(run_id, reason="startup")
                self.assertEqual(self.platform_calls, [])
                self.assertEqual(stats.get("skipped"), "settled_or_missing")
                self.assertEqual(self._state(run_id), terminal)
                component = self.tracker.get_component(run_id=run_id, component_id="primary:0")
                self.assertEqual(component["state"], "pending")
                self.assertEqual(component["attempt_count"], 0)
                self._exit()

    async def test_terminal_delivery_survives_repeat_record(self) -> None:
        land_run(self.tracker, run_id="run-terminal")
        self._enter()
        await monitor.reconcile_run("run-terminal", reason="startup")
        self.assertEqual(self.platform_calls, [GROUP])
        self.assertEqual(self._state("run-terminal"), "terminal")
        self.assertFalse(self.tracker.record_expected_delivery(
            run_id="run-terminal", output=OUTPUT, expect_structured=True,
        ))
        self.assertEqual(self._state("run-terminal"), "terminal")
        self.assertEqual(self.platform_calls, [GROUP])

    async def test_first_record_is_ready_and_sends_once(self) -> None:
        land_run(self.tracker, run_id="run-first", expected=False)
        self.assertIsNone(self.tracker.get_delivery("run-first"))
        self.assertTrue(self.tracker.record_expected_delivery(
            run_id="run-first", output=OUTPUT, expect_structured=True,
        ))
        prepare_terminal_batch(tracker=self.tracker, run_id="run-first", units=UNITS)
        self.assertEqual(self._state("run-first"), "ready")
        self._enter()
        stats = await monitor.reconcile_run("run-first", reason="startup")
        self.assertEqual(self.platform_calls, [GROUP])
        self.assertTrue(stats.get("terminal"))
        self.assertEqual(self._state("run-first"), "terminal")

    def test_streaming_promotion_to_ready_is_preserved(self) -> None:
        land_run(self.tracker, run_id="run-stream", expected=False)
        self.tracker.open_stream_delivery("run-stream")
        self.assertEqual(self._state("run-stream"), "streaming")
        self.assertTrue(self.tracker.record_expected_delivery(
            run_id="run-stream", output=OUTPUT, expect_structured=True,
        ))
        self.assertEqual(self._state("run-stream"), "ready")

    def test_failed_rearm_requires_explicit_flag(self) -> None:
        land_run(self.tracker, run_id="run-failed")
        self.tracker.set_expected_state("run-failed", "failed", cancel_reason="stream_after_partial")
        self.assertFalse(self.tracker.record_expected_delivery(
            run_id="run-failed", output=OUTPUT, expect_structured=True,
        ))
        self.assertEqual(self._state("run-failed"), "failed")
        self.assertTrue(self.tracker.record_expected_delivery(
            run_id="run-failed", output=OUTPUT, expect_structured=True, rearm=True,
        ))
        self.assertEqual(self._state("run-failed"), "ready")

    def test_rearm_never_revives_business_terminal(self) -> None:
        """恢复链显式 rearm 也只能救 failed；业务终态仍然保持。"""
        land_run(self.tracker, run_id="run-rearm-suppressed")
        self.tracker.set_expected_state(
            "run-rearm-suppressed", "suppressed", cancel_reason="bounce"
        )
        self.assertFalse(self.tracker.record_expected_delivery(
            run_id="run-rearm-suppressed", output=OUTPUT, expect_structured=True, rearm=True,
        ))
        self.assertEqual(self._state("run-rearm-suppressed"), "suppressed")

    def test_unbound_submission_still_rejected(self) -> None:
        """既有拒绝语义不变：没有绑定 submission 不得伪造交付身份。"""
        with self.assertRaises(KeyError):
            self.tracker.record_expected_delivery(
                run_id="run-ghost", output=OUTPUT, expect_structured=True,
            )

    def test_sealed_plan_conflict_still_rejected_after_repeat_record(self) -> None:
        """重复落账不放宽计划约束：已封存计划冲突仍按既有规则拒绝。"""
        land_run(self.tracker, run_id="run-plan")
        self.tracker.record_expected_delivery(
            run_id="run-plan", output=OUTPUT, expect_structured=True,
        )
        with self.assertRaises(TerminalPlanError):
            prepare_terminal_batch(
                tracker=self.tracker, run_id="run-plan",
                units=[{"body": "换了正文", "media_urls": [], "file_paths": [], "reply_to": ""}],
            )


if __name__ == "__main__":
    unittest.main()
