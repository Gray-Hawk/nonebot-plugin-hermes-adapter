"""P-06 回归：turn_end 对账落在宽限窗内被 deferred 后,宽限结束自动重排补发。

真实 SQLite 台账 + 真实 ``core.outbound`` 发送/认领链,只把平台边界
(``alconna.UniMessage.send``)换成进程内替身;宽限用真实 asyncio 计时(不冻结
时钟、不 mock sleep 结构),因此"宽限结束后无需新 run／重启即补发"是被真实
状态链证明的。

覆盖:
  - 命中 deferred=grace 后按剩余宽限重排,宽限结束补发一次(无新 run、无重启);
  - 已 sent 的正文不重发(只补同一份的 history);
  - cancelled 交付／cancelled 组件不复活;
  - observe 模式行为不变且不重排;
  - 同一 run 的重复 turn_end 只保留一个重排、只发一次;
  - 负对照:去掉重排钩子后,缺件在宽限结束后仍停在 pending(修复前现场)。
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import outbound as ob  # noqa: E402
from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor  # noqa: E402
from nonebot_plugin_hermes_adapter.core.bot_registry import BotRegistry  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_delivery_config import (  # noqa: E402
    RunsDeliveryConfig,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    prepare_terminal_batch,
)

BOT = "999"
GRACE = 0.5


def _land_run(tracker: RunTracker, *, run_id: str = "run-grace", group_id: str = "g1",
              body: str = "宽限后应补发的正文") -> None:
    """落一条「run 已终态、计划完整、组件全 pending」的真实台账现场。"""
    scope = f"onebotv11:group:{group_id}"
    tracker.upsert(
        run_id=run_id, scope_key=scope, adapter="onebotv11", is_private=False,
        user_id="u1", group_id=group_id, bot_self_id=BOT, session_key="s",
        mode="reactive", expect_structured=True,
    )
    tracker.record_submission(
        idempotency_key=f"idem-{run_id}", request_body={}, target_fingerprint="x|y",
        scope_key=scope, session_key="s", mode="reactive",
        expect_structured=True, bot_self_id=BOT,
    )
    tracker.bind_submission_and_register(
        idempotency_key=f"idem-{run_id}", run_id=run_id, scope_key=scope,
        adapter="onebotv11", is_private=False, user_id="u1", group_id=group_id,
        bot_self_id=BOT, session_key="s", mode="reactive", expect_structured=True,
    )
    tracker.record_expected_delivery(
        run_id=run_id, output=f'{{}}\n{{"body":"{body}"}}', expect_structured=True,
    )
    prepare_terminal_batch(
        tracker=tracker, run_id=run_id,
        units=[{"body": body, "media_urls": [], "file_paths": [], "reply_to": ""}],
    )
    tracker.finish(run_id)


class GraceRearmReconcileTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.registry = BotRegistry()
        self.registry.upsert(
            adapter="onebotv11", scope="group", scope_id="g1",
            bot_self_id=BOT, target=SimpleNamespace(private=False, id="g1"), ts=0,
        )
        self.message_buffer = MagicMock()
        self.runtime = SimpleNamespace(
            run_tracker=self.tracker, bot_registry=self.registry,
            message_buffer=self.message_buffer,
        )
        self.cfg = RunsDeliveryConfig(mode="repair", grace_sec=GRACE)
        self.platform_calls: list[str] = []
        monitor._grace_rearm_pending.clear()
        self.addCleanup(monitor._grace_rearm_pending.clear)

        self.patches = [
            patch.object(monitor, "_runtime", lambda: self.runtime),
            patch.object(monitor, "load_runs_delivery_config", lambda: self.cfg),
            patch.object(monitor, "get_bot", lambda self_id: MagicMock(self_id=self_id)),
            patch.object(monitor, "_maybe_prune_delivery_ledger", lambda t: None),
            patch.object(ob, "load_misc_config", lambda: SimpleNamespace(max_length=4000)),
            patch.object(ob, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=False)),
            patch.object(ob, "get_bot_nickname", AsyncMock(return_value="ledger-bot")),
            patch.object(ob.alconna.UniMessage, "send", self._platform_boundary()),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def _platform_boundary(self):
        async def platform_send(uni_self, *, target=None, bot=None, **kwargs):
            self.platform_calls.append(str(uni_self))
            return SimpleNamespace(msg_ids=[f"ack-{len(self.platform_calls)}"])

        return platform_send

    async def _wait_until(self, predicate, timeout: float = 4.0) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.01)
        return bool(predicate())

    def _component(self, run_id: str, component_id: str) -> dict:
        return self.tracker.get_component(run_id=run_id, component_id=component_id)

    async def test_turn_end_grace_rearm_delivers_after_grace_without_new_run(self) -> None:
        _land_run(self.tracker)
        started = time.monotonic()

        stats = await monitor.reconcile_run("run-grace", reason="turn_end")

        self.assertEqual(stats["deferred"], "grace")
        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-grace", "primary:0")["state"], "pending")
        self.assertIn("run-grace", monitor._grace_rearm_pending)

        # 宽限结束前不得偷偷补发
        await asyncio.sleep(GRACE * 0.4)
        self.assertEqual(self.platform_calls, [])

        # 没有任何新 run / startup / 真实感知,仅靠重排完成补发
        self.assertTrue(
            await self._wait_until(
                lambda: self._component("run-grace", "primary:0")["state"] == "sent"
            )
        )
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, GRACE * 0.8)
        self.assertEqual(self.platform_calls, ["宽限后应补发的正文"])
        self.assertEqual(self._component("run-grace", "history:0")["state"], "sent")
        self.assertEqual(self._component("run-grace", "primary:0")["attempt_count"], 1)
        self.assertEqual(
            self.tracker.get_delivery("run-grace")["expected_state"], "terminal"
        )
        self.assertTrue(
            await self._wait_until(lambda: "run-grace" not in monitor._grace_rearm_pending)
        )

    async def test_already_sent_primary_is_not_resent_after_grace(self) -> None:
        _land_run(self.tracker, run_id="run-sent")
        self.tracker.mark_component_sent(
            run_id="run-sent", component_id="primary:0", msg_ids=["m1"]
        )

        stats = await monitor.reconcile_run("run-sent", reason="turn_end")

        self.assertEqual(stats["deferred"], "grace")
        self.assertTrue(
            await self._wait_until(
                lambda: self._component("run-sent", "history:0")["state"] == "sent"
            )
        )
        self.assertEqual(self.platform_calls, [])  # 已 sent 的正文不复活
        self.assertEqual(self._component("run-sent", "primary:0")["msg_ids"], ["m1"])
        # 直接落 sent 的正文从未被认领重发,尝试次数保持 0
        self.assertEqual(self._component("run-sent", "primary:0")["attempt_count"], 0)

    async def test_cancelled_delivery_is_not_rearmed(self) -> None:
        _land_run(self.tracker, run_id="run-cancelled")
        self.tracker.set_expected_state("run-cancelled", "cancelled", cancel_reason="clear")

        stats = await monitor.reconcile_run("run-cancelled", reason="turn_end")

        self.assertEqual(stats["skipped"], "settled_or_missing")
        self.assertNotIn("run-cancelled", monitor._grace_rearm_pending)
        await asyncio.sleep(GRACE * 2)
        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-cancelled", "primary:0")["state"], "pending")

    async def test_cancelled_components_do_not_revive_after_grace(self) -> None:
        _land_run(self.tracker, run_id="run-cparts")
        for component_id in ("primary:0", "history:0"):
            # 业务免发终态(clear/取消的既有写法):组件 cancelled,交付行仍 ready。
            self.tracker._mark_component("run-cparts", component_id, "cancelled")

        await monitor.reconcile_run("run-cparts", reason="turn_end")
        self.assertTrue(
            await self._wait_until(
                lambda: self.tracker.get_delivery("run-cparts")["expected_state"]
                == "terminal"
            )
        )
        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-cparts", "primary:0")["state"], "cancelled")

    async def test_observe_mode_unchanged_and_not_rearmed(self) -> None:
        _land_run(self.tracker, run_id="run-observe")
        self.cfg = RunsDeliveryConfig(mode="observe", grace_sec=GRACE)

        stats = await monitor.reconcile_run("run-observe", reason="turn_end")

        self.assertEqual(stats["mode"], "observe")
        self.assertEqual(stats["would_repair"], 2)
        self.assertNotIn("run-observe", monitor._grace_rearm_pending)
        await asyncio.sleep(GRACE * 2)
        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-observe", "primary:0")["state"], "pending")

    async def test_repeated_turn_end_rearms_once_and_sends_once(self) -> None:
        _land_run(self.tracker, run_id="run-dup")

        first = await monitor.reconcile_run("run-dup", reason="turn_end")
        second = await monitor.reconcile_run("run-dup", reason="turn_end")

        self.assertEqual(first["deferred"], "grace")
        self.assertEqual(second["deferred"], "grace")
        self.assertEqual(
            [run_id for run_id in monitor._grace_rearm_pending if run_id == "run-dup"],
            ["run-dup"],
        )
        self.assertTrue(
            await self._wait_until(
                lambda: self._component("run-dup", "primary:0")["state"] == "sent"
            )
        )
        await asyncio.sleep(GRACE)  # 再等一拍:不得出现第二次重排/第二次发送
        self.assertEqual(self.platform_calls, ["宽限后应补发的正文"])
        self.assertEqual(self._component("run-dup", "primary:0")["attempt_count"], 1)

    async def test_negative_control_without_rearm_stays_pending(self) -> None:
        """负对照:关掉宽限重排后,缺件在宽限结束后仍停在 pending(修复前现场)。"""
        _land_run(self.tracker, run_id="run-noop")
        with patch.object(monitor, "_schedule_reconcile_after_grace", lambda *a: None):
            stats = await monitor.reconcile_run("run-noop", reason="turn_end")

        self.assertEqual(stats["deferred"], "grace")
        await asyncio.sleep(GRACE * 2)
        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-noop", "primary:0")["state"], "pending")


if __name__ == "__main__":
    unittest.main()
