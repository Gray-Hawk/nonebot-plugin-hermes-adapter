"""F1 回归：相互独立的并发对账不得共享可重入认领身份（真实链）。

真实 SQLite ＋ 真实 `core.outbound` 发送与 `claim_component` 认领链，只把平台
边界（`alconna.UniMessage.send`）换成进程内替身；不 mock 计数、不关闭发送。

覆盖：
  - 顺序对账（启动对账 → scope 对账）只发 1 次；
  - 同进程重叠的 `reconcile_pending_deliveries` ＋ `reconcile_scope` 只发 1 次
    （修复前为 2 次，见 ACCEPTANCE/probes/probe_reconcile_overlap.py）；
  - attempt_count 与平台回执按一次尝试记；
  - 不同 run 的对账互不阻塞；
  - 崩溃进程留下的租约按原语义由租约过期后的下一次对账接管。
"""

from __future__ import annotations

import asyncio
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
    prepare_terminal_batch,
)

BOT = "999"
CRASHED_OWNER = "reconcile-4242-deadbeef"


def _land_run(tracker: RunTracker, *, run_id: str, group_id: str) -> None:
    """落一条「run 已终态、交付待对账」的真实台账现场。"""
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
        run_id=run_id, output='{}\n{"body":"补发的正文"}', expect_structured=True,
    )
    prepare_terminal_batch(
        tracker=tracker, run_id=run_id,
        units=[{"body": "补发的正文", "media_urls": [], "file_paths": [], "reply_to": ""}],
    )


class ReconcileClaimIdentityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.registry = BotRegistry()
        for group_id in ("g1", "g2"):
            self.registry.upsert(
                adapter="onebotv11", scope="group", scope_id=group_id,
                bot_self_id=BOT, target=SimpleNamespace(private=False, id=group_id), ts=0,
            )
        self.runtime = SimpleNamespace(
            run_tracker=self.tracker, bot_registry=self.registry, message_buffer=MagicMock(),
        )
        self.cfg = RunsDeliveryConfig(mode="repair", grace_sec=0)
        self.platform_calls: list[str] = []

    def _patches(self, on_send):
        ob = monitor._ob
        stack = [
            patch.object(monitor, "_runtime", lambda: self.runtime),
            patch.object(monitor, "load_runs_delivery_config", lambda: self.cfg),
            patch.object(monitor, "get_bot", lambda self_id: MagicMock(self_id=self_id)),
            patch.object(monitor, "_maybe_prune_delivery_ledger", lambda t: None),
            patch.object(ob, "load_misc_config", lambda: SimpleNamespace(max_length=4000)),
            patch.object(ob, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=False)),
            patch.object(ob, "get_bot_nickname", AsyncMock(return_value="ledger-bot")),
            patch.object(ob.alconna.UniMessage, "send", on_send),
        ]
        return stack

    @staticmethod
    def _enter(stack):
        for item in stack:
            item.start()

    @staticmethod
    def _exit(stack):
        for item in reversed(stack):
            item.stop()

    def _platform(self, *, block_on: set[str] | None = None):
        """平台替身：记录每次真实出站；block_on 里的 group 第一次发送挂起等释放。"""
        calls: list[str] = []
        blocking = set(block_on or ())
        entered = {group_id: asyncio.Event() for group_id in blocking}
        releases = {group_id: asyncio.Event() for group_id in blocking}
        held: set[str] = set()

        async def fake_send(self, *, target=None, bot=None, **kwargs):
            assert self.__class__.__name__ == "UniMessage", "platform boundary must be UniMessage"
            group_id = getattr(target, "id", "?")
            calls.append(group_id)
            if group_id in blocking and group_id not in held:
                held.add(group_id)
                entered[group_id].set()
                await releases[group_id].wait()
            return SimpleNamespace(msg_ids=[f"ack-{len(calls)}"])

        return fake_send, calls, entered, releases

    async def test_sequential_reconcile_sends_once(self) -> None:
        _land_run(self.tracker, run_id="run-seq", group_id="g1")
        fake, calls, _, _ = self._platform()
        stack = self._patches(fake)
        self._enter(stack)
        try:
            await monitor.reconcile_pending_deliveries()
            await monitor.reconcile_scope("onebotv11:group:g1", reason="turn_end")
        finally:
            self._exit(stack)
        component = self.tracker.get_component(run_id="run-seq", component_id="primary:0")
        self.assertEqual(calls, ["g1"])
        self.assertEqual(component["state"], "sent")
        self.assertEqual(component["attempt_count"], 1)
        self.assertEqual(component["msg_ids"], ["ack-1"])

    async def test_overlapping_reconciles_send_once(self) -> None:
        """同进程重叠对账：第二次不得凭同 owner 重入再调一次平台。"""
        _land_run(self.tracker, run_id="run-ovl", group_id="g1")
        fake, calls, entered, releases = self._platform(block_on={"g1"})
        stack = self._patches(fake)
        self._enter(stack)
        try:
            first = asyncio.create_task(monitor.reconcile_pending_deliveries())
            await asyncio.wait_for(entered["g1"].wait(), 2)
            second = asyncio.create_task(
                monitor.reconcile_scope("onebotv11:group:g1", reason="turn_end")
            )
            # 第二次在第一次的租约期间完成：它必须被 busy 挡住，而不是重入发送。
            await asyncio.wait_for(second, 2)
            releases["g1"].set()
            await asyncio.wait_for(first, 2)
        finally:
            self._exit(stack)
        component = self.tracker.get_component(run_id="run-ovl", component_id="primary:0")
        self.assertEqual(calls, ["g1"])
        self.assertEqual(component["state"], "sent")
        self.assertEqual(component["attempt_count"], 1)
        self.assertEqual(component["msg_ids"], ["ack-1"])

    async def test_different_runs_do_not_block(self) -> None:
        _land_run(self.tracker, run_id="run-a", group_id="g1")
        _land_run(self.tracker, run_id="run-b", group_id="g2")
        fake, calls, entered, releases = self._platform(block_on={"g1"})
        stack = self._patches(fake)
        self._enter(stack)
        try:
            task_a = asyncio.create_task(monitor.reconcile_run("run-a", reason="startup"))
            await asyncio.wait_for(entered["g1"].wait(), 2)
            task_b = asyncio.create_task(monitor.reconcile_run("run-b", reason="startup"))
            await asyncio.wait_for(task_b, 2)  # A 还卡在平台上，B 必须能自行完成
            releases["g1"].set()
            await asyncio.wait_for(task_a, 2)
        finally:
            self._exit(stack)
        comp_a = self.tracker.get_component(run_id="run-a", component_id="primary:0")
        comp_b = self.tracker.get_component(run_id="run-b", component_id="primary:0")
        self.assertEqual(comp_a["state"], "sent")
        self.assertEqual(comp_b["state"], "sent")
        self.assertEqual(comp_a["attempt_count"], 1)
        self.assertEqual(comp_b["attempt_count"], 1)
        self.assertEqual(sorted(calls), ["g1", "g2"])

    async def test_expired_lease_is_taken_over(self) -> None:
        """崩溃进程留下的认领按租约语义恢复：过期后由新一次对账接管。"""
        _land_run(self.tracker, run_id="run-crash", group_id="g1")
        # lease_sec=-1 → claim_until == now，等价于「崩溃后租约已过期」。
        self.assertEqual(
            self.tracker.claim_component(
                run_id="run-crash", component_id="primary:0",
                owner=CRASHED_OWNER, lease_sec=-1,
            ),
            "send",
        )
        fake, calls, _, _ = self._platform()
        stack = self._patches(fake)
        self._enter(stack)
        try:
            stats = await monitor.reconcile_run("run-crash", reason="startup")
        finally:
            self._exit(stack)
        component = self.tracker.get_component(run_id="run-crash", component_id="primary:0")
        self.assertEqual(len(calls), 1)
        self.assertEqual(stats["run_id"], "run-crash")
        self.assertEqual(component["state"], "sent")
        self.assertEqual(component["attempt_count"], 2)  # 崩溃那次 + 接管这次
        self.assertIsNone(component["claim_owner"])
        self.assertEqual(component["msg_ids"], ["ack-1"])

    async def test_live_foreign_lease_is_not_taken_over(self) -> None:
        """他人有效租约仍按原语义挡住：不发送、不推进尝试次数。"""
        _land_run(self.tracker, run_id="run-live", group_id="g1")
        self.assertEqual(
            self.tracker.claim_component(
                run_id="run-live", component_id="primary:0",
                owner=CRASHED_OWNER, lease_sec=60,
            ),
            "send",
        )
        fake, calls, _, _ = self._platform()
        stack = self._patches(fake)
        self._enter(stack)
        try:
            await monitor.reconcile_run("run-live", reason="startup")
        finally:
            self._exit(stack)
        component = self.tracker.get_component(run_id="run-live", component_id="primary:0")
        delivery = self.tracker.get_delivery("run-live")
        self.assertEqual(calls, [])
        self.assertEqual(component["state"], "claimed")
        self.assertEqual(component["claim_owner"], CRASHED_OWNER)
        self.assertEqual(component["attempt_count"], 1)
        self.assertEqual(delivery["expected_state"], "ready")

    def test_reconcile_owner_is_unique_per_call(self) -> None:
        first, second = monitor._reconcile_owner(), monitor._reconcile_owner()
        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith(f"reconcile-{monitor.os.getpid()}-"))
        self.assertTrue(second.startswith(f"reconcile-{monitor.os.getpid()}-"))


if __name__ == "__main__":
    unittest.main()
