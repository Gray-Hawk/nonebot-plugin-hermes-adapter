"""P-16 恢复链回归:业务终态的拒绝落账必须贯穿恢复出站。

真实 SQLite 台账 + 真实 ``_resume_row`` + 真实 loopback HTTP gateway + 真实
``core.outbound``/``reconcile_run``,只把 QQ 平台边界(``UniMessage.send``)换成
进程内替身。

覆盖:
  - 控制组:首次 ready 交付照旧补发一次并落 terminal(不受本次收紧影响);
  - 四种业务终态(silent／suppressed／cancelled／expired)上,恢复链零平台调用、
    交付台账与组件逐字段不变(不再把 pending 计划当补发依据);
  - 真实在途交错:恢复已进入 status HTTP 等待,正常终态链先 finish 删 active 行
    并结算 suppressed/bounce,放行后恢复仍零发送;
  - failed 显式 rearm(恢复链唯一合法重臂入口)仍然补发一次。
"""

from __future__ import annotations

import asyncio
import json
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_restart_route_recovery import (
    RestartRouteHarness,
    _GatewayHandler,
    run_resume,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import prepare_terminal_batch


class ResumeBusinessTerminalTests(RestartRouteHarness):
    def _land_terminal_delivery(self, run_id: str, state: str) -> dict:
        """落一条「run 已按业务终态销账、计划里仍有 pending 份」的真实现场。"""
        self._bind(
            run_id=run_id, scope="onebotv11:group:12345", is_private=False,
            user_id="10001", group_id="12345",
        )
        self.registry.upsert(
            adapter="onebotv11", scope="group", scope_id="12345", bot_self_id="999",
            target=SimpleNamespace(private=False, id="12345"), ts=0,
        )
        output = _GatewayHandler.payload["output"]
        self.tracker.record_expected_delivery(
            run_id=run_id, output=output, expect_structured=True,
        )
        prepare_terminal_batch(
            tracker=self.tracker, run_id=run_id,
            units=run_resume._build_units(output, True),
        )
        row = self.tracker.get_by_run_id(run_id)
        if state != "ready":
            # 与真实链同序:交付计划先落账,业务结算随后把交付销成终态。
            run_resume._settle(row, state, "resume-terminal-regression")
        return row

    async def _assert_no_send(self, state: str) -> None:
        run_id = f"run-{state}"
        row = self._land_terminal_delivery(run_id, state)
        delivery_before = self.tracker.get_delivery(run_id)
        component_before = self._component(run_id)
        self.assertEqual(delivery_before["expected_state"], state)
        self.assertEqual(component_before["state"], "pending")
        self.assertIsNotNone(self.tracker.get_by_run_id(run_id))

        await run_resume._resume_row(row)

        self.assertEqual(self.platform_calls, [], state)
        self.assertEqual(self.sent_texts, [], state)
        # 交付台账与组件逐字段不变:终态不因重复落账被改写,也不被恢复链推进。
        self.assertEqual(self.tracker.get_delivery(run_id), delivery_before, state)
        self.assertEqual(self._component(run_id), component_before, state)
        # 已销账的 run 不再留 active 行僵尸,避免每次启动重演。
        self.assertIsNone(self.tracker.get_by_run_id(run_id), state)

    async def test_first_ready_resume_sends_once(self) -> None:
        """控制组:首次 ready 正常补发一次,不因终态闸误伤。"""
        run_id = "run-ready"
        row = self._land_terminal_delivery(run_id, "ready")
        self.assertEqual(self.tracker.get_delivery(run_id)["expected_state"], "ready")

        await run_resume._resume_row(row)

        self.assertEqual(len(self.platform_calls), 1)
        self.assertEqual(self.sent_texts, ["重启恢复正文"])
        self.assertEqual(self._component(run_id)["state"], "sent")
        self.assertEqual(self.tracker.get_delivery(run_id)["expected_state"], "terminal")

    async def test_silent_terminal_resume_sends_nothing(self) -> None:
        await self._assert_no_send("silent")

    async def test_suppressed_terminal_resume_sends_nothing(self) -> None:
        await self._assert_no_send("suppressed")

    async def test_cancelled_terminal_resume_sends_nothing(self) -> None:
        await self._assert_no_send("cancelled")

    async def test_expired_terminal_resume_sends_nothing(self) -> None:
        await self._assert_no_send("expired")

    async def test_failed_rearm_resume_still_sends(self) -> None:
        """failed 是恢复链显式 rearm 的唯一合法重臂对象,仍须补发。"""
        run_id = "run-failed"
        row = self._land_terminal_delivery(run_id, "failed")
        self.assertEqual(self.tracker.get_delivery(run_id)["expected_state"], "failed")

        await run_resume._resume_row(row)

        self.assertEqual(len(self.platform_calls), 1)
        self.assertEqual(self.sent_texts, ["重启恢复正文"])
        self.assertEqual(self._component(run_id)["state"], "sent")

    async def test_inflight_normal_finish_terminal_wins_over_recovery(self) -> None:
        """恢复已在 status HTTP 在途,正常终态链先销账 → 放行后仍零发送。"""
        run_id = "run-inflight-suppressed"
        row = self._land_terminal_delivery(run_id, "ready")
        entered, release = threading.Event(), threading.Event()
        original = _GatewayHandler.do_GET

        def gated_http(handler):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("regression harness HTTP release timeout")
            original(handler)

        with patch.object(_GatewayHandler, "do_GET", gated_http):
            task = asyncio.create_task(run_resume._resume_row(row))
            try:
                self.assertTrue(
                    await self._wait_until(entered.is_set),
                    "恢复未到达真实 status HTTP 请求",
                )
                # 真实正常链:先 finish 删 active 行,再按平台回执结算 suppressed。
                self.tracker.finish(run_id)
                run_resume._settle(row, "suppressed", "bounce")
                delivery_before = self.tracker.get_delivery(run_id)
                component_before = self._component(run_id)
                self.assertEqual(delivery_before["expected_state"], "suppressed")
                self.assertIsNone(self.tracker.get_by_run_id(run_id))
                release.set()
                await asyncio.wait_for(task, 3)
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self.tracker.get_delivery(run_id), delivery_before)
        self.assertEqual(self._component(run_id), component_before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
