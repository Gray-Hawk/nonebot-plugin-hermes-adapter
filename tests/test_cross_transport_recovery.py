"""QQ-04：QQTPC 域内跨协议恢复（OneBot 时代落的交付台账 → 当前 Milky 连接补投）。

真实 SQLite 台账 + 真实 BotRegistry + 真实恢复/对账/出站链（沿用
``test_restart_route_recovery`` 的夹具），只把平台边界换成进程内替身。
覆盖：
  - ready 交付：旧 adapter=onebotv11、当前只有 Milky 在线 → 用当前驱动补投一次；
  - 在飞 run：停机期间完成 → 唤醒 reattach 并经当前驱动送达；
  - 官方 QQ 域（adapter=qq）不得被 QQTPC 同域规则命中；
  - 旧驱动仍在线（双驱动）时不得跨驱动乱投（歧义按等待）。
"""

from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests.test_restart_route_recovery import (  # noqa: E402
    BOT_ID,
    RestartRouteHarness,
    _GatewayHandler,
    framed,
    run_resume,
)

from nonebot_plugin_hermes_adapter.core import session as session_mod  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    prepare_terminal_batch,
)

GROUP = "g1"


class CrossTransportRecoveryTests(RestartRouteHarness):
    def _milky_bot(self):
        return type(
            "MilkyBotLike",
            (),
            {
                "self_id": BOT_ID,
                "adapter": type("A", (), {"get_name": staticmethod(lambda: "Milky")})(),
            },
        )()

    def _bind_with_adapter(
        self, *, run_id: str, adapter: str, group_id: str = GROUP
    ) -> None:
        """与 harness._bind 同序，只把台账里的历史 adapter 显式写出来。"""
        self.tracker.upsert(
            run_id=run_id, scope_key=f"{adapter}:group:{group_id}", adapter=adapter,
            is_private=False, user_id="u1", group_id=group_id, bot_self_id=BOT_ID,
            session_key="s", mode="reactive", expect_structured=True,
        )
        self.tracker.record_submission(
            idempotency_key=f"idem-{run_id}", request_body={}, target_fingerprint="x|y",
            scope_key=f"{adapter}:group:{group_id}", session_key="s", mode="reactive",
            expect_structured=True, bot_self_id=BOT_ID,
        )
        self.tracker.bind_submission_and_register(
            idempotency_key=f"idem-{run_id}", run_id=run_id,
            scope_key=f"{adapter}:group:{group_id}", adapter=adapter, is_private=False,
            user_id="u1", group_id=group_id, bot_self_id=BOT_ID, session_key="s",
            mode="reactive", expect_structured=True,
        )

    def _land_ready_with_adapter(
        self, *, run_id: str, adapter: str, body: str = "跨协议补投正文"
    ) -> None:
        self._bind_with_adapter(run_id=run_id, adapter=adapter)
        self.tracker.record_expected_delivery(
            run_id=run_id, output=framed(body), expect_structured=True,
        )
        prepare_terminal_batch(
            tracker=self.tracker, run_id=run_id,
            units=[{"body": body, "media_urls": [], "file_paths": [], "reply_to": ""}],
        )
        self.tracker.finish(run_id)

    async def test_ready_delivery_switches_to_current_milky_bot(self) -> None:
        self._land_ready_with_adapter(run_id="run-switch", adapter="onebotv11")

        with patch.object(session_mod, "_live_transports", lambda domain: ["milky"]):
            run_resume.recover_bot_deliveries(self._milky_bot())

            self.assertTrue(
                await self._wait_until(
                    lambda: self._component("run-switch")["state"] == "sent"
                )
            )

        self.assertEqual(self.platform_calls, [("g1", False)])
        self.assertEqual(self.sent_texts, ["跨协议补投正文"])
        entry = self.registry.get("milky", "group", GROUP)
        self.assertIsNotNone(entry, "路由按当前连接驱动重建")
        self.assertEqual(entry.bot_self_id, BOT_ID)
        self.assertIsNone(
            self.registry.get("onebotv11", "group", GROUP),
            "旧驱动没有在线连接，不得再为它合成路由",
        )
        appended = [call.args[0] for call in self.message_buffer.append.call_args_list]
        self.assertTrue(appended)
        self.assertTrue(all(item.adapter == "milky" for item in appended), "出站历史记当前驱动")
        self.assertEqual(
            self.tracker.get_delivery("run-switch")["expected_state"], "terminal"
        )

    async def test_running_run_reattaches_through_current_driver(self) -> None:
        self._bind_with_adapter(run_id="run-active", adapter="onebotv11")
        _GatewayHandler.payload = {
            "status": "completed",
            "output": framed("停机期间完成的跨协议回复"),
            "updated_at": time.time(),
        }

        with patch.object(session_mod, "_live_transports", lambda domain: ["milky"]):
            run_resume.recover_bot_deliveries(self._milky_bot())

            self.assertTrue(
                await self._wait_until(
                    lambda: self.tracker.get_by_run_id("run-active") is None
                )
            )

        self.assertEqual(self.platform_calls, [("g1", False)])
        self.assertEqual(self.sent_texts, ["停机期间完成的跨协议回复"])
        self.assertEqual(self._component("run-active")["state"], "sent")
        appended = [call.args[0] for call in self.message_buffer.append.call_args_list]
        self.assertTrue(all(item.adapter == "milky" for item in appended))

    async def test_current_driver_inflight_blocks_cross_transport_resume(self) -> None:
        self._bind_with_adapter(run_id="run-current-inflight", adapter="onebotv11")
        inflight = run_resume._mcp.inflight
        inflight.is_inflight.side_effect = lambda key: key[0] == "milky"

        with patch.object(session_mod, "_live_transports", lambda domain: ["milky"]), patch.object(
            run_resume, "_RESUME_RETRY_MAX", 1
        ), patch.object(run_resume, "_RESUME_RETRY_SEC", 0):
            run_resume.recover_bot_deliveries(self._milky_bot())
            await self._wait_until(lambda: False, timeout=0.2)

        self.assertEqual(self.sent_texts, [], "当前 Milky 仍在跑时必须等待，不能插入旧回复")
        self.assertIsNotNone(self.tracker.get_by_run_id("run-current-inflight"))

    async def test_official_qq_row_is_not_resumed_by_qqtpc_bot(self) -> None:
        self._land_ready_with_adapter(run_id="run-qq", adapter="qq")

        with patch.object(session_mod, "_live_transports", lambda domain: ["milky"]):
            run_resume.recover_bot_deliveries(self._milky_bot())
            await self._wait_until(lambda: False, timeout=0.2)

        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-qq")["state"], "pending")
        self.assertEqual(self.registry.known(), [])

    async def test_old_driver_still_live_keeps_row_for_its_own_bot(self) -> None:
        self._land_ready_with_adapter(run_id="run-dual", adapter="onebotv11")

        with patch.object(
            session_mod, "_live_transports", lambda domain: ["onebotv11", "milky"]
        ):
            run_resume.recover_bot_deliveries(self._milky_bot())
            await self._wait_until(lambda: False, timeout=0.2)

        self.assertEqual(self.platform_calls, [], "双驱动在线时按歧义等待，不跨驱动乱投")
        self.assertEqual(self._component("run-dual")["state"], "pending")
        self.assertEqual(self.registry.known(), [])


if __name__ == "__main__":
    unittest.main()
