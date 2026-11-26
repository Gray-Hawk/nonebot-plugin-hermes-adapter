"""QQTPC 审批通知：管理员身份按会话域匹配，发送原语跟随真实在线 adapter。

审批通知是插件唯一不经 BotRegistry 的私聊出站：能不能发由能力位决定、
怎么发由当前真实 bot 的协议决定。QQTPC 内协议切换后，配置在旧 transport 下的
管理员必须在当前驱动上收到通知；官方 QQ 仍零平台调用。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests._qqtpc_fixtures import MilkyHarness, onebot_bot  # noqa: E402

from nonebot_plugin_hermes_adapter.core import approvals  # noqa: E402


def _entry(adapter: str = "onebotv11") -> approvals.PendingApproval:
    return approvals.PendingApproval(
        approval_id=9,
        run_id="run-9",
        adapter=adapter,
        scope_desc="群 462540549",
        command="rm -rf /tmp/x",
        choices=["once", "deny"],
    )


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def call_api(self, action, **kwargs):
        self.calls.append((action, kwargs))


class ApprovalIdentityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        approvals._pending.clear()
        self.addCleanup(approvals._pending.clear)

    async def test_admin_from_onebot_entry_is_sent_via_online_milky(self) -> None:
        harness = MilkyHarness()
        harness.replies["send_private_message"] = {"message_seq": 11, "time": 1}

        sent = await approvals.notify_admins(
            bot=harness.bot,
            adapter_name="onebotv11",  # 台账/会话身份来自旧 transport
            entry=_entry("onebotv11"),
            admin_users={"onebotv11:10001"},
        )

        self.assertEqual(sent, 1)
        self.assertEqual(harness.actions(), ["send_private_message"],
                         "真实在线驱动是 Milky，发送原语必须跟随它")
        self.assertEqual(harness.params_for("send_private_message")[0]["user_id"], 10001)

    async def test_onebot_bot_still_uses_legacy_primitive(self) -> None:
        bot, socket = onebot_bot({"status": "ok", "retcode": 0, "data": {"message_id": 5}})

        sent = await approvals.notify_admins(
            bot=bot, adapter_name="milky", entry=_entry("milky"),
            admin_users={"milky:10002"},
        )

        self.assertEqual(sent, 1)
        frames = [f for f in socket.frames if f.get("action") == "send_private_msg"]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["params"]["user_id"], 10002)

    async def test_renotify_delivers_all_qqtpc_admins_through_live_driver(self) -> None:
        harness = MilkyHarness()
        harness.replies["send_private_message"] = {"message_seq": 12, "time": 1}
        approvals._pending[9] = _entry("onebotv11")

        with patch("nonebot.get_bots", lambda: {"3889000001": harness.bot}):
            sent = await approvals.renotify_pending(
                admin_users={"onebotv11:10001", "qqtpc:10002"}, delay_sec=0.0,
            )

        self.assertEqual(sent, 2, "同域条目都由当前在线 Milky 送达")
        self.assertEqual(harness.actions(), ["send_private_message", "send_private_message"])
        self.assertEqual(
            sorted(p["user_id"] for p in harness.params_for("send_private_message")),
            [10001, 10002],
        )
        self.assertIn(9, approvals._pending)

    async def test_official_qq_admin_is_not_notified_without_capability(self) -> None:
        bot = _Recorder()
        sent = await approvals.notify_admins(
            bot=bot, adapter_name="qq", entry=_entry("qq"),
            admin_users={"qq:OPENID_A"},
        )
        self.assertEqual(sent, 0)
        self.assertEqual(bot.calls, [])

    async def test_qqtpc_entry_does_not_leak_to_official_qq_admin(self) -> None:
        bot = SimpleNamespace(
            self_id="qq-1",
            adapter=SimpleNamespace(get_name=lambda: "QQ"),
        )
        approvals._pending[9] = _entry("qq")
        with patch("nonebot.get_bots", lambda: {"qq-1": bot}):
            sent = await approvals.renotify_pending(
                admin_users={"onebotv11:10001"}, delay_sec=0.0,
            )
        self.assertEqual(sent, 0)
        self.assertIn(9, approvals._pending)


if __name__ == "__main__":
    unittest.main()
