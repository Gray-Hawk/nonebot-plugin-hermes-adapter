"""QQ-01：审批私聊通知与重启重发支持 Milky（复用 QQTPC 发送引线，保留 OneBot）。

真实 Milky ``Bot``/``Adapter``（只替换线形 I/O）与真实 OneBot 帧替身；能力位
（``platform_compat.capabilities().approval_notify``）决定「能不能发」，协议引线
（``qqtpc_api``）决定「怎么发」。官方 QQ 没有私聊通知能力，必须零平台调用。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests._qqtpc_fixtures import MilkyHarness, onebot_bot  # noqa: E402

from nonebot_plugin_hermes_adapter.core import approvals  # noqa: E402


def _entry(adapter: str = "milky") -> approvals.PendingApproval:
    return approvals.PendingApproval(
        approval_id=7,
        run_id="run-7",
        adapter=adapter,
        scope_desc="群 462540549",
        command="rm -rf /tmp/x",
        choices=["once", "deny"],
    )


class _Recorder:
    """只记录平台调用的假 Bot（官方 QQ 的「零调用」反证用）。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def call_api(self, action, **kwargs):
        self.calls.append((action, kwargs))
        return {}


class ApprovalNotifyTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        approvals._pending.clear()
        self.addCleanup(approvals._pending.clear)

    async def test_milky_notify_uses_native_private_send(self) -> None:
        harness = MilkyHarness()
        harness.replies["send_private_message"] = {"message_seq": 11, "time": 1}

        sent = await approvals.notify_admins(
            bot=harness.bot,
            adapter_name="milky",
            entry=_entry(),
            admin_users={"milky:10001"},
        )

        self.assertEqual(sent, 1)
        self.assertEqual(harness.actions(), ["send_private_message"])
        params = harness.params_for("send_private_message")[0]
        self.assertEqual(params["user_id"], 10001)
        # Milky 消息在协议边界序列化成 elements 列表;按文本段还原后断言正文。
        elements = params["message"]
        rendered = "".join(
            str(seg.get("data", {}).get("text", ""))
            for seg in elements if isinstance(seg, dict)
        )
        self.assertIn("审批请求 #7", rendered)
        self.assertIn("approve once", rendered)

    async def test_onebot_notify_keeps_legacy_send_private_msg(self) -> None:
        bot, socket = onebot_bot({"status": "ok", "retcode": 0, "data": {"message_id": 5}})

        sent = await approvals.notify_admins(
            bot=bot,
            adapter_name="onebotv11",
            entry=_entry("onebotv11"),
            admin_users={"onebotv11:10001"},
        )

        self.assertEqual(sent, 1)
        frames = [f for f in socket.frames if f.get("action") == "send_private_msg"]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["params"]["user_id"], 10001)
        self.assertIn("审批请求 #7", frames[0]["params"]["message"])

    async def test_official_qq_capability_off_makes_no_platform_call(self) -> None:
        bot = _Recorder()

        sent = await approvals.notify_admins(
            bot=bot, adapter_name="qq", entry=_entry("qq"),
            admin_users={"qq:OPENID_A"},
        )

        self.assertEqual(sent, 0)
        self.assertEqual(bot.calls, [])

    async def test_renotify_notifies_same_domain_admins_through_live_driver(self) -> None:
        harness = MilkyHarness()
        harness.replies["send_private_message"] = {"message_seq": 12, "time": 1}
        approvals._pending[7] = _entry()

        with patch(
            "nonebot.get_bots",
            lambda: {"3889000001": harness.bot},
        ):
            sent = await approvals.renotify_pending(
                admin_users={"milky:10001", "onebotv11:10002"}, delay_sec=0.0,
            )

        self.assertEqual(sent, 2, "同属 QQTPC 域：两条管理员都经当前 Milky 在线驱动送达")
        self.assertEqual(harness.actions(), ["send_private_message", "send_private_message"])
        self.assertEqual(
            sorted(p["user_id"] for p in harness.params_for("send_private_message")),
            [10001, 10002],
        )
        self.assertIn(7, approvals._pending, "条目由 /approve 应答后才删除")

    async def test_renotify_without_any_capable_bot_keeps_entries(self) -> None:
        approvals._pending[7] = _entry()
        qq_bot = SimpleNamespace(
            self_id="qq-1",
            adapter=SimpleNamespace(get_name=lambda: "QQ"),
        )

        with patch("nonebot.get_bots", lambda: {"qq-1": qq_bot}):
            sent = await approvals.renotify_pending(
                admin_users={"qq:OPENID_A"}, delay_sec=0.0,
            )

        self.assertEqual(sent, 0)
        self.assertIn(7, approvals._pending)


if __name__ == "__main__":
    unittest.main()
