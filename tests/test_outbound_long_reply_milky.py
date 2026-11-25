"""QQ-07 实链复核后的安全收口：Milky 长回复禁用自定义合并转发。

Milky 自定义节点缺少正常客户端转发所带的消息引用，真实 QQ 会把这种包判成
非法合并转发。Milky 群聊与私聊都走截断；OneBot 既有分支保持原样。
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
from tests._qqtpc_fixtures import MilkyHarness  # noqa: E402

import nonebot_plugin_alconna as alconna  # noqa: E402
from nonebot_plugin_hermes_adapter.core import outbound as ob  # noqa: E402

GROUP = "462540549"
USER = "10001"
LONG_TEXT = "长回复" * 120  # 240 字，远超 max_length=40


class LongReplyForwardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patches = [
            patch.object(ob, "load_misc_config", lambda: SimpleNamespace(max_length=40)),
            patch.object(
                ob, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=True)
            ),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)

    async def test_milky_group_long_reply_truncates_without_custom_forward(self) -> None:
        harness = MilkyHarness()
        harness.replies["send_group_message"] = {"message_seq": 99, "time": 1}

        result = await ob.send_text_with_media(
            bot=harness.bot,
            target=alconna.Target(GROUP, adapter="milky"),
            text=LONG_TEXT,
            adapter_name="milky",
        )

        self.assertTrue(result.ok)
        self.assertEqual(harness.actions(), ["send_group_message"])
        elements = harness.params_for("send_group_message")[0]["message"]
        self.assertFalse([e for e in elements if e.get("type") == "forward"])
        text = "".join(
            str(e.get("data", {}).get("text", "")) for e in elements if e.get("type") == "text"
        )
        self.assertIn("已截断", text)

    async def test_milky_private_long_reply_still_truncates(self) -> None:
        harness = MilkyHarness()
        harness.replies["send_private_message"] = {"message_seq": 100, "time": 1}

        result = await ob.send_text_with_media(
            bot=harness.bot,
            target=alconna.Target(USER, private=True, adapter="milky"),
            text=LONG_TEXT,
            adapter_name="milky",
        )

        self.assertTrue(result.ok)
        self.assertEqual(harness.actions(), ["send_private_message"])
        elements = harness.params_for("send_private_message")[0]["message"]
        self.assertFalse([e for e in elements if e.get("type") == "forward"])
        text = "".join(
            str(e.get("data", {}).get("text", "")) for e in elements if e.get("type") == "text"
        )
        self.assertIn("已截断", text)

    async def test_onebot_group_gate_still_uses_forward(self) -> None:
        bot = SimpleNamespace(self_id="999")
        sender = AsyncMock(return_value=["1"])

        with patch.object(ob, "_send_unimsg", sender):
            result = await ob.send_text_with_media(
                bot=bot,
                target=alconna.Target(GROUP, adapter="onebotv11"),
                text=LONG_TEXT,
                adapter_name="onebotv11",
            )

        self.assertTrue(result.ok)
        sender.assert_awaited_once()
        sent_msg = sender.await_args.args[0]
        self.assertTrue(sent_msg.has(alconna.Reference), "OneBot 群长回复仍走合并转发")

    async def test_forward_disabled_keeps_truncation_on_milky(self) -> None:
        harness = MilkyHarness()
        harness.replies["send_group_message"] = {"message_seq": 101, "time": 1}

        with patch.object(
            ob, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=False)
        ):
            result = await ob.send_text_with_media(
                bot=harness.bot,
                target=alconna.Target(GROUP, adapter="milky"),
                text=LONG_TEXT,
                adapter_name="milky",
            )

        self.assertTrue(result.ok)
        elements = harness.params_for("send_group_message")[0]["message"]
        self.assertFalse([e for e in elements if e.get("type") == "forward"])
        text = "".join(
            str(e.get("data", {}).get("text", "")) for e in elements if e.get("type") == "text"
        )
        self.assertIn("已截断", text)


if __name__ == "__main__":
    unittest.main()
