"""QQ-05：Milky emoji 回执走适配器原生 ``send_group_message_reaction``。

能力位与真实协议必须一致：Milky 群聊贴表情是 ``send_group_message_reaction``
（``message_seq`` 是 P0-1 落库的十进制外部 ID），不存在 ``set_msg_emoji_like``；
私聊没有群消息 reaction 语义，必须本地拒绝；官方 QQ 能力位为假，零平台调用。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests._qqtpc_fixtures import MilkyHarness  # noqa: E402

from nonebot_plugin_hermes_adapter.handlers import message_feedback as fb  # noqa: E402

GROUP = "462540549"
EMOJI = 424


class _RecorderBot:
    self_id = "999"

    def __init__(self, data=None, *, adapter_name: str = "QQ") -> None:
        self.data = data if data is not None else {"result": 0}
        self.calls: list[tuple[str, dict]] = []
        # 真实 bot 的 adapter 名是能力位的唯一来源(官方 QQ 归一成 "qq")。
        self.adapter = SimpleNamespace(get_name=lambda: adapter_name)

    async def call_api(self, action, **kwargs):
        self.calls.append((action, kwargs))
        return self.data


class MilkyEmojiReceiptTests(unittest.IsolatedAsyncioTestCase):
    async def test_milky_group_uses_native_reaction(self) -> None:
        harness = MilkyHarness()

        ok = await fb._emoji_like_quiet(harness.bot, "23097", EMOJI, group_id=GROUP)

        self.assertTrue(ok)
        self.assertEqual(harness.actions(), ["send_group_message_reaction"])
        params = harness.params_for("send_group_message_reaction")[0]
        self.assertEqual(params["group_id"], int(GROUP))
        self.assertEqual(params["message_seq"], 23097)
        self.assertEqual(params["reaction"], str(EMOJI))
        self.assertEqual(params["reaction_type"], "face")

    async def test_milky_private_has_no_reaction_and_makes_no_call(self) -> None:
        harness = MilkyHarness()

        ok = await fb._emoji_like_quiet(harness.bot, "30001", EMOJI, group_id=None)

        self.assertFalse(ok)
        self.assertEqual(harness.actions(), [])

    async def test_milky_platform_rejection_is_reported_false(self) -> None:
        harness = MilkyHarness()
        harness.failures["send_group_message_reaction"] = {
            "status": "failed", "retcode": 1400, "message": "no such message",
        }

        ok = await fb._emoji_like_quiet(harness.bot, "23097", EMOJI, group_id=GROUP)

        self.assertFalse(ok)

    async def test_official_qq_capability_off_makes_no_call(self) -> None:
        bot = _RecorderBot()

        ok = await fb._emoji_like_quiet(bot, "EVT_1", EMOJI, group_id="GROUP_OPENID")

        self.assertFalse(ok)
        self.assertEqual(bot.calls, [])

    async def test_onebot_path_is_unchanged(self) -> None:
        bot = _RecorderBot({"result": 0, "errMsg": ""}, adapter_name="OneBot V11")

        ok = await fb._emoji_like_quiet(bot, 12345, EMOJI)

        self.assertTrue(ok)
        self.assertEqual(bot.calls[0][0], "set_msg_emoji_like")
        self.assertEqual(bot.calls[0][1], {"message_id": 12345, "emoji_id": EMOJI})


if __name__ == "__main__":
    unittest.main()
