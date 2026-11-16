"""出站戳一戳：正文内联 ``[CQ:poke,type=1,id=<QQ>]`` 走 OneBot 公开 API。

NapCat 群聊实测拒绝原生 poke 消息段（retcode=1400，仅允许私聊），
`send_poke(group_id, user_id)` 群聊 retcode=0。本文件锁定：

- 群聊/私聊都走 ``send_poke`` 公开 API，绝不再拼原生 poke 消息段；
- 混合文本、多个 poke、@ 与媒体保持动作顺序（消息 / 戳 / 消息……）；
- 参数非法（非数字/顺序不符/缺 id）、0、数字身份自戳仍按文本处理，原样留下；
- 整批只认领一次交付组件：回执汇总回写，中途失败整体记 unknown，不把部分失败
  当成功、也不重复发送同一组件。
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests import _bootstrap  # noqa: F401

import nonebot_plugin_alconna as alconna  # noqa: E402

from nonebot_plugin_hermes_adapter.core import outbound  # noqa: E402

SELF_ID = "999"


def _text_parts(dump: list[dict]) -> str:
    return "".join(part.get("text", "") for part in dump if part.get("type") == "text")


class _Harness:
    """记录平台调用顺序：消息发送与 send_poke 落在同一条时间线上。"""

    def __init__(self, *, self_id: str = SELF_ID, fail_poke: bool = False) -> None:
        self.calls: list[tuple] = []
        self._seq = 0
        self.fail_poke = fail_poke
        self.self_id = self_id
        self.bot = SimpleNamespace(self_id=self_id, call_api=self._call_api)

    async def _call_api(self, api: str, **kwargs):
        self._seq += 1
        if api == "send_poke" and self.fail_poke:
            raise RuntimeError("send_poke boom")
        self.calls.append(("poke", kwargs))
        return {"retcode": 0}

    async def _send(self, uni, *, bot=None, target=None, delivery=None):
        self._seq += 1
        self.calls.append(("msg", uni.dump()))
        return SimpleNamespace(msg_ids=[{"message_id": 1000 + self._seq}])

    async def _send_unimsg(self, uni, *, bot=None, target=None, delivery=None):
        self._seq += 1
        self.calls.append(("msg", uni.dump()))
        return [str(1000 + self._seq)]

    def send(self, text: str, *, private: bool = False, media_urls=(), delivery=None,
             adapter_name: str = "onebotv11"):
        target = SimpleNamespace(private=private, id=("12345" if private else "54321"))
        with patch.object(outbound, "_send_msg", new=self._send), patch.object(
            outbound, "_send_unimsg", new=self._send_unimsg
        ):
            result = asyncio.run(
                outbound.send_text_with_media(
                    bot=self.bot, target=target, text=text,
                    media_urls=list(media_urls), adapter_name=adapter_name,
                    delivery=delivery,
                )
            )
        return result


class GroupPokeTests(unittest.TestCase):
    def test_group_poke_uses_public_api_not_message_segment(self) -> None:
        h = _Harness()
        result = h.send("[CQ:poke,id=12345]")
        self.assertTrue(result.ok)
        self.assertEqual(h.calls, [("poke", {"group_id": 54321, "user_id": 12345})])
        # 没有消息被发出：poke 不再作为原生段塞进消息体。
        self.assertNotIn("msg", [kind for kind, _ in h.calls])

    def test_mixed_text_keeps_action_order(self) -> None:
        h = _Harness()
        result = h.send("先戳你 [CQ:poke,type=1,id=10001] 一下")
        self.assertTrue(result.ok)
        self.assertEqual([kind for kind, _ in h.calls], ["msg", "poke", "msg"])
        self.assertEqual(h.calls[0][1][0]["text"], "先戳你 ")
        self.assertEqual(h.calls[1][1], {"group_id": 54321, "user_id": 10001})
        self.assertEqual(h.calls[2][1][0]["text"], " 一下")
        self.assertEqual(len(result.msg_ids), 2)

    def test_multiple_pokes_keep_order_and_at_stays_in_message(self) -> None:
        h = _Harness()
        h.send("[CQ:at,qq=20002] 前 [CQ:poke,type=2,id=10001] 中 [CQ:poke,id=30003] 后")
        kinds = [kind for kind, _ in h.calls]
        self.assertEqual(kinds, ["msg", "poke", "msg", "poke", "msg"])
        self.assertEqual(h.calls[1][1]["user_id"], 10001)
        self.assertEqual(h.calls[3][1]["user_id"], 30003)
        first = h.calls[0][1]
        self.assertEqual(first[0]["type"], "at")
        self.assertEqual(_text_parts(first), " 前 ")

    def test_media_and_pokes_share_one_delivery_claim(self) -> None:
        tracker = MagicMock()
        tracker.claim_component_attempt.return_value = SimpleNamespace(
            outcome="send", token="t1"
        )
        delivery = outbound.DeliveryContext(tracker=tracker, run_id="r1")
        h = _Harness()
        result = h.send(
            "看图 [CQ:poke,id=12345]",
            media_urls=["https://example.invalid/pic.png"],
            delivery=delivery,
        )
        self.assertTrue(result.ok)
        tracker.claim_component_attempt.assert_called_once()
        sent = tracker.mark_component_sent.call_args.kwargs
        self.assertEqual(len(sent["msg_ids"]), 2)
        self.assertEqual(
            [kind for kind, _ in h.calls], ["msg", "poke", "msg"]
        )
        # 媒体在 poke 之后单独成条（动作顺序：文本 → 戳 → 图）。
        self.assertEqual([p["type"] for p in h.calls[2][1]], ["image"])
        self.assertEqual(h.calls[0][1][0]["text"], "看图 ")

    def test_partial_failure_is_recorded_unknown_not_success(self) -> None:
        tracker = MagicMock()
        tracker.claim_component_attempt.return_value = SimpleNamespace(
            outcome="send", token="t1"
        )
        delivery = outbound.DeliveryContext(tracker=tracker, run_id="r1")
        h = _Harness(fail_poke=True)
        result = h.send("先说一句 [CQ:poke,id=12345] 再补一句", delivery=delivery)
        self.assertFalse(result.ok)
        tracker.mark_component_sent.assert_not_called()
        unknown = tracker.mark_component_unknown.call_args.kwargs
        self.assertIn("send_poke boom", str(unknown["error"]))

    def test_already_sent_component_is_not_resent(self) -> None:
        tracker = MagicMock()
        tracker.claim_component_attempt.return_value = SimpleNamespace(
            outcome="sent", token=None
        )
        tracker.get_component.return_value = {"msg_ids": ["777"]}
        delivery = outbound.DeliveryContext(tracker=tracker, run_id="r1")
        h = _Harness()
        result = h.send("戳 [CQ:poke,id=12345]", delivery=delivery)
        self.assertTrue(result.ok)
        self.assertEqual(result.msg_ids, ["777"])
        self.assertEqual(h.calls, [])


class PrivatePokeTests(unittest.TestCase):
    def test_private_poke_uses_user_id_only(self) -> None:
        h = _Harness()
        result = h.send("[CQ:poke,id=12345]", private=True)
        self.assertTrue(result.ok)
        self.assertEqual(h.calls, [("poke", {"user_id": 12345})])

    def test_private_mixed_text_keeps_order(self) -> None:
        h = _Harness()
        h.send("戳你 [CQ:poke,id=12345] 一下", private=True)
        self.assertEqual([kind for kind, _ in h.calls], ["msg", "poke", "msg"])


class PokeArgumentGuardTests(unittest.TestCase):
    def test_invalid_params_stay_literal_text(self) -> None:
        for text in (
            "[CQ:poke,id=abc]",
            "[CQ:poke,type=x,id=10001]",
            "[CQ:poke,id=10001,type=1]",
            "[CQ:poke]",
            "[CQ:poke,id=]",
            "[CQ:poke,id=0]",
        ):
            with self.subTest(text=text):
                h = _Harness()
                result = h.send(text)
                self.assertTrue(result.ok)
                self.assertEqual([kind for kind, _ in h.calls], ["msg"])
                self.assertEqual(_text_parts(h.calls[0][1]), text)

    def test_self_poke_is_not_converted(self) -> None:
        for text in ("[CQ:poke,id=999]", "[CQ:poke,type=2,id=000999]"):
            with self.subTest(text=text):
                h = _Harness()
                h.send(text)
                self.assertEqual([kind for kind, _ in h.calls], ["msg"])
                self.assertEqual(_text_parts(h.calls[0][1]), text)

    def test_padded_other_target_still_converts(self) -> None:
        h = _Harness()
        h.send("[CQ:poke,id=000123]")
        self.assertEqual(h.calls, [("poke", {"group_id": 54321, "user_id": 123})])

    def test_at_regression_unchanged(self) -> None:
        h = _Harness()
        h.send("[CQ:at,qq=12345] 你看")
        self.assertEqual([kind for kind, _ in h.calls], ["msg"])
        ats = [p for p in h.calls[0][1] if p.get("type") == "at"]
        self.assertEqual(len(ats), 1)
        self.assertEqual(ats[0].get("target"), "12345")


class PokeForwardPackingTests(unittest.TestCase):
    def test_poke_is_not_packed_into_forward_nodes(self) -> None:
        tokens = outbound._parse_inline_markers("前文 [CQ:poke,type=1,id=10001] 后文")
        nodes = outbound._pack_forward_nodes(
            tokens, bot_self_id="999", bot_nickname="Bot", max_chars_per_node=2
        )
        contents = [seg for node in nodes for seg in node.content]
        self.assertFalse(
            [seg for seg in contents if isinstance(seg, alconna.Other)]
        )
        text = "".join(str(seg) for seg in contents if isinstance(seg, alconna.Text))
        self.assertNotIn("[CQ:poke", text)


if __name__ == "__main__":
    unittest.main()
