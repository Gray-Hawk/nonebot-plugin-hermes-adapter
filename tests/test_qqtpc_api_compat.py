"""QQ Third-Party Client（QQTPC）薄兼容层（``core/qqtpc_api``）的结构级测试。

每个方法至少覆盖 OneBot v11 与 Milky 两侧，且两侧都用**真实**适配器对象：
OneBot 用真 ``Bot.call_api`` → 只记录帧的 WebSocket；Milky 用真 ``Bot`` → 只回放
协议端 JSON 的 ``Adapter.request``。假对象只出现在「平台线形收发」这一层。
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

from tests import _qqtpc_fixtures as fx
from tests import _bootstrap  # noqa: F401

from nonebot.adapters.milky.bot import _check_at_me
from nonebot.adapters.milky.model.api import MessageResponse

from nonebot_plugin_hermes_adapter.core import qqtpc_api

GROUP = fx.GROUP


class _FailingBot:
    """缺 OneBot 撤回方法的 bot（Milky 真 Bot 的 ``__getattr__`` 同形态）。"""

    self_id = fx.ONEBOT_SELF


class ReceiptIdTests(unittest.TestCase):
    """P0-1：发送回执统一成干净的外部消息 ID 字符串。"""

    def test_milky_message_response_gives_decimal_message_seq(self) -> None:
        response = MessageResponse(message_seq=23097, time=1795538642)
        self.assertEqual(str(response), "message_seq=23097 time=1795538642")  # 基线脏形态
        self.assertEqual(qqtpc_api.extract_message_id(response), "23097")

    def test_milky_dict_payload_gives_message_seq(self) -> None:
        self.assertEqual(
            qqtpc_api.extract_message_id({"message_seq": 23097, "time": 1795538642}),
            "23097",
        )

    def test_onebot_dict_payload_gives_message_id(self) -> None:
        self.assertEqual(
            qqtpc_api.extract_message_id(
                {"message_id": 7180000001, "message_id_v2": "x"}
            ),
            "7180000001",
        )

    def test_scalars_pass_through_and_empty_values_are_none(self) -> None:
        self.assertEqual(qqtpc_api.extract_message_id(12345), "12345")
        self.assertEqual(qqtpc_api.extract_message_id(" 67890 "), "67890")
        self.assertIsNone(qqtpc_api.extract_message_id(None))
        self.assertIsNone(qqtpc_api.extract_message_id({}))

    def test_receipt_ids_normalizes_both_protocols(self) -> None:
        receipt = SimpleNamespace(
            msg_ids=[
                {"message_id": 7180000001},
                MessageResponse(message_seq=23097, time=1795538642),
                None,
                {},
            ]
        )
        self.assertEqual(
            qqtpc_api.receipt_msg_ids(receipt), ["7180000001", "23097"]
        )


class MentionTests(unittest.TestCase):
    """P0-2：两种协议的 mention 形状都要能识别。"""

    def test_milky_mention_shape_is_detected(self) -> None:
        event = fx.milky_group_message(segments=[fx.mention(int(fx.MILKY_SELF))])
        self.assertTrue(qqtpc_api.has_at_bot_in_original(event, fx.MILKY_SELF))
        self.assertFalse(qqtpc_api.has_at_bot_in_original(event, "10002"))

    def test_milky_mention_survives_adapter_strip(self) -> None:
        event = fx.milky_group_message(segments=[fx.mention(int(fx.MILKY_SELF))])
        _check_at_me(_milky_bot(), event)
        self.assertEqual(event.get_plaintext().strip(), "")  # 正文已被剥空
        self.assertTrue(qqtpc_api.has_at_bot_in_original(event, fx.MILKY_SELF))

    def test_onebot_at_shape_is_detected(self) -> None:
        event = fx.milky_group_message(segments=[fx.text("x")])
        # OneBot v11 的段形状：type=at / data.qq（这里只验解析口径）
        seg = SimpleNamespace(type="at", data={"qq": fx.ONEBOT_SELF})
        self.assertEqual(qqtpc_api.at_target_of(seg), fx.ONEBOT_SELF)
        self.assertIsNone(qqtpc_api.at_target_of(SimpleNamespace(type="face", data={})))

    def test_events_without_original_message_are_safe(self) -> None:
        self.assertFalse(
            qqtpc_api.has_at_bot_in_original(SimpleNamespace(), fx.MILKY_SELF)
        )


_MILKY_BOT = None


def _milky_bot():
    global _MILKY_BOT
    if _MILKY_BOT is None:
        _MILKY_BOT = fx.MilkyHarness().bot
    return _MILKY_BOT


class SenderInfoTests(unittest.TestCase):
    """P0-3：发送者资料统一（群名片优先、私聊昵称、空值回退由调用方处理）。"""

    def test_milky_group_member_prefers_card_then_nickname(self) -> None:
        event = fx.milky_group_message(segments=[fx.text("hi")], card="群名片", nickname="昵称")
        self.assertEqual(qqtpc_api.sender_display_name(event), "群名片")
        blank = fx.milky_group_message(segments=[fx.text("hi")], card="", nickname="昵称")
        self.assertEqual(qqtpc_api.sender_display_name(blank), "昵称")

    def test_milky_private_reads_nickname(self) -> None:
        event = fx.milky_friend_message(segments=[fx.text("hi")], nickname="好友昵称")
        self.assertEqual(qqtpc_api.sender_display_name(event), "好友昵称")

    def test_milky_blank_profile_returns_none(self) -> None:
        event = fx.milky_group_message(segments=[fx.text("hi")], card="  ", nickname="")
        self.assertIsNone(qqtpc_api.sender_display_name(event))

    def test_onebot_sender_card_then_nickname(self) -> None:
        event = SimpleNamespace(sender=SimpleNamespace(card="名片", nickname="昵称"))
        self.assertEqual(qqtpc_api.sender_display_name(event), "名片")
        event = SimpleNamespace(sender=SimpleNamespace(card=None, nickname="昵称"))
        self.assertEqual(qqtpc_api.sender_display_name(event), "昵称")
        self.assertIsNone(
            qqtpc_api.sender_display_name(SimpleNamespace(sender=None, data=None))
        )


class PokeTests(unittest.TestCase):
    """P0-4 出站：OneBot 走 send_poke，Milky 走 nudge。"""

    def test_onebot_group_and_private_use_send_poke(self) -> None:
        bot, socket = fx.onebot_bot({"status": "ok", "retcode": 0, "data": None})
        asyncio.run(
            qqtpc_api.send_poke(bot, adapter_name="onebotv11", group_id=GROUP, user_id="10001")
        )
        asyncio.run(
            qqtpc_api.send_poke(bot, adapter_name="onebotv11", group_id=None, user_id="10001")
        )
        self.assertEqual([f["action"] for f in socket.frames], ["send_poke", "send_poke"])
        self.assertEqual(
            socket.frames[0]["params"], {"group_id": int(GROUP), "user_id": 10001}
        )
        self.assertEqual(socket.frames[1]["params"], {"user_id": 10001})

    def test_milky_group_uses_group_nudge(self) -> None:
        harness = fx.MilkyHarness()
        asyncio.run(
            qqtpc_api.send_poke(
                harness.bot, adapter_name="milky", group_id=GROUP, user_id="10001"
            )
        )
        self.assertEqual(
            harness.params_for("send_group_nudge"),
            [{"group_id": int(GROUP), "user_id": 10001}],
        )

    def test_milky_private_uses_friend_nudge_with_is_self_false(self) -> None:
        harness = fx.MilkyHarness()
        asyncio.run(
            qqtpc_api.send_poke(
                harness.bot, adapter_name="milky", group_id=None, user_id="10001"
            )
        )
        self.assertEqual(
            harness.params_for("send_friend_nudge"),
            [{"user_id": 10001, "is_self": False}],
        )


class RecallTests(unittest.TestCase):
    """P0-5 出站：撤回统一入口，参数按现场签名。"""

    def test_milky_group_recall_uses_group_id_and_message_seq(self) -> None:
        harness = fx.MilkyHarness()
        asyncio.run(
            qqtpc_api.recall_message(
                harness.bot,
                adapter_name="milky",
                group_id=GROUP,
                user_id=None,
                message_id="23097",
            )
        )
        self.assertEqual(
            harness.params_for("recall_group_message"),
            [{"group_id": int(GROUP), "message_seq": 23097}],
        )

    def test_milky_private_recall_uses_user_id_and_message_seq(self) -> None:
        harness = fx.MilkyHarness()
        asyncio.run(
            qqtpc_api.recall_message(
                harness.bot,
                adapter_name="milky",
                group_id=None,
                user_id="10001",
                message_id="30101",
            )
        )
        self.assertEqual(
            harness.params_for("recall_private_message"),
            [{"user_id": 10001, "message_seq": 30101}],
        )

    def test_onebot_recall_uses_delete_msg(self) -> None:
        bot, socket = fx.onebot_bot({"status": "ok", "retcode": 0, "data": None})
        asyncio.run(
            qqtpc_api.recall_message(
                bot,
                adapter_name="onebotv11",
                group_id=GROUP,
                user_id=None,
                message_id="7180000001",
            )
        )
        self.assertEqual(socket.frames[0]["action"], "delete_msg")
        self.assertEqual(socket.frames[0]["params"], {"message_id": 7180000001})

    def test_missing_recall_api_is_an_explicit_unsupported_error(self) -> None:
        with self.assertRaises(qqtpc_api.UnsupportedOperation):
            asyncio.run(
                qqtpc_api.recall_message(
                    _FailingBot(),
                    adapter_name="onebotv11",
                    group_id=GROUP,
                    user_id=None,
                    message_id="1",
                )
            )


class NicknameResolveTests(unittest.TestCase):
    """P0-4 入站 poke 需要昵称：按协议选对成员资料 API。"""

    def test_milky_group_member_prefers_card(self) -> None:
        harness = fx.MilkyHarness()
        harness.replies["get_group_member_info"] = {
            "member": {
                "user_id": 10001,
                "nickname": "昵称",
                "card": "群名片",
                "sex": "unknown",
                "group_id": int(GROUP),
                "title": "",
                "level": 1,
                "role": "member",
                "join_time": 1,
                "last_sent_time": 2,
            }
        }
        name = asyncio.run(
            qqtpc_api.resolve_display_name(
                harness.bot, adapter_name="milky", user_id="10001", group_id=GROUP
            )
        )
        self.assertEqual(name, "群名片")
        self.assertEqual(
            harness.params_for("get_group_member_info"),
            [{"group_id": int(GROUP), "user_id": 10001, "no_cache": False}],
        )

    def test_milky_private_uses_friend_info(self) -> None:
        harness = fx.MilkyHarness()
        harness.replies["get_friend_info"] = {
            "friend": {
                "user_id": 10001,
                "nickname": "好友昵称",
                "sex": "unknown",
                "qid": "",
                "remark": "",
                "category": {"category_id": 0, "category_name": "默认"},
            }
        }
        name = asyncio.run(
            qqtpc_api.resolve_display_name(
                harness.bot, adapter_name="milky", user_id="10001"
            )
        )
        self.assertEqual(name, "好友昵称")
        self.assertEqual(
            harness.params_for("get_friend_info"), [{"user_id": 10001, "no_cache": False}]
        )

    def test_onebot_group_member_still_uses_get_group_member_info(self) -> None:
        bot, socket = fx.onebot_bot(
            {
                "status": "ok",
                "retcode": 0,
                "data": {"user_id": 10001, "nickname": "昵称", "card": "群名片"},
            }
        )
        name = asyncio.run(
            qqtpc_api.resolve_display_name(
                bot, adapter_name="onebotv11", user_id="10001", group_id=GROUP
            )
        )
        self.assertEqual(name, "群名片")
        self.assertEqual(socket.frames[0]["action"], "get_group_member_info")
        self.assertEqual(
            socket.frames[0]["params"], {"group_id": int(GROUP), "user_id": 10001}
        )


class ProtocolTagTests(unittest.TestCase):
    def test_protocol_tag_is_normalized_like_the_router(self) -> None:
        self.assertEqual(qqtpc_api.protocol("Milky"), qqtpc_api.MILKY)
        self.assertEqual(qqtpc_api.protocol("onebot.v11"), qqtpc_api.ONEBOT)
        self.assertEqual(qqtpc_api.protocol("  "), "")


if __name__ == "__main__":
    unittest.main()
