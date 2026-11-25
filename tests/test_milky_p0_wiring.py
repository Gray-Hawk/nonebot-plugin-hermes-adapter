"""Milky P0 五项修复的双协议接线回归（业务调用点）。

每个用例都用**真实** OneBot v11 / Milky 适配器类（只替换线形 I/O），断言的是
业务模块（出站发送器、入站解析、notice、MCP 工具）对两种协议的统一行为：

- P0-1 发送回执消息 ID：Milky 拿 ``message_seq`` 的十进制串，OneBot 仍拿 ``message_id``；
- P0-2 入站 @bot 与 @ 占位：纯 @bot 是有效唤醒输入，第三方 @ 保留占位且不误触发；
- P0-3 发送者昵称／群名片：Milky 读 ``event.data.sender``，群名片优先、空值回退 QQ 号；
- P0-4 poke 出入站：Milky 走 nudge，OneBot 仍走 ``send_poke``；
- P0-5 撤回：Milky 走 ``recall_*_message(message_seq=…)``，OneBot 仍走 ``delete_msg``。
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests import _qqbot_fixtures as fx
from tests import _bootstrap  # noqa: F401

import nonebot_plugin_alconna as alconna
from nonebot.adapters.milky.bot import _check_at_me
from nonebot.adapters.onebot.v11 import Bot as OneBotV11Bot
from nonebot.compat import type_validate_python

from nonebot_plugin_hermes_adapter.core import outbound
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage, MessageBuffer
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin, MessageSource
from nonebot_plugin_hermes_adapter.core.routing import CallerScope
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore
from nonebot_plugin_hermes_adapter.handlers import message_ingress, message_media, notices
from nonebot_plugin_hermes_adapter.mcp.tools import poke_user as pu
from nonebot_plugin_hermes_adapter.mcp.tools import recall_message as rm

SELF = fx.MILKY_SELF
OB_SELF = fx.ONEBOT_SELF
GROUP = fx.GROUP


def _milky_target(group_id: str = GROUP, *, private: bool = False) -> alconna.Target:
    return alconna.Target(
        id=str(group_id), private=private, adapter="milky"
    )


def _onebot_target(group_id: str = GROUP, *, private: bool = False) -> alconna.Target:
    return alconna.Target(id=str(group_id), private=private, adapter="onebotv11")


# ------------------------------------------------------------------ P0-1 回执 ID


class SendReceiptIdTests(unittest.TestCase):
    """出站发送回执必须落成干净的外部消息 ID 字符串。"""

    def test_milky_group_send_yields_clean_message_seq(self) -> None:
        harness = fx.MilkyHarness()
        harness.replies["send_group_message"] = {"message_seq": 23097, "time": 1795538642}
        result = asyncio.run(
            outbound.send_text_with_media(
                bot=harness.bot,
                target=_milky_target(),
                text="看这个",
                adapter_name="milky",
            )
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(result.msg_ids, ["23097"], result.msg_ids)

    def test_milky_private_send_yields_clean_message_seq(self) -> None:
        harness = fx.MilkyHarness()
        harness.replies["send_private_message"] = {"message_seq": 30101, "time": 1795538642}
        result = asyncio.run(
            outbound.send_text_with_media(
                bot=harness.bot,
                target=_milky_target("10001", private=True),
                text="私聊",
                adapter_name="milky",
            )
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(result.msg_ids, ["30101"], result.msg_ids)

    def test_onebot_group_send_keeps_message_id(self) -> None:
        bot, socket = fx.onebot_bot(
            {"status": "ok", "retcode": 0, "data": {"message_id": 7180000001}}
        )
        result = asyncio.run(
            outbound.send_text_with_media(
                bot=bot,
                target=_onebot_target(),
                text="看这个",
                adapter_name="onebotv11",
            )
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(result.msg_ids, ["7180000001"], result.msg_ids)
        self.assertEqual(socket.frames[0]["action"], "send_msg")
        self.assertEqual(socket.frames[0]["params"]["group_id"], int(GROUP))

    def test_milky_receipt_id_is_usable_as_reply_target(self) -> None:
        """P0-1 的干净 id 必须能通过出站引用 id 校验（否则引用 bot 自己消息失效）。"""
        harness = fx.MilkyHarness()
        harness.replies["send_group_message"] = {"message_seq": 23097, "time": 1795538642}
        first = asyncio.run(
            outbound.send_text_with_media(
                bot=harness.bot, target=_milky_target(), text="第一条", adapter_name="milky"
            )
        )
        self.assertEqual(first.msg_ids, ["23097"])
        follow = asyncio.run(
            outbound.send_text_with_media(
                bot=harness.bot,
                target=_milky_target(),
                text="第二条",
                reply_qq_message_id=first.msg_ids[0],
                adapter_name="milky",
            )
        )
        self.assertTrue(follow.ok, follow)
        payload = harness.params_for("send_group_message")[-1]
        self.assertEqual(
            payload["message"][0], {"type": "reply", "data": {"message_seq": 23097}}
        )


# ------------------------------------------------------------------ P0-2 入站 @


def _bot_for_event(event):
    """按事件所属适配器复用同一个真实 bot（昵称/解析 API 都走真实 ``call_api``）。"""
    if str(event.self_id) == SELF:
        if "milky" not in _BOTS:
            _BOTS["milky"] = fx.MilkyHarness().bot
        return _BOTS["milky"]
    if "onebot" not in _BOTS:
        _BOTS["onebot"] = fx.onebot_bot()[0]
    return _BOTS["onebot"]


_BOTS: dict[str, object] = {}


class InboundAtBotTests(unittest.TestCase):
    """入站 @bot：两种协议的 mention 形状都要认，且第三方 @ 不得误触发。"""

    def _stripped_milky(self, segments: list[dict], **kwargs):
        event = fx.milky_group_message(segments=segments, **kwargs)
        _check_at_me(_bot_for_event(event), event)  # 真实适配器剥首/尾 @bot
        return event, _uni_with(event)

    def test_pure_milky_at_bot_is_wake_input(self) -> None:
        event, uni = self._stripped_milky([fx.mention(int(SELF))])
        self.assertEqual(event.get_plaintext().strip(), "")
        self.assertTrue(event.is_tome())
        self.assertTrue(message_ingress._has_at_bot_in_original(event, SELF))
        placeholders = message_media._collect_at_placeholders(
            uni, event=event, bot_self_id=SELF
        )
        # 非空占位 = 上层 has_real_content 成立，纯 @bot 不会被空消息闸门吞掉。
        self.assertEqual(placeholders, [f"@{SELF}"])
        self.assertTrue(asyncio.run(message_ingress._ignore_rule(event)))

    def test_text_plus_at_bot_keeps_at_placeholder(self) -> None:
        event, uni = self._stripped_milky(
            [fx.mention(int(SELF)), fx.text(" 看这个")]
        )
        self.assertEqual(event.get_plaintext().strip(), "看这个")
        placeholders = message_media._collect_at_placeholders(
            uni, event=event, bot_self_id=SELF
        )
        self.assertEqual(placeholders, [f"@{SELF}"])
        self.assertFalse(message_ingress._msg_at_only_other_users(uni, SELF))

    def test_third_party_mention_keeps_position_placeholder(self) -> None:
        event, uni = self._stripped_milky(
            [fx.text("@"), fx.mention(10002), fx.text(" 你好")]
        )
        placeholders = message_media._collect_at_placeholders(
            uni, event=event, bot_self_id=SELF
        )
        self.assertEqual(placeholders, ["@10002"])
        targets = message_media._collect_at_targets(uni, event=event, bot_self_id=SELF)
        self.assertEqual(targets, ("10002",))
        self.assertFalse(message_ingress._is_bot_at(uni, SELF))
        self.assertTrue(message_ingress._msg_at_only_other_users(uni, SELF))

    def test_pure_third_party_mention_does_not_address_bot(self) -> None:
        event, uni = self._stripped_milky([fx.mention(10002)])
        self.assertFalse(event.is_tome())
        self.assertFalse(message_ingress._has_at_bot_in_original(event, SELF))
        self.assertEqual(
            message_media._collect_at_placeholders(uni, event=event, bot_self_id=SELF),
            ["@10002"],
        )
        self.assertTrue(message_ingress._msg_at_only_other_users(uni, SELF))

    def test_plain_milky_message_has_no_at_placeholders(self) -> None:
        event, uni = self._stripped_milky([fx.text("随便聊聊")])
        self.assertEqual(
            message_media._collect_at_placeholders(uni, event=event, bot_self_id=SELF),
            [],
        )
        self.assertFalse(message_ingress._has_at_bot_in_original(event, SELF))

    def test_onebot_original_at_bot_still_detected(self) -> None:
        """OneBot 回归：适配器剥走 @bot 后，original_message 兜底仍生效。"""
        event = _onebot_group_message(
            message=[
                {"type": "at", "data": {"qq": OB_SELF}},
                {"type": "text", "data": {"text": " 看这个"}},
            ],
            stripped=[{"type": "text", "data": {"text": "看这个"}}],
        )
        self.assertTrue(message_ingress._has_at_bot_in_original(event, OB_SELF))
        placeholders = message_media._collect_at_placeholders(
            _uni_with(event), event=event, bot_self_id=OB_SELF
        )
        self.assertEqual(placeholders, [f"@{OB_SELF}"])

    def test_onebot_pure_at_bot_is_wake_input(self) -> None:
        event = _onebot_group_message(
            message=[{"type": "at", "data": {"qq": OB_SELF}}],
            stripped=[{"type": "text", "data": {"text": ""}}],
        )
        self.assertTrue(message_ingress._has_at_bot_in_original(event, OB_SELF))
        self.assertEqual(
            message_media._collect_at_placeholders(
                _uni_with(event), event=event, bot_self_id=OB_SELF
            ),
            [f"@{OB_SELF}"],
        )


def _uni_with(event) -> alconna.UniMessage:
    return alconna.UniMessage.generate_without_reply(event=event, bot=_bot_for_event(event))


def _onebot_segments(items: list[dict]):
    from nonebot.adapters.onebot.v11 import Message, MessageSegment

    return Message([MessageSegment(item["type"], item["data"]) for item in items])


def _onebot_group_message(*, message, stripped=None, **overrides):
    """构造真实 v11 群消息。

    ``message`` 是平台上「剥段前」的完整消息；``original_message`` 由适配器自身的
    ``check_message`` 校验器深拷贝成剥段前形态（与真实分发一致）。``stripped`` 用来
    模拟 ``_check_at_me`` 把首/尾 @bot 从 ``event.message`` 摘掉之后的状态。
    """
    from nonebot.adapters.onebot.v11 import GroupMessageEvent

    payload = {
        "time": 1795538642,
        "self_id": int(OB_SELF),
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "message_id": 7180000001,
        "user_id": 10001,
        "message": _onebot_segments(message),
        "raw_message": "看这个",
        "font": 0,
        "sender": {"user_id": 10001, "nickname": "昵称A", "card": "名片A"},
        "to_me": True,
        "group_id": int(GROUP),
    }
    payload.update(overrides)
    event = type_validate_python(GroupMessageEvent, payload)
    if stripped is not None:
        event.message = _onebot_segments(stripped)
    return event


# ------------------------------------------------------------------ P0-3 昵称


class SenderInfoTests(unittest.TestCase):
    """发送者资料统一：Milky 在 event.data.sender，OneBot 在 event.sender。"""

    def test_milky_group_card_wins(self) -> None:
        event = fx.milky_group_message(segments=[fx.text("hi")], card="群名片", nickname="昵称")
        self.assertEqual(message_ingress._extract_sender_nickname(event, "milky"), "群名片")
        event = fx.milky_group_message(segments=[fx.text("hi")], card="   ", nickname="昵称")
        self.assertEqual(message_ingress._extract_sender_nickname(event, "milky"), "昵称")

    def test_milky_private_reads_nickname(self) -> None:
        event = fx.milky_friend_message(segments=[fx.text("hi")], nickname="私聊昵称")
        self.assertEqual(message_ingress._extract_sender_nickname(event, "milky"), "私聊昵称")

    def test_milky_blank_profile_falls_back_to_qq(self) -> None:
        event = fx.milky_group_message(segments=[fx.text("hi")], card="", nickname="")
        self.assertIsNone(message_ingress._extract_sender_nickname(event, "milky"))
        # 上层调用点（handlers/message.py）契约：昵称为空回退 QQ 号。
        self.assertEqual(
            message_ingress._extract_sender_nickname(event, "milky") or "10001", "10001"
        )

    def test_onebot_sender_card_and_nickname_semantics_unchanged(self) -> None:
        event = _onebot_group_message(
            message=[{"type": "text", "data": {"text": "hi"}}],
        )
        self.assertEqual(message_ingress._extract_sender_nickname(event, "onebotv11"), "名片A")
        blank = _onebot_group_message(
            message=[{"type": "text", "data": {"text": "hi"}}],
            sender={"user_id": 10001, "nickname": "昵称A", "card": ""},
        )
        self.assertEqual(
            message_ingress._extract_sender_nickname(blank, "onebotv11"), "昵称A"
        )


# ------------------------------------------------------------------ P0-4 poke


class _Sessions:
    def __init__(self, alive: bool = True) -> None:
        self.alive = alive

    def is_alive_for_trigger(self, adapter: str, group_id: str, now_ms: int) -> bool:
        return self.alive


class _Registry:
    def __init__(self, bot, *, adapter: str, group_id: str = GROUP) -> None:
        self.bot = bot
        self.adapter = adapter
        self.group_id = group_id

    def get(self, adapter: str, scope: str, scope_id: str):
        if (adapter, scope, scope_id) != (self.adapter, "group", self.group_id):
            return None
        return SimpleNamespace(
            bot_self_id=str(self.bot.self_id),
            target=SimpleNamespace(private=False, id=self.group_id),
        )


def _run_poke(inp, *, bot, adapter: str, private_route=None, alive: bool = True):
    patches = [patch.object(pu, "get_bot", new=lambda _sid: bot)]
    if private_route is not None:
        from nonebot_plugin_hermes_adapter.handlers import message_relay

        patches.append(
            patch.object(message_relay, "_resolve_wake_route", new=lambda *a, **k: private_route)
        )
    for item in patches:
        item.start()
    try:
        return asyncio.run(
            pu.poke_user_impl(
                inp,
                active_sessions=_Sessions(alive=alive),
                bot_registry=_Registry(bot, adapter=adapter),
                scope=CallerScope.dev(),
            )
        )
    finally:
        for item in reversed(patches):
            item.stop()


class PokeOutboundTests(unittest.TestCase):
    def test_milky_group_poke_uses_group_nudge(self) -> None:
        harness = fx.MilkyHarness()
        result = _run_poke(
            pu.PokeUserInput(adapter="milky", user_id="10001", group_id=GROUP),
            bot=harness.bot,
            adapter="milky",
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(
            harness.params_for("send_group_nudge"),
            [{"group_id": int(GROUP), "user_id": 10001}],
        )
        # poke 仍然是独立动作：不混入任何聊天消息。
        self.assertEqual(harness.actions(), ["send_group_nudge"])

    def test_milky_private_poke_uses_friend_nudge(self) -> None:
        harness = fx.MilkyHarness()
        with patch(
            "nonebot_plugin_hermes_adapter.core.user_lists.private_allowed",
            return_value=True,
        ):
            result = _run_poke(
                pu.PokeUserInput(adapter="milky", user_id="10001"),
                bot=harness.bot,
                adapter="milky",
                private_route=(harness.bot, SimpleNamespace(private=True, id="10001")),
            )
        self.assertTrue(result.ok, result)
        self.assertEqual(
            harness.params_for("send_friend_nudge"),
            [{"user_id": 10001, "is_self": False}],
        )
        self.assertEqual(harness.actions(), ["send_friend_nudge"])

    def test_onebot_poke_still_uses_send_poke(self) -> None:
        bot, socket = fx.onebot_bot({"status": "ok", "retcode": 0, "data": None})
        result = _run_poke(
            pu.PokeUserInput(adapter="onebotv11", user_id="10001", group_id=GROUP),
            bot=bot,
            adapter="onebotv11",
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(socket.frames[0]["action"], "send_poke")
        self.assertEqual(
            socket.frames[0]["params"], {"group_id": int(GROUP), "user_id": 10001}
        )

    def test_milky_self_poke_is_refused_without_platform_call(self) -> None:
        harness = fx.MilkyHarness()
        result = _run_poke(
            pu.PokeUserInput(adapter="milky", user_id=SELF, group_id=GROUP),
            bot=harness.bot,
            adapter="milky",
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertEqual(harness.frames, [])

    def test_milky_poke_platform_rejection_keeps_retcode(self) -> None:
        harness = fx.MilkyHarness()
        harness.failures["send_group_nudge"] = {
            "status": "failed",
            "retcode": 1400,
            "data": None,
            "message": "forbidden",
        }
        result = _run_poke(
            pu.PokeUserInput(adapter="milky", user_id="10001", group_id=GROUP),
            bot=harness.bot,
            adapter="milky",
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "platform_rejected")
        self.assertEqual(result.retcode, 1400)


def _dispatch_poke(bot, event, *, isolation: bool = True):
    """跑真实 notice 分发；只截取「合成输入」出口，路由/昵称仍走真实代码。"""
    captured: list[dict] = []

    async def fake_route(**kwargs):
        captured.append(kwargs)

    with patch.object(notices, "route_synthesized_input", new=fake_route), patch.object(
        notices, "check_isolation", new=lambda *a, **k: isolation
    ):
        asyncio.run(notices.dispatch(bot=bot, event=event))
    return captured


def _member_payload(card: str = "群名片", nickname: str = "昵称") -> dict:
    return {
        "member": {
            "user_id": 10001,
            "nickname": nickname,
            "card": card,
            "sex": "unknown",
            "group_id": int(GROUP),
            "title": "",
            "level": 1,
            "role": "member",
            "join_time": 1,
            "last_sent_time": 2,
        }
    }


def _friend_payload(nickname: str = "好友昵称") -> dict:
    return {
        "friend": {
            "user_id": 10001,
            "nickname": nickname,
            "sex": "unknown",
            "qid": "",
            "remark": "",
            "category": {"category_id": 0, "category_name": "默认"},
        }
    }


class PokeIngressTests(unittest.TestCase):
    def test_milky_group_nudge_to_bot_synthesizes_poke(self) -> None:
        harness = fx.MilkyHarness()
        harness.replies["get_group_member_info"] = _member_payload()
        captured = _dispatch_poke(harness.bot, fx.milky_group_nudge())
        self.assertEqual(len(captured), 1, captured)
        call = captured[0]
        self.assertEqual(call["adapter_name"], "milky")
        self.assertEqual(call["user_id"], "10001")
        self.assertEqual(call["group_id"], GROUP)
        self.assertEqual(call["source"], MessageSource.NOTICE_POKE)
        self.assertTrue(call["addressed_to_bot"])
        self.assertTrue(call["allow_passive"])
        self.assertEqual(call["nickname"], "群名片")
        self.assertIn("戳了你一下", call["text"])
        self.assertIn("10001", call["text"])
        self.assertFalse(call["target"].private)

    def test_milky_friend_nudge_to_bot_synthesizes_private_poke(self) -> None:
        harness = fx.MilkyHarness()
        harness.replies["get_friend_info"] = _friend_payload()
        captured = _dispatch_poke(harness.bot, fx.milky_friend_nudge())
        self.assertEqual(len(captured), 1, captured)
        call = captured[0]
        self.assertEqual(call["adapter_name"], "milky")
        self.assertTrue(call["target"].private)
        self.assertIsNone(call["group_id"])
        self.assertEqual(call["nickname"], "好友昵称")
        self.assertIn("戳了你一下", call["text"])

    def test_milky_group_nudge_to_other_member_is_ignored(self) -> None:
        harness = fx.MilkyHarness()
        captured = _dispatch_poke(
            harness.bot, fx.milky_group_nudge(sender_id=10001, receiver_id=10002)
        )
        self.assertEqual(captured, [])
        self.assertEqual(harness.frames, [])

    def test_milky_self_nudge_is_ignored(self) -> None:
        harness = fx.MilkyHarness()
        captured = _dispatch_poke(
            harness.bot, fx.milky_group_nudge(sender_id=int(SELF), receiver_id=int(SELF))
        )
        self.assertEqual(captured, [])

    def test_milky_friend_nudge_sent_by_bot_is_ignored(self) -> None:
        harness = fx.MilkyHarness()
        captured = _dispatch_poke(
            harness.bot, fx.milky_friend_nudge(is_self_send=True, is_self_receive=False)
        )
        self.assertEqual(captured, [])

    def test_onebot_poke_notice_still_synthesizes(self) -> None:
        bot, _socket = fx.onebot_bot()
        captured = _dispatch_poke(bot, fx.onebot_poke_notice())
        self.assertEqual(len(captured), 1, captured)
        call = captured[0]
        self.assertEqual(call["adapter_name"], "onebotv11")
        self.assertEqual(call["user_id"], "10001")
        self.assertEqual(call["source"], MessageSource.NOTICE_POKE)
        self.assertTrue(call["addressed_to_bot"])


# ------------------------------------------------------------------ P0-5 撤回


class RecallHarness(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = MessageStore(Path(self.tmp.name) / "messages.db")
        self.addCleanup(self.store.close)
        self.buffer = MessageBuffer(store=self.store, fetcher=MagicMock())

    def _store_bot_message(
        self,
        *,
        adapter: str,
        qq_message_id: str,
        group_id: str | None = GROUP,
        bot_self_id: str = SELF,
        private_owner: str | None = None,
    ) -> int:
        msg = BufferedMessage(
            ts=1,
            adapter=adapter,
            group_id=group_id,
            user_id=str(bot_self_id),
            nickname="bot",
            content="bot 说的话",
            origin=MessageOrigin.bot_output(),
            is_bot=True,
            qq_message_id=qq_message_id,
            private_owner_id=private_owner,
        )
        self.buffer.append(msg)
        return self._last_id(adapter)

    def _last_id(self, adapter: str) -> int:
        row = self.store._conn.execute(
            "SELECT id FROM messages WHERE adapter=? ORDER BY id DESC LIMIT 1", (adapter,)
        ).fetchone()
        return int(row["id"])

    def _run(self, inp, *, bot, adapter: str, private_route=None):
        patches = [patch.object(rm, "get_bot", new=lambda _sid: bot)]
        if private_route is not None:
            from nonebot_plugin_hermes_adapter.handlers import message_relay

            patches.append(
                patch.object(
                    message_relay, "_resolve_wake_route", new=lambda *a, **k: private_route
                )
            )
        for item in patches:
            item.start()
        try:
            return asyncio.run(
                rm.recall_message_impl(
                    inp,
                    active_sessions=_Sessions(),
                    bot_registry=_Registry(bot, adapter=adapter),
                    scope=CallerScope.dev(),
                    message_buffer=self.buffer,
                )
            )
        finally:
            for item in reversed(patches):
                item.stop()


class RecallTests(RecallHarness):
    def test_milky_group_recall_uses_message_seq(self) -> None:
        harness = fx.MilkyHarness()
        logical = self._store_bot_message(adapter="milky", qq_message_id="23097")
        result = self._run(
            rm.RecallMessageInput(adapter="milky", message_id=f"m:{logical}", group_id=GROUP),
            bot=harness.bot,
            adapter="milky",
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(
            harness.params_for("recall_group_message"),
            [{"group_id": int(GROUP), "message_seq": 23097}],
        )

    def test_milky_private_recall_uses_message_seq(self) -> None:
        harness = fx.MilkyHarness()
        logical = self._store_bot_message(
            adapter="milky",
            qq_message_id="30101",
            group_id=None,
            private_owner="10001",
        )
        with patch(
            "nonebot_plugin_hermes_adapter.core.user_lists.private_allowed",
            return_value=True,
        ):
            result = self._run(
                rm.RecallMessageInput(adapter="milky", message_id=f"m:{logical}", user_id="10001"),
                bot=harness.bot,
                adapter="milky",
                private_route=(harness.bot, SimpleNamespace(private=True, id="10001")),
            )
        self.assertTrue(result.ok, result)
        self.assertEqual(
            harness.params_for("recall_private_message"),
            [{"user_id": 10001, "message_seq": 30101}],
        )

    def test_onebot_recall_still_uses_delete_msg(self) -> None:
        bot, socket = fx.onebot_bot({"status": "ok", "retcode": 0, "data": None})
        logical = self._store_bot_message(
            adapter="onebotv11", qq_message_id="7180000001", bot_self_id=OB_SELF
        )
        result = self._run(
            rm.RecallMessageInput(
                adapter="onebotv11", message_id=f"m:{logical}", group_id=GROUP
            ),
            bot=bot,
            adapter="onebotv11",
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(socket.frames[0]["action"], "delete_msg")
        self.assertEqual(socket.frames[0]["params"], {"message_id": 7180000001})

    def test_recall_of_other_senders_message_is_still_refused(self) -> None:
        harness = fx.MilkyHarness()
        logical = self._store_bot_message(
            adapter="milky", qq_message_id="23097", bot_self_id="10002"
        )
        result = self._run(
            rm.RecallMessageInput(adapter="milky", message_id=f"m:{logical}", group_id=GROUP),
            bot=harness.bot,
            adapter="milky",
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertIn("only messages sent by the current bot", result.error or "")
        self.assertEqual(harness.frames, [])

    def test_legacy_dirty_receipt_is_refused_not_crashed(self) -> None:
        harness = fx.MilkyHarness()
        logical = self._store_bot_message(
            adapter="milky", qq_message_id="message_seq=23097 time=1795538642"
        )
        result = self._run(
            rm.RecallMessageInput(adapter="milky", message_id=f"m:{logical}", group_id=GROUP),
            bot=harness.bot,
            adapter="milky",
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertIn("invalid", result.error or "")
        self.assertEqual(harness.frames, [])

    def test_send_receipt_feeds_recall_end_to_end(self) -> None:
        """P0-1 ↔ P0-5 联动：真实发送回执 → 落库 → 按 m:id 撤回同一 message_seq。"""
        harness = fx.MilkyHarness()
        harness.replies["send_group_message"] = {"message_seq": 23097, "time": 1795538642}
        sent = asyncio.run(
            outbound.send_text_with_media(
                bot=harness.bot, target=_milky_target(), text="要撤回的", adapter_name="milky"
            )
        )
        self.assertEqual(sent.msg_ids, ["23097"])
        logical = self._store_bot_message(
            adapter="milky", qq_message_id=sent.primary_qq_message_id
        )
        harness.frames.clear()
        result = self._run(
            rm.RecallMessageInput(adapter="milky", message_id=f"m:{logical}", group_id=GROUP),
            bot=harness.bot,
            adapter="milky",
        )
        self.assertTrue(result.ok, result)
        self.assertEqual(
            harness.params_for("recall_group_message"),
            [{"group_id": int(GROUP), "message_seq": 23097}],
        )


if __name__ == "__main__":
    unittest.main()
