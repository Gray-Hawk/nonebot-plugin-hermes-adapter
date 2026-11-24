"""QQ 官方 Bot(nonebot-adapter-qq)真事件夹具:平台差异层 + 入口/出站链路。

本文件用 **真实** ``nonebot.adapters.qq`` 事件与 Message 类做夹具(在装了
nonebot-adapter-qq 的 venv 里跑,未装则整体 skip)。覆盖交付口径:

- C2C 私聊、群 @、群全量非 @、群全量正文中间 @ 机器人;
- OpenID 与 Bot self_id 不同(不得拿 member_openid 等同 self_id);
- 重复 event.id 不得造成双轮;
- 基础图片入站(attachment → alconna Image → HTTP URL);
- 引用(QQReplyMessage 模型)的正文/图片/引用号;
- 连续两段正文交付:每段一条平台 send,msg_id=触发事件 id、msg_seq 递增;
- 官方额度/被动回复被拒时如实失败(不伪造成功),并记下平台 code。
"""

from __future__ import annotations

import asyncio
import json
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401

import nonebot
from nonebot.drivers import Response
from nonebot.matcher import Matcher

import nonebot_plugin_alconna as alconna

try:  # adapter-qq 只在灰度/QQ 样板环境安装;主 venv 没有时整体 skip。
    from nonebot.adapters.qq import Bot as QQBot
    from nonebot.adapters.qq.bot import _check_at_me, _check_reply
    from nonebot.adapters.qq.event import (
        C2CMessageCreateEvent,
        GroupAtMessageCreateEvent,
        GroupMessageCreateEvent,
    )
    from nonebot.adapters.qq.models import PostC2CMessagesReturn, PostGroupMessagesReturn
    from nonebot_plugin_alconna.uniseg.adapters import BUILDER_MAPPING, EXPORTER_MAPPING
    from nonebot_plugin_alconna.uniseg.adapters.qq.builder import QQMessageBuilder
    from nonebot_plugin_alconna.uniseg.adapters.qq.exporter import QQMessageExporter

    HAS_QQ = True
except Exception:  # pragma: no cover - 主 venv 路径
    HAS_QQ = False

if HAS_QQ:
    from nonebot_plugin_hermes_adapter.core import platform_compat
    from nonebot_plugin_hermes_adapter.core import outbound
    from nonebot_plugin_hermes_adapter.core.chat_config import parse_chat_config
    from nonebot_plugin_hermes_adapter.handlers import message as message_handler
    from nonebot_plugin_hermes_adapter.handlers import message_perception
    from nonebot_plugin_hermes_adapter.handlers.message_ingress import _is_bot_at
    from nonebot_plugin_hermes_adapter.handlers.message_media import (
        _collect_at_placeholders,
        _extract_image_urls,
        _extract_replied_media,
    )

    # alconna 的 builder/exporter 映射只在 adapter 已注册时填充;测试进程用
    # fastapi 驱动注册不了 QQ adapter,这里显式注入同一份真实实现。
    BUILDER_MAPPING.setdefault("QQ", QQMessageBuilder())
    EXPORTER_MAPPING.setdefault("QQ", QQMessageExporter())

BOT_APPID = "102000001"
GROUP_OPENID = "GROUP_OPENID_1"
GROUP_NUM = "700000001"
MEMBER_OPENID = "MEMBER_OPENID_A"


class _Adapter:
    @staticmethod
    def get_name() -> str:
        return "QQ"


def _bot(*, send_group=None, send_c2c=None):
    """真 QQBot 实例(只跳过 __init__),供 exporter 的 isinstance 判定。"""
    bot = QQBot.__new__(QQBot)
    bot.adapter = _Adapter()
    bot.self_id = BOT_APPID
    bot.self_info = SimpleNamespace(id=BOT_APPID, username="bot")
    if send_group is not None:
        bot.send_to_group = send_group
    if send_c2c is not None:
        bot.send_to_c2c = send_c2c
    return bot


def _group_at_event(**overrides):
    payload = {
        "id": "EVT_GROUP_AT_1",
        "content": " 看一下这张图",
        "timestamp": "2026-11-24T12:00:00+08:00",
        "author": {
            "id": MEMBER_OPENID, "bot": False, "member_openid": MEMBER_OPENID,
            "member_role": "member", "username": "小明",
        },
        "group_id": GROUP_NUM,
        "group_openid": GROUP_OPENID,
        "mentions": [
            {"scope": "single", "bot": True, "id": BOT_APPID, "is_you": True,
             "member_openid": BOT_APPID, "username": "bot"},
        ],
        "attachments": [
            {"content_type": "image/png", "filename": "a.png", "size": 123,
             "url": "https://example.invalid/a.png", "width": 10, "height": 10},
        ],
        "message_scene": {"ext": ["msg_idx=IDX1"], "source": "default"},
        "message_type": 103,
        "msg_idx": "IDX1",
    }
    payload.update(overrides)
    event = GroupAtMessageCreateEvent.model_validate(payload)
    _check_at_me(_bot(), event)
    return event


def _group_full_event(**overrides):
    payload = {
        "id": "EVT_GROUP_PLAIN_1",
        "content": "大家好啊",
        "timestamp": "2026-11-24T12:00:02+08:00",
        "author": {
            "id": MEMBER_OPENID, "bot": False, "member_openid": MEMBER_OPENID,
            "member_role": "member",
        },
        "group_id": GROUP_NUM,
        "group_openid": GROUP_OPENID,
    }
    payload.update(overrides)
    event = GroupMessageCreateEvent.model_validate(payload)
    _check_at_me(_bot(), event)
    return event


def _c2c_event(**overrides):
    payload = {
        "id": "EVT_C2C_1",
        "content": "你好",
        "timestamp": "2026-11-24T12:00:01+08:00",
        "author": {"id": "USER_OPENID_B", "user_openid": "USER_OPENID_B", "username": "小红"},
    }
    payload.update(overrides)
    return C2CMessageCreateEvent.model_validate(payload)


def _target_for(event):
    return EXPORTER_MAPPING["QQ"].get_target(event, _bot())


@unittest.skipUnless(HAS_QQ, "nonebot-adapter-qq 未安装(QQ 官方样板在装了它的 venv 里跑)")
class OfficialEventShapeTests(unittest.TestCase):
    def setUp(self):
        platform_compat.reset_inbound_dedupe()

    def test_platform_message_id_is_event_id(self):
        for event in (_c2c_event(), _group_at_event(), _group_full_event()):
            with self.subTest(kind=type(event).__name__):
                self.assertFalse(hasattr(event, "message_id"))
                self.assertEqual(platform_compat.platform_message_id(event), event.id)

    def test_c2c_target_is_private_with_user_openid(self):
        event = _c2c_event()
        target = _target_for(event)
        self.assertTrue(target.private)
        self.assertEqual(target.id, "USER_OPENID_B")
        self.assertEqual(target.source, event.id)
        self.assertTrue(event.is_tome())
        self.assertEqual(event.get_user_id(), "USER_OPENID_B")

    def test_group_at_target_is_group_openid(self):
        event = _group_at_event()
        target = _target_for(event)
        self.assertFalse(target.private)
        self.assertEqual(target.id, GROUP_OPENID)
        self.assertEqual(target.source, event.id)
        self.assertTrue(event.is_tome())

    def test_group_at_mention_is_addressed_to_bot(self):
        event = _group_at_event()
        uni = alconna.UniMessage.generate_without_reply(event=event, bot=_bot())
        self.assertTrue(_is_bot_at(uni, BOT_APPID))
        self.assertIn(f"@{BOT_APPID}", _collect_at_placeholders(uni))

    def test_full_group_plain_message_is_not_addressed(self):
        event = _group_full_event()
        uni = alconna.UniMessage.generate_without_reply(event=event, bot=_bot())
        self.assertFalse(event.is_tome())
        self.assertFalse(_is_bot_at(uni, BOT_APPID))
        self.assertEqual(uni.extract_plain_text().strip(), "大家好啊")

    def test_full_group_mid_text_mention_counts_as_addressed(self):
        # 全量群里正文中间 @bot:适配器不置 to_me,但 mention 段 user_id 已改成
        # bot.self_info.id —— 寻址事实必须由 At 段给出,不能只看 is_tome()。
        event = _group_full_event(
            id="EVT_GROUP_MID_1", content=f"请 <@!{BOT_APPID}> 看看"
        )
        uni = alconna.UniMessage.generate_without_reply(event=event, bot=_bot())
        self.assertFalse(event.is_tome())
        self.assertTrue(_is_bot_at(uni, BOT_APPID))

    def test_openid_is_not_bot_self_id(self):
        event = _group_full_event()
        user_id = event.get_user_id()
        self.assertNotEqual(user_id, str(BOT_APPID))
        self.assertFalse(platform_compat.is_bot_authored(event, adapter_name="qq"))

    def test_bot_authored_group_message_is_detected(self):
        payload = {
            "id": "EVT_BOT_MSG",
            "content": "我是另一个机器人",
            "timestamp": "2026-11-24T12:00:09+08:00",
            "author": {"id": "OTHER_BOT", "bot": True, "member_openid": "OTHER_BOT",
                       "member_role": "member"},
            "group_id": GROUP_NUM,
            "group_openid": GROUP_OPENID,
        }
        event = GroupMessageCreateEvent.model_validate(payload)
        self.assertTrue(platform_compat.is_bot_authored(event, adapter_name="qq"))

    def test_basic_image_resolves_to_http_url(self):
        event = _group_at_event()
        uni = alconna.UniMessage.generate_without_reply(event=event, bot=_bot())
        self.assertEqual(
            asyncio.run(_extract_image_urls(uni, _bot(), "qq")),
            ["https://example.invalid/a.png"],
        )

    def test_reply_model_gives_text_image_and_quote_id(self):
        reply_payload = {
            "content": "原消息内容",
            "msg_idx": "IDX0",
            "message_type": 103,
            "attachments": [
                {"content_type": "image/jpeg", "url": "https://example.invalid/q.jpg"}
            ],
            "author": {"id": "MEMBER_OPENID_X", "bot": False,
                       "member_openid": "MEMBER_OPENID_X", "username": "旧人"},
        }
        event = _group_at_event(
            id="EVT_GROUP_AT_2",
            content="同意",
            msg_idx="IDX2",
            message_scene={"ext": ["msg_idx=IDX2", "ref_msg_idx=IDX0"], "source": "default"},
            msg_elements=[reply_payload],
        )
        asyncio.run(_check_reply(_bot(), event))
        self.assertIsNotNone(event.reply)
        replied = asyncio.run(_extract_replied_media(event, _bot(), adapter_name="qq"))
        self.assertEqual(replied.msg_id, "IDX0")
        self.assertEqual(replied.text, "原消息内容")
        self.assertEqual(replied.image_urls, ["https://example.invalid/q.jpg"])


@unittest.skipUnless(HAS_QQ, "nonebot-adapter-qq 未安装")
class DuplicateDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        platform_compat.reset_inbound_dedupe()
        message_handler._debounce_states.clear()

    def tearDown(self):
        platform_compat.reset_inbound_dedupe()
        message_handler._debounce_states.clear()

    def _harness(self, stack, *, adapter_name="qq", turns=None):
        cfg = parse_chat_config({"reply_debounce_sec": 0})
        target = _target_for(_group_at_event())
        stack.enter_context(patch.object(message_handler.alconna, "get_target", lambda: target))
        stack.enter_context(patch.object(message_handler, "get_adapter_name", lambda _t: adapter_name))
        stack.enter_context(patch.object(message_handler, "load_chat_config", lambda: cfg))
        stack.enter_context(patch.object(message_handler, "check_isolation", lambda *_: True))
        stack.enter_context(patch.object(message_handler, "_now_ms", lambda: 100000))
        stack.enter_context(patch.object(message_handler.session_manager, "get_delivery_generation",
                                        lambda *a, **k: 0))
        stack.enter_context(patch.object(message_handler._mcp, "message_buffer", object()))
        stack.enter_context(patch.object(
            message_handler._mcp, "active_sessions", _sessions_stub()
        ))
        stack.enter_context(patch.object(message_handler.user_lists, "is_known_bot", lambda _u: False))
        stack.enter_context(patch.object(message_handler.user_lists, "is_blacklisted", lambda *_: False))
        if turns is not None:
            stack.enter_context(patch.object(
                message_handler, "_dispatch_chat", _async_turn_recorder(turns)
            ))
            stack.enter_context(patch.object(
                message_handler, "_debounce_arrive", _sync_turn_recorder(turns)
            ))

    async def _deliver_twice(self, event):
        # 一轮 turn = 一次 dispatch 或一次防抖首条登记;去重生效时只能有一次。
        turns: list = []
        with ExitStack() as stack:
            self._harness(stack, turns=turns)
            for _ in range(2):
                try:
                    await message_handler.handle_message(_bot(), event, Matcher)
                except Exception as exc:  # SkippedException 等:入口拒绝面
                    if type(exc).__name__ not in {"SkippedException", "StopPropagation"}:
                        raise
        return turns

    async def test_same_event_id_does_not_trigger_twice(self):
        turns = await self._deliver_twice(_group_at_event())
        self.assertEqual(len(turns), 1)

    async def test_distinct_event_ids_both_trigger(self):
        first = await self._deliver_twice(_group_at_event())
        second = await self._deliver_twice(
            _group_at_event(id="EVT_GROUP_AT_OTHER", content="第二句")
        )
        self.assertEqual((len(first), len(second)), (1, 1))

    async def test_onebot_adapter_keeps_repeat_behavior(self):
        class _OneBotEvent:
            message_id = 4242
            raw_message = "同一条"
            original_message = None

            def get_user_id(self):
                return "10001"

            def is_tome(self):
                return False

            def get_message(self):
                return alconna.UniMessage(["同一条"])

            def get_plaintext(self):
                return "同一条"

        class _OneBotAdapter:
            @staticmethod
            def get_name() -> str:
                return "OneBot V11"

        target = SimpleNamespace(id="54321", private=True)
        cfg = parse_chat_config({"reply_debounce_sec": 0})
        turns: list = []
        with ExitStack() as stack:
            stack.enter_context(patch.object(message_handler.alconna, "get_target", lambda: target))
            stack.enter_context(patch.object(message_handler, "get_adapter_name", lambda _t: "onebotv11"))
            stack.enter_context(patch.object(message_handler, "load_chat_config", lambda: cfg))
            stack.enter_context(patch.object(message_handler, "check_isolation", lambda *_: True))
            stack.enter_context(patch.object(message_handler, "_now_ms", lambda: 100000))
            stack.enter_context(patch.object(message_handler.session_manager, "get_delivery_generation",
                                            lambda *a, **k: 0))
            stack.enter_context(patch.object(message_handler._mcp, "message_buffer", object()))
            stack.enter_context(patch.object(
                message_handler._mcp, "active_sessions", _sessions_stub()
            ))
            stack.enter_context(patch.object(message_handler.user_lists, "is_known_bot", lambda _u: False))
            stack.enter_context(patch.object(message_handler.user_lists, "is_blacklisted", lambda *_: False))
            stack.enter_context(patch.object(message_handler, "_dispatch_chat", _async_turn_recorder(turns)))
            stack.enter_context(patch.object(message_handler, "_debounce_arrive", _sync_turn_recorder(turns)))
            stack.enter_context(patch.object(
                alconna.UniMessage, "generate_without_reply", lambda **kw: kw["event"].get_message()
            ))
            bot = SimpleNamespace(
                self_id="999", adapter=_OneBotAdapter(),
            )
            for _ in range(2):
                try:
                    await message_handler.handle_message(bot, _OneBotEvent(), Matcher)
                except Exception as exc:
                    if type(exc).__name__ not in {"SkippedException", "StopPropagation"}:
                        raise
        # 旧行为不变:OneBot 不做平台 id 去重,两条都各自起一轮。
        self.assertEqual(len(turns), 2)


async def _noop_dispatch(**kwargs):
    return None


def _async_turn_recorder(sink: list):
    """`_dispatch_chat` 是 async:记一笔代表起了这一轮。"""

    async def record(*args, **kwargs):
        sink.append(kwargs)

    return record


def _sync_turn_recorder(sink: list):
    """`_debounce_arrive` 是同步:记一笔代表防抖首条已登记(=会起一轮)。"""

    def record(*args, **kwargs):
        sink.append(kwargs)

    return record


def _sessions_stub():
    """活跃窗桩:窗口恒关(必须由显式 @ 触发),trigger/touch 都可调用。"""
    return SimpleNamespace(
        is_alive_for_trigger=lambda *a, **k: False,
        trigger=lambda *a, **k: None,
        touch=lambda *a, **k: None,
    )


@unittest.skipUnless(HAS_QQ, "nonebot-adapter-qq 未安装")
class PerceptionDedupeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        platform_compat.reset_inbound_dedupe()

    def tearDown(self):
        platform_compat.reset_inbound_dedupe()

    async def test_duplicate_event_inserted_once(self):
        stored: list = []
        buffer = SimpleNamespace(
            append=lambda m: stored.append(m) or setattr(m, "id", len(stored)),
            update_content=lambda *a, **k: None,
        )
        registry = SimpleNamespace(upsert=lambda **kw: None)
        target = _target_for(_group_at_event())
        event = _group_at_event()
        with ExitStack() as stack:
            stack.enter_context(patch.object(message_perception.alconna, "get_target", lambda: target))
            stack.enter_context(patch.object(message_perception, "get_adapter_name", lambda _t: "qq"))
            stack.enter_context(patch.object(
                message_perception, "load_misc_config",
                lambda: SimpleNamespace(history_text_max_chars=800),
            ))
            stack.enter_context(patch.object(message_perception, "sync_buddy_to_group", lambda *a: None))
            stack.enter_context(patch.object(message_perception._mcp, "message_buffer", buffer))
            stack.enter_context(patch.object(message_perception._mcp, "bot_registry", registry))
            for _ in range(2):
                try:
                    await message_perception.handle_perception(_bot(), event)
                except Exception as exc:
                    if type(exc).__name__ not in {"SkippedException", "StopPropagation"}:
                        raise
        self.assertEqual(len(stored), 1)
        # 平台消息号与 m:id 同一行落库(QQ 官方取 event.id)
        self.assertEqual(stored[0].qq_message_id, event.id)


@unittest.skipUnless(HAS_QQ, "nonebot-adapter-qq 未安装")
class OfficialDeliveryTests(unittest.TestCase):
    def setUp(self):
        platform_compat.reset_inbound_dedupe()

    def _sending_bot(self, *, replies, error=None):
        calls: list = []

        async def send_to_group(*, group_openid, message, msg_id=None, msg_seq=None,
                                msg_ref_id=None, **kwargs):
            calls.append({
                "openid": group_openid,
                "text": message.extract_plain_text(),
                "msg_id": msg_id,
                "msg_seq": msg_seq,
            })
            if error is not None:
                raise error
            return replies[len(calls) - 1]

        bot = _bot(send_group=send_to_group)
        return bot, calls

    def test_two_consecutive_parts_use_event_id_and_increment_msg_seq(self):
        event = _group_at_event()
        target = _target_for(event)
        bot, calls = self._sending_bot(replies=[
            PostGroupMessagesReturn(id="MSG_OUT_1"),
            PostGroupMessagesReturn(id="MSG_OUT_2"),
        ])
        cfg = SimpleNamespace(long_reply_forward=False)
        with patch.object(outbound, "load_chat_config", lambda: cfg):
            first = asyncio.run(outbound.send_text_with_media(
                bot=bot, target=target, text="第一段", adapter_name="qq",
            ))
            second = asyncio.run(outbound.send_text_with_media(
                bot=bot, target=target, text="第二段", adapter_name="qq",
            ))
        self.assertEqual(first.msg_ids, ["MSG_OUT_1"])
        self.assertEqual(second.msg_ids, ["MSG_OUT_2"])
        self.assertEqual([c["msg_id"] for c in calls], [event.id, event.id])
        self.assertEqual([c["msg_seq"] for c in calls], [1, 2])
        self.assertEqual([c["text"] for c in calls], ["第一段", "第二段"])

    def test_c2c_send_uses_user_openid(self):
        calls: list = []

        async def send_to_c2c(*, openid, message, msg_id=None, msg_seq=None,
                              msg_ref_id=None, **kwargs):
            calls.append({"openid": openid, "msg_id": msg_id})
            return PostC2CMessagesReturn(id="MSG_C2C_1")

        event = _c2c_event()
        target = _target_for(event)
        bot = _bot(send_c2c=send_to_c2c)
        with patch.object(outbound, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=False)):
            result = asyncio.run(outbound.send_text_with_media(
                bot=bot, target=target, text="收到", adapter_name="qq",
            ))
        self.assertTrue(result.ok)
        self.assertEqual(calls, [{"openid": "USER_OPENID_B", "msg_id": event.id}])

    def test_quota_or_passive_rejection_is_reported_not_faked(self):
        from nonebot.adapters.qq.exception import ActionFailed as QQActionFailed

        event = _group_at_event()
        target = _target_for(event)
        error = QQActionFailed(
            Response(
                400,
                headers={},
                content=json.dumps({"code": 40034, "message": "msg_seq invalid"}).encode(),
            )
        )
        bot, calls = self._sending_bot(replies=[], error=error)
        with patch.object(outbound, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=False)), \
                patch.object(outbound.logger, "warning") as warn:
            result = asyncio.run(outbound.send_text_with_media(
                bot=bot, target=target, text="会被拒", adapter_name="qq",
            ))
        self.assertFalse(result.ok)
        self.assertEqual(result.msg_ids, [])
        self.assertEqual(len(calls), 1)
        logged = " ".join(str(c.args[0]) for c in warn.call_args_list if c.args)
        self.assertIn("40034", logged)
        self.assertNotIn("secret", logged.lower())


if __name__ == "__main__":
    unittest.main()
