"""平台差异层:OneBot v11 / QQ 官方 Bot(adapter-qq)语义差异的集中点。

本文件不需要 ``nonebot.adapters.qq`` 即可运行(test_qq_official_events.py 才需要):
它锁定的是「差异层本身」的契约 —— 取号、去重、能力表、各处的降级口径。

覆盖:
- ``platform_message_id``:OneBot=``event.message_id``,QQ 官方=``event.id``;
  旧适配器没有 ``message_id`` 时**不**回落到 ``.id``(旧行为不变)。
- ``claim_inbound``:仅官方 QQ 启用,按 (scope, adapter, 平台消息 id) 有界去重;
  OneBot 恒放行;TTL 过期后可再次认领;容量上限生效。
- ``capabilities``:官方 QQ 明确 unsupported 的能力集合,OneBot/未知一律旧行为。
- ``reply_content`` / ``reply_message_id``:平台模型型引用(QQ 官方 QQReplyMessage)
  的正文、图片与引用号;OneBot 的 ``Message`` 形态返回 None 交回旧路径。
- ``_extract_msg_id``:QQ 官方回执是 pydantic 模型(``.id``),不能 str() 成 repr;
  模型 ``id=None`` 必须是 None,不得伪造。
- 能力不足时的明确拒绝:poke / recall / group_files / emoji 回执 / 内联 @ 标记。
"""

from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401

import nonebot

try:  # 仅某个用例需要 adapter-qq 的真实异常类型
    from nonebot.adapters.qq.exception import ActionFailed as QQActionFailed

    HAS_QQ_ADAPTER = True
except Exception:  # pragma: no cover - 未装 adapter-qq 的环境
    QQActionFailed = None  # type: ignore[assignment]
    HAS_QQ_ADAPTER = False

from nonebot_plugin_hermes_adapter.core import platform_compat
from nonebot_plugin_hermes_adapter.core import outbound
from nonebot_plugin_hermes_adapter.core.routing import CallerScope
from nonebot_plugin_hermes_adapter.handlers import message_feedback
from nonebot_plugin_hermes_adapter.mcp.tools import group_files as gf
from nonebot_plugin_hermes_adapter.mcp.tools import poke_user as pu
from nonebot_plugin_hermes_adapter.mcp.tools import recall_message as rm


class _Event:
    """最小事件桩:只带 platform_message_id 关心的字段。"""

    def __init__(self, **fields):
        self.__dict__.update(fields)


class PlatformMessageIdTests(unittest.TestCase):
    def test_onebot_shape_uses_message_id(self):
        ev = _Event(message_id=12345)
        # 原始类型必须保住:int 一路进 set_msg_emoji_like(message_id=…) 等平台调用。
        self.assertEqual(platform_compat.platform_message_id(ev), 12345)
        self.assertIsInstance(platform_compat.platform_message_id(ev), int)

    def test_onebot_string_message_id_stays_str(self):
        ev = _Event(message_id=" 12345 ")
        self.assertEqual(platform_compat.platform_message_id(ev), " 12345 ")

    def test_official_qq_shape_falls_back_to_event_id(self):
        ev = _Event(id="EVT_QQ_1")
        self.assertEqual(
            platform_compat.platform_message_id(ev, adapter_name="qq"), "EVT_QQ_1"
        )

    def test_official_qq_adapter_name_is_normalised(self):
        # adapter-qq 的 get_name() 是 "QQ";归一化后才是路由键 "qq"。
        ev = _Event(id="EVT_QQ_2")
        self.assertEqual(
            platform_compat.platform_message_id(ev, adapter_name="QQ"), "EVT_QQ_2"
        )

    def test_unknown_platform_without_message_id_keeps_old_none(self):
        # 没有 message_id 的非官方事件过去得到 None,不得因为多了 .id 就改口径。
        ev = _Event(id="SOMETHING")
        self.assertIsNone(platform_compat.platform_message_id(ev))
        self.assertIsNone(platform_compat.platform_message_id(ev, adapter_name="onebotv11"))

    def test_blank_values_are_none(self):
        self.assertIsNone(platform_compat.platform_message_id(_Event(message_id="")))
        self.assertIsNone(platform_compat.platform_message_id(_Event(message_id=None)))
        self.assertIsNone(platform_compat.platform_message_id(None))


class DedupeTests(unittest.TestCase):
    def setUp(self):
        platform_compat.reset_inbound_dedupe()

    def test_first_claim_wins_and_repeat_is_dropped(self):
        self.assertTrue(platform_compat.claim_inbound("main", "qq", "EVT_1"))
        self.assertFalse(platform_compat.claim_inbound("main", "qq", "EVT_1"))

    def test_scopes_are_independent(self):
        # perception 与主 handler 各自认领一次:同一事件必须两条路径都能走。
        self.assertTrue(platform_compat.claim_inbound("perception", "qq", "EVT_1"))
        self.assertTrue(platform_compat.claim_inbound("main", "qq", "EVT_1"))
        self.assertFalse(platform_compat.claim_inbound("perception", "qq", "EVT_1"))

    def test_other_adapters_are_never_deduped(self):
        for _ in range(3):
            self.assertTrue(platform_compat.claim_inbound("main", "onebotv11", "42"))

    def test_missing_id_never_dedupes(self):
        self.assertTrue(platform_compat.claim_inbound("main", "qq", None))
        self.assertTrue(platform_compat.claim_inbound("main", "qq", None))

    def test_ttl_expiry_releases_the_key(self):
        with patch.object(platform_compat, "_DEDUPE_TTL_S", -1.0):
            # TTL 为负 = 写入即过期:第二次必须能重新认领(不残留永久黑名单)。
            self.assertTrue(platform_compat.claim_inbound("main", "qq", "EVT_2"))
            self.assertTrue(platform_compat.claim_inbound("main", "qq", "EVT_2"))

    def test_capacity_is_bounded(self):
        with patch.object(platform_compat, "_DEDUPE_MAX", 4):
            for i in range(10):
                platform_compat.claim_inbound("main", "qq", f"EVT_{i}")
        self.assertLessEqual(len(platform_compat._seen_inbound()), 4)


class CapabilityTests(unittest.TestCase):
    def test_official_qq_marks_onebot_only_features_unsupported(self):
        caps = platform_compat.capabilities("qq")
        for field in (
            "poke", "recall", "group_files", "emoji_reaction", "file_upload",
            "forward_expand", "approval_notify", "sticker_library",
            "integer_message_id", "at_markup",
            # 群全量消息里的「@ 机器人」由平台 mentions.is_you 给出,正文回填
            # 的 @<appid> 是内部标识,不是可敲的用户账号。
        ):
            self.assertFalse(getattr(caps, field), field)

    def test_onebot_and_unknown_keep_legacy_capabilities(self):
        for name in ("onebotv11", "OneBot V11", "astrbot", None, ""):
            caps = platform_compat.capabilities(name)
            self.assertTrue(caps.poke, name)
            self.assertTrue(caps.recall, name)
            self.assertTrue(caps.at_markup, name)

    def test_adapter_name_of_bot_uses_adapter_get_name(self):
        bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        self.assertEqual(platform_compat.adapter_name_of(bot), "qq")


class ReplyModelTests(unittest.TestCase):
    def _attachment(self, content_type, url):
        return SimpleNamespace(content_type=content_type, url=url)

    def test_message_shaped_reply_returns_none(self):
        # OneBot 的 event.reply 是 Message(带 .message),走旧路径。
        reply = SimpleNamespace(message=object(), message_id=7)
        self.assertIsNone(platform_compat.reply_content(reply))

    def test_official_model_reply_yields_text_and_images(self):
        reply = SimpleNamespace(
            content=" 原消息内容 ",
            attachments=[
                self._attachment("image/jpeg", "https://example.invalid/q.jpg"),
                self._attachment("file/pdf", "https://example.invalid/q.pdf"),
                self._attachment("image/png", ""),
            ],
        )
        self.assertEqual(
            platform_compat.reply_content(reply),
            ("原消息内容", ["https://example.invalid/q.jpg"]),
        )

    def test_reply_id_prefers_platform_fields(self):
        self.assertEqual(
            platform_compat.reply_message_id(SimpleNamespace(msg_idx="IDX0")), "IDX0"
        )
        # OneBot 的 message_id 原样透传(int 仍是 int,旧调用方按原类型比较)。
        self.assertEqual(
            platform_compat.reply_message_id(SimpleNamespace(message_id=9)), 9
        )
        self.assertIsNone(platform_compat.reply_message_id(None))
        self.assertIsNone(platform_compat.reply_message_id(SimpleNamespace()))

    def test_bot_authored_detection(self):
        human = SimpleNamespace(author=SimpleNamespace(bot=False, member_openid="u1"))
        bot_author = SimpleNamespace(author=SimpleNamespace(bot=True, member_openid="u2"))
        onebot = SimpleNamespace(sender=SimpleNamespace(user_id=123))
        self.assertFalse(platform_compat.is_bot_authored(human, adapter_name="qq"))
        self.assertTrue(platform_compat.is_bot_authored(bot_author, adapter_name="qq"))
        # OneBot 没有 author.bot 语义,旧行为不变。
        self.assertFalse(platform_compat.is_bot_authored(onebot, adapter_name="onebotv11"))


class ReceiptIdTests(unittest.TestCase):
    def test_official_model_receipt_uses_id_field(self):
        receipt = SimpleNamespace(id="MSG_ABC", timestamp=None)
        self.assertEqual(outbound._extract_msg_id(receipt), "MSG_ABC")

    def test_official_model_receipt_without_id_is_not_faked(self):
        receipt = SimpleNamespace(id=None, timestamp=None)
        self.assertIsNone(outbound._extract_msg_id(receipt))

    def test_onebot_dict_and_scalar_paths_unchanged(self):
        self.assertEqual(outbound._extract_msg_id({"message_id": 12345}), "12345")
        self.assertEqual(outbound._extract_msg_id(999), "999")
        self.assertIsNone(outbound._extract_msg_id(None))


class UnsupportedRefusalTests(unittest.TestCase):
    """能力不足必须明确拒绝,不得伪造成功、不得打平台 API。"""

    def test_poke_refused_on_official_qq(self):
        inp = pu.PokeUserInput(adapter="qq", user_id="123456789", group_id="987654321")

        class _Bot:
            calls: list = []

            async def call_api(self, api, **kwargs):  # pragma: no cover - 不应调用
                self.calls.append(api)

        bot = _Bot()
        with patch.object(pu, "get_bot", lambda _sid: bot), patch.object(
            pu, "validate_push_context", lambda **kw: None
        ), patch.object(
            pu,
            "_resolve_target",
            lambda inp, *, bot_registry: (bot, object()),
        ):
            result = asyncio.run(
                pu.poke_user_impl(
                    inp,
                    active_sessions=object(),
                    bot_registry=object(),
                    scope=CallerScope.dev(),
                )
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertIn("unsupported", (result.error or "").lower())
        self.assertEqual(bot.calls, [])

    def test_recall_refused_before_touching_platform(self):
        inp = rm.RecallMessageInput(adapter="qq", message_id="m:1", group_id="g-openid")

        class _Bot:
            calls: list = []

            async def delete_msg(self, **kwargs):  # pragma: no cover - 不应调用
                self.calls.append(kwargs)

        bot = _Bot()
        buffer = SimpleNamespace(
            get_reply_target=lambda *a, **kw: self.fail("不得查询平台回执")
        )
        with patch.object(rm, "get_bot", lambda _sid: bot), patch.object(
            rm, "validate_push_context", lambda **kw: None
        ), patch.object(
            rm,
            "bot_registry",
            SimpleNamespace(get=lambda *a: SimpleNamespace(bot_self_id="BOT")),
            create=True,
        ):
            result = asyncio.run(
                rm.recall_message_impl(
                    inp,
                    active_sessions=object(),
                    bot_registry=SimpleNamespace(
                        get=lambda *a: SimpleNamespace(bot_self_id="BOT")
                    ),
                    scope=CallerScope.dev(),
                    message_buffer=buffer,
                )
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertIn("unsupported", (result.error or "").lower())
        self.assertEqual(bot.calls, [])

    def test_group_files_refused_on_official_qq(self):
        class _Bot:
            calls: list = []

            async def call_api(self, api, **kwargs):  # pragma: no cover - 不应调用
                self.calls.append(api)

        bot = _Bot()
        inp = gf.BrowseGroupFilesInput(adapter="qq", group_id="group-openid")
        result = asyncio.run(
            gf.browse_group_files_impl(inp, bot=bot, scope=CallerScope.dev())
        )
        self.assertFalse(result.ok)
        self.assertIn("unsupported", (result.error or "").lower())
        self.assertEqual(bot.calls, [])

    def test_emoji_receipt_skipped_on_official_qq(self):
        class _Bot:
            adapter = SimpleNamespace(get_name=lambda: "QQ")
            self_id = "BOT_APPID"
            calls: list = []

            async def call_api(self, api, **kwargs):  # pragma: no cover - 不应调用
                self.calls.append(api)

        bot = _Bot()
        ok = asyncio.run(message_feedback._emoji_like_quiet(bot, "EVT", 96))
        self.assertFalse(ok)
        self.assertEqual(bot.calls, [])


class InlineAtMarkupTests(unittest.TestCase):
    """正文 `@QQ号` 内联标记在官方 QQ 上退化为普通文本(数字号不是 openid)。"""

    def _sent_message(self, *, adapter_name: str) -> list:
        captured: list = []

        async def fake_send(msg, *, bot, target, delivery=None):
            captured.append(msg)
            return ["MSG_1"]

        bot = SimpleNamespace(self_id="BOT", adapter=SimpleNamespace(get_name=lambda: adapter_name))
        target = SimpleNamespace(id="g1", private=False)
        with patch.object(outbound, "_send_unimsg", fake_send), patch.object(
            outbound, "load_chat_config", lambda: SimpleNamespace(
                long_reply_forward=False,
            )
        ), patch.object(outbound, "_warn_residual_cq", lambda tokens: None):
            result = asyncio.run(
                outbound.send_text_with_media(
                    bot=bot,
                    target=target,
                    text="@10001 你好",
                    adapter_name=adapter_name,
                )
            )
        self.assertTrue(result.ok)
        return captured

    def test_official_qq_keeps_literal_text(self):
        (msg,) = self._sent_message(adapter_name="qq")
        self.assertFalse(msg.has(outbound.alconna.At))
        self.assertEqual(msg.extract_plain_text(), "@10001 你好")

    def test_onebot_still_exports_mention(self):
        (msg,) = self._sent_message(adapter_name="onebotv11")
        self.assertTrue(msg.has(outbound.alconna.At))
        self.assertEqual([str(a.target) for a in msg[outbound.alconna.At]], ["10001"])


@unittest.skipUnless(HAS_QQ_ADAPTER, "nonebot-adapter-qq 未安装")
class SendErrorDescriptionTests(unittest.TestCase):
    def test_official_action_failed_carries_code_without_secrets(self):
        from nonebot.drivers import Response

        resp = Response(
            400,
            headers={},
            content=json.dumps({"code": 40034, "message": "msg_seq invalid"}).encode(),
        )
        exc = QQActionFailed(resp)
        text = platform_compat.describe_send_error(exc)
        self.assertIn("40034", text)
        self.assertIn("msg_seq", text)
        self.assertNotIn("secret", text.lower())


if __name__ == "__main__":
    unittest.main()
