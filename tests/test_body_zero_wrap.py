"""正文零包装第一阶段：user content 只留正文与媒体，动态事实走请求级上下文。

覆盖主人 2026-11-27 定稿：
  1. 可见 user content = 真实消息正文 + 媒体；时间／区域标记／固定说明／发送者与 m:id
     行头／系统通知／批尾提醒全部移出；
  2. 多条消息保持原顺序，消息间不补空行，原消息自身换行保留；
  3. 引用原文与 [图片]／[表情包] 语义占位保留；
  4. 当前时间、有序「第几条 → 发送者 → m:id」、pinned tasks、媒体寻址进 request_context；
  5. 静态输入说明／标记含义／表情判读／输出契约进 system prompt，清理旧区域说法；
  6. runs 纯文本与多模态 parts 都零包装；completions 旧路径不动（明确未覆盖）。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import (  # noqa: E402
    MessageOrigin,
    MessageSource,
)

ADAPTER = "onebotv11"
SELF = "999"
GROUP = "g-1"
FIXED_TIME = "2026-11-27 周五 12:00"


def human(user="10001", nick="小光", *, mid=11, content="你怎么看", group: str | None = GROUP,
          at=(), reply_to_ts=None, images=(), stickers=(), forward="") -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=user, nickname=nick,
        content=content, origin=MessageOrigin.human(), at_user_ids=at, id=mid,
        reply_to_ts=reply_to_ts, image_urls=list(images), sticker_urls=list(stickers),
        forward_content=forward,
    )


def bot(*, mid=12, content="我在", group: str | None = GROUP) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=SELF, nickname="Hermes",
        content=content, origin=MessageOrigin.bot_output(), id=mid, is_bot=True,
    )


def notice(content="提醒：十点开会", *, label="跨会话通知", group: str | None = GROUP) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id="0", nickname="",
        content=content, origin=MessageOrigin.synthetic(MessageSource.WAKE_MANUAL, label=label),
        id=None,
    )


class _Interface:
    def __init__(self, test, mode: str) -> None:
        self.test = test
        self.mode = mode

    def __enter__(self):
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", self.mode)
        self.test.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)
        return self

    def __exit__(self, *exc):
        return False


def build_reactive(rows, recent=(), *, images=(), stickers=(), pinned=(), addressed=False):
    return pb.build_reactive_user_content(
        addressed_to_bot=addressed, recent_messages=list(recent), rows=rows,
        current_image_urls=list(images), current_sticker_urls=list(stickers),
        pinned_anchors=list(pinned), self_id=SELF, adapter=ADAPTER,
    )


def build_passive(rows, recent=(), *, images=(), stickers=(), private=True):
    return pb.build_passive_user_content(
        recent_messages=list(recent), rows=rows,
        current_image_urls=list(images), current_sticker_urls=list(stickers),
        is_private=private, self_id=SELF, adapter=ADAPTER,
    )


class _TimePatched(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch.object(pb, "_current_time_str", return_value=FIXED_TIME)
        patcher.start()
        self.addCleanup(patcher.stop)


class ZeroWrapContentTests(_TimePatched):
    def test_recent_bodies_stay_in_order_in_request_context(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive(
                [notice()],
                recent=[human(mid=13, content="好"), bot(mid=12), human(mid=11, content="你怎么看")],
            )
        self.assertEqual(out.content, " ")
        for dead in (
            "当前时间", "<聊天消息区域起始>", "<聊天消息区域结束>",
            "聊天消息区域仅含", "[m:", "id=10001", "跨会话通知", "§",
        ):
            self.assertNotIn(dead, out.content, dead)
        self.assertIn("最近消息", out.request_context)
        positions = [out.request_context.index(text) for text in ("你怎么看", "我在", "好")]
        self.assertEqual(positions, sorted(positions), "recent 仍按旧→新顺序")

    def test_multiple_messages_use_single_newline_and_keep_inner_newlines(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive([
                human(mid=21, content="第一行\n第二行"),
                human(mid=22, content="下一句"),
            ])
        self.assertEqual(out.content, "第一行\n第二行\n下一句")
        self.assertNotIn("\n\n", out.content)

    def test_multiline_index_preserves_body_ownership(self) -> None:
        def render(first: str, second: str):
            with _Interface(self, "runs"):
                return build_reactive([
                    human(user="10001", mid=51, content=first),
                    human(user="10002", mid=52, content=second),
                ])

        left = render("第一行\n第二行", "第三行")
        right = render("第一行", "第二行\n第三行")
        self.assertEqual(left.content, right.content, "对照仅改变两条正文的归属边界")
        self.assertNotEqual(left.request_context, right.request_context)
        self.assertIn("第1条 → 普通用户 [id=10001 user=小光] [m:51]；正文第1-2行", left.request_context)
        self.assertIn("第2条 → 普通用户 [id=10002 user=小光] [m:52]；正文第3行", left.request_context)
        self.assertIn("第1条 → 普通用户 [id=10001 user=小光] [m:51]；正文第1行", right.request_context)
        self.assertIn("第2条 → 普通用户 [id=10002 user=小光] [m:52]；正文第2-3行", right.request_context)

    def test_recent_messages_move_to_request_context(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive(
                [human(mid=22, content="当前正文")],
                recent=[human(mid=21, content="最近正文")],
            )
        self.assertEqual(out.content, "当前正文")
        self.assertNotIn("最近正文", out.content)
        self.assertIn("最近消息", out.request_context)
        self.assertIn("最近正文", out.request_context)
        self.assertIn("[m:21]", out.request_context)

    def test_recent_forward_keeps_latest_expansion_rule(self) -> None:
        older = human(mid=19, content="<forwarded_messages count=\"1\"/>", forward="旧折叠全文")
        newest = human(mid=20, content="<forwarded_messages count=\"1\"/>", forward="新折叠全文")
        with _Interface(self, "runs"):
            out = build_reactive([human(mid=21, content="当前")], recent=[newest, older])
        self.assertEqual(out.content, "当前")
        self.assertIn("最近消息", out.request_context)
        self.assertIn("<forwarded_messages count=\"1\"/>", out.request_context)
        self.assertIn("新折叠全文", out.request_context)
        self.assertNotIn("旧折叠全文", out.request_context)

    def test_current_bot_row_still_uses_source_gate(self) -> None:
        with _Interface(self, "runs"):
            with self.assertRaisesRegex(ValueError, "bot 消息不得作为当前输入"):
                build_reactive([bot(mid=25)])

    def test_quote_text_is_kept_verbatim(self) -> None:
        quoted = "(引用 合成用户(10002): 合成引文) 当前消息原文"
        with _Interface(self, "runs"):
            out = build_reactive([human(mid=23, content=quoted)])
        self.assertEqual(out.content, quoted)

    def test_file_ref_leaves_body_but_remains_addressable(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers.message_media import _collect_file_placeholders

        entries = [{"name": "例.pdf"}]
        row = human(mid=51, content=" ".join(_collect_file_placeholders(entries, 51)))
        row.file_entries = entries
        with _Interface(self, "runs"):
            out = build_reactive([row])
        self.assertEqual(out.content, "[文件:例.pdf]")
        self.assertNotIn("#f0", out.content)
        self.assertIn("[m:51]#f0", out.request_context)
        self.assertIn("get_message_files", out.request_context)

    def test_video_ref_leaves_body_but_remains_addressable(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers.message_media import _collect_file_placeholders

        entries = [{"kind": "video", "name": "例.mp4"}]
        row = human(mid=52, content=" ".join(_collect_file_placeholders(entries, 52)))
        row.file_entries = entries
        with _Interface(self, "runs"):
            out = build_reactive([row])
        self.assertEqual(out.content, "[视频]")
        self.assertNotIn("#f0", out.content)
        self.assertIn("[m:52]#f0", out.request_context)
        self.assertIn("get_message_files", out.request_context)

    def test_media_placeholder_stays_but_addressing_moves_out(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive([human(mid=24, content="[图片]", images=["data:image/png;base64,IMG"])])
        self.assertEqual(out.content, "[图片]")
        self.assertNotIn("图#0", out.content)
        self.assertIn("第1条 [m:24]#0", out.request_context)

    def test_private_content_and_index_keep_sender_facts(self) -> None:
        with _Interface(self, "runs"):
            out = build_passive(
                [notice(group=None)],
                recent=[bot(group=None, mid=32, content="在"),
                        human(group=None, mid=31, content="在吗")],
                private=True,
            )
        self.assertEqual(out.content, " ")
        index = out.request_context.split("最近消息", 1)[1]
        self.assertIn("第1条 → 普通用户 [id=10001 user=小光] [m:31]", index)
        self.assertIn("第2条 → 我 [id=999 user=Hermes] [m:32]", index)
        self.assertIn("在吗", index)
        self.assertIn("在", index)

    def test_at_me_mark_moves_to_index(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive([human(mid=41, content="11 @999", at=(SELF,))])
        self.assertEqual(out.content, "11 @999")
        self.assertIn("第1条 → [有人@我] 普通用户 [id=10001 user=小光] [m:41]", out.request_context)

    def test_request_context_has_time_index_pinned_and_media(self) -> None:
        anchor = type("A", (), {"commissioner_id": "10001", "commissioner_nickname": "小光",
                                "text": "把这件事办了", "ts": 0, "m_id": 77})()
        with _Interface(self, "runs"):
            out = build_reactive(
                [human(mid=51, content="[图片]", images=["data:image/png;base64,IMG"])],
                pinned=[anchor],
            )
        rc = out.request_context
        self.assertTrue(rc.startswith(f"（当前时间：{FIXED_TIME}）"), rc)
        self.assertIn("本轮消息索引", rc)
        self.assertIn("第1条 → 普通用户 [id=10001 user=小光] [m:51]", rc)
        self.assertIn("<pinned_tasks>", rc)
        self.assertIn("[m:77]", rc)
        self.assertIn("媒体：第1条 [m:51]#0 图", rc)
        self.assertIn("get_message_images", rc)
        self.assertEqual(rc.count("第1条"), 2, "索引与媒体各一处")

    def test_notice_only_turn_uses_space_placeholder_and_notice_goes_to_context(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive([notice()])
        self.assertEqual(out.content, " ")
        self.assertIn("跨会话通知", out.context_notices)
        self.assertNotIn("跨会话通知", out.content)

    def test_reactive_reminder_moves_to_closing_field(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive([human(), notice()])
        self.assertTrue(out.closing_reminder.startswith("\n\n请直接输出正文"))
        self.assertNotIn("请直接输出正文", out.content)
        self.assertNotIn("请直接输出正文", out.request_context)

    def test_reactive_reminder_without_notice_still_moves_to_closing_field(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive([human(content="普通一句")])
        self.assertTrue(out.closing_reminder.startswith("\n\n请直接输出正文"))
        self.assertNotIn("请直接输出正文", out.content)
        self.assertNotIn("请直接输出正文", out.request_context)

    def test_sticker_mood_note_is_not_appended_to_content(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive(
                [human(mid=61, content="[表情包]", stickers=["data:image/gif;base64,R0lG"])],
            )
        self.assertEqual(out.content, "[表情包]")
        self.assertNotIn("仅作情绪", out.content)


class CurrentFileEntryWiringTests(_TimePatched):
    def test_real_fragment_carries_file_and_video_addressing_out_of_body(self):
        from nonebot_plugin_hermes_adapter.handlers import message as message_mod
        from nonebot_plugin_hermes_adapter.handlers.message_media import _collect_file_placeholders

        own = [{"kind": "file", "name": "例.pdf"}]
        forwarded = [{"kind": "video", "name": "例.mp4"}]
        entries = own + forwarded
        text = "看这个 " + " ".join(_collect_file_placeholders(entries, 51))
        with patch.object(message_mod, "_peek_event_db_id", return_value=51):
            fragment = message_mod._build_current_fragment(
                now_ms=1000, adapter_name=ADAPTER, group_id=GROUP, user_id="10001",
                nickname="小光", msg_text=text,
                own_image_urls=[], sticker_urls=[], quote_image_urls=[], forward_image_urls=[],
                own_file_entries=own, forward_video_entries=forwarded,
                delivery_generation=1, event_msg_id="ev-1", at_user_ids=(),
            )
        self.assertEqual(fragment.file_entries, entries)
        with _Interface(self, "runs"):
            reactive = build_reactive([fragment])
            passive = build_passive([fragment])
        for out in (reactive, passive):
            self.assertEqual(out.content, "看这个 [文件:例.pdf] [视频]")
            self.assertNotIn("#f0", out.content)
            self.assertNotIn("#f1", out.content)
            self.assertIn("[m:51]#f0", out.request_context)
            self.assertIn("[m:51]#f1", out.request_context)
            self.assertIn("get_message_files", out.request_context)
        steer = pb.render_steer_rows([fragment], self_id=SELF, adapter=ADAPTER)
        self.assertEqual(steer.text, "看这个 [文件:例.pdf] [视频]")
        self.assertNotIn("#f0", steer.text)
        self.assertNotIn("#f1", steer.text)


class ZeroWrapMultimodalTests(_TimePatched):
    def test_parts_keep_body_then_media_without_labels(self) -> None:
        with _Interface(self, "runs"):
            out = build_reactive(
                [human(mid=71, content="看这张"), notice()],
                images=[(("image", 71, 0), ["data:image/png;base64,IMG"])],
            )
        self.assertIsInstance(out.content, list)
        self.assertEqual(out.content[0], {"type": "text", "text": "看这张"})
        self.assertEqual(
            [part["type"] for part in out.content],
            ["text", "image_url"],
            "零包装 parts 不再贴 [图 m:id#idx] 标签",
        )
        self.assertTrue(out.context_notices.startswith("<系统通知区域起始>"))
        self.assertNotIn("系统通知", out.content[0]["text"])
        self.assertIn("第1条 [m:71]#0", out.request_context)

    def test_parts_still_carry_request_context_field(self) -> None:
        with _Interface(self, "runs"):
            out = build_passive(
                [human(group=None, mid=72, content="图")],
                images=[(("image", 72, 0), ["data:image/png;base64,IMG"])],
            )
        self.assertTrue(out.request_context.startswith("（当前时间："))
        self.assertIn("[m:72]", out.request_context)


class StaticPromptTests(unittest.TestCase):
    def test_runs_protocol_describes_new_input_and_drops_old_regions(self) -> None:
        with _Interface(self, "runs"):
            private = pb._private_protocol_block()
            group = pb._reactive_protocol_block()
        for block in (private, group):
            self.assertIn("消息正文", block)
            self.assertIn("消息索引", block)
            self.assertIn("第几条 → 发送者 → m:id", block)
            self.assertIn("[图片]", block)
            self.assertIn("[表情包]", block)
            self.assertIn("[有人@我]", block)
            self.assertIn("get_message_images", block)
            self.assertIn("（a／b）", block)
            self.assertIn("§", block)
            self.assertNotIn("<聊天消息区域起始>", block)
            self.assertNotIn("输入两区", block)
            self.assertNotIn("每条一行 [m:id]", block)
            self.assertNotIn("消息之间空一行", block)

    def test_sticker_mood_rule_is_static(self) -> None:
        with _Interface(self, "runs"):
            block = pb._private_protocol_block()
        self.assertIn("表情包", block)
        self.assertIn("情绪", block)

    def test_completions_keeps_legacy_region_protocol(self) -> None:
        with _Interface(self, "completions"):
            block = pb._reactive_protocol_block()
        self.assertNotIn("本轮消息索引", block)


class LegacyCompletionsTests(_TimePatched):
    def test_completions_content_keeps_legacy_regions(self) -> None:
        with _Interface(self, "completions"):
            out = build_reactive([human(mid=81, content="你怎么看")])
        self.assertIsInstance(out.content, str)
        self.assertIn("<聊天消息区域起始>", out.content)
        self.assertIn("[m:81]", out.content)
        self.assertEqual(out.request_context, "", "备用通道未接入请求级上下文")


if __name__ == "__main__":
    unittest.main()
