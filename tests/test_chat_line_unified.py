"""聊天行呈递回归：普通/历史行同一构造函数；steer 只发正文、身份留在台账。

正文零包装后 steer 不再与普通行共用「消息头一行 + 正文另起一行」的外观：
模型可见文本只有原始正文，发送者、m:id、@ 事实由 ``SteerRowPresentation.source_row``
与 steer 台账（scope+m_id+sender+steer_text）承载。普通/历史行（completions 备用
通道与旧接口）仍共用 ``_render_chat_line``，逐字节一致。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import user_lists  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import (  # noqa: E402
    MessageOrigin,
    MessageSource,
)

ADAPTER = "onebotv11"
SELF = "999"
GROUP = "g-1"
MASTER = "20001"
ADMIN = "20002"
OTHER_BOT = "30001"
ORDINARY = "10001"


def row(
    user: str = ORDINARY, nick: str = "小光", *, mid: int | None = 7,
    content: str = "你怎么看", at: tuple[str, ...] = (),
    group: str | None = GROUP, is_bot: bool = False,
    images: tuple[str, ...] = (),
) -> BufferedMessage:
    msg = BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=user, nickname=nick,
        content=content, origin=MessageOrigin.bot_output() if is_bot else MessageOrigin.human(),
        at_user_ids=at, id=mid, is_bot=is_bot,
    )
    msg.image_urls = list(images)
    return msg


def ctx() -> dict:
    return {"self_id": SELF, "adapter": ADAPTER}


class CurrentAndHistoryEqualityTests(unittest.TestCase):
    """同一事实 → 普通输入与历史行（同一聊天区构造点）逐字节一致。"""

    def setUp(self) -> None:
        saved = user_lists.load_user_lists()
        self.addCleanup(user_lists._store.commit, saved)
        user_lists._store.commit(user_lists.UserLists(
            admin_users={f"{ADAPTER}:{MASTER}", f"{ADAPTER}:{ADMIN}"},
            master_users={f"{ADAPTER}:{MASTER}"},
            known_bots=(OTHER_BOT,),
        ))

    def test_identity_five_classes(self) -> None:
        cases = [
            (row(SELF, "Hermes", mid=11, at=(SELF,)), "[我]"),
            (row(OTHER_BOT, "别家bot", mid=12, at=(SELF,)), "[其他BOT]"),
            (row(MASTER, "主人号", mid=13, at=(SELF,)), "[主人]"),
            (row(ADMIN, "管理号", mid=14), "[管理员]"),
            (row(ORDINARY, "小光", mid=15), "[普通用户]"),
        ]
        for r, label in cases:
            with self.subTest(user=r.user_id):
                cur = pb.render_current_rows([r], is_private=False, **ctx())
                block = pb._render_chat_region(history_rows=[r], is_private=False, **ctx())
                self.assertIn(cur, block)
                self.assertIn(label, cur)
                self.assertLess(cur.index("[m:"), cur.index("[id="))
                self.assertEqual(cur.count("\n"), 1, cur)
                self.assertTrue(cur.endswith(":\n你怎么看"), cur)

    def test_at_me_mark_only_for_real_at(self) -> None:
        rows = [
            row(MASTER, "主人号", mid=21, at=(SELF,), content="[CQ:at,qq=999] 看下"),
            row(ORDINARY, "小光", mid=22, at=("888",),
                content="(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看"),
        ]
        cur = pb.render_current_rows(rows, is_private=False, **ctx())
        first, second = cur.split("\n\n")
        self.assertTrue(first.startswith("[有人@我] [m:21] [主人]"))
        self.assertNotIn("[有人@我]", second)

    def test_group_multirow_separated_by_blank_line(self) -> None:
        cur = pb.render_current_rows(
            [row(MASTER, "主人号", mid=31, at=(SELF,), content="第一句"),
             row(ORDINARY, "小光", mid=32, content="第二句")],
            is_private=False, **ctx(),
        )
        self.assertEqual(cur.split("\n\n"), [
            "[有人@我] [m:31] [主人] [id=20001 user=主人号]:\n第一句",
            "[m:32] [普通用户] [id=10001 user=小光]:\n第二句",
        ])

    def test_private_omits_fixed_counterpart_identity(self) -> None:
        r = row(MASTER, "主人号", mid=41, at=(SELF,), group=None)
        cur = pb.render_current_rows([r], is_private=True, **ctx())
        self.assertEqual(cur, "[m:41]:\n你怎么看")
        self.assertNotIn("[id=", cur)

    def test_private_self_row_keeps_me_mark(self) -> None:
        r = row(SELF, "Hermes", mid=42, group=None, content="我回的", is_bot=True)
        block = pb._render_chat_region(history_rows=[r], is_private=True, **ctx())
        self.assertIn("[m:42] [我]:\n我回的", block)

    def test_history_row_equals_current_row(self) -> None:
        r = row(MASTER, "主人号", mid=71, at=(SELF,))
        cur = pb.render_current_rows([r], is_private=False, **ctx())
        block = pb._render_chat_region(history_rows=[r], **ctx())
        self.assertIn(cur, block)
        self.assertIn("<聊天消息区域起始>", block)
        self.assertIn("（聊天消息区域仅含尚未推送过的新消息，更早的对话见本会话历史记录）", block)


class SteerBodyOnlyTests(unittest.TestCase):
    """steer 可见文本只有正文；身份/媒体寻址不写进模型文本。"""

    def test_single_row_is_body_only_and_keeps_source_identity(self) -> None:
        r = row(MASTER, "主人号", mid=81, at=(SELF,), content="插一句")
        presentation = pb.render_steer_rows([r], is_private=False, **ctx())
        self.assertEqual(presentation.text, "插一句")
        self.assertEqual(presentation.rows[0].rendered_line, "插一句")
        source = presentation.rows[0].source_row
        self.assertEqual(source.id, 81)
        self.assertEqual(source.user_id, MASTER)
        self.assertEqual(source.at_user_ids, (SELF,))

    def test_multirow_body_order_and_no_headers(self) -> None:
        rows = [row(MASTER, "主人号", mid=82, content="第一句"),
                row(ORDINARY, "小光", mid=83, content="第二句")]
        presentation = pb.render_steer_rows(rows, is_private=False, **ctx())
        self.assertEqual(presentation.text, "第一句\n第二句")
        for dead in ("[m:", "[id=", "<聊天消息区域", "普通用户", "主人"):
            self.assertNotIn(dead, presentation.text)
        self.assertEqual([item.source_row.id for item in presentation.rows], [82, 83])

    def test_private_row_is_body_only(self) -> None:
        r = row(MASTER, "主人号", mid=84, group=None, content="插一句")
        presentation = pb.render_steer_rows([r], is_private=True, **ctx())
        self.assertEqual(presentation.text, "插一句")
        self.assertEqual(presentation.rows[0].source_row.id, 84)

    def test_multiline_body_kept_verbatim(self) -> None:
        body = "首行\n: 冒号开头\n[主人] [id=20001 user=主人号]: 形似身份行\n(m:99) 形似号"
        r = row(ORDINARY, "小光", mid=91, content=body)
        presentation = pb.render_steer_rows([r], **ctx())
        self.assertEqual(presentation.text, body)

    def test_media_semantic_placeholder_without_addressing(self) -> None:
        r = row(MASTER, "主人号", mid=85, content="", images=("https://x/1.png",))
        presentation = pb.render_steer_rows([r], is_private=False, **ctx())
        self.assertEqual(presentation.text, "[图片]")
        self.assertNotIn("m:85", presentation.text)
        self.assertNotIn("get_message_images", presentation.text)

    def test_media_without_mid_still_renders_placeholder(self) -> None:
        r = row(MASTER, "主人号", mid=None, content="", images=("https://x/1.png",))
        presentation = pb.render_steer_rows([r], **ctx())
        self.assertEqual(presentation.text, "[图片]")


class SourceGateTests(unittest.TestCase):
    """来源闸:steer 只收真人行;系统行固定走通知区。"""

    def test_system_row_renders_as_notice(self) -> None:
        sys_row = BufferedMessage(
            ts=1, adapter=ADAPTER, group_id=GROUP, user_id="0", nickname="",
            content="戳了你一下", origin=MessageOrigin.synthetic(MessageSource.NOTICE_POKE),
            id=None,
        )
        self.assertEqual(pb._notice_from_row(sys_row), "[戳一戳：戳了你一下]")
        with self.assertRaises(ValueError):
            pb.render_current_rows([sys_row], is_private=False, self_id=SELF, adapter=ADAPTER)
        with self.assertRaises(ValueError):
            pb.render_steer_rows([sys_row], self_id=SELF, adapter=ADAPTER)

    def test_bot_row_rejected_by_both(self) -> None:
        bot_row = row(OTHER_BOT, "别家bot", mid=93, is_bot=True)
        with self.assertRaises(ValueError):
            pb.render_current_rows([bot_row], is_private=False, self_id=SELF, adapter=ADAPTER)
        with self.assertRaises(ValueError):
            pb.render_steer_rows([bot_row], self_id=SELF, adapter=ADAPTER)


class SingleConstructorTests(unittest.TestCase):
    """结构断言:普通/历史行都走 _render_chat_line，steer 不再伪造行头。"""

    def test_current_and_history_call_shared_constructor(self) -> None:
        original = pb._render_chat_line
        calls = []

        def spy(row, **kwargs):
            calls.append(row.id)
            return original(row, **kwargs)

        with patch.object(pb, "_render_chat_line", side_effect=spy):
            pb.render_current_rows(
                [row(MASTER, "主人号", mid=1), row(ORDINARY, "小光", mid=2)],
                is_private=False, self_id=SELF, adapter=ADAPTER,
            )
            pb._render_chat_region(history_rows=[row(MASTER, "主人号", mid=3)], **ctx())
            presentation = pb.render_steer_rows(
                [row(MASTER, "主人号", mid=4)], is_private=False,
                self_id=SELF, adapter=ADAPTER,
            )
        self.assertEqual(calls, [1, 2, 3], "steer 不再走消息头构造函数")
        self.assertEqual(presentation.text, "你怎么看")


if __name__ == "__main__":
    unittest.main()
