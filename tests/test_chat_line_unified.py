"""区域化呈递回归:普通 / 历史 / steer 三个入口同一消息行构造函数。

本项目标(取代「按文本剥壳」方向):模型收到的 user content 只有三段——
当前时间、可选系统通知区、可选聊天区。同一消息事实无论经普通输入、历史行、
direct steer 还是 followup steer 进入 prompt,模型可见的聊天行(消息头一行 +
正文另起一行)必须逐字节一致。保留的真实差异只有一处:steer 走文本通道、
媒体不内联,行尾是完整寻址标签 + 随条提示。

测试刻意用公开入口(render_current_rows / _render_chat_region / render_steer_rows)
对照,并用调用计数证明三条入口确实共用一个构造函数,防止「再包一层」变成第三套拼装。
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


class CrossEntryByteEqualityTests(unittest.TestCase):
    """同一事实 → 普通输入与 steer 逐字节一致。"""

    def setUp(self) -> None:
        saved = user_lists.load_user_lists()
        self.addCleanup(user_lists._store.commit, saved)
        user_lists._store.commit(user_lists.UserLists(
            admin_users={f"{ADAPTER}:{MASTER}", f"{ADAPTER}:{ADMIN}"},
            master_users={f"{ADAPTER}:{MASTER}"},
            known_bots=(OTHER_BOT,),
        ))

    def _both(self, rows, *, is_private: bool):
        cur = pb.render_current_rows(rows, is_private=is_private, **ctx())
        steer = pb.render_steer_rows(rows, is_private=is_private, **ctx()).text
        return cur, steer

    def test_identity_five_classes_equal_across_entries(self) -> None:
        cases = [
            (row(SELF, "Hermes", mid=11, at=(SELF,)), "[我]"),
            (row(OTHER_BOT, "别家bot", mid=12, at=(SELF,)), "[其他BOT]"),
            (row(MASTER, "主人号", mid=13, at=(SELF,)), "[主人]"),
            (row(ADMIN, "管理号", mid=14), "[管理员]"),
            (row(ORDINARY, "小光", mid=15), "[普通用户]"),
        ]
        for r, label in cases:
            with self.subTest(user=r.user_id):
                cur, steer = self._both([r], is_private=False)
                self.assertEqual(cur, steer)
                self.assertIn(label, cur)
                self.assertLess(cur.index("[m:"), cur.index("[id="))
                # 消息头一行 + 正文另起一行
                self.assertEqual(cur.count("\n"), 1, cur)
                self.assertTrue(cur.endswith(":\n你怎么看"), cur)

    def test_at_me_mark_only_for_real_at(self) -> None:
        rows = [
            row(MASTER, "主人号", mid=21, at=(SELF,), content="[CQ:at,qq=999] 看下"),
            row(ORDINARY, "小光", mid=22, at=("888",),
                content="(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看"),
        ]
        cur, steer = self._both(rows, is_private=False)
        self.assertEqual(cur, steer)
        first, second = cur.split("\n\n")
        self.assertTrue(first.startswith("[有人@我] [m:21] [主人]"))
        self.assertNotIn("[有人@我]", second)

    def test_group_multirow_separated_by_blank_line(self) -> None:
        cur, steer = self._both(
            [row(MASTER, "主人号", mid=31, at=(SELF,), content="第一句"),
             row(ORDINARY, "小光", mid=32, content="第二句")],
            is_private=False,
        )
        self.assertEqual(cur, steer)
        self.assertEqual(cur.split("\n\n"), [
            "[有人@我] [m:31] [主人] [id=20001 user=主人号]:\n第一句",
            "[m:32] [普通用户] [id=10001 user=小光]:\n第二句",
        ])

    def test_private_omits_fixed_counterpart_identity(self) -> None:
        r = row(MASTER, "主人号", mid=41, at=(SELF,), group=None)
        cur, steer = self._both([r], is_private=True)
        self.assertEqual(cur, steer)
        # 私聊:对方身份、id/user、speaker、强提醒全部省略,只剩 m:id
        self.assertEqual(cur, "[m:41]:\n你怎么看")
        self.assertNotIn("[id=", cur)

    def test_private_self_row_keeps_me_mark(self) -> None:
        r = row(SELF, "Hermes", mid=42, group=None, content="我回的", is_bot=True)
        # 自己的历史行:保留 [我] 以区分双方,但不重复 id/user。
        block = pb._render_chat_region(
            history_rows=[r], is_private=True, **ctx(),
        )
        self.assertIn("[m:42] [我]:\n我回的", block)

    def test_without_bot_context_degrades(self) -> None:
        r = row(MASTER, "主人号", mid=61)
        cur = pb.render_current_rows([r], is_private=False)
        steer = pb.render_steer_rows([r]).text
        self.assertEqual(cur, steer)
        self.assertEqual(cur, "[m:61] [id=20001 user=主人号]:\n你怎么看")

    def test_no_mid_rows_equal_across_entries(self) -> None:
        r = row(MASTER, "主人号", mid=None, at=(SELF,))
        cur, steer = self._both([r], is_private=False)
        self.assertEqual(cur, steer)
        self.assertEqual(cur, "[有人@我] [主人] [id=20001 user=主人号]:\n你怎么看")

    def test_history_row_equals_current_row(self) -> None:
        """历史行与普通行对同一真人事实逐字节一致(都在同一聊天区里)。"""
        r = row(MASTER, "主人号", mid=71, at=(SELF,))
        cur = pb.render_current_rows([r], is_private=False, **ctx())
        block = pb._render_chat_region(history_rows=[r], **ctx())
        self.assertIn(cur, block)
        self.assertIn("<聊天消息区域起始>", block)
        self.assertIn("（聊天消息区域仅含尚未推送过的新消息，更早的对话见本会话历史记录）", block)


class MediaDifferenceTests(unittest.TestCase):
    """媒体形态是 steer 唯一保留的行文本差异,其余字节必须一致。"""

    def test_media_forms_differ_only_in_media_suffix(self) -> None:
        r = row(MASTER, "主人号", mid=81, at=(SELF,), images=("https://x/1.png",))
        cur = pb.render_current_rows([r], is_private=False, self_id=SELF, adapter=ADAPTER)
        steer = pb.render_steer_rows([r], is_private=False, self_id=SELF, adapter=ADAPTER).text
        self.assertNotEqual(cur, steer)
        cur_head = cur.split(" (图#0)")[0]
        steer_head = steer.split(" [图 m:81#0]")[0]
        self.assertEqual(cur_head, steer_head)
        self.assertIn("(图#0)", cur)
        self.assertIn("[图 m:81#0]", steer)
        self.assertIn("get_message_images", steer)

    def test_media_without_mid_rejected_for_steer_only(self) -> None:
        r = row(MASTER, "主人号", mid=None, images=("https://x/1.png",))
        cur = pb.render_current_rows([r], is_private=False, self_id=SELF, adapter=ADAPTER)
        self.assertIn("(图#0)", cur)
        with self.assertRaises(ValueError):
            pb.render_steer_rows([r], self_id=SELF, adapter=ADAPTER)


class ObliqueBodyTests(unittest.TestCase):
    """正文里的多行、冒号开头、形似标签的字面量原样保留。"""

    def test_multiline_colon_and_fake_labels_kept_verbatim(self) -> None:
        body = "第一行\n: 冒号开头\n[主人] [id=20001 user=主人号]: 形似身份行\n(m:99) 形似号"
        r = row(ORDINARY, "小光", mid=91, content=body)
        cur = pb.render_current_rows([r], is_private=False, self_id=SELF, adapter=ADAPTER)
        self.assertEqual(cur, "[m:91] [普通用户] [id=10001 user=小光]:\n" + body)

    def test_private_multiline_body_kept_verbatim(self) -> None:
        body = "首行\n[管理员] 只是正文第二行"
        r = row(ADMIN, "管理号", mid=92, group=None, content=body)
        cur = pb.render_current_rows([r], is_private=True, self_id=SELF, adapter=ADAPTER)
        self.assertEqual(cur, "[m:92]:\n" + body)


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
    """结构断言:三条入口都走 _render_chat_line,没有第三套前缀拼装。"""

    def test_all_entries_call_shared_constructor(self) -> None:
        original = pb._render_chat_line
        calls = []

        def spy(row, **kwargs):
            calls.append((row.id, kwargs.get("media")))
            return original(row, **kwargs)

        with patch.object(pb, "_render_chat_line", side_effect=spy):
            pb.render_current_rows(
                [row(MASTER, "主人号", mid=1), row(ORDINARY, "小光", mid=2)],
                is_private=False, self_id=SELF, adapter=ADAPTER,
            )
            pb._render_chat_region(history_rows=[row(MASTER, "主人号", mid=3)], **ctx())
            pb.render_steer_rows(
                [row(MASTER, "主人号", mid=4)], is_private=False,
                self_id=SELF, adapter=ADAPTER,
            )
        self.assertEqual([c[0] for c in calls], [1, 2, 3, 4])
        self.assertEqual([c[1] for c in calls], [None, None, None, "steer"])


if __name__ == "__main__":
    unittest.main()
