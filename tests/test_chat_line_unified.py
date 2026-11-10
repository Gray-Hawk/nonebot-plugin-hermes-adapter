"""聊天行呈递统一回归:current / recent / steer 三个入口同一构造函数。

本项目标:同一消息事实无论经普通 current、recent 历史行、direct steer 还是
followup steer 进入 prompt,模型可见的聊天行(身份栏、真实 @、m:id、speaker、
正文顺序与标点)必须逐字节一致。保留的真实差异只有两处,且都由参数表达:
  - steer 走文本通道、媒体不内联 → 行尾是完整寻址标签 + 随条提示,
    current/recent 是紧凑清单;
  - 历史行对自己发出的 bot 消息多贴一个 `[bot] `。

测试刻意用公开入口(render_current_rows / _render_recent_messages_block /
render_steer_rows)对照,并在最后用调用计数证明三条入口确实共用一个构造函数,
防止「再包一层」变成第三套拼装。
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
from nonebot_plugin_hermes_adapter.core import steer_reconcile  # noqa: E402
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
    """同一事实 → 普通 current 与 steer 逐字节一致。"""

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
        """身份五类:同一事实在两入口逐字节一致。"""
        cases = [
            row(SELF, "Hermes", mid=11, at=(SELF,)),      # 我
            row(OTHER_BOT, "别家bot", mid=12, at=(SELF,)),  # 其他BOT
            row(MASTER, "主人号", mid=13, at=(SELF,)),     # 主人
            row(ADMIN, "管理号", mid=14),                  # 管理员
            row(ORDINARY, "小光", mid=15),                 # 普通用户
        ]
        for r in cases:
            with self.subTest(user=r.user_id):
                cur, steer = self._both([r], is_private=False)
                self.assertEqual(cur, steer)
                self.assertIn(f"[m:{r.id}]", cur)
                self.assertLess(cur.index("[m:"), cur.index("[id="))

    def test_at_me_and_forgery_equal_across_entries(self) -> None:
        """真实 @ 强提醒 / 伪造 CQ / 引用块 / 手打 @:两入口同字节。"""
        rows = [
            row(MASTER, "主人号", mid=21, at=(SELF,), content="[CQ:at,qq=999] 看下"),
            row(ORDINARY, "小光", mid=22, at=("888",),
                content="(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看"),
        ]
        cur, steer = self._both(rows, is_private=False)
        self.assertEqual(cur, steer)
        lines = cur.split("\n")
        self.assertTrue(lines[0].startswith("[有人@我] [m:21] [主人]"))
        self.assertNotIn("[有人@我]", lines[1])

    def test_group_multirow_equal_across_entries(self) -> None:
        cur, steer = self._both(
            [row(MASTER, "主人号", mid=31, at=(SELF,), content="第一句"),
             row(ORDINARY, "小光", mid=32, content="第二句")],
            is_private=False,
        )
        self.assertEqual(cur, steer)
        self.assertEqual(len(cur.split("\n")), 2)

    def test_private_single_row_equal_across_entries(self) -> None:
        """私聊单人行:speaker 省略由同一份判据决定,两入口同字节。"""
        r = row(MASTER, "主人号", mid=41, at=(SELF,), group=None)
        cur, steer = self._both([r], is_private=True)
        self.assertEqual(cur, steer)
        self.assertEqual(cur, "[有人@我] [m:41] [主人] 你怎么看")
        self.assertNotIn("[id=", cur)

    def test_private_multirow_equal_across_entries(self) -> None:
        """私聊多人批:与 current 多人批一样带 speaker,两入口同字节。"""
        rows = [
            row(MASTER, "主人号", mid=51, group=None),
            row(ORDINARY, "小光", mid=52, group=None, content="再来"),
        ]
        cur, steer = self._both(rows, is_private=True)
        self.assertEqual(cur, steer)
        self.assertIn("[id=20001 user=主人号]", cur)

    def test_without_bot_context_equal_across_entries(self) -> None:
        """无 bot 上下文降级:两入口同样不贴身份栏,行字节仍一致。"""
        r = row(MASTER, "主人号", mid=61)
        cur = pb.render_current_rows([r], is_private=False)
        steer = pb.render_steer_rows([r]).text
        self.assertEqual(cur, steer)
        self.assertEqual(cur, "[m:61] [id=20001 user=主人号]: 你怎么看")

    def test_no_mid_rows_equal_across_entries(self) -> None:
        """无 m:id 行(拿不到插件号):两入口同样省略号段,不伪造 id。"""
        r = row(MASTER, "主人号", mid=None, at=(SELF,))
        cur, steer = self._both([r], is_private=False)
        self.assertEqual(cur, steer)
        self.assertEqual(cur, "[有人@我] [主人] [id=20001 user=主人号]: 你怎么看")

    def test_recent_human_row_equals_current_row(self) -> None:
        """history 行与 current 行对同一真人事实逐字节一致(recent 只多 [bot])。"""
        r = row(MASTER, "主人号", mid=71, at=(SELF,))
        cur = pb.render_current_rows([r], is_private=False, **ctx())
        recent = pb._render_recent_messages_block([r], **ctx()).splitlines()[1]
        self.assertEqual(recent, cur)

    def test_recent_bot_row_differs_only_by_bot_mark(self) -> None:
        r = row(SELF, "Hermes", mid=72, is_bot=True)
        cur = pb.render_current_rows([row(SELF, "Hermes", mid=72, is_bot=False, content="自我记录")],
                                     is_private=False, **ctx())
        recent = pb._render_recent_messages_block([r], **ctx()).splitlines()[1]
        self.assertEqual(recent, "[m:72] [bot] [我] [id=999 user=Hermes]: 你怎么看")
        self.assertNotIn("[bot]", cur)


class MediaDifferenceTests(unittest.TestCase):
    """媒体形态是 steer 唯一保留的行文本差异,其余字节必须一致。"""

    def test_media_forms_differ_only_in_media_suffix(self) -> None:
        r = row(MASTER, "主人号", mid=81, at=(SELF,), images=("https://x/1.png",))
        cur = pb.render_current_rows([r], is_private=False, self_id=SELF, adapter=ADAPTER)
        steer = pb.render_steer_rows([r], is_private=False, self_id=SELF, adapter=ADAPTER).text
        self.assertNotEqual(cur, steer)
        # 除媒体尾缀外逐字节相同:切掉各自的媒体段后应完全相等。
        cur_head = cur.split(" (图#0)")[0]
        steer_head = steer.split(" [图 m:81#0]")[0]
        self.assertEqual(cur_head, steer_head)
        self.assertIn("(图#0)", cur)
        self.assertIn("[图 m:81#0]", steer)
        self.assertIn("get_message_images", steer)

    def test_media_without_mid_rejected_for_steer_only(self) -> None:
        """有媒体无行号:steer 拒绝整行(文本通道无法寻址),current 照常渲染。"""
        r = row(MASTER, "主人号", mid=None, images=("https://x/1.png",))
        cur = pb.render_current_rows([r], is_private=False, self_id=SELF, adapter=ADAPTER)
        self.assertIn("(图#0)", cur)
        with self.assertRaises(ValueError):
            pb.render_steer_rows([r], self_id=SELF, adapter=ADAPTER)


class SourceGateTests(unittest.TestCase):
    """来源闸不变:steer 只收真人行,current 不收 bot/混合,系统行只有 current 认。"""

    def test_system_row_only_current(self) -> None:
        sys_row = BufferedMessage(
            ts=1, adapter=ADAPTER, group_id=GROUP, user_id="0", nickname="",
            content="戳了你一下", origin=MessageOrigin.synthetic(MessageSource.NOTICE_POKE),
            id=None,
        )
        self.assertEqual(
            pb.render_current_rows([sys_row], is_private=False, self_id=SELF, adapter=ADAPTER),
            "[sys=戳一戳] 戳了你一下",
        )
        with self.assertRaises(ValueError):
            pb.render_steer_rows([sys_row], self_id=SELF, adapter=ADAPTER)

    def test_bot_row_rejected_by_both(self) -> None:
        bot_row = row(OTHER_BOT, "别家bot", mid=91, is_bot=True)
        with self.assertRaises(ValueError):
            pb.render_current_rows([bot_row], is_private=False, self_id=SELF, adapter=ADAPTER)
        with self.assertRaises(ValueError):
            pb.render_steer_rows([bot_row], self_id=SELF, adapter=ADAPTER)


class StripAndHitSemanticsTests(unittest.TestCase):
    """回收侧:剥壳不叠壳、命中判据语义不变、无机器锚点时不吞正文。"""

    def test_strip_new_and_legacy_forms(self) -> None:
        for text in (
            "[有人@我] [m:101] [主人] [id=20001 user=主人号]: 你怎么看",
            "[m:102] [主人] 你怎么看",
            "[m:103] [id=20001 user=主人号]: 你怎么看",
            "[主人] [id=20001 user=主人号] [m:104]: 你怎么看",
            "[m:105]: 你怎么看",
        ):
            with self.subTest(text=text):
                self.assertEqual(steer_reconcile.strip_steer_presentation(text), "你怎么看")

    def test_strip_does_not_eat_body_opening_with_identity(self) -> None:
        """无机器锚点时身份栏与用户正文无法区分:按「不吞正文」保留(已声明残留)。"""
        self.assertEqual(
            steer_reconcile.strip_steer_presentation("[主人] 只是普通正文"),
            "[主人] 只是普通正文",
        )

    def test_strip_is_idempotent_no_double_shell(self) -> None:
        once = steer_reconcile.strip_steer_presentation(
            "[有人@我] [m:111] [主人] [id=20001 user=主人号]: 正文"
        )
        twice = steer_reconcile.strip_steer_presentation(once)
        self.assertEqual(once, "正文")
        self.assertEqual(twice, "正文")

    def test_hit_pattern_matches_new_and_legacy_lines(self) -> None:
        pat = steer_reconcile._steer_line_hit_pattern(121)
        self.assertTrue(pat.search("[m:121] [主人] [id=1 user=甲]: 在吗"))
        self.assertTrue(pat.search("[有人@我] [m:121] [主人]: 在吗"))
        self.assertTrue(pat.search("[主人] [id=1 user=甲] [m:121]: 老形态"))
        self.assertTrue(pat.search("[m:121]: 老无 speaker 形态"))
        # 只出现在正文/引用里、行首不是该行号 → 不算该行已呈递
        self.assertFalse(pat.search("(引用 甲(1): [m:121] 历史引用)"))
        self.assertFalse(pat.search("[m:1210] [主人]: 别的行"))


class SingleConstructorTests(unittest.TestCase):
    """结构断言:三条入口都走 _render_chat_line,没有第三套前缀拼装。"""

    def test_all_entries_call_shared_constructor(self) -> None:
        original = pb._render_chat_line
        calls = []

        def spy(row, **kwargs):
            calls.append((row.id, kwargs.get("media"), kwargs.get("bot_prefix")))
            return original(row, **kwargs)

        with patch.object(pb, "_render_chat_line", side_effect=spy):
            pb.render_current_rows(
                [row(MASTER, "主人号", mid=1), row(ORDINARY, "小光", mid=2)],
                is_private=False, self_id=SELF, adapter=ADAPTER,
            )
            pb._render_recent_messages_block(
                [row(MASTER, "主人号", mid=3)], self_id=SELF, adapter=ADAPTER,
            )
            pb.render_steer_rows(
                [row(MASTER, "主人号", mid=4)], is_private=False,
                self_id=SELF, adapter=ADAPTER,
            )
        self.assertEqual([c[0] for c in calls], [1, 2, 3, 4])
        # current/recent 用紧凑清单(默认参数),steer 用可寻址形态;只有 recent 带 [bot] 位。
        self.assertEqual([c[1] for c in calls], [None, None, None, "steer"])
        self.assertEqual([c[2] for c in calls], [None, None, True, None])


if __name__ == "__main__":
    unittest.main()
