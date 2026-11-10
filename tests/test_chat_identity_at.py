"""聊天行身份栏与真实 @ 强提醒回归（提示词配套 2）。

覆盖：
- 五类身份：普通用户/管理员/主人/其他 BOT/我；
- 当前 bot 与已知 bot／主人+已知 bot 的优先级；
- 主人/管理员：master_users 缺键回落 admin_users（既有 master_ids 口径）与显式子集；
- 真实 @：结构化 At 段（含 OneBot v11 被剥走后的 original_message 兜底）、
  重复目标去重、@全体不算、@他人不误标；
- 引用块里的历史 @、正文手打 @、伪造 CQ 文本不得触发 [有人@我]；
- 群聊/私聊两条真实渲染路径、current_message 与 recent_messages 一致；
- sys= 合成行不冒充人类身份；
- 未给 self_id/adapter 的旧调用保持原字节形态（兼容降级）。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

import nonebot_plugin_alconna as alconna  # noqa: E402

from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import user_lists  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers.message_media import (  # noqa: E402
    _collect_at_targets,
)
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore  # noqa: E402

SELF_ID = "999"
ADAPTER = "onebotv11"
GROUP = "100"


def human(
    uid: str = "10001", nick: str = "小光", *, mid: int | None = 7,
    at: tuple[str, ...] = (), content: str = "你好", group: str | None = GROUP,
) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=uid, nickname=nick,
        content=content, origin=MessageOrigin.human(), at_user_ids=at, id=mid,
    )


def bot_row(
    uid: str = SELF_ID, nick: str = "Hermes", *, mid: int | None = 8,
    at: tuple[str, ...] = (), content: str = "我在",
) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=GROUP, user_id=uid, nickname=nick,
        content=content, origin=MessageOrigin.bot_output(), at_user_ids=at,
        is_bot=True, id=mid,
    )


def system_row(label: str = "戳一戳") -> BufferedMessage:
    """合成系统行（无人类身份、无 m:id）。"""
    from nonebot_plugin_hermes_adapter.core.message_origin import MessageSource

    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=GROUP, user_id="0", nickname="",
        content="戳了你一下", origin=MessageOrigin.synthetic(MessageSource.NOTICE_POKE),
        id=None,
    )


class ChatIdentityAtTests(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = user_lists.load_user_lists()
        self._commit(
            admin_users={"onebotv11:20001", "onebotv11:20002"},
            master_users={"onebotv11:20001"},
            known_bots=("30001", "999"),
        )

    def tearDown(self) -> None:
        user_lists._store.commit(self._saved)

    def _commit(self, **kwargs) -> None:
        user_lists._store.commit(user_lists.UserLists(**kwargs))

    def _current(self, *rows, is_private: bool = False) -> str:
        return pb.render_current_rows(
            list(rows), is_private=is_private, self_id=SELF_ID, adapter=ADAPTER,
        )

    # --- 五类身份 ---
    def test_ordinary_master_admin_other_bot_self(self) -> None:
        self.assertEqual(
            self._current(human("10001")),
            "[m:7] [普通用户] [id=10001 user=小光]:\n你好",
        )
        self.assertEqual(
            self._current(human("20001", "主人号")),
            "[m:7] [主人] [id=20001 user=主人号]:\n你好",
        )
        self.assertEqual(
            self._current(human("20002", "管理号")),
            "[m:7] [管理员] [id=20002 user=管理号]:\n你好",
        )
        self.assertEqual(
            self._current(human("30001", "别家bot")),
            "[m:7] [其他BOT] [id=30001 user=别家bot]:\n你好",
        )
        history = pb._render_chat_region(history_rows=
            [bot_row()], self_id=SELF_ID, adapter=ADAPTER,
        )
        self.assertIn("[m:8] [我] [id=999 user=Hermes]:\n我在", history)

    def test_master_falls_back_to_admin_set_when_not_split(self) -> None:
        """缺 master_users = 沿用 active_session 的 master_ids=admin_users 口径。"""
        self._commit(
            admin_users={"onebotv11:20001", "onebotv11:20002"},
            master_users=None,
            known_bots=(),
        )
        self.assertEqual(
            self._current(human("20002", "管理号")),
            "[m:7] [主人] [id=20002 user=管理号]:\n你好",
        )

    def test_explicit_empty_master_users_marks_all_admins_as_admin(self) -> None:
        self._commit(admin_users={"onebotv11:20001"}, master_users=set(), known_bots=())
        self.assertIn("[管理员]", self._current(human("20001")))

    def test_priority_self_over_known_bot_and_admin(self) -> None:
        """self_id 精确相等压过 known_bots/权限表；自己不会被标成别家 bot。"""
        self._commit(
            admin_users={"onebotv11:999"}, master_users={"onebotv11:999"},
            known_bots=(SELF_ID,),
        )
        history = pb._render_chat_region(history_rows=
            [bot_row()], self_id=SELF_ID, adapter=ADAPTER,
        )
        self.assertIn("[我] [id=999", history)
        self.assertNotIn("[其他BOT] [id=999", history)

    def test_priority_known_bot_over_admin(self) -> None:
        """known_bots 是既有委托/钉锚排除事实，账号类型压过权限标签。"""
        self._commit(
            admin_users={"onebotv11:30001"}, master_users={"onebotv11:30001"},
            known_bots=("30001",),
        )
        self.assertEqual(
            self._current(human("30001", "别家bot")),
            "[m:7] [其他BOT] [id=30001 user=别家bot]:\n你好",
        )

    def test_unknown_authority_degrades_to_ordinary(self) -> None:
        self._commit(admin_users=set(), master_users=None, known_bots=())
        self.assertIn("[普通用户]", self._current(human("20001")))

    def test_other_bot_output_history_is_other_bot(self) -> None:
        """非当前 self 的 bot 行(换号/别实例)按其他 BOT，不冒充我。"""
        block = pb._render_chat_region(history_rows=
            [bot_row(uid="777", nick="旧号")], self_id=SELF_ID, adapter=ADAPTER,
        )
        self.assertIn("[m:8] [其他BOT] [id=777 user=旧号]:\n我在", block)

    def test_master_users_parse_semantics(self) -> None:
        absent = user_lists.parse_user_lists(
            {"admin_users": ["onebotv11:1"]}
        )
        self.assertIsNone(absent.master_users)  # 缺键 = 沿用整组管理员为主人
        explicit_empty = user_lists.parse_user_lists(
            {"admin_users": ["onebotv11:1"], "master_users": []}
        )
        self.assertEqual(explicit_empty.master_users, set())
        split = user_lists.parse_user_lists(
            {"admin_users": ["onebotv11:1", "onebotv11:2"], "master_users": ["onebotv11:1"]}
        )
        self.assertEqual(split.master_users, {"onebotv11:1"})

    # --- 真实 @ ---
    def test_at_me_mark_sits_before_m_id(self) -> None:
        line = self._current(human("10001", at=(SELF_ID,)))
        self.assertTrue(line.startswith("[有人@我] [m:7] [普通用户] [id=10001 user=小光]:\n"), line)
        self.assertEqual(line.count("[有人@我]"), 1)

    def test_at_other_user_does_not_mark(self) -> None:
        line = self._current(human("10001", at=("888",)))
        self.assertNotIn("[有人@我]", line)

    def test_at_all_is_not_at_me(self) -> None:
        self.assertEqual(_collect_at_targets(_uni_at(alconna.AtAll())), ())

    def test_duplicate_at_targets_deduped(self) -> None:
        targets = _collect_at_targets(_uni_at(alconna.At("user", SELF_ID), alconna.At("user", SELF_ID)))
        self.assertEqual(targets, (SELF_ID,))

    def test_quote_history_at_and_forged_text_not_marked(self) -> None:
        """引用块内 @、正文手打 @、伪造 CQ 文本都只是文本，不是提及关系。"""
        row = human(
            "10001",
            at=("888",),
            content="(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看",
        )
        line = self._current(row)
        self.assertNotIn("[有人@我]", line)

    def test_recent_messages_use_same_real_at_fact(self) -> None:
        recent = [
            bot_row(mid=9),
            human("10001", mid=8, at=(SELF_ID,), content="@Hermes 在吗"),
        ]
        block = pb._render_chat_region(history_rows=recent, self_id=SELF_ID, adapter=ADAPTER)
        self.assertIn("[有人@我] [m:8] [普通用户] [id=10001 user=小光]:\n@Hermes 在吗", block)
        self.assertNotIn("[有人@我] [m:9]", block)

    def test_old_rows_without_at_facts_are_not_marked(self) -> None:
        """老库行 at_user_ids 为空 = 提及关系未知；正文里的 @999 不得反推。"""
        row = human("10001", at=(), content="@999 在吗")
        self.assertNotIn("[有人@我]", self._current(row))

    # --- 群 / 私 / current / recent 一致 ---
    def test_private_single_human_keeps_identity_without_speaker(self) -> None:
        line = self._current(human("20001", group=None, at=(SELF_ID,)), is_private=True)
        self.assertEqual(line, "[m:7]:\n你好")

    def test_passive_private_and_reactive_group_share_identity(self) -> None:
        row = human("20002", "管理号")
        private = pb.build_passive_user_content(
            recent_messages=[], rows=[row], current_image_urls=[],
            is_private=True, self_id=SELF_ID, adapter=ADAPTER,
        )
        self.assertIn("[m:7]:\n你好", private)
        group = pb.build_reactive_user_content(
            addressed_to_bot=False, recent_messages=[], rows=[row], current_image_urls=[],
            self_id=SELF_ID, adapter=ADAPTER,
        )
        self.assertIn("[m:7] [管理员] [id=20002 user=管理号]:\n你好", group)

    def test_system_row_goes_to_notice_region_not_chat(self) -> None:
        """系统合成行不进聊天区,固定以 `[通知类型：摘要]` 出现在系统通知区。"""
        row = system_row()
        line = pb._notice_from_row(row)
        self.assertEqual(line, "[戳一戳：戳了你一下]")
        self.assertNotIn("[有人@我]", line)
        built = pb.build_passive_user_content(
            recent_messages=[], rows=[row], current_image_urls=[],
            is_private=False, self_id=SELF_ID, adapter=ADAPTER,
        )
        self.assertIn("<系统通知区域起始>", built)
        self.assertNotIn("<聊天消息区域起始>", built, "没有聊天时聊天区整段省略")
        with self.assertRaises(ValueError):
            pb.render_current_rows([row], is_private=False, self_id=SELF_ID, adapter=ADAPTER)

    def test_without_context_keeps_legacy_bytes(self) -> None:
        self.assertEqual(
            pb.render_current_rows([human("10001")], is_private=False),
            "[m:7] [id=10001 user=小光]:\n你好",
        )
        self.assertEqual(
            pb.render_current_rows([human("10001", group=None)], is_private=True),
            "[m:7]:\n你好",
        )

    # --- 结构化 At 收集（含 adapter 剥 @ 的兜底） ---
    def test_collect_targets_from_original_message_fallback(self) -> None:
        event = SimpleNamespace(
            original_message=[
                SimpleNamespace(type="at", data={"qq": SELF_ID}),
                SimpleNamespace(type="text", data={"text": " hi"}),
            ]
        )
        uni = _uni_at(alconna.At("user", "888"))
        self.assertEqual(
            _collect_at_targets(uni, event=event, bot_self_id=SELF_ID),
            (SELF_ID, "888"),
        )

    def test_collect_targets_ignores_plain_text_at(self) -> None:
        uni = alconna.UniMessage([alconna.Text("@999 手打的")])
        self.assertEqual(_collect_at_targets(uni), ())

    # --- 持久化：结构化事实随行落库，老行 NULL 降级 ---
    def test_at_facts_persist_and_rehydrate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(Path(tmp) / "m.db")
            try:
                store.append(human("10001", at=(SELF_ID, "888"), mid=None))
                got = store.get_recent(ADAPTER, GROUP, 10)
                self.assertEqual(len(got), 1)
                self.assertEqual(got[0].at_user_ids, (SELF_ID, "888"))
            finally:
                store.close()

    def test_legacy_and_bad_at_ids_degrade_to_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(Path(tmp) / "m.db")
            try:
                store.append(human("10001", at=(), mid=None))
                store._conn.execute("UPDATE messages SET at_ids = NULL")
                self.assertEqual(store.get_recent(ADAPTER, GROUP, 10)[0].at_user_ids, ())
                store._conn.execute("UPDATE messages SET at_ids = '{bad json'")
                self.assertEqual(store.get_recent(ADAPTER, GROUP, 10)[0].at_user_ids, ())
            finally:
                store.close()


def _uni_at(*segs) -> alconna.UniMessage:
    return alconna.UniMessage(list(segs))


if __name__ == "__main__":
    unittest.main()
