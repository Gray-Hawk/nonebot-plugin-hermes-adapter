"""steer 原文恢复的全路径保真:行边界、空串行、跨 run 旧账、身份事实、身份表边界。

覆盖第二轮复审四项(都是真实 HTTP/SSE/台账链路上暴露的缺口):

- N1 无号私聊行的正文可以以空白/换行开头(呈递就是正文本身),匹配器先跳空白会把
  一整批合成一条;空串行(用户发了一条空行消息)在 pending 原文里没有字符可锚定,
  也必须保条数保顺序;
- N2 无号行的虚拟锚在 run 正常收尾后必须结算,否则后一个 run 按 scope 拉候选会
  拿到上一轮的原文;
- N3 恢复出的带号行要按库里那一行水合昵称与结构化 At,否则下一轮普通 handler 里
  真实 @ 与原昵称消失;
- N4 当前权威身份表已有条目时,缺某个用户就是「没有事实」,不得跨目录借旧名字。

这里只用真实 RunTracker / MessageStore / MessageBuffer 与纯函数,不连网络。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

ADAPTER = "onebotv11"
SELF = "999"
UID = "10001"
GROUP = "g-1"
RENDERED = "[有人@我] [m:1] [普通用户] [id=10001 user=临时昵称]:\n看这里"


def _human(*, mid=None, content="看这里", group=GROUP, user=UID, nickname="临时昵称",
           at=()):
    from nonebot_plugin_agent_adapter.core.message_buffer import BufferedMessage
    from nonebot_plugin_agent_adapter.core.message_origin import MessageOrigin

    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=user, nickname=nickname,
        content=content, origin=MessageOrigin.human(), id=mid, at_user_ids=at,
    )


def _scope_key() -> str:
    from nonebot_plugin_agent_adapter.core.run_tracker import make_scope_key

    return make_scope_key(ADAPTER, False, UID, GROUP)


class MatcherRowBoundaryTests(unittest.TestCase):
    """N1:原文里每条账项的边界与条数只认 pending 文本,先精确匹配再跳分隔符。"""

    def setUp(self) -> None:
        from nonebot_plugin_agent_adapter.handlers import message_turns as mt

        self.mt = mt

    def test_leading_whitespace_row_keeps_batch_split(self) -> None:
        first = "  : 第一条\n尾行\n"
        second = "第二条"
        pending = first + "\n\n" + second
        rows = [
            {"m_id": "noid:1", "steer_text": first, "raw_text": first},
            {"m_id": "noid:2", "steer_text": second, "raw_text": second},
        ]
        matched = self.mt._match_pending_steer_rows(pending, rows)
        self.assertEqual([m["m_id"] for m in matched], ["noid:1", "noid:2"])

    def test_trailing_whitespace_only_row_is_not_missing(self) -> None:
        body = "原文尾部有两个换行\n\n"
        pending = body + "\n\n" + "第二条"
        rows = [
            {"m_id": "noid:1", "steer_text": body, "raw_text": body},
            {"m_id": "noid:2", "steer_text": "第二条", "raw_text": "第二条"},
        ]
        matched = self.mt._match_pending_steer_rows(pending, rows)
        self.assertEqual([m["m_id"] for m in matched], ["noid:1", "noid:2"])

    def test_empty_row_keeps_its_place_before_numbered_row(self) -> None:
        pending = "\n\n[m:92]:\n有效正文"
        rows = [
            {"m_id": "noid:empty", "steer_text": "", "raw_text": ""},
            {"m_id": "92", "steer_text": "[m:92]:\n有效正文", "raw_text": "有效正文"},
        ]
        matched = self.mt._match_pending_steer_rows(pending, rows)
        self.assertEqual([m["m_id"] for m in matched], ["noid:empty", "92"])

    def test_trailing_empty_rows_are_appended(self) -> None:
        pending = "[m:92]:\n有效正文\n\n\n\n"
        rows = [
            {"m_id": "92", "steer_text": "[m:92]:\n有效正文", "raw_text": "有效正文"},
            {"m_id": "noid:e1", "steer_text": "", "raw_text": ""},
            {"m_id": "noid:e2", "steer_text": "", "raw_text": ""},
        ]
        matched = self.mt._match_pending_steer_rows(pending, rows)
        self.assertEqual([m["m_id"] for m in matched], ["92", "noid:e1", "noid:e2"])

    def test_uncovered_body_still_fails_closed(self) -> None:
        rows = [{"m_id": "noid:1", "steer_text": "甲", "raw_text": "甲"}]
        self.assertIsNone(
            self.mt._match_pending_steer_rows("甲\n\n没有账项的正文", rows)
        )


class RecoveryFactsTests(unittest.TestCase):
    """N3:带号回收行按库里那一行水合昵称/发送者/结构化 At,正文与号不变。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.custom = Path(self._tmp.name)

    def _rig(self):
        from nonebot_plugin_agent_adapter.core.message_buffer import MessageBuffer
        from nonebot_plugin_agent_adapter.core.run_tracker import RunTracker
        from nonebot_plugin_agent_adapter.core.storage.message_store import MessageStore

        store = MessageStore(self.custom / "facts.db")
        self.addCleanup(store.close)
        buffer = MessageBuffer(store=store, fetcher=MagicMock())
        tracker = RunTracker(self.custom / "tracker.db")
        self.addCleanup(tracker.close)
        tracker.upsert(
            run_id="run-g", scope_key=_scope_key(),
            adapter=ADAPTER, is_private=False, user_id=UID, group_id=GROUP,
            bot_self_id=SELF, session_key="sess", mode="reactive",
            expect_structured=True,
        )
        return store, buffer, tracker

    def test_numbered_recovery_restores_nickname_and_at(self) -> None:
        from nonebot_plugin_agent_adapter.handlers import message_turns as mt

        store, buffer, tracker = self._rig()
        row = _human(content="看这里", at=(SELF,))
        buffer.append(row)
        self.assertIsNotNone(row.id)
        tracker.record_steer(
            run_id="run-g", scope_key=make_scope_key_safe(False, UID, GROUP),
            session_key="sess", accepted_at=0,
            items=[{"m_id": str(row.id), "sender_id": UID,
                    "steer_text": RENDERED, "raw_text": "看这里"}],
        )
        rendered = f"[有人@我] [m:{row.id}] [普通用户] [id=10001 user=临时昵称]:\n看这里"
        with patch.object(
            mt, "_mcp", SimpleNamespace(run_tracker=tracker, message_buffer=buffer)
        ):
            recovered = mt._pending_steer_recovery_rows(
                rendered, adapter_name=ADAPTER, is_private=False,
                user_id=UID, group_id=GROUP, now_ms=2,
            )
        self.assertIsNotNone(recovered)
        m_id, fragment = recovered[0]
        self.assertEqual(m_id, str(row.id))
        self.assertEqual(fragment.content, "看这里")
        self.assertEqual(fragment.id, row.id)
        self.assertEqual(fragment.nickname, "临时昵称")
        self.assertEqual(fragment.user_id, UID)
        self.assertEqual(fragment.at_user_ids, (SELF,))

    def test_store_returns_none_for_unknown_message(self) -> None:
        store, _buffer, _tracker = self._rig()
        self.assertIsNone(store.get_message_facts(123456))


def make_scope_key_safe(is_private: bool, user_id: str, group_id: str | None) -> str:
    from nonebot_plugin_agent_adapter.core.run_tracker import make_scope_key

    return make_scope_key(ADAPTER, is_private, user_id, group_id)


class NoIdLedgerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    """N2:无号虚拟锚随 run 正常收尾结算,不留在 live 里被后一个 run 当候选。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.custom = Path(self._tmp.name)
        from nonebot_plugin_agent_adapter.core.run_tracker import RunTracker

        self.tracker = RunTracker(self.custom / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.scope_key = make_scope_key_safe(False, UID, GROUP)

    def _register(self, run_id: str) -> None:
        self.tracker.upsert(
            run_id=run_id, scope_key=self.scope_key, adapter=ADAPTER,
            is_private=False, user_id=UID, group_id=GROUP, bot_self_id=SELF,
            session_key="sess", mode="reactive", expect_structured=True,
        )

    async def test_virtual_anchor_settles_when_run_finishes(self) -> None:
        from nonebot_plugin_agent_adapter.core import steer_reconcile as sc

        self._register("run-old")
        self.tracker.record_steer(
            run_id="run-old", scope_key=self.scope_key, session_key="sess",
            accepted_at=0,
            items=[{"m_id": "noid:abc", "sender_id": UID, "steer_text": "有效正文",
                    "raw_text": "有效正文\n解释:\n"}],
        )
        tracked = self.tracker.get_by_run_id("run-old")
        self.assertTrue(self.tracker.pending_steers_by_scope(self.scope_key))
        with patch.object(sc, "_mcp", SimpleNamespace(run_tracker=self.tracker)):
            await sc.reconcile_run_finished(tracked)
        self.assertEqual(
            self.tracker.pending_steers_by_scope(self.scope_key), [],
            "run 收尾后虚拟锚不得继续作为后一轮的原文候选",
        )
        status = self.tracker._conn.execute(
            "SELECT status FROM steer_ledger WHERE run_id='run-old'"
        ).fetchone()[0]
        self.assertEqual(status, "reconciled")

    async def test_text_recovery_is_scoped(self) -> None:
        from nonebot_plugin_agent_adapter.core.run_tracker import make_scope_key

        self._register("run-old")
        self.tracker.record_steer(
            run_id="run-old", scope_key=self.scope_key, session_key="sess",
            accepted_at=0,
            items=[{"m_id": "noid:empty", "sender_id": UID, "steer_text": "",
                    "raw_text": ""}],
        )
        other_scope = make_scope_key(ADAPTER, False, UID, "g-2")
        self.tracker.record_steer(
            run_id="run-other", scope_key=other_scope, session_key="sess",
            accepted_at=0,
            items=[{"m_id": "noid:other", "sender_id": UID, "steer_text": "",
                    "raw_text": ""}],
        )
        # 空串的 LIKE 会退化成 %%:限定 scope 后只核销本会话,别的会话还等着回收的
        # 账项不得被一起核销(否则静默丢失)。
        self.assertEqual(
            self.tracker.recover_by_text("任何无关正文", scope_key=self.scope_key), 1
        )
        statuses = dict(self.tracker._conn.execute(
            "SELECT run_id, status FROM steer_ledger"
        ).fetchall())
        self.assertEqual(statuses["run-old"], "recovered")
        self.assertEqual(statuses["run-other"], "pending")

    async def test_virtual_anchor_recovered_by_text_is_not_replayed(self) -> None:
        from nonebot_plugin_agent_adapter.core import steer_reconcile as sc

        self._register("run-old")
        self.tracker.record_steer(
            run_id="run-old", scope_key=self.scope_key, session_key="sess",
            accepted_at=0,
            items=[{"m_id": "noid:abc", "sender_id": UID, "steer_text": "有效正文",
                    "raw_text": "有效正文"}],
        )
        # 真被吞:回收点先按 pending 原文核销,终态对账看不到这一行,不得再补投。
        self.assertEqual(self.tracker.recover_by_text("有效正文"), 1)
        tracked = self.tracker.get_by_run_id("run-old")
        with patch.object(sc, "_mcp", SimpleNamespace(run_tracker=self.tracker)):
            await sc.reconcile_run_finished(tracked)
        status = self.tracker._conn.execute(
            "SELECT status FROM steer_ledger WHERE run_id='run-old'"
        ).fetchone()[0]
        self.assertEqual(status, "recovered")


class IdentityTableBoundaryTests(unittest.TestCase):
    """N4:身份事实只有当前配置那一份表;当前表有条目时缺人就是不借。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.custom = Path(self._tmp.name)
        from nonebot_plugin_agent_adapter.core import identity

        self.identity = identity
        identity.clear_cache()
        self.addCleanup(identity.clear_cache)

    def _write(self, root: Path, text: str) -> Path:
        path = root / "memory" / ADAPTER / "identities.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("user_id,nickname,aliases,merged_into,note\n" + text)
        return root

    def test_missing_user_in_existing_table_does_not_borrow_other_dir(self) -> None:
        from nonebot_plugin_agent_adapter.handlers import message_turns as mt

        previous = self._write(self.custom / "previous", "10001,旧目录名字,,,\n")
        configured = self._write(self.custom / "configured", "20002,当前目录别的用户,,,\n")
        # 先把「旧目录」那份表加载进进程缓存,模拟定制目录搬迁前的状态。
        self.assertEqual(self.identity.resolve_display_name(previous, ADAPTER, UID), "旧目录名字")
        with patch.object(mt, "resolve_custom_dir", lambda: configured):
            self.assertIsNone(
                mt._speaker_name_resolver(ADAPTER)(UID),
                "当前权威表已有条目:缺这个用户就是没有事实,不得跨目录借",
            )

    def test_current_table_wins_when_it_has_the_user(self) -> None:
        from nonebot_plugin_agent_adapter.handlers import message_turns as mt

        previous = self._write(self.custom / "previous", "10001,旧目录名字,,,\n")
        configured = self._write(self.custom / "configured", "10001,当前正主名,,,\n")
        self.identity.resolve_display_name(previous, ADAPTER, UID)
        with patch.object(mt, "resolve_custom_dir", lambda: configured):
            self.assertEqual(mt._speaker_name_resolver(ADAPTER)(UID), "当前正主名")

    def test_absent_table_still_uses_loaded_same_table(self) -> None:
        from nonebot_plugin_agent_adapter.handlers import message_turns as mt

        previous = self._write(self.custom / "previous", "10001,旧目录名字,,,\n")
        empty_configured = self.custom / "configured"
        empty_configured.mkdir()
        self.identity.resolve_display_name(previous, ADAPTER, UID)
        with patch.object(mt, "resolve_custom_dir", lambda: empty_configured):
            self.assertEqual(
                mt._speaker_name_resolver(ADAPTER)(UID), "旧目录名字",
                "配置目录里整张表还不存在时,沿用同一份已加载身份表(搬迁/预览场景)",
            )


if __name__ == "__main__":
    unittest.main()
