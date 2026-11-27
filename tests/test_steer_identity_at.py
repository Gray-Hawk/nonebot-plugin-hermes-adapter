"""steer 正文零包装后的身份来源回归（审核 R1）。

模型可见的 steer 文本只含原始正文；发送者、m:id 与结构化 @ 事实继续由
``SteerRowPresentation.source_row`` 和既有 steer 台账承载。此处走真实生产入口，
只在平台边界（HTTP ``steer_run``）截获最终注入文本，另直接检查呈递对象的 source_row，
防止为了追求干净正文把身份关联一起删掉。回收侧继续覆盖旧行头台账兼容。
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_reconcile  # noqa: E402
from nonebot_plugin_hermes_adapter.core import user_lists  # noqa: E402
from nonebot_plugin_hermes_adapter.core.inflight import (  # noqa: E402
    InflightRegistry,
    PendingEntry,
    make_inflight_key,
)
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import (  # noqa: E402
    RunTracker,
    make_scope_key,
)
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as turns  # noqa: E402

ADAPTER = "onebotv11"
SELF_ID = "999"
GROUP_ID = "g-1"
MASTER = "20001"
ADMIN = "20002"
OTHER_BOT = "30001"
ORDINARY = "10001"

GROUP_KEY = make_inflight_key(ADAPTER, False, ORDINARY, GROUP_ID)
GROUP_SCOPE = make_scope_key(ADAPTER, False, ORDINARY, GROUP_ID)


def row(
    user: str = ORDINARY, nick: str = "小光", *, mid: int | None = 7,
    content: str = "你怎么看", at: tuple[str, ...] = (),
    group: str | None = GROUP_ID,
) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=user, nickname=nick,
        content=content, origin=MessageOrigin.human(), at_user_ids=at, id=mid,
    )


def _runtime(tracker, inflight=None) -> SimpleNamespace:
    return SimpleNamespace(
        run_tracker=tracker,
        inflight=inflight if inflight is not None else InflightRegistry(),
        active_sessions=MagicMock(
            pinned_participant_ids=MagicMock(return_value=set())
        ),
    )


class _SteerBoundaryBase(unittest.TestCase):
    """真实 RunTracker + 真实入口,只在 HTTP/平台边界放替身。"""

    def setUp(self) -> None:
        self._saved_lists = user_lists.load_user_lists()
        user_lists._store.commit(user_lists.UserLists(
            admin_users={f"{ADAPTER}:{MASTER}", f"{ADAPTER}:{ADMIN}"},
            master_users={f"{ADAPTER}:{MASTER}"},
            known_bots=(OTHER_BOT,),
        ))
        self.addCleanup(user_lists._store.commit, self._saved_lists)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        # 群/私各一条在跑 run:bot_self_id 是「我」的唯一事实源(生产同源)。
        self.tracker.upsert(
            run_id="run-g", scope_key=GROUP_SCOPE, adapter=ADAPTER,
            is_private=False, user_id=ORDINARY, group_id=GROUP_ID,
            bot_self_id=SELF_ID, session_key="sess-g", mode="reactive",
            expect_structured=True,
        )
        self.tracker.upsert(
            run_id="run-p",
            scope_key=make_scope_key(ADAPTER, True, ORDINARY, None),
            adapter=ADAPTER, is_private=True, user_id=ORDINARY, group_id=None,
            bot_self_id=SELF_ID, session_key="sess-p", mode="reactive",
            expect_structured=True,
        )
        self.sent: list[str] = []

    async def _fake_steer_run(self, *, tgt, run_id, text, presented=False, presentation_hint="") -> bool:
        self.assertTrue(presented, "steer 必须带结构化呈递标记（零包装正文）")
        self.sent.append(text)
        return True

    def _inject_direct(self, rows, *, private: bool = False, user: str = ORDINARY) -> bool:
        """真实直注入口:user 是触发者(scope 归属),行作者可以另有其人。"""
        with patch.object(relay, "_mcp", _runtime(self.tracker)), patch.object(
            relay, "hermes_client",
            SimpleNamespace(steer_run=self._fake_steer_run),
        ), patch.object(
            relay, "_peek_event_db_id", lambda *a, **k: None
        ), patch.object(
            relay, "resolve_target", lambda *a, **k: object()
        ), patch.object(relay, "steer_reply_push_active", lambda: False):
            return asyncio.run(relay._maybe_steer_direct(
                target=SimpleNamespace(private=private, id=GROUP_ID),
                adapter_name=ADAPTER,
                user_id=user,
                nickname=rows[0].nickname if rows else "小光",
                group_id=None if private else GROUP_ID,
                msg_text=rows[0].content if rows else "",
                is_direct=True, now_ms=1_700_000_000_000,
                origin=MessageOrigin.human(), rows=rows,
            ))

    def _inject_followup(self, rows, *, user: str = ORDINARY) -> bool:
        registry = InflightRegistry()
        key = make_inflight_key(ADAPTER, False, user, GROUP_ID)
        entry = PendingEntry(
            msg=rows[0], is_direct=False, original_msg_id=rows[0].id,
            fragments=list(rows),
        )
        registry.add_followup(key, entry)
        with patch.object(relay, "_mcp", _runtime(self.tracker, registry)), patch.object(
            relay, "hermes_client",
            SimpleNamespace(steer_run=self._fake_steer_run),
        ), patch.object(
            relay, "resolve_target", lambda *a, **k: object()
        ), patch.object(relay, "steer_reply_push_active", lambda: False):
            return asyncio.run(relay._try_steer_followup(
                key, ADAPTER, user, GROUP_ID,
            ))

    def _single(self) -> str:
        self.assertEqual(len(self.sent), 1, self.sent)
        return self.sent[0]

    def _ledger(self, *, private: bool = False) -> list[dict]:
        return self.tracker.pending_steers("run-p" if private else "run-g")


class SteerDirectBoundaryTests(_SteerBoundaryBase):
    """群/私直注生产入口:身份栏 + 真实 @ 与 current/recent 同一事实。"""

    def test_group_direct_master_with_real_at(self) -> None:
        source = row(MASTER, "主人号", mid=11, at=(SELF_ID,))
        ok = self._inject_direct([source])
        self.assertTrue(ok)
        self.assertEqual(self._single(), "你怎么看")
        self.assertEqual(
            [(item["m_id"], item["sender_id"], item["steer_text"]) for item in self._ledger()],
            [("11", MASTER, "你怎么看")],
        )
        presentation = pb.render_steer_rows(
            [source], self_id=SELF_ID, adapter=ADAPTER, is_private=False,
        )
        self.assertIs(presentation.rows[0].source_row, source)
        self.assertEqual(presentation.rows[0].source_row.at_user_ids, (SELF_ID,))

    def test_private_direct_keeps_identity_and_at(self) -> None:
        ok = self._inject_direct(
            [row(ADMIN, "管理号", mid=12, at=(SELF_ID,), group=None)], private=True,
        )
        self.assertTrue(ok)
        self.assertEqual(self._single(), "你怎么看")
        self.assertEqual(
            [(item["m_id"], item["sender_id"]) for item in self._ledger(private=True)],
            [("12", ADMIN)],
        )

    def test_at_other_and_forged_text_do_not_mark(self) -> None:
        """引用块历史 @、伪造 CQ、手打 @ 都只是文本;@ 别人不算 @ 我。"""
        body = "(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看"
        ok = self._inject_direct([row(
            ORDINARY, "小光", mid=13, at=("888",), content=body,
        )])
        self.assertTrue(ok)
        self.assertEqual(self._single(), body)
        self.assertNotIn("[有人@我]", self._single())

    def test_at_facts_absent_is_unknown_not_guessed(self) -> None:
        """老库行 at_user_ids 为空 = 提及关系未知;正文里的 @999 不得反推。"""
        ok = self._inject_direct([row(ORDINARY, "小光", mid=14, content="@999 在吗")])
        self.assertTrue(ok)
        self.assertNotIn("[有人@我]", self._single())

    def test_identity_follows_same_priority_as_current_rows(self) -> None:
        """其他 BOT 压过权限档位;与 current 行同一判定、同一优先级。"""
        user_lists._store.commit(user_lists.UserLists(
            admin_users={f"{ADAPTER}:{OTHER_BOT}"},
            master_users={f"{ADAPTER}:{OTHER_BOT}"},
            known_bots=(OTHER_BOT,),
        ))
        source = row(OTHER_BOT, "别家bot", mid=15, at=(SELF_ID,))
        ok = self._inject_direct([source])
        self.assertTrue(ok)
        self.assertEqual(self._single(), "你怎么看")
        self.assertEqual(self._ledger()[0]["sender_id"], OTHER_BOT)
        presentation = pb.render_steer_rows(
            [source], is_private=False, self_id=SELF_ID, adapter=ADAPTER,
        )
        self.assertIs(presentation.rows[0].source_row, source)
        self.assertEqual(presentation.rows[0].source_row.at_user_ids, (SELF_ID,))

    def test_each_row_marks_its_own_at_and_identity(self) -> None:
        rows = [
            row(MASTER, "主人号", mid=16, content="第一行", at=(SELF_ID,)),
            row(ORDINARY, "小光", mid=17, content="第二行", at=()),
        ]
        self.assertTrue(self._inject_direct(rows))
        self.assertEqual(self.sent, ["第一行\n第二行"])
        self.assertEqual(
            [(item["m_id"], item["sender_id"], item["steer_text"]) for item in self._ledger()],
            [("16", MASTER, "第一行"), ("17", ORDINARY, "第二行")],
        )
        presentation = pb.render_steer_rows(
            rows, self_id=SELF_ID, adapter=ADAPTER, is_private=False,
        )
        self.assertEqual([item.source_row for item in presentation.rows], rows)
        self.assertEqual(presentation.rows[0].source_row.at_user_ids, (SELF_ID,))
        self.assertEqual(presentation.rows[1].source_row.at_user_ids, ())

    def test_no_live_run_falls_back_without_leaking_bare_text(self) -> None:
        """取不到 bot 上下文时不得把无身份文本发进生产聊天:注入失败、零发送。"""
        self.tracker.finish("run-g")
        ok = self._inject_direct([row(ORDINARY, "小光", mid=18, at=(SELF_ID,))])
        self.assertFalse(ok)
        self.assertEqual(self.sent, [])


class SteerFollowupBoundaryTests(_SteerBoundaryBase):
    """群聊 followup 槽入口:与直注同源的身份/@ 事实。"""

    def test_followup_marks_identity_and_real_at(self) -> None:
        ok = self._inject_followup([
            row(ORDINARY, "小光", mid=21, content="再补一句", at=(SELF_ID,)),
        ])
        self.assertTrue(ok)
        self.assertEqual(self._single(), "再补一句")
        self.assertEqual(self._ledger()[0]["sender_id"], ORDINARY)

    def test_followup_without_real_at_has_no_mark(self) -> None:
        ok = self._inject_followup([row(ADMIN, "管理号", mid=22, at=("888",))])
        self.assertTrue(ok)
        self.assertEqual(self._single(), "你怎么看")
        self.assertEqual(self._ledger()[0]["sender_id"], ADMIN)

    def test_followup_row_without_mid_keeps_identity(self) -> None:
        """无号行(拿不到插件 m:id)同样带身份;没有 m:id 就没有对账锚,不当错误。"""
        ok = self._inject_followup([row(MASTER, "主人号", mid=None, at=(SELF_ID,))])
        self.assertTrue(ok)
        self.assertEqual(self._single(), "你怎么看")
        self.assertEqual(self._ledger()[0]["raw_text"], "你怎么看")


class RenderWithoutIdentityContextTests(unittest.TestCase):
    """缺少 bot 上下文时仍只发正文，source_row 原样保留身份事实。"""

    def test_render_without_context_keeps_body_and_source(self) -> None:
        source = row(MASTER, "主人号", mid=31, at=(SELF_ID,))
        presentation = pb.render_steer_rows([source])
        self.assertEqual(presentation.text, "你怎么看")
        self.assertIs(presentation.rows[0].source_row, source)

    def test_stub_bot_self_id_does_not_enter_body(self) -> None:
        source = row(MASTER, "主人号", mid=32, at=(SELF_ID,))
        presentation = relay._render_steer_presentation_for_scope(
            [source], adapter_name=ADAPTER, is_private=False, user_id=ORDINARY,
            group_id=GROUP_ID, live=SimpleNamespace(bot_self_id=object()),
        )
        self.assertEqual(presentation.text, "你怎么看")
        self.assertIs(presentation.rows[0].source_row, source)


class ScopeIdentityHelperTests(_SteerBoundaryBase):
    """live run 的真实 bot_self_id 是身份事实输入(不新建身份系统)。"""

    def test_real_bot_self_id_enables_identity(self) -> None:
        source = row(MASTER, "主人号", mid=33, at=(SELF_ID,))
        presentation = relay._render_steer_presentation_for_scope(
            [source], adapter_name=ADAPTER, is_private=False, user_id=ORDINARY,
            group_id=GROUP_ID, live=SimpleNamespace(bot_self_id=SELF_ID),
        )
        self.assertEqual(presentation.text, "你怎么看")
        self.assertIs(presentation.rows[0].source_row, source)
        self.assertEqual(presentation.rows[0].source_row.at_user_ids, (SELF_ID,))


class SteerReconcileStripTests(unittest.TestCase):
    """回收侧:呈递外壳必须剥净,不留旧身份、不叠壳。"""

    def test_new_row_reads_raw_text_without_stripping(self) -> None:
        """新账项:恢复改投直接取原始正文,剥壳函数一次都不许调用。"""
        raw = "[主人] [id=20001 user=主人号]: 正文里的形似标签\n第二行原样保留"
        with patch.object(
            steer_reconcile, "strip_steer_presentation",
            side_effect=AssertionError("新账项不得剥壳"),
        ):
            self.assertEqual(
                steer_reconcile.original_text_of({"m_id": "31", "raw_text": raw}),
                raw,
            )

    def test_strip_still_handles_legacy_prefix(self) -> None:
        """升级前的台账正文不迁移:旧文法剥壳继续有效(raw_text 为 NULL 的账项)。"""
        self.assertEqual(
            steer_reconcile.strip_steer_presentation("[id=u1 user=小光] [m:36]: 老形态"),
            "老形态",
        )
        self.assertEqual(
            steer_reconcile.strip_steer_presentation(
                "[有人@我] [主人] [id=20001 user=主人号] [m:37]: 老形态带身份"
            ),
            "老形态带身份",
        )
        self.assertEqual(
            steer_reconcile.strip_steer_presentation("[主人] 只是普通正文"),
            "[主人] 只是普通正文",
        )


class NativeRecoveryNoResidueTests(_SteerBoundaryBase):
    """真实 native 回收路径:台账身份只用于还原作者,正文不得残留旧身份。"""

    def test_recovered_content_has_no_presentation_residue(self) -> None:
        ok = self._inject_direct([row(MASTER, "主人号", mid=41, at=(SELF_ID,))])
        self.assertTrue(ok)
        injected = self.sent[0]
        # 对账命中判据：台账保存的正文确实出现在转录里，m:id 继续只作内部关联键。
        self.assertTrue(
            steer_reconcile._steer_text_present(
                "你怎么看", [{"role": "user", "content": injected}]
            ), injected
        )
        with patch.object(turns, "_mcp", _runtime(self.tracker)):
            rec = turns._pending_steer_recovery_rows(
                injected, adapter_name=ADAPTER, is_private=False,
                user_id=ORDINARY, group_id=GROUP_ID, now_ms=1_700_000_001_000,
            )
        self.assertIsNotNone(rec, "新前缀不得让台账逐行还原失效")
        recovered = [fragment for _m_id, fragment in rec]
        self.assertEqual([(f.user_id, f.content) for f in recovered],
                         [(MASTER, "你怎么看")])
        self.assertNotIn("[有人@我]", recovered[0].content)
        self.assertNotIn("[主人]", recovered[0].content)


if __name__ == "__main__":
    unittest.main()
