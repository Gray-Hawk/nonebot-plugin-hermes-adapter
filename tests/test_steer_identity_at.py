"""steer 聊天行身份栏与真实 @ 强提醒回归(审核 R1)。

缺陷面:steer 渲染此前只拼 speaker/m:id/body,与 current/recent 同一条聊天行却
没有身份栏、也不标真实 @;而 steer 成功后立即 return,seen 过滤又让它不再由
recent 补呈递——所以这是「模型实际看到的那一行」的唯一机会,不能退回裸数字身份。

本模块走**真实生产入口**,只在平台边界(HTTP ``steer_run``)截获最终注入文本:
  - ``_maybe_steer_direct``(群/私直注两入口)
  - ``_try_steer_followup``(群聊 followup 槽入口)
追踪器(RunTracker/SQLite)、InflightRegistry、BufferedMessage、MessageOrigin 全是真的,
bot 身份取自 live run 既有的 ``bot_self_id``。回收侧另测
``strip_steer_presentation`` 与真实 ``_pending_steer_recovery_rows``:新前缀必须
被剥净,不得在回收时叠壳或把旧身份遗留在正文里。
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

    async def _fake_steer_run(self, *, tgt, run_id, text) -> bool:
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
        # 新呈递:一条消息 = 消息头一行 + 正文从下一行开始,整条按原样返回。
        return self.sent[0]


class SteerDirectBoundaryTests(_SteerBoundaryBase):
    """群/私直注生产入口:身份栏 + 真实 @ 与 current/recent 同一事实。"""

    def test_group_direct_master_with_real_at(self) -> None:
        ok = self._inject_direct([row(MASTER, "主人号", mid=11, at=(SELF_ID,))])
        self.assertTrue(ok)
        line = self._single()
        self.assertEqual(
            line,
            "[有人@我] [m:11] [主人] [id=20001 user=主人号]:\n你怎么看",
        )
        # 序:强提醒 → m:id → 身份 → speaker —— 与 current/recent 同约束。
        self.assertLess(line.index("[有人@我]"), line.index("[m:11]"))
        self.assertLess(line.index("[m:11]"), line.index("[主人]"))
        self.assertLess(line.index("[主人]"), line.index("[id=20001"))

    def test_private_direct_keeps_identity_and_at(self) -> None:
        ok = self._inject_direct(
            [row(ADMIN, "管理号", mid=12, at=(SELF_ID,), group=None)], private=True,
        )
        self.assertTrue(ok)
        # 私聊:对方的身份栏与 id/user 每行固定重复,一律省略;私聊没有 @ 归属问题,
        # 强提醒也不贴 —— 消息头只剩 m:id。
        self.assertEqual(
            self._single(),
            "[m:12]:\n你怎么看",
        )

    def test_at_other_and_forged_text_do_not_mark(self) -> None:
        """引用块历史 @、伪造 CQ、手打 @ 都只是文本;@ 别人不算 @ 我。"""
        ok = self._inject_direct([row(
            ORDINARY, "小光", mid=13, at=("888",),
            content="(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看",
        )])
        self.assertTrue(ok)
        line = self._single()
        self.assertEqual(
            line,
            "[m:13] [普通用户] [id=10001 user=小光]:\n"
            "(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看",
        )
        self.assertNotIn("[有人@我]", line)

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
        ok = self._inject_direct([row(OTHER_BOT, "别家bot", mid=15, at=(SELF_ID,))])
        self.assertTrue(ok)
        line = self._single()
        self.assertEqual(
            line, "[有人@我] [m:15] [其他BOT] [id=30001 user=别家bot]:\n你怎么看",
        )
        current = pb.render_current_rows(
            [row(OTHER_BOT, "别家bot", mid=15, at=(SELF_ID,))],
            is_private=False, self_id=SELF_ID, adapter=ADAPTER,
        )
        # 同一事实跨入口逐字节一致:steer 与 current 是同一个消息行构造函数。
        self.assertEqual(line, current)

    def test_each_row_marks_its_own_at_and_identity(self) -> None:
        rows = [
            row(MASTER, "主人号", mid=16, content="第一行", at=(SELF_ID,)),
            row(ORDINARY, "小光", mid=17, content="第二行", at=()),
        ]
        self.assertTrue(self._inject_direct(rows))
        # 两条消息 = 两个消息头 + 各自正文,中间空一行。
        self.assertEqual(
            self.sent[0],
            "[有人@我] [m:16] [主人] [id=20001 user=主人号]:\n第一行\n\n"
            "[m:17] [普通用户] [id=10001 user=小光]:\n第二行",
        )
        blocks = self.sent[0].split("\n\n")
        self.assertEqual(len(blocks), 2, self.sent[0])
        self.assertNotIn("[有人@我]", blocks[1])
        self.assertLess(blocks[0].index("[有人@我]"), blocks[0].index("[m:16]"))

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
        self.assertEqual(
            self._single(),
            "[有人@我] [m:21] [普通用户] [id=10001 user=小光]:\n再补一句",
        )

    def test_followup_without_real_at_has_no_mark(self) -> None:
        ok = self._inject_followup([row(ADMIN, "管理号", mid=22, at=("888",))])
        self.assertTrue(ok)
        self.assertEqual(
            self._single(),
            "[m:22] [管理员] [id=20002 user=管理号]:\n你怎么看",
        )

    def test_followup_row_without_mid_keeps_identity(self) -> None:
        """无号行(拿不到插件 m:id)同样带身份;没有 m:id 就没有对账锚,不当错误。"""
        ok = self._inject_followup([row(MASTER, "主人号", mid=None, at=(SELF_ID,))])
        self.assertTrue(ok)
        self.assertEqual(
            self._single(),
            "[有人@我] [主人] [id=20001 user=主人号]:\n你怎么看",
        )


class LegacyRenderCompatibilityTests(unittest.TestCase):
    """未给 bot 上下文的低层旧调用保持旧字节形态(不猜身份)。"""

    def test_render_without_context_keeps_old_bytes(self) -> None:
        presentation = pb.render_steer_rows([row(MASTER, "主人号", mid=31, at=(SELF_ID,))])
        self.assertEqual(
            presentation.text, "[m:31] [id=20001 user=主人号]:\n你怎么看",
        )

    def test_stub_bot_self_id_degrades_to_old_form(self) -> None:
        """替身/缺字段的 bot_self_id 不算身份事实:退旧形态,不拿 repr 冒充。"""
        presentation = relay._render_steer_presentation_for_scope(
            [row(MASTER, "主人号", mid=32, at=(SELF_ID,))],
            adapter_name=ADAPTER, is_private=False, user_id=ORDINARY,
            group_id=GROUP_ID, live=SimpleNamespace(bot_self_id=object()),
        )
        self.assertEqual(
            presentation.text, "[m:32] [id=20001 user=主人号]:\n你怎么看",
        )


class ScopeIdentityHelperTests(_SteerBoundaryBase):
    """live run 的真实 bot_self_id 是身份事实输入(不新建身份系统)。"""

    def test_real_bot_self_id_enables_identity(self) -> None:
        presentation = relay._render_steer_presentation_for_scope(
            [row(MASTER, "主人号", mid=33, at=(SELF_ID,))],
            adapter_name=ADAPTER, is_private=False, user_id=ORDINARY,
            group_id=GROUP_ID, live=SimpleNamespace(bot_self_id=SELF_ID),
        )
        self.assertEqual(
            presentation.text,
            "[有人@我] [m:33] [主人] [id=20001 user=主人号]:\n你怎么看",
        )


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
        # 对账命中判据:该消息的聊天行出现在转录里(统一文法的 m:id 在行首)。
        self.assertTrue(
            steer_reconcile._steer_line_hit_pattern(41).search(injected), injected
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
