"""多模态转录读取回归:content 是部件列表时,对账不得崩退、台账不得永久卡死。

现场形态(上游会话 API 的合法多模态 user 行):
``content=[{"type":"text",...},{"type":"text",...},{"type":"image_url",...}]``。
``_fetch_transcript`` 的提前命中搜索与 ``reconcile_run_finished`` 的终态搜索都把
``content`` 直接交给 ``re.Pattern.search``,于是
``TypeError: expected string or bytes-like object, got 'list'``:第一次异常被分页
兜底吞掉,第二次异常直接逃出后台任务,``steer_ledger`` 永久停在 ``reconciling``。

覆盖:
  - A 真实形态 content=[text,text,image_url]:含目标 m:id → 核销;缺件但证据完整
    → 照旧进入既有补投路径,全程无 TypeError;
  - B 直接跑真实 reconcile_run_finished:首次转录读取抛 TypeError → 不向外炸、
    台账回 pending;第二次正常返回 → 成功处理、无 reconciling 残留、补投至多一次;
  - C reply_recovery 共用同一读取入口与同一纯文本视图,普通字符串行为不变。

用真实 RunTracker / SQLite 台账与真实对账函数;只把 HTTP 边界(httpx.AsyncClient)
与补投派发换成本地替身。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

ADAPTER = "onebotv11"
SELF = "999"
UID = "1000000009"  # 合成夹具账号(非生产身份)
M_ID = "70001"
STEER_TEXT = "合成插话正文"
TRIGGER_TEXT = "合成触发正文"
# 真实行里的第三个部件:非文本,必须被忽略(不能拼进证据文案或正则输入)。
IMAGE_PART = {
    "type": "image_url",
    "image_url": {"url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="},
}
MEDIA_HINT = "[图片]（随条媒体未内联，需要看图时用 get_message_images 按 m:id 取）"


def _steer_content(m_id: str = M_ID, text: str = STEER_TEXT) -> list[dict]:
    """现场 steer 行形态:聊天行一段 + 媒体提示一段 + image_url 一段。"""
    return [
        {"type": "text", "text": f"[m:{m_id}] [普通用户] [id={UID} user=合成昵称]:\n{text}"},
        {"type": "text", "text": MEDIA_HINT},
        IMAGE_PART,
    ]


def _trigger_content(text: str = TRIGGER_TEXT) -> list[dict]:
    """同形态的普通 user 行(本 run 触发批):有正文、有媒体提示、有图片部件。"""
    return [
        {"type": "text", "text": f"[m:70002] [普通用户] [id={UID} user=合成昵称]:\n{text}"},
        {"type": "text", "text": MEDIA_HINT},
        IMAGE_PART,
    ]


class _ReconcileRig(unittest.IsolatedAsyncioTestCase):
    """真实台账 + 真实对账函数,只替换 HTTP 边界与补投派发。"""

    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.core.run_tracker import (
            RunTracker,
            make_scope_key,
        )

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.custom = Path(tmp.name)
        self.tracker = RunTracker(self.custom / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.scope_key = make_scope_key(ADAPTER, True, UID, None)
        self.replays: list[tuple[str, str]] = []

    def _register(self, run_id: str = "run-mm") -> None:
        self.tracker.upsert(
            run_id=run_id, scope_key=self.scope_key, adapter=ADAPTER,
            is_private=True, user_id=UID, group_id=None, bot_self_id=SELF,
            session_key="sess", mode="passive", expect_structured=True,
        )

    def _pending_steer(self, run_id: str = "run-mm", m_id: str = M_ID) -> None:
        self._register(run_id)
        self.tracker.record_steer(
            run_id=run_id, scope_key=self.scope_key, session_key="sess",
            accepted_at=0,
            items=[{"m_id": m_id, "sender_id": UID,
                    "steer_text": f"[m:{m_id}]:\n{STEER_TEXT}", "raw_text": STEER_TEXT}],
        )

    def _statuses(self, run_id: str = "run-mm") -> list[tuple[str, str]]:
        return [
            (str(r[0]), str(r[1]))
            for r in self.tracker._conn.execute(
                "SELECT m_id, status FROM steer_ledger WHERE run_id=? ORDER BY id",
                (run_id,),
            ).fetchall()
        ]

    def _patch_reconcile(self, page: list[dict], *, status: int = 200):
        """对账外部边界的替身:HTTP 返回一页转录,补投只记录不发送。"""
        import httpx

        from nonebot_plugin_hermes_adapter.core import steer_reconcile as sc

        transport = httpx.MockTransport(
            lambda request: httpx.Response(status, json={"data": page})
        )
        real_async_client = httpx.AsyncClient

        def http_factory(*args, **kwargs):
            return real_async_client(transport=transport, **kwargs)

        def record_replay(tracked, text, user_id, nickname, now_ms, *, origin):
            self.replays.append((text, user_id))

        return sc, [
            patch.object(sc, "_mcp", SimpleNamespace(run_tracker=self.tracker)),
            patch.object(sc, "resolve_target",
                         lambda *a, **k: SimpleNamespace(base_url="http://x", api_key="")),
            patch.object(sc.httpx, "AsyncClient", http_factory),
            patch.object(sc, "_spawn_replay_turn", record_replay),
        ]


class ContentTextTests(unittest.TestCase):
    """content→纯文本视图:字符串原样;列表只拼可见文本;原始内容不被改写。"""

    def test_string_is_returned_verbatim(self) -> None:
        from nonebot_plugin_hermes_adapter.core.steer_reconcile import transcript_text

        raw = f"[m:{M_ID}]:\n{STEER_TEXT}\n"
        self.assertEqual(transcript_text(raw), raw)

    def test_parts_list_joins_text_and_ignores_media(self) -> None:
        from nonebot_plugin_hermes_adapter.core.steer_reconcile import transcript_text

        content = [
            {"type": "text", "text": "第一段正文"},
            IMAGE_PART,
            {"type": "text", "text": "第二段正文"},
        ]
        text = transcript_text(content)
        self.assertIn("第一段正文", text)
        self.assertIn("第二段正文", text)
        self.assertNotIn("base64", text)
        self.assertNotIn("image_url", text)

    def test_original_content_is_not_mutated(self) -> None:
        from nonebot_plugin_hermes_adapter.core.steer_reconcile import transcript_text

        content = _steer_content()
        before = repr(content)
        transcript_text(content)
        self.assertEqual(repr(content), before)

    def test_empty_and_unknown_shapes_degrade_to_empty(self) -> None:
        from nonebot_plugin_hermes_adapter.core.steer_reconcile import transcript_text

        self.assertEqual(transcript_text(None), "")
        self.assertEqual(transcript_text([]), "")
        self.assertEqual(transcript_text([IMAGE_PART]), "")


class MultimodalHitTests(_ReconcileRig):
    """A:含目标 m:id 的多模态行必须如实核销,不得因 TypeError 崩退。"""

    async def test_hit_in_multimodal_row_is_settled(self) -> None:
        self._pending_steer()
        tracked = self.tracker.get_by_run_id("run-mm")
        page = [
            {"id": 21, "role": "assistant", "content": "最终回复正文"},
            {"id": 20, "role": "user", "content": _steer_content()},
        ]
        sc, stack = self._patch_reconcile(page)
        for item in stack:
            item.start()
            self.addCleanup(item.stop)
        await sc.reconcile_run_finished(tracked)
        self.assertEqual(self._statuses(), [(M_ID, "reconciled")])
        self.assertEqual(self.replays, [], "已进入转录的行不得再补投")


class MultimodalReplayTests(_ReconcileRig):
    """A:缺件但证据完整时,照旧进入既有补投路径,全程无 TypeError。

    补投文案是情形 A 的时间线重构,三个读取点(assistant/steer/普通 user)都走多模态
    行的真实形态:正文照收,图片部件不进文案。
    """

    async def test_missing_row_with_multimodal_rows_replays_once(self) -> None:
        self._pending_steer()
        tracked = self.tracker.get_by_run_id("run-mm")
        page = [
            {"id": 22, "role": "assistant", "content": "最终回复正文"},
            {"id": 21, "role": "assistant",
             "content": [{"type": "text", "text": "中途回应的正文"}, IMAGE_PART]},
            {"id": 20, "role": "user", "display_kind": "steer",
             "content": _steer_content("70003", "另一条已送达插话")},
            {"id": 19, "role": "user", "content": _trigger_content()},
        ]
        sc, stack = self._patch_reconcile(page)
        for item in stack:
            item.start()
            self.addCleanup(item.stop)
        await sc.reconcile_run_finished(tracked)
        self.assertEqual(len(self.replays), 1, "缺件证据完整:恰好补投一次")
        text, user_id = self.replays[0]
        self.assertEqual(user_id, UID)
        self.assertIn(f"★ 1. {STEER_TEXT}", text)
        self.assertIn(TRIGGER_TEXT, text, "普通 user(触发批)读取点要拿到正文")
        self.assertIn("另一条已送达插话", text, "steer 行读取点要拿到正文")
        self.assertIn("中途回应的正文", text, "assistant 行读取点要拿到正文")
        self.assertIn("最终回复正文", text)
        self.assertNotIn("base64", text, "图片部件不得漏进补投文案")
        self.assertNotIn("image_url", text)
        self.assertEqual(self._statuses(), [(M_ID, "replayed")])


class ReconcileCrashContainmentTests(_ReconcileRig):
    """B:认领后的未处理异常不得让台账永久卡在 reconciling。

    直接跑真实 ``reconcile_run_finished``:首次转录读取抛现场那条 TypeError,
    任务不向外炸、已认领行回 pending(原 m:id/原文不动);下一次对账重新接管并成功
    处理,最终无 reconciling 残留,补投至多一次。
    """

    def _flaky_fetch(self, rows: list[dict]):
        calls = {"n": 0}

        async def fetch(tracked, pending=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise TypeError("expected string or bytes-like object, got 'list'")
            return rows, True

        return fetch, calls

    async def test_type_error_releases_claim_then_next_reconcile_succeeds(self) -> None:
        from nonebot_plugin_hermes_adapter.core import steer_reconcile as sc

        self._pending_steer()
        tracked = self.tracker.get_by_run_id("run-mm")
        rows = [
            {"id": 22, "role": "assistant", "content": "最终回复正文"},
            {"id": 19, "role": "user", "content": TRIGGER_TEXT},
        ]
        fetch, calls = self._flaky_fetch(rows)
        with patch.object(sc, "_mcp", SimpleNamespace(run_tracker=self.tracker)), \
                patch.object(sc, "_fetch_transcript", fetch), \
                patch.object(sc, "_spawn_replay_turn",
                             lambda tracked, text, uid, nick, now, *, origin:
                             self.replays.append((text, uid))):
            await sc.reconcile_run_finished(tracked)  # 不得向外抛
            self.assertEqual(calls["n"], 1)
            self.assertEqual(
                self._statuses(), [(M_ID, "pending")],
                "异常后必须释放回 pending,原 m:id 与原文保留,等下一次对账接管",
            )
            self.assertEqual(self.replays, [], "处理未完成时不得补投")
            await sc.reconcile_run_finished(tracked)  # 下一次对账重新接管
        self.assertEqual(calls["n"], 2)
        self.assertEqual(len(self.replays), 1, "成功处理时补投恰好一次")
        self.assertEqual(self._statuses(), [(M_ID, "replayed")])

    def test_release_does_not_resurrect_settled_rows(self) -> None:
        """释放只碰仍在 reconciling 的认领行:已结算行走终端态,不得被拉回 pending。"""
        self._pending_steer()
        self.tracker.record_steer(
            run_id="run-mm", scope_key=self.scope_key, session_key="sess", accepted_at=1,
            items=[{"m_id": "70004", "sender_id": UID,
                    "steer_text": "[m:70004]:\n另一条", "raw_text": "另一条"}],
        )
        self.tracker.claim_steers_for_reconcile("run-mm")
        self.tracker.mark_steers("run-mm", ["70004"], "reconciled")
        self.tracker.release_reconcile_claims("run-mm", [M_ID, "70004"])
        self.assertEqual(
            self._statuses(), [(M_ID, "pending"), ("70004", "reconciled")]
        )


class ReplyRecoverySharedReadTests(_ReconcileRig):
    """C:reply_recovery 共用同一转录读取入口与同一纯文本视图,字符串行为不变。"""

    def _decision(self, reply_text: str) -> str:
        return (
            '{"should_reply": true, "reply_text": "%s", "reply_to": "", "media": []}'
            % reply_text
        )

    def test_plain_string_transcript_behavior_is_unchanged(self) -> None:
        """普通字符串转录:前序结构化答复照旧全取,终态 output 不重复投。"""
        from nonebot_plugin_hermes_adapter.core.reply_recovery import (
            extract_overwritten_replies,
        )

        first, second = self._decision("一"), self._decision("二")
        rows = [
            {"id": 1, "role": "user", "content": "触发"},
            {"id": 2, "role": "assistant", "content": first},
            {"id": 3, "role": "assistant", "content": "旁白不算决策"},
            {"id": 4, "role": "assistant", "content": second},
        ]
        recovered = extract_overwritten_replies(rows, second)
        self.assertEqual(
            [(item.transcript_row_id, item.raw_text) for item in recovered], [(2, first)]
        )

    async def test_multimodal_transcript_is_read_through_the_shared_entry(self) -> None:
        from nonebot_plugin_hermes_adapter.core import reply_recovery as rr
        from nonebot_plugin_hermes_adapter.core import steer_reconcile as sc

        self.assertEqual(
            rr._fetch_transcript, sc._fetch_transcript,
            "reply_recovery 必须共用 steer 对账的转录读取入口,不得另起一套解析器",
        )
        self._register("run-rr")
        tracked = self.tracker.get_by_run_id("run-rr")
        final = self._decision("最终")
        earlier = self._decision("前序")
        page = [
            {"id": 31, "role": "assistant", "content": final},
            {"id": 30, "role": "assistant",
             "content": [{"type": "text", "text": earlier}, IMAGE_PART]},
            {"id": 29, "role": "user", "content": _trigger_content()},
        ]
        _sc, stack = self._patch_reconcile(page)
        for item in stack:
            item.start()
            self.addCleanup(item.stop)
        recovered = await rr.recover_run_overwritten_replies(tracked, final)
        self.assertEqual(
            [(item.transcript_row_id, item.raw_text) for item in recovered], [(30, earlier)]
        )


if __name__ == "__main__":
    unittest.main()
