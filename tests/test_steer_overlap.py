"""P-07 跨调用/重叠入口幂等 与 P-09 ACK 乱序回归。

两个场景都走**真实入口链**,只替换外部 I/O(steer HTTP、平台目标解析、入站领号):

  - 重叠 reactive/followup:A 的 steer HTTP 尚未返回,B 经第二个真实
    `_handle_reactive_path` 并入同一 followup 槽并再次尝试注入;
  - 两个合资格 direct steer:A(201)、B(202)按顺序被上游接受,A 的 HTTP 回应延迟,
    B 先返回 → 真实 SQLite 台账序为 B/A,再经真实 `RunsEventParser.feed` 喂
    `completed.pending_steer`,走实际恢复函数。

每条都配负对照:同一份台账/原文按修复前的算法(无条件 INSERT、台账序子串匹配)
计算,断言它确实会错——证明用例抓的是缺陷而不是新代码的形状。
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

import httpx  # noqa: E402

from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import run_tracker as rt  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_reconcile as sc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_seen  # noqa: E402
from nonebot_plugin_hermes_adapter.core.hermes_client import RunsEventParser  # noqa: E402
from nonebot_plugin_hermes_adapter.core import inflight as inflight_mod  # noqa: E402
from nonebot_plugin_hermes_adapter.core.inflight import (  # noqa: E402
    InflightRegistry,
    PendingEntry,
    make_inflight_key,
)
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.prompt_builder import render_current_rows  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import (  # noqa: E402
    RunTracker,
    make_scope_key,
)
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as turns  # noqa: E402

ADAPTER = "onebotv11"
GROUP_ID = "g1"
TRIGGER = "u1"
RUN_ID = "run-overlap"
KEY = make_inflight_key(ADAPTER, False, TRIGGER, GROUP_ID)
SCOPE_KEY = make_scope_key(ADAPTER, False, TRIGGER, GROUP_ID)


def msg(user: str, mid: int, text: str) -> BufferedMessage:
    return BufferedMessage(
        ts=mid, adapter=ADAPTER, group_id=GROUP_ID, user_id=user, nickname=user,
        content=text, origin=MessageOrigin.human(), id=mid,
    )


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        steer_seen._steer_seen_reset_for_test()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.tracker.upsert(
            run_id=RUN_ID, scope_key=SCOPE_KEY, adapter=ADAPTER, is_private=False,
            user_id=TRIGGER, group_id=GROUP_ID, bot_self_id="999", session_key="sess",
            mode="reactive", expect_structured=True,
        )

    def ledger(self) -> list[dict]:
        return self.tracker.pending_steers(RUN_ID)

    def legacy_ledger_rows(self, items: list[tuple[int, str, str]]) -> None:
        """按修复前形态(record_steer 无条件 INSERT)落一份台账,复现旧库现场。"""
        for mid, sender, rendered in items:
            self.tracker._conn.execute(
                "INSERT INTO steer_ledger (run_id, m_id, scope_key, session_key,"
                " sender_id, steer_text, accepted_at, status)"
                " VALUES (?,?,?,?,?,?,0.0,'pending')",
                (RUN_ID, str(mid), SCOPE_KEY, "sess", sender, rendered),
            )
        self.tracker._conn.commit()

    def restore(self, pending_text: str):
        with patch.object(turns, "_mcp", SimpleNamespace(run_tracker=self.tracker)):
            return turns._pending_steer_recovery_fragments(
                pending_text, adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=300,
            )


def _legacy_restore(pending_text: str, candidates: list[dict]) -> list[dict]:
    """负对照:修复前的恢复算法——台账序子串匹配,重复账项各还原一次。"""
    matched = [c for c in candidates if c.get("steer_text") and c["steer_text"] in pending_text]
    residual = pending_text
    for c in matched:
        residual = residual.replace(c["steer_text"], "", 1)
    if residual.replace(relay._STEER_REPLY_CONVENTION_LINE, "").strip():
        return []
    return matched


def _legacy_merge_recovered_batch(
    slot_fragments: list[BufferedMessage],
    entry_fragments: list[BufferedMessage],
    known_m_ids,
) -> tuple[list[BufferedMessage], list[str], list[BufferedMessage]]:
    """负对照:第 4 轮实现——吸收槽内已存在的行,缺行追加到槽尾。

    槽里只剩尾行 B 时,payload A/B 被排成 B/A(第 5 轮评审 R5)。
    """
    present = {str(row.id) for row in slot_fragments if row.id is not None}
    kept: list[BufferedMessage] = []
    absorbed: list[str] = []
    for m_id, fragment in zip(known_m_ids, entry_fragments):
        if m_id is not None and str(m_id) in present:
            absorbed.append(str(m_id))
            continue
        kept.append(fragment)
    return [*slot_fragments, *kept], absorbed, kept


class OverlappingFollowupEntryTests(_Base):
    """真实 reactive 入口:A 在途,B 并入同一 followup 槽。"""

    def _run_overlap(self) -> dict:
        registry = InflightRegistry()
        self.assertIsNotNone(
            registry.try_enter(
                KEY, msg(TRIGGER, 100, "running"), is_direct=True,
                original_msg_id="event-100", now_ms=100, commission=True,
            )
        )
        active = MagicMock()
        active.is_alive_for_trigger.return_value = True
        runtime = SimpleNamespace(
            run_tracker=self.tracker, inflight=registry, active_sessions=active
        )
        started = asyncio.Event()
        release = asyncio.Event()
        sent: list[str] = []

        async def steer(*, tgt, run_id, text):
            sent.append(text)
            if len(sent) == 1:
                started.set()
                await release.wait()   # 第一个 HTTP 迟迟不回
            return True

        async def enter(row: BufferedMessage):
            await relay._handle_reactive_path(
                bot=MagicMock(), target=SimpleNamespace(private=False, id=GROUP_ID),
                adapter_name=ADAPTER, user_id=row.user_id, nickname=row.nickname,
                group_id=GROUP_ID, text=row.content, image_urls=[], is_direct=False,
                addressed_to_bot=False, now_ms=row.ts, origin=row.origin,
                rows=[row], event_msg_id=f"event-{row.id}",
            )

        async def scenario():
            row_a, row_b = msg("u2", 101, "alpha"), msg("u3", 102, "beta")
            first = asyncio.create_task(enter(row_a))
            await asyncio.wait_for(started.wait(), 2)
            entry_during = registry.peek_followup(KEY)
            self.assertEqual([r.content for r in entry_during.fragments], ["alpha"])
            await enter(row_b)
            # B 已并入并发出自己的行;A 那一行仍在别的入口手里发送,槽不得结算它。
            entry_mid = registry.peek_followup(KEY)
            mid_state = None if entry_mid is None else [r.content for r in entry_mid.fragments]
            release.set()
            await first
            return {
                "sent": list(sent),
                "ledger": [(r["m_id"], r["sender_id"]) for r in self.ledger()],
                "counts": dict(Counter(r["m_id"] for r in self.ledger())),
                "slot_after_b": mid_state,
                "slot_final": registry.peek_followup(KEY),
            }

        with patch.object(relay, "_mcp", runtime), patch.object(turns, "_mcp", runtime), \
                patch.object(relay, "hermes_client", SimpleNamespace(steer_run=steer)), \
                patch.object(relay, "resolve_target", lambda *a, **k: object()), \
                patch.object(relay, "_claim_event_db_id",
                             lambda adapter, event: int(event.split("-")[-1])), \
                patch.object(relay, "steer_reply_push_active", lambda: False), \
                patch.object(relay, "plugin_config",
                             SimpleNamespace(hermes_api_interface="runs")):
            return asyncio.run(scenario())

    def test_each_original_row_sent_and_ledged_once(self) -> None:
        facts = self._run_overlap()
        # 每个原始行各发一次:A 只发 A,B 那批只发 B(不重发已接受/在途的前缀)。
        self.assertEqual(facts["sent"], [
            "[id=u2] [m:101]: alpha",
            "[id=u3] [m:102]: beta",
        ])
        # 同一原始行只入账一次。
        self.assertEqual(facts["ledger"], [("101", "u2"), ("102", "u3")])
        self.assertEqual(facts["counts"], {"101": 1, "102": 1})
        # A 在途时槽里只剩它(未呈现),B 发完只摘 B;A 的入口成功后摘槽。
        self.assertEqual(facts["slot_after_b"], ["alpha"])
        self.assertIsNone(facts["slot_final"])

    def test_native_restore_of_overlap_payload_has_no_phantom_row(self) -> None:
        facts = self._run_overlap()
        pending = "\n".join(facts["sent"])
        rows = self.restore(pending)
        self.assertEqual(
            [(r.user_id, r.content) for r in rows or []],
            [("u2", "alpha"), ("u3", "beta")],
        )
        # 上游只还回最后一批(B)时也只还原 B,不会因为账里有 A 而多出一行。
        last_batch = self.restore(facts["sent"][-1])
        self.assertEqual([(r.user_id, r.content) for r in last_batch or []],
                         [("u3", "beta")])
        # 负对照:旧形态同一 run 会把 A 入账两次,再按台账序子串匹配 → A+B+A。
        self.legacy_ledger_rows([])  # 空操作,仅为明确这里用的是同一张真实表
        self.tracker._conn.execute("DELETE FROM steer_ledger")
        self.tracker._conn.commit()
        self.legacy_ledger_rows([
            (101, "u2", "[id=u2] [m:101]: alpha"),
            (102, "u3", "[id=u3] [m:102]: beta"),
            (101, "u2", "[id=u2] [m:101]: alpha"),
        ])
        legacy = _legacy_restore(pending, self.tracker.pending_steers_by_scope(SCOPE_KEY))
        self.assertEqual([c["m_id"] for c in legacy], ["101", "102", "101"])
        fixed = self.restore(pending)
        self.assertEqual([(r.user_id, r.content) for r in fixed or []],
                         [("u2", "alpha"), ("u3", "beta")])


class RepeatedTriggerTests(_Base):
    """普通重复触发:同一原始行已入账后不再重复呈递,也不丢内容。"""

    def _direct(self, row: BufferedMessage) -> tuple[bool, list[str]]:
        sent: list[str] = []

        async def steer(*, tgt, run_id, text):
            sent.append(text)
            return True

        with patch.object(relay, "_mcp", SimpleNamespace(
            run_tracker=self.tracker, active_sessions=MagicMock(
                pinned_participant_ids=MagicMock(return_value=set())),
            inflight=InflightRegistry(),
        )), patch.object(relay, "hermes_client", SimpleNamespace(steer_run=steer)), \
                patch.object(relay, "resolve_target", lambda *a, **k: object()), \
                patch.object(relay, "steer_reply_push_active", lambda: False):
            ok = asyncio.run(relay._maybe_steer_direct(
                target=SimpleNamespace(private=False, id=GROUP_ID),
                adapter_name=ADAPTER, user_id=TRIGGER, nickname="小光",
                group_id=GROUP_ID, msg_text=row.content, is_direct=True,
                now_ms=row.ts, origin=row.origin, rows=[row],
            ))
        return ok, sent

    def test_same_row_triggered_twice_is_presented_once(self) -> None:
        row = msg("u2", 101, "alpha")
        first_ok, first_sent = self._direct(row)
        second_ok, second_sent = self._direct(row)
        self.assertTrue(first_ok)
        self.assertEqual(first_sent, ["[id=u2] [m:101]: alpha"])
        # 第二次触发算「已呈现」但不重发:调用方不再 dispatch 一遍。
        self.assertTrue(second_ok)
        self.assertEqual(second_sent, [])
        self.assertEqual(Counter(r["m_id"] for r in self.ledger()), {"101": 1})

    def test_followup_entry_of_accepted_row_settles_without_resend(self) -> None:
        row = msg("u2", 101, "alpha")
        self._direct(row)
        registry = InflightRegistry()
        entry = PendingEntry(msg=row, is_direct=False, original_msg_id=101)
        registry.add_followup(KEY, entry)
        sent: list[str] = []

        async def steer(*, tgt, run_id, text):
            sent.append(text)
            return True

        with patch.object(relay, "_mcp", SimpleNamespace(
            run_tracker=self.tracker, inflight=registry,
        )), patch.object(relay, "hermes_client", SimpleNamespace(steer_run=steer)), \
                patch.object(relay, "resolve_target", lambda *a, **k: object()), \
                patch.object(relay, "steer_reply_push_active", lambda: False):
            ok = asyncio.run(relay._try_steer_followup(KEY, ADAPTER, TRIGGER, GROUP_ID))
        self.assertTrue(ok)
        self.assertEqual(sent, [])
        self.assertIsNone(registry.peek_followup(KEY), "已呈现的条目应结算,留槽只会再呈递一遍")
        self.assertEqual(Counter(r["m_id"] for r in self.ledger()), {"101": 1})

    def test_failed_send_releases_claim_and_keeps_slot(self) -> None:
        registry = InflightRegistry()
        entry = PendingEntry(msg=msg("u3", 103, "跟一句"), is_direct=False, original_msg_id=103)
        registry.add_followup(KEY, entry)

        async def steer(*, tgt, run_id, text):
            return False   # 409 / 网络失败

        with patch.object(relay, "_mcp", SimpleNamespace(
            run_tracker=self.tracker, inflight=registry,
        )), patch.object(relay, "hermes_client", SimpleNamespace(steer_run=steer)), \
                patch.object(relay, "resolve_target", lambda *a, **k: object()), \
                patch.object(relay, "steer_reply_push_active", lambda: False):
            ok = asyncio.run(relay._try_steer_followup(KEY, ADAPTER, TRIGGER, GROUP_ID))
        self.assertFalse(ok)
        self.assertEqual(self.ledger(), [], "发送失败必须撤销在途认领")
        self.assertIs(registry.peek_followup(KEY), entry, "内容留在槽里走接力,零丢失")
        self.assertEqual([r.content for r in entry.fragments], ["跟一句"])

    def test_cancelled_send_releases_claim(self) -> None:
        """发送被取消/中断:在途认领不得滞留在账上把同一行永久标成「在途」。"""
        registry = InflightRegistry()
        entry = PendingEntry(msg=msg("u3", 104, "又来一句"), is_direct=False, original_msg_id=104)
        registry.add_followup(KEY, entry)

        async def steer(*, tgt, run_id, text):
            raise asyncio.CancelledError()

        with patch.object(relay, "_mcp", SimpleNamespace(
            run_tracker=self.tracker, inflight=registry,
        )), patch.object(relay, "hermes_client", SimpleNamespace(steer_run=steer)), \
                patch.object(relay, "resolve_target", lambda *a, **k: object()), \
                patch.object(relay, "steer_reply_push_active", lambda: False):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(relay._try_steer_followup(KEY, ADAPTER, TRIGGER, GROUP_ID))
        self.assertEqual(self.ledger(), [], "中断也必须撤销在途认领")
        self.assertEqual([r.content for r in entry.fragments], ["又来一句"])


class NativeRestoreOrderTests(_Base):
    """P-09:恢复顺序/次数只认 pending 原文,不认 HTTP 回应(台账)序。"""

    def _two_direct_steers(self, rows: list[BufferedMessage], *, delay_first: bool) -> list[str]:
        sent: list[str] = []
        started = asyncio.Event()
        release = asyncio.Event()

        async def steer(*, tgt, run_id, text):
            sent.append(text)
            if delay_first and len(sent) == 1:
                started.set()
                await release.wait()
            return True

        active = MagicMock()
        active.pinned_participant_ids.return_value = {"u2", "u3"}
        runtime = SimpleNamespace(run_tracker=self.tracker, active_sessions=active)

        async def scenario():
            first = None
            for row in rows:
                call = relay._maybe_steer_direct(
                    target=SimpleNamespace(private=False, id=GROUP_ID),
                    adapter_name=ADAPTER, user_id=row.user_id, nickname=row.nickname,
                    group_id=GROUP_ID, msg_text=row.content, is_direct=True,
                    now_ms=row.ts, origin=row.origin, rows=[row],
                )
                if delay_first and first is None:
                    first = asyncio.create_task(call)
                    await asyncio.wait_for(started.wait(), 2)
                    continue
                self.assertTrue(await call)
            release.set()
            if first is not None:
                self.assertTrue(await first)

        with patch.object(relay, "_mcp", runtime), patch.object(turns, "_mcp", runtime), \
                patch.object(relay, "hermes_client", SimpleNamespace(steer_run=steer)), \
                patch.object(relay, "resolve_target", lambda *a, **k: object()), \
                patch.object(relay, "steer_reply_push_active", lambda: False):
            asyncio.run(scenario())
        return sent

    def _parser_pending(self, pending: str) -> str:
        parser = RunsEventParser()
        payload = json.dumps({"seq": 0, "output": "", "pending_steer": pending})
        parser.feed("event: run.completed")
        parser.feed("data: " + payload)
        parser.feed("")
        self.assertTrue(parser.terminal)
        return parser.pending_steer

    def test_response_order_does_not_affect_claim_or_restore(self) -> None:
        rows = [msg("u1", 201, "first"), msg("u2", 202, "second")]
        sent = self._two_direct_steers(rows, delay_first=True)
        # 上游按 A、B 接受;A 的 HTTP 回应后到。台账是「发送前认领」落的库,
        # 顺序 = 发送/接受序,不再是 HTTP 回应序(旧形态这里会是 B/A)。
        self.assertEqual(sent, ["[id=u1] [m:201]: first", "[id=u2] [m:202]: second"])
        self.assertEqual([(r["m_id"], r["sender_id"]) for r in self.ledger()],
                         [("201", "u1"), ("202", "u2")])
        pending = self._parser_pending("\n".join(sent))
        restored = self.restore(pending)
        self.assertEqual([(r.user_id, r.content) for r in restored or []],
                         [("u1", "first"), ("u2", "second")])

    def test_ack_ordered_legacy_ledger_restores_in_payload_order(self) -> None:
        """旧版本留下的 ACK 乱序台账(真实生产库形态):恢复仍按原文位置。"""
        a = "[id=u1] [m:201]: first"
        b = "[id=u2] [m:202]: second"
        self.legacy_ledger_rows([(202, "u2", b), (201, "u1", a)])   # B 先回、A 后回
        self.assertEqual([(r["m_id"], r["sender_id"]) for r in self.ledger()],
                         [("202", "u2"), ("201", "u1")])
        pending = self._parser_pending(f"{a}\n{b}")
        restored = self.restore(pending)
        self.assertEqual([(r.user_id, r.content) for r in restored or []],
                         [("u1", "first"), ("u2", "second")])
        # 负对照:旧的台账序子串匹配在同一份库上确实颠倒。
        legacy = _legacy_restore(pending, self.tracker.pending_steers_by_scope(SCOPE_KEY))
        self.assertEqual([(c["sender_id"], c["m_id"]) for c in legacy],
                         [("u2", "202"), ("u1", "201")])

    def test_occurrence_count_follows_payload_not_ledger(self) -> None:
        """旧库留下的重复账项:原文出现几次还原几行,绝不多还原一行。"""
        a = "[id=u2] [m:101]: alpha"
        b = "[id=u3] [m:102]: beta"
        self.legacy_ledger_rows([(101, "u2", a), (102, "u3", b), (101, "u2", a)])
        self.assertEqual([r["m_id"] for r in self.ledger()], ["101", "102", "101"])
        self.assertEqual(
            [(r.user_id, r.content) for r in self.restore(f"{a}\n{b}") or []],
            [("u2", "alpha"), ("u3", "beta")],
        )
        self.assertEqual(
            [(r.user_id, r.content) for r in self.restore(f"{a}\n{a}\n{b}") or []],
            [("u2", "alpha"), ("u2", "alpha"), ("u3", "beta")],
        )

    def test_different_rows_with_identical_body_keep_payload_order(self) -> None:
        rows = [msg("u2", 301, "同意"), msg("u3", 302, "同意")]
        sent = self._two_direct_steers(rows, delay_first=True)
        restored = self.restore("\n".join(sent))
        # 两个不同原始行正文相同:按各自 [m:id] 身份分别还原,顺序仍是原文顺序。
        self.assertEqual([(r.user_id, r.content) for r in restored or []],
                         [("u2", "同意"), ("u3", "同意")])
        # 负对照:两行正文相同,只有身份不同——任何「按正文去重」的做法都会把两条
        # 塌成一条;台账里两行 steer_text 的差异只在 speaker/[m:id] 前缀。
        texts = [c["steer_text"] for c in self.ledger()]
        self.assertEqual(len(texts), 2)
        self.assertEqual(len({t.split("]: ", 1)[-1] for t in texts}), 1)


class TerminalInFlightSteerTests(_Base):
    """R3:在途认领 × 终态钩子 × 409／晚到 200 只走一条恢复路径。

    真实 `_handle_reactive_path`／`_maybe_steer_direct`、真实 `HermesClient.steer_run`
    （httpx.MockTransport 只替换网络边界）、真实 `_spawn_steer_reconcile_if_pending`
    ＋`tracker.finish`、真实 reconcile 认领与转录判定。只替换 bot 解析、目标解析和
    最后的补投派发边界（记录次数与文案），不替换台账、inflight 槽与行渲染。
    """

    def _statuses(self) -> list[tuple]:
        return [
            (str(r[0]), r[1]) for r in self.tracker._conn.execute(
                "SELECT m_id, status FROM steer_ledger ORDER BY id"
            ).fetchall()
        ]

    def _run_case(
        self, *, http_status: int, transcript: str = "trigger",
        pop_during: bool = False, direct: bool = False,
        reply_before_terminal: bool = False,
    ) -> dict:
        registry = InflightRegistry()
        if not direct:
            self.assertTrue(registry.try_enter(
                KEY, msg(TRIGGER, 100, "running"), is_direct=True,
                original_msg_id="event-100", now_ms=100, commission=True,
            ))
        participants = {"u2"} if direct else set()
        active = MagicMock()
        active.is_alive_for_trigger.return_value = True
        active.pinned_participant_ids.side_effect = lambda *a, **k: set(participants)
        runtime = SimpleNamespace(
            run_tracker=self.tracker, inflight=registry, active_sessions=active,
        )
        started, release = asyncio.Event(), asyncio.Event()
        tasks: list = []
        requests: list[str] = []
        replays: list[tuple[str, str]] = []

        def spawn(coro, **kw):
            task = asyncio.create_task(coro)
            tasks.append(task)
            return task

        async def http(request):
            requests.append(request.url.path)
            if request.url.path.endswith("/steer"):
                started.set()
                await release.wait()
                return httpx.Response(
                    http_status,
                    json={"accepted": http_status == 200,
                          "error": {"message": "run is completed"}},
                )
            if request.url.path.endswith("/messages"):
                content = "[m:101]: alpha" if transcript == "has" else "primary trigger"
                return httpx.Response(
                    200, json={"data": [{"id": 1, "role": "user", "content": content}]}
                )
            raise AssertionError(str(request.url))

        transport = httpx.MockTransport(http)
        real_async_client = httpx.AsyncClient

        def http_factory(*a, **kw):
            return real_async_client(transport=transport, **kw)

        tgt = SimpleNamespace(base_url="http://offline.invalid", api_key="")
        row = msg("u2", 101, "alpha")

        def record_replay(tracked, text, user_id, nickname, now_ms, *, origin):
            replays.append((text, user_id))

        async def scenario() -> dict:
            async with real_async_client(transport=transport) as client:
                with patch.object(relay, "_mcp", runtime), \
                        patch.object(turns, "_mcp", runtime), \
                        patch.object(sc, "_mcp", runtime), \
                        patch.object(hc, "_get_http_client", lambda: client), \
                        patch.object(relay, "resolve_target", lambda *a, **k: tgt), \
                        patch.object(sc, "resolve_target", lambda *a, **k: tgt), \
                        patch.object(sc.httpx, "AsyncClient", http_factory), \
                        patch.object(hc, "spawn_bg_task", spawn), \
                        patch.object(sc, "spawn_bg_task", spawn), \
                        patch.object(relay, "spawn_bg_task", spawn), \
                        patch.object(sc, "_spawn_replay_turn", record_replay), \
                        patch.object(relay, "_claim_event_db_id",
                                     lambda adapter, event: 101), \
                        patch.object(relay, "steer_reply_push_active", lambda: False), \
                        patch.object(relay, "plugin_config",
                                     SimpleNamespace(hermes_api_interface="runs")), \
                        patch.object(relay, "_release_slot_and_retry",
                                     lambda key, *a, **k: registry.exit(key)), \
                        patch.object(relay, "_run_reactive_turn",
                                     lambda **kw: asyncio.sleep(0)):
                    if direct:
                        call = relay._maybe_steer_direct(
                            target=SimpleNamespace(private=False, id=GROUP_ID),
                            adapter_name=ADAPTER, user_id="u2", nickname="u2",
                            group_id=GROUP_ID, msg_text="alpha", is_direct=True,
                            now_ms=101, origin=MessageOrigin.human(), rows=[row],
                        )
                    else:
                        call = relay._handle_reactive_path(
                            bot=MagicMock(), target=SimpleNamespace(private=False, id=GROUP_ID),
                            adapter_name=ADAPTER, user_id="u2", nickname="u2",
                            group_id=GROUP_ID, text="alpha", image_urls=[],
                            is_direct=False, addressed_to_bot=False, now_ms=101,
                            origin=MessageOrigin.human(), rows=[row],
                            event_msg_id="event-101",
                        )
                    first = asyncio.create_task(call)
                    await asyncio.wait_for(started.wait(), 2)
                    # 资格判定发生在发送前, 这里清空只影响后续补投的 A/B 分派:
                    # 让补投走情形 B 原文直插(断言只看是否补投、补投了谁的原文)。
                    participants.clear()
                    if reply_before_terminal:
                        # 常规时序:HTTP 先被接受、账项转 pending,run 随后收尾。
                        release.set()
                        await first
                    before = self._statuses()
                    # 生产终态缝:run 收尾(与 _finish 同序)。
                    hc._spawn_steer_reconcile_if_pending(self.tracker, RUN_ID)
                    self.tracker.finish(RUN_ID)
                    while tasks:
                        batch, tasks[:] = tasks[:], []
                        await asyncio.gather(*batch)
                    after_terminal = self._statuses()
                    popped = None
                    popped_snapshot: list | None = None
                    if pop_during:
                        popped = registry.pop_next_pending(KEY, after_ts=100)
                        # 接力轮(_refire)拿的是 pop 时的列表对象本身;结算侧用
                        # 新列表赋值,不会改到这份快照——这里照抄同一语义。
                        popped_snapshot = list(popped.fragments) if popped is not None else None
                    if not reply_before_terminal:
                        release.set()
                        await first
                    while tasks:
                        batch, tasks[:] = tasks[:], []
                        await asyncio.gather(*batch)
                    entry = registry.peek_followup(KEY)
                    return {
                        "before": before,
                        "after_terminal": after_terminal,
                        "statuses": self._statuses(),
                        "requests": list(requests),
                        "replays": list(replays),
                        "slot_rows": [] if entry is None else [r.content for r in entry.fragments],
                        "slot_rendered": (
                            "" if entry is None or not entry.fragments
                            else render_current_rows(entry.fragments, is_private=False)
                        ),
                        "popped_rendered": (
                            None if popped_snapshot is None
                            else render_current_rows(popped_snapshot, is_private=False)
                        ),
                        "popped_rows": None if popped_snapshot is None else [
                            (r.id, r.content) for r in popped_snapshot
                        ],
                    }

        return asyncio.run(scenario())

    def test_terminal_while_sending_does_not_replay_and_409_relays_once(self) -> None:
        facts = self._run_case(http_status=409)
        # 初始:发送前认领是在途 sending,尚未被上游接受。
        self.assertEqual(facts["before"], [("101", "sending")])
        # 终态钩子不得把在途认领当「已接受但被吞」:不查转录、不改账、不补投。
        self.assertEqual(
            [p for p in facts["requests"] if p.endswith("/messages")], [],
            "终态钩子不得为未确认的在途认领去查转录",
        )
        self.assertEqual(facts["after_terminal"], [("101", "sending")])
        self.assertEqual(facts["replays"], [])
        # 409 后认领撤销,原 followup 留槽走接力:同一原始行只呈递一次。
        self.assertEqual(facts["statuses"], [])
        self.assertEqual(facts["slot_rows"], ["alpha"])
        self.assertEqual(facts["slot_rendered"].count("alpha"), 1)
        self.assertIn("[m:101]", facts["slot_rendered"])

    def test_negative_control_old_terminal_candidate_set_duplicates(self) -> None:
        """负对照:把 sending 放回终态候选(旧形态),同一时序确实呈递两次。"""
        with patch.object(rt, "_STEER_ACCEPTED_STATUSES", "'pending','sending'"):
            facts = self._run_case(http_status=409)
        # 终态把在途认领当缺件补投一次(replayed),409 又撤不掉它,原行仍留槽
        # → 补投 + 接力 = 两行 alpha;这正是评审 R3 的重复形态。
        self.assertEqual(facts["replays"], [("alpha", "u2")])
        self.assertEqual(facts["statuses"], [("101", "replayed")])
        self.assertEqual(facts["slot_rows"], ["alpha"])
        self.assertEqual(
            len(facts["replays"]) + facts["slot_rendered"].count("alpha"), 2,
            "旧形态同一原始行会呈递两次",
        )

    def test_late_200_after_terminal_hands_content_to_single_reconcile(self) -> None:
        facts = self._run_case(http_status=200)
        self.assertEqual(facts["after_terminal"], [("101", "sending")])
        # 晚到接受:确认后交给同一条对账路径;转录里没有这一行 → 恰好补投一次。
        self.assertEqual(facts["statuses"], [("101", "replayed")])
        self.assertEqual(facts["replays"], [("alpha", "u2")])
        self.assertEqual(facts["slot_rows"], [], "已呈递(补投)的行不得再留在接力槽里")

    def test_late_200_seen_in_transcript_is_reconciled_not_replayed(self) -> None:
        facts = self._run_case(http_status=200, transcript="has")
        # 上游确实把这一行写进了转录:核销,不补投(修重复不能改成静默丢内容)。
        self.assertEqual(facts["statuses"], [("101", "reconciled")])
        self.assertEqual(facts["replays"], [])
        self.assertEqual(facts["slot_rows"], [])

    def test_late_200_after_entry_popped_does_not_replay_again(self) -> None:
        facts = self._run_case(http_status=200, pop_during=True)
        # 条目在 HTTP 在途时已被 pop_next_pending 取走接力:接力轮手里的快照
        # 会呈递这一行, 对账再补投就是第二次。账项按「已由接力回收」核销。
        self.assertEqual(facts["statuses"], [("101", "recovered")])
        self.assertEqual(facts["replays"], [])
        self.assertEqual(facts["popped_rows"], [(101, "alpha")])
        self.assertEqual(facts["popped_rendered"].count("alpha"), 1)

    def test_direct_late_200_after_terminal_replays_once(self) -> None:
        facts = self._run_case(http_status=200, direct=True)
        self.assertEqual(facts["after_terminal"], [("101", "sending")])
        self.assertEqual(facts["statuses"], [("101", "replayed")])
        self.assertEqual(facts["replays"], [("alpha", "u2")])

    def test_accepted_before_terminal_still_replays_swallowed_row(self) -> None:
        """对照:HTTP 200 先到、run 后收尾——已接受的缺件仍由终态对账补投一次。"""
        facts = self._run_case(
            http_status=200, direct=True, reply_before_terminal=True,
        )
        self.assertEqual(facts["before"], [("101", "pending")])
        # 终态钩子按已接受账项对账,转录缺件 → 补投恰好一次(修重复不引入静默丢内容)。
        self.assertEqual(facts["after_terminal"], [("101", "replayed")])
        self.assertEqual(facts["replays"], [("alpha", "u2")])

    def test_concurrent_reconciles_claim_once(self) -> None:
        """终态钩子与晚到确认各起一次对账:原子认领保证只补投一次。"""
        a = "[id=u2] [m:101]: alpha"
        self.legacy_ledger_rows([(101, "u2", a)])
        tracked = self.tracker.get_by_run_id(RUN_ID)
        replays: list[tuple[str, str]] = []
        transport = httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"data": [{"id": 1, "role": "user", "content": "primary trigger"}]}
            )
        )
        real_async_client = httpx.AsyncClient

        def http_factory(*args, **kwargs):
            return real_async_client(transport=transport, **kwargs)

        with patch.object(sc, "_mcp", SimpleNamespace(run_tracker=self.tracker)), \
                patch.object(sc, "resolve_target",
                             lambda *a, **k: SimpleNamespace(base_url="http://x", api_key="")), \
                patch.object(sc.httpx, "AsyncClient", http_factory), \
                patch.object(sc, "_spawn_replay_turn",
                             lambda tracked, text, uid, nick, now, **kw: replays.append((text, uid))):
            async def both():
                await asyncio.gather(
                    sc.reconcile_run_finished(tracked),
                    sc.reconcile_run_finished(tracked),
                )
            asyncio.run(both())
        self.assertEqual(replays, [("alpha", "u2")])
        self.assertEqual(self._statuses(), [("101", "replayed")])

    def test_restart_returns_unfinished_reconcile_claim_to_pending(self) -> None:
        a = "[id=u2] [m:101]: alpha"
        self.legacy_ledger_rows([(101, "u2", a)])
        self.tracker._conn.execute(
            "UPDATE steer_ledger SET status='reconciling' WHERE m_id='101'"
        )
        self.tracker._conn.commit()
        reopened = RunTracker(Path(self.tracker._conn.execute("PRAGMA database_list").fetchone()[2]))
        self.addCleanup(reopened.close)
        self.assertEqual(
            [tuple(r) for r in reopened._conn.execute(
                "SELECT m_id, status FROM steer_ledger"
            ).fetchall()],
            [("101", "pending")],
        )


class NativeHandoverTests(_Base):
    """R4:native 归还 × 仍在 followup 槽的原行,下一轮只呈递一次。

    真实 `_handle_reactive_path` 的 followup 注入(`_try_steer_followup`)、真实
    `_steer_text_into_live_run`/SQLite 台账/`InflightRegistry`、真实
    `_pending_steer_recovery_rows` 与 `recover_by_text`、真实 pop 与
    `render_current_rows`。只替换 steer HTTP 本体(等 ack 事件)、目标解析与 bot。
    """

    def _chain(
        self,
        *,
        queued: list[BufferedMessage],
        pending_text: str,
        http_ok: bool = True,
        ack_before_pop: bool = False,
        legacy_handover: bool = False,
        legacy_partial_merge: bool = False,
        ledger_prefix: list[tuple[int, str, str]] | None = None,
    ) -> dict:
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        if ledger_prefix:
            # 前序行已被接受、甚至已 ACK 摘槽:台账里仍留着身份,供 native 认行。
            self.legacy_ledger_rows(ledger_prefix)
        if queued:
            registry.add_followup(KEY, PendingEntry(
                msg=queued[0], is_direct=False,
                original_msg_id=queued[0].id, fragments=list(queued),
            ))
        active = MagicMock()
        active.is_alive_for_trigger.return_value = True
        runtime = SimpleNamespace(
            run_tracker=self.tracker, inflight=registry, active_sessions=active,
        )
        started, release = asyncio.Event(), asyncio.Event()
        sent: list[str] = []

        async def steer(*, tgt, run_id, text):
            sent.append(text)
            started.set()
            await release.wait()
            return http_ok

        def hand_over() -> tuple[str, ...]:
            with patch.object(turns, "_mcp", runtime):
                rec = turns._pending_steer_recovery_rows(
                    pending_text, adapter_name=ADAPTER, is_private=False,
                    user_id=TRIGGER, group_id=GROUP_ID, now_ms=101,
                )
            self.assertIsNotNone(rec, "native 回收必须能按台账逐行还原")
            entry = PendingEntry(
                msg=BufferedMessage(
                    ts=101, adapter=ADAPTER, group_id=GROUP_ID, user_id=TRIGGER,
                    nickname=TRIGGER, content=sc.strip_steer_presentation(pending_text),
                    origin=MessageOrigin.human_replay(), image_urls=[],
                    sticker_urls=[], reply_to_ts=None, is_bot=False,
                ),
                is_direct=False,
                fragments=[fragment for _m_id, fragment in rec],
            )
            if legacy_handover:
                # 负对照:修复前 native 回收只是 add_followup,不看槽里是否已有原行。
                registry.add_followup(KEY, entry)
                absorbed: tuple[str, ...] = ()
            else:
                absorbed = registry.add_recovered_followup(
                    KEY, entry, known_m_ids=[m for m, _ in rec],
                )
            self.tracker.recover_by_text(pending_text)
            return absorbed

        async def scenario() -> dict:
            task = asyncio.create_task(
                relay._try_steer_followup(KEY, ADAPTER, TRIGGER, GROUP_ID)
            )
            await asyncio.wait_for(started.wait(), 2)
            # 与生产同序:run 在自己 turn 里收尾(native pending_steer 上浮),随后
            # 外壳才能 pop 接力;steer 的 HTTP 回应仍挂在网络上。
            self.tracker.finish(RUN_ID)
            slot_before_handover = registry.peek_followup(KEY)
            queued_objects = (
                [] if slot_before_handover is None else list(slot_before_handover.fragments)
            )
            absorbed = hand_over()
            slot_after_handover = registry.peek_followup(KEY)
            handover_rows = (
                None if slot_after_handover is None
                else [(r.id, r.content) for r in slot_after_handover.fragments]
            )
            if ack_before_pop:
                release.set()
                await task
            pending = registry.pop_next_pending(KEY, after_ts=100)
            rendered = (
                None if pending is None
                else render_current_rows(pending.fragments, is_private=False)
            )
            identities = (
                [] if pending is None
                else [
                    (fragment.id, any(fragment is row for row in queued_objects))
                    for fragment in pending.fragments
                ]
            )
            if not ack_before_pop:
                release.set()
                await task
            return {
                "sent": list(sent),
                "absorbed": absorbed,
                "handover_rows": handover_rows,
                "rendered": rendered,
                "identities": identities,
                "slot_final": registry.peek_followup(KEY),
                "statuses": [
                    (str(r[0]), r[1]) for r in self.tracker._conn.execute(
                        "SELECT m_id, status FROM steer_ledger ORDER BY id"
                    ).fetchall()
                ],
            }

        with patch.object(relay, "_mcp", runtime), \
                patch.object(relay, "hermes_client", SimpleNamespace(steer_run=steer)), \
                patch.object(relay, "resolve_target", lambda *a, **k: object()), \
                patch.object(relay, "steer_reply_push_active", lambda: False), \
                patch.object(
                    inflight_mod, "_merge_recovered_batch",
                    _legacy_merge_recovered_batch if legacy_partial_merge
                    else inflight_mod._merge_recovered_batch,
                ):
            return asyncio.run(scenario())

    def test_native_before_ack_presents_original_row_once(self) -> None:
        """R4 主场景:回收先到、ACK 晚到,原行只呈递一次(保留原 m:id+媒体身份)。"""
        facts = self._chain(
            queued=[msg("u2", 101, "alpha")], pending_text="[id=u2] [m:101]: alpha",
        )
        self.assertEqual(facts["sent"], ["[id=u2] [m:101]: alpha"])
        # 槽里已有同一原始行:回收副本被吸收,不再追加第二份。
        self.assertEqual(facts["absorbed"], ("101",))
        self.assertEqual(facts["handover_rows"], [(101, "alpha")])
        self.assertEqual(facts["rendered"], "[m:101] [id=u2]: alpha")
        # 晚到的 200 不得把「已被 native 归还、仍欠一次呈递」的行从槽里摘掉。
        self.assertEqual(facts["statuses"], [("101", "recovered")])
        self.assertEqual(facts["rendered"].count("alpha"), 1)

    def test_ack_before_pop_also_presents_once(self) -> None:
        """对照:ACK 先到(结算时已 recovered)→ 槽保留原行,下一轮仍一份。"""
        facts = self._chain(
            queued=[msg("u2", 101, "alpha")], pending_text="[id=u2] [m:101]: alpha",
            ack_before_pop=True,
        )
        self.assertEqual(facts["absorbed"], ("101",))
        self.assertEqual(facts["rendered"], "[m:101] [id=u2]: alpha")
        self.assertEqual(facts["statuses"], [("101", "recovered")])

    def test_negative_control_plain_add_followup_duplicates_row(self) -> None:
        """负对照:修复前只 add_followup(不看槽内原行),同一原行确实呈递两次。"""
        facts = self._chain(
            queued=[msg("u2", 101, "alpha")], pending_text="[id=u2] [m:101]: alpha",
            legacy_handover=True,
        )
        self.assertEqual(facts["absorbed"], ())
        self.assertEqual(
            facts["handover_rows"], [(101, "alpha"), (None, "alpha")],
            "旧形态:回收副本与原行同时在槽",
        )
        self.assertEqual(facts["rendered"].count("alpha"), 2)

    def test_409_after_native_keeps_row_for_relay_once(self) -> None:
        """native 归还后 steer 返回 409:撤认领、原行留槽,下一轮一份。"""
        facts = self._chain(
            queued=[msg("u2", 101, "alpha")], pending_text="[id=u2] [m:101]: alpha",
            http_ok=False,
        )
        self.assertEqual(facts["absorbed"], ("101",))
        self.assertEqual(facts["rendered"], "[m:101] [id=u2]: alpha")
        self.assertEqual(facts["statuses"], [("101", "recovered")])

    def test_recovered_row_not_in_slot_is_still_appended(self) -> None:
        """原行不在槽里(已被 ACK 摘除/从未排队):回收副本照旧补一次,内容不丢。"""
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        self.legacy_ledger_rows([(101, "u2", "[id=u2] [m:101]: alpha")])
        runtime = SimpleNamespace(run_tracker=self.tracker, inflight=registry)
        with patch.object(turns, "_mcp", runtime):
            rec = turns._pending_steer_recovery_rows(
                "[id=u2] [m:101]: alpha", adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=101,
            )
        self.assertIsNotNone(rec)
        entry = PendingEntry(
            msg=BufferedMessage(
                ts=101, adapter=ADAPTER, group_id=GROUP_ID, user_id=TRIGGER,
                nickname=TRIGGER, content="alpha", origin=MessageOrigin.human_replay(),
                image_urls=[], sticker_urls=[], reply_to_ts=None, is_bot=False,
            ),
            is_direct=False,
            fragments=[fragment for _m_id, fragment in rec],
        )
        absorbed = registry.add_recovered_followup(
            KEY, entry, known_m_ids=[m for m, _ in rec],
        )
        self.tracker.recover_by_text("[id=u2] [m:101]: alpha")
        pending = registry.pop_next_pending(KEY, after_ts=100)
        self.assertIsNotNone(pending)
        self.assertEqual(absorbed, ())
        self.assertEqual([(r.id, r.content) for r in pending.fragments], [(None, "alpha")])
        self.assertEqual(render_current_rows(pending.fragments, is_private=False),
                         "[id=u2]: alpha")
        self.assertEqual(
            [(str(r[0]), r[1]) for r in self.tracker._conn.execute(
                "SELECT m_id, status FROM steer_ledger ORDER BY id"
            ).fetchall()],
            [("101", "recovered")],
        )

    def test_partial_absorption_appends_only_missing_rows(self) -> None:
        """部分与槽重叠:只追加槽里没有的回收行,顺序保留,批级正文不重复。"""
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        registry.add_followup(KEY, PendingEntry(
            msg=msg("u2", 101, "alpha"), is_direct=False,
            original_msg_id=101, fragments=[msg("u2", 101, "alpha")],
        ))
        self.legacy_ledger_rows([
            (101, "u2", "[id=u2] [m:101]: alpha"),
            (102, "u3", "[id=u3] [m:102]: beta"),
        ])
        runtime = SimpleNamespace(run_tracker=self.tracker, inflight=registry)
        pending_text = "[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta"
        with patch.object(turns, "_mcp", runtime):
            rec = turns._pending_steer_recovery_rows(
                pending_text, adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=101,
            )
        self.assertIsNotNone(rec)
        entry = PendingEntry(
            msg=BufferedMessage(
                ts=101, adapter=ADAPTER, group_id=GROUP_ID, user_id=TRIGGER,
                nickname=TRIGGER, content=sc.strip_steer_presentation(pending_text),
                origin=MessageOrigin.human_replay(), image_urls=[],
                sticker_urls=[], reply_to_ts=None, is_bot=False,
            ),
            is_direct=False,
            fragments=[fragment for _m_id, fragment in rec],
        )
        absorbed = registry.add_recovered_followup(
            KEY, entry, known_m_ids=[m for m, _ in rec],
        )
        slot = registry.peek_followup(KEY)
        self.assertEqual(absorbed, ("101",))
        self.assertEqual(
            [(r.id, r.user_id, r.content) for r in slot.fragments],
            [(101, "u2", "alpha"), (None, "u3", "beta")],
        )
        # 被吸收行的正文不得再并进批级载体(避免同一条内容以文本形式出现两次)。
        # 混合来源(原行 human + 回收 human_replay)按既有权重只保留首条批级正文,
        # 逐段正文由 fragments 承担。
        self.assertEqual(slot.msg.content.count("alpha"), 1)
        self.assertEqual(
            [r.content for r in slot.fragments].count("beta"), 1,
        )

    def test_partial_absorption_tail_row_restores_payload_order(self) -> None:
        """R5:槽里只剩尾行 B、native 按 A/B 归还 → 缺行 A 补在 B 之前,顺序 A/B。"""
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        tail = msg("u3", 102, "beta")
        registry.add_followup(KEY, PendingEntry(
            msg=tail, is_direct=False, original_msg_id=102, fragments=[tail],
        ))
        self.legacy_ledger_rows([
            (101, "u2", "[id=u2] [m:101]: alpha"),
            (102, "u3", "[id=u3] [m:102]: beta"),
        ])
        entry, known = self._recovery_entry(
            "[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
        )
        tail_fragment = registry.peek_followup(KEY).fragments[0]
        absorbed = registry.add_recovered_followup(KEY, entry, known_m_ids=known)
        slot = registry.peek_followup(KEY)
        self.assertEqual(absorbed, ("102",))
        self.assertEqual(
            [(r.id, r.user_id, r.content) for r in slot.fragments],
            [(None, "u2", "alpha"), (102, "u3", "beta")],
        )
        # 槽内原 fragment 对象原样保留(真实 m:id/作者/媒体),回收副本只补缺行。
        self.assertIs(slot.fragments[1], tail_fragment)
        self.assertEqual(
            render_current_rows(slot.fragments, is_private=False),
            "[id=u2]: alpha\n[m:102] [id=u3]: beta",
        )
        self.assertEqual(slot.msg.content.count("alpha"), 0)

    def test_partial_absorption_keeps_unrelated_queued_row(self) -> None:
        """不属于这批的排队行不被吞、不被重排:仍在自己的位置两侧。"""
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        other = msg("u4", 103, "gamma")
        tail = msg("u3", 102, "beta")
        registry.add_followup(KEY, PendingEntry(
            msg=other, is_direct=False, original_msg_id=103,
            fragments=[other, tail],
        ))
        self.legacy_ledger_rows([
            (101, "u2", "[id=u2] [m:101]: alpha"),
            (102, "u3", "[id=u3] [m:102]: beta"),
        ])
        entry, known = self._recovery_entry(
            "[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
        )
        queued_fragments = list(registry.peek_followup(KEY).fragments)
        absorbed = registry.add_recovered_followup(KEY, entry, known_m_ids=known)
        slot = registry.peek_followup(KEY)
        self.assertEqual(absorbed, ("102",))
        self.assertEqual(
            [(r.id, r.content) for r in slot.fragments],
            [(103, "gamma"), (None, "alpha"), (102, "beta")],
        )
        self.assertIs(slot.fragments[0], queued_fragments[0])
        self.assertIs(slot.fragments[2], queued_fragments[1])

    def test_all_present_rows_follow_payload_order(self) -> None:
        """整批行都在槽但槽内顺序与原文相反 → 按 pending 原文校正,对象不变。"""
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        a, b = msg("u2", 101, "alpha"), msg("u3", 102, "beta")
        registry.add_followup(KEY, PendingEntry(
            msg=b, is_direct=False, original_msg_id=102, fragments=[b, a],
        ))
        self.legacy_ledger_rows([
            (101, "u2", "[id=u2] [m:101]: alpha"),
            (102, "u3", "[id=u3] [m:102]: beta"),
        ])
        entry, known = self._recovery_entry(
            "[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
        )
        queued_fragments = list(registry.peek_followup(KEY).fragments)
        absorbed = registry.add_recovered_followup(KEY, entry, known_m_ids=known)
        slot = registry.peek_followup(KEY)
        self.assertEqual(absorbed, ("101", "102"))
        self.assertEqual([r.id for r in slot.fragments], [101, 102])
        self.assertIs(slot.fragments[0], queued_fragments[1])
        self.assertIs(slot.fragments[1], queued_fragments[0])

    def test_negative_control_append_reverses_partial_overlap(self) -> None:
        """负对照:第 4 轮「槽内先、缺行后补」在同一输入上确实呈递 B/A。"""
        a = BufferedMessage(
            ts=101, adapter=ADAPTER, group_id=GROUP_ID, user_id="u2", nickname="u2",
            content="alpha", origin=MessageOrigin.human_replay(), image_urls=[],
            sticker_urls=[], reply_to_ts=None, is_bot=False,
        )
        b = BufferedMessage(
            ts=101, adapter=ADAPTER, group_id=GROUP_ID, user_id="u3", nickname="u3",
            content="beta", origin=MessageOrigin.human_replay(), image_urls=[],
            sticker_urls=[], reply_to_ts=None, is_bot=False,
        )
        tail = msg("u3", 102, "beta")
        ordered, absorbed, kept = _legacy_merge_recovered_batch(
            [tail], [a, b], ["101", "102"],
        )
        self.assertEqual(absorbed, ["102"])
        self.assertEqual(kept, [a])
        self.assertEqual(
            [(r.id, r.content) for r in ordered], [(102, "beta"), (None, "alpha")],
        )
        self.assertEqual(
            render_current_rows(ordered, is_private=False),
            "[m:102] [id=u3]: beta\n[id=u2]: alpha",
        )

    def test_interleaved_unrelated_row_keeps_batch_and_position(self) -> None:
        """批内行之间夹着无关排队行:只补缺行,不把无关行挪到批后。"""
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        a = msg("u2", 101, "alpha")
        other = msg("u4", 103, "gamma")
        b = msg("u3", 102, "beta")
        registry.add_followup(KEY, PendingEntry(
            msg=a, is_direct=False, original_msg_id=101,
            fragments=[a, other, b],
        ))
        self.legacy_ledger_rows([
            (101, "u2", "[id=u2] [m:101]: alpha"),
            (102, "u3", "[id=u3] [m:102]: beta"),
        ])
        entry, known = self._recovery_entry(
            "[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
        )
        queued = list(registry.peek_followup(KEY).fragments)
        absorbed = registry.add_recovered_followup(KEY, entry, known_m_ids=known)
        slot = registry.peek_followup(KEY)
        self.assertEqual(absorbed, ("101", "102"))
        self.assertEqual(
            [(r.id, r.content) for r in slot.fragments],
            [(101, "alpha"), (103, "gamma"), (102, "beta")],
        )
        self.assertEqual(
            [slot.fragments[i] is queued[i] for i in range(3)], [True, True, True],
        )

    def test_missing_row_inserted_next_to_previous_batch_row(self) -> None:
        """缺行补在上一个批内行之后(而不是槽尾):槽 [无关, A] + 原文 A/B → 无关,A,B。"""
        registry = InflightRegistry()
        self.assertTrue(registry.try_enter(
            KEY, msg(TRIGGER, 100, "running"), is_direct=True,
            original_msg_id="event-100", now_ms=100, commission=True,
        ))
        other = msg("u4", 103, "gamma")
        a = msg("u2", 101, "alpha")
        registry.add_followup(KEY, PendingEntry(
            msg=other, is_direct=False, original_msg_id=103, fragments=[other, a],
        ))
        self.legacy_ledger_rows([
            (101, "u2", "[id=u2] [m:101]: alpha"),
            (102, "u3", "[id=u3] [m:102]: beta"),
        ])
        entry, known = self._recovery_entry(
            "[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
        )
        absorbed = registry.add_recovered_followup(KEY, entry, known_m_ids=known)
        slot = registry.peek_followup(KEY)
        self.assertEqual(absorbed, ("101",))
        self.assertEqual(
            [(r.id, r.content) for r in slot.fragments],
            [(103, "gamma"), (101, "alpha"), (None, "beta")],
        )

    def _recovery_entry(self, pending_text: str) -> tuple[PendingEntry, list]:
        """真实 `_pending_steer_recovery_rows` 构造 native 回收条目(含台账身份)。"""
        with patch.object(turns, "_mcp", SimpleNamespace(run_tracker=self.tracker)):
            rec = turns._pending_steer_recovery_rows(
                pending_text, adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=101,
            )
        self.assertIsNotNone(rec, "native 回收必须能按台账逐行还原")
        entry = PendingEntry(
            msg=BufferedMessage(
                ts=101, adapter=ADAPTER, group_id=GROUP_ID, user_id=TRIGGER,
                nickname=TRIGGER, content=sc.strip_steer_presentation(pending_text),
                origin=MessageOrigin.human_replay(), image_urls=[], sticker_urls=[],
                reply_to_ts=None, is_bot=False,
            ),
            is_direct=False,
            fragments=[fragment for _m_id, fragment in rec],
        )
        return entry, [m_id for m_id, _fragment in rec]

    def test_multi_author_rows_keep_identity_and_order_once(self) -> None:
        """多作者多行:槽内两行都被吸收,原 m:id/作者/顺序保留,各一份。"""
        a = msg("u2", 101, "alpha")
        b = msg("u3", 102, "beta")
        facts = self._chain(
            queued=[a, b],
            pending_text="[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
        )
        self.assertEqual(facts["absorbed"], ("101", "102"))
        self.assertEqual(facts["handover_rows"], [(101, "alpha"), (102, "beta")])
        self.assertEqual(
            facts["rendered"], "[m:101] [id=u2]: alpha\n[m:102] [id=u3]: beta",
        )
        self.assertEqual(facts["statuses"], [("101", "recovered"), ("102", "recovered")])

    def test_different_rows_with_same_body_are_not_content_deduped(self) -> None:
        """不同原始行同文:按 m_id 认行,槽内另一行不被正文去重吞掉。"""
        facts = self._chain(
            queued=[msg("u2", 101, "alpha"), msg("u3", 102, "alpha")],
            pending_text="[id=u2] [m:101]: alpha",
        )
        self.assertEqual(facts["absorbed"], ("101",))
        self.assertEqual(facts["handover_rows"], [(101, "alpha"), (102, "alpha")])
        self.assertEqual(facts["rendered"].count("alpha"), 2)
        self.assertIn("[m:102]", facts["rendered"])

    def test_partial_overlap_tail_row_keeps_payload_order(self) -> None:
        """R5 主场景:A 已 ACK 摘槽、B 仍在途,native 按 A/B 归还 → 下一轮 A/B 各一次。

        真实 followup steer 入口、真实台账、真实 `_pending_steer_recovery_rows`、
        真实 pop 与 `render_current_rows`;只把 steer HTTP 换成等 ACK 的替身。
        """
        facts = self._chain(
            ledger_prefix=[(101, "u2", "[id=u2] [m:101]: alpha")],
            queued=[msg("u3", 102, "beta")],
            pending_text="[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
        )
        # 只有 B 走真实入口(它的 ACK 仍在途);A 早已被接受并摘槽。
        self.assertEqual(facts["sent"], ["[id=u3] [m:102]: beta"])
        self.assertEqual(facts["absorbed"], ("102",))
        self.assertEqual(facts["handover_rows"], [(None, "alpha"), (102, "beta")])
        self.assertEqual(
            facts["rendered"], "[id=u2]: alpha\n[m:102] [id=u3]: beta",
        )
        # 槽内 B 的原 fragment 对象保留(真实 m:id/作者),回收只补缺行 A。
        self.assertEqual(facts["identities"], [(None, False), (102, True)])
        self.assertEqual(facts["rendered"].count("alpha"), 1)
        self.assertEqual(facts["rendered"].count("beta"), 1)
        self.assertEqual(
            facts["statuses"], [("101", "recovered"), ("102", "recovered")],
        )
        self.assertIsNone(facts["slot_final"])

    def test_partial_overlap_ack_first_control_same_order(self) -> None:
        """对照:两条 ACK 都先到(槽已空)时,回收仍按 A/B 各一份。"""
        facts = self._chain(
            ledger_prefix=[(101, "u2", "[id=u2] [m:101]: alpha")],
            queued=[msg("u3", 102, "beta")],
            pending_text="[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
            ack_before_pop=True,
        )
        self.assertEqual(facts["absorbed"], ("102",))
        self.assertEqual(
            facts["rendered"], "[id=u2]: alpha\n[m:102] [id=u3]: beta",
        )
        self.assertEqual(facts["rendered"].count("alpha"), 1)
        self.assertEqual(facts["rendered"].count("beta"), 1)

    def test_negative_control_append_reverses_tail_row_chain(self) -> None:
        """负对照:同一真实链走第 4 轮「缺行追加到槽尾」→ 模型边界收到 B/A。"""
        facts = self._chain(
            ledger_prefix=[(101, "u2", "[id=u2] [m:101]: alpha")],
            queued=[msg("u3", 102, "beta")],
            pending_text="[id=u2] [m:101]: alpha\n[id=u3] [m:102]: beta",
            legacy_partial_merge=True,
        )
        self.assertEqual(facts["absorbed"], ("102",))
        self.assertEqual(
            facts["handover_rows"], [(102, "beta"), (None, "alpha")],
            "旧形态:缺行 A 被追加到槽尾 B 之后",
        )
        self.assertEqual(
            facts["rendered"], "[m:102] [id=u3]: beta\n[id=u2]: alpha",
        )
        self.assertEqual(facts["rendered"].count("alpha"), 1)
        self.assertEqual(facts["rendered"].count("beta"), 1)


if __name__ == "__main__":
    unittest.main()
