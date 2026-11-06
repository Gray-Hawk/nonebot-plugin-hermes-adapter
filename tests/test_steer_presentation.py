"""P-07／P-08／P-09／P-18／P-19 回归:steer 逐行呈递与跟帖冻结。

本模块全部走**真实入口**:
  - `_maybe_steer_direct`（直注批量入口）
  - `_try_steer_followup`（followup 槽入口）
  - `_build_current_fragment`（触发 fragment 生产构造点）
  - `_run_reactive_turn` 的 native `pending_steer` 回收支

只在平台边界(HTTP `steer_run` 与平台 send)与「渲染出 prompts」处放替身；追踪器
（RunTracker/SQLite）、`InflightRegistry`、`BufferedMessage`、`MessageOrigin` 与
`steer_seen` 全是真的。每个场景都配一条**负对照**：同一输入按修复前的形态计算，
断言它确实会错（媒体错贴、媒体丢失、多作者归一人），而不是只证明新代码「看起来对」。
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import steer_seen  # noqa: E402
from nonebot_plugin_hermes_adapter.core.inflight import (  # noqa: E402
    InflightRegistry,
    PendingEntry,
    make_inflight_key,
)
from nonebot_plugin_hermes_adapter.core.message_buffer import (  # noqa: E402
    BufferedMessage,
    iter_message_media,
    media_label,
)
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.prompt_builder import (  # noqa: E402
    _format_speaker_tag,
    render_steer_rows,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import (  # noqa: E402
    RunTracker,
    make_scope_key,
)
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402

ADAPTER = "onebotv11"
GROUP_ID = "g1"
TRIGGER = "u1"
RUN_ID = "run-g"
KEY = make_inflight_key(ADAPTER, False, TRIGGER, GROUP_ID)


def msg(
    *, user: str, nickname: str, content: str, m_id: int | None = None,
    images: tuple[str, ...] = (), stickers: tuple[str, ...] = (),
    quote: tuple[str, ...] = (), forward: tuple[str, ...] = (),
) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=GROUP_ID, user_id=user, nickname=nickname,
        content=content, origin=MessageOrigin.human(), id=m_id,
        image_urls=list(images), sticker_urls=list(stickers),
        quote_image_urls=list(quote), forward_image_urls=list(forward),
    )


class _TrackerMixin:
    """真实 SQLite 台账 + 一条 group 在跑 run。"""

    def setUp(self) -> None:
        steer_seen._steer_seen_reset_for_test()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.scope_key = make_scope_key(ADAPTER, False, TRIGGER, GROUP_ID)
        self.tracker.upsert(
            run_id=RUN_ID, scope_key=self.scope_key, adapter=ADAPTER,
            is_private=False, user_id=TRIGGER, group_id=GROUP_ID, bot_self_id="999",
            session_key="sess", mode="reactive", expect_structured=True,
        )

    def ledger(self) -> list[dict]:
        return self.tracker.pending_steers(RUN_ID)

    def seen_ids(self) -> set[int]:
        return set(steer_seen._steer_seen_ids.get(KEY, set()))


class _TrackerCase(_TrackerMixin, unittest.TestCase):
    pass


def _runtime(tracker=None, inflight=None, **kw):
    return SimpleNamespace(
        run_tracker=tracker,
        active_sessions=kw.pop(
            "active_sessions", MagicMock(pinned_participant_ids=MagicMock(return_value=set()))
        ),
        inflight=inflight,
        **kw,
    )


class SteerRowPresentationTests(_TrackerCase):
    """P-07:多作者多行逐行呈递,正文/台账/去重同源。"""

    def _direct(self, rows, *, image_urls=None, sticker_urls=None) -> tuple[bool, list[str]]:
        captured: list[str] = []

        async def fake_steer_run(*, tgt, run_id, text):
            captured.append(text)
            return True

        with patch.object(relay, "_mcp", _runtime(self.tracker)), patch.object(
            relay, "hermes_client", SimpleNamespace(steer_run=fake_steer_run)
        ), patch.object(relay, "_peek_event_db_id", lambda *a, **k: None), patch.object(
            relay, "resolve_target", lambda *a, **k: object()
        ), patch.object(relay, "steer_reply_push_active", lambda: False):
            ok = asyncio.run(
                relay._maybe_steer_direct(
                    target=SimpleNamespace(private=False, id=GROUP_ID),
                    adapter_name=ADAPTER, user_id=TRIGGER, nickname="小光",
                    group_id=GROUP_ID, msg_text="A 这条先发\nB 再补一句",
                    is_direct=True, now_ms=1_700_000_000_000,
                    origin=MessageOrigin.human(),
                    image_urls=image_urls, sticker_urls=sticker_urls,
                    rows=rows,
                )
            )
        return ok, captured

    def test_two_authors_two_rows_keep_own_speaker_and_mid(self) -> None:
        row_a = msg(user="u1", nickname="小光", content="A 这条先发", m_id=101,
                    images=("https://example.invalid/own.png",),
                    quote=("https://example.invalid/quote.png",))
        row_b = msg(user="u2", nickname="小李", content="B 再补一句", m_id=102,
                    forward=("https://example.invalid/fwd.png",))

        ok, captured = self._direct(
            [row_a, row_b],
            image_urls=[*row_a.image_urls, *row_a.quote_image_urls],
        )

        self.assertTrue(ok)
        self.assertEqual(len(captured), 1)
        lines = captured[0].split("\n")
        self.assertEqual(len(lines), 2, captured[0])
        self.assertTrue(lines[0].startswith("[id=u1 user=小光] [m:101]: "), lines[0])
        self.assertTrue(lines[1].startswith("[id=u2 user=小李] [m:102]: "), lines[1])
        # 媒体寻址只贴本行自己的号与自己的 idx
        self.assertIn("[图 m:101#0]", lines[0])
        self.assertIn("[引用图 m:101#1]", lines[0])
        self.assertNotIn("m:102", lines[0])
        self.assertIn("[转发图 m:102#0]", lines[1])
        self.assertNotIn("m:101", lines[1])
        # 台账逐行,发送人/正文与实发文本逐字对齐
        ledger = self.ledger()
        self.assertEqual([(r["m_id"], r["sender_id"]) for r in ledger],
                         [("101", "u1"), ("102", "u2")])
        self.assertEqual([r["steer_text"] for r in ledger], lines)
        # 呈现去重名单同样从这份呈递派生
        self.assertEqual(self.seen_ids(), {101, 102})

    def test_negative_control_single_row_form_loses_authors_and_media_ids(self) -> None:
        """负对照:修复前形态(整段挂单一 speaker／m:id)在同一输入下确实错。"""
        row_a = msg(user="u1", nickname="小光", content="A 这条先发", m_id=101,
                    images=("https://example.invalid/own.png",),
                    quote=("https://example.invalid/quote.png",))
        row_b = msg(user="u2", nickname="小李", content="B 再补一句", m_id=102)
        old_form = (
            f"{_format_speaker_tag('小光', 'u1')} [m:101]: A 这条先发\nB 再补一句"
        )
        # 旧形态只有一条 m:id、一个 speaker:第二作者被吞,引用图只能贴首条号,
        # 且 idx 按 own+sticker 局部重算(引用图在感知库里是 #1,这里根本没有标签)
        self.assertEqual(old_form.count("[m:"), 1)
        self.assertNotIn("id=u2", old_form)
        self.assertNotIn("[引用图", old_form)

        ok, captured = self._direct(
            [row_a, row_b],
            image_urls=[*row_a.image_urls, *row_a.quote_image_urls],
        )
        self.assertTrue(ok)
        self.assertNotEqual(captured[0], old_form)
        self.assertIn("id=u2", captured[0])
        self.assertIn("[引用图 m:101#1]", captured[0])

    def test_same_row_twice_presented_and_ledged_once(self) -> None:
        row = msg(user="u1", nickname="小光", content="同一行", m_id=101)
        ok, captured = self._direct([row, row])
        self.assertTrue(ok)
        self.assertEqual(captured[0].split("\n"), ["[id=u1 user=小光] [m:101]: 同一行"])
        self.assertEqual(len(self.ledger()), 1)
        self.assertEqual(self.seen_ids(), {101})

    def test_conflicting_same_mid_refuses_whole_batch(self) -> None:
        """同号异作者/异正文是身份事实矛盾:发送前整批回退,不发、不入账。"""
        one = msg(user="u1", nickname="小光", content="第一版", m_id=101)
        two = msg(user="u2", nickname="小李", content="第二版", m_id=101)
        ok, captured = self._direct([one, two])
        self.assertFalse(ok)
        self.assertEqual(captured, [])
        self.assertEqual(self.ledger(), [])

    def test_steered_row_is_not_represented_in_next_history(self) -> None:
        row = msg(user="u1", nickname="小光", content="只出现一次", m_id=101)
        ok, _ = self._direct([row])
        self.assertTrue(ok)
        self.assertEqual(steer_seen._steer_seen_filter(KEY, [row]), [])


class MediaTypingTests(unittest.TestCase):
    """P-08:触发 fragment 的 own／quote／forward 分型与感知入库同序同 idx。"""

    def _fragment(self):
        from nonebot_plugin_hermes_adapter.handlers import message as message_mod

        with patch.object(message_mod, "_peek_event_db_id", lambda *a, **k: 501):
            return message_mod._build_current_fragment(
                now_ms=1, adapter_name=ADAPTER, group_id=GROUP_ID, user_id=TRIGGER,
                nickname="小光", msg_text="own 图 + 引用图 + 转发图",
                own_image_urls=["https://example.invalid/own.png"],
                sticker_urls=["https://example.invalid/sticker.png"],
                quote_image_urls=["https://example.invalid/quote.png"],
                forward_image_urls=["https://example.invalid/fwd.png"],
                delivery_generation=None, event_msg_id="evt-1",
            )

    def test_media_enumeration_matches_perception_order(self) -> None:
        fragment = self._fragment()
        self.assertEqual(
            [kind for kind, _u in iter_message_media(fragment)],
            ["image", "sticker", "quote", "forward_image"],
        )
        self.assertEqual(fragment.id, 501)

        line = render_steer_rows([fragment]).rows[0].rendered_line
        self.assertIn("[图 m:501#0]", line)
        self.assertIn("[表情包 m:501#1]", line)
        self.assertIn("[引用图 m:501#2]", line)
        self.assertIn("[转发图 m:501#3]", line)

    def test_negative_control_flat_image_urls_shift_quote_index(self) -> None:
        """负对照:引用图并进 image_urls 的旧构造会把它编到 #1,与入库 idx 错位。"""
        old_style = BufferedMessage(
            ts=1, adapter=ADAPTER, group_id=GROUP_ID, user_id=TRIGGER,
            nickname="小光", content="own 图 + 引用图", origin=MessageOrigin.human(),
            id=501,
            image_urls=["https://example.invalid/own.png", "https://example.invalid/quote.png"],
            sticker_urls=["https://example.invalid/sticker.png"],
        )
        old_labels = [
            media_label(kind, 501, idx)
            for idx, (kind, _u) in enumerate(iter_message_media(old_style))
        ]
        # 旧形态:引用图被当成第二张 own 图 → [图 m:501#1];感知库里的它是 quote#2
        self.assertIn("[图 m:501#1]", old_labels)
        self.assertNotIn("[引用图 m:501#2]", old_labels)

        fixed = self._fragment()
        fixed_labels = [
            media_label(kind, fixed.id, idx)
            for idx, (kind, _u) in enumerate(iter_message_media(fixed))
        ]
        self.assertIn("[引用图 m:501#2]", fixed_labels)


class FollowupSteerGateTests(_TrackerCase):
    """P-18／P-19:followup 冻结集与「先查媒体再过滤空行」。"""

    def setUp(self) -> None:
        super().setUp()
        self.registry = InflightRegistry()

    def _followup(self, entry_msg: BufferedMessage) -> PendingEntry:
        entry = PendingEntry(msg=entry_msg, is_direct=False, original_msg_id=entry_msg.id)
        self.registry.add_followup(KEY, entry)
        return entry

    def _try(self, *, during_await=None) -> tuple[bool, list[str]]:
        captured: list[str] = []

        async def fake_steer_run(*, tgt, run_id, text):
            captured.append(text)
            if during_await is not None:
                during_await()
            return True

        with patch.object(relay, "_mcp", _runtime(self.tracker, self.registry)), patch.object(
            relay, "hermes_client", SimpleNamespace(steer_run=fake_steer_run)
        ), patch.object(relay, "resolve_target", lambda *a, **k: object()), patch.object(
            relay, "steer_reply_push_active", lambda: False
        ):
            ok = asyncio.run(relay._try_steer_followup(KEY, ADAPTER, TRIGGER, GROUP_ID))
        return ok, captured

    # --- 纯文本:行为与旧路完全一致(E3 即时回应不受影响) ---

    def test_plain_text_followup_steers_with_own_mid_and_settles(self) -> None:
        entry = self._followup(
            msg(user="u3", nickname="小周", content="你怎么看", m_id=103)
        )
        ok, captured = self._try()
        self.assertTrue(ok)
        self.assertEqual(captured, ["[id=u3 user=小周] [m:103]: 你怎么看"])
        self.assertIsNone(self.registry.peek_followup(KEY), "纯文本注入成功后应摘槽")
        self.assertEqual(
            [(r["m_id"], r["sender_id"]) for r in self.ledger()], [("103", "u3")]
        )
        self.assertEqual(self.seen_ids(), {103})

    def test_pure_sticker_placeholder_stays_pending(self) -> None:
        entry = self._followup(msg(user="u3", nickname="小周", content="[表情包]", m_id=103))
        ok, captured = self._try()
        self.assertFalse(ok)
        self.assertEqual(captured, [])
        self.assertIs(self.registry.peek_followup(KEY), entry)

    def test_row_without_mid_is_pruned_by_object_identity(self) -> None:
        """无号行没有台账锚点:结算只能按对象身份摘,不能靠 m:id。"""
        entry = self._followup(
            msg(user="u3", nickname="小周", content="没有行号的跟帖", m_id=None)
        )
        ok, captured = self._try()
        self.assertTrue(ok)
        self.assertEqual(captured, ["[id=u3 user=小周]: 没有行号的跟帖"])
        self.assertIsNone(
            self.registry.peek_followup(KEY), "已呈现的无号行必须结算,留槽会重复呈递"
        )
        self.assertEqual(self.ledger(), [], "无号行不入账(对账无锚点)")

    # --- P-19:quote／forward 媒体不得随摘槽丢失 ---

    def test_quote_image_followup_stays_pending(self) -> None:
        entry = self._followup(msg(
            user="u3", nickname="小周", content="这张引用图你看懂没", m_id=104,
            quote=("https://example.invalid/q.png",),
        ))
        ok, captured = self._try()
        self.assertFalse(ok)
        self.assertEqual(captured, [], "带引用图的跟帖不许走文本通道注入")
        self.assertIs(self.registry.peek_followup(KEY), entry)
        self.assertTrue(iter_message_media(entry.msg))
        # 负对照:旧媒体门只看 own image/sticker,对它一律放行 → 会注入并摘槽
        self.assertFalse(bool(entry.msg.image_urls or entry.msg.sticker_urls))

    def test_forward_image_followup_stays_pending(self) -> None:
        entry = self._followup(msg(
            user="u3", nickname="小周", content="转发里那张图", m_id=105,
            forward=("https://example.invalid/f.png",),
        ))
        ok, captured = self._try()
        self.assertFalse(ok)
        self.assertEqual(captured, [])
        self.assertIs(self.registry.peek_followup(KEY), entry)

    def test_image_only_followup_row_is_seen_before_empty_filter(self) -> None:
        """图片-only 行 + 文本行同批:媒体门必须在空行过滤之前看到那张图。"""
        text_row = msg(user="u3", nickname="小周", content="先看这张", m_id=106)
        entry = self._followup(text_row)
        # 同槽并入一条纯引用图、无文字的跟帖(真实合并路径)。
        # 用引用图而不是 own 图:_merge_pending_message 只把 own 图片并进聚合 msg,
        # quote/forward 不并——所以只有引用/转发图才会被「聚合外壳媒体门」漏掉。
        self.registry.try_enter(
            KEY,
            msg(user="u4", nickname="小陈", content="", m_id=107,
                quote=("https://example.invalid/only.png",)),
            is_direct=False, original_msg_id=107, now_ms=2, commission=False,
        )
        self.assertEqual(len(entry.fragments), 2)
        # entry.msg(聚合外壳)本身没有引用媒体 —— 只看外壳的旧门永远看不到那张图
        self.assertEqual(iter_message_media(entry.msg), [])
        ok, captured = self._try()
        self.assertFalse(ok, "任一投影行带媒体即整批留 pending")
        self.assertEqual(captured, [])
        self.assertIs(self.registry.peek_followup(KEY), entry)
        # 负对照:先按空行过滤再查媒体,就会漏掉这条纯图行(顺序即缺陷)
        old_order = [row for row in entry.fragments if (row.content or "").strip()]
        self.assertEqual([r.user_id for r in old_order], ["u3"])
        self.assertEqual(
            [kind for r in old_order for kind, _u in iter_message_media(r)], []
        )

    def test_image_only_own_media_row_keeps_pending(self) -> None:
        """own 图-only 行(旧门也能看到的形态)同样整批留槽,不漏。"""
        entry = self._followup(msg(user="u3", nickname="小周", content="先看这张", m_id=112))
        self.registry.try_enter(
            KEY,
            msg(user="u3", nickname="小周", content="", m_id=113,
                images=("https://example.invalid/own-only.png",)),
            is_direct=False, original_msg_id=113, now_ms=2, commission=False,
        )
        ok, captured = self._try()
        self.assertFalse(ok)
        self.assertEqual(captured, [])
        self.assertIs(self.registry.peek_followup(KEY), entry)

    # --- P-18:await 期间并入 → 冻结集比对留槽 ---

    def test_media_only_merge_during_await_keeps_slot(self) -> None:
        entry = self._followup(msg(user="u3", nickname="小周", content="我再补一句", m_id=108))
        content_before = entry.msg.content

        def merge_media_only() -> None:
            self.registry.try_enter(
                KEY,
                msg(user="u4", nickname="小陈", content="", m_id=109,
                    quote=("https://example.invalid/late.png",)),
                is_direct=False, original_msg_id=109, now_ms=2, commission=False,
            )

        ok, _ = self._try(during_await=merge_media_only)
        self.assertTrue(ok, "冻结集已送出,注入成功")
        # 结算只摘已呈现的那一行:纯图行留在槽里走接力(P-07 起槽只持有未呈现
        # fragment)——既不会把已 steer 的前缀再呈递一遍,也不会丢晚到的图。
        self.assertEqual([r.id for r in entry.fragments], [109])
        self.assertIs(self.registry.peek_followup(KEY), entry, "await 期间并入 → 留槽接力")
        # 负对照:内容快照在这条路径上根本没变 —— 只比 content 的旧条件必然摘槽丢图
        self.assertEqual(entry.msg.content, content_before)
        self.assertIn("late.png", " ".join(
            url for row in entry.fragments for _k, url in iter_message_media(row)
        ))

    def test_text_merge_during_await_keeps_slot(self) -> None:
        entry = self._followup(msg(user="u3", nickname="小周", content="我再补一句", m_id=110))

        def merge_text() -> None:
            self.registry.try_enter(
                KEY,
                msg(user="u3", nickname="小周", content="还有一句", m_id=111),
                is_direct=False, original_msg_id=111, now_ms=2, commission=False,
            )

        ok, _ = self._try(during_await=merge_text)
        self.assertTrue(ok)
        self.assertIs(self.registry.peek_followup(KEY), entry)

    def test_cross_source_merge_during_await_keeps_slot(self) -> None:
        """跨源合并(真人行 + 系统事件行)同样不得摘槽:正文不变,身份已变。"""
        from nonebot_plugin_hermes_adapter.core.message_origin import MessageSource

        entry = self._followup(msg(user="u3", nickname="小周", content="我再补一句", m_id=114))
        content_before = entry.msg.content
        notice = BufferedMessage(
            ts=2, adapter=ADAPTER, group_id=GROUP_ID, user_id="system", nickname="系统",
            content="戳了戳你",
            origin=MessageOrigin.synthetic(MessageSource.NOTICE_POKE),
        )

        def merge_notice() -> None:
            self.registry.try_enter(
                KEY, notice, is_direct=False, original_msg_id=None, now_ms=2,
                commission=False,
            )

        ok, captured = self._try(during_await=merge_notice)
        # 冻结集在 await 前已定:injected 只含真人那一行(系统事件行不冒充 steer 输入);
        # 结算按对象身份发现新 fragment → 槽保留,系统行留给接力轮呈递
        self.assertEqual(captured, ["[id=u3 user=小周] [m:114]: 我再补一句"])
        self.assertTrue(ok)
        # 槽里只剩系统事件行(未呈现,交由接力轮呈递);已 steer 的真人行已摘。
        self.assertEqual(len(entry.fragments), 1)
        self.assertEqual(entry.fragments[0].content, "戳了戳你")
        self.assertIs(self.registry.peek_followup(KEY), entry)
        # 负对照:跨源合并(MessageOrigin.merge → MIXED)不拼正文,内容快照原样相等
        self.assertEqual(entry.msg.content, content_before)
        self.assertEqual(entry.msg.origin.source.value, "mixed")


class NativeRecoveryTests(_TrackerMixin, unittest.IsolatedAsyncioTestCase):
    """P-09:native 回收按既有台账逐行还原多作者;无台账整段 fallback。"""

    async def _record_multi_author_steer(self) -> str:
        """用真实直注入口记两行台账,返回上游形态的 pending_steer 拼接文本。"""
        captured: list[str] = []

        async def fake_steer_run(*, tgt, run_id, text):
            captured.append(text)
            return True

        rows = [
            msg(user="u2", nickname="小李", content="先别开枪", m_id=201),
            msg(user="u3", nickname="小周", content="等等我", m_id=202),
        ]
        with patch.object(relay, "_mcp", _runtime(self.tracker)), patch.object(
            relay, "hermes_client", SimpleNamespace(steer_run=fake_steer_run)
        ), patch.object(relay, "_peek_event_db_id", lambda *a, **k: None), patch.object(
            relay, "resolve_target", lambda *a, **k: object()
        ), patch.object(relay, "steer_reply_push_active", lambda: False):
            ok = await relay._maybe_steer_direct(
                target=SimpleNamespace(private=False, id=GROUP_ID),
                adapter_name=ADAPTER, user_id=TRIGGER, nickname="小光",
                group_id=GROUP_ID, msg_text="先别开枪\n等等我", is_direct=True,
                now_ms=1, origin=MessageOrigin.human(), rows=rows,
            )
        self.assertTrue(ok)
        self.assertEqual(len(self.ledger()), 2)
        return captured[0]

    async def _run_reactive_turn(self, result, *, tracker) -> InflightRegistry:
        """真实 `_run_reactive_turn`,只替换外部 I/O(与既有 harness 同形)。"""
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt
        from nonebot_plugin_hermes_adapter.core import prompt_builder

        registry = InflightRegistry()
        cfg = MagicMock()
        cfg.group_profile_enabled = False
        cfg.sticker_vision_enabled = False
        cfg.history_message_limit = 5
        cfg.interim_echo_enabled = False
        bot = MagicMock(self_id=1)
        target = SimpleNamespace(private=False, id=GROUP_ID)
        runtime = MagicMock(
            active_sessions=MagicMock(get_if_trigger_alive=MagicMock(return_value=None)),
            message_buffer=MagicMock(get_recent=MagicMock(return_value=[])),
            run_tracker=tracker,
            inflight=registry,
        )
        patches = {
            "load_misc_config": MagicMock(return_value=cfg),
            "load_chat_config": MagicMock(return_value=cfg),
            "load_media_config": MagicMock(return_value=cfg),
            "session_manager": MagicMock(
                get_push_watermark=MagicMock(return_value=None),
                get_session_key=MagicMock(return_value="sess"),
            ),
            "_current_delivery_generation": MagicMock(return_value=None),
            "resolve_target": MagicMock(return_value=object()),
            "_filter_current_and_steered": MagicMock(return_value=[]),
            "resolve_custom_dir": MagicMock(return_value=None),
            "build_reactive_system_prompt": MagicMock(return_value="sys"),
            "build_group_profile_block": MagicMock(return_value=""),
            "get_bot_nickname": AsyncMock(return_value="nano"),
            "split_unpushed_messages": MagicMock(return_value=([], None)),
            "ActiveSession": MagicMock(return_value=SimpleNamespace(
                generation=1, last_bot_reply_at=0, session_id="sess",
                last_bot_reply_text="", last_bot_reply_media=0,
                triggered_by="u1", topic_hint="", participants=set(),
                should_exit_active=False, interim_echo_enabled=False,
            )),
            "_chat_with_overflow_recovery": AsyncMock(return_value=result),
            "_settle": MagicMock(),
            "_record_background_note": MagicMock(),
            "_delivery_identity": MagicMock(
                side_effect=lambda res: (tracker, getattr(res, "run_id", None))
            ),
            "_schedule_reconcile": MagicMock(),
            "_finish_history": MagicMock(),
            "send_text_with_media": AsyncMock(
                return_value=SimpleNamespace(ok=True, msg_ids=["m:send"])
            ),
            "send_local_files": AsyncMock(return_value=0),
            "task_notes": MagicMock(push_plugin_note=MagicMock()),
            "_mcp": runtime,
            "_maybe_intercept_bounce": AsyncMock(return_value=False),
            "_post_send_feedback": AsyncMock(),
            "_collect_inline_image_urls": MagicMock(return_value=[]),
            "inline_image_frames": AsyncMock(return_value=[]),
            "inline_gif_frames": AsyncMock(return_value=[]),
            "_absent_media_note": MagicMock(return_value=""),
            "build_media_address_index": MagicMock(return_value={}),
            "deliver_companion_run_output": AsyncMock(return_value=False),
            "resolve_reply": MagicMock(return_value=None),
        }
        prompts = patch.object(
            prompt_builder, "render_current_rows", MagicMock(return_value="hi")
        )
        prompts.start()
        self.addCleanup(prompts.stop)
        with patch.multiple(mt, **patches):
            await mt._run_reactive_turn(
                bot=bot, target=target, adapter_name=ADAPTER,
                user_id=TRIGGER, group_id=GROUP_ID, text="hi", image_urls=[],
                is_direct=True, addressed_to_bot=True, now_ms=1,
                recent_override=[], ephemeral=True,
            )
        return registry

    async def test_native_recovery_restores_authors_in_order(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        pending_text = await self._record_multi_author_steer()
        result = ChatResult(raw_text="", pending_steer=pending_text)

        registry = await self._run_reactive_turn(result, tracker=self.tracker)

        entry = registry.peek_followup(KEY)
        self.assertIsNotNone(entry, "native 回收必须排回 pending")
        self.assertEqual(
            [(row.user_id, row.content) for row in entry.fragments],
            [("u2", "先别开枪"), ("u3", "等等我")],
        )
        # 回收行是 human_replay:无号、不重复入库,台账供追溯
        self.assertTrue(all(row.id is None for row in entry.fragments))
        self.assertTrue(all(row.origin.is_pure_human for row in entry.fragments))
        self.assertEqual(self.ledger(), [], "native 上浮即核销台账,防双重投递")

    async def test_negative_control_without_ledger_attributes_everything_to_trigger(self) -> None:
        """负对照:无可靠台账时只能整段 fallback —— 多作者被归给 run 发起者。"""
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        pending_text = await self._record_multi_author_steer()
        empty_tracker = RunTracker(Path(tempfile.mkdtemp()) / "empty.db")
        self.addCleanup(empty_tracker.close)

        registry = await self._run_reactive_turn(
            ChatResult(raw_text="", pending_steer=pending_text), tracker=empty_tracker
        )

        entry = registry.peek_followup(KEY)
        self.assertIsNotNone(entry)
        self.assertEqual(len(entry.fragments), 1, "fallback 只有一条聚合行")
        self.assertEqual(entry.fragments[0].user_id, TRIGGER)
        self.assertIn("先别开枪", entry.fragments[0].content)
        self.assertIn("等等我", entry.fragments[0].content)
        self.assertNotIn("u2", entry.fragments[0].user_id)

    async def test_two_separate_steers_with_convention_still_restore_authors(self) -> None:
        """多条 steer 各自成批(每批都挂约定行)时仍能逐行还原。"""
        captured: list[str] = []

        async def fake_steer_run(*, tgt, run_id, text):
            captured.append(text)
            return True

        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        for user, nickname, content, m_id in (
            ("u2", "小李", "先别开枪", 301),
            ("u3", "小周", "等等我", 302),
        ):
            with patch.object(relay, "_mcp", _runtime(self.tracker)), patch.object(
                relay, "hermes_client", SimpleNamespace(steer_run=fake_steer_run)
            ), patch.object(relay, "_peek_event_db_id", lambda *a, **k: None), patch.object(
                relay, "resolve_target", lambda *a, **k: object()
            ), patch.object(relay, "steer_reply_push_active", lambda: True):
                ok = await relay._maybe_steer_direct(
                    target=SimpleNamespace(private=False, id=GROUP_ID),
                    adapter_name=ADAPTER, user_id=TRIGGER, nickname="小光",
                    group_id=GROUP_ID, msg_text=content, is_direct=True,
                    now_ms=1, origin=MessageOrigin.human(),
                    rows=[msg(user=user, nickname=nickname, content=content, m_id=m_id)],
                )
            self.assertTrue(ok)

        pending_text = "\n".join(captured)
        self.assertEqual(pending_text.count(relay._STEER_REPLY_CONVENTION_LINE), 2)

        with patch.object(mt, "_mcp", _runtime(self.tracker)):
            rows = mt._pending_steer_recovery_fragments(
                pending_text, adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=1,
            )
        self.assertEqual([(r.user_id, r.content) for r in rows or []],
                         [("u2", "先别开枪"), ("u3", "等等我")])

    async def test_partial_ledger_match_falls_back_whole_segment(self) -> None:
        """部分命中不做半还原:宁可作者回退,也不让未命中正文从呈递中消失。"""
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        pending_text = await self._record_multi_author_steer()
        with patch.object(mt, "_mcp", _runtime(self.tracker)):
            rows = mt._pending_steer_recovery_fragments(
                pending_text, adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=1,
            )
            self.assertEqual([r.user_id for r in rows or []], ["u2", "u3"])

            # 台账只覆盖前一行,另外补一段台账里没有的正文 → 整段 fallback
            ledger_row = self.ledger()[0]
            tampered = ledger_row["steer_text"] + "\n[id=u9 user=野的]: 台账外的正文"
            self.assertIsNone(mt._pending_steer_recovery_fragments(
                tampered, adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=1,
            ))

            # 无台账(tracker 缺失)也走整段 fallback,不猜作者
            with patch.object(mt, "_mcp", _runtime(None)):
                self.assertIsNone(mt._pending_steer_recovery_fragments(
                    pending_text, adapter_name=ADAPTER, is_private=False,
                    user_id=TRIGGER, group_id=GROUP_ID, now_ms=1,
                ))


if __name__ == "__main__":
    unittest.main()
