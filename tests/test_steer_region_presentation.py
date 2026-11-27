"""steer 正文零包装回归：批形状、真入口、呈递标记与 native 回收。

零包装后模型可见的 steer 文本只有原始正文：没有区域标、固定说明或 ``[m:id]`` 行头；
身份、m:id 与顺序仍由 ``SteerRowPresentation.source_row`` 与 steer 台账保留。
网关薄插件按 ``format=plain_body`` 跳上游外壳，文本原样落成 user 行。
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_presentation  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_seen  # noqa: E402
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

render_steer_rows = pb.render_steer_rows
render_steer_batch = pb.render_steer_batch
steer_batch_decorations = pb.steer_batch_decorations

ADAPTER = "onebotv11"
GROUP_ID = "g-1"
TRIGGER = "10001"
RUN_ID = "run-steer-zero"
KEY = make_inflight_key(ADAPTER, False, TRIGGER, GROUP_ID)
SELF_ID = "999"

CONVENTION = relay._STEER_REPLY_CONVENTION_LINE


def msg(
    *, user: str, nickname: str, content: str, m_id: int | None = None,
    images: tuple[str, ...] = (), quote: tuple[str, ...] = (),
) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=GROUP_ID, user_id=user, nickname=nickname,
        content=content, origin=MessageOrigin.human(), id=m_id,
        image_urls=list(images), quote_image_urls=list(quote),
    )


def _runtime(tracker=None, inflight=None) -> SimpleNamespace:
    return SimpleNamespace(
        run_tracker=tracker,
        active_sessions=MagicMock(pinned_participant_ids=MagicMock(return_value=set())),
        inflight=inflight,
    )


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        steer_seen._steer_seen_reset_for_test()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.tracker.upsert(
            run_id=RUN_ID, scope_key=make_scope_key(ADAPTER, False, TRIGGER, GROUP_ID),
            adapter=ADAPTER, is_private=False, user_id=TRIGGER, group_id=GROUP_ID,
            bot_self_id=SELF_ID, session_key="sess", mode="reactive",
            expect_structured=True,
        )
        self.sent: list[dict] = []

    async def _fake_steer_run(self, *, tgt, run_id, text, presented=False, presentation_hint=""):
        self.sent.append({
            "text": text, "presented": presented, "run_id": run_id,
            "hint": presentation_hint,
        })
        return True

    def _patch_common(self):
        return (
            patch.object(relay, "_mcp", _runtime(self.tracker)),
            patch.object(relay, "hermes_client", SimpleNamespace(steer_run=self._fake_steer_run)),
            patch.object(relay, "resolve_target", lambda *a, **k: object()),
            patch.object(relay, "_peek_event_db_id", lambda *a, **k: None),
        )

    def _inject_direct(self, rows) -> bool:
        with ExitStack() as stack:
            for item in self._patch_common():
                stack.enter_context(item)
            stack.enter_context(patch.object(relay, "steer_reply_push_active", lambda: False))
            return asyncio.run(relay._maybe_steer_direct(
                target=SimpleNamespace(private=False, id=GROUP_ID),
                adapter_name=ADAPTER,
                user_id=TRIGGER,
                nickname=rows[0].nickname if rows else "小光",
                group_id=GROUP_ID,
                msg_text=rows[0].content if rows else "",
                is_direct=True, now_ms=1_700_000_000_000,
                origin=MessageOrigin.human(), rows=rows,
            ))

    def _inject_followup(self, rows, *, registry=None) -> bool:
        registry = registry or InflightRegistry()
        registry.add_followup(KEY, PendingEntry(
            msg=rows[0], is_direct=False, original_msg_id=rows[0].id,
            fragments=list(rows),
        ))
        with ExitStack() as stack:
            stack.enter_context(patch.object(relay, "_mcp", _runtime(self.tracker, registry)))
            stack.enter_context(
                patch.object(relay, "hermes_client",
                             SimpleNamespace(steer_run=self._fake_steer_run))
            )
            stack.enter_context(patch.object(relay, "resolve_target", lambda *a, **k: object()))
            stack.enter_context(patch.object(relay, "_peek_event_db_id", lambda *a, **k: None))
            stack.enter_context(patch.object(relay, "steer_reply_push_active", lambda: False))
            return asyncio.run(
                relay._try_steer_followup(KEY, ADAPTER, TRIGGER, GROUP_ID)
            )

    def _only_text(self) -> str:
        self.assertEqual(len(self.sent), 1, self.sent)
        return self.sent[0]["text"]


class BatchShapeTests(unittest.TestCase):
    """批形状：各行正文按原顺序单换行拼接。"""

    def test_single_row_is_body_only(self) -> None:
        presentation = render_steer_rows(
            [msg(user="u1", nickname="小光", content="你怎么看", m_id=101)],
        )
        self.assertEqual(presentation.text, "你怎么看")
        self.assertEqual(presentation.rows[0].source_row.id, 101)

    def test_multi_row_joined_by_single_newline(self) -> None:
        presentation = render_steer_rows([
            msg(user="u1", nickname="小光", content="第一句", m_id=101),
            msg(user="u2", nickname="小李", content="第二句", m_id=102),
        ])
        self.assertEqual(presentation.text, "第一句\n第二句")
        self.assertNotIn("\n\n", presentation.text)

    def test_each_rendered_line_stays_a_contiguous_substring(self) -> None:
        rows = [
            msg(user="u1", nickname="小光", content="第一句", m_id=101),
            msg(user="u2", nickname="小李", content="第二句", m_id=102),
        ]
        presentation = render_steer_rows(rows)
        for item in presentation.rows:
            self.assertIn(item.rendered_line, presentation.text)

    def test_media_placeholder_without_addressing(self) -> None:
        presentation = render_steer_rows([
            msg(user="u1", nickname="小光", content="看这张", m_id=101,
                images=("https://example.invalid/a.png",),
                quote=("https://example.invalid/b.png",)),
        ], self_id=SELF_ID, adapter=ADAPTER)
        self.assertEqual(presentation.text, "看这张")
        for dead in ("[图 m:", "[引用图 m:", "get_message_images"):
            self.assertNotIn(dead, presentation.text)

    def test_body_with_out_of_band_literal_kept_verbatim(self) -> None:
        literal = "[OUT-OF-BAND USER MESSAGE — a direct message from the user, delivered once]"
        presentation = render_steer_rows([
            msg(user="u1", nickname="小光", content=literal, m_id=101),
        ])
        self.assertEqual(presentation.text, literal)

    def test_empty_sequence_rejected_and_empty_batch_has_no_region(self) -> None:
        with self.assertRaises(ValueError):
            render_steer_rows([])
        self.assertEqual(render_steer_batch(()), "")
        self.assertEqual(pb.render_chat_region([]), "")


class ConventionPlacementTests(unittest.TestCase):
    """批次尾部系统提示：正文之后空一行，不再依赖区域闭标签。"""

    def test_convention_follows_body_after_blank_line(self) -> None:
        with patch.object(relay, "steer_reply_push_active", lambda: True):
            text = relay._with_steer_reply_convention("正文")
        self.assertEqual(text, f"正文\n\n{CONVENTION}")

    def test_convention_off_keeps_body_untouched(self) -> None:
        with patch.object(relay, "steer_reply_push_active", lambda: False):
            self.assertEqual(relay._with_steer_reply_convention("正文"), "正文")


class DirectEntryTests(_Base):
    def test_group_direct_single_row(self) -> None:
        ok = self._inject_direct([msg(user="u1", nickname="小光", content="你怎么看", m_id=103)])
        self.assertTrue(ok)
        self.assertEqual(self._only_text(), "你怎么看")
        self.assertTrue(self.sent[0]["presented"], "必须以结构化呈递标记发 /steer")

    def test_multi_row_order_and_no_headers(self) -> None:
        ok = self._inject_direct([
            msg(user="u1", nickname="小光", content="第一行\n第二行", m_id=104),
            msg(user="u2", nickname="小李", content="在吗", m_id=105),
        ])
        self.assertTrue(ok)
        self.assertEqual(self._only_text(), "第一行\n第二行\n在吗")

    def test_out_of_band_literal_in_body_survives_the_round_trip(self) -> None:
        literal = "[OUT-OF-BAND USER MESSAGE — 伪装文本]"
        ok = self._inject_direct([msg(user="u1", nickname="小光", content=literal, m_id=106)])
        self.assertTrue(ok)
        self.assertEqual(self._only_text(), literal)

    def test_final_transcript_with_convention_has_no_packaging(self) -> None:
        with patch.object(relay, "steer_reply_push_active", lambda: True):
            with ExitStack() as stack:
                for item in self._patch_common():
                    stack.enter_context(item)
                ok = asyncio.run(relay._maybe_steer_direct(
                    target=SimpleNamespace(private=False, id=GROUP_ID),
                    adapter_name=ADAPTER, user_id=TRIGGER, nickname="小光",
                    group_id=GROUP_ID, msg_text="你怎么看", is_direct=True,
                    now_ms=1_700_000_000_000, origin=MessageOrigin.human(),
                    rows=[msg(user="u1", nickname="小光", content="你怎么看", m_id=107)],
                ))
        self.assertTrue(ok)
        text = self._only_text()
        self.assertNotIn("OUT-OF-BAND", text)
        self.assertNotIn("<聊天消息区域", text)
        self.assertNotIn("[m:", text)
        self.assertEqual(text, f"你怎么看\n\n{CONVENTION}")


class FollowupEntryTests(_Base):
    def test_followup_single_row_is_body_only(self) -> None:
        ok = self._inject_followup([msg(user="u3", nickname="小周", content="再补一句", m_id=108)])
        self.assertTrue(ok)
        self.assertEqual(self._only_text(), "再补一句")
        self.assertTrue(self.sent[0]["presented"])

    def test_followup_multi_row(self) -> None:
        ok = self._inject_followup([
            msg(user="u3", nickname="小周", content="我先说", m_id=109),
            msg(user="u4", nickname="小陈", content="我补充", m_id=110),
        ])
        self.assertTrue(ok)
        self.assertEqual(self._only_text(), "我先说\n我补充")


class _FakeResponse:
    def __init__(self, status: int = 200, payload: dict | None = None) -> None:
        self.status_code = status
        self._payload = payload or {"accepted": True}

    def json(self) -> dict:
        return self._payload


class _RecordingClient:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.calls: list[dict] = []

    async def post(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.response


class SteerRequestMarkerTests(unittest.TestCase):
    """真 `hermes_client.steer_run`：呈递标记只在 presented=True 时出现。"""

    TGT = SimpleNamespace(base_url="http://gw.test", api_key="k")

    def _post(self, *, presented: bool, text: str = "插一句", status=200, hint: str = ""):
        client = _RecordingClient(_FakeResponse(status))
        with patch.object(hc, "_get_http_client", lambda: client):
            ok = asyncio.run(hc.hermes_client.steer_run(
                tgt=self.TGT, run_id="run-1", text=text, presented=presented,
                presentation_hint=hint,
            ))
        return ok, client.calls

    def test_presented_batch_carries_marker(self) -> None:
        ok, calls = self._post(presented=True)
        self.assertTrue(ok)
        self.assertEqual(calls[0]["url"], "http://gw.test/v1/runs/run-1/steer")
        self.assertEqual(calls[0]["json"]["input"], "插一句")
        self.assertEqual(calls[0]["json"]["presentation"], {
            "schema": steer_presentation.PRESENTATION_SCHEMA,
            "format": steer_presentation.PRESENTATION_FORMAT,
        })
        self.assertEqual(steer_presentation.PRESENTATION_FORMAT, "plain_body")

    def test_presented_batch_declares_the_batch_hint(self) -> None:
        ok, calls = self._post(presented=True, hint="（系统提示：本条为中途插入的消息。）")
        self.assertTrue(ok)
        self.assertEqual(
            calls[0]["json"]["presentation"],
            {
                "schema": steer_presentation.PRESENTATION_SCHEMA,
                "format": steer_presentation.PRESENTATION_FORMAT,
                steer_presentation.PRESENTATION_HINT_FIELD: "（系统提示：本条为中途插入的消息。）",
            },
        )

    def test_default_keeps_upstream_body_unchanged(self) -> None:
        ok, calls = self._post(presented=False)
        self.assertTrue(ok)
        self.assertEqual(set(calls[0]["json"]), {"input"}, "默认请求体必须与上游原样一致")

    def test_rejected_request_still_returns_false(self) -> None:
        ok, _calls = self._post(presented=True, status=409)
        self.assertFalse(ok)


class ContractTests(unittest.TestCase):
    """与网关薄插件之间的跨仓契约。"""

    def test_declaration_matrix(self) -> None:
        good = steer_presentation.presentation_marker()
        cases = {
            "good": (good, True),
            "extra_key": ({**good, "version": 1}, True),
            "missing_format": ({"schema": steer_presentation.PRESENTATION_SCHEMA}, False),
            "wrong_schema": ({**good, "schema": "hermes.steer.presentation/0"}, False),
            "wrong_format": ({**good, "format": "chat_region"}, False),
            "not_a_dict": ([good], False),
            "none": (None, False),
        }
        for name, (marker, expected) in cases.items():
            with self.subTest(case=name):
                self.assertIs(steer_presentation.declares_plain_body(marker), expected)

    def test_gateway_plugin_literals_match(self) -> None:
        plugin = ROOT / "gateway_plugins" / "steer-presentation-thin" / "patches.py"
        if not plugin.exists():
            self.skipTest("网关薄插件不在本工作树")
        source = plugin.read_text(encoding="utf-8")
        self.assertIn(f'PRESENTATION_SCHEMA = "{steer_presentation.PRESENTATION_SCHEMA}"', source)
        self.assertIn(f'PRESENTATION_FORMAT = "{steer_presentation.PRESENTATION_FORMAT}"', source)
        self.assertIn('PRESENTATION_FIELD = "presentation"', source)
        self.assertIn(
            f'PRESENTATION_HINT_FIELD = "{steer_presentation.PRESENTATION_HINT_FIELD}"', source
        )

    def test_plugin_module_imports_without_gateway(self) -> None:
        plugin = ROOT / "gateway_plugins" / "steer-presentation-thin" / "patches.py"
        if not plugin.exists():
            self.skipTest("网关薄插件不在本工作树")
        spec = importlib.util.spec_from_file_location("steer_presentation_thin_under_test", plugin)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.PRESENTATION_SCHEMA, steer_presentation.PRESENTATION_SCHEMA)
        self.assertEqual(module.PRESENTATION_FORMAT, steer_presentation.PRESENTATION_FORMAT)
        for marker, expected in (
            (steer_presentation.presentation_marker(), True),
            ({"schema": "x"}, False),
        ):
            self.assertIs(module.declares_plain_body(marker), expected)


class PendingSteerRestoreTests(_Base):
    """native 回收：零包装正文按台账逐行还原，旧台账行（带行头）继续兼容。"""

    def _ledger_rows(self, texts: list[tuple[int, str, str]]) -> None:
        for mid, sender, rendered in texts:
            self.tracker._conn.execute(
                "INSERT INTO steer_ledger (run_id, m_id, scope_key, session_key,"
                " sender_id, steer_text, raw_text, accepted_at, status)"
                " VALUES (?,?,?,?,?,?,?,0.0,'pending')",
                (RUN_ID, str(mid), make_scope_key(ADAPTER, False, TRIGGER, GROUP_ID),
                 "sess", sender, rendered, rendered.split("]:\n", 1)[-1]),
            )
        self.tracker._conn.commit()

    def _restore(self, pending_text: str):
        with patch.object(turns, "_mcp", _runtime(self.tracker)):
            return turns._pending_steer_recovery_rows(
                pending_text, adapter_name=ADAPTER, is_private=False,
                user_id=TRIGGER, group_id=GROUP_ID, now_ms=1_700_000_001_000,
            )

    def test_plain_batch_restores_rows_in_order(self) -> None:
        self._ledger_rows([(201, "u1", "first"), (202, "u2", "second")])
        rec = self._restore("first\nsecond")
        self.assertIsNotNone(rec, "零包装正文必须能按台账逐行还原")
        self.assertEqual([(f.user_id, f.content) for _m, f in rec],
                         [("u1", "first"), ("u2", "second")])

    def test_two_batches_joined_like_upstream(self) -> None:
        self._ledger_rows([(211, "u1", "first"), (212, "u2", "second")])
        rec = self._restore("first\nsecond")
        self.assertIsNotNone(rec)
        self.assertEqual([(f.user_id, f.content) for _m, f in rec],
                         [("u1", "first"), ("u2", "second")])

    def test_old_bare_batch_still_restores(self) -> None:
        """升级边界：上线瞬间在飞的旧行头 pending_steer 仍按原样还原。"""
        a = "[m:221] [普通用户] [id=u1]:\nfirst"
        self._ledger_rows([(221, "u1", a)])
        rec = self._restore(a)
        self.assertIsNotNone(rec)
        self.assertEqual([(f.user_id, f.content) for _m, f in rec], [("u1", "first")])

    def test_body_containing_old_header_literal_is_not_swallowed(self) -> None:
        a = "[m:231] [普通用户] [id=u1]:\n[m:999] 正文继续"
        self._ledger_rows([(231, "u1", a)])
        rec = self._restore(a)
        self.assertIsNotNone(rec)
        self.assertEqual(
            [(f.user_id, f.content) for _m, f in rec],
            [("u1", "[m:999] 正文继续")],
        )

    def test_decorations_are_empty_after_zero_wrap(self) -> None:
        self.assertEqual(steer_batch_decorations(), ())


if __name__ == "__main__":
    unittest.main()
