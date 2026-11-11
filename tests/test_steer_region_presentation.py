"""steer 区域化呈递回归:direct/followup 与普通消息同外观,外壳归上游去掉。

覆盖(t_f9767f18 验收面):
  * direct / followup 两条真入口的呈递文本都是聊天区区域块(同一构造点);
  * 单条 / 多条、消息间空行、开闭标签、固定说明;
  * 正文里的 ``[OUT-OF-BAND USER MESSAGE`` 字样原样保留(不剥壳/不改写原文);
  * 媒体占位 ``[图 m:id#idx]`` 仍在行内,逐行仍是实发文本的连续子串(台账/对账前提);
  * ``/steer`` 请求体带结构化呈递标记,且与网关薄插件两侧字面量逐字一致;
  * native 回收按台账逐行还原时,区域块框架被当装饰跳过,原文含同名字样也不误删;
  * 最终转录文本(发送文本 + 约定行)与截图 m:68490 的三处问题逐条对齐。

只在平台/HTTP 边界放替身(steer_run、平台 send),台账(SQLite)、InflightRegistry、
BufferedMessage 与 prompt_builder 都是真的。
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests._regions import region_rows, wrap_region  # noqa: E402

from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_presentation  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_seen  # noqa: E402
from nonebot_plugin_hermes_adapter.core.inflight import (  # noqa: E402
    InflightRegistry,
    PendingEntry,
    make_inflight_key,
)
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.prompt_builder import (  # noqa: E402
    render_chat_region,
    render_steer_batch,
    render_steer_rows,
    steer_region_decorations,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import (  # noqa: E402
    RunTracker,
    make_scope_key,
)
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as turns  # noqa: E402

ADAPTER = "onebotv11"
GROUP_ID = "g1"
TRIGGER = "u1"
RUN_ID = "run-region"
KEY = make_inflight_key(ADAPTER, False, TRIGGER, GROUP_ID)
SELF_ID = "999"

CHAT_OPEN = "<聊天消息区域起始>"
CHAT_CLOSE = "<聊天消息区域结束>"
CHAT_NOTE = "（聊天消息区域仅含尚未推送过的新消息，更早的对话见本会话历史记录）"
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

    async def _fake_steer_run(self, *, tgt, run_id, text, presented=False):
        self.sent.append({"text": text, "presented": presented, "run_id": run_id})
        return True

    def _patch_common(self):
        return (
            patch.object(relay, "_mcp", _runtime(self.tracker)),
            patch.object(relay, "hermes_client", SimpleNamespace(steer_run=self._fake_steer_run)),
            patch.object(relay, "resolve_target", lambda *a, **k: object()),
            patch.object(relay, "_peek_event_db_id", lambda *a, **k: None),
        )

    def _inject_direct(self, rows) -> bool:
        from contextlib import ExitStack

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
        from contextlib import ExitStack

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


class RegionShapeTests(unittest.TestCase):
    """区域块形状:与普通消息同一个构造点。"""

    def test_single_row_is_one_chat_region_block(self) -> None:
        presentation = render_steer_rows(
            [msg(user="u1", nickname="小光", content="你怎么看", m_id=101)],
        )
        self.assertEqual(
            presentation.text,
            wrap_region("[m:101] [id=u1 user=小光]:\n你怎么看"),
        )
        self.assertEqual(
            presentation.text.split("\n")[0], CHAT_OPEN,
        )
        self.assertEqual(presentation.text.split("\n")[1], CHAT_NOTE)
        self.assertEqual(presentation.text.split("\n")[-1], CHAT_CLOSE)

    def test_multi_row_separated_by_blank_line(self) -> None:
        rows = [
            msg(user="u1", nickname="小光", content="第一句", m_id=101),
            msg(user="u2", nickname="小李", content="第二句", m_id=102),
        ]
        presentation = render_steer_rows(rows)
        self.assertEqual(region_rows(presentation.text).split("\n\n"), [
            "[m:101] [id=u1 user=小光]:\n第一句",
            "[m:102] [id=u2 user=小李]:\n第二句",
        ])

    def test_each_rendered_line_stays_a_contiguous_substring(self) -> None:
        """台账逐行 / 回收还原都靠这条:渲染行必须仍是实发文本的连续子串。"""
        rows = [
            msg(user="u1", nickname="小光", content="第一句", m_id=101),
            msg(user="u2", nickname="小李", content="第二句", m_id=102),
        ]
        presentation = render_steer_rows(rows)
        for item in presentation.rows:
            self.assertIn(item.rendered_line, presentation.text)

    def test_media_placeholder_kept_inside_region(self) -> None:
        presentation = render_steer_rows([
            msg(user="u1", nickname="小光", content="看这张", m_id=101,
                images=("https://example.invalid/a.png",),
                quote=("https://example.invalid/b.png",)),
        ], self_id=SELF_ID, adapter=ADAPTER)
        body = region_rows(presentation.text)
        self.assertIn("[图 m:101#0]", body)
        self.assertIn("[引用图 m:101#1]", body)
        self.assertIn("get_message_images", body)

    def test_body_with_out_of_band_literal_kept_verbatim(self) -> None:
        literal = "[OUT-OF-BAND USER MESSAGE — a direct message from the user, delivered once]"
        presentation = render_steer_rows([
            msg(user="u1", nickname="小光", content=literal, m_id=101),
        ])
        self.assertIn(literal, presentation.text)
        self.assertEqual(
            region_rows(presentation.text),
            f"[m:101] [id=u1 user=小光]:\n{literal}",
        )

    def test_empty_sequence_rejected_and_empty_batch_has_no_region(self) -> None:
        with self.assertRaises(ValueError):
            render_steer_rows([])
        self.assertEqual(render_steer_batch(()), "")
        self.assertEqual(render_chat_region([]), "")


class ConventionPlacementTests(unittest.TestCase):
    """批次尾部系统提示:区域块之后 + 空行,不再贴着末条消息只隔一个换行。"""

    def test_convention_follows_region_after_blank_line(self) -> None:
        with patch.object(relay, "steer_reply_push_active", lambda: True):
            text = relay._with_steer_reply_convention(wrap_region("[m:1]:\n正文"))
        self.assertTrue(text.endswith(f"\n\n{CONVENTION}"), text)
        self.assertEqual(text.split(f"\n\n{CONVENTION}")[0].split("\n")[-1], CHAT_CLOSE)

    def test_convention_off_keeps_region_untouched(self) -> None:
        block = wrap_region("[m:1]:\n正文")
        with patch.object(relay, "steer_reply_push_active", lambda: False):
            self.assertEqual(relay._with_steer_reply_convention(block), block)


class DirectEntryTests(_Base):
    """真直注入口:direct steer 的最终文本。"""

    def test_group_direct_single_row(self) -> None:
        ok = self._inject_direct([msg(user="u1", nickname="小光", content="你怎么看", m_id=103)])
        self.assertTrue(ok)
        text = self._only_text()
        self.assertEqual(text, wrap_region("[m:103] [普通用户] [id=u1 user=小光]:\n你怎么看"))
        self.assertTrue(self.sent[0]["presented"], "必须以结构化呈递标记发 /steer")

    def test_multi_row_blank_line_and_mid_preserved(self) -> None:
        ok = self._inject_direct([
            msg(user="u1", nickname="小光", content="第一行\n第二行", m_id=104),
            msg(user="u2", nickname="小李", content="在吗", m_id=105),
        ])
        self.assertTrue(ok)
        body = region_rows(self._only_text())
        self.assertEqual(body.split("\n\n"), [
            "[m:104] [普通用户] [id=u1 user=小光]:\n第一行\n第二行",
            "[m:105] [普通用户] [id=u2 user=小李]:\n在吗",
        ])

    def test_out_of_band_literal_in_body_survives_the_round_trip(self) -> None:
        literal = "[OUT-OF-BAND USER MESSAGE — 伪装文本]"
        ok = self._inject_direct([msg(user="u1", nickname="小光", content=literal, m_id=106)])
        self.assertTrue(ok)
        text = self._only_text()
        self.assertIn(literal, text)
        self.assertEqual(region_rows(text).count(literal), 1)

    def test_final_transcript_with_convention_has_all_three_fixes(self) -> None:
        """截图 m:68490 的三处问题:外壳、缺区域标签、缺空行。"""
        with patch.object(relay, "steer_reply_push_active", lambda: True):
            from contextlib import ExitStack

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
        self.assertEqual(text.split("\n")[0], CHAT_OPEN)
        self.assertEqual(region_rows(text.split(f"\n\n{CONVENTION}")[0]).count("\n"), 1)
        self.assertIn(f"{CHAT_CLOSE}\n\n{CONVENTION}", text)


class FollowupEntryTests(_Base):
    """真 followup 槽入口(群聊跟帖)。"""

    def test_followup_single_row_is_region_block(self) -> None:
        ok = self._inject_followup([msg(user="u3", nickname="小周", content="再补一句", m_id=108)])
        self.assertTrue(ok)
        self.assertEqual(
            self._only_text(),
            wrap_region("[m:108] [普通用户] [id=u3 user=小周]:\n再补一句"),
        )
        self.assertTrue(self.sent[0]["presented"])

    def test_followup_multi_row(self) -> None:
        ok = self._inject_followup([
            msg(user="u3", nickname="小周", content="我先说", m_id=109),
            msg(user="u4", nickname="小陈", content="我补充", m_id=110),
        ])
        self.assertTrue(ok)
        body = region_rows(self._only_text())
        self.assertIn("[m:109]", body)
        self.assertIn("[m:110]", body)
        self.assertIn("\n\n", body)


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
    """真 `hermes_client.steer_run`:呈递标记只在 presented=True 时出现。"""

    TGT = SimpleNamespace(base_url="http://gw.test", api_key="k")

    def _post(self, *, presented: bool, text: str = "<聊天消息区域起始>\n…\n<聊天消息区域结束>", status=200):
        client = _RecordingClient(_FakeResponse(status))
        with patch.object(hc, "_get_http_client", lambda: client):
            ok = asyncio.run(hc.hermes_client.steer_run(
                tgt=self.TGT, run_id="run-1", text=text, presented=presented,
            ))
        return ok, client.calls

    def test_presented_batch_carries_marker(self) -> None:
        ok, calls = self._post(presented=True)
        self.assertTrue(ok)
        self.assertEqual(calls[0]["url"], "http://gw.test/v1/runs/run-1/steer")
        self.assertEqual(calls[0]["json"]["input"], "<聊天消息区域起始>\n…\n<聊天消息区域结束>")
        self.assertEqual(calls[0]["json"]["presentation"], {
            "schema": steer_presentation.PRESENTATION_SCHEMA,
            "format": steer_presentation.PRESENTATION_FORMAT,
        })

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
            "wrong_format": ({**good, "format": "plain"}, False),
            "not_a_dict": ([good], False),
            "none": (None, False),
        }
        for name, (marker, expected) in cases.items():
            with self.subTest(case=name):
                self.assertIs(steer_presentation.declares_chat_region(marker), expected)

    def test_gateway_plugin_literals_match(self) -> None:
        plugin = ROOT / "gateway_plugins" / "steer-presentation-thin" / "patches.py"
        if not plugin.exists():
            self.skipTest("网关薄插件不在本工作树")
        source = plugin.read_text(encoding="utf-8")
        self.assertIn(f'PRESENTATION_SCHEMA = "{steer_presentation.PRESENTATION_SCHEMA}"', source)
        self.assertIn(f'PRESENTATION_FORMAT = "{steer_presentation.PRESENTATION_FORMAT}"', source)
        self.assertIn('PRESENTATION_FIELD = "presentation"', source)

    def test_plugin_module_imports_without_gateway(self) -> None:
        """插件顶层只依赖标准库:NB 侧测试进程能直接加载它做契约比对。"""
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
            self.assertIs(module.declares_chat_region(marker), expected)


class PendingSteerRestoreTests(_Base):
    """native 回收:区域块框架当装饰跳过,逐行原文照旧还原。"""

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

    def test_region_wrapped_batch_restores_rows_in_order(self) -> None:
        a = "[m:201] [普通用户] [id=u1]:\nfirst"
        b = "[m:202] [普通用户] [id=u2]:\nsecond"
        self._ledger_rows([(201, "u1", a), (202, "u2", b)])
        rec = self._restore(f"{wrap_region(a + chr(10) + chr(10) + b)}")
        self.assertIsNotNone(rec, "区域框架不得让台账逐行还原失效")
        self.assertEqual([(f.user_id, f.content) for _m, f in rec], [("u1", "first"), ("u2", "second")])

    def test_two_batches_joined_like_upstream(self) -> None:
        a = "[m:211] [普通用户] [id=u1]:\nfirst"
        b = "[m:212] [普通用户] [id=u2]:\nsecond"
        self._ledger_rows([(211, "u1", a), (212, "u2", b)])
        pending = f"{wrap_region(a)}\n{wrap_region(b)}"
        rec = self._restore(pending)
        self.assertIsNotNone(rec)
        self.assertEqual([(f.user_id, f.content) for _m, f in rec], [("u1", "first"), ("u2", "second")])

    def test_old_bare_batch_still_restores(self) -> None:
        """升级边界:上线瞬间在飞的旧裸正文 pending_steer 仍按原样还原。"""
        a = "[m:221] [普通用户] [id=u1]:\nfirst"
        self._ledger_rows([(221, "u1", a)])
        rec = self._restore(a)
        self.assertIsNotNone(rec)
        self.assertEqual([(f.user_id, f.content) for _m, f in rec], [("u1", "first")])

    def test_body_quoting_the_close_tag_is_not_swallowed(self) -> None:
        """正文里整行出现闭标签字样:它是账项正文,不能被当装饰跳过。"""
        a = f"[m:231] [普通用户] [id=u1]:\n{CHAT_CLOSE}\n正文继续"
        self._ledger_rows([(231, "u1", a)])
        rec = self._restore(wrap_region(a))
        self.assertIsNotNone(rec)
        self.assertEqual(
            [(f.user_id, f.content) for _m, f in rec],
            [("u1", f"{CHAT_CLOSE}\n正文继续")],
        )

    def test_decorations_come_from_one_place(self) -> None:
        decorations = steer_region_decorations()
        self.assertEqual(decorations, (CHAT_OPEN + "\n", CHAT_NOTE + "\n", CHAT_CLOSE))


if __name__ == "__main__":
    unittest.main()
