"""E3：Runs delta 流式即时多决策（候选检测、折叠、台账追加、真实链式回放）。"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core.hermes_client import (  # noqa: E402
    ChatResult,
    RunsEventParser,
)
from nonebot_plugin_hermes_adapter.core.npha_terminal import (  # noqa: E402
    decode_terminal_batch,
    scan_stream_decisions,
    structured_batch_from_terminal,
    structured_from_terminal,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    TerminalPlanError,
    append_stream_unit,
    append_terminal_batch,
    build_batch_plan,
    delivery_plan_units,
    fold_plan_units,
    fold_stream_decisions,
    prepare_terminal_batch,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from tests import test_runs_terminal as _e2  # noqa: E402


def load_sample(name: str) -> dict:
    return json.loads((ROOT / "SAMPLES" / name).read_text(encoding="utf-8"))


def sse_lines(events: list[dict]) -> list[str]:
    """真实事件 payload → 上游 runs 的 data-only SSE 行形态。"""
    return [f"data: {json.dumps(event, ensure_ascii=False)}" for event in events]


def _load_run_resume():
    """按真实包路径加载 tasks.run_resume，绕开 tasks/__init__ 的 apscheduler 依赖。"""
    import importlib
    import types

    pkg_name = "nonebot_plugin_hermes_adapter.tasks"
    if pkg_name not in sys.modules:
        package = types.ModuleType(pkg_name)
        package.__path__ = [str(ROOT / "tasks")]
        package.__package__ = pkg_name
        sys.modules[pkg_name] = package
    return importlib.import_module(f"{pkg_name}.run_resume")


def unit(control: dict | None = None, body: str = "正文") -> str:
    return (
        json.dumps(control or {}, ensure_ascii=False)
        + "\n"
        + json.dumps({"body": body}, ensure_ascii=False)
    )


def replay_sample(name: str):
    """真实样本回放：返回 (parser, 每帧后的已完成决策数, 终态 output, 样本, 收到的决策)。

    决策只由回放循环自己 ``drain_stream_decisions`` 收集（与生产消费循环同口径）。
    """
    data = load_sample(name)
    parser = RunsEventParser()
    counts: list[int] = []
    drained: list = []
    for line in sse_lines(data["events"]):
        parser.feed(line)
        drained.extend(parser.drain_stream_decisions())
        counts.append(len(drained))
    return parser, counts, data["status"]["output"], data, drained


def _png_bytes() -> bytes:
    """真实有效 1x1 RGB PNG（给本地图片内联链用，不依赖外部资产）。"""
    import struct
    import zlib

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack("!I", len(data)) + kind + data
            + struct.pack("!I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack("!2I5B", 1, 1, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\x00\xff\x00\x00"))
        + chunk(b"IEND", b"")
    )


def _chat_result(output: str, streamed: list[dict], run_id: str = "run-e2-h") -> ChatResult:
    """按 client 终态分支的形状造结果（真实解码器，不手写 structured）。"""
    decisions = structured_batch_from_terminal(decode_terminal_batch(output))
    result = ChatResult(raw_text=output, structured=decisions[0] if decisions else None)
    result.terminal_decisions = decisions
    result.streamed_decisions = list(streamed)
    result.run_id = run_id
    return result


class StreamScanTests(unittest.TestCase):
    """真实 delta 分段上的候选检测（复用 decode_terminal）。"""

    def test_b4_two_units_with_tool_between(self) -> None:
        data = load_sample("runs_terminal_single_b4.json")
        parser = RunsEventParser()
        drained: list = []
        counts_before_second_segment = None
        for event in data["events"]:
            if (
                event["event"] == "message.delta"
                and event.get("delta") == "\n\n{}\n"
                and event["seq"] > 20
                and counts_before_second_segment is None
            ):
                counts_before_second_segment = len(drained)
            parser.feed(f"data: {json.dumps(event, ensure_ascii=False)}")
            drained.extend(parser.drain_stream_decisions())
        self.assertEqual(
            [d.body for d in drained],
            ["先记录“E0-FIX 乙取证”待办", "待办已记录，当前为进行中"],
        )
        # 第一份在第一个工具段之后就已完成；第二段正文开始时仍是 1 份
        self.assertEqual(counts_before_second_segment, 1)
        self.assertEqual([d.body for d in decode_terminal_batch(data["status"]["output"])],
                         ["待办已记录，当前为进行中"])

    def test_b5_two_units_no_tools(self) -> None:
        parser, _, output, _, drained = replay_sample("runs_terminal_multi_b5.json")
        self.assertEqual(
            [d.body for d in drained],
            ["第一份正文", "第二份第一行\n第二份第二行"],
        )
        self.assertEqual(len(decode_terminal_batch(output)), 2)

    def test_no_decision_before_body_complete(self) -> None:
        text = ""
        offset = 0
        emitted = []
        for piece in ['{}', '\n', '{"bo', 'dy":"半', '句', '"', '}']:
            text += piece
            new, offset = scan_stream_decisions(text, offset)
            emitted.extend(new)
            if piece != '}':
                self.assertEqual(emitted, [])
        self.assertEqual([d.body for d in emitted], ["半句"])

    def test_body_with_brace_survives_split(self) -> None:
        text = ""
        offset = 0
        emitted = []
        for piece in ['{}\n{"body":"a}', 'b}"}']:
            text += piece
            new, offset = scan_stream_decisions(text, offset)
            emitted.extend(new)
        # 中间那一步不能把 "a}" 当完整正文提前发出去
        self.assertEqual([d.body for d in emitted], ["a}b}"])

    def test_narration_before_envelope_ignored(self) -> None:
        text = "我先查一下\n\n{}\n" + json.dumps({"body": "好"}, ensure_ascii=False)
        decisions, offset = scan_stream_decisions(text)
        self.assertEqual([d.body for d in decisions], ["好"])
        self.assertEqual(offset, len(text))

    def test_missing_control_body_only(self) -> None:
        decisions, _ = scan_stream_decisions('{"body":"只有正文"}')
        self.assertEqual([d.body for d in decisions], ["只有正文"])

    def test_fullwidth_control_with_body(self) -> None:
        text = '｛"reply_to":"m:1"｝\n{"body":"全角控制"}\n'
        decisions, _ = scan_stream_decisions(text)
        self.assertEqual(decisions[0].body, "全角控制")
        self.assertEqual(decisions[0].control["reply_to"], "m:1")

    def test_empty_body_unit_still_emitted(self) -> None:
        decisions, _ = scan_stream_decisions(unit({"topic_hint": "收尾"}, "") + "\n")
        self.assertEqual(len(decisions), 1)
        self.assertTrue(decisions[0].silent)
        self.assertEqual(decisions[0].control["topic_hint"], "收尾")

    def test_same_body_twice_is_two_units(self) -> None:
        decisions, _ = scan_stream_decisions(unit({}, "同一句") + "\n" + unit({}, "同一句") + "\n")
        self.assertEqual([d.body for d in decisions], ["同一句", "同一句"])

    def test_partial_body_offset_kept(self) -> None:
        text = '{"reply_to":"m:1"}\n{"body":"未完成'
        decisions, offset = scan_stream_decisions(text)
        self.assertEqual(decisions, [])
        self.assertEqual(offset, 0)
        text += '"}'
        decisions, offset = scan_stream_decisions(text, offset)
        self.assertEqual([d.body for d in decisions], ["未完成"])

    def test_real_b4_gap_segment_excludes_sent_decision(self) -> None:
        """R1：真实 b4 工具间隙缓冲里的决策信封已即时消费，不得再成旁白段。"""
        data = load_sample("runs_terminal_single_b4.json")
        parser = RunsEventParser()
        drained: list = []

        def feed_and_drain(line: str):
            segment = parser.feed(line)
            drained.extend(parser.drain_stream_decisions())
            return segment

        segments = [
            segment for segment in (feed_and_drain(line) for line in sse_lines(data["events"]))
            if segment
        ]
        self.assertEqual(
            [d.body for d in drained],
            ["先记录“E0-FIX 乙取证”待办", "待办已记录，当前为进行中"],
        )
        self.assertEqual(segments, [])

    def test_gap_segment_keeps_ordinary_narration(self) -> None:
        parser = RunsEventParser()
        drained: list = []

        def feed_and_drain(line: str):
            segment = parser.feed(line)
            drained.extend(parser.drain_stream_decisions())
            return segment

        events = [
            {"event": "message.delta", "delta": "先看一眼工具。", "seq": 0},
            {"event": "message.delta", "delta": "\n\n{}\n", "seq": 1},
            {"event": "message.delta", "delta": '{"body":"真正正文"}', "seq": 2},
            {"event": "tool.started", "tool": "todo_list", "seq": 3},
        ]
        segments = [
            segment for segment in (feed_and_drain(line) for line in sse_lines(events))
            if segment
        ]
        self.assertEqual([d.body for d in drained], ["真正正文"])
        self.assertEqual(segments, ["先看一眼工具。"])

    def test_interim_frame_decision_envelope_not_echoed(self) -> None:
        """R1：message.interim 里的决策信封同样不能从旁白出口发出去。"""
        parser = RunsEventParser()
        envelope = unit({}, "已经即时发送的正文")
        self.assertIsNone(parser.feed(f"data: {json.dumps({'event': 'message.interim', 'text': envelope, 'seq': 0}, ensure_ascii=False)}"))
        self.assertEqual(
            parser.feed(f"data: {json.dumps({'event': 'message.interim', 'text': '正在查资料', 'seq': 1}, ensure_ascii=False)}"),
            "正在查资料",
        )


class FoldTests(unittest.TestCase):
    def _d(self, body: str, **control) -> dict:
        return structured_from_terminal(
            decode_terminal_batch(unit(control, body))[0]
        )

    def test_local_image_delta_and_terminal_fold_to_one(self) -> None:
        """R2：同一张本地图片经 delta（路径）与终态（真实 inliner → data URL）是同一份。"""
        from nonebot_plugin_hermes_adapter.core import media_whitelist as mw

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        img = Path(tmp.name) / "actual.png"
        img.write_bytes(_png_bytes())
        raw = unit({}, f"图在这里 ![image]({img})")
        with patch.object(mw, "allowed_roots", return_value=[img.parent]):
            terminal_text = mw.inline_local_media_images(raw)
            self.assertIn("data:image/png;base64,", terminal_text)
            stream = structured_batch_from_terminal(decode_terminal_batch(raw))
            terminal = structured_batch_from_terminal(decode_terminal_batch(terminal_text))
            merged, folded = fold_stream_decisions(stream, terminal)
        self.assertEqual(folded, 1)
        self.assertEqual(len(merged), 1)

    def test_terminal_is_suffix_of_stream(self) -> None:
        stream = [self._d("A"), self._d("B")]
        merged, folded = fold_stream_decisions(stream, [self._d("B")])
        self.assertEqual([d["body"] for d in merged], ["A", "B"])
        self.assertEqual(folded, 1)

    def test_terminal_all_streamed(self) -> None:
        stream = [self._d("A"), self._d("B")]
        merged, folded = fold_stream_decisions(stream, [self._d("A"), self._d("B")])
        self.assertEqual(merged, stream)
        self.assertEqual(folded, 2)

    def test_terminal_extra_unit_appended(self) -> None:
        stream = [self._d("A")]
        merged, folded = fold_stream_decisions(stream, [self._d("B")])
        self.assertEqual([d["body"] for d in merged], ["A", "B"])
        self.assertEqual(folded, 0)

    def test_same_text_different_segment_not_collapsed(self) -> None:
        stream = [self._d("同一句"), self._d("同一句")]
        merged, folded = fold_stream_decisions(stream, [self._d("同一句")])
        self.assertEqual(len(merged), 2)
        self.assertEqual(folded, 1)

    def test_formatting_difference_does_not_change_decision(self) -> None:
        stream = [self._d("正文", reply_to="m:1")]
        terminal = structured_from_terminal(
            decode_terminal_batch('{ "reply_to" : "m:1" }\n{"body":"正文"}')[0]
        )
        merged, folded = fold_stream_decisions(stream, [terminal])
        self.assertEqual(len(merged), 1)
        self.assertEqual(folded, 1)

    def test_no_streamed_returns_terminal(self) -> None:
        term = [self._d("A")]
        merged, folded = fold_stream_decisions([], term)
        self.assertEqual(merged, term)
        self.assertEqual(folded, 0)


class PlanGrowthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.tracker = RunTracker(Path(self.tmp.name) / "t.db")
        self.addCleanup(self.tracker.close)
        self.tracker.record_submission(
            idempotency_key="idem", request_body={}, target_fingerprint="x|y",
            scope_key="onebotv11:group:g1", session_key="s", mode="reactive",
            expect_structured=True, bot_self_id="999",
        )
        self.tracker.bind_submission_and_register(
            idempotency_key="idem", run_id="r1", scope_key="onebotv11:group:g1",
            adapter="onebotv11", is_private=False, user_id="u1", group_id="g1",
            bot_self_id="999", session_key="s", mode="reactive", expect_structured=True,
        )

    def _unit(self, body: str) -> dict:
        from nonebot_plugin_hermes_adapter.core.runs_terminal import units_from_decisions

        return units_from_decisions([structured_from_terminal(
            decode_terminal_batch(unit({}, body))[0]
        )])[0][1]

    def test_append_grows_plan_by_suffix(self) -> None:
        self.assertIsNone(self.tracker.get_delivery("r1"))
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("A"))
        self.assertEqual(self.tracker.get_delivery("r1")["expected_state"], "streaming")
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("B"))
        plan_units = delivery_plan_units(self.tracker.get_delivery("r1"))
        self.assertEqual([u["cleaned_text"] for u in plan_units], ["A", "B"])
        self.assertEqual(
            {c["component_id"] for c in self.tracker.list_components("r1")},
            {"primary:0", "history:0", "primary:1", "history:1"},
        )

    def test_streaming_row_not_ready_for_reconcile(self) -> None:
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("A"))
        self.assertEqual(self.tracker.list_ready_deliveries(), [])

    def test_changed_prefix_rejected_even_with_growth(self) -> None:
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("A"))
        old = delivery_plan_units(self.tracker.get_delivery("r1"))
        changed = [dict(old[0], cleaned_text="篡改"), {
            "cleaned_text": "B", "media_urls": [], "files": [], "reply_to": "",
        }]
        plan, comps = build_batch_plan([self._unit("B")])
        with self.assertRaises(ValueError):
            self.tracker.upsert_delivery_plan(
                run_id="r1", plan={"batch": True, "units": changed},
                components=[], allow_prefix_growth=True,
            )
        del plan, comps

    def test_append_terminal_batch_no_new_units_keeps_plan(self) -> None:
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("A"))
        before = self.tracker.get_delivery("r1")["plan_json"]
        contexts = append_terminal_batch(tracker=self.tracker, run_id="r1", units=[])
        self.assertEqual(len(contexts), 1)
        self.assertEqual(self.tracker.get_delivery("r1")["plan_json"], before)

    def test_terminal_promotes_streaming_row_to_ready(self) -> None:
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("A"))
        self.tracker.record_expected_delivery(
            run_id="r1", output=unit({}, "A"), expect_structured=True
        )
        delivery = self.tracker.get_delivery("r1")
        self.assertEqual(delivery["expected_state"], "ready")
        self.assertEqual(delivery["output"], unit({}, "A"))

    def test_fold_plan_units_matches_terminal_suffix(self) -> None:
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("A"))
        append_stream_unit(tracker=self.tracker, run_id="r1", unit=self._unit("B"))
        append_units, existing = fold_plan_units(
            tracker=self.tracker, run_id="r1", units=[self._unit("B")]
        )
        self.assertEqual(existing, 2)
        self.assertEqual(append_units, [])


class ParserStreamTests(unittest.TestCase):
    def test_real_b4_frames_drive_terminal_and_stream(self) -> None:
        parser, counts, output, data, drained = replay_sample("runs_terminal_single_b4.json")
        self.assertEqual(parser.output, output)
        self.assertTrue(parser.terminal)
        self.assertEqual(parser.last_seq, 39)
        self.assertEqual(parser.usage, data["status"]["usage"])
        self.assertEqual(counts[-1], 2)
        # 终态帧不解出重复决策（只有 delta 产出）：回放已逐帧 drain，末尾不再有待发份
        self.assertEqual(len(drained), 2)
        self.assertEqual(parser.drain_stream_decisions(), [])

    def test_steer_frame_does_not_disturb_stream(self) -> None:
        events = [
            {"event": "message.delta", "delta": '{"reply_to":"m:1"}\n', "seq": 0},
            {"event": "message.delta", "delta": '{"body":"第一份"}', "seq": 1},
            {"event": "run.steered", "accepted": True, "seq": 2},
            {"event": "message.delta", "delta": '\n\n{}\n', "seq": 3},
            {"event": "message.delta", "delta": '{"body":"回应 steer"}', "seq": 4},
            {"event": "run.completed", "output": unit({}, "回应 steer"), "seq": 5},
        ]
        parser = RunsEventParser()
        drained: list = []
        for line in sse_lines(events):
            parser.feed(line)
            drained.extend(parser.drain_stream_decisions())
        self.assertEqual(
            [d.body for d in drained], ["第一份", "回应 steer"]
        )
        self.assertEqual(parser.output, unit({}, "回应 steer"))


class _ClientChain:
    """runs 客户端链测试的假 HTTP：submit 202 + events SSE 行。"""

    def __init__(self, run_id: str, lines: list[str]) -> None:
        self.run_id = run_id
        self.lines = lines
        self.streams: list[str] = []
        self.payloads: list[dict] = []

    class _Resp:
        def __init__(self, status: int, payload: dict | None = None, lines=None) -> None:
            self.status_code = status
            self._payload = payload or {}
            self._lines = list(lines or [])
            self.headers = {}

        def json(self):
            return self._payload

        async def aread(self) -> bytes:
            return b""

        async def aiter_lines(self):
            for line in self._lines:
                yield line

    class _Stream:
        def __init__(self, resp) -> None:
            self._resp = resp

        async def __aenter__(self):
            return self._resp

        async def __aexit__(self, *exc):
            return False

    async def post(self, url, json=None, headers=None, timeout=None):
        self.payloads.append(json or {})
        return self._Resp(202, {"run_id": self.run_id, "status": "started", "replayed": False})

    def stream(self, method, url, **kwargs):
        self.streams.append(url)
        return self._Stream(self._Resp(200, lines=self.lines))


def real_chat(tracker, events: list[dict], run_id: str = "run-e2-h"):
    """真实 HermesClient.chat + 真实 SSE 行（只换 HTTP 客户端与台账单例）。"""
    from nonebot_plugin_hermes_adapter.core import hermes_client as hc
    from nonebot_plugin_hermes_adapter import mcp as mcp_module

    fake = _ClientChain(run_id, sse_lines(events))
    public_args = inspect.signature(hc.hermes_client.chat).parameters

    async def chat(*args, **kwargs):
        params = {key: value for key, value in kwargs.items() if key in public_args}
        params["target"] = SimpleNamespace(
            base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t",
        )
        params["bot_self_id"] = "999"
        with patch.object(hc, "_get_http_client", return_value=fake), \
                patch.object(mcp_module, "run_tracker", tracker):
            return await hc.hermes_client.chat(**params)

    return chat


class ClientChainTests(unittest.IsolatedAsyncioTestCase):
    async def test_runs_does_not_append_legacy_structured_output_hint(self) -> None:
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter import mcp as mcp_module

        fake = _ClientChain("run-prompt", sse_lines([
            {"event": "run.completed", "output": '{"body":"ok"}', "seq": 0},
        ]))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        try:
            with patch.object(hc, "_get_http_client", return_value=fake), \
                    patch.object(mcp_module, "run_tracker", tracker):
                await hc.hermes_client.chat(
                    text="hi", session_key="s", user_id="u1", group_id=None,
                    adapter_name="onebotv11", is_private=True, mode="passive",
                    expect_structured=True, structured_tool_name="submit_decision",
                    system_prompt="SYS", bot_self_id="999",
                    target=SimpleNamespace(
                        base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t",
                    ),
                )
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        instructions = fake.payloads[0]["instructions"]
        self.assertEqual(instructions, "SYS")
        self.assertNotIn("STRUCTURED OUTPUT", instructions)
        self.assertNotIn("should_reply", instructions)

    async def test_chat_wires_immediate_callback_and_streamed_result(self) -> None:
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter import mcp as mcp_module

        data = load_sample("runs_terminal_single_b4.json")
        lines = sse_lines(data["events"])
        fake = _ClientChain("run-e3", lines)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        seen: list[str] = []

        async def deliver(decision, run_id):
            self.assertEqual(run_id, "run-e3")
            seen.append(decision["body"])

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        try:
            with patch.object(hc, "_get_http_client", return_value=fake), \
                    patch.object(mcp_module, "run_tracker", tracker):
                result = await hc.hermes_client.chat(
                    text="hi", session_key="s", user_id="u1", group_id="g1",
                    adapter_name="onebotv11", is_private=False, mode="reactive",
                    expect_structured=True, structured_tool_name="submit_decision",
                    system_prompt="SYS", bot_self_id="999",
                    target=SimpleNamespace(
                        base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t",
                    ),
                    decision_callback=deliver,
                )
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        self.assertEqual(
            seen, ["先记录“E0-FIX 乙取证”待办", "待办已记录，当前为进行中"]
        )
        self.assertEqual([d["body"] for d in result.streamed_decisions], seen)
        self.assertEqual([d["body"] for d in result.terminal_decisions],
                         ["待办已记录，当前为进行中"])
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.run_id, "run-e3")
        delivery = tracker.get_delivery("run-e3")
        self.assertEqual(delivery["expected_state"], "ready")
        self.assertEqual(delivery["output"], data["status"]["output"])

    async def test_terminal_parse_failure_after_stream_does_not_duplicate(self) -> None:
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter import mcp as mcp_module

        lines = sse_lines([
            {"event": "message.delta", "delta": '{"body":"说了一半"}', "seq": 0},
            {"event": "run.completed", "output": '{"body":"说了一半"', "seq": 1},
        ])
        fake = _ClientChain("run-e3b", lines)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        seen: list[str] = []

        async def deliver(decision, run_id):
            seen.append(decision["body"])

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        try:
            with patch.object(hc, "_get_http_client", return_value=fake), \
                    patch.object(mcp_module, "run_tracker", tracker):
                result = await hc.hermes_client.chat(
                    text="hi", session_key="s", user_id="u1", group_id="g1",
                    adapter_name="onebotv11", is_private=False, mode="reactive",
                    expect_structured=True, structured_tool_name="submit_decision",
                    system_prompt="SYS", bot_self_id="999",
                    target=SimpleNamespace(
                        base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t",
                    ),
                    decision_callback=deliver,
                )
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        self.assertEqual(seen, ["说了一半"])
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.terminal_decisions, [])
        self.assertEqual(len(result.streamed_decisions), 1)


class _StreamingChat:
    """把真实 SSE 帧喂进真实 parser + 真实 handler 即时发送器，再返回 ChatResult。"""

    def __init__(self, tracker, events: list[dict], run_id: str = "run-e2-h") -> None:
        self.tracker = tracker
        self.events = events
        self.run_id = run_id
        self.sent_at: list[str] = []
        self.callback = None

    async def __call__(self, *args, **kwargs):
        from nonebot_plugin_hermes_adapter.core.hermes_client import RunsEventParser

        self.callback = kwargs.get("decision_callback")
        parser = RunsEventParser()
        streamed: list[dict] = []
        for line in sse_lines(self.events):
            parser.feed(line)
            for decision in parser.drain_stream_decisions():
                payload = structured_from_terminal(decision)
                if self.callback is not None:
                    await self.callback(payload, self.run_id)
                streamed.append(payload)
                self.sent_at.append(payload["body"])
        output = parser.output if parser.output is not None else ""
        if parser.terminal:
            self.tracker.record_expected_delivery(
                run_id=self.run_id, output=output, expect_structured=True
            )
        result = _chat_result(output, streamed, self.run_id)
        return result


class ReactiveLiveChainTests(_e2._ReactiveHarness):
    """真实 handler + 真实台账 + 真实 parser/scanner 的即时多决策链。"""

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    async def _run_stream(self, events: list[dict], *, output: str | None = None):
        data_events = list(events)
        if output is not None:
            data_events.append({"event": "run.completed", "output": output, "seq": 999})
        tracker = self._tracker(record_delivery=False)
        chat = _StreamingChat(tracker, data_events)
        calls = await self._run(None, tracker=tracker, chat=chat)
        return tracker, chat, calls

    async def test_tool_before_and_after_both_sent_immediately(self) -> None:
        data = load_sample("runs_terminal_single_b4.json")
        tracker, chat, calls = await self._run_stream(
            data["events"], output=data["status"]["output"]
        )
        self.assertEqual(
            [c["text"] for c in calls["send"]],
            ["先记录“E0-FIX 乙取证”待办", "待办已记录，当前为进行中"],
        )
        self.assertEqual(
            [c["component"] for c in calls["send"]], ["primary:0", "primary:1"]
        )
        for index in (0, 1):
            self.assertEqual(
                tracker.get_component(
                    run_id="run-e2-h", component_id=f"history:{index}"
                )["state"],
                "sent",
            )
        self.assertEqual(calls["settle"], [])
        # 第二份在终态帧到达前就已发出（终态只做折叠，不再发）
        self.assertEqual(chat.sent_at, [c["text"] for c in calls["send"]])
        plan = delivery_plan_units(tracker.get_delivery("run-e2-h"))
        self.assertEqual([u["cleaned_text"] for u in plan],
                         ["先记录“E0-FIX 乙取证”待办", "待办已记录，当前为进行中"])

    async def test_normal_multi_decision_immediate_and_folded(self) -> None:
        data = load_sample("runs_terminal_multi_b5.json")
        tracker, chat, calls = await self._run_stream(
            data["events"], output=data["status"]["output"]
        )
        self.assertEqual(
            [c["text"] for c in calls["send"]], ["第一份正文", "第二份第一行\n第二份第二行"]
        )
        self.assertEqual(chat.sent_at, [c["text"] for c in calls["send"]])

    async def test_same_text_in_two_segments_sends_both_once(self) -> None:
        body = json.dumps({"body": "同一句"}, ensure_ascii=False)
        events = [
            {"event": "message.delta", "delta": "{}", "seq": 0},
            {"event": "message.delta", "delta": "\n" + body, "seq": 1},
            {"event": "tool.started", "tool": "todo_list", "seq": 2},
            {"event": "tool.completed", "tool": "todo_list", "seq": 3},
            {"event": "message.delta", "delta": "\n{}", "seq": 4},
            {"event": "message.delta", "delta": "\n" + body, "seq": 5},
        ]
        tracker, chat, calls = await self._run_stream(events, output="{}\n" + body)
        self.assertEqual([c["text"] for c in calls["send"]], ["同一句", "同一句"])
        self.assertEqual(chat.sent_at, ["同一句", "同一句"])
        self.assertEqual(
            [c["component"] for c in calls["send"]], ["primary:0", "primary:1"]
        )

    async def test_silent_empty_body_control_kept_and_next_unit_sent(self) -> None:
        events = [
            {"event": "message.delta", "delta": '{"topic_hint":"收尾"}', "seq": 0},
            {"event": "message.delta", "delta": '\n{"body":""}', "seq": 1},
            {"event": "tool.started", "tool": "todo_list", "seq": 2},
            {"event": "tool.completed", "tool": "todo_list", "seq": 3},
            {"event": "message.delta", "delta": '\n{}\n{"body":"正式答复"}', "seq": 4},
        ]
        tracker, chat, calls = await self._run_stream(
            events, output='{}\n{"body":"正式答复"}'
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["正式答复"])
        self.assertEqual(calls["control"]["update_topic"], 1)
        self.assertEqual(calls["settle"], [])

    async def test_terminal_only_unit_appended_and_sent(self) -> None:
        events = [
            {"event": "message.delta", "delta": '{"body":"已流式"}', "seq": 0},
        ]
        tracker, chat, calls = await self._run_stream(
            events, output='{"body":"已流式"}\n{"body":"终态补份"}'
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["已流式", "终态补份"])
        self.assertEqual(
            [c["component"] for c in calls["send"]], ["primary:0", "primary:1"]
        )
        plan = delivery_plan_units(tracker.get_delivery("run-e2-h"))
        self.assertEqual([u["cleaned_text"] for u in plan], ["已流式", "终态补份"])

    async def test_pending_stream_unit_reused_not_duplicated(self) -> None:
        """即时发送中途失败的 pending 份：终态只补这一份，不追加重复计划。"""
        from nonebot_plugin_hermes_adapter.core.runs_terminal import units_from_decisions

        def u(body: str) -> dict:
            return units_from_decisions([structured_from_terminal(
                decode_terminal_batch(unit({}, body))[0]
            )])[0][1]

        tracker = self._tracker(record_delivery=False)
        append_stream_unit(tracker=tracker, run_id="run-e2-h", unit=u("A"))
        append_stream_unit(tracker=tracker, run_id="run-e2-h", unit=u("B"))
        tracker.mark_component_sent(run_id="run-e2-h", component_id="primary:0", msg_ids=["m1"])
        tracker.record_expected_delivery(
            run_id="run-e2-h", output=unit({}, "B"), expect_structured=True
        )
        streamed = [structured_from_terminal(decode_terminal_batch(unit({}, "A"))[0])]
        result = _chat_result(unit({}, "B"), streamed)
        calls = await self._run(result, tracker=tracker)
        self.assertEqual([c["text"] for c in calls["send"]], ["B"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:1"])
        plan = delivery_plan_units(tracker.get_delivery("run-e2-h"))
        self.assertEqual([x["cleaned_text"] for x in plan], ["A", "B"])

    async def test_steer_midway_gets_immediate_reply(self) -> None:
        events = [
            {"event": "message.delta", "delta": '{"body":"先答这句"}', "seq": 0},
            {"event": "run.steered", "accepted": True, "seq": 1},
            {"event": "message.delta", "delta": '\n\n{}\n{"body":"回应 steer"}', "seq": 2},
        ]
        tracker, chat, calls = await self._run_stream(
            events, output='{}\n{"body":"回应 steer"}'
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["先答这句", "回应 steer"])
        self.assertEqual(chat.sent_at, ["先答这句", "回应 steer"])
        self.assertEqual(calls["settle"], [])

    async def test_terminal_replay_does_not_resend(self) -> None:
        data = load_sample("runs_terminal_single_b4.json")
        # 同一份终态帧重复到达（重连重放）：终态只折叠，不新增发送
        tracker, chat, calls = await self._run_stream(
            data["events"] + data["events"][-1:], output=data["status"]["output"]
        )
        self.assertEqual(len(calls["send"]), 2)

    def _real_chat(self, tracker, events: list[dict], run_id: str = "run-e2-h"):
        """真实 HermesClient.chat + 真实 SSE → 真实 handler（只换 HTTP 与发送边界）。"""
        return real_chat(tracker, events, run_id)

    def _plain_tracker(self):
        """真实 client 自己登记 run/submission，不能复用已绑定的 harness 台账。"""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        return tracker

    async def test_interim_enabled_does_not_resend_decision_envelope(self) -> None:
        """R1：开启旁白转发后，已即时发送的决策信封不得再当旁白发一遍。"""
        data = load_sample("runs_terminal_single_b4.json")
        tracker = self._plain_tracker()
        calls = await self._run(
            None, tracker=tracker,
            chat=self._real_chat(tracker, data["events"]),
            interim_echo=True,
        )
        texts = [c["text"] for c in calls["send"]]
        self.assertEqual(texts, ["先记录“E0-FIX 乙取证”待办", "待办已记录，当前为进行中"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:0", "primary:1"])
        self.assertFalse(any('"body"' in text or "{}" in text for text in texts))

    async def test_interim_enabled_keeps_ordinary_narration(self) -> None:
        """R1：隔离决策不等于关掉旁白——普通过程旁白仍然即时转发一次。"""
        body = json.dumps({"body": "正式答复"}, ensure_ascii=False)
        events = [
            {"event": "message.delta", "delta": "先查一下资料。", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
            {"event": "tool.completed", "tool": "todo_list", "seq": 2},
            {"event": "message.delta", "delta": "\n\n{}\n", "seq": 3},
            {"event": "message.delta", "delta": body, "seq": 4},
        ]
        tracker = self._plain_tracker()
        calls = await self._run(
            None, tracker=tracker,
            chat=self._real_chat(tracker, events),
            interim_echo=True,
        )
        self.assertEqual(
            [c["text"] for c in calls["send"]], ["先查一下资料。", "正式答复"]
        )
        # 旁白一次（component 为 None），正文一次（primary:0）
        self.assertEqual([c["component"] for c in calls["send"]], [None, "primary:0"])

    async def test_interim_cannot_bypass_bounce(self) -> None:
        """R1：bounce 拦下的正文不能从旁白出口绕出去，正常份照发。"""
        data = load_sample("runs_terminal_single_b4.json")
        tracker = self._plain_tracker()
        calls = await self._run(
            None, tracker=tracker,
            chat=self._real_chat(tracker, data["events"]),
            interim_echo=True,
            bounce=[True, False],
        )
        self.assertEqual(
            [c["text"] for c in calls["send"]], ["待办已记录，当前为进行中"]
        )
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:1"])
        self.assertIn(
            tracker.get_component(run_id="run-e2-h", component_id="primary:0")["state"],
            ("cancelled", "not_required"),
        )

    async def test_local_image_sent_once_reactive(self) -> None:
        """R2：delta 正文里的本地图片与终态内联后是同一份，只发一次、一份计划。"""
        from nonebot_plugin_hermes_adapter.core import media_whitelist as mw

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        img = Path(tmp.name) / "actual.png"
        img.write_bytes(_png_bytes())
        output = unit({}, f"图在这里 ![image]({img})")
        events = [
            {"event": "message.delta", "delta": output, "seq": 0},
            {"event": "run.completed", "output": output, "seq": 1},
        ]
        tracker = self._plain_tracker()
        with patch.object(mw, "allowed_roots", return_value=[img.parent]):
            calls = await self._run(
                None, tracker=tracker, chat=self._real_chat(tracker, events)
            )
        self.assertEqual(len(calls["send"]), 1)
        self.assertEqual(calls["send"][0]["text"], "图在这里")
        self.assertTrue(calls["send"][0]["media_urls"][0].startswith("data:image/png;base64,"))
        plan = delivery_plan_units(tracker.get_delivery("run-e2-h"))
        self.assertEqual([u["cleaned_text"] for u in plan], ["图在这里"])


class PassiveLiveChainTests(_e2._PassiveHarness):
    """私聊 passive 的 runs 决策同样即时发送；群 passive 不受影响。"""

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    def _plain_tracker(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        return tracker

    async def test_private_passive_multi_immediate(self) -> None:
        data = load_sample("runs_terminal_multi_b5.json")
        events = data["events"] + [{
            "event": "run.completed", "output": data["status"]["output"], "seq": 999,
        }]
        tracker = self._tracker(record_delivery=False)
        chat = _StreamingChat(tracker, events, run_id="run-e2-p")
        calls = await self._run(None, tracker=tracker, chat=chat)
        self.assertEqual(
            [c["text"] for c in calls["send"]], ["第一份正文", "第二份第一行\n第二份第二行"]
        )
        self.assertEqual(
            [c["component"] for c in calls["send"]], ["primary:0", "primary:1"]
        )
        self.assertEqual(calls["settle"], [])

    async def test_private_passive_local_image_sent_once(self) -> None:
        """R2：私聊 passive 同样用真实 client 内联，一份正文/媒体只发一次。"""
        from nonebot_plugin_hermes_adapter.core import media_whitelist as mw

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        img = Path(tmp.name) / "actual.png"
        img.write_bytes(_png_bytes())
        output = unit({}, f"图在这里 ![image]({img})")
        events = [
            {"event": "message.delta", "delta": output, "seq": 0},
            {"event": "run.completed", "output": output, "seq": 1},
        ]
        tracker = self._plain_tracker()
        with patch.object(mw, "allowed_roots", return_value=[img.parent]):
            calls = await self._run(
                None, tracker=tracker, chat=real_chat(tracker, events, "run-e2-p")
            )
        self.assertEqual([c["text"] for c in calls["send"]], ["图在这里"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:0"])
        self.assertTrue(calls["send"][0]["media_urls"][0].startswith("data:image/png;base64,"))
        plan = delivery_plan_units(tracker.get_delivery("run-e2-p"))
        self.assertEqual([u["cleaned_text"] for u in plan], ["图在这里"])


class PushGateTests(unittest.IsolatedAsyncioTestCase):
    def _tracker(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        tracker.upsert(
            run_id="r1", scope_key="onebotv11:group:g1", adapter="onebotv11",
            is_private=False, user_id="u1", group_id="g1", bot_self_id="999",
            session_key="s", mode="reactive", expect_structured=True,
        )
        return tracker

    async def _push(self, *, tracker, interface="runs", user_id=None, group_id="g1"):
        from nonebot_plugin_hermes_adapter.mcp.tools import push_message as pm
        from nonebot_plugin_hermes_adapter import mcp as mcp_module

        inp = pm.PushMessageInput(adapter="onebotv11", group_id=group_id, user_id=user_id, text="hi")
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", interface)
        try:
            with patch.object(pm, "validate_push_context"), \
                    patch.object(pm, "send_text_with_media", AsyncMock(return_value=None)) as send, \
                    patch.object(mcp_module, "run_tracker", tracker):
                result = await pm.push_message_impl(
                    inp, active_sessions=MagicMock(), bot_registry=MagicMock(),
                    scope=MagicMock(),
                )
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        return result, send

    async def test_in_turn_group_push_rejected(self) -> None:
        result, send = await self._push(tracker=self._tracker())
        self.assertFalse(result.ok)
        self.assertIn("runs turn in progress", result.error or "")
        send.assert_not_called()

    async def test_completions_push_unchanged(self) -> None:
        result, send = await self._push(tracker=self._tracker(), interface="completions")
        self.assertNotIn("runs turn in progress", result.error or "")


class ResumeFoldChainTests(unittest.IsolatedAsyncioTestCase):
    """断流恢复：已即时发送的前缀份不重发，只补计划里未发送的份。"""

    async def test_resume_sends_only_pending_unit(self) -> None:
        from nonebot_plugin_hermes_adapter.core.runs_terminal import units_from_decisions

        run_resume = _load_run_resume()

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        tracker.upsert(
            run_id="r1", scope_key="onebotv11:group:g1", adapter="onebotv11",
            is_private=False, user_id="u1", group_id="g1", bot_self_id="999",
            session_key="s", mode="reactive", expect_structured=True,
        )
        tracker.record_submission(
            idempotency_key="idem", request_body={}, target_fingerprint="x|y",
            scope_key="onebotv11:group:g1", session_key="s", mode="reactive",
            expect_structured=True, bot_self_id="999",
        )
        tracker.bind_submission_and_register(
            idempotency_key="idem", run_id="r1", scope_key="onebotv11:group:g1",
            adapter="onebotv11", is_private=False, user_id="u1", group_id="g1",
            bot_self_id="999", session_key="s", mode="reactive", expect_structured=True,
        )

        def u(body: str) -> dict:
            return units_from_decisions([structured_from_terminal(
                decode_terminal_batch(unit({}, body))[0]
            )])[0][1]

        # 模拟：A 已即时发送，B 只落了计划（进程在发送前断掉）
        append_stream_unit(tracker=tracker, run_id="r1", unit=u("A"))
        append_stream_unit(tracker=tracker, run_id="r1", unit=u("B"))
        tracker.mark_component_sent(run_id="r1", component_id="primary:0", msg_ids=["m1"])
        output = unit({}, "B")
        tracker.record_expected_delivery(run_id="r1", output=output, expect_structured=True)

        sent: list[str] = []

        async def fake_send(**kwargs):
            sent.append(kwargs["text"])
            return SimpleNamespace(ok=True, msg_ids=["m2"], primary_qq_message_id="m2")

        class _Resp:
            status_code = 200

            def json(self):
                return {"status": "completed", "updated_at": __import__("time").time(),
                        "output": output}

        class _Client:
            def __init__(self, *a, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def get(self, url, headers=None):
                return _Resp()

        row = tracker.get_by_run_id("r1")
        self.assertIsNotNone(row)
        with patch.object(run_resume.httpx, "AsyncClient", _Client), \
                patch.object(run_resume, "resolve_target", MagicMock(return_value=SimpleNamespace(
                    base_url="http://x", api_key="", timeout=5))), \
                patch.object(run_resume, "get_bot", MagicMock(return_value=MagicMock(self_id=1))), \
                patch.object(run_resume, "send_text_with_media", fake_send), \
                patch.object(run_resume, "send_local_files", AsyncMock(return_value=0)), \
                patch.object(run_resume, "get_bot_nickname", AsyncMock(return_value="nano")), \
                patch.object(run_resume, "deliver_companion_run_output", AsyncMock(return_value=False)), \
                patch.object(run_resume, "reconcile_run", AsyncMock(return_value={})), \
                patch.object(run_resume._mcp, "run_tracker", tracker), \
                patch.object(run_resume._mcp, "bot_registry", MagicMock(get=MagicMock(
                    return_value=SimpleNamespace(bot_self_id="999", target=object())))), \
                patch.object(run_resume._mcp, "inflight", MagicMock(is_inflight=MagicMock(return_value=False))), \
                patch.object(run_resume._mcp, "message_buffer", MagicMock()), \
                patch.object(run_resume._mcp, "active_sessions", MagicMock(
                    get_if_trigger_alive=MagicMock(return_value=None))):
            await run_resume._resume_row_impl(row)

        self.assertEqual(sent, ["B"])
        # 已即时发送的前缀份没有被恢复链重发/重置
        self.assertEqual(
            tracker.get_component(run_id="r1", component_id="primary:0")["msg_ids"], ["m1"]
        )
        plan = delivery_plan_units(tracker.get_delivery("r1"))
        self.assertEqual([x["cleaned_text"] for x in plan], ["A", "B"])


class ResumeSilentTerminalTests(unittest.IsolatedAsyncioTestCase):
    """R3：工具前落账、发送前中断 → 重启后终态静默，pending 流式份仍须补发一次。"""

    RUN_ID = "r1"

    def _tracker(self):
        tracker = RunTracker(Path(tempfile.mkdtemp(prefix="e3-r3-")) / "t.db")
        self.addCleanup(tracker.close)
        tracker.upsert(
            run_id=self.RUN_ID, scope_key="onebotv11:group:g1", adapter="onebotv11",
            is_private=False, user_id="u1", group_id="g1", bot_self_id="999",
            session_key="s", mode="reactive", expect_structured=True,
        )
        tracker.record_submission(
            idempotency_key="idem", request_body={}, target_fingerprint="x|y",
            scope_key="onebotv11:group:g1", session_key="s", mode="reactive",
            expect_structured=True, bot_self_id="999",
        )
        tracker.bind_submission_and_register(
            idempotency_key="idem", run_id=self.RUN_ID, scope_key="onebotv11:group:g1",
            adapter="onebotv11", is_private=False, user_id="u1", group_id="g1",
            bot_self_id="999", session_key="s", mode="reactive", expect_structured=True,
        )
        return tracker

    def _stream_unit(self, body: str) -> dict:
        from nonebot_plugin_hermes_adapter.core.runs_terminal import units_from_decisions

        return units_from_decisions([structured_from_terminal(
            decode_terminal_batch(unit({}, body))[0]
        )])[0][1]

    async def _resume(self, tracker, *, output: str, sent: list[str]):
        import time

        run_resume = _load_run_resume()
        row = tracker.get_by_run_id(self.RUN_ID)
        self.assertIsNotNone(row)

        async def fake_send(**kwargs):
            sent.append(kwargs["text"])
            delivery = kwargs.get("delivery")
            if delivery is not None:
                # 真实 outbound 在平台成功后按同一交付上下文回写 sent；这里替掉的是
                # 平台 I/O，回执契约照旧，后续对账才能看到真实终态。
                delivery.tracker.mark_component_sent(
                    run_id=delivery.run_id, component_id=delivery.component_id,
                    msg_ids=["ack"],
                )
            return SimpleNamespace(ok=True, msg_ids=["ack"], primary_qq_message_id="ack")

        class _Resp:
            status_code = 200

            def json(self):
                return {"status": "completed", "updated_at": time.time(), "output": output}

        class _Client:
            def __init__(self, *a, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def get(self, url, headers=None):
                return _Resp()

        with patch.object(run_resume.httpx, "AsyncClient", _Client), \
                patch.object(run_resume, "resolve_target", MagicMock(return_value=SimpleNamespace(
                    base_url="http://x", api_key="", timeout=5))), \
                patch.object(run_resume, "get_bot", MagicMock(return_value=MagicMock(self_id=1))), \
                patch.object(run_resume, "send_text_with_media", fake_send), \
                patch.object(run_resume, "send_local_files", AsyncMock(return_value=0)), \
                patch.object(run_resume, "get_bot_nickname", AsyncMock(return_value="nano")), \
                patch.object(run_resume, "deliver_companion_run_output", AsyncMock(return_value=False)), \
                patch.object(run_resume, "reconcile_run", AsyncMock(return_value={})), \
                patch.object(run_resume._mcp, "run_tracker", tracker), \
                patch.object(run_resume._mcp, "bot_registry", MagicMock(get=MagicMock(
                    return_value=SimpleNamespace(bot_self_id="999", target=object())))), \
                patch.object(run_resume._mcp, "inflight", MagicMock(is_inflight=MagicMock(return_value=False))), \
                patch.object(run_resume._mcp, "message_buffer", MagicMock()), \
                patch.object(run_resume._mcp, "active_sessions", MagicMock()):
            await run_resume._resume_row_impl(row)

    async def test_silent_terminal_keeps_pending_stream_unit(self) -> None:
        tracker = self._tracker()
        append_stream_unit(tracker=tracker, run_id=self.RUN_ID, unit=self._stream_unit("工具前已形成但尚未发送"))
        output = unit({}, "")
        tracker.record_expected_delivery(run_id=self.RUN_ID, output=output, expect_structured=True)
        sent: list[str] = []
        await self._resume(tracker, output=output, sent=sent)
        self.assertEqual(sent, ["工具前已形成但尚未发送"])
        delivery = tracker.get_delivery(self.RUN_ID)
        self.assertNotEqual(delivery["expected_state"], "silent")
        self.assertEqual(
            [u["cleaned_text"] for u in delivery_plan_units(delivery)],
            ["工具前已形成但尚未发送"],
        )
        self.assertEqual(
            tracker.get_component(run_id=self.RUN_ID, component_id="primary:0")["state"],
            "sent",
        )

    async def test_silent_terminal_only_resends_pending_not_sent(self) -> None:
        tracker = self._tracker()
        append_stream_unit(tracker=tracker, run_id=self.RUN_ID, unit=self._stream_unit("已发出"))
        append_stream_unit(tracker=tracker, run_id=self.RUN_ID, unit=self._stream_unit("未发出"))
        tracker.mark_component_sent(run_id=self.RUN_ID, component_id="primary:0", msg_ids=["m1"])
        output = unit({}, "")
        tracker.record_expected_delivery(run_id=self.RUN_ID, output=output, expect_structured=True)
        sent: list[str] = []
        await self._resume(tracker, output=output, sent=sent)
        self.assertEqual(sent, ["未发出"])
        self.assertEqual(
            tracker.get_component(run_id=self.RUN_ID, component_id="primary:0")["msg_ids"],
            ["m1"],
        )

    async def test_followup_reconcile_does_not_resend_or_strand(self) -> None:
        """补发后交付台账进入终态：后续对账不重发、也不把 pending 份当漏件。"""
        from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor

        tracker = self._tracker()
        append_stream_unit(tracker=tracker, run_id=self.RUN_ID, unit=self._stream_unit("只发一次"))
        output = unit({}, "")
        tracker.record_expected_delivery(run_id=self.RUN_ID, output=output, expect_structured=True)
        sent: list[str] = []
        await self._resume(tracker, output=output, sent=sent)
        self.assertEqual(sent, ["只发一次"])

        async def fake_send(**kwargs):
            sent.append(kwargs["text"])
            return SimpleNamespace(ok=True, msg_ids=["again"])

        runtime = SimpleNamespace(
            run_tracker=tracker,
            message_buffer=MagicMock(),
            bot_registry=MagicMock(get=MagicMock(
                return_value=SimpleNamespace(bot_self_id="999", target=object()))),
        )
        cfg = SimpleNamespace(
            mode="repair", repair_scopes=(), grace_sec=0.0,
            order_gate_timeout_sec=1.0, unknown_backoff_sec=0.0,
            unknown_max_attempts=2, retention_hours=24.0,
        )
        with patch.object(monitor, "_runtime", MagicMock(return_value=runtime)), \
                patch.object(monitor, "load_runs_delivery_config", MagicMock(return_value=cfg)), \
                patch.object(monitor, "get_bot", MagicMock(return_value=MagicMock(self_id=1))), \
                patch.object(monitor, "_send_text", fake_send), \
                patch.object(monitor, "_send_files", AsyncMock(return_value=0)):
            stats = await monitor.reconcile_run(self.RUN_ID, reason="startup")
        self.assertEqual(sent, ["只发一次"])
        self.assertEqual(stats.get("terminal"), True)
        self.assertEqual(tracker.get_delivery(self.RUN_ID)["expected_state"], "terminal")
        self.assertEqual(tracker.list_ready_deliveries(), [])

    async def test_sent_primary_pending_file_survives_silent_terminal(self) -> None:
        """R4：正文 ack 后、文件上传前中断 → 恢复不得 silent 销账，对账补文件/history。"""
        from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor

        tracker = self._tracker()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "result.txt"
        path.write_text("actual generated file", encoding="utf-8")
        append_stream_unit(
            tracker=tracker, run_id=self.RUN_ID,
            unit=self._stream_unit(f"结果文件 MEDIA:{path}"),
        )
        # 真实中断窗口：正文已回写 sent，文件与历史尚未落盘。
        tracker.mark_component_sent(
            run_id=self.RUN_ID, component_id="primary:0", msg_ids=["primary-ack"]
        )
        self.assertEqual(
            tracker.get_component(run_id=self.RUN_ID, component_id="file:0:0")["state"],
            "pending",
        )
        output = unit({}, "")
        tracker.record_expected_delivery(
            run_id=self.RUN_ID, output=output, expect_structured=True
        )
        sent: list[str] = []
        await self._resume(tracker, output=output, sent=sent)
        # 已 ack 的正文不重发；整份不得被静默销账退出 ready 扫描。
        self.assertEqual(sent, [])
        self.assertEqual(
            tracker.get_component(run_id=self.RUN_ID, component_id="file:0:0")["state"],
            "pending",
        )
        self.assertNotEqual(tracker.get_delivery(self.RUN_ID)["expected_state"], "silent")
        self.assertIn(
            self.RUN_ID, [r["run_id"] for r in tracker.list_ready_deliveries()]
        )

        # 既有对账路径补文件与 history，最终落 terminal；正文仍不重发。
        files: list[str] = []
        resent: list[str] = []

        async def upload(**kwargs):
            files.extend(kwargs["file_paths"])
            tracker.mark_component_sent(
                run_id=self.RUN_ID, component_id="file:0:0", msg_ids=["file-ack"]
            )
            return 1

        async def resend(**kwargs):
            resent.append(kwargs.get("text") or "")
            return SimpleNamespace(ok=True, msg_ids=["again"])

        cfg = SimpleNamespace(
            mode="repair", repair_scopes=(), grace_sec=0.0,
            order_gate_timeout_sec=1.0, unknown_backoff_sec=0.0,
            unknown_max_attempts=2, retention_hours=24.0,
        )
        runtime = SimpleNamespace(
            run_tracker=tracker, message_buffer=MagicMock(),
            bot_registry=MagicMock(get=MagicMock(
                return_value=SimpleNamespace(bot_self_id="999", target=object()))),
        )
        with patch.object(monitor, "_runtime", MagicMock(return_value=runtime)), \
                patch.object(monitor, "load_runs_delivery_config", MagicMock(return_value=cfg)), \
                patch.object(monitor, "get_bot", MagicMock(return_value=MagicMock(self_id=1))), \
                patch.object(monitor, "_send_files", upload), \
                patch.object(monitor, "_send_text", resend):
            stats = await monitor.reconcile_run(self.RUN_ID, reason="startup")
        self.assertEqual(files, [str(path)])
        self.assertEqual(resent, [])
        self.assertEqual(stats.get("terminal"), True)
        self.assertEqual(tracker.get_delivery(self.RUN_ID)["expected_state"], "terminal")
        self.assertEqual(tracker.list_ready_deliveries(), [])


class ReactiveControlParityTests(_e2._ReactiveHarness):
    """R5/R6：delta 与终态切帧必须得到同一份锚控制／窗口生命周期结果。"""

    RUN_ID = "run-e2-h"

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    def _real_chat(self, tracker, events):
        return real_chat(tracker, events, self.RUN_ID)

    def _plain_tracker(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        return tracker

    @staticmethod
    def _manager_with_commission(now: int):
        from nonebot_plugin_hermes_adapter.core.active_session import (
            ActiveSessionManager,
            AnchorRecord,
            Participant,
        )

        manager = ActiveSessionManager()
        session = manager.trigger("onebotv11", "g1", "u1", now_ms=now)
        manager.pin_anchor(
            session,
            AnchorRecord(
                participants=[Participant(user_id="u1", nickname="user", joined_ts=now)],
                m_id=1, platform_message_id="source", text="真实委托", ts=now,
            ),
            master_ids=set(), max_age_ms=3600_000, now_ms=now,
        )
        return manager

    async def _run_with_manager(self, *, manager, output: str, live: bool, ephemeral: bool, bounce=None):
        import time as _time
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        # 真实 harness 收尾会读这两个调用的 call_count；包一层不影响状态机本身。
        # harness 的 ephemeral 替身 session 不是真实 ActiveSession（无 adapter 键），
        # 真实管理器对「不在册的临时 session」本就是 no-op，这里显式返回 False。
        if not isinstance(manager.end_if_current_generation, MagicMock):
            real_end = manager.end_if_current_generation

            def _end_if_current(session, generation):
                if not hasattr(session, "adapter"):
                    return False
                return real_end(session, generation)

            manager.end_if_current_generation = MagicMock(side_effect=_end_if_current)
        if not isinstance(manager.update_topic, MagicMock):
            manager.update_topic = MagicMock(wraps=manager.update_topic)
        now = int(_time.time() * 1000)
        events = ([{"event": "message.delta", "delta": output, "seq": 0}] if live else []) + [
            {"event": "run.completed", "output": output, "seq": 1}
        ]
        tracker = self._plain_tracker()
        original = mt._run_reactive_turn

        async def invoke(**kwargs):
            # 真实 public chat→SSE→reactive handler 链，只换窗口管理器与时间基准。
            mt._mcp.active_sessions = manager
            mt.load_chat_config.return_value.anchor_max_age_sec = 3600
            kwargs["now_ms"] = now
            kwargs["ephemeral"] = ephemeral
            return await original(**kwargs)

        with patch.object(mt, "_run_reactive_turn", invoke):
            calls = await self._run(
                None, tracker=tracker, chat=self._real_chat(tracker, events), bounce=bounce
            )
        return manager, calls

    async def test_streamed_anchor_controls_equal_terminal_only(self) -> None:
        import time as _time

        now = int(_time.time() * 1000)
        output = unit({"answer_anchor": "all", "release_anchor": "all"}, "任务完成")
        terminal_manager, terminal_calls = await self._run_with_manager(
            manager=self._manager_with_commission(now), output=output, live=False, ephemeral=False
        )
        live_manager, live_calls = await self._run_with_manager(
            manager=self._manager_with_commission(now), output=output, live=True, ephemeral=False
        )
        self.assertEqual([c["text"] for c in terminal_calls["send"]], ["任务完成"])
        self.assertEqual([c["text"] for c in live_calls["send"]], ["任务完成"])
        self.assertEqual(
            live_manager.pinned_commissioners("onebotv11", "g1"),
            terminal_manager.pinned_commissioners("onebotv11", "g1"),
        )
        self.assertEqual(live_manager.pinned_commissioners("onebotv11", "g1"), set())

    async def test_cold_window_opens_on_immediate_speech(self) -> None:
        from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager

        output = unit({}, "正式答复")
        terminal_manager, terminal_calls = await self._run_with_manager(
            manager=ActiveSessionManager(), output=output, live=False, ephemeral=True
        )
        live_manager, live_calls = await self._run_with_manager(
            manager=ActiveSessionManager(), output=output, live=True, ephemeral=True
        )
        self.assertEqual([c["text"] for c in terminal_calls["send"]], ["正式答复"])
        self.assertEqual([c["text"] for c in live_calls["send"]], ["正式答复"])
        self.assertIsNotNone(terminal_manager.get("onebotv11", "g1"))
        self.assertIsNotNone(live_manager.get("onebotv11", "g1"))

    async def test_cold_window_not_opened_when_turn_exits(self) -> None:
        from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager

        output = unit({"should_exit_active": True}, "谈完了")
        live_manager, live_calls = await self._run_with_manager(
            manager=ActiveSessionManager(), output=output, live=True, ephemeral=True
        )
        self.assertEqual([c["text"] for c in live_calls["send"]], ["谈完了"])
        self.assertIsNone(live_manager.get("onebotv11", "g1"))

    async def test_bounced_immediate_speech_does_not_open_window(self) -> None:
        from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager

        output = unit({}, "被拦截")
        live_manager, live_calls = await self._run_with_manager(
            manager=ActiveSessionManager(), output=output,
            live=True, ephemeral=True, bounce=[True],
        )
        self.assertEqual(live_calls["send"], [])
        self.assertIsNone(live_manager.get("onebotv11", "g1"))

    async def test_failed_live_send_not_recorded_as_sent(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        tracker = self._tracker(record_delivery=False)
        decision = structured_from_terminal(decode_terminal_batch(unit({}, "正文"))[0])

        async def fail_send(**kwargs):
            return None

        with patch.object(mt, "_mcp", SimpleNamespace(
                    run_tracker=tracker, message_buffer=MagicMock(),
                    active_sessions=MagicMock())), \
                patch.object(mt, "_maybe_intercept_bounce", AsyncMock(return_value=False)), \
                patch.object(mt, "resolve_reply", MagicMock(return_value=None)), \
                patch.object(mt, "send_text_with_media", fail_send), \
                patch.object(mt, "send_local_files", AsyncMock(return_value=0)):
            deliver, state = mt._make_runs_live_decisions(
                bot=MagicMock(self_id=1), target=object(), adapter_name="onebotv11",
                user_id="u1", group_id="g1", is_private=False, fb_key="k",
                nickname="n", session=None, mark_replied=False,
            )
            await deliver(decision, self.RUN_ID)
        self.assertEqual(state["outcomes"][id(decision)][0], "failed")


class RetryGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_persistence_retry_skipped_after_live_send(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        failure = ChatResult(raw_text="x", parse_failed=True, is_transport_error=True)
        failure.is_persistence_error = True
        failure.persistence_cause = "locked"

        async def deliver(decision, run_id):
            return None

        setattr(deliver, "has_sent", lambda: True)
        with patch.object(mt, "_chat_and_adopt_rotation", AsyncMock(return_value=failure)) as call:
            result = await mt._chat_with_persistence_retry(
                "reactive", decision_callback=deliver, session_key="s",
                adapter_name="onebotv11", is_private=False, user_id="u1", group_id="g1",
            )
        self.assertIs(result, failure)
        self.assertEqual(call.await_count, 1)
