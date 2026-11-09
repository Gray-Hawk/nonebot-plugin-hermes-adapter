"""纯文本正文协议：Runs 段落切分、段内去重、终态与真实链式回放。

覆盖实现卡 t_979bff13 的硬验收：
- 单句零工具零额外调用、§ 跨帧闭合与连发、静默／进度／最终；
- 段内 interim／delta 同文去重、跨段同文两份；
- 控制帧在 tool.started 封段（不切批）、控制 FIFO 与作用域；
- 旧 JSON 逐字原样一条、不识别控制语义；
- 流式／终态／恢复一致，断流与协议失败关闭。
"""

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
from nonebot_plugin_hermes_adapter.core import runs_text  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    append_stream_unit,
    build_batch_plan,
    delivery_plan_units,
    prepare_terminal_batch,
    units_from_decisions,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from tests import test_runs_terminal as _e2  # noqa: E402


def sse_lines(events: list[dict]) -> list[str]:
    """真实事件 payload → 上游 runs 的 data-only SSE 行形态。"""
    return [f"data: {json.dumps(event, ensure_ascii=False)}" for event in events]


def unit(control: dict | None = None, body: str = "正文") -> str:
    """纯文本协议：一份正文就是这段文本（control 形参只为保留调用形状）。"""
    return body


def batch(*pairs) -> str:
    return "\n§\n".join(unit(control, body) for control, body in pairs)


def decisions(*bodies) -> list[dict]:
    return [{"body": body, "should_reply": True} for body in bodies if str(body).strip()]


def feed_all(parser: RunsEventParser, events: list[dict]) -> list[dict]:
    """按生产消费循环同口径喂帧：每帧后取走新单元。"""
    drained: list[dict] = []
    for line in sse_lines(events):
        parser.feed(line)
        drained.extend(parser.drain_stream_decisions())
    return drained


class TextProtocolParserTests(unittest.TestCase):
    """parser 只做「段 + 段内全文匹配」，不产出旁白通道、不识别旧 JSON。"""

    def test_single_sentence_is_one_verbatim_unit(self) -> None:
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.delta", "delta": "你好", "seq": 0},
            {"event": "run.completed", "output": "你好", "seq": 1},
        ])
        self.assertEqual(drained, [])
        self.assertEqual(parser.output, "你好")
        # 终态只认 output：同文不会因为 delta 也出现过而提前发。
        self.assertEqual(parser.text_error, "")

    def test_three_units_split_by_standalone_separator(self) -> None:
        text = batch(({}, "甲"), ({}, "乙"), ({}, "丙"))
        self.assertEqual(runs_text.split_text_units(text), ["甲", "乙", "丙"])

    def test_separator_split_across_delta_frames(self) -> None:
        events = [
            {"event": "message.delta", "delta": "甲\n", "seq": 0},
            {"event": "message.delta", "delta": "§", "seq": 1},
            {"event": "message.delta", "delta": "\n乙", "seq": 2},
            {"event": "tool.started", "tool": "todo_list", "seq": 3},
        ]
        parser = RunsEventParser()
        drained = feed_all(parser, events)
        # 工具边界封段：跨帧闭合的 § 正确切开，两段都发。
        self.assertEqual([d["body"] for d in drained], ["甲", "乙"])

    def test_separator_with_spaces_does_not_split(self) -> None:
        for line in (" §", "§ ", " § "):
            parser = RunsEventParser()
            drained = feed_all(parser, [
                {"event": "message.delta", "delta": f"甲\n{line}\n乙", "seq": 0},
                {"event": "tool.started", "tool": "todo_list", "seq": 1},
            ])
            self.assertEqual([d["body"] for d in drained], [f"甲\n{line}\n乙"])

    def test_spaced_separator_across_frames_does_not_split(self) -> None:
        parser = RunsEventParser()
        spilled: list[dict] = []
        for line in sse_lines([
            {"event": "message.delta", "delta": "甲\n ", "seq": 0},
            {"event": "message.delta", "delta": "§\n", "seq": 1},
            {"event": "message.delta", "delta": "乙", "seq": 2},
            {"event": "tool.started", "tool": "todo_list", "seq": 3},
        ]):
            parser.feed(line)
            spilled.extend(parser.drain_stream_decisions())
        # 跨帧拼出的是「空格 + §」，不是独立整行 §，所以仍是一条消息。
        self.assertEqual([d["body"] for d in spilled], ["甲\n §\n乙"])

    def test_multiline_body_is_not_split(self) -> None:
        body = "第一行\n第二行\n\n第三段"
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.delta", "delta": body, "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ])
        self.assertEqual([d["body"] for d in drained], [body])

    def test_interim_one_char_progress_is_sent(self) -> None:
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.interim", "text": "好", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ])
        # 不走 _INTERIM_MIN_CHARS：1 字进度原样发出。
        self.assertEqual([d["body"] for d in drained], ["好"])
        self.assertEqual(parser.interim_count, 1)

    def test_interim_json_literal_progress_is_sent_verbatim(self) -> None:
        literal = '{"body":"进度"}'
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.interim", "text": literal, "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ])
        # 不经过 strip_complete_decisions：旧 JSON 片段按正文原样发。
        self.assertEqual([d["body"] for d in drained], [literal])

    def test_interim_and_delta_same_text_sent_once_per_segment(self) -> None:
        text = batch(({}, "A"), ({}, "B"))
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.interim", "text": text, "seq": 0},
            {"event": "message.delta", "delta": text, "seq": 1},
            {"event": "tool.started", "tool": "todo_list", "seq": 2},
        ])
        self.assertEqual([d["body"] for d in drained], ["A", "B"])

    def test_interim_prefix_then_delta_adds_only_new_unit(self) -> None:
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.interim", "text": "A", "seq": 0},
            {"event": "message.delta", "delta": batch(({}, "A"), ({}, "B")), "seq": 1},
            {"event": "tool.started", "tool": "todo_list", "seq": 2},
        ])
        self.assertEqual([d["body"] for d in drained], ["A", "B"])

    def test_same_text_across_segments_sends_twice(self) -> None:
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.delta", "delta": "同一句", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
            {"event": "tool.completed", "tool": "todo_list", "seq": 2},
            {"event": "message.delta", "delta": "同一句", "seq": 3},
            {"event": "tool.started", "tool": "todo_list", "seq": 4},
        ])
        self.assertEqual([d["body"] for d in drained], ["同一句", "同一句"])

    def test_control_tool_frame_seals_segment_without_new_batch(self) -> None:
        parser = RunsEventParser()
        drained: list[dict] = []
        events = [
            {"event": "message.delta", "delta": "控制前的正文", "seq": 0},
            {"event": "tool.started", "tool": "mcp__hermes__submit_control", "seq": 1},
        ]
        for line in sse_lines(events):
            parser.feed(line)
            drained.extend(parser.drain_stream_decisions())
        # 封段点在 tool.started：控制前的正文立刻发出，控制只配给之后的正文。
        self.assertEqual([d["body"] for d in drained], ["控制前的正文"])
        self.assertEqual(parser.decision_batch, 0)

    def test_work_tool_frame_bumps_batch(self) -> None:
        parser = RunsEventParser()
        feed_all(parser, [
            {"event": "message.delta", "delta": "进度", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ])
        self.assertEqual(parser.decision_batch, 1)

    def test_terminal_ignores_unsealed_delta_tail(self) -> None:
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.delta", "delta": "未封口的尾巴", "seq": 0},
            {"event": "run.completed", "output": "真正的终态正文", "seq": 1},
        ])
        # 终态只认 output：delta 尾巴丢弃，不补发、不猜。
        self.assertEqual(drained, [])
        self.assertEqual(parser.output, "真正的终态正文")

    def test_legacy_json_output_is_plain_text(self) -> None:
        legacy = '{"reply_to":"m:1"}\n{"body":"旧格式正文"}'
        units = runs_text.units_from_text(legacy)
        self.assertEqual([u["body"] for u in units], [legacy])
        self.assertEqual([u["reply_to"] for u in units], [""])

    def test_overlong_unit_fails_closed_without_emitting(self) -> None:
        parser = RunsEventParser()
        drained = feed_all(parser, [
            {"event": "message.delta", "delta": "长" * (runs_text.MAX_BODY_CHARS + 1), "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ])
        self.assertEqual(drained, [])
        self.assertEqual(parser.text_error, "body_overflow")

    def test_feed_never_returns_narration(self) -> None:
        parser = RunsEventParser()
        for line in sse_lines([
            {"event": "message.interim", "text": "旁白", "seq": 0},
            {"event": "message.delta", "delta": "正文", "seq": 1},
            {"event": "tool.started", "tool": "todo_list", "seq": 2},
        ]):
            self.assertIsNone(parser.feed(line))


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
    """真实 client 的终态分支：正文即消息、零额外调用、旧 JSON 不识别。"""

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(self.tracker.close)
        self.seen: list[str] = []

    async def _chat(self, events: list[dict], run_id: str = "run-e2-h") -> ChatResult:
        chat = real_chat(self.tracker, events, run_id)

        async def deliver(decision, rid):
            self.seen.append(decision["body"])

        return await chat(
            text="hi", session_key="s", user_id="u1", group_id="g1",
            adapter_name="onebotv11", is_private=False, mode="reactive",
            expect_structured=True, structured_tool_name="submit_decision",
            system_prompt="SYS", decision_callback=deliver,
        )

    async def test_single_sentence_zero_extra_calls(self) -> None:
        result = await self._chat([
            {"event": "message.delta", "delta": "你好", "seq": 0},
            {"event": "run.completed", "output": "你好", "seq": 1},
        ])
        # 终态唯一一份、逐字相等；delta 不额外产出一条。
        self.assertEqual(self.seen, ["你好"])
        self.assertEqual([d["body"] for d in result.terminal_decisions], ["你好"])
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.run_id, "run-e2-h")

    async def test_three_units_from_terminal_in_order(self) -> None:
        output = batch(({}, "甲"), ({}, "乙"), ({}, "丙"))
        result = await self._chat([
            {"event": "run.completed", "output": output, "seq": 0},
        ])
        self.assertEqual(self.seen, ["甲", "乙", "丙"])
        self.assertEqual([d["body"] for d in result.terminal_decisions], ["甲", "乙", "丙"])

    async def test_legacy_json_sent_exactly_once_verbatim(self) -> None:
        from nonebot_plugin_hermes_adapter.core import control_intent

        legacy = '{"reply_to":"m:1"}\n{"body":"旧格式正文"}'
        result = await self._chat([
            {"event": "message.delta", "delta": legacy, "seq": 0},
            {"event": "run.completed", "output": legacy, "seq": 1},
        ])
        self.assertEqual(self.seen, [legacy])
        self.assertEqual(result.terminal_decisions[0]["body"], legacy)
        # 不识别控制语义：reply_to 仍由 submit_control 决定，这里是缺省。
        self.assertEqual(
            control_intent.resolve_control(result.terminal_decisions[0])["reply_to"], ""
        )

    async def test_silent_output_sends_nothing(self) -> None:
        result = await self._chat([
            {"event": "run.completed", "output": "", "seq": 0},
        ])
        self.assertEqual(self.seen, [])
        self.assertEqual(result.terminal_decisions, [])
        self.assertEqual(result.structured, {"should_reply": False})

    async def test_interim_progress_then_final(self) -> None:
        result = await self._chat([
            {"event": "message.interim", "text": "查一下", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
            {"event": "tool.completed", "tool": "todo_list", "seq": 2},
            {"event": "run.completed", "output": "查到了", "seq": 3},
        ])
        self.assertEqual(self.seen, ["查一下", "查到了"])
        self.assertEqual([d["body"] for d in result.terminal_decisions], ["查到了"])

    async def test_control_frame_seals_segment_before_registration(self) -> None:
        result = await self._chat([
            {"event": "message.delta", "delta": "控制前正文", "seq": 0},
            {"event": "tool.started", "tool": "submit_control", "seq": 1},
            {"event": "tool.completed", "tool": "submit_control", "seq": 2},
            {"event": "message.delta", "delta": "控制后正文", "seq": 3},
            {"event": "run.completed", "output": "控制后正文", "seq": 4},
        ])
        # 控制前正文在 tool.started 就已封段发出，控制只会配给「控制后正文」。
        self.assertEqual(self.seen, ["控制前正文", "控制后正文"])
        self.assertEqual([d["body"] for d in result.terminal_decisions], ["控制后正文"])

    async def test_terminal_repeating_last_sealed_segment_not_resent(self) -> None:
        result = await self._chat([
            {"event": "message.delta", "delta": "正文", "seq": 0},
            {"event": "tool.started", "tool": "submit_control", "seq": 1},
            {"event": "tool.completed", "tool": "submit_control", "seq": 2},
            {"event": "run.completed", "output": "正文", "seq": 3},
        ])
        # 控制帧封段后没有新文本：output 只是同一段被终态重复带来，不再追加一份。
        self.assertEqual(self.seen, ["正文"])

    async def test_overflow_terminal_fails_closed(self) -> None:
        chat = real_chat(self.tracker, [
            {"event": "run.completed", "output": "长" * (runs_text.MAX_BODY_CHARS + 1), "seq": 0},
        ])

        async def deliver(decision, rid):
            self.seen.append(decision["body"])

        result = await chat(
            text="hi", session_key="s", user_id="u1", group_id="g1",
            adapter_name="onebotv11", is_private=False, mode="reactive",
            expect_structured=True, structured_tool_name="submit_decision",
            system_prompt="SYS", decision_callback=deliver,
        )
        self.assertEqual(self.seen, [])
        self.assertTrue(result.parse_failed)
        self.assertEqual(result.parse_failed_reason, "text_body_overflow")

    async def test_stream_break_sends_no_half_content(self) -> None:
        """断流：未封口的 delta 尾部不发送，run 留给恢复链，不补猜内容。"""
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter import mcp as mcp_module

        class _Resp(_ClientChain._Resp):
            async def aiter_lines(self):
                yield "event: message.delta"
                yield 'data: {"event":"message.delta","delta":"半截话","seq":0}'
                raise RuntimeError("stream reset")

        class _Broken(_ClientChain):
            def stream(self, method, url, **kwargs):
                self.streams.append(url)
                return _ClientChain._Stream(_Resp(200))

        fake = _Broken("run-break", [])
        public_args = inspect.signature(hc.hermes_client.chat).parameters

        async def deliver(decision, rid):
            self.seen.append(decision["body"])

        kwargs = {
            "text": "hi", "session_key": "s", "user_id": "u1", "group_id": "g1",
            "adapter_name": "onebotv11", "is_private": False, "mode": "reactive",
            "expect_structured": True, "structured_tool_name": "submit_decision",
            "system_prompt": "SYS", "decision_callback": deliver,
        }
        params = {key: value for key, value in kwargs.items() if key in public_args}
        params["target"] = SimpleNamespace(
            base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t",
        )
        params["bot_self_id"] = "999"
        with patch.object(hc, "_get_http_client", return_value=fake), \
                patch.object(mcp_module, "run_tracker", self.tracker):
            result = await hc.hermes_client.chat(**params)
        self.assertEqual(self.seen, [])                     # 半截话不发送
        self.assertTrue(result.parse_failed)
        self.assertTrue(getattr(result, "is_run_detached", False))
        self.assertEqual(result.run_id, "run-break")


class _StreamingChat:
    """把真实 SSE 帧喂进真实 parser + 真实 handler 即时发送器，再返回 ChatResult。

    与生产消费循环同序：每个单元先 ``prepare``（绑定控制／落计划）再 ``deliver``；
    终态单元也走同一条队列（生产 client 在 run.completed 时把它们 put 进队列）。
    """

    def __init__(self, tracker, events: list[dict], run_id: str = "run-e2-h") -> None:
        self.tracker = tracker
        self.events = events
        self.run_id = run_id
        self.sent_at: list[str] = []
        self.streamed: list[dict] = []
        self.callback = None

    async def _emit(self, decision: dict) -> None:
        prepare = getattr(self.callback, "prepare", None)
        if callable(prepare):
            prepare(decision, self.run_id)
        if self.callback is not None:
            await self.callback(decision, self.run_id)
        self.streamed.append(decision)
        self.sent_at.append(decision["body"])

    async def __call__(self, *args, **kwargs):
        self.callback = kwargs.get("decision_callback")
        parser = RunsEventParser()
        for line in sse_lines(self.events):
            parser.feed(line)
            for decision in parser.drain_stream_decisions():
                await self._emit(decision)
        output = parser.output if parser.output is not None else ""
        if parser.terminal:
            self.tracker.record_expected_delivery(
                run_id=self.run_id, output=output, expect_structured=True
            )
        terminal = runs_text.decisions_from_text(output) if output.strip() else []
        if not parser.final_segment_pending and parser.last_sealed:
            terminal = [item for item in terminal if item["body"] not in parser.last_sealed]
        terminal_only = not self.streamed
        for decision in terminal:
            if terminal_only:
                decision["_runs_terminal_only"] = True
            await self._emit(decision)
        result = ChatResult(
            raw_text=output,
            structured=terminal[0] if terminal else {"should_reply": False},
        )
        result.terminal_decisions = terminal
        result.streamed_decisions = list(self.streamed)
        result.run_id = self.run_id
        return result


class ReactiveLiveChainTests(_e2._ReactiveHarness):
    """真实 handler + 真实台账 + 真实 parser 的即时发送链。"""

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    async def _run_stream(self, events: list[dict], *, output: str):
        tracker = self._tracker(record_delivery=False)
        chat = _StreamingChat(tracker, events + [
            {"event": "run.completed", "output": output, "seq": 999},
        ])
        calls = await self._run(None, tracker=tracker, chat=chat)
        return tracker, chat, calls

    def _register(self, control: dict, key=("onebotv11", "group:g1")) -> None:
        from nonebot_plugin_hermes_adapter.core import control_intent

        self.addCleanup(control_intent.CONTROL_QUEUE.discard, key)
        control_intent.CONTROL_QUEUE.register(key, control)

    async def test_progress_then_final_sent_once_each(self) -> None:
        events = [
            {"event": "message.delta", "delta": "先查一下", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
            {"event": "tool.completed", "tool": "todo_list", "seq": 2},
        ]
        tracker, chat, calls = await self._run_stream(events, output="查到了")
        self.assertEqual([c["text"] for c in calls["send"]], ["先查一下", "查到了"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:0", "primary:1"])
        self.assertEqual(chat.sent_at, ["先查一下", "查到了"])
        plan = delivery_plan_units(tracker.get_delivery("run-e2-h"))
        self.assertEqual([u["cleaned_text"] for u in plan], ["先查一下", "查到了"])
        self.assertEqual(calls["settle"], [])

    async def test_same_text_in_two_segments_sends_both(self) -> None:
        events = [
            {"event": "message.delta", "delta": "同一句", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
            {"event": "tool.completed", "tool": "todo_list", "seq": 2},
            {"event": "message.delta", "delta": "同一句", "seq": 3},
        ]
        tracker, chat, calls = await self._run_stream(events, output="同一句")
        self.assertEqual([c["text"] for c in calls["send"]], ["同一句", "同一句"])
        self.assertEqual(chat.sent_at, ["同一句", "同一句"])

    async def test_silent_turn_sends_nothing(self) -> None:
        tracker, chat, calls = await self._run_stream([], output="")
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("silent", "empty_output")])
        self.assertEqual(calls["control"]["end_window"], 0)

    async def test_reply_to_control_bound_to_first_unit(self) -> None:
        from nonebot_plugin_hermes_adapter.core import control_intent

        self._register({"reply_to": "m:9"})
        events = [
            {"event": "message.delta", "delta": batch(({}, "第一条"), ({}, "第二条")), "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ]
        tracker, chat, calls = await self._run_stream(events, output="")
        self.assertEqual([c["text"] for c in calls["send"]], ["第一条", "第二条"])
        # 一份控制只配一份正文：第二份回到缺省。
        self.assertEqual(control_intent.resolve_control(chat.streamed[0])["reply_to"], "m:9")
        self.assertEqual(control_intent.resolve_control(chat.streamed[1])["reply_to"], "")
        # 台账计划只由 output 派生：工具登记的 reply_to 不进 plan（既有边界，
        # 保证恢复路径重复调用得到逐字相同的 plan_json）。
        plan = delivery_plan_units(tracker.get_delivery("run-e2-h"))
        self.assertEqual([u["reply_to"] for u in plan], ["", ""])

    async def test_exit_control_closes_window_after_group(self) -> None:
        self._register({"should_exit_active": True})
        events = [
            {"event": "message.delta", "delta": batch(({}, "说完一"), ({}, "说完二"))}
            , {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ]
        tracker, chat, calls = await self._run_stream(events, output="")
        self.assertEqual([c["text"] for c in calls["send"]], ["说完一", "说完二"])
        self.assertEqual(calls["control"]["end_window"], 1)


class ResumeConsistencyTests(unittest.IsolatedAsyncioTestCase):
    """恢复链与正常链共用同一份切分：同一 output → 逐字相同计划，已发份不重发。"""

    def test_build_units_matches_plan(self) -> None:
        from tests.test_runs_terminal import _resume_build_units

        output = batch(({}, "第一份"), ({}, "第二份"))
        units = _resume_build_units()(output, True)
        self.assertEqual([u["body"] for u in units], ["第一份", "第二份"])
        plan, _ = build_batch_plan(units)
        self.assertEqual([u["cleaned_text"] for u in plan["units"]], ["第一份", "第二份"])

    def test_sent_component_not_reset(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "t.db")
        self.addCleanup(tracker.close)
        tracker.record_submission(
            idempotency_key="k", request_body={"input": "hi"},
            target_fingerprint="onebot11|x", scope_key="qq:g:1",
            session_key="s", mode="reactive", expect_structured=True, bot_self_id="999",
        )
        tracker.bind_submission_and_register(
            idempotency_key="k", run_id="r1", scope_key="qq:g:1",
            adapter="onebotv11", is_private=False, user_id="u1", group_id="g1",
            bot_self_id="999", session_key="s", mode="reactive", expect_structured=True,
        )
        tracker.record_expected_delivery(run_id="r1", output="A", expect_structured=True)
        stream_unit = units_from_decisions(decisions("A"))[0][1]
        append_stream_unit(tracker=tracker, run_id="r1", unit=stream_unit)
        tracker.mark_component_sent(run_id="r1", component_id="primary:0", msg_ids=["m1"])
        prepare_terminal_batch(tracker=tracker, run_id="r1", units=[stream_unit])
        self.assertEqual(
            tracker.get_component(run_id="r1", component_id="primary:0")["state"], "sent"
        )
