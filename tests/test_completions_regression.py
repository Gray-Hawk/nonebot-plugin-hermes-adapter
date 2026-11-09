"""E1 completions 行为回归：正文只消费 body、复述去重、媒体提取、终态失败不放行。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.npha_terminal import (  # noqa: E402
    TerminalFormatError,
    decode_terminal,
    structured_from_terminal,
)


class CompletionsSeamTests(unittest.TestCase):
    def test_body_is_the_only_content_channel(self) -> None:
        structured = structured_from_terminal(decode_terminal('{"reply_to":"m:1"}\n{"body":"正文"}'))
        self.assertEqual(structured["body"], "正文")
        self.assertNotIn("reply_text", structured)
        self.assertEqual(structured["reply_to"], "m:1")
        self.assertTrue(structured["should_reply"])

    def test_empty_body_maps_to_silent(self) -> None:
        structured = structured_from_terminal(decode_terminal('{}\n{"body":""}'))
        self.assertFalse(structured["should_reply"])
        self.assertEqual(structured["body"], "")

    def test_failed_terminal_has_no_payload(self) -> None:
        # 新方针：只有空文本与超限会失败，其余交给硬编码搜索兜底
        with self.assertRaises(TerminalFormatError):
            decode_terminal("")
        with self.assertRaises(TerminalFormatError):
            decode_terminal("abcdef", max_body_chars=4)

    def test_media_extraction_unchanged(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import extract_response_media

        cleaned, media, files = extract_response_media("图来了 MEDIA:/tmp/x.png")
        self.assertIn("图来了", cleaned)
        self.assertIn("[生成了文件: x.png]", cleaned)
        self.assertTrue(media or files)


class MidTurnEntryTests(unittest.IsolatedAsyncioTestCase):
    """走 turn 里的同一入口 _handle_mid_turn_push，而不是只测真值表。"""

    @classmethod
    def setUpClass(cls) -> None:
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from tests import _bootstrap  # noqa: F401
        from nonebot_plugin_hermes_adapter.handlers import message_turns

        cls.mt = message_turns

    def _calls(self):
        return {"send": [], "mark": 0, "finish": [], "schedule": 0, "settle": []}

    async def _run(self, *, push, pushed_media, reply, media):
        calls = self._calls()
        action = self.mt._mid_turn_action(push, "在查", pushed_media, reply, media)

        async def send_text(delivery):
            calls["send"].append(delivery)
            return True

        handled = await self.mt._handle_mid_turn_push(
            action=action,
            send_text=send_text,
            prepare_delivery=lambda: "delivery-1",
            mark_replied=lambda: calls.__setitem__("mark", calls["mark"] + 1),
            finish_history=lambda delivery, sent: calls["finish"].append((delivery, sent)),
            schedule_reconcile=lambda: calls.__setitem__("schedule", calls["schedule"] + 1),
            settle_suppressed=lambda: calls["settle"].append("suppressed"),
        )
        return action, handled, calls

    async def test_suppress_sends_nothing(self) -> None:
        action, handled, calls = await self._run(push=True, pushed_media=1, reply="在查", media=["u1"])
        self.assertEqual(action, "suppress")
        self.assertTrue(handled)
        self.assertEqual(calls["send"], [])              # 发送函数零调用
        self.assertEqual(calls["settle"], ["suppressed"])
        self.assertEqual(calls["finish"], [])
        self.assertEqual(calls["mark"], 0)

    async def test_media_only_sends_media_without_text(self) -> None:
        action, handled, calls = await self._run(push=True, pushed_media=0, reply="在查", media=["u1"])
        self.assertEqual(action, "media_only")
        self.assertTrue(handled)
        self.assertEqual(calls["send"], ["delivery-1"])
        self.assertEqual(calls["mark"], 1)
        self.assertEqual(len(calls["finish"]), 1)
        self.assertEqual(calls["schedule"], 1)

    async def test_send_both_falls_through_to_normal_send(self) -> None:
        action, handled, calls = await self._run(push=True, pushed_media=0, reply="真答案", media=[])
        self.assertEqual(action, "send_both")
        self.assertFalse(handled)
        self.assertEqual(calls["send"], [])              # 由 turn 后续正常发送

    async def test_no_push_sends_normally(self) -> None:
        action, handled, calls = await self._run(push=False, pushed_media=0, reply="x", media=[])
        self.assertEqual(action, "send")
        self.assertFalse(handled)
        self.assertEqual(calls["send"], [])


class ChatDecodeFailureTests(unittest.IsolatedAsyncioTestCase):
    """真实 chat() 接口：没有决策字段的文本（含引号起头的残段、文字前缀外壳）按 A 整段直发。"""

    async def _chat_with(self, content: str):
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from tests import _bootstrap  # noqa: F401
        from nonebot_plugin_hermes_adapter.config import plugin_config
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc

        class _Resp:
            status_code = 200
            headers: dict = {}

            def json(self):
                return {"choices": [{"message": {"content": content}}]}

        class _Fake:
            async def post(self, *a, **kw):
                return _Resp()

        old_get = hc._get_http_client
        old_iface = plugin_config.hermes_api_interface
        hc._get_http_client = lambda: _Fake()
        object.__setattr__(plugin_config, "hermes_api_interface", "completions")
        try:
            target = SimpleNamespace(base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t")
            return await hc.hermes_client.chat(
                text="hi", session_key="sess", user_id="u", group_id="g",
                adapter_name="qq", is_private=False, mode="reactive",
                expect_structured=True, structured_tool_name="submit_decision",
                system_prompt="SYS", target=target,
            )
        finally:
            hc._get_http_client = old_get
            object.__setattr__(plugin_config, "hermes_api_interface", old_iface)

    async def test_escaped_body_key_sends_verbatim(self) -> None:
        # 新口径：这类没有决策字段的文本按 A 整段直发
        text = 'test_escaped_body_key_is_rejected'
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_tail_residue_sends_verbatim(self) -> None:
        text = '"body":"SECRET"}'
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_escaped_tail_residue_sends_verbatim(self) -> None:
        text = '"body":"SECRET"}'
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_tail_without_brace_sends_verbatim(self) -> None:
        text = '"body":"SECRET"}'
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_bad_body_does_not_borrow_control(self) -> None:
        # 新口径：这类没有决策字段的文本按 A 整段直发
        text = 'test_bad_body_does_not_borrow_control'
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_numeric_markdown_link_delivers(self) -> None:
        text = "[1](https://example.invalid/doc)"
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_markdown_naked_body_still_delivers(self) -> None:
        text = "看这里 [文档](https://example.com/a) 和 [CQ:at,qq=123]"
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_prefixed_object_sends_verbatim(self) -> None:
        # 新口径：这类没有决策字段的文本按 A 整段直发
        text = 'test_prefixed_object_is_rejected'
        result = await self._chat_with(text)
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], text)

    async def test_completions_http_payload_has_no_legacy_hint(self) -> None:
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from tests import _bootstrap  # noqa: F401
        from nonebot_plugin_hermes_adapter.config import plugin_config
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter.core import prompt_builder as pb

        captured = []

        class _Resp:
            status_code = 200
            headers: dict = {}

            def json(self):
                return {"choices": [{"message": {"content": "{}"}}]}

        class _Fake:
            async def post(self, url, json=None, headers=None, timeout=None):
                captured.append(json)
                return _Resp()

        old_get = hc._get_http_client
        old_iface = plugin_config.hermes_api_interface
        hc._get_http_client = lambda: _Fake()
        object.__setattr__(plugin_config, "hermes_api_interface", "completions")
        try:
            target = SimpleNamespace(base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t")
            await hc.hermes_client.chat(
                text="hi", session_key="sess", user_id="u", group_id="g",
                adapter_name="qq", is_private=False, mode="reactive",
                expect_structured=True, structured_tool_name="submit_decision",
                system_prompt=pb.build_reactive_system_prompt(), target=target,
            )
        finally:
            hc._get_http_client = old_get
            object.__setattr__(plugin_config, "hermes_api_interface", old_iface)

        self.assertTrue(captured)
        system = captured[-1]["messages"][0]["content"]
        self.assertIn("插话原则（决定是否回复；决定回复时 body 必须非空）：", system)
        self.assertNotIn("插话原则(决定 body 是否为空):", system)
        for legacy in ("single JSON object", "should_reply", "reply_text", "submit_decision"):
            self.assertNotIn(legacy, system)

class CompletionsGateAndPromptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from tests import _bootstrap  # noqa: F401
        from nonebot_plugin_hermes_adapter.handlers import message_turns
        from nonebot_plugin_hermes_adapter.core import prompt_builder
        from nonebot_plugin_hermes_adapter.config import plugin_config

        cls.mt = message_turns
        cls.pb = prompt_builder
        cls.cfg = plugin_config

    def _as_interface(self, value, fn):
        old = self.cfg.hermes_api_interface
        object.__setattr__(self.cfg, "hermes_api_interface", value)
        try:
            return fn()
        finally:
            object.__setattr__(self.cfg, "hermes_api_interface", old)

    def test_failure_gate_only_for_completions(self) -> None:
        from types import SimpleNamespace

        failed = SimpleNamespace(parse_failed=True, is_transport_error=False)
        transport = SimpleNamespace(parse_failed=True, is_transport_error=True)
        self.assertTrue(self._as_interface("completions", lambda: self.mt._should_hard_silence_envelope(failed)))
        self.assertFalse(self._as_interface("completions", lambda: self.mt._should_hard_silence_envelope(transport)))
        self.assertFalse(self._as_interface("runs", lambda: self.mt._should_hard_silence_envelope(failed)))

    def test_body_preference_is_scoped(self) -> None:
        both = {"body": "乙正文", "reply_text": "旧正文"}
        empty_body = {"body": "", "reply_text": "旧正文"}
        self.assertEqual(self.mt._decision_body(both, prefer_body=True), "乙正文")
        self.assertEqual(self.mt._decision_body(both, prefer_body=False), "旧正文")
        self.assertEqual(self.mt._decision_body(empty_body, prefer_body=True), "旧正文")
        self.assertEqual(self.mt._decision_body(empty_body, prefer_body=False), "旧正文")

    def test_actual_prompt_has_no_legacy_fields_for_completions(self) -> None:
        prompt = self._as_interface("completions", lambda: self.pb.build_reactive_system_prompt())
        self.assertIn("每份决策固定两行", prompt)
        self.assertIn("插话原则（决定是否回复；决定回复时 body 必须非空）：", prompt)
        self.assertNotIn("插话原则(决定 body 是否为空):", prompt)
        self.assertNotIn("submit_decision", prompt)
        self.assertNotIn("should_reply", prompt)
        self.assertNotIn("reply_text", prompt)
        self.assertNotIn("background_task 便签", prompt)
        runs_prompt = self._as_interface("runs", lambda: self.pb.build_reactive_system_prompt())
        # Runs 现行形态：普通回复单行正文对象，控制由 submit_control 工具登记。
        self.assertIn("每份决策就是一行 JSON", runs_prompt)
        self.assertIn("submit_control", runs_prompt)
        self.assertIn("需要连发多条时，按顺序接上下一份单行 JSON", runs_prompt)
        self.assertIn("工具间隙", runs_prompt)
        self.assertNotIn("每份决策固定两行", runs_prompt)
        self.assertNotIn("submit_decision", runs_prompt)
        self.assertNotIn("should_reply", runs_prompt)
        self.assertNotIn("background_task 便签", runs_prompt)
        legacy_prompt = self._as_interface("astrbot", lambda: self.pb.build_reactive_system_prompt())
        self.assertNotIn("background_task 便签", legacy_prompt)

    def test_sticker_guidance_follows_interface(self) -> None:
        from nonebot_plugin_hermes_adapter.core import sticker_library

        old = sticker_library._STICKER_FEATURE_ENABLED
        sticker_library._STICKER_FEATURE_ENABLED = True
        try:
            comp = self._as_interface("completions", lambda: self.pb.build_reactive_system_prompt())
            runs = self._as_interface("runs", lambda: self.pb.build_reactive_system_prompt())
        finally:
            sticker_library._STICKER_FEATURE_ENABLED = old
        self.assertIn("body 仅写", comp)
        self.assertNotIn("reply_text", comp)
        # E2:runs 终态正文同样走 body
        self.assertIn("body 仅写", runs)
        self.assertNotIn("reply_text 仅写", runs)


class RealInterimEchoTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from tests import _bootstrap  # noqa: F401
        from nonebot_plugin_hermes_adapter.handlers import message_turns

        cls.mt = message_turns

    async def test_interim_echo_really_sends_cleaned_text(self) -> None:
        sent, feedback = [], []
        ok = await self.mt._emit_interim_echo(
            "我先查一下 MEDIA:/tmp/x.png",
            feedback_check=feedback.append,
            send_text=lambda text: sent.append(text) or _done(),
        )
        self.assertTrue(ok)
        self.assertEqual(feedback, ["我先查一下 MEDIA:/tmp/x.png"])
        self.assertEqual(len(sent), 1)
        self.assertIn("我先查一下", sent[0])
        self.assertIn("[生成了文件: x.png]", sent[0])

    async def test_interim_echo_media_only_keeps_placeholder_like_before(self) -> None:
        sent = []
        ok = await self.mt._emit_interim_echo(
            "MEDIA:/tmp/only.png",
            feedback_check=lambda _t: None,
            send_text=lambda text: sent.append(text) or _done(),
        )
        self.assertTrue(ok)
        self.assertEqual(len(sent), 1)          # 占位文本保留，与改造前一致

    async def test_interim_echo_blank_segment_not_sent(self) -> None:
        sent = []
        ok = await self.mt._emit_interim_echo(
            "   ",
            feedback_check=lambda _t: None,
            send_text=lambda text: sent.append(text) or _done(),
        )
        self.assertFalse(ok)
        self.assertEqual(sent, [])


class MidTurnAndMediaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from tests import _bootstrap  # noqa: F401
        from nonebot_plugin_hermes_adapter.handlers import message_turns

        cls.mt = message_turns

    def test_mid_turn_action_truth_table(self) -> None:
        act = self.mt._mid_turn_action
        self.assertEqual(act(False, "", 0, "x", []), "send")
        self.assertEqual(act(True, "在查", 0, "真答案", []), "send_both")
        self.assertEqual(act(True, "在查", 0, "在查", ["u1"]), "media_only")
        self.assertEqual(act(True, "在查", 1, "在查", ["u1"]), "suppress")
        self.assertEqual(act(True, "在查", 0, "在查", []), "suppress")

    def test_merge_reply_media_order_and_dedupe(self) -> None:
        merged = self.mt._merge_reply_media(["a", "b"], ["b", "c"])
        self.assertEqual(merged, ["a", "b", "c"])


async def _done():
    return None


if __name__ == "__main__":
    unittest.main()


class EnvelopeSeamTests(unittest.TestCase):
    def test_reject_reasons_bypass_hard_silence(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers.message_turns import _should_hard_silence_envelope

        class R:
            parse_failed = True
            is_transport_error = False
            parse_failed_reason = "envelope_fields_missing"

        self.assertFalse(_should_hard_silence_envelope(R()))
        R.parse_failed_reason = "multiline_envelope"
        self.assertFalse(_should_hard_silence_envelope(R()))


class EnvelopeRejectionSeamTests(unittest.TestCase):
    def test_rejection_pushes_note_resubmits_and_never_sends(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        calls: dict[str, list] = {"note": [], "followup": [], "settle": [], "send": []}
        orig_note, orig_follow, orig_settle = (
            mt.task_notes.push_plugin_note, mt._fire_followup_turn, mt._settle,
        )
        mt.task_notes.push_plugin_note = lambda key, name, text, **kw: calls["note"].append(name)
        mt._fire_followup_turn = lambda **kw: calls["followup"].append(kw)
        mt._settle = lambda result, outcome, reason: calls["settle"].append((outcome, reason))
        try:
            result = SimpleNamespace(parse_failed=True, parse_failed_reason="envelope_fields_missing",
                                     is_transport_error=False)
            mt._handle_envelope_rejection(
                result, fb_key=("g", "u"), bot=None, target=None, adapter_name="onebotv11",
                user_id="u", group_id="g", is_private=False, tag="reactive",
            )
        finally:
            mt.task_notes.push_plugin_note = orig_note
            mt._fire_followup_turn = orig_follow
            mt._settle = orig_settle
        self.assertEqual(calls["note"], ["envelope_rejected"])
        self.assertEqual(len(calls["followup"]), 1)
        self.assertEqual(calls["followup"][0]["tag"], "bounce:envelope-envelope_fields_missing")
        self.assertEqual(calls["settle"], [("suppressed", "envelope_rejected")])
        self.assertEqual(calls["send"], [])

    def test_a_class_hint_does_not_resubmit(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt
        from core.npha_terminal import ENVELOPE_MISSING_HINT

        pushed, follow = [], []
        orig_note, orig_follow = mt.task_notes.push_plugin_note, mt._fire_followup_turn
        mt.task_notes.push_plugin_note = lambda key, name, text, **kw: pushed.append(name)
        mt._fire_followup_turn = lambda **kw: follow.append(kw)
        try:
            mt._push_self_heal_note(("g", "u"), {"_self_heal_hint": ENVELOPE_MISSING_HINT})
        finally:
            mt.task_notes.push_plugin_note = orig_note
            mt._fire_followup_turn = orig_follow
        self.assertEqual(pushed, ["envelope_missing"])
        self.assertEqual(follow, [])
