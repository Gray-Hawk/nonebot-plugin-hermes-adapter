"""completions 纯文本正文回归：正文即消息、§ 连发、静默、媒体提取、提示词口径。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402


class CompletionsTextSeamTests(unittest.TestCase):
    """终态正文切分与静默口径（Runs 与 completions 共用同一份分节符信封）。"""

    @staticmethod
    def _unit(body: str) -> str:
        return f"§\n{body}\n§"

    def test_single_sentence_is_one_unit(self) -> None:
        from nonebot_plugin_agent_adapter.core.runs_text import decisions_from_text

        decisions = decisions_from_text(self._unit("正文"))
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["body"], "正文")
        self.assertNotIn("reply_text", decisions[0])
        self.assertNotIn("reply_to", decisions[0])
        self.assertTrue(decisions[0]["should_reply"])

    def test_empty_text_maps_to_silent(self) -> None:
        from nonebot_plugin_agent_adapter.core.runs_text import decisions_from_text

        for text in ("", "   ", "\n\n", "§\n§"):
            self.assertEqual(decisions_from_text(text), [])

    def test_separator_makes_multiple_units(self) -> None:
        from nonebot_plugin_agent_adapter.core.runs_text import decisions_from_text

        decisions = decisions_from_text("§\n甲\n§\n乙\n§\n丙\n§")
        self.assertEqual([d["body"] for d in decisions], ["甲", "乙", "丙"])

    def test_bare_text_is_not_a_body(self) -> None:
        from nonebot_plugin_agent_adapter.core.runs_text import decisions_from_text

        # 旧裸正文不保留兼容层：没有分节符就没有正文。
        self.assertEqual(decisions_from_text("甲\n§\n乙"), [])
        self.assertEqual(decisions_from_text("直接一句话"), [])

    def test_over_limit_fails_closed(self) -> None:
        from nonebot_plugin_agent_adapter.core.runs_text import (
            MAX_BODY_CHARS,
            TextUnitsError,
            decisions_from_text,
        )

        with self.assertRaises(TextUnitsError):
            decisions_from_text(self._unit("长" * (MAX_BODY_CHARS + 1)))

    def test_media_extraction_unchanged(self) -> None:
        from nonebot_plugin_agent_adapter.core.hermes_client import extract_response_media

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
        from nonebot_plugin_agent_adapter.handlers import message_turns

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


class ChatTextUnitTests(unittest.IsolatedAsyncioTestCase):
    """真实 chat() completions 分支：正文就是最终答复，不再要求 JSON。"""

    async def _chat_with(self, content: str):
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from nonebot_plugin_agent_adapter.config import plugin_config
        from nonebot_plugin_agent_adapter.core import hermes_client as hc

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

    async def test_plain_sentence_is_one_decision(self) -> None:
        result = await self._chat_with("§\n你好\n§")
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], "你好")
        self.assertEqual([d["body"] for d in result.terminal_decisions], ["你好"])

    async def test_separator_splits_final_answer(self) -> None:
        result = await self._chat_with("§\n甲\n§\n乙\n§")
        self.assertEqual([d["body"] for d in result.terminal_decisions], ["甲", "乙"])

    async def test_legacy_json_is_not_a_body(self) -> None:
        legacy = '{"body":"旧格式"}'
        result = await self._chat_with(legacy)
        # 旧裸正文不再兼容：零发送、零决策，也不是解析失败（只是没有可发的正文）。
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.terminal_decisions, [])
        self.assertFalse(result.structured["should_reply"])

    async def test_empty_content_is_silent(self) -> None:
        result = await self._chat_with("")
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.terminal_decisions, [])
        self.assertFalse(result.structured["should_reply"])

    async def test_markdown_and_cq_kept_verbatim(self) -> None:
        text = "看这里 [文档](https://example.com/a) 和 [CQ:at,qq=123]"
        result = await self._chat_with(f"§\n{text}\n§")
        self.assertEqual(result.structured["body"], text)

    async def test_quotes_and_tail_residue_kept_verbatim(self) -> None:
        text = '"body":"SECRET"}'
        result = await self._chat_with(f"§\n{text}\n§")
        self.assertEqual(result.structured["body"], text)


class CompletionsPromptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from nonebot_plugin_agent_adapter.handlers import message_turns
        from nonebot_plugin_agent_adapter.core import prompt_builder
        from nonebot_plugin_agent_adapter.config import plugin_config

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

    def test_decision_body_reads_body_only(self) -> None:
        self.assertEqual(self.mt._decision_body({"body": "正文"}), "正文")
        self.assertEqual(self.mt._decision_body({"reply_text": "旧正文"}), "")

    def test_completions_prompt_is_plain_text(self) -> None:
        prompt = self._as_interface("completions", lambda: self.pb.build_reactive_system_prompt())
        self.assertIn("整行只有一个 §", prompt)
        self.assertIn("request_continuation", prompt)
        self.assertIn("push_message", prompt)
        self.assertIn("插话原则（决定是否发言；决定发言时正文必须非空）：", prompt)
        for legacy in ("每份决策固定两行", "submit_decision", "should_reply", "reply_text", "not_final"):
            self.assertNotIn(legacy, prompt)
        # 纯文本口径：插话原则不写布尔字段值，回复纪律不留双空格，退窗不教输出字段。
        for residue in ("→ true", "→ false", "决定 正文 范围", "should_exit_active=false"):
            self.assertNotIn(residue, prompt)
        self.assertIn("→ 输出正文", prompt)

    def test_runs_prompt_is_plain_text(self) -> None:
        prompt = self._as_interface("runs", lambda: self.pb.build_reactive_system_prompt())
        self.assertIn("整行只有一个 §", prompt)
        self.assertIn("submit_control", prompt)
        for legacy in ("每份决策就是一行 JSON", "submit_decision", "should_reply", "reply_text", "not_final"):
            self.assertNotIn(legacy, prompt)
        self.assertNotIn("background_task 便签", prompt)
        # Runs 没有 push_message 工具：正文/进度都走同一条正文通道。
        self.assertNotIn("push_message", prompt)
        for residue in ("→ true", "→ false", "决定 正文 范围", "should_exit_active=false"):
            self.assertNotIn(residue, prompt)
        self.assertIn("→ 输出正文", prompt)

    def test_runs_private_prompt_has_no_push_message(self) -> None:
        block = self.pb._JSON_LINES_PRIVATE_BLOCK_RUNS
        self.assertNotIn("push_message", block)
        self.assertIn("submit_control", block)

    def test_sticker_guidance_follows_interface(self) -> None:
        from nonebot_plugin_agent_adapter.core import sticker_library

        old = sticker_library._STICKER_FEATURE_ENABLED
        sticker_library._STICKER_FEATURE_ENABLED = True
        try:
            comp = self._as_interface("completions", lambda: self.pb.build_reactive_system_prompt())
            runs = self._as_interface("runs", lambda: self.pb.build_reactive_system_prompt())
        finally:
            sticker_library._STICKER_FEATURE_ENABLED = old
        self.assertIn("正文仅写", comp)
        self.assertIn("正文仅写", runs)
        self.assertNotIn("reply_text 仅写", runs)


class MidTurnAndMediaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import nonebot
            nonebot.init()
        except Exception:
            pass
        from nonebot_plugin_agent_adapter.handlers import message_turns

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


if __name__ == "__main__":
    unittest.main()
