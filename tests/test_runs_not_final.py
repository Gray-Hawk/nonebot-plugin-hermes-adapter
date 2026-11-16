"""Runs not_final 恢复：submit_control 布尔字段 → 下一份正文 decision → 既有 relay 续跑链。

f3c7fbc 退役旧 JSON 信封时删掉了 Runs 的 not_final。本文件锁定最小恢复口径：
- 只有 Runs 的 ``submit_control`` 接受 ``not_final``，登记进下一份正文的 decision；
- 发送后复用 ``decision.get("not_final")`` → ``_post_send_feedback`` → relay 的现有续跑链，
  恰好一次、链上限沿用；
- completions 继续只走 ``request_continuation``（本工具拒收 not_final）；
- 普通正文不受影响。
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tests import _bootstrap  # noqa: F401

from nonebot_plugin_hermes_adapter.core import control_intent
from nonebot_plugin_hermes_adapter.core.inflight import make_inflight_key
from nonebot_plugin_hermes_adapter.core.prompt_protocol import render_decision_protocol
from nonebot_plugin_hermes_adapter.core.routing import CallerScope
from nonebot_plugin_hermes_adapter.handlers import message_feedback as fb
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt
from nonebot_plugin_hermes_adapter.mcp.tools.control import (
    SubmitControlInput,
    submit_control_impl,
)

GROUP_KEY = make_inflight_key("onebotv11", False, "u1", "g1")
PRIVATE_KEY = make_inflight_key("onebotv11", True, "u1", None)


def _register(*, interface: str = "runs", **kwargs):
    """走真实工具入口登记一份控制，返回 (结果, 队列)。"""
    queue = control_intent.ControlQueue()
    inp = SubmitControlInput(adapter="onebotv11", group_id="g1", **kwargs)
    result = submit_control_impl(
        inp,
        scope=CallerScope.uniform(),
        interface=interface,
        queue=queue,
    )
    return result, queue


class ControlRegistrationTests(unittest.TestCase):
    def test_runs_registers_not_final_on_next_decision_only(self) -> None:
        result, queue = _register(not_final=True)
        self.assertTrue(result.ok, result.error)
        self.assertTrue(result.registered)
        self.assertIn("not_final", result.fields)

        first = {"body": "收到，我马上继续处理", "should_reply": True}
        self.assertIsNotNone(control_intent.bind_control(first, GROUP_KEY, queue=queue))
        self.assertIs(first.get("not_final"), True)

        # 第二份正文没有登记控制：不得继承上一份的 not_final。
        second = {"body": "办完了", "should_reply": True}
        control_intent.bind_control(second, GROUP_KEY, queue=queue)
        self.assertNotIn("not_final", second)

    def test_not_final_false_registers_nothing(self) -> None:
        result, queue = _register(not_final=False)
        self.assertTrue(result.ok, result.error)
        self.assertFalse(result.registered)
        self.assertEqual(queue.pending(GROUP_KEY), 0)

    def test_completions_rejects_not_final(self) -> None:
        result, queue = _register(interface="completions", not_final=True)
        self.assertFalse(result.ok)
        self.assertFalse(result.registered)
        self.assertEqual(queue.pending(GROUP_KEY), 0)
        self.assertIn("completions", result.error or "")

    def test_runs_keeps_existing_control_fields(self) -> None:
        # @回归：not_final 与既有字段同一份控制里不互相挤掉。
        result, queue = _register(not_final=True, reply_to="m:12", should_exit_active=True)
        self.assertTrue(result.ok, result.error)
        self.assertEqual(
            result.fields, ["not_final", "reply_to", "should_exit_active"]
        )
        decision = {"body": "这条引用并退窗", "should_reply": True}
        control_intent.bind_control(decision, GROUP_KEY, queue=queue)
        self.assertIs(decision.get("not_final"), True)
        effective = control_intent.resolve_control(decision)
        self.assertEqual(effective["reply_to"], "m:12")
        self.assertTrue(effective["should_exit_active"])


class _RelayHarness(unittest.TestCase):
    def setUp(self) -> None:
        self.followups: list[dict] = []
        relay._chain_reset(GROUP_KEY, "relay")
        patches = [
            patch.object(fb, "load_rules", MagicMock(return_value=[])),
            patch.object(
                fb, "_fire_followup_turn",
                MagicMock(side_effect=lambda **kw: self.followups.append(kw)),
            ),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(relay._chain_reset, GROUP_KEY, "relay")

    def _feedback(self, *, not_final: bool) -> None:
        asyncio.run(
            fb._post_send_feedback(
                key=GROUP_KEY,
                reply_text="收到，我马上继续处理",
                bot=SimpleNamespace(self_id="999"),
                target=SimpleNamespace(private=False, id="g1"),
                adapter_name="onebotv11",
                user_id="u1",
                group_id="g1",
                is_private=False,
                not_final=not_final,
            )
        )


class RelayChainTests(_RelayHarness):
    def test_not_final_fires_exactly_once_per_body(self) -> None:
        self._feedback(not_final=True)
        self.assertEqual([c["tag"] for c in self.followups], ["relay:not_final"])

    def test_plain_body_does_not_continue_and_resets_chain(self) -> None:
        self._feedback(not_final=False)
        self.assertEqual(self.followups, [])
        # 普通投递把连续接力链归零：随后 not_final 仍可点火。
        self._feedback(not_final=True)
        self.assertEqual([c["tag"] for c in self.followups], ["relay:not_final"])

    def test_chain_limit_is_reused(self) -> None:
        for _ in range(fb._RELAY_CHAIN_MAX + 1):
            self._feedback(not_final=True)
        self.assertEqual(len(self.followups), fb._RELAY_CHAIN_MAX)
        # 普通正文归零后重新可用。
        self._feedback(not_final=False)
        self._feedback(not_final=True)
        self.assertEqual(len(self.followups), fb._RELAY_CHAIN_MAX + 1)


class _LiveDeliverHarness(unittest.TestCase):
    """走真实 Runs 即时发送器 ``_make_runs_live_decisions``。"""

    def setUp(self) -> None:
        control_intent.CONTROL_QUEUE.discard(GROUP_KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, GROUP_KEY)
        relay._chain_reset(GROUP_KEY, "relay")
        self.addCleanup(relay._chain_reset, GROUP_KEY, "relay")
        self.followups: list[dict] = []
        self.sent: list[str] = []

        async def fake_send(*, bot=None, target=None, text="", media_urls=(), delivery=None, **kw):
            self.sent.append(text)
            return SimpleNamespace(ok=True, msg_ids=["m:sent"], history_content=lambda t: t)

        patches = [
            patch.object(fb, "load_rules", MagicMock(return_value=[])),
            patch.object(
                fb, "_fire_followup_turn",
                MagicMock(side_effect=lambda **kw: self.followups.append(kw)),
            ),
            patch.object(mt, "send_text_with_media", fake_send),
            patch.object(mt, "_maybe_intercept_bounce", AsyncMock(return_value=False)),
            patch.object(mt, "resolve_reply", MagicMock(return_value=None)),
            patch.object(
                mt, "_mcp",
                SimpleNamespace(message_buffer=None, active_sessions=None, run_tracker=MagicMock()),
            ),
            patch(
                "nonebot_plugin_hermes_adapter.core.runs_terminal.append_stream_unit",
                MagicMock(return_value=SimpleNamespace(component_id="primary:0")),
            ),
            patch.object(mt, "_mark_history_component", MagicMock()),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        self.deliver, self.state = mt._make_runs_live_decisions(
            bot=SimpleNamespace(self_id="999"),
            target=SimpleNamespace(private=False, id="g1"),
            adapter_name="onebotv11",
            user_id="u1",
            group_id="g1",
            is_private=False,
            fb_key=GROUP_KEY,
            nickname="Bot",
        )

    def _deliver(self, decision: dict) -> None:
        asyncio.run(self.deliver(decision, "run-1"))


class RunsLiveDeliverTests(_LiveDeliverHarness):
    def test_registered_not_final_continues_once_after_send(self) -> None:
        # 按产品路径登记进真实进程内队列（工具入口 + 真实 CONTROL_QUEUE）。
        result, _queue = _register(not_final=True)
        self.assertTrue(result.registered, result.error)
        control_intent.CONTROL_QUEUE.register(GROUP_KEY, {"not_final": True})

        receipt = {"body": "收到，我马上继续处理", "should_reply": True}
        self._deliver(receipt)
        self.assertEqual(self.sent, ["收到，我马上继续处理"])
        self.assertIs(receipt.get("not_final"), True)
        self.assertEqual([c["tag"] for c in self.followups], ["relay:not_final"])

        # 再来一份没有控制的正文：不继承、不续跑。
        final = {"body": "办完了", "should_reply": True}
        self._deliver(final)
        self.assertNotIn("not_final", final)
        self.assertEqual(len(self.followups), 1)

    def test_plain_body_is_untouched(self) -> None:
        decision = {"body": "普通回复", "should_reply": True}
        self._deliver(decision)
        self.assertEqual(self.sent, ["普通回复"])
        self.assertNotIn("not_final", decision)
        self.assertEqual(self.followups, [])


class RunsPromptTests(unittest.TestCase):
    def test_runs_protocol_documents_not_final(self) -> None:
        runs = render_decision_protocol(runs=True, group_rules="群规则")
        self.assertIn("not_final", runs)
        self.assertIn("继续执行轮", runs)

    def test_completions_protocol_has_no_not_final(self) -> None:
        completions = render_decision_protocol(runs=False, group_rules="群规则")
        self.assertNotIn("not_final", completions)
        self.assertIn("request_continuation", completions)


if __name__ == "__main__":
    unittest.main()
