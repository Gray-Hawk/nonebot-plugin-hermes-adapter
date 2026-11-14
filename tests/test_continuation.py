"""request_continuation：completions 显式续跑工具 + 进程内登记队列。

覆盖设计的硬边界：只在 completions 生效、只登记一次、只取一次、按会话隔离、
轮末未消费即丢弃；Runs 直接拒绝且不污染队列。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests import test_runs_terminal as e2  # noqa: E402
from nonebot_plugin_agent_adapter.core import continuation  # noqa: E402
from nonebot_plugin_agent_adapter.core.hermes_client import ChatResult  # noqa: E402
from nonebot_plugin_agent_adapter.core.inflight import make_inflight_key  # noqa: E402
from nonebot_plugin_agent_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_agent_adapter.mcp.tools.continuation import (  # noqa: E402
    RequestContinuationInput,
    request_continuation_impl,
)

ADAPTER = "onebotv11"
GROUP_KEY = make_inflight_key(ADAPTER, False, "u1", "g1")
PRIVATE_KEY = make_inflight_key(ADAPTER, True, "u1", None)


class ContinuationQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue = continuation.ContinuationQueue()

    def test_request_then_take_once(self) -> None:
        self.assertFalse(self.queue.pending(GROUP_KEY))
        self.queue.request(GROUP_KEY)
        self.assertTrue(self.queue.pending(GROUP_KEY))
        self.assertTrue(self.queue.take(GROUP_KEY))
        self.assertFalse(self.queue.pending(GROUP_KEY))
        # 只取一次：第二轮不会凭空再来一次续跑。
        self.assertFalse(self.queue.take(GROUP_KEY))

    def test_repeat_request_is_idempotent_within_turn(self) -> None:
        self.queue.request(GROUP_KEY)
        self.queue.request(GROUP_KEY)
        self.assertTrue(self.queue.take(GROUP_KEY))
        self.assertFalse(self.queue.take(GROUP_KEY))

    def test_scopes_are_isolated(self) -> None:
        self.queue.request(GROUP_KEY)
        self.assertFalse(self.queue.pending(PRIVATE_KEY))
        self.assertFalse(self.queue.take(PRIVATE_KEY))
        self.assertTrue(self.queue.take(GROUP_KEY))

    def test_discard_drops_unconsumed_request(self) -> None:
        self.queue.request(GROUP_KEY)
        self.assertTrue(self.queue.discard(GROUP_KEY))
        self.assertFalse(self.queue.pending(GROUP_KEY))
        self.assertFalse(self.queue.discard(GROUP_KEY))  # 幂等

    def test_no_capacity_cap(self) -> None:
        # 设计定案：不做 20 份上限；集合只按会话键去重。
        for index in range(50):
            self.queue.request(make_inflight_key(ADAPTER, False, f"u{index}", "g1"))
        self.assertTrue(self.queue.pending(make_inflight_key(ADAPTER, False, "u49", "g1")))


class RequestContinuationImplTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue = continuation.ContinuationQueue()
        self.addCleanup(continuation.CONTINUATION_QUEUE.discard, GROUP_KEY)
        self.addCleanup(continuation.CONTINUATION_QUEUE.discard, PRIVATE_KEY)

    def _group(self) -> RequestContinuationInput:
        return RequestContinuationInput(adapter=ADAPTER, group_id="g1")

    def _private(self) -> RequestContinuationInput:
        return RequestContinuationInput(adapter=ADAPTER, user_id="u1")

    def _call(self, inp, *, interface="completions", scope=CallerScope.dev()):
        return request_continuation_impl(
            inp, scope=scope, interface=interface, queue=self.queue,
        )

    def test_completions_group_registers_once(self) -> None:
        result = self._call(self._group())
        self.assertTrue(result.ok)
        self.assertTrue(result.registered)
        self.assertEqual(result.scope, "group")
        self.assertEqual(result.target_id, "g1")
        self.assertTrue(self.queue.pending(GROUP_KEY))
        self.assertEqual(self.queue.pending(GROUP_KEY), self.queue.pending(GROUP_KEY))

    def test_completions_private_registers(self) -> None:
        result = self._call(self._private())
        self.assertTrue(result.ok)
        self.assertEqual(result.scope, "private")
        self.assertEqual(result.target_id, "u1")
        self.assertTrue(self.queue.pending(PRIVATE_KEY))

    def test_runs_interface_is_rejected_and_queue_stays_clean(self) -> None:
        result = self._call(self._group(), interface="runs")
        self.assertFalse(result.ok)
        self.assertFalse(result.registered)
        self.assertIn("completions-only", result.error or "")
        self.assertFalse(self.queue.pending(GROUP_KEY))

    def test_unresolved_caller_is_rejected(self) -> None:
        private = self._call(self._private(), scope=None)
        self.assertFalse(private.ok)
        self.assertFalse(self.queue.pending(PRIVATE_KEY))
        group = self._call(self._group(), scope=None)
        self.assertFalse(group.ok)
        self.assertIn("not authorized", group.error or "")
        self.assertFalse(self.queue.pending(GROUP_KEY))

    def test_scope_requires_exactly_one_target(self) -> None:
        with self.assertRaises(ValueError):
            RequestContinuationInput(adapter=ADAPTER, group_id="g1", user_id="u1")
        with self.assertRaises(ValueError):
            RequestContinuationInput(adapter=ADAPTER)


class ContinuationConsumptionTests(unittest.TestCase):
    """handler 侧消费面：take 成功才算要续跑，未消费的请求轮末丢弃。"""

    def setUp(self) -> None:
        continuation.CONTINUATION_QUEUE.discard(GROUP_KEY)
        self.addCleanup(continuation.CONTINUATION_QUEUE.discard, GROUP_KEY)

    def test_take_returns_true_only_when_requested(self) -> None:
        self.assertFalse(continuation.take_continuation(GROUP_KEY))
        continuation.request_continuation(GROUP_KEY)
        self.assertTrue(continuation.pending_continuation(GROUP_KEY))
        self.assertTrue(continuation.take_continuation(GROUP_KEY))
        self.assertFalse(continuation.take_continuation(GROUP_KEY))


class PassiveContinuationConsumptionTests(e2._PassiveHarness):
    """真实 _run_passive_turn：零正文轮也要消费 request_continuation。

    completions 私聊先用 push_message 发回执、最终正文为空时，若这两个早退
    分支不消费续跑请求，任务会静默停住（与 reactive 口径不一致）。
    """

    def setUp(self) -> None:
        continuation.CONTINUATION_QUEUE.discard(PRIVATE_KEY)
        self.addCleanup(continuation.CONTINUATION_QUEUE.discard, PRIVATE_KEY)

    async def _run_with_continuation(self, result, *, tracker):
        from nonebot_plugin_agent_adapter.handlers import message_turns as mt

        async def chat(*args, **kwargs):
            continuation.request_continuation(PRIVATE_KEY)
            return result

        fired = AsyncMock()
        with patch.object(mt, "_fire_continuation", fired):
            calls = await self._run(result, tracker=tracker, chat=chat)
        return calls, fired

    async def test_passive_zero_body_still_fires_continuation(self) -> None:
        result = ChatResult(raw_text="", structured={"should_reply": False})
        calls, fired = await self._run_with_continuation(
            result, tracker=self._tracker(record_delivery=False)
        )
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("silent", "empty_output")])
        fired.assert_awaited_once()
        self.assertFalse(continuation.pending_continuation(PRIVATE_KEY))

    async def test_passive_empty_content_still_fires_continuation(self) -> None:
        result = ChatResult(raw_text="", structured={"should_reply": True, "body": ""})
        result.run_id = "run-e2-p"
        calls, fired = await self._run_with_continuation(
            result, tracker=self._tracker(record_delivery=False)
        )
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("silent", "empty_content")])
        fired.assert_awaited_once()
        self.assertFalse(continuation.pending_continuation(PRIVATE_KEY))


if __name__ == "__main__":
    unittest.main()
