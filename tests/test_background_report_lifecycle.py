"""提醒链生命周期回归（R1／R2）：真实 Runs 客户端、SSE 消费、live 决策回调、
真实出站认领回执与真实 SQLite；只替身 HTTP 响应与平台发送边界。

与 ``test_background_report_chain`` 的分工：那个文件用夹具直接摆出交付台账的
终态，验证销账／重试规则；这里跑**真实生命周期**，两条来源各自独立：

  R1 流式前缀不是完成：run 仍在跑、首批决定已实发（``expected_state='streaming'``）
     时不得销账，也不得放行同会话的下一条事件（两个方向都测，来源互不吞）。
  R2 重试串账：第一次尝试被业务终态（suppressed），第二次已由正常 client 提交并
     绑定新 run、但尚未产出 delivery 时，不得拿第一次的判定耗尽第二次。

另含「正常完成」控制：不暂停的完整 run 必须一次销账、只唤醒同会话下一条一次。
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests.test_background_report_chain import (  # noqa: E402
    SESSION,
    USER,
    FakeGateway,
    FakeWake,
    Harness,
)

from nonebot_plugin_hermes_adapter import mcp  # noqa: E402
from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import outbound as ob  # noqa: E402
from nonebot_plugin_hermes_adapter.core.background_report_outbox import (  # noqa: E402
    SOURCE_KANBAN,
    SOURCE_SUBAGENT,
)
from nonebot_plugin_hermes_adapter.core import background_report_runtime as rt  # noqa: E402
from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult  # noqa: E402
from nonebot_plugin_hermes_adapter.core.inflight import (  # noqa: E402
    InflightRegistry,
    make_inflight_key,
)
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt  # noqa: E402

SAMPLE = ROOT / "SAMPLES" / "runs_terminal_single_b4.json"


class _HTTP:
    """替身 Runs HTTP：POST 返回 run_id；SSE 可在首批实发后或首个事件前暂停。"""

    status_code = 200

    def __init__(self, run_id: str, events: list[dict], *, pause_after_first: bool = False,
                 pause_before_events: bool = False) -> None:
        self.run_id = run_id
        self.events = events
        self.pause_after_first = pause_after_first
        self.pause_before_events = pause_before_events
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.requests: list[tuple[str, dict | None]] = []

    async def post(self, url, **kwargs):
        self.requests.append((url, kwargs.get("json")))
        return SimpleNamespace(
            status_code=202,
            json=lambda: {"run_id": self.run_id, "status": "started"},
            headers={},
        )

    def stream(self, *args, **kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def aiter_lines(self):
        if self.pause_before_events:
            self.entered.set()
            await self.release.wait()
        paused = False
        for event in self.events:
            yield "data: " + json.dumps(event, ensure_ascii=False)
            if self.pause_after_first and not paused:
                delivery = mcp.run_tracker.get_delivery(self.run_id)
                components = mcp.run_tracker.list_components(self.run_id)
                if delivery and components and all(c["state"] == "sent" for c in components):
                    paused = True
                    self.entered.set()
                    await self.release.wait()


def _output(body: str) -> str:
    return "{}\n" + json.dumps({"body": body}, ensure_ascii=False)


def _simple_events(body: str) -> list[dict]:
    return [
        {"event": "message.delta", "delta": _output(body), "seq": 0},
        {"event": "run.completed", "output": _output(body), "seq": 1},
    ]


class _Runtime:
    """装配真实运行时的模块全局；只替 HTTP 与平台发送。"""

    def __init__(self, tracker, http: _HTTP, sends: list[str]) -> None:
        self.tracker = tracker
        self.http = http
        self.sends = sends
        self.stack = ExitStack()

    async def _platform_send_fn(self, _unimsg, **kwargs):
        self.sends.append(str(_unimsg))
        return SimpleNamespace(msg_ids=[f"offline-{len(self.sends)}"])

    def __enter__(self) -> "_Runtime":
        async def platform_send(_unimsg, **kwargs):
            return await self._platform_send_fn(_unimsg, **kwargs)

        self.stack.enter_context(patch.object(mcp, "run_tracker", self.tracker))
        self.stack.enter_context(patch.object(hc, "_get_http_client", lambda: self.http))
        self.stack.enter_context(patch.object(
            mt, "_mcp",
            SimpleNamespace(run_tracker=self.tracker, message_buffer=MagicMock(), active_sessions=None),
        ))
        self.stack.enter_context(patch.object(mt, "_maybe_intercept_bounce", AsyncMock(return_value=False)))
        self.stack.enter_context(patch.object(mt, "resolve_reply", lambda *args, **kw: None))
        self.stack.enter_context(patch.object(mt, "_post_send_feedback", AsyncMock()))
        self.stack.enter_context(patch.object(ob, "load_misc_config", lambda: SimpleNamespace(max_length=4000)))
        self.stack.enter_context(patch.object(ob, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=False)))
        self.stack.enter_context(patch.object(ob.alconna.UniMessage, "send", platform_send))
        self.stack.enter_context(patch.object(ob, "get_bot_nickname", AsyncMock(return_value="offline-bot")))
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.stack.callback(object.__setattr__, plugin_config, "hermes_api_interface", previous)
        return self

    def __exit__(self, *exc):
        return self.stack.__exit__(*exc)


async def _chat(event_id: str):
    return await hc.hermes_client.chat(
        text="[系统内部通知] 离线回归夹具", session_key=SESSION, user_id=USER, group_id=None,
        adapter_name="onebotv11", is_private=True, mode="passive",
        expect_structured=True, structured_tool_name="submit_decision", system_prompt="SYS",
        bot_self_id="999", source_event_id=event_id,
        target=SimpleNamespace(base_url="http://offline.invalid", api_key="", timeout=30, label="offline"),
        decision_callback=_callback(),
    )


def _callback():
    return mt._make_runs_live_decisions(
        bot=SimpleNamespace(self_id="999"), target=SimpleNamespace(private=True, id=USER),
        adapter_name="onebotv11", user_id=USER, group_id=None, is_private=True,
        fb_key="offline", nickname="offline-bot",
    )[0]


class _LifecycleCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.now = time.time()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def _assert_acked_once(self, h: Harness, event_id: str, run_id: str, source: str,
                                 next_event: str) -> None:
        self.assertEqual(h.outbox.get_event(event_id)["report_state"], "acked")
        self.assertEqual(
            h.gateway.completed,
            [(event_id, run_id)] if source == SOURCE_SUBAGENT else [],
        )
        self.assertEqual(h.wake.ids(), [next_event])
        await h.consumer.tick(self.now + 3)
        self.assertEqual(h.wake.ids(), [next_event])  # 恢复后只唤醒一次


class StreamingPrefixTests(_LifecycleCase):
    """R1：streaming 首份已发 ≠ 整次汇报完成。"""

    def _run_chain(self, source: str, next_source: str) -> None:
        h = Harness(self.tmp)
        event_id = "deleg_stream" if source == SOURCE_SUBAGENT else "kanban:t_stream:1"
        next_event = "kanban:t_next:2" if next_source == SOURCE_KANBAN else "deleg_next"
        h.add_event(event_id, source=source)
        h.outbox.claim_event(event_id, idempotency_key="attempt-1", now=self.now, lease_seconds=120)
        events = json.loads(SAMPLE.read_text())["events"]
        http = _HTTP("run-stream", events, pause_after_first=True)
        sends: list[str] = []

        async def scenario() -> None:
            with _Runtime(h.tracker, http, sends):
                task = asyncio.create_task(_chat(event_id))
                try:
                    await asyncio.wait_for(http.entered.wait(), 10)
                    delivery = h.tracker.get_delivery(http.run_id)
                    components = h.tracker.list_components(http.run_id)
                    # 前置：run 仍在产出计划，首份组件已实发
                    self.assertEqual(delivery["expected_state"], "streaming")
                    self.assertIsNotNone(h.tracker.get_by_run_id(http.run_id))
                    self.assertEqual([c["state"] for c in components], ["sent", "sent"])
                    self.assertEqual(len(sends), 1)

                    # 同会话另一来源排队：streaming 期间不得销账、不得放行
                    h.add_event(next_event, source=next_source)
                    await h.consumer.tick(self.now + 1)
                    self.assertEqual(h.outbox.get_event(event_id)["report_state"], "reporting")
                    self.assertEqual(h.wake.ids(), [])
                    self.assertEqual(h.gateway.completed, [])

                    # 放行 SSE：第二份决定真的还会来，证明前缀不是终态
                    http.release.set()
                    result = await asyncio.wait_for(task, 10)
                    self.assertFalse(result.parse_failed)
                    self.assertEqual(len(sends), 2)
                    final_state = h.tracker.get_delivery(http.run_id)["expected_state"]
                    self.assertIn(final_state, {"ready", "terminal"})

                    await h.consumer.tick(self.now + 2)
                    await self._assert_acked_once(h, event_id, http.run_id, source, next_event)
                finally:
                    http.release.set()
                    if not task.done():
                        task.cancel()

        asyncio.run(scenario())
        h.tracker.close()
        h.outbox.close()

    def test_subagent_stream_does_not_release_kanban_in_same_session(self) -> None:
        self._run_chain(SOURCE_SUBAGENT, SOURCE_KANBAN)

    def test_kanban_stream_does_not_release_subagent_in_same_session(self) -> None:
        self._run_chain(SOURCE_KANBAN, SOURCE_SUBAGENT)

    def test_completed_run_acks_and_wakes_once_per_source(self) -> None:
        """正常完成控制：不暂停的整条 run 必须一次销账，且只唤醒下一条一次。"""
        for source, next_source in ((SOURCE_SUBAGENT, SOURCE_KANBAN), (SOURCE_KANBAN, SOURCE_SUBAGENT)):
            with self.subTest(source=source):
                self._tmp.cleanup()
                self._tmp = tempfile.TemporaryDirectory()
                self.tmp = Path(self._tmp.name)
                h = Harness(self.tmp)
                event_id = "deleg_done" if source == SOURCE_SUBAGENT else "kanban:t_done:1"
                next_event = "kanban:t_after:2" if next_source == SOURCE_KANBAN else "deleg_after"
                h.add_event(event_id, source=source)
                h.outbox.claim_event(event_id, idempotency_key="attempt-1", now=self.now, lease_seconds=120)
                http = _HTTP("run-done", _simple_events("正常完成汇报"))
                sends: list[str] = []

                async def scenario() -> None:
                    with _Runtime(h.tracker, http, sends):
                        result = await _chat(event_id)
                        self.assertFalse(result.parse_failed)
                        self.assertEqual(len(sends), 1)
                        self.assertIn(h.tracker.get_delivery(http.run_id)["expected_state"], {"ready", "terminal"})
                        h.add_event(next_event, source=next_source)
                        await h.consumer.tick(self.now + 1)
                        await self._assert_acked_once(h, event_id, http.run_id, source, next_event)

                asyncio.run(scenario())
                h.tracker.close()
                h.outbox.close()


class RetryAttemptTests(_LifecycleCase):
    """R2：上一代尝试的 suppressed 判定不得误杀在飞的新尝试。"""

    def _run_chain(self, source: str) -> None:
        h = Harness(self.tmp)
        event_id = "deleg_retry" if source == SOURCE_SUBAGENT else "kanban:t_retry:3"
        h.add_event(event_id, source=source)
        h.outbox.claim_event(event_id, idempotency_key="attempt-1", now=self.now, lease_seconds=120)
        sends: list[str] = []

        async def scenario() -> None:
            first = _HTTP("run-old-unsent", _simple_events("第一次被输出闸拦截"))
            with _Runtime(h.tracker, first, sends), patch.object(
                mt, "_maybe_intercept_bounce", AsyncMock(return_value=True)
            ):
                result = await _chat(event_id)
                self.assertFalse(result.parse_failed)
                self.assertEqual(sends, [])
                mt._settle(result, "suppressed", "bounce")
            self.assertEqual(h.tracker.get_delivery(first.run_id)["expected_state"], "suppressed")

            await h.consumer.tick(self.now + 1)
            self.assertEqual(h.outbox.get_event(event_id)["report_state"], "retry_wait")

            second = _HTTP("run-new-retry", _simple_events("第二次汇报实际成功"), pause_before_events=True)
            tasks: list[asyncio.Task] = []

            async def wake(*args):
                tasks.append(asyncio.create_task(_chat(event_id)))
                return None

            h.consumer.wake = wake
            with _Runtime(h.tracker, second, sends):
                await h.consumer.tick(self.now + 17)
                await asyncio.wait_for(second.entered.wait(), 10)
                latest = h.tracker.get_latest_submission_by_source_event(event_id)
                old = h.tracker.get_latest_delivery_by_source_event(event_id)
                # 前置：第二次尝试已绑定新 run，但还没有任何 delivery
                self.assertEqual(latest["run_id"], second.run_id)
                self.assertEqual(old["run_id"], first.run_id)
                self.assertIsNone(h.tracker.get_delivery(second.run_id))
                self.assertEqual(h.outbox.get_event(event_id)["attempt_count"], 2)

                await h.consumer.tick(self.now + 18)
                row = h.outbox.get_event(event_id)
                self.assertEqual(row["report_state"], "reporting")  # 在飞的新尝试
                self.assertEqual(row["run_id"], second.run_id)      # 不被旧 run 覆盖
                self.assertEqual(h.gateway.dropped, [])             # 不 drop 上游
                self.assertEqual(sends, [])                         # 尚未产出

                second.release.set()
                await asyncio.wait_for(tasks[0], 10)
                self.assertEqual(len(sends), 1)                     # 第二次真的发出去了

                await h.consumer.tick(self.now + 19)
                self.assertEqual(h.outbox.get_event(event_id)["report_state"], "acked")
                self.assertEqual(h.outbox.get_event(event_id)["run_id"], second.run_id)
                self.assertIn(h.tracker.get_delivery(second.run_id)["expected_state"], {"ready", "terminal"})
                self.assertEqual(
                    h.gateway.completed,
                    [(event_id, second.run_id)] if source == SOURCE_SUBAGENT else [],
                )

        asyncio.run(scenario())
        h.tracker.close()
        h.outbox.close()

    def test_subagent_retry_survives_previous_suppressed_delivery(self) -> None:
        self._run_chain(SOURCE_SUBAGENT)

    def test_kanban_retry_survives_previous_suppressed_delivery(self) -> None:
        self._run_chain(SOURCE_KANBAN)


class QueuedRetryTests(_LifecycleCase):
    """R2 排队窗口：第二次 wake 已接受但仍在 inflight 队列、尚无新 submission。

    链上 ``wake_private_turn``／``_handle_passive_path``／InflightRegistry／
    pop／``_refire``／public HermesClient／Runs SSE／live 决策／出站回执都是真的；
    只替 HTTP 响应、平台发送与合法路由边界。上一代 suppressed 交付不得耗尽本次
    尝试，排队 turn 真正跑起来后要正常销账。
    """

    def _run_chain(self, source: str) -> None:
        h = Harness(self.tmp)
        event_id = "deleg_queue" if source == SOURCE_SUBAGENT else "kanban:t_queue:4"
        h.add_event(event_id, source=source)
        h.outbox.claim_event(event_id, idempotency_key="first", now=self.now, lease_seconds=120)
        sends: list[str] = []

        async def scenario() -> None:
            first = _HTTP("run-old-suppressed", _simple_events("第一次被输出闸拦截"))
            with _Runtime(h.tracker, first, sends), patch.object(
                mt, "_maybe_intercept_bounce", AsyncMock(return_value=True)
            ):
                result = await _chat(event_id)
                mt._settle(result, "suppressed", "bounce")
            self.assertEqual(sends, [])
            await h.consumer.tick(self.now + 1)
            self.assertEqual(h.outbox.get_event(event_id)["report_state"], "retry_wait")
            old = h.tracker.get_latest_submission_by_source_event(event_id)
            self.assertEqual(old["run_id"], first.run_id)

            registry = InflightRegistry()
            tasks: list[asyncio.Task] = []
            user_entered = asyncio.Event()
            user_release = asyncio.Event()

            def spawn(coro, *args, **kwargs):
                task = asyncio.create_task(coro)
                tasks.append(task)
                return task

            async def turn(**kwargs):
                row = kwargs["current_buffered"]
                if row.source_event_id is None:
                    user_entered.set()
                    await user_release.wait()
                    return ChatResult(raw_text="control user completed")
                self.assertEqual(row.source_event_id, event_id)
                self.assertEqual(row.origin.author_kind.value, "system")
                return await _chat(row.source_event_id)

            route = (SimpleNamespace(self_id="999"), SimpleNamespace(private=True, id=USER))
            h.consumer.wake = rt._wake
            second = _HTTP("run-new-queued", _simple_events("排队重试真的发出去了"))
            with _Runtime(h.tracker, second, sends), \
                 patch.object(mcp, "inflight", registry), \
                 patch.object(relay, "spawn_bg_task", spawn), \
                 patch.object(relay, "_run_passive_turn", turn), \
                 patch.object(relay, "_resolve_wake_route", lambda *args: route), \
                 patch.object(relay, "_wake_rate_limited", lambda *args: None), \
                 patch.object(relay.user_lists, "private_allowed", lambda *args: True), \
                 patch.object(relay, "_QUEUE_AGGREGATE_WINDOW_SEC", 0.001), \
                 patch.object(relay, "_release_slot_and_retry", lambda key, *args: registry.exit(key)):
                user = asyncio.create_task(relay._handle_passive_path(
                    bot=route[0], target=route[1], adapter_name="onebotv11", user_id=USER,
                    group_id=None, text="ordinary user turn in progress", image_urls=[],
                    is_private=True, now_ms=int(self.now * 1000) - 1000,
                    origin=MessageOrigin.human(),
                ))
                await asyncio.wait_for(user_entered.wait(), 10)

                await h.consumer.tick(self.now + 17)
                claimed = h.outbox.get_event(event_id)
                self.assertEqual(claimed["attempt_count"], 2)
                await asyncio.gather(*tasks)

                key = make_inflight_key("onebotv11", True, USER, None)
                pending = registry.peek_next_pending(key, after_ts=0)
                self.assertIsNotNone(pending)
                self.assertTrue(any(r.source_event_id == event_id for r in pending.fragments))
                # 前置：wake 只在排队，本次尝试尚无 submission；latest 仍是旧 suppressed。
                self.assertEqual(
                    h.tracker.get_latest_submission_by_source_event(event_id)["idempotency_key"],
                    old["idempotency_key"],
                )
                self.assertIsNone(h.tracker.get_delivery(second.run_id))
                self.assertEqual(sends, [])

                # 正常下一拍（+30s）：不得拿上一代的判定耗尽本次尝试。
                await h.consumer.tick(self.now + 47)
                queued_row = h.outbox.get_event(event_id)
                self.assertEqual(queued_row["report_state"], "reporting")
                self.assertEqual(h.gateway.dropped, [])
                self.assertEqual(queued_row["attempt_baseline_seq"], old["submission_seq"])
                self.assertIsNone(queued_row["run_id"])

                # 释放占槽：真实 finally → pop → refire，本次 run 实际发出一次正文。
                user_release.set()
                await asyncio.wait_for(user, 10)
                for _ in range(5):
                    await asyncio.gather(*list(tasks))
                    await asyncio.sleep(0)
                self.assertEqual(len(sends), 1)
                self.assertEqual(
                    h.tracker.get_latest_submission_by_source_event(event_id)["run_id"],
                    second.run_id,
                )

                await h.consumer.tick(self.now + 48)
                final = h.outbox.get_event(event_id)
                self.assertEqual(final["report_state"], "acked")
                self.assertEqual(final["run_id"], second.run_id)
                self.assertEqual(
                    h.gateway.completed,
                    [(event_id, second.run_id)] if source == SOURCE_SUBAGENT else [],
                )

        asyncio.run(scenario())
        h.tracker.close()
        h.outbox.close()

    def test_subagent_queued_retry_stays_reporting_then_acks(self) -> None:
        self._run_chain(SOURCE_SUBAGENT)

    def test_kanban_queued_retry_stays_reporting_then_acks(self) -> None:
        self._run_chain(SOURCE_KANBAN)


class WatermarkReadFailureTests(_LifecycleCase):
    """R3：新尝试的代际水位读不到时必须 fail closed，恢复后按当前水位继续。

    故障边界是**临时 tracker 连接上真实的一次 SQLite 读中止**（progress handler
    返回非零 → ``OperationalError: interrupted``），随后立即恢复正常读写；不替换
    水位方法、过滤、认领、drop 或 ACK。链上 wake→inflight 排队→pop/refire→public
    HermesClient→Runs SSE→live→outbound→SQLite 均为真实对象。
    """

    def _run_chain(self, source: str) -> None:
        h = Harness(self.tmp)
        event_id = "deleg_wm_fail" if source == SOURCE_SUBAGENT else "kanban:t_wm_fail:7"
        h.add_event(event_id, source=source)
        h.outbox.claim_event(event_id, idempotency_key="first", now=self.now, lease_seconds=120)
        sends: list[str] = []

        async def scenario() -> None:
            first = _HTTP("run-old-suppressed", _simple_events("第一次被输出闸拦截"))
            with _Runtime(h.tracker, first, sends), patch.object(
                mt, "_maybe_intercept_bounce", AsyncMock(return_value=True)
            ):
                result = await _chat(event_id)
                mt._settle(result, "suppressed", "bounce")
            self.assertEqual(sends, [])
            await h.consumer.tick(self.now + 1)
            scheduled = h.outbox.get_event(event_id)
            self.assertEqual(scheduled["report_state"], "retry_wait")
            self.assertEqual(scheduled["attempt_count"], 1)
            old = h.tracker.get_latest_submission_by_source_event(event_id)
            self.assertEqual(old["run_id"], first.run_id)

            registry = InflightRegistry()
            tasks: list[asyncio.Task] = []
            user_entered = asyncio.Event()
            user_release = asyncio.Event()

            def spawn(coro, *args, **kwargs):
                task = asyncio.create_task(coro)
                tasks.append(task)
                return task

            async def turn(**kwargs):
                row = kwargs["current_buffered"]
                if row.source_event_id is None:
                    user_entered.set()
                    await user_release.wait()
                    return ChatResult(raw_text="control user completed")
                self.assertEqual(row.source_event_id, event_id)
                self.assertEqual(row.origin.author_kind.value, "system")
                return await _chat(row.source_event_id)

            route = (SimpleNamespace(self_id="999"), SimpleNamespace(private=True, id=USER))
            h.consumer.wake = rt._wake
            second = _HTTP("run-new-queued", _simple_events("排队重试真的发出去了"))
            with _Runtime(h.tracker, second, sends), \
                 patch.object(mcp, "inflight", registry), \
                 patch.object(relay, "spawn_bg_task", spawn), \
                 patch.object(relay, "_run_passive_turn", turn), \
                 patch.object(relay, "_resolve_wake_route", lambda *args: route), \
                 patch.object(relay, "_wake_rate_limited", lambda *args: None), \
                 patch.object(relay.user_lists, "private_allowed", lambda *args: True), \
                 patch.object(relay, "_QUEUE_AGGREGATE_WINDOW_SEC", 0.001), \
                 patch.object(relay, "_release_slot_and_retry", lambda key, *args: registry.exit(key)):
                user = asyncio.create_task(relay._handle_passive_path(
                    bot=route[0], target=route[1], adapter_name="onebotv11", user_id=USER,
                    group_id=None, text="ordinary user turn in progress", image_urls=[],
                    is_private=True, now_ms=int(self.now * 1000) - 1000,
                    origin=MessageOrigin.human(),
                ))
                await asyncio.wait_for(user_entered.wait(), 10)

                # 一次真实读失败：认领前的水位 SELECT 被 SQLite 中止，随后立即恢复。
                interrupted: list[bool] = []

                def abort_read() -> int:
                    interrupted.append(True)
                    return 1

                h.tracker._conn.set_progress_handler(abort_read, 1)
                try:
                    await h.consumer.tick(self.now + 16)
                finally:
                    h.tracker._conn.set_progress_handler(None, 0)
                self.assertEqual(interrupted, [True])  # 故障确实发生在这一次读取上
                blocked = h.outbox.get_event(event_id)
                # fail closed：不认领、不唤醒、不耗预算、不 drop、不覆盖归属。
                self.assertEqual(blocked["report_state"], "retry_wait")
                self.assertEqual(blocked["attempt_count"], 1)
                self.assertEqual(blocked["run_id"], old["run_id"])
                self.assertEqual(tasks, [])  # 没有不可追踪的 wake／排队
                self.assertEqual(h.gateway.dropped, [])

                # 水位读取恢复：按当前水位认领，wake 被接受但排在真人 turn 之后。
                await h.consumer.tick(self.now + 17)
                claimed = h.outbox.get_event(event_id)
                self.assertEqual(claimed["attempt_count"], 2)
                self.assertEqual(claimed["attempt_baseline_seq"], old["submission_seq"])
                self.assertIsNone(claimed["run_id"])
                await asyncio.gather(*tasks)
                key = make_inflight_key("onebotv11", True, USER, None)
                pending = registry.peek_next_pending(key, after_ts=0)
                self.assertIsNotNone(pending)
                self.assertEqual(sum(r.source_event_id == event_id for r in pending.fragments), 1)
                self.assertEqual(sends, [])

                # 正常下一拍：本次尝试尚无 submission，不得拿旧 suppressed 判死。
                await h.consumer.tick(self.now + 47)
                queued = h.outbox.get_event(event_id)
                self.assertEqual(queued["report_state"], "reporting")
                self.assertEqual(queued["run_id"], None)
                self.assertEqual(h.gateway.dropped, [])

                # 释放占槽：真实 pop/refire，本次 run 实际发出一次正文后正常销账。
                user_release.set()
                await asyncio.wait_for(user, 10)
                for _ in range(5):
                    await asyncio.gather(*list(tasks))
                    await asyncio.sleep(0)
                self.assertEqual(len(sends), 1)
                self.assertEqual(
                    h.tracker.get_latest_submission_by_source_event(event_id)["run_id"],
                    second.run_id,
                )
                await h.consumer.tick(self.now + 48)
                final = h.outbox.get_event(event_id)
                self.assertEqual(final["report_state"], "acked")
                self.assertEqual(final["run_id"], second.run_id)
                self.assertEqual(
                    h.gateway.completed,
                    [(event_id, second.run_id)] if source == SOURCE_SUBAGENT else [],
                )

        asyncio.run(scenario())
        h.tracker.close()
        h.outbox.close()

    def test_subagent_watermark_read_failure_fails_closed_then_acks(self) -> None:
        self._run_chain(SOURCE_SUBAGENT)

    def test_kanban_watermark_read_failure_fails_closed_then_acks(self) -> None:
        self._run_chain(SOURCE_KANBAN)


if __name__ == "__main__":
    unittest.main()
