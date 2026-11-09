"""控制配对与跨轮清理的正式回归（来自 GPT 审核 1cb9bd9 的真实探针见证）。

三组真实时序：
- 甲乙丙交错：正文乙已到达、正在等 3 秒时登记给丙的控制，不能被乙抢走；
- 接力轮剩余控制：真实 ``_refire`` 正常/异常/交接退出都不许把控制带进下一轮；
- 一组连发的控制作用域：reply_to 只挂首条；should_exit_active 在末条发出后才
  生效；锚控制只在首条执行一次。
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tests import _bootstrap  # noqa: F401

from tests import test_runs_terminal as e2
from tests.test_runs_stream import _ClientChain, real_chat

from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import control_intent
from nonebot_plugin_hermes_adapter.core import hermes_client as hc
from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager
from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
from nonebot_plugin_hermes_adapter.core.inflight import InflightRegistry, make_inflight_key
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin
from nonebot_plugin_hermes_adapter.core.npha_terminal import (
    decode_terminal_batch,
    structured_batch_from_terminal,
)
from nonebot_plugin_hermes_adapter.core.routing import CallerScope
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay
from nonebot_plugin_hermes_adapter.mcp.tools.control import SubmitControlInput, submit_control_impl

KEY = make_inflight_key("onebotv11", False, "u1", "g1")


def _result(text: str) -> ChatResult:
    decisions = structured_batch_from_terminal(decode_terminal_batch(text))
    result = ChatResult(raw_text=text, structured=decisions[0])
    result.terminal_decisions = decisions
    result.run_id = "run-e2-h"
    return result


def _row(text: str = "queued input", *, now: int | None = None) -> BufferedMessage:
    return BufferedMessage(
        ts=now if now is not None else int(time.time() * 1000),
        adapter="onebotv11", group_id="g1", user_id="u1", nickname="user",
        content=text, origin=MessageOrigin.human(),
    )


class ControlBindingAtArrivalTests(e2._ReactiveHarness):
    """R1：控制按「完整正文到达/入队」消费，而不是延迟发送时才绑定。"""

    async def _exercise(self, *, register_after_second_arrival: bool):
        control_intent.CONTROL_QUEUE.discard(KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, KEY)
        blocked, resume = asyncio.Event(), asyncio.Event()
        sleeps, quotes, observed = [], [], []
        output = "\n".join(
            json.dumps({"body": body}, ensure_ascii=False) for body in ["甲", "乙", "丙"]
        )
        lines = output.splitlines()
        events = [
            {"event": "message.delta", "delta": "\n".join(lines[:2]), "seq": 0},
            {"event": "tool.started", "tool": "mcp__bridge__submit_control", "seq": 1},
            {"event": "tool.completed", "tool": "mcp__bridge__submit_control", "seq": 2},
            {"event": "message.delta", "delta": "\n" + lines[2], "seq": 3},
            {"event": "run.completed", "output": output, "seq": 4},
        ]
        temp = tempfile.TemporaryDirectory(prefix="fifo-regression-")
        self.addCleanup(temp.cleanup)
        tracker = RunTracker(Path(temp.name) / "runs.db")
        self.addCleanup(tracker.close)
        transport = real_chat(tracker, events)
        original_lines = _ClientChain._Resp.aiter_lines

        def register(**fields) -> None:
            result = submit_control_impl(
                SubmitControlInput(adapter="onebotv11", group_id="g1", **fields),
                scope=CallerScope.dev(), interface="runs",
            )
            self.assertTrue(result.ok)

        async def paced_lines(response):
            index = 0
            expected_quote = "" if register_after_second_arrival else "m:11"
            async for line in original_lines(response):
                yield line
                if index == 0:
                    # 乙已到达并进入发送队列，正在等条间节奏；此刻才登记给丙的控制。
                    await asyncio.wait_for(blocked.wait(), 1)
                    self.assertEqual(quotes, [expected_quote])
                    if register_after_second_arrival:
                        register(reply_to="m:12")
                if index == 3:
                    resume.set()
                index += 1

        async def sleep(delay):
            sleeps.append(delay)
            if len(sleeps) == 1:
                blocked.set()
                await resume.wait()
            await asyncio.sleep(0)

        async def chat(*args, **kwargs):
            result = await transport(*args, **kwargs)
            observed.extend(
                (decision["body"], control_intent.control_value(decision, "reply_to"))
                for decision in result.streamed_decisions
            )
            return result

        def resolve(*args, **kwargs):
            quotes.append(kwargs.get("reply_to"))
            return None

        if not register_after_second_arrival:
            register(reply_to="m:11")

        with patch.object(plugin_config, "hermes_api_interface", "runs"), \
                patch.object(hc, "_runs_reply_sleep", sleep), \
                patch.object(_ClientChain._Resp, "aiter_lines", paced_lines):
            calls = await asyncio.wait_for(
                self._run(
                    None, tracker=tracker, chat=chat,
                    resolve_reply=MagicMock(side_effect=resolve),
                ),
                3,
            )
        self.assertEqual([call["text"] for call in calls["send"]], ["甲", "乙", "丙"])
        self.assertEqual(len(tracker.list_components("run-e2-h")), 6)
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(KEY), 0)
        return observed, quotes

    async def test_control_registered_while_second_body_waits_goes_to_third(self) -> None:
        observed, quotes = await self._exercise(register_after_second_arrival=True)
        # 甲/乙到达时还没有控制 → 缺省；3 秒等待期间登记的控制属于下一份正文丙。
        self.assertEqual(observed, [("甲", ""), ("乙", ""), ("丙", "m:12")])
        self.assertEqual(quotes, ["", "", "m:12"])

    async def test_control_before_first_body_still_goes_to_first(self) -> None:
        observed, quotes = await self._exercise(register_after_second_arrival=False)
        self.assertEqual(observed, [("甲", "m:11"), ("乙", ""), ("丙", "")])
        self.assertEqual(quotes, ["m:11", "", ""])


class RefireControlCleanupTests(e2._ReactiveHarness):
    """R2：真实接力轮（_refire）退出时清掉剩余控制，不跨轮误继承。"""

    def setUp(self) -> None:
        control_intent.CONTROL_QUEUE.discard(KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, KEY)
        self.now = int(time.time() * 1000)

    def _runtime(self) -> tuple[SimpleNamespace, InflightRegistry, ActiveSessionManager]:
        inflight = InflightRegistry()
        manager = ActiveSessionManager()
        manager.trigger("onebotv11", "g1", "u1", now_ms=self.now)
        return SimpleNamespace(inflight=inflight, active_sessions=manager, run_tracker=None), inflight, manager

    async def test_refire_leftover_control_does_not_leak_to_next_turn(self) -> None:
        runtime, inflight, manager = self._runtime()
        row = _row(now=self.now)
        self.assertEqual(
            inflight.try_enter(KEY, row, is_direct=True, original_msg_id=None, now_ms=self.now),
            "entered",
        )
        tracker = self._tracker()

        async def first_chat(*args, **kwargs):
            for reply_to in ("m:11", "m:99"):
                result = submit_control_impl(
                    SubmitControlInput(adapter="onebotv11", group_id="g1", reply_to=reply_to),
                    scope=CallerScope.dev(), interface="runs",
                )
                self.assertTrue(result.ok)
            return _result('{"body":""}')

        first_calls = []

        async def real_turn_through_harness(**kwargs):
            first_calls.append(await self._run(
                None, tracker=tracker, chat=first_chat, sessions=manager, now_ms=self.now,
            ))
            return _result('{"body":""}')

        with patch.object(relay, "_mcp", runtime), \
                patch.object(relay, "_run_reactive_turn", real_turn_through_harness):
            await relay._refire(
                key=KEY, trigger_msg=row, is_direct=True, original_msg_id=None,
                depth=1, mode="reactive", addressed_to_bot=True,
                bot=MagicMock(self_id=999), target=SimpleNamespace(private=False),
                adapter_name="onebotv11", group_id="g1",
            )
        self.assertEqual(len(first_calls), 1)
        self.assertEqual(first_calls[0]["send"], [])
        self.assertFalse(inflight.is_inflight(KEY))
        # 空 body 只消费一份控制；接力轮退出时剩余一份必须就地丢弃。
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(KEY), 0)

        resolved = []
        calls = await self._run(
            _result('{"body":"new unrelated reply"}'), tracker=self._tracker(),
            sessions=manager, now_ms=self.now + 1,
            resolve_reply=MagicMock(side_effect=lambda *a, **kw: resolved.append(kw["reply_to"])),
        )
        self.assertEqual([call["text"] for call in calls["send"]], ["new unrelated reply"])
        # 下一轮零控制工具：若接力轮泄漏，这里会解析出 m:99。
        self.assertEqual(resolved, [""])

    async def test_refire_exception_exit_drops_leftover_control(self) -> None:
        runtime, inflight, manager = self._runtime()
        row = _row(now=self.now)
        self.assertEqual(
            inflight.try_enter(KEY, row, is_direct=True, original_msg_id=None, now_ms=self.now),
            "entered",
        )

        async def exploding_turn(**kwargs):
            result = submit_control_impl(
                SubmitControlInput(adapter="onebotv11", group_id="g1", reply_to="m:99"),
                scope=CallerScope.dev(), interface="runs",
            )
            self.assertTrue(result.ok)
            raise RuntimeError("refire boom")

        with patch.object(relay, "_mcp", runtime), \
                patch.object(relay, "_run_reactive_turn", exploding_turn):
            await relay._refire(
                key=KEY, trigger_msg=row, is_direct=True, original_msg_id=None,
                depth=1, mode="reactive", addressed_to_bot=True,
                bot=MagicMock(self_id=999), target=SimpleNamespace(private=False),
                adapter_name="onebotv11", group_id="g1",
            )
        self.assertFalse(inflight.is_inflight(KEY))
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(KEY), 0)

    async def test_refire_handoff_discards_before_spawning_next_hop(self) -> None:
        runtime, inflight, manager = self._runtime()
        row = _row(now=self.now)
        queued = _row("next pending", now=self.now + 1)
        self.assertEqual(
            inflight.try_enter(KEY, row, is_direct=True, original_msg_id=None, now_ms=self.now),
            "entered",
        )
        # 接力开始前已在 inflight 里排队的下一发：本跳结束时会交给下一跳。
        self.assertEqual(
            inflight.try_enter(
                KEY, queued, is_direct=True, original_msg_id=None, now_ms=self.now + 1,
            ),
            "queued",
        )

        async def quiet_turn(**kwargs):
            result = submit_control_impl(
                SubmitControlInput(adapter="onebotv11", group_id="g1", reply_to="m:42"),
                scope=CallerScope.dev(), interface="runs",
            )
            self.assertTrue(result.ok)
            return _result('{"body":""}')

        pending_at_spawn = []

        def fake_spawn(coro):
            # 交接前必须已丢弃本跳剩余控制。
            pending_at_spawn.append(control_intent.CONTROL_QUEUE.pending(KEY))
            coro.close()

        with patch.object(relay, "_mcp", runtime), \
                patch.object(relay, "_run_reactive_turn", quiet_turn), \
                patch.object(relay, "spawn_bg_task", fake_spawn):
            await relay._refire(
                key=KEY, trigger_msg=row, is_direct=True, original_msg_id=None,
                depth=1, mode="reactive", addressed_to_bot=True,
                bot=MagicMock(self_id=999), target=SimpleNamespace(private=False),
                adapter_name="onebotv11", group_id="g1",
            )
        self.assertEqual(pending_at_spawn, [0])
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(KEY), 0)


class ControlGroupScopeTests(e2._ReactiveHarness):
    """一组连发（拆分输出）里的控制作用域。

    主人定案：reply_to 只挂首条；锚（pin/release/answer/merge）只在首条执行一次；
    should_exit_active 在整组末条发出之后才生效。
    """

    def setUp(self) -> None:
        control_intent.CONTROL_QUEUE.discard(KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, KEY)
        self.now = int(time.time() * 1000)
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    def _plain_tracker(self) -> RunTracker:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "plain.db")
        self.addCleanup(tracker.close)
        return tracker

    def _manager(self, *, progress: list | None = None):
        manager = ActiveSessionManager()
        session = manager.trigger("onebotv11", "g1", "u1", now_ms=self.now)
        observed = {"marks": 0, "closes": [], "pins": 0, "releases": 0}

        real_mark = manager.mark_bot_replied
        def mark(*args, **kwargs):
            observed["marks"] += 1
            return real_mark(*args, **kwargs)

        real_end = manager.end_if_current_generation
        def end_if(current, generation):
            # 收窗时刻已处理过的份数：必须等于组内份数（末条之后），不能是 0。
            observed["closes"].append(len(progress or []))
            return real_end(current, generation)

        real_pin = manager.pin_anchor
        def pin_anchor(*args, **kwargs):
            observed["pins"] += 1
            return real_pin(*args, **kwargs)

        real_release = manager.release_anchor
        def release_anchor(*args, **kwargs):
            observed["releases"] += 1
            return real_release(*args, **kwargs)

        manager.mark_bot_replied = MagicMock(side_effect=mark)
        manager.end_if_current_generation = MagicMock(side_effect=end_if)
        manager.pin_anchor = MagicMock(side_effect=pin_anchor)
        manager.release_anchor = MagicMock(side_effect=release_anchor)
        return manager, observed

    async def test_reply_to_first_only_and_exit_after_last(self) -> None:
        resolved: list = []
        manager, observed = self._manager(progress=resolved)
        control_intent.CONTROL_QUEUE.register(
            KEY, {"reply_to": "m:7", "should_exit_active": True},
        )
        calls = await self._run(
            _result('{"body":"甲"}\n{"body":"乙"}'), tracker=self._tracker(),
            sessions=manager, now_ms=self.now,
            resolve_reply=MagicMock(side_effect=lambda *a, **kw: resolved.append(kw["reply_to"])),
        )
        self.assertEqual([call["text"] for call in calls["send"]], ["甲", "乙"])
        # 引用只挂首条，后续条不得重复引用。
        self.assertEqual(resolved, ["m:7", ""])
        # 两条都发完（都 mark 过）之后才收窗；窗口最终关闭。
        self.assertEqual(observed["closes"], [2])
        self.assertEqual(observed["marks"], 2)
        self.assertIsNone(manager.get("onebotv11", "g1"))

    async def test_anchor_control_runs_once_on_first_message(self) -> None:
        manager, observed = self._manager()
        buffered = _row("帮我跟进这件事", now=self.now)
        buffered.id = 5
        control_intent.CONTROL_QUEUE.register(KEY, {"pin_anchor": True})
        calls = await self._run(
            _result('{"body":"甲"}\n{"body":"乙"}'), tracker=self._tracker(),
            sessions=manager, current_buffered=buffered, now_ms=self.now,
        )
        self.assertEqual([call["text"] for call in calls["send"]], ["甲", "乙"])
        # 锚有持续性：只按首条执行一次，不在后续条重复。
        self.assertEqual(observed["pins"], 1)
        self.assertEqual(manager.pinned_commissioners("onebotv11", "g1"), {"u1"})
        # 没有退窗控制 → 窗口仍在。
        self.assertIsNotNone(manager.get("onebotv11", "g1"))

    async def test_legacy_control_line_scopes_the_same_way(self) -> None:
        resolved: list = []
        manager, observed = self._manager(progress=resolved)
        calls = await self._run(
            _result('{"should_exit_active":true,"reply_to":"m:9"}\n{"body":"甲"}\n{"body":"乙"}'),
            tracker=self._tracker(), sessions=manager, now_ms=self.now,
            resolve_reply=MagicMock(side_effect=lambda *a, **kw: resolved.append(kw["reply_to"])),
        )
        self.assertEqual([call["text"] for call in calls["send"]], ["甲", "乙"])
        self.assertEqual(resolved, ["m:9", ""])
        self.assertEqual(observed["closes"], [2])
        self.assertIsNone(manager.get("onebotv11", "g1"))

    async def test_live_streamed_group_same_scope(self) -> None:
        resolved: list = []
        manager, observed = self._manager(progress=resolved)
        control_intent.CONTROL_QUEUE.register(
            KEY, {"reply_to": "m:8", "should_exit_active": True},
        )
        tracker = self._plain_tracker()

        async def no_wait(_delay):
            return None

        with patch.object(hc, "_runs_reply_sleep", no_wait):
            calls = await self._run(
                None, tracker=tracker,
                chat=real_chat(tracker, [
                    {"event": "message.delta", "delta": '{"body":"甲"}\n{"body":"乙"}', "seq": 0},
                    {"event": "run.completed", "output": '{"body":"甲"}\n{"body":"乙"}', "seq": 1},
                ]),
                sessions=manager, now_ms=self.now,
                resolve_reply=MagicMock(side_effect=lambda *a, **kw: resolved.append(kw["reply_to"])),
            )
        self.assertEqual([call["text"] for call in calls["send"]], ["甲", "乙"])
        self.assertEqual(resolved, ["m:8", ""])
        self.assertEqual(observed["closes"], [2])
        self.assertIsNone(manager.get("onebotv11", "g1"))


if __name__ == "__main__":
    unittest.main()
