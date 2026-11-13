"""运行中 steer 前移到防抖之前（定案 118）：真实 handler 入口的时机与回退。

链路仍是候选的真实路径：`message.handle_message` → 真 `_maybe_steer_direct`
（资格 / § 呈递 / 台账 / Runs 分发）→ 真 `RunTracker`；只替换上游 steer HTTP、
平台目标解析与时钟。断言：

  - A 未发车且无在跑轮：B 不 steer，A/B 维持原顺序与防抖；
  - A 已在跑：B 在防抖到期前完成直注，且不调用防抖 dispatch；
  - steer 返回 False / 抛异常：继续原防抖，不吞、不重复；
  - 群非 @、跨 scope、无资格用户、bot 来源、普通首条仍走旧规则；
  - `_dispatch_chat` 不再重复直注（同一消息只在 pre-debounce 入口试一次）。
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401

from nonebot.matcher import Matcher
from nonebot_plugin_alconna import UniMessage

from nonebot_plugin_hermes_adapter import mcp as mcp_mod
from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import chat_config as chat_config_mod
from nonebot_plugin_hermes_adapter.core import steer_seen
from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager
from nonebot_plugin_hermes_adapter.core.chat_config import parse_chat_config
from nonebot_plugin_hermes_adapter.core.inflight import InflightRegistry
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker, make_scope_key
from nonebot_plugin_hermes_adapter.handlers import message
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay
from nonebot_plugin_hermes_adapter.handlers import message_turns as turns

ADAPTER = "onebotv11"
RUN_ID = "run-1"
BASE_MONO = 100.0


class _PreDebounceHarness(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        message._debounce_states.clear()
        self.addCleanup(message._debounce_states.clear)
        steer_seen._steer_seen_reset_for_test()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.inflight = InflightRegistry()
        self.sessions = ActiveSessionManager()
        self.dispatched: list[tuple[float, dict]] = []
        self.steered: list[tuple[float, str, str]] = []
        self.steer_calls: list[dict] = []
        self._msg_id = 0

    # ------------------------------------------------------------------ setup
    def add_live_run(
        self, *, private: bool, user_id: str, group_id: str | None = None,
        run_id: str = RUN_ID, bot_self_id: str = "999",
    ) -> None:
        self.tracker.upsert(
            run_id=run_id,
            scope_key=make_scope_key(ADAPTER, private, user_id, group_id),
            adapter=ADAPTER, is_private=private, user_id=user_id,
            group_id=group_id, bot_self_id=bot_self_id, session_key="sess",
            mode="reactive", expect_structured=True,
        )

    def ledger(self, run_id: str = RUN_ID) -> list[dict]:
        return self.tracker.pending_steers(run_id)

    async def _fake_steer_run(self, *, tgt, run_id, text, presented=False):
        self.steered.append((self._clock.now - BASE_MONO, run_id, text))
        return True

    # ----------------------------------------------------------------- replay
    async def replay(
        self, rows, *, private: bool = False, config=None, known_bots=(),
        steer_hook=None, prepare=None, interface: str = "runs",
    ):
        """rows: (相对秒, 发送者, 文本, addressed) 序列，首条由本调用发车。

        `steer_hook` 非空时替换 `message._maybe_steer_direct`（用于 False/异常路径），
        否则走真实直注入口。`prepare(sessions)` 在补丁就位后、发车之前调用。
        """
        cfg = parse_chat_config(config or {})
        target = SimpleNamespace(id=("g1" if not private else rows[0][1]), private=private)
        bot = SimpleNamespace(self_id="SELF")
        clock = SimpleNamespace(now=BASE_MONO)
        clock.time = lambda: clock.now
        self._clock = clock
        todo = list(rows[1:])

        async def deliver(row):
            _, sender, content, addressed = row
            self._msg_id += 1
            event = SimpleNamespace(
                message=UniMessage(content) if isinstance(content, str) else content,
                get_user_id=lambda: sender,
                is_tome=lambda: addressed,
                message_id=self._msg_id,
            )
            await message.handle_message(bot, event, Matcher)

        async def sleep(duration):
            end = clock.now + duration
            while todo and BASE_MONO + todo[0][0] <= end:
                row = todo.pop(0)
                clock.now = BASE_MONO + row[0]
                await deliver(row)
            clock.now = end

        async def dispatch(**payload):
            self.dispatched.append((clock.now - BASE_MONO, payload))

        steer = steer_hook or getattr(message, "_maybe_steer_direct", None)

        async def record_steer(**kwargs):
            self.steer_calls.append(kwargs)
            return await steer(**kwargs)

        with ExitStack() as stack:
            for entry in [
                (message, "asyncio", SimpleNamespace(get_running_loop=lambda: clock, sleep=sleep)),
                (message, "load_chat_config", lambda: cfg),
                (chat_config_mod, "load_chat_config", lambda: cfg),
                (relay, "load_chat_config", lambda: cfg),
                (message.alconna, "get_target", lambda: target),
                (UniMessage, "generate_without_reply", lambda **kw: kw["event"].message),
                (message, "get_adapter_name", lambda _: ADAPTER),
                (message, "check_isolation", lambda *_: True),
                (message, "_now_ms", lambda: int(clock.now * 1000)),
                (message, "_dispatch_chat", dispatch),
                (message, "_peek_event_db_id", lambda adapter, mid: mid),
                (message, "_maybe_steer_direct", record_steer, True),
                (message.session_manager, "get_delivery_generation", lambda *_: 0),
                (message.user_lists, "is_known_bot", lambda uid: uid in known_bots),
                (message.user_lists, "is_blacklisted", lambda *_: False),
                (mcp_mod, "message_buffer", object()),
                (mcp_mod, "active_sessions", self.sessions),
                (mcp_mod, "inflight", self.inflight),
                (mcp_mod, "run_tracker", self.tracker),
                (relay, "hermes_client", SimpleNamespace(steer_run=self._fake_steer_run)),
                (relay, "resolve_target", lambda *a, **k: object()),
                (relay, "steer_reply_push_active", lambda: False),
                (plugin_config, "hermes_api_interface", interface),
            ]:
                obj, name, value, *rest = entry
                stack.enter_context(
                    patch.object(obj, name, value, create=bool(rest and rest[0]))
                )
            if prepare is not None:
                prepare(self.sessions)
            await deliver(rows[0])
        self.assertFalse(message._debounce_states)
        return self.dispatched


class PreDebounceSteerTests(_PreDebounceHarness):
    """Runs 通路：真实 handle_message 入口 + 真实 _maybe_steer_direct。"""

    # ------------------------------------------------------------------- cases
    async def test_first_message_without_live_run_keeps_order_and_debounce(self) -> None:
        """A：A 尚未发车且无在跑轮，B 不得注入，A/B 维持原顺序与防抖。"""
        dispatched = await self.replay([
            (0, "A", "first", False),
            (1.0, "A", "second", False),
        ], private=True)
        self.assertEqual(self.steered, [])
        self.assertEqual(self.ledger(), [])
        self.assertEqual(len(self.steer_calls), 2, "两条消息都应在防抖之前尝试入口")
        self.assertEqual(len(dispatched), 1)
        at, payload = dispatched[0]
        self.assertEqual(at, 4.0, "私聊防抖顺延：B 不注入，只把首条窗口顺延到 B+3s")
        self.assertEqual([f.user_id for f in payload["fragments"]], ["A", "A"])
        self.assertEqual(payload["msg_text"], "first\nsecond")

    async def test_running_turn_steers_before_debounce_without_dispatch(self) -> None:
        """B：在跑轮存在时，B 在 3 秒防抖到期前完成直注，不调用防抖 dispatch。"""
        self.add_live_run(private=True, user_id="A")
        dispatched = await self.replay([(0, "A", "插一句 B", False)], private=True)
        self.assertEqual(dispatched, [], "直注成功后不得再走防抖 dispatch")
        self.assertEqual(len(self.steered), 1)
        at, run_id, text = self.steered[0]
        self.assertEqual(at, 0.0, "直注必须发生在防抖等待之前")
        self.assertEqual(run_id, RUN_ID)
        self.assertIn("插一句 B", text)
        ledger = self.ledger()
        self.assertEqual([(r["m_id"], r["sender_id"]) for r in ledger], [("1", "A")])

    async def test_steer_false_falls_back_to_original_debounce(self) -> None:
        """C：steer 返回 False，消息落回原防抖，不吞、不重复。"""
        self.add_live_run(private=True, user_id="A")
        calls = []

        async def refuses(**kwargs):
            calls.append(kwargs)
            return False

        dispatched = await self.replay(
            [(0, "A", "落回防抖", False)], private=True, steer_hook=refuses,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(dispatched), 1)
        at, payload = dispatched[0]
        self.assertEqual(at, 3.0)
        self.assertEqual([f.user_id for f in payload["fragments"]], ["A"])
        self.assertEqual(payload["msg_text"], "落回防抖")
        self.assertEqual(self.ledger(), [])

    async def test_steer_exception_falls_back_without_swallowing(self) -> None:
        """C：steer 抛异常，消息仍按原防抖发车一次。"""
        self.add_live_run(private=True, user_id="A")

        async def explodes(**kwargs):
            raise RuntimeError("steer backend down")

        dispatched = await self.replay(
            [(0, "A", "异常也要发", False)], private=True, steer_hook=explodes,
        )
        self.assertEqual(len(dispatched), 1)
        at, payload = dispatched[0]
        self.assertEqual(at, 3.0)
        self.assertEqual(payload["msg_text"], "异常也要发")

    async def test_group_unaddressed_message_never_steers(self) -> None:
        """D：群聊非 @ 消息即便有在跑轮也不直注，保持原防抖。"""
        self.add_live_run(private=False, user_id="A", group_id="g1")

        def arm(sessions):
            sessions.trigger(ADAPTER, "g1", "A", now_ms=int(BASE_MONO * 1000))

        dispatched = await self.replay(
            [(0, "A", "随便说说", False)], prepare=arm,
        )
        self.assertEqual(self.steered, [])
        self.assertEqual(self.ledger(), [])
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(dispatched[0][0], 3.0)
        self.assertFalse(dispatched[0][1]["is_direct"])

    async def test_group_ineligible_sender_never_steers(self) -> None:
        """D：群聊 @bot 但发送者不在资格集合时不直注，仍走原防抖。"""
        self.add_live_run(private=False, user_id="A", group_id="g1")
        dispatched = await self.replay([(0, "B", "@bot 帮个忙", True)])
        self.assertEqual(self.steered, [])
        self.assertEqual(self.ledger(), [])
        self.assertEqual(len(dispatched), 1)
        at, payload = dispatched[0]
        self.assertEqual(at, 3.0)
        self.assertEqual(payload["user_id"], "B")
        self.assertTrue(payload["is_direct"])

    async def test_private_other_scope_never_steers(self) -> None:
        """D：跨 scope（别人在跑）不直注，保持原防抖。"""
        self.add_live_run(private=True, user_id="A")
        dispatched = await self.replay([(0, "X", "另一个人", False)], private=True)
        self.assertEqual(self.steered, [])
        self.assertEqual(self.ledger(), [])
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(dispatched[0][1]["user_id"], "X")

    async def test_private_bot_source_keeps_old_private_eligibility(self) -> None:
        """D：bot 来源仍守旧规则——私聊恒资格，有在跑轮即直注。"""
        self.add_live_run(private=True, user_id="BOT")
        dispatched = await self.replay(
            [(0, "BOT", "自动跟帖", False)], private=True, known_bots={"BOT"},
        )
        self.assertEqual(dispatched, [])
        self.assertEqual(len(self.steered), 1)
        self.assertIn("自动跟帖", self.steered[0][2])

    async def test_dispatch_chat_no_longer_injects_steer(self) -> None:
        """E：`_dispatch_chat` 不再重复直注，同一消息只在 pre-debounce 入口试一次。"""
        self.add_live_run(private=True, user_id="A")
        passive_calls = []

        async def passive(**kwargs):
            passive_calls.append(kwargs)

        with ExitStack() as stack:
            for obj, name, value in [
                (turns, "_handle_passive_path", passive),
                (relay, "hermes_client", SimpleNamespace(steer_run=self._fake_steer_run)),
                (relay, "resolve_target", lambda *a, **k: object()),
                (mcp_mod, "run_tracker", self.tracker),
                (mcp_mod, "inflight", self.inflight),
                (mcp_mod, "active_sessions", self.sessions),
                (plugin_config, "hermes_api_interface", "runs"),
            ]:
                stack.enter_context(patch.object(obj, name, value))
            await turns._dispatch_chat(
                bot=SimpleNamespace(self_id="SELF"),
                event=SimpleNamespace(message_id=2),
                target=SimpleNamespace(id="A", private=True),
                adapter_name=ADAPTER, user_id="A", nickname="A", group_id=None,
                msg_text="刚到的跟帖", image_urls=[], sticker_urls=[],
                is_direct=True, addressed_to_bot=False,
                now_ms=int(BASE_MONO * 1000), delivery_generation=0,
                fragments=[],
            )
        self.assertEqual(self.steered, [], "dispatch 阶段不得再直注")
        self.assertEqual(self.ledger(), [])
        self.assertEqual(len(passive_calls), 1, "未直注时必须继续正常 dispatch")


if __name__ == "__main__":
    unittest.main()
