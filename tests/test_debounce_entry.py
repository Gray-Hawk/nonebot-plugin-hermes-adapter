"""Replay the real ingress/merge/deadline loop without platform or Runs I/O."""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from nonebot.exception import SkippedException
from nonebot.matcher import Matcher
from nonebot_plugin_alconna import Image, UniMessage

from nonebot_plugin_hermes_adapter.core import chat_config
from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager
from nonebot_plugin_hermes_adapter.core.chat_config import parse_chat_config
from nonebot_plugin_hermes_adapter.handlers import message


class DebounceEntryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        message._debounce_states.clear()
        self.addCleanup(message._debounce_states.clear)

    async def replay(self, rows, *, private=False, config=None, known_bots=()):
        """Rows are (relative seconds, sender, text/UniMessage, addressed)."""
        cfg = parse_chat_config(config or {})
        target = SimpleNamespace(id="g1" if not private else rows[0][1], private=private)
        bot = SimpleNamespace(self_id="SELF")
        sessions = ActiveSessionManager()
        clock = SimpleNamespace(now=100.0)
        clock.time = lambda: clock.now
        todo = list(rows[1:])
        sent = []

        async def deliver(row):
            _, sender, content, addressed = row
            event = SimpleNamespace(
                message=UniMessage(content) if isinstance(content, str) else content,
                get_user_id=lambda: sender,
                is_tome=lambda: addressed,
            )
            try:
                await message.handle_message(bot, event, Matcher)
            except SkippedException:
                pass

        async def sleep(duration):
            end = clock.now + duration
            while todo and 100.0 + todo[0][0] <= end:
                row = todo.pop(0)
                clock.now = 100.0 + row[0]
                await deliver(row)
            clock.now = end

        async def dispatch(**payload):
            sent.append((clock.now - 100.0, payload))

        with ExitStack() as stack:
            for obj, name, value in [
                (message, "asyncio", SimpleNamespace(get_running_loop=lambda: clock, sleep=sleep)),
                (message, "load_chat_config", lambda: cfg),
                (chat_config, "load_chat_config", lambda: cfg),
                (message.alconna, "get_target", lambda: target),
                (UniMessage, "generate_without_reply", lambda **kw: kw["event"].message),
                (message, "get_adapter_name", lambda _: "onebotv11"),
                (message, "check_isolation", lambda *_: True),
                (message, "_now_ms", lambda: int(clock.now * 1000)),
                (message, "_dispatch_chat", dispatch),
                (message._mcp, "message_buffer", object()),
                (message._mcp, "active_sessions", sessions),
                (message.user_lists, "is_known_bot", lambda uid: uid in known_bots),
                (message.user_lists, "is_blacklisted", lambda *_: False),
                (message.session_manager, "get_delivery_generation", lambda *_: 0),
            ]:
                stack.enter_context(patch.object(obj, name, value))
            await deliver(rows[0])
        self.assertFalse(message._debounce_states)
        self.assertEqual(len(sent), 1)
        return sent[0]

    async def test_unified_window_controls_group_duration(self):
        for config, expected in (({}, 3.0), ({"reply_debounce_sec": 3.0}, 3.0),
                                 ({"reply_debounce_sec": 4}, 4.0)):
            with self.subTest(config=config):
                at, _ = await self.replay([(0, "A", "/ai first", False)], config=config)
                self.assertEqual(at, expected)

    async def test_unified_window_controls_private_duration(self):
        for config, expected in (({}, 3.0), ({"reply_debounce_sec": 3.0}, 3.0),
                                 ({"reply_debounce_sec": 4}, 4.0)):
            with self.subTest(config=config):
                at, _ = await self.replay([(0, "A", "first", False)], private=True, config=config)
                self.assertEqual(at, expected)

    async def test_private_sliding_wait_is_capped_at_three_unified_windows(self):
        for duration in (2.0, 3.0, 4.0, 6.0):
            with self.subTest(duration=duration):
                at, payload = await self.replay([
                    (0, "A", "first", False),
                    (duration - 0.5, "A", "second", False),
                    (2 * duration - 1, "A", "third", False),
                    (3 * duration - 1.5, "A", "fourth", False),
                    (3 * duration - 0.1, "A", "last", False),
                ], private=True, config={"reply_debounce_sec": duration})
                self.assertEqual(at, duration * 3)
                self.assertEqual(len(payload["fragments"]), 5)

    async def test_zero_bypasses_ordinary_debounce_in_both_scopes(self):
        for private in (False, True):
            with self.subTest(private=private), patch.object(
                message, "_debounce_arrive", wraps=message._debounce_arrive,
            ) as arrive:
                at, _ = await self.replay(
                    [(0, "A", "/ai first", False)], private=private,
                    config={"reply_debounce_sec": 0},
                )
                self.assertEqual(at, 0)
                arrive.assert_not_called()

    async def test_plain_other_human_interrupts_keyword_first_window(self):
        at, payload = await self.replay([
            (0, "A", "/ai first", False),
            (2, "B", "ordinary interruption", False),
        ])
        self.assertEqual(at, 2.0)
        self.assertEqual(payload["user_id"], "B")
        self.assertEqual([f.user_id for f in payload["fragments"]], ["A", "B"])
        self.assertTrue(payload["ephemeral"])

    async def test_default_group_waits_three_seconds(self):
        at, _ = await self.replay([(0, "A", "/ai first", False)])
        self.assertEqual(at, 3.0)

    async def test_group_ignores_legacy_window_and_max_wait(self):
        at, _ = await self.replay(
            [(0, "A", "/ai first", False)],
            config={"reply_debounce_group_sec": 90, "reply_debounce_max_wait_sec": 1},
        )
        self.assertEqual(at, 3.0)

    async def test_plain_same_human_joins_without_extending_keyword_window(self):
        at, payload = await self.replay([
            (0, "A", "/ai first", False),
            (2, "A", "second", False),
            (2.9, "A", "third", False),
        ])
        self.assertEqual(at, 3.0)
        self.assertEqual(payload["msg_text"], "/ai first\nsecond\nthird")
        self.assertEqual(len(payload["fragments"]), 3)

    async def test_known_bot_cannot_interrupt_or_extend_human_window(self):
        at, _ = await self.replay([
            (0, "A", "/ai first", False),
            (2, "BOT", "bot response", False),
            (3, "A", "same human resumes", False),
        ], known_bots={"BOT"}, config={"reply_debounce_sec": 4, "reply_delay_for_bot_sec": 30})
        self.assertEqual(at, 4.0)

    async def test_self_event_is_ignored_in_window(self):
        at, payload = await self.replay([
            (0, "A", "/ai first", False),
            (2, "SELF", "self echo", True),
        ], config={"reply_debounce_sec": 4})
        self.assertEqual(at, 4.0)
        self.assertEqual(payload["user_id"], "A")
        self.assertEqual(len(payload["fragments"]), 1)

    async def test_media_only_other_human_fires_without_replacing_text(self):
        at, payload = await self.replay([
            (0, "A", "/ai first", False),
            (2, "B", UniMessage(Image(url="https://example.invalid/image.png")), False),
        ])
        self.assertEqual(at, 2.0)
        self.assertEqual(payload["user_id"], "A")
        self.assertEqual(payload["msg_text"], "/ai first")

    async def test_sticker_only_other_human_fires_without_replacing_text(self):
        at, payload = await self.replay([
            (0, "A", "/ai first", False),
            (2, "B", UniMessage(Image(url="https://example.invalid/sticker.png", sticker=True)), False),
        ])
        self.assertEqual(at, 2.0)
        self.assertEqual(payload["user_id"], "A")
        self.assertEqual(payload["msg_text"], "/ai first")

    async def test_other_human_deadline_is_observed_within_half_second(self):
        at, _ = await self.replay([
            (0, "A", "direct first", True),
            (2.25, "B", "interrupt", False),
        ])
        self.assertEqual(at, 2.5)

    async def test_active_direct_window_keeps_unbounded_trigger_context(self):
        _, payload = await self.replay([
            (0, "A", "direct first", True),
            (2, "B", "interrupt", False),
        ])
        self.assertIsNone(payload["context_limit"])

    async def test_direct_takeover_does_not_inherit_keyword_only_context(self):
        _, payload = await self.replay([
            (0, "A", "/ai first", False),
            (2, "B", "direct takeover", True),
        ])
        self.assertTrue(payload["is_direct"])
        self.assertFalse(payload["ephemeral"])
        self.assertIsNone(payload["context_limit"])

    async def test_bot_first_keeps_forced_delay_after_first_human(self):
        for delay in (10, 30):
            with self.subTest(delay=delay):
                at, _ = await self.replay([
                    (0, "BOT", "/ai first", False),
                    (2, "A", "first human", False),
                    (delay - 1, "BOT2", "later bot", False),
                ], known_bots={"BOT", "BOT2"}, config={"reply_delay_for_bot_sec": delay})
                self.assertEqual(at, delay)

    async def test_second_real_human_can_interrupt_bot_first_window(self):
        at, _ = await self.replay([
            (0, "BOT", "/ai first", False),
            (2, "A", "first human", False),
            (3, "B", "second human", False),
        ], known_bots={"BOT"}, config={"reply_delay_for_bot_sec": 30})
        self.assertEqual(at, 3.0)

    async def test_private_slides_by_the_shared_three_seconds(self):
        at, payload = await self.replay([
            (0, "A", "first", False),
            (2, "A", "second", False),
            (4, "A", "third", False),
        ], private=True)
        self.assertEqual(at, 7.0)
        self.assertEqual(len(payload["fragments"]), 3)

    async def test_default_private_sliding_wait_stops_at_nine_seconds(self):
        at, payload = await self.replay([
            (0, "A", "first", False),
            (2.5, "A", "second", False),
            (5, "A", "third", False),
            (7.5, "A", "fourth", False),
            (8.9, "A", "last", False),
        ], private=True)
        self.assertEqual(at, 9.0)
        self.assertEqual(len(payload["fragments"]), 5)

    async def test_private_zero_still_dispatches_immediately(self):
        at, _ = await self.replay(
            [(0, "A", "first", False)], private=True,
            config={"reply_debounce_sec": 0},
        )
        self.assertEqual(at, 0)

    async def test_bot_delay_preserves_independent_scope_specific_waits(self):
        for private in (False, True):
            for ordinary in (0, 3, 6):
                for delay in (10, 12, 30):
                    for later_bot in (False, True):
                        with self.subTest(private=private, ordinary=ordinary, delay=delay, later_bot=later_bot):
                            rows = [(0, "BOT", "/ai first", False)]
                            if later_bot:
                                rows.append((delay - 2, "BOT", "later bot", False))
                            at, _ = await self.replay(
                                rows, private=private, known_bots={"BOT"},
                                config={"reply_debounce_sec": ordinary, "reply_delay_for_bot_sec": delay},
                            )
                            # 专用延迟沿用旧语义：群聊固定；私聊顺延且仅普通防抖开启时有硬上限。
                            expected = delay
                            if private and later_bot:
                                expected = 2 * delay - 2
                                if ordinary:
                                    expected = min(expected, max(ordinary * 3, delay))
                            self.assertEqual(at, expected)


if __name__ == "__main__":
    unittest.main()
