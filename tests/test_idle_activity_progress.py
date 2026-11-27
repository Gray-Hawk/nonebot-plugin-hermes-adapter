"""正常 run 的活动进展：事件刷新、全局限频合批、保活不登记。"""
from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401

from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult, RunsEventParser
from nonebot_plugin_hermes_adapter.core.session import SessionManager
from nonebot_plugin_hermes_adapter.core.storage.session_key_store import SessionKeyStore
from nonebot_plugin_hermes_adapter.handlers import message_turns


class ActivityEventTests(unittest.TestCase):
    def test_parser_reports_tool_and_nonempty_text_progress(self):
        parser = RunsEventParser()
        seen: list[str] = []
        parser.on_activity = lambda: seen.append("progress")

        for payload in (
            {"event": "message.delta", "delta": "甲"},
            {"event": "message.delta", "delta": "   "},
            {"event": "message.interim", "text": "乙"},
            {"event": "tool.started", "tool": "lookup"},
            {"event": "tool.completed", "tool": "lookup"},
            {"event": "hermes.tool.progress", "label": "lookup"},
            {"event": "reasoning.delta", "delta": "思考"},
            {"event": "run.completed", "output": "丙"},
        ):
            parser.feed("data: " + json.dumps(payload, ensure_ascii=False))

        self.assertEqual(len(seen), 6, "非空正文/思考与工具事件算进展，空白 delta 和终态不重复算")

    def test_empty_separator_envelope_is_not_activity(self):
        parser = RunsEventParser()
        seen: list[str] = []
        parser.on_activity = lambda: seen.append("progress")

        for payload in (
            {"event": "message.delta", "delta": "§\n"},
            {"event": "message.delta", "delta": "§"},
            {"event": "message.interim", "text": "§\n§"},
            {"event": "reasoning.delta", "delta": "§"},
            {"event": "message.delta", "delta": "\u200b"},
            {"event": "run.completed", "output": "§\n§"},
        ):
            parser.feed("data: " + json.dumps(payload, ensure_ascii=False))

        self.assertEqual(seen, [])


class ActivityWriteCoalescingTests(unittest.TestCase):
    def setUp(self):
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = SessionKeyStore(Path(self._tmp.name) / "session_keys.db")
        self.addCleanup(self.store.close)
        self.manager = SessionManager()
        self.manager.bind_store(self.store)

    def test_progress_writes_are_globally_coalesced(self):
        for index in range(20):
            self.manager.get_session_key("qq", False, "", str(1000 + index))
        with patch.object(
            self.store, "set_idle_states_batch", wraps=self.store.set_idle_states_batch
        ) as batch, patch(
            "nonebot_plugin_hermes_adapter.core.session.time.monotonic", return_value=1000.5
        ):
            self.manager._activity_last_flush = 1000.0
            for index in range(20):
                self.manager.mark_activity(
                    "qq", False, "", str(1000 + index), now=1001.0 + index, force=False
                )
            batch.assert_not_called()
            self.manager.mark_activity("qq", False, "", "1000", now=1100.0, force=True)
            batch.assert_called_once()

        states = self.store.load_idle_states()
        self.assertEqual(len([v for v in states.values() if v[1]]), 20)
        self.assertEqual(states["qq+group+1000"][0], 1100.0)
        self.assertEqual(states["qq+group+1019"][0], 1020.0)


class ActivityWrapperTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_progress_and_finish_use_distinct_write_modes(self):
        observed = []

        async def fake_chat(**kwargs):
            observed.append(kwargs)
            kwargs["activity_callback"]()
            return ChatResult(raw_text="§\n§")

        kwargs = dict(
            text="hello",
            session_key="hermes-qq+group+1",
            user_id="",
            group_id="1",
            adapter_name="qq",
            is_private=False,
        )
        with patch.object(message_turns.hermes_client, "chat", side_effect=fake_chat), patch.object(
            message_turns.session_manager, "mark_activity"
        ) as mark:
            await message_turns._chat_and_adopt_rotation(**kwargs)

        self.assertEqual(len(observed), 1)
        self.assertTrue(callable(observed[0]["activity_callback"]))
        self.assertEqual(mark.call_count, 3)
        self.assertEqual([call.kwargs.get("force", True) for call in mark.call_args_list], [True, False, True])

    async def test_stale_generation_cancellation_disarms_idle_instead_of_rearming(self):
        async def fake_chat(**kwargs):
            result = ChatResult(raw_text="")
            result.is_stale_generation = True
            return result

        kwargs = dict(
            text="@bot",
            session_key="hermes-qq+group+1-g23",
            user_id="",
            group_id="1",
            adapter_name="qq",
            is_private=False,
        )
        with patch.object(message_turns.hermes_client, "chat", side_effect=fake_chat), patch.object(
            message_turns.session_manager, "mark_activity"
        ) as mark, patch.object(message_turns.session_manager, "disarm_idle") as disarm:
            await message_turns._chat_and_adopt_rotation(**kwargs)

        self.assertEqual(mark.call_count, 1, "只有入场登记；取消收尾不得再次武装保活")
        disarm.assert_called_once_with("qq", False, "", "1")


if __name__ == "__main__":
    unittest.main()
