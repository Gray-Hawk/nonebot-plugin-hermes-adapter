"""get_recent_messages 群私双路:输入契约、私聊只读范围、分页、上限与媒体计数。

私聊查询复用 MessageStore.get_recent(group_id=None, owner_user_id=...) 的既有 where
与索引,不新建表/身份映射;这里只验证工具层真的把 user_id 传到了 owner 口径,并且
群聊回归与返回结构不变。
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ["LOCALSTORE_DATA_DIR"] = str(_ROOT / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_ROOT / "cfg")
(_ROOT / "cfg").mkdir()

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

ADAPTER = "onebotv11"
SELF = "999"
OWNER = "10001"
OTHER = "10002"
GROUP = "g-1"


def _msg(
    *,
    ts: int,
    group: str | None = GROUP,
    user: str = OWNER,
    nickname: str = "主人",
    content: str = "hi",
    is_bot: bool = False,
    private_owner: str | None = None,
    images: tuple[str, ...] = (),
    stickers: tuple[str, ...] = (),
    files: tuple[dict, ...] = (),
):
    from nonebot_plugin_agent_adapter.core.message_buffer import BufferedMessage
    from nonebot_plugin_agent_adapter.core.message_origin import MessageOrigin

    return BufferedMessage(
        ts=ts,
        adapter=ADAPTER,
        group_id=group,
        user_id=user,
        nickname=nickname,
        content=content,
        origin=MessageOrigin.bot_output() if is_bot else MessageOrigin.human(),
        image_urls=list(images),
        sticker_urls=list(stickers),
        file_entries=list(files),
        is_bot=is_bot,
        private_owner_id=private_owner,
    )


class RecentMessagesHarness(unittest.TestCase):
    def setUp(self) -> None:
        from nonebot_plugin_agent_adapter.core.message_buffer import MessageBuffer
        from nonebot_plugin_agent_adapter.core.storage.message_store import MessageStore

        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = MessageStore(Path(self.tmp.name) / "m.db")
        self.addCleanup(self.store.close)
        self.buffer = MessageBuffer(store=self.store, fetcher=MagicMock())
        self.cfg = SimpleNamespace(mcp_recent_limit_max=50)

    def _input(self, **kwargs):
        from nonebot_plugin_agent_adapter.mcp.tools.get_recent_messages import (
            GetRecentMessagesInput,
        )

        kwargs.setdefault("adapter", ADAPTER)
        return GetRecentMessagesInput(**kwargs)

    def _run(self, **kwargs):
        from nonebot_plugin_agent_adapter.core.routing import CallerScope
        from nonebot_plugin_agent_adapter.mcp.tools.get_recent_messages import (
            get_recent_messages_impl,
        )

        scope = kwargs.pop("scope", CallerScope.dev())
        inp = self._input(**kwargs)
        with patch(
            "nonebot_plugin_agent_adapter.mcp.tools.get_recent_messages.load_chat_config",
            lambda: self.cfg,
        ):
            return asyncio.run(
                get_recent_messages_impl(inp, message_buffer=self.buffer, scope=scope)
            )


class GroupPathRegressionTests(RecentMessagesHarness):
    def test_group_query_unchanged_and_does_not_mix_private_rows(self) -> None:
        self.buffer.append(_msg(ts=100, content="g-early"))
        self.buffer.append(_msg(ts=200, content="g-late", images=("http://i/1",)))
        self.buffer.append(_msg(ts=300, group=None, private_owner=OWNER, content="p-owner"))
        self.buffer.append(_msg(ts=400, group=None, private_owner=OTHER, content="p-other"))

        result = self._run(group_id=GROUP, limit=10)

        self.assertIsNone(result.error)
        self.assertEqual([m.content for m in result.messages], ["g-late", "g-early"])
        self.assertEqual([m.image_count for m in result.messages], [1, 0])
        self.assertTrue(all(m.id > 0 for m in result.messages))

    def test_group_scope_still_refused_when_caller_unresolved(self) -> None:
        self.buffer.append(_msg(ts=100))
        result = self._run(group_id=GROUP, scope=None)
        self.assertEqual(result.messages, [])
        self.assertIn("not authorized", result.error or "")


class PrivatePathTests(RecentMessagesHarness):
    def test_private_reads_only_target_owner(self) -> None:
        self.buffer.append(_msg(ts=10, group=None, private_owner=OWNER, content="owner-1"))
        self.buffer.append(
            _msg(
                ts=20,
                group=None,
                user=SELF,
                nickname="bot",
                is_bot=True,
                private_owner=OWNER,
                content="owner-bot",
            )
        )
        self.buffer.append(_msg(ts=30, group=None, private_owner=OTHER, content="other-1"))

        result = self._run(user_id=OWNER, limit=10)

        self.assertIsNone(result.error)
        # bot 自己发的私聊行也归属同一个 owner,必须一并读到。
        self.assertEqual([m.content for m in result.messages], ["owner-bot", "owner-1"])
        self.assertEqual({m.content for m in result.messages}, {"owner-1", "owner-bot"})
        self.assertNotIn("other-1", [m.content for m in result.messages])

    def test_before_ts_pages_back_through_private_history(self) -> None:
        for ts in (10, 20, 30):
            self.buffer.append(
                _msg(ts=ts, group=None, private_owner=OWNER, content=f"m{ts}")
            )

        first = self._run(user_id=OWNER, limit=2)
        self.assertEqual([m.content for m in first.messages], ["m30", "m20"])

        older = [m.id for m in first.messages if m.content == "m20"][0]
        page_ts = next(m.ts for m in first.messages if m.id == older)
        second = self._run(user_id=OWNER, limit=2, before_ts=page_ts)
        self.assertEqual([m.content for m in second.messages], ["m10"])

    def test_limit_is_capped_by_config(self) -> None:
        self.cfg.mcp_recent_limit_max = 2
        for ts in (10, 20, 30, 40, 50):
            self.buffer.append(_msg(ts=ts, group=None, private_owner=OWNER, content=f"m{ts}"))

        result = self._run(user_id=OWNER, limit=100)

        self.assertEqual(len(result.messages), 2)
        self.assertEqual([m.content for m in result.messages], ["m50", "m40"])

    def test_private_media_counts_are_preserved(self) -> None:
        self.buffer.append(
            _msg(
                ts=10,
                group=None,
                private_owner=OWNER,
                content="with media",
                images=("http://i/1", "http://i/2"),
                stickers=("http://s/1",),
                files=(
                    {"file_id": "f1", "name": "a.txt", "size": 3, "url": None, "qq_message_id": None},
                ),
            )
        )

        result = self._run(user_id=OWNER, limit=10)

        view = result.messages[0]
        self.assertEqual(view.image_count, 2)
        self.assertEqual(view.sticker_count, 1)
        self.assertEqual(view.file_count, 1)

    def test_unresolved_caller_cannot_read_private(self) -> None:
        self.buffer.append(_msg(ts=10, group=None, private_owner=OWNER, content="secret"))

        result = self._run(user_id=OWNER, scope=None)

        self.assertEqual(result.messages, [])
        self.assertIn("private chat", result.error or "")
        self.assertIn("could not identify", result.error or "")


class InputContractTests(RecentMessagesHarness):
    def test_exactly_one_target_required(self) -> None:
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            self._input()
        with self.assertRaises(ValidationError):
            self._input(group_id=GROUP, user_id=OWNER)

    def test_limit_bounds(self) -> None:
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            self._input(group_id=GROUP, limit=0)
        with self.assertRaises(ValidationError):
            self._input(group_id=GROUP, limit=101)
        self.assertEqual(self._input(group_id=GROUP, limit=100).limit, 100)

    def test_mcp_tool_schema_exposes_both_targets(self) -> None:
        from nonebot_plugin_agent_adapter.mcp import server

        real_factory = server.FastMCP
        holder: dict = {}

        def capture(*args, **kwargs):
            instance = real_factory(*args, **kwargs)
            holder["mcp"] = instance
            return instance

        with patch.object(server, "FastMCP", new=capture):
            server.build_mcp_app(
                message_buffer=None,
                active_sessions=None,
                bot_registry=None,
                message_store=None,
                image_cache=None,
                background_outbox=None,
            )
        tools = {t.name: t for t in asyncio.run(holder["mcp"].list_tools())}
        schema = tools["get_recent_messages"].parameters
        props = schema["properties"]
        self.assertIn("group_id", props)
        self.assertIn("user_id", props)
        self.assertNotIn("group_id", schema.get("required", []))
        self.assertNotIn("user_id", schema.get("required", []))
        self.assertIn("adapter", schema.get("required", []))


if __name__ == "__main__":
    unittest.main()
