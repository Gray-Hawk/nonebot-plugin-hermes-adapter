"""MCP 注册：poke_user 是独立工具（群号可选，被戳 QQ 必填）。

定案：poke 必须且只能独立使用。它出现在工具面里，正文里的任何 poke 标记都不再
触发动作（见 tests/test_outbound_mention.py）。
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ.setdefault("LOCALSTORE_DATA_DIR", str(_ROOT / "data"))
os.environ.setdefault("AGENT_PLUGIN_CUSTOM_DIR", str(_ROOT / "cfg"))
(_ROOT / "cfg").mkdir(parents=True, exist_ok=True)

from tests import _bootstrap  # noqa: F401,E402

from nonebot_plugin_hermes_adapter.mcp import server  # noqa: E402


def _build():
    real_factory = server.FastMCP
    holder = {}

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
        )
    return holder["mcp"]


class PokeToolRegistrationTests(unittest.TestCase):
    def test_poke_user_is_registered_with_group_and_private_params(self) -> None:
        mcp = _build()
        tool = asyncio.run(mcp.get_tool("poke_user"))
        self.assertIsNotNone(tool)
        schema = tool.parameters
        self.assertEqual(set(schema.get("required", [])), {"adapter", "user_id"})
        self.assertIn("group_id", schema.get("properties", {}))

    def test_poke_is_not_a_text_marker(self) -> None:
        mcp = _build()
        push = asyncio.run(mcp.get_tool("push_message"))
        description = str(push.description or "") + str(push.parameters or "")
        self.assertNotIn("[CQ:poke", description)
        self.assertIn("poke_user", description)


if __name__ == "__main__":
    unittest.main()
