"""控制工具与后台状态工具必须真的挂在 MCP app 上（模型看得到才谈得上可选使用）。"""
from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ["LOCALSTORE_DATA_DIR"] = str(_ROOT / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_ROOT / "cfg")
(_ROOT / "cfg").mkdir()

from tests import _bootstrap  # noqa: F401,E402
from nonebot_plugin_hermes_adapter.mcp import server  # noqa: E402


class ControlMcpRegistrationTests(unittest.TestCase):
    def _tools(self) -> set[str]:
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
                background_outbox=None,
            )
        return {tool.name for tool in asyncio.run(holder["mcp"].list_tools())}

    def test_submit_control_and_background_status_are_registered(self) -> None:
        tools = self._tools()
        self.assertIn("submit_control", tools)
        self.assertIn("get_background_tasks", tools)
        # 既有工具不受影响。
        self.assertIn("push_message", tools)


if __name__ == "__main__":
    unittest.main()
