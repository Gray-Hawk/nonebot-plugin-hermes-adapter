from __future__ import annotations

import os
import tempfile
import unittest
import asyncio
from pathlib import Path
from unittest.mock import patch


_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ["LOCALSTORE_DATA_DIR"] = str(_ROOT / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_ROOT / "cfg")
(_ROOT / "cfg").mkdir()

from tests import _bootstrap  # noqa: F401,E402
from nonebot_plugin_hermes_adapter.mcp import server  # noqa: E402


class StickerMcpRegistrationTests(unittest.TestCase):
    def test_search_and_preview_are_registered(self) -> None:
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
        mcp = holder["mcp"]
        self.assertIsNotNone(asyncio.run(mcp.get_tool("search_sticker_library")))
        self.assertIsNotNone(asyncio.run(mcp.get_tool("preview_sticker")))


if __name__ == "__main__":
    unittest.main()
