"""get_message_images 接口回归:adapter/group_id 是与 get_recent_messages 同形的目标参数。

字段不能是"可选防御性过滤"却没人读:先按 message_ids 反查 DB owners + CallerScope 判权,
再按调用方给的 adapter/group_id 做一致性校验;不匹配与越权同口径按 not_found。
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ["LOCALSTORE_DATA_DIR"] = str(_ROOT / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_ROOT / "cfg")
(_ROOT / "cfg").mkdir()

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401


class _Store:
    def __init__(self, owners: dict[int, tuple[str, str | None]]) -> None:
        self._owners = owners

    def get_message_owners(self, ids):
        return {mid: self._owners[mid] for mid in ids if mid in self._owners}

    def get_message_images_meta(self, ids):
        # sha256=None → cache_miss,足以区分「未被拒」而不必造图字节。
        return {
            mid: [{"idx": 0, "kind": "image", "sha256": None, "mime_type": "image/jpeg"}]
            for mid in ids
            if mid in self._owners
        }


class _Cache:
    def get_bytes(self, sha):  # pragma: no cover - sha 恒为 None
        return None


class GetMessageImagesContractTests(unittest.TestCase):
    def _impl(self, owners, *, scope, adapter=None, group_id=None):
        from nonebot_plugin_hermes_adapter.mcp.tools.get_message_images import (
            GetMessageImagesInput,
            get_message_images_impl,
        )

        inp = GetMessageImagesInput(
            message_ids=list(owners), adapter=adapter, group_id=group_id
        )
        with patch(
            "nonebot_plugin_hermes_adapter.mcp.tools.get_message_images.load_media_config",
            lambda: SimpleNamespace(),
        ):
            return asyncio.run(
                get_message_images_impl(
                    inp, store=_Store(owners), cache=_Cache(), scope=scope
                )
            )

    def _reasons(self, blocks) -> list[str]:
        import json

        header = blocks[0].text
        return [r["reason"] for r in json.loads(header)["results"]]

    def test_schema_matches_get_recent_messages_target_shape(self) -> None:
        from nonebot_plugin_hermes_adapter.mcp import server

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
        schema = tools["get_message_images"].parameters
        self.assertEqual(
            set(schema["properties"]), {"message_ids", "idx", "adapter", "group_id"}
        )
        self.assertEqual(schema.get("required"), ["message_ids"])
        self.assertNotIn("user_id", schema["properties"])

    def test_group_and_private_fetch_pass_consistency_check(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope

        owners = {1: ("ob11", "g-1"), 2: ("ob11", None)}

        group_blocks = self._impl(
            owners, scope=CallerScope.dev(), adapter="ob11", group_id="g-1"
        )
        self.assertEqual(self._reasons(group_blocks)[0], "cache_miss")

        # 群聊省略 group_id 也放行(只有给了才校验)。
        group_blocks = self._impl(owners, scope=CallerScope.dev(), adapter="ob11")
        self.assertEqual(self._reasons(group_blocks)[0], "cache_miss")

        # 私聊按现规则省略 group_id。
        private_blocks = self._impl(
            {2: owners[2]}, scope=CallerScope.dev(), adapter="ob11"
        )
        self.assertEqual(self._reasons(private_blocks), ["cache_miss"])

    def test_adapter_or_group_mismatch_is_not_found(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope

        owners = {1: ("ob11", "g-1"), 2: ("ob11", None)}

        wrong_adapter = self._impl(
            {1: owners[1]}, scope=CallerScope.dev(), adapter="snowluma", group_id="g-1"
        )
        self.assertEqual(self._reasons(wrong_adapter), ["not_found"])

        wrong_group = self._impl(
            {1: owners[1]}, scope=CallerScope.dev(), adapter="ob11", group_id="g-2"
        )
        self.assertEqual(self._reasons(wrong_group), ["not_found"])

        # 私聊 id 没有群号:给了 group_id 即不一致。
        private_with_group = self._impl(
            {2: owners[2]}, scope=CallerScope.dev(), adapter="ob11", group_id="g-1"
        )
        self.assertEqual(self._reasons(private_with_group), ["not_found"])

    def test_no_target_params_still_authorizes_by_stored_owner(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope

        blocks = self._impl({1: ("ob11", "g-1")}, scope=CallerScope.dev())
        self.assertEqual(self._reasons(blocks), ["cache_miss"])

    def test_unresolved_caller_gets_not_found_for_group_and_private_ids(self) -> None:
        owners = {1: ("ob11", "g-1"), 2: ("ob11", None)}
        blocks = self._impl(owners, scope=None, adapter="ob11")

        self.assertEqual(self._reasons(blocks), ["not_found", "not_found"])


if __name__ == "__main__":
    unittest.main()
