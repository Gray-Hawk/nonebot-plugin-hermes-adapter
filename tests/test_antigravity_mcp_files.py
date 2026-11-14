"""阶段2C3：`get_message_files` 作为第三个 ACP MCP 工具，与原生 `read_file` 的权限边界。

链路与 2C1 同源：真 `build_mcp_app` / 真 FastMCP 注册表与线上 wire / 真 `CallerScope` /
真 `AntigravityACPClient` / 真 `HermesClient.chat` / 真 handler / 真 `core.approvals`；
只把官方 `agy acp` 换成 NDJSON 夹具子进程、平台发送换成进程内替身。

卡面口径（计划 122）：
- 白名单恰三个只读工具（顺序保留原两个，第三个是 get_message_files）；默认全表不变；
- 第三工具的 schema／wire 与全表同一份定义，模型看得到 refs 契约与 local_path 交付；
- get_message_files 的真实 `mcp_` 权限请求自动 allow_once、不登记人工单、不发通知；
- 原生 read_file／run_command 仍走人工（对立面：不因 MCP 自动放行而扩大范围）；
- 文件工具的群／私 scope 与越权拒绝保持既有语义（未解析调用方 → not_found，不下载）。

注：`DUMMY_TOKEN` 是本文件自造的离线占位值，不是任何真实凭据；权限帧的 optionId
`allow`／`deny` 取自 2026-11-13 真实 agy 1.5.8 抓包（`kind=allow_once`／`reject_once`），
用于证明自动放行按 kind 选取、不靠猜 id 字面量。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ["LOCALSTORE_DATA_DIR"] = str(_ROOT / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_ROOT / "cfg")
(_ROOT / "cfg").mkdir()

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from tests.acp_2b_harness import (  # noqa: E402
    ACPHandlerHarness,
    permission_frames as _permission_frames,
    wait_until as _wait_until,
)

# 独立字面量：这是卡面定案的「恰三个工具」，不从产品常量反向推断。
EXPECTED_AG_TOOL_NAMES = ("get_recent_messages", "get_message_images", "get_message_files")

# 默认（未过滤）工具表必须仍然包含的既有工具（sticker 两个受特性开关控制，单独判）。
BASE_ALL_TOOL_NAMES = {
    "push_message",
    "recall_message",
    "list_active_sessions",
    "get_recent_messages",
    "get_message_images",
    "get_message_files",
    "browse_group_files",
    "download_group_file",
    "wake_turn",
    "submit_control",
    "request_continuation",
    "get_background_tasks",
}
DUMMY_TOKEN = "offline-dummy-token-not-a-real-credential"
MCP_HOST = "127.0.0.1"
MCP_PORT = 18878
GROUP = "100"
PRIVATE_USER = "123"


# --- 夹具 ------------------------------------------------------------------


def _plugin_config_override(testcase: unittest.TestCase, **overrides) -> None:
    from nonebot_plugin_hermes_adapter.config import plugin_config

    saved = {name: getattr(plugin_config, name) for name in overrides}
    for name, value in overrides.items():
        object.__setattr__(plugin_config, name, value)

    def restore() -> None:
        for name, value in saved.items():
            object.__setattr__(plugin_config, name, value)

    testcase.addCleanup(restore)


def mcp_config(testcase: unittest.TestCase, *, enabled: bool = True) -> None:
    _plugin_config_override(
        testcase,
        hermes_mcp_enabled=enabled,
        hermes_mcp_host=MCP_HOST,
        hermes_mcp_port=MCP_PORT,
        hermes_api_key=DUMMY_TOKEN,
    )


def build_app(**kwargs):
    """真 `build_mcp_app`，只截获 FastMCP 实例以便读注册表／当 in-process server 用。"""
    from nonebot_plugin_hermes_adapter.mcp import server

    real_factory = server.FastMCP
    holder: dict = {}

    def capture(*args, **inner):
        instance = real_factory(*args, **inner)
        holder["mcp"] = instance
        return instance

    with patch.object(server, "FastMCP", new=capture):
        app = server.build_mcp_app(
            message_buffer=None,
            active_sessions=None,
            bot_registry=None,
            message_store=kwargs.pop("message_store", None),
            image_cache=None,
            background_outbox=None,
            **kwargs,
        )
    return holder["mcp"], app


def registered_tools(**kwargs) -> dict:
    mcp, _ = build_app(**kwargs)
    return {tool.name: tool for tool in asyncio.run(mcp.list_tools())}


def product_tool_names() -> tuple[str, ...]:
    from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
        ANTIGRAVITY_MCP_TOOL_NAMES,
    )

    return ANTIGRAVITY_MCP_TOOL_NAMES


# --- 1. 工具表：恰三个、顺序、默认全表不变 ---------------------------------


class AntigravityToolWhitelistTests(unittest.TestCase):
    def test_product_whitelist_is_exactly_the_three_read_only_tools_in_order(self) -> None:
        self.assertEqual(product_tool_names(), EXPECTED_AG_TOOL_NAMES)

    def test_registration_with_the_product_whitelist_exposes_exactly_three(self) -> None:
        tools = registered_tools(tool_names=product_tool_names())
        self.assertEqual(tuple(tools), EXPECTED_AG_TOOL_NAMES, "注册顺序必须保留原两个在前")

    def test_default_table_is_unfiltered_and_still_has_the_third_tool(self) -> None:
        full = registered_tools()
        missing = BASE_ALL_TOOL_NAMES - set(full)
        self.assertEqual(missing, set(), f"默认工具表少了 {missing}")
        # 默认表是完整表，不是被裁剪的三工具表。
        self.assertGreater(len(full), len(EXPECTED_AG_TOOL_NAMES))
        for name in EXPECTED_AG_TOOL_NAMES:
            self.assertIn(name, full)

    def test_registered_tool_descriptions_do_not_name_the_upstream_backend(self) -> None:
        """工具说明会被外部 Agent 原样读进上下文：只允许中性措辞。"""
        tools = registered_tools(tool_names=product_tool_names())
        self.assertEqual(tuple(tools), EXPECTED_AG_TOOL_NAMES)
        for name, tool in tools.items():
            description = tool.description or ""
            self.assertTrue(description.strip(), f"{name} 的 description 为空")
            self.assertNotIn("hermes", description.lower(), f"{name}: {description}")

    def test_third_tool_definition_is_identical_in_filtered_and_default_tables(self) -> None:
        """过滤只改注册集合，不改写第三个工具的描述与 schema。"""
        full = registered_tools()
        filtered = registered_tools(tool_names=product_tool_names())
        self.assertEqual(
            filtered["get_message_files"].description, full["get_message_files"].description
        )
        self.assertEqual(
            filtered["get_message_files"].parameters, full["get_message_files"].parameters
        )
        self.assertEqual(
            filtered["get_message_files"].output_schema,
            full["get_message_files"].output_schema,
        )


# --- 2. 第三工具的 schema 与真实 wire --------------------------------------


class GetMessageFilesWireTests(unittest.TestCase):
    def test_schema_requires_refs_of_message_id_and_idx(self) -> None:
        tools = registered_tools(tool_names=product_tool_names())
        schema = tools["get_message_files"].parameters

        self.assertEqual(set(schema["properties"]), {"refs"})
        self.assertEqual(schema["required"], ["refs"])
        self.assertEqual(schema["properties"]["refs"]["type"], "array")
        item = schema["properties"]["refs"]["items"]
        self.assertEqual(item["type"], "object")
        self.assertEqual(set(item["properties"]), {"message_id", "idx"})
        self.assertEqual(item["required"], ["message_id", "idx"])
        self.assertEqual(item["properties"]["message_id"]["type"], "integer")
        self.assertEqual(item["properties"]["idx"]["type"], "integer")
        self.assertEqual(item["properties"]["idx"]["minimum"], 0)

    def test_description_tells_the_model_it_returns_local_paths(self) -> None:
        description = registered_tools(tool_names=product_tool_names())[
            "get_message_files"
        ].description or ""
        self.assertIn("local paths", description)
        self.assertIn("max 4 refs", description)
        self.assertIn("message_id, idx", description)

    def test_recent_messages_output_schema_points_at_the_file_tool(self) -> None:
        """模型能从消息元数据发现第三个工具（file_count 说明带工具名）。"""
        tools = registered_tools(tool_names=product_tool_names())
        output = json.dumps(tools["get_recent_messages"].output_schema, ensure_ascii=False)
        self.assertIn("file_count", output)
        self.assertIn("get_message_files", output)

    def test_wire_serves_the_third_tool_and_rejects_five_refs(self) -> None:
        """真 FastMCP 客户端（线上 wire）：工具可列、可调，参数越界在服务端拒绝。"""
        from fastmcp import Client

        class _Store:
            def get_message_targets(self, ids):
                return {mid: ("onebotv11", GROUP, PRIVATE_USER) for mid in ids}

            def get_message_files(self, ids):
                return {mid: [] for mid in ids}

            def update_file_failed(self, *args, **kwargs):  # pragma: no cover - 无 url
                return None

        mcp, _ = build_app(tool_names=product_tool_names(), message_store=_Store())

        async def read(args):
            async with Client(mcp) as client:
                names = [tool.name for tool in await client.list_tools()]
                return names, await client.call_tool("get_message_files", args)

        names, result = asyncio.run(read({"refs": [{"message_id": 1, "idx": 0}]}))
        self.assertEqual(tuple(names), EXPECTED_AG_TOOL_NAMES)
        # 本进程没有 HTTP 调用方 → scope 认不出 → 安全拒绝，绝不返回路径。
        payload = result.structured_content["results"][0]
        self.assertFalse(payload["available"])
        self.assertEqual(payload["reason"], "not_found")
        self.assertIsNone(payload["local_path"])

        async def call_five():
            async with Client(mcp) as client:
                await client.call_tool(
                    "get_message_files",
                    {"refs": [{"message_id": i, "idx": 0} for i in range(5)]},
                )

        with self.assertRaises(Exception) as ctx:
            asyncio.run(call_five())
        self.assertIn("parameters", str(ctx.exception))


# --- 3. 权限：MCP 自动放行 vs 原生 read_file／run_command 人工 -------------


class GetMessageFilesPermissionTests(ACPHandlerHarness):
    MCP_TOOL = "mcp_nonebot-bridge_get_message_files"
    # 真实 agy 1.5.8 抓包的 optionId：kind 才是判定依据，id 不是。
    REAL_OPTIONS = (
        '[{"optionId": "allow", "name": "Allow", "kind": "allow_once"},'
        ' {"optionId": "deny", "name": "Deny", "kind": "reject_once"}]'
    )

    def _permission_flags(
        self, *, request_id: str, name: str, title: str, raw: dict
    ) -> tuple[str, ...]:
        return (
            "--permission",
            f"--permission-id={request_id}",
            f"--permission-name={name}",
            f"--permission-title={title}",
            f"--permission-raw={json.dumps(raw)}",
            f"--permission-options={self.REAL_OPTIONS}",
        )

    def _client_permissions(self) -> dict:
        self.assertEqual(len(self.clients), 1, "本用例只允许一个 ACP 客户端")
        return self.clients[0]._permissions

    async def test_get_message_files_permission_is_auto_allowed_once_without_pending(self) -> None:
        mcp_config(self)
        self._client(
            reply="§\n文件已读\n§",
            flags=self._permission_flags(
                request_id="mcp-f1",
                name=self.MCP_TOOL,
                title=f"Run {self.MCP_TOOL}",
                raw={"refs": [{"message_id": 1, "idx": 0}]},
            ),
        )
        result = await asyncio.wait_for(self._turn(), 10)

        self.assertFalse(result.is_transport_error)
        self.assertIn("文件已读", self.platform)
        self.assertEqual(self.approvals.pending_count(), 0, "MCP 不得登记人工审批")
        self.assertEqual(self.admin_notices(), [], "MCP 不得发人工通知")
        self.assertEqual(self._client_permissions(), {}, "MCP 不得留挂起条目")
        frames = _permission_frames(self.wire(), "mcp-f1")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(
            frames[0]["result"]["outcome"], {"outcome": "selected", "optionId": "allow"}
        )

    async def test_read_file_permission_still_waits_for_the_admin(self) -> None:
        mcp_config(self)
        path = str(self.base / "outside-cwd.txt")
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=self._permission_flags(
                request_id="read-1",
                name="read_file",
                title="File ../outside-cwd.txt",
                raw={"Path": path},
            ),
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(
            lambda: self.approvals.pending_count() == 1, what="read_file 审批登记"
        )
        entry = self.approvals.peek_oldest()
        self.assertEqual(entry.backend, "antigravity")
        self.assertEqual(entry.request_id, "read-1")
        self.assertEqual(entry.choices, ["once", "deny"])
        self.assertEqual(len(self.admin_notices()), 1, "原生 read_file 仍必须通知管理员")
        self.assertEqual(
            _permission_frames(self.wire(), "read-1"), [], "未应答前不得自动回帧"
        )

        await self._approve("once")
        result = await asyncio.wait_for(task, 10)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(self.approvals.pending_count(), 0)
        frames = _permission_frames(self.wire(), "read-1")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(
            frames[0]["result"]["outcome"], {"outcome": "selected", "optionId": "allow"}
        )

    async def test_read_file_deny_answers_the_original_request(self) -> None:
        mcp_config(self)
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=self._permission_flags(
                request_id="read-2",
                name="read_file",
                title="File ../outside-cwd.txt",
                raw={"Path": str(self.base / "outside-cwd.txt")},
            ),
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(
            lambda: self.approvals.pending_count() == 1, what="read_file 审批登记"
        )
        await self._approve("deny")
        await asyncio.wait_for(task, 10)

        frames = _permission_frames(self.wire(), "read-2")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(
            frames[0]["result"]["outcome"], {"outcome": "selected", "optionId": "deny"}
        )
        self.assertEqual(self.approvals.pending_count(), 0)


# --- 4. 文件工具的群／私 scope 与越权（现有语义不变） ----------------------


class _FileDownloadError(Exception):
    """名字必须是 FileDownloadError：impl 按类型名分流可见错误。"""


class _Store:
    def __init__(self, targets: dict, files: dict) -> None:
        self._targets = targets
        self._files = files
        self.updates: list[tuple] = []

    def get_message_targets(self, ids):
        return {mid: self._targets[mid] for mid in ids if mid in self._targets}

    def get_message_files(self, ids):
        return {mid: self._files.get(mid, []) for mid in ids}

    def update_file_ref(self, *args, **kwargs):
        self.updates.append(("ref", args, kwargs))

    def update_file_download(self, *args, **kwargs):
        self.updates.append(("download", args, kwargs))

    def update_file_failed(self, *args, **kwargs):
        self.updates.append(("failed", args, kwargs))


class _Cache:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path

    def get_path(self, sha):
        return self._path


class _Downloader:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def download(self, url, file_name, expected_size=0):
        self.calls.append((url, file_name, expected_size))
        raise _FileDownloadError("offline-download")


def _row(*, idx=0, name="canary.txt", sha256=None, local_path="", url="", actual_size=None):
    return {
        "idx": idx,
        "name": name,
        "size": 7,
        "sha256": sha256,
        "local_path": local_path,
        "url": url,
        "file_id": "",
        "qq_message_id": "",
        "actual_size": actual_size,
    }


class GetMessageFilesScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="acp2c3-files-")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)

    def _impl(self, refs, *, store, cache=None, downloader=None, scope, resolver=None):
        from nonebot_plugin_hermes_adapter.mcp.tools.get_message_files import (
            GetMessageFilesInput,
            get_message_files_impl,
        )

        self.downloader = downloader or _Downloader()
        self.resolver = resolver or (lambda adapter, group_id, user_id: None)
        inp = GetMessageFilesInput(
            refs=[{"message_id": mid, "idx": idx} for mid, idx in refs]
        )
        return asyncio.run(
            get_message_files_impl(
                inp,
                store=store,
                cache=cache or _Cache(),
                downloader=self.downloader,
                bot_resolver=self.resolver,
                scope=scope,
            )
        )

    def test_unresolved_caller_gets_not_found_and_downloads_nothing(self) -> None:
        resolver_calls: list[tuple] = []

        def resolver(adapter, group_id, user_id):
            resolver_calls.append((adapter, group_id, user_id))
            return None

        targets = {1: ("onebotv11", GROUP, PRIVATE_USER), 2: ("onebotv11", None, PRIVATE_USER)}
        files = {1: [_row()], 2: [_row(url="http://offline.invalid/private")]}
        store = _Store(targets, files)

        result = self._impl([(1, 0), (2, 0)], store=store, scope=None, resolver=resolver)

        self.assertEqual([item.reason for item in result.results], ["not_found", "not_found"])
        self.assertEqual(self.downloader.calls, [], "认不出调用方时不得发起下载")
        self.assertEqual(resolver_calls, [], "越权校验先于 bot 解析")
        self.assertEqual(store.updates, [])

    def test_group_and_private_refs_of_an_allowed_caller_reach_the_download_step(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope

        targets = {1: ("onebotv11", GROUP, PRIVATE_USER), 2: ("onebotv11", None, PRIVATE_USER)}
        files = {
            1: [_row(url="http://offline.invalid/group")],
            2: [_row(name="private.txt", url="http://offline.invalid/private")],
        }
        result = self._impl(
            [(1, 0), (2, 0)], store=_Store(targets, files), scope=CallerScope.uniform()
        )

        self.assertEqual(
            [item.reason for item in result.results], ["download_failed", "download_failed"]
        )
        self.assertEqual(
            [call[0] for call in self.downloader.calls],
            ["http://offline.invalid/group", "http://offline.invalid/private"],
        )
        for item in result.results:
            self.assertIsNone(item.local_path, "下载失败不得返回半截路径")

    def test_unknown_message_id_is_indistinguishable_from_missing(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope

        store = _Store({1: ("onebotv11", GROUP, PRIVATE_USER)}, {1: []})
        result = self._impl([(1, 0), (99, 0)], store=store, scope=CallerScope.uniform())
        self.assertEqual([item.reason for item in result.results], ["not_found", "not_found"])

    def test_cached_file_returns_the_local_path_for_the_model(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope

        cached = self.base / "cache-canary.txt"
        cached.write_text("ACP2C3-FILE-CANARY", encoding="utf-8")
        store = _Store({1: ("onebotv11", GROUP, PRIVATE_USER)}, {1: [_row(sha256="abc123")]})

        result = self._impl(
            [(1, 0)],
            store=store,
            cache=_Cache(cached),
            scope=CallerScope.uniform(),
        )

        item = result.results[0]
        self.assertTrue(item.available)
        self.assertEqual(item.local_path, str(cached))
        self.assertEqual(item.sha256, "abc123")
        self.assertEqual(item.actual_size, cached.stat().st_size)
        self.assertEqual(self.downloader.calls, [], "缓存命中不得重复下载")
        self.assertEqual([entry[0] for entry in store.updates], ["download"], "回填缓存路径")

    def test_input_model_caps_refs_at_four(self) -> None:
        from pydantic import ValidationError

        from nonebot_plugin_hermes_adapter.mcp.tools.get_message_files import (
            GetMessageFilesInput,
        )

        GetMessageFilesInput(refs=[{"message_id": i, "idx": 0} for i in range(4)])
        with self.assertRaises(ValidationError):
            GetMessageFilesInput(refs=[{"message_id": i, "idx": 0} for i in range(5)])
        with self.assertRaises(ValidationError):
            GetMessageFilesInput(refs=[])


if __name__ == "__main__":
    unittest.main()
