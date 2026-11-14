"""阶段2C1：只读 MCP 工具 + `mcp_` 自动审批（离线 TDD）。

链路与 2B 同源：真 `build_mcp_app` / 真 `CallerScope` / 真 `resolve_caller_scope` /
真 `AntigravityACPClient` / 真 `HermesClient.chat` / 真 handler / 真 `core.approvals`，
只把官方 `agy acp` 换成 NDJSON 夹具子进程、平台发送换成进程内替身。

覆盖：
- Antigravity 工具表恰好三个只读工具；默认工具表不变、定义未被改写；
  （阶段2C3 起第三个是 get_message_files；恰三个的定案见 tests/test_antigravity_mcp_files.py）
- session/new 与 session/resume 携带同一份 HTTP mcpServers（群/私 scope 一致）；
- 无 key（dev）、无 scope、无目标时不挂 MCP；外部/错误 Bearer 被拒；
- `mcp_` 权限请求自动 allow_once，不登记人工审批、不发通知；
- 缺 allow_once 时安全拒绝；自动应答写帧失败不登记人工单；
- 非 MCP（run_command）仍走 2B once/deny；
- token 不出现在日志里。

注：`DUMMY_TOKEN` 是本文件自造的离线占位值，不是任何真实凭据。
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from tests.acp_2b_harness import (  # noqa: E402
    ACPHandlerHarness,
    permission_frames as _permission_frames,
    wait_until as _wait_until,
)
from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

FIXTURE = ROOT / "tests" / "fixtures" / "fake_agy_acp.py"

READ_ONLY_TOOLS = ("get_recent_messages", "get_message_images", "get_message_files")
ALL_TOOL_NAMES = {
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
MCP_PORT = 18877
MCP_URL = f"http://{MCP_HOST}:{MCP_PORT}/mcp"
GROUP = "100"


def expected_mcp_servers(*, with_bearer: bool = True) -> list[dict]:
    """本轮 ACP 应挂载的定义（独立字面量，不借用产品 helper）。"""
    server: dict = {
        "type": "http",
        "name": "nonebot-bridge",
        "url": MCP_URL,
    }
    if with_bearer:
        server["headers"] = [{"name": "Authorization", "value": f"Bearer {DUMMY_TOKEN}"}]
    return [server]


def _plugin_config_override(testcase: unittest.TestCase, **overrides) -> None:
    """按用例覆盖 plugin_config 字段，并在结束后逐项还原。"""
    from nonebot_plugin_hermes_adapter.config import plugin_config

    saved = {name: getattr(plugin_config, name) for name in overrides}
    for name, value in overrides.items():
        object.__setattr__(plugin_config, name, value)

    def restore() -> None:
        for name, value in saved.items():
            object.__setattr__(plugin_config, name, value)

    testcase.addCleanup(restore)


def mcp_config(testcase: unittest.TestCase, *, enabled: bool = True, key: str = DUMMY_TOKEN) -> None:
    _plugin_config_override(
        testcase,
        hermes_mcp_enabled=enabled,
        hermes_mcp_host=MCP_HOST,
        hermes_mcp_port=MCP_PORT,
        hermes_api_key=key,
    )


# --- 1. 工具表 ------------------------------------------------------------


class McpToolTableTests(unittest.TestCase):
    def _tools(self, **kwargs) -> dict:
        from nonebot_plugin_hermes_adapter.mcp import server

        real_factory = server.FastMCP
        holder: dict = {}

        def capture(*args, **inner):
            instance = real_factory(*args, **inner)
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
                **kwargs,
            )
        return {tool.name: tool for tool in asyncio.run(holder["mcp"].list_tools())}

    def test_antigravity_table_is_exactly_the_three_read_only_tools(self) -> None:
        tools = self._tools(tool_names=READ_ONLY_TOOLS)
        self.assertEqual(set(tools), set(READ_ONLY_TOOLS))

    def test_default_table_keeps_every_previous_tool(self) -> None:
        tools = self._tools()
        missing = ALL_TOOL_NAMES - set(tools)
        self.assertEqual(missing, set(), f"默认工具表少了 {missing}")
        # 默认表是完整表，不是被过滤后的三工具表。
        self.assertGreater(len(tools), len(READ_ONLY_TOOLS))
        self.assertTrue(set(READ_ONLY_TOOLS).issubset(tools))

    def test_filter_keeps_the_same_tool_definitions(self) -> None:
        """过滤只改注册集合，不改写这三个工具的描述与 schema。"""
        full = self._tools()
        filtered = self._tools(tool_names=READ_ONLY_TOOLS)
        for name in READ_ONLY_TOOLS:
            with self.subTest(tool=name):
                self.assertEqual(filtered[name].description, full[name].description)
                self.assertEqual(filtered[name].parameters, full[name].parameters)


# --- 2. mcpServers 定义与 Bearer ------------------------------------------


class McpServerDefinitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="acp2c1-def-")
        self.addCleanup(self.tmp.cleanup)

    def _servers(self, meta: dict) -> list[dict]:
        from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag

        return ag.mcp_servers_for_turn(meta)

    def _turn_meta(self, *, private: bool, group_id: str | None = GROUP) -> dict:
        return {
            "adapter": "onebotv11",
            "is_private": private,
            "user_id": "123",
            "group_id": None if private else group_id,
        }

    def test_group_turn_gets_scoped_bearer_definition(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import (
            CallerScope,
            resolve_caller_scope,
        )

        mcp_config(self)
        servers = self._servers(self._turn_meta(private=False))
        self.assertEqual(servers, expected_mcp_servers())

        header = servers[0]["headers"][0]["value"]
        scope = resolve_caller_scope(header)
        self.assertEqual(scope, CallerScope.uniform(), "本轮 Bearer 必须被服务端认成 uniform")
        self.assertTrue(scope.allows("onebotv11", GROUP))

    def test_private_turn_gets_the_same_scoped_bearer_definition(self) -> None:
        from nonebot_plugin_hermes_adapter.mcp.auth import private_scope_refusal
        from nonebot_plugin_hermes_adapter.core.routing import resolve_caller_scope

        mcp_config(self)
        servers = self._servers(self._turn_meta(private=True))
        self.assertEqual(servers, expected_mcp_servers())
        scope = resolve_caller_scope(servers[0]["headers"][0]["value"])
        self.assertIsNone(private_scope_refusal("onebotv11", "123", scope))

    def test_dev_mode_has_no_authorization_header_and_still_identifies_caller(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope, resolve_caller_scope

        mcp_config(self, key="")
        servers = self._servers(self._turn_meta(private=True))
        self.assertEqual(servers, expected_mcp_servers(with_bearer=False))
        # 无 key 就是既有 dev 语义：服务端仍能识别调用方（不是匿名）。
        self.assertEqual(resolve_caller_scope(None), CallerScope.dev())

    def test_foreign_bearer_is_refused(self) -> None:
        """跨 scope：不是本实例凭据的 Bearer 既认不出，也不能通过群/私范围判定。"""
        from nonebot_plugin_hermes_adapter.core.routing import resolve_caller_scope
        from nonebot_plugin_hermes_adapter.mcp.auth import private_scope_refusal, scope_refusal

        mcp_config(self)
        self.assertIsNone(resolve_caller_scope(f"Bearer {DUMMY_TOKEN}-other"))
        self.assertIsNone(resolve_caller_scope("Bearer "))
        self.assertIsNone(resolve_caller_scope(None))
        self.assertIn("not authorized", scope_refusal("onebotv11", GROUP, None) or "")
        self.assertIn("private chat", private_scope_refusal("onebotv11", "123", None) or "")

    def test_unresolved_scope_does_not_attach_mcp(self) -> None:
        from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag

        mcp_config(self)
        with patch.object(ag, "resolve_caller_scope", return_value=None):
            self.assertEqual(self._servers(self._turn_meta(private=True)), [])

    def test_disabled_mcp_attaches_nothing(self) -> None:
        mcp_config(self, enabled=False)
        self.assertEqual(self._servers(self._turn_meta(private=False)), [])

    def test_missing_group_or_private_target_attaches_nothing(self) -> None:
        mcp_config(self)
        self.assertEqual(self._servers(self._turn_meta(private=False, group_id=None)), [])
        meta = self._turn_meta(private=True)
        meta["user_id"] = ""
        self.assertEqual(self._servers(meta), [])

    def test_http_layer_rejects_a_foreign_bearer(self) -> None:
        from nonebot_plugin_hermes_adapter.mcp import server

        mcp_config(self)
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
                message_store=None,
                image_cache=None,
                background_outbox=None,
                tool_names=READ_ONLY_TOOLS,
            )

        sent: list[dict] = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "headers": [(b"authorization", b"Bearer foreign-token")],
        }
        asyncio.run(app(scope, receive, send))
        starts = [item for item in sent if item["type"] == "http.response.start"]
        self.assertEqual(len(starts), 1, sent)
        self.assertEqual(starts[0]["status"], 401)


# --- 3. session/new 与 session/resume 同一份定义 ---------------------------


class McpSessionDefinitionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="acp2c1-session-")
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log_path = self.tmp / "wire.jsonl"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self.clients = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    def make_client(self) -> "object":
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            AntigravityACPClient,
        )

        client = AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=[
                str(FIXTURE),
                str(self.log_path),
                json.dumps("§\nOK\n§", ensure_ascii=False),
            ],
            workspace=self.tmp,
            prompt_timeout=6.0,
            rpc_timeout=5.0,
        )
        self.clients.append(client)
        return client

    def wire(self) -> list[dict]:
        if not self.log_path.exists():
            return []
        return [
            json.loads(line)
            for line in self.log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def rows(self, method: str) -> list[dict]:
        return [item for item in self.wire() if item.get("method") == method]

    def group_meta(self) -> dict:
        return {
            "adapter": "onebotv11",
            "is_private": False,
            "user_id": "123",
            "group_id": GROUP,
        }

    async def test_new_and_resume_carry_the_same_http_definition(self) -> None:
        mcp_config(self)
        client = self.make_client()
        await client.prompt(
            session_key="k-group",
            system_prompt="SYS",
            user_text="hi",
            store=self.store,
            turn_meta=self.group_meta(),
        )
        await client.prompt(
            session_key="k-group",
            system_prompt="SYS",
            user_text="again",
            store=self.store,
            turn_meta=self.group_meta(),
        )

        new = self.rows("session/new")
        resume = self.rows("session/resume")
        self.assertEqual(len(new), 1, new)
        self.assertEqual(len(resume), 1, resume)
        self.assertEqual(new[0]["params"]["mcpServers"], expected_mcp_servers())
        self.assertEqual(resume[0]["params"]["mcpServers"], expected_mcp_servers())
        self.assertEqual(
            new[0]["params"]["mcpServers"], resume[0]["params"]["mcpServers"],
            "新建与恢复必须传同一份定义",
        )
        # 本客户端没有 ACP session/load 调用点（阶段4恢复才需要），不得凭空新增。
        self.assertEqual(self.rows("session/load"), [])

    async def test_private_turn_gets_the_private_scope_definition(self) -> None:
        mcp_config(self)
        client = self.make_client()
        await client.prompt(
            session_key="k-private",
            system_prompt="SYS",
            user_text="hi",
            store=self.store,
            turn_meta={
                "adapter": "onebotv11",
                "is_private": True,
                "user_id": "123",
                "group_id": None,
            },
        )
        new = self.rows("session/new")
        self.assertEqual(new[0]["params"]["mcpServers"], expected_mcp_servers())

    async def test_disabled_mcp_keeps_the_2b_empty_list(self) -> None:
        mcp_config(self, enabled=False)
        client = self.make_client()
        await client.prompt(
            session_key="k-off",
            system_prompt="SYS",
            user_text="hi",
            store=self.store,
            turn_meta=self.group_meta(),
        )
        self.assertEqual(self.rows("session/new")[0]["params"]["mcpServers"], [])

    async def test_token_never_reaches_client_logs(self) -> None:
        mcp_config(self)
        from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag

        records: list[str] = []

        class Recorder:
            def info(self, message, *args, **kwargs):
                records.append(str(message))

            def warning(self, message, *args, **kwargs):
                records.append(str(message))

            def error(self, message, *args, **kwargs):
                records.append(str(message))

            def debug(self, message, *args, **kwargs):
                records.append(str(message))

        client = self.make_client()
        with patch.object(ag, "logger", Recorder()):
            await client.prompt(
                session_key="k-log",
                system_prompt="SYS",
                user_text="hi",
                store=self.store,
                turn_meta=self.group_meta(),
            )
        joined = "\n".join(records)
        self.assertNotIn(DUMMY_TOKEN, joined, "Bearer 令牌不得写入日志")
        self.assertTrue(any("MCP" in line for line in records), records)


# --- 4. mcp_ 自动审批（客户端协议层） -------------------------------------


class McpAutoApprovalClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.core import approvals

        self.approvals = approvals
        self._tmp = tempfile.TemporaryDirectory(prefix="acp2c1-perm-")
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log_path = self.tmp / "wire.jsonl"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self.clients = []
        self.payloads: list[dict] = []

        self._prev_pending = dict(approvals._pending)
        self._prev_store = approvals._store_path
        approvals._pending.clear()
        approvals._store_path = self.tmp / "pending_approvals.json"
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        self.approvals._pending.clear()
        self.approvals._pending.update(self._prev_pending)
        self.approvals._store_path = self._prev_store

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    def make_client(self, *, flags=(), start: bool = True):
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            AntigravityACPClient,
        )

        client = AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=[
                str(FIXTURE),
                str(self.log_path),
                json.dumps("§\n第一句\n§\n第二句\n§", ensure_ascii=False),
                *flags,
            ],
            workspace=self.tmp,
            prompt_timeout=6.0,
            rpc_timeout=5.0,
        )
        self.clients.append(client)
        if not start:
            # 不调用 _ensure_started：模拟「子进程不可用」时自动应答写帧失败。
            client._closed = True
        return client

    def wire(self) -> list[dict]:
        if not self.log_path.exists():
            return []
        return [
            json.loads(line)
            for line in self.log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    async def _notify(self, payload: dict) -> None:
        self.payloads.append(payload)

    async def test_mcp_permission_is_auto_allowed_once_without_notification(self) -> None:
        mcp_config(self)
        client = self.make_client(
            flags=(
                "--permission",
                "--permission-id=mcp-1",
                "--permission-name=mcp_nonebot-bridge_get_recent_messages",
            )
        )
        text = await asyncio.wait_for(
            client.prompt(
                session_key="k1",
                system_prompt="SYS",
                user_text="hi",
                store=self.store,
                approval_callback=self._notify,
                turn_meta={
                    "adapter": "onebotv11",
                    "is_private": False,
                    "user_id": "123",
                    "group_id": GROUP,
                },
            ),
            8,
        )
        self.assertEqual(text, "§\n第一句\n§\n第二句\n§")
        self.assertEqual(self.payloads, [], "MCP 自动放行不得发人工通知")
        frames = _permission_frames(self.wire(), "mcp-1")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(
            frames[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "allow-once"},
        )
        self.assertEqual(client._permissions, {}, "MCP 自动放行不得登记挂起审批")
        self.assertEqual(self.approvals.pending_count(), 0)

    async def test_mcp_permission_without_allow_once_is_safely_refused(self) -> None:
        mcp_config(self)
        client = self.make_client(
            flags=(
                "--permission",
                "--permission-id=mcp-noallow",
                "--permission-name=mcp_nonebot-bridge_get_message_images",
                "--permission-options="
                + json.dumps([{"optionId": "reject-only", "name": "Reject", "kind": "reject_once"}]),
            )
        )
        await asyncio.wait_for(
            client.prompt(
                session_key="k1",
                system_prompt="SYS",
                user_text="hi",
                store=self.store,
                approval_callback=self._notify,
            ),
            8,
        )
        frames = _permission_frames(self.wire(), "mcp-noallow")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(frames[0]["result"]["outcome"], {"outcome": "cancelled"})
        self.assertEqual(self.payloads, [])
        self.assertEqual(self.approvals.pending_count(), 0)

    async def test_auto_reply_write_failure_opens_no_manual_ticket(self) -> None:
        mcp_config(self)
        client = self.make_client(start=False)
        message = {
            "jsonrpc": "2.0",
            "id": "mcp-dead",
            "method": "session/request_permission",
            "params": {
                "sessionId": "sess-dead",
                "toolCall": {"name": "mcp_nonebot-bridge_get_recent_messages"},
                "options": [
                    {"optionId": "allow-once", "kind": "allow_once"},
                    {"optionId": "reject-once", "kind": "reject_once"},
                ],
            },
        }
        await client._answer_agent_request(message)
        self.assertEqual(self.approvals.pending_count(), 0, "写帧失败不得登记人工单")
        self.assertEqual(self.payloads, [])
        self.assertEqual(client._permissions, {})

    async def test_non_mcp_permission_still_waits_for_the_admin(self) -> None:
        client = self.make_client(
            flags=(
                "--permission",
                "--permission-id=cmd-1",
                "--permission-name=run_command",
                "--permission-raw=" + json.dumps({"CommandLine": "pwd"}),
            )
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1",
                system_prompt="SYS",
                user_text="hi",
                store=self.store,
                approval_callback=self._notify,
            )
        )
        await _wait_until(lambda: bool(self.payloads), what="非 MCP 审批登记")
        self.assertEqual(self.payloads[0]["title"], "write_file")
        self.assertEqual(_permission_frames(self.wire(), "cmd-1"), [], "仍必须等管理员应答")
        self.assertTrue(
            await client.answer_permission(session_key="k1", request_id="cmd-1", choice="deny")
        )
        await asyncio.wait_for(task, 8)
        self.assertEqual(
            _permission_frames(self.wire(), "cmd-1")[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
        )


# --- 5. 真 handler 链：MCP 自动放行 / run_command 不变 ---------------------


class McpAutoApprovalHandlerTests(ACPHandlerHarness):
    MCP_TOOL = "mcp_nonebot-bridge_get_recent_messages"

    async def _reactive_turn(self):
        row = self._row(private=False)
        return await self.mt._run_reactive_turn(
            bot=self.bot,
            target=SimpleNamespace(private=False, id=GROUP),
            adapter_name="onebotv11",
            user_id=self.ADMIN,
            group_id=GROUP,
            text=row.content,
            image_urls=[],
            now_ms=int(time.time() * 1000),
            current_buffered=row,
            is_direct=True,
            addressed_to_bot=True,
            ephemeral=True,
        )

    async def test_mcp_tool_is_auto_allowed_without_pending_or_notice(self) -> None:
        mcp_config(self)
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=(
                "--permission",
                "--permission-id=mcp-h1",
                f"--permission-name={self.MCP_TOOL}",
            ),
        )
        result = await asyncio.wait_for(self._turn(), 10)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(
            [text for text in self.platform if text in ("第一句", "第二句")],
            ["第一句", "第二句"],
        )
        self.assertEqual(self.approvals.pending_count(), 0, "MCP 不得登记人工审批")
        self.assertEqual(self.admin_notices(), [], "MCP 不得发人工通知")
        frames = _permission_frames(self.wire(), "mcp-h1")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(
            frames[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "allow-once"},
        )

    async def test_group_turn_mounts_scoped_mcp_and_auto_allows(self) -> None:
        mcp_config(self)
        self._client(
            reply="§\n群正文\n§",
            flags=(
                "--permission",
                "--permission-id=mcp-g1",
                f"--permission-name={self.MCP_TOOL}",
            ),
        )
        result = await asyncio.wait_for(self._reactive_turn(), 10)
        self.assertFalse(result.is_transport_error)
        new = self.rows("session/new")
        self.assertEqual(len(new), 1, new)
        self.assertEqual(new[0]["params"]["mcpServers"], expected_mcp_servers())
        self.assertEqual(self.approvals.pending_count(), 0)
        self.assertEqual(self.admin_notices(), [])

    async def test_run_command_still_goes_through_admin_approval(self) -> None:
        mcp_config(self)
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=(
                "--permission",
                "--permission-id=cmd-h1",
                "--permission-name=run_command",
                "--permission-raw=" + json.dumps({"CommandLine": "pwd"}),
            ),
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="run_command 审批登记")
        entry = self.approvals.peek_oldest()
        self.assertEqual(entry.backend, "antigravity")
        self.assertEqual(entry.request_id, "cmd-h1")
        self.assertEqual(entry.choices, ["once", "deny"])
        self.assertEqual(len(self.admin_notices()), 1)

        await self._approve("once")
        result = await asyncio.wait_for(task, 10)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(self.approvals.pending_count(), 0)


if __name__ == "__main__":
    unittest.main()
