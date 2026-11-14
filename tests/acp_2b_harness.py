"""阶段2B 共用的「真 handler → 假 ACP → 真出站」夹具（非测试模块）。

链路与 `tests/test_antigravity_stream_steer.py` 同源：假 `agy acp` NDJSON 子进程 →
真 `AntigravityACPClient` / `HermesClient.chat` → 真 handler
（`_run_passive_turn`）→ 真 `core.outbound.send_text_with_media` → 进程内平台替身。
只替换官方平台发送边界、管理员白名单、隔离配置与存储；不发真实 QQ、不跑真实模型。
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests import _bootstrap  # noqa: F401

from nonebot_plugin_agent_adapter.core.storage.session_key_store import SessionKeyStore

FIXTURE = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "fake_agy_acp.py"


async def wait_until(predicate, *, timeout: float = 6.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{what} 未出现")


def permission_frames(wire: list[dict], request_id: object) -> list[dict]:
    """客户端回给权限请求的应答帧（inbound 无 method）。"""
    return [item for item in wire if item.get("id") == request_id and "method" not in item]


class ACPHandlerHarness(unittest.IsolatedAsyncioTestCase):
    ADMIN = "123"

    def setUp(self) -> None:
        from nonebot_plugin_agent_adapter.config import plugin_config

        self._prev_interface = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._prev_interface
        )

        self._tmp = tempfile.TemporaryDirectory(prefix="acp-2b-handler-")
        self.addCleanup(self._tmp.cleanup)
        self.base = Path(self._tmp.name)
        self._prev_custom_dir = plugin_config.hermes_custom_dir
        object.__setattr__(plugin_config, "hermes_custom_dir", str(self.base / "config"))
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_custom_dir", self._prev_custom_dir
        )

        from nonebot_plugin_agent_adapter import mcp
        from nonebot_plugin_agent_adapter.core import approvals
        from nonebot_plugin_agent_adapter.core import hermes_client as hc
        from nonebot_plugin_agent_adapter.core import outbound
        from nonebot_plugin_agent_adapter.core.active_session import ActiveSessionManager
        from nonebot_plugin_agent_adapter.core.chat_config import ChatConfig
        from nonebot_plugin_agent_adapter.core.inflight import InflightRegistry
        from nonebot_plugin_agent_adapter.core.media_config import MediaConfig
        from nonebot_plugin_agent_adapter.core.misc_config import MiscConfig
        from nonebot_plugin_agent_adapter.core.run_tracker import RunTracker
        from nonebot_plugin_agent_adapter.core.session import SessionManager
        from nonebot_plugin_agent_adapter.handlers import commands
        from nonebot_plugin_agent_adapter.handlers import message_feedback as feedback
        from nonebot_plugin_agent_adapter.handlers import message_turns as mt

        self.mcp = mcp
        self.hc = hc
        self.mt = mt
        self.commands = commands
        self.approvals = approvals
        self.store = SessionKeyStore(self.base / "session_keys.db")
        self.addCleanup(self.store.close)
        self.tracker = RunTracker(self.base / "run_tracker.db")
        self.addCleanup(self.tracker.close)
        self.manager = SessionManager()
        self.manager.bind_store(self.store)
        self.manager.bind_run_tracker(self.tracker)
        self.sessions = ActiveSessionManager()
        self.clients = []
        self.platform: list[str] = []
        self.bot = SimpleNamespace(self_id="999", call_api=AsyncMock(return_value={}))
        self.target = SimpleNamespace(
            base_url="http://offline.invalid", api_key="", timeout=5, label="offline"
        )
        self.msgbuffer = SimpleNamespace(get_recent=lambda **kw: [], append=lambda row: None)
        self.admin_lists = SimpleNamespace(admin_users={"onebotv11:" + self.ADMIN})

        self._prev_pending = dict(approvals._pending)
        self._prev_store = approvals._store_path
        approvals._pending.clear()
        approvals._store_path = self.base / "pending_approvals.json"
        self.addCleanup(self._restore_approvals)

        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(
            patch.multiple(
                mt,
                session_manager=self.manager,
                load_chat_config=lambda: ChatConfig(task_anchor_enabled=False),
                load_misc_config=lambda: MiscConfig(
                    group_profile_enabled=False,
                    transport_error_fallback_text="transport-fallback",
                ),
                load_media_config=lambda: MediaConfig(mode="off"),
                resolve_target=lambda *a: self.target,
                get_bot_nickname=AsyncMock(return_value="PermBot"),
                _maybe_intercept_bounce=AsyncMock(return_value=False),
                _post_send_feedback=AsyncMock(),
                _is_blacklisted=lambda *a: False,
                _speaker_name_resolver=lambda *a: None,
            )
        )
        self.stack.enter_context(
            patch.multiple(
                mcp,
                session_key_store=self.store,
                session_manager=self.manager,
                message_buffer=self.msgbuffer,
                active_sessions=self.sessions,
                run_tracker=self.tracker,
                inflight=InflightRegistry(),
            )
        )
        self.stack.enter_context(
            patch.object(mt, "send_text_with_media", outbound.send_text_with_media)
        )
        self.stack.enter_context(
            patch.multiple(
                outbound,
                load_chat_config=lambda: ChatConfig(task_anchor_enabled=False),
                load_misc_config=lambda: MiscConfig(),
                load_media_config=lambda: MediaConfig(mode="off"),
            )
        )

        async def platform_send(msg, **kwargs):
            self.platform.append(str(msg))
            return SimpleNamespace(msg_ids=[str(len(self.platform))])

        self.stack.enter_context(patch.object(outbound.alconna.UniMessage, "send", platform_send))

        async def instant(_delay):
            await asyncio.sleep(0)

        self.stack.enter_context(patch.object(hc, "_runs_reply_sleep", instant))
        self.stack.enter_context(
            patch.object(hc, "_get_http_client", side_effect=AssertionError("HTTP forbidden"))
        )
        self.stack.enter_context(patch.object(feedback, "load_user_lists", lambda: self.admin_lists))
        self.stack.enter_context(patch.object(commands, "load_user_lists", lambda: self.admin_lists))
        self.stack.enter_context(patch.object(commands, "session_manager", self.manager))

    def _restore_approvals(self) -> None:
        self.approvals._pending.clear()
        self.approvals._pending.update(self._prev_pending)
        self.approvals._store_path = self._prev_store

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    # ---------------------------------------------------------------- helpers
    def _client(self, *, reply=None, flags=(), chunks=None, delay=0.05, permission_timeout=None):
        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag

        args = [str(FIXTURE), str(self.base / "wire.jsonl"), json.dumps(reply or "", ensure_ascii=False)]
        if chunks is not None:
            args += [
                f"--chunks={json.dumps(chunks, ensure_ascii=False)}",
                f"--chunk-delay={delay}",
            ]
        args += list(flags)
        client = ag.AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=args,
            workspace=self.base,
            prompt_timeout=10.0,
            rpc_timeout=5.0,
            permission_timeout=permission_timeout,
        )
        self.clients.append(client)
        self.stack.enter_context(patch.object(ag, "get_client", return_value=client))
        return client

    def wire(self) -> list[dict]:
        log = self.base / "wire.jsonl"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]

    def rows(self, method: str) -> list[dict]:
        return [item for item in self.wire() if item.get("method") == method]

    def session_key(self) -> str:
        return self.manager.get_session_key("onebotv11", True, self.ADMIN, None)

    def _row(self, *, private=True, content="hi", mid=1, image_urls=None):
        from nonebot_plugin_agent_adapter.core.message_buffer import BufferedMessage
        from nonebot_plugin_agent_adapter.core.message_origin import MessageOrigin

        return BufferedMessage(
            ts=int(time.time() * 1000),
            adapter="onebotv11",
            group_id=None if private else "100",
            user_id=self.ADMIN,
            nickname="PermUser",
            content=content,
            origin=MessageOrigin.human(),
            id=mid,
            private_owner_id=self.ADMIN if private else None,
            image_urls=list(image_urls or []),
        )

    async def _turn(self, row=None, *, private=True, image_urls=None):
        row = row or self._row(private=private)
        return await self.mt._run_passive_turn(
            bot=self.bot,
            target=SimpleNamespace(private=private, id=self.ADMIN if private else "100"),
            adapter_name="onebotv11",
            user_id=self.ADMIN,
            group_id=None if private else "100",
            text=row.content,
            image_urls=list(image_urls or []),
            now_ms=int(time.time() * 1000),
            current_buffered=row,
            is_private=private,
        )

    async def _approve(self, choice: str, *, user_id: str | None = None):
        target = SimpleNamespace(private=True, id=self.ADMIN)
        raw = f"/{self.commands._cmd('approve')}"
        if choice:
            raw += f" {choice}"
        await self._command(
            self.commands.handle_approve, target=target, raw=raw, user_id=user_id
        )

    async def _run_stop(self, *, user_id: str | None = None, private: bool = True):
        target = SimpleNamespace(private=private, id=self.ADMIN if private else "100")
        raw = f"/{self.commands._cmd('run-stop')}"
        await self._command(
            self.commands.handle_run_stop, target=target, raw=raw, user_id=user_id
        )

    async def _command(self, handler, *, target, raw, user_id=None):
        event = SimpleNamespace(
            get_user_id=lambda: user_id or self.ADMIN,
            get_plaintext=lambda: raw,
        )
        with (
            patch.object(self.commands.alconna, "get_target", return_value=target),
            patch.object(self.commands, "check_isolation", return_value=True),
            patch.object(self.commands, "get_adapter_name", return_value="onebotv11"),
        ):
            await handler(self.bot, event, SimpleNamespace(skip=lambda: None))

    def admin_notices(self) -> list[str]:
        texts = []
        for call in self.bot.call_api.await_args_list:
            if call.args and call.args[0] == "send_private_msg":
                texts.append(str(call.kwargs.get("message") or ""))
        return texts

    def receipts(self) -> list[str]:
        return [
            text
            for text in self.platform
            if text.startswith(("✅", "⚠️", "🛑", "审批", "用法", "当前没有", "当前会话"))
        ]
