"""ACP /clear 旧代出站围栏回归（阶段1 返修 R1）。

链路仍是候选的真实路径：真 `_run_passive_turn` / `_run_reactive_turn`
→ 真 `HermesClient.chat` → 假 ACP NDJSON 子进程（合法离线夹具）
→ 真 `core.outbound.send_text_with_media`。

只替换官方平台发送边界（`alconna.UniMessage.send`）与隔离配置/存储，不 mock
发送 helper、不替换会话代际或出站判断函数，也不发真实 QQ / 不启动真实 agy 模型。

时序：普通 prompt 在假 ACP 里被挂起 → 跑真实 `commands.handle_clear`
（generation 推进、新 session 已建立）→ 再放行旧回复。
旧代正文必须在平台边界零次；下一轮新代正文仍正常发送。
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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402

# 配置根在导入插件模块前就隔离，避免读到本机真实配置。
_CONFIG_TMP = tempfile.TemporaryDirectory(prefix="acp-clear-fence-config-")
object.__setattr__(plugin_config, "hermes_custom_dir", _CONFIG_TMP.name)
object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")

from nonebot_plugin_hermes_adapter import mcp  # noqa: E402
from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag  # noqa: E402
from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import outbound  # noqa: E402
from nonebot_plugin_hermes_adapter.core.active_session import (  # noqa: E402
    ActiveSessionManager,
)
from nonebot_plugin_hermes_adapter.core.chat_config import ChatConfig  # noqa: E402
from nonebot_plugin_hermes_adapter.core.media_config import MediaConfig  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.misc_config import MiscConfig  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from nonebot_plugin_hermes_adapter.core.session import SessionManager  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)
from nonebot_plugin_hermes_adapter.handlers import commands  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "fake_agy_acp.py"
OLD_BODY = "OLD"


class ClearFenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._prev_interface = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__,
            plugin_config,
            "hermes_api_interface",
            self._prev_interface,
        )

        self._tmp = tempfile.TemporaryDirectory(prefix="acp-clear-fence-")
        self.addCleanup(self._tmp.cleanup)
        self.base = Path(self._tmp.name)
        self.store = SessionKeyStore(self.base / "session_keys.db")
        self.addCleanup(self.store.close)
        self.tracker = RunTracker(self.base / "run_tracker.db")
        self.addCleanup(self.tracker.close)
        self.manager = SessionManager()
        self.manager.bind_store(self.store)
        self.manager.bind_run_tracker(self.tracker)
        self.sessions = ActiveSessionManager()
        self.clients: list[ag.AntigravityACPClient] = []
        self.platform: list[str] = []

        self.bot = SimpleNamespace(self_id="999", call_api=AsyncMock())
        self.target = SimpleNamespace(
            base_url="http://offline.invalid", api_key="", timeout=5, label="offline"
        )
        self.msgbuffer = SimpleNamespace(get_recent=lambda **kw: [], append=lambda row: None)
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
                get_bot_nickname=AsyncMock(return_value="FenceBot"),
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
                message_buffer=self.msgbuffer,
                active_sessions=self.sessions,
                run_tracker=self.tracker,
            )
        )
        # 真实 outbound：只把官方平台发送边界换成进程内替身。
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
        # 旧代围栏必须与会话代际对齐；Hermes HTTP 一律不得被调用。
        self.stack.enter_context(
            patch.object(hc, "_get_http_client", side_effect=AssertionError("HTTP forbidden"))
        )

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    # ---------------------------------------------------------------- helpers
    def _client(self, reply: str, *flags: str, gate: bool = False) -> ag.AntigravityACPClient:
        log = self.base / "wire.jsonl"
        args = ["-B", str(FIXTURE), str(log), json.dumps(reply, ensure_ascii=False), *flags]
        if gate:
            args.append(f"--gate-prompt={self.base / 'wire.jsonl.release'}")
        client = ag.AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=args,
            workspace=self.base,
            prompt_timeout=5,
            rpc_timeout=5,
        )
        self.clients.append(client)
        self.stack.enter_context(patch.object(ag, "get_client", return_value=client))
        return client

    def _wire(self) -> list[dict]:
        log = self.base / "wire.jsonl"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]

    def _entries(self, method: str) -> list[dict]:
        return [item for item in self._wire() if item.get("method") == method]

    async def _turn(self, route: str):
        private = route == "private"
        group = None if private else "100"
        now = int(time.time() * 1000)
        row = BufferedMessage(
            ts=now,
            adapter="onebotv11",
            group_id=group,
            user_id="123",
            nickname="FenceUser",
            content="neutral text canary",
            origin=MessageOrigin.human(),
            id=1,
            private_owner_id="123" if private else None,
        )
        kwargs = dict(
            bot=self.bot,
            target=SimpleNamespace(private=private, id="123" if private else group),
            adapter_name="onebotv11",
            user_id="123",
            group_id=group,
            text=row.content,
            image_urls=[],
            now_ms=now,
            current_buffered=row,
        )
        if route == "reactive":
            return await mt._run_reactive_turn(
                **kwargs, is_direct=True, addressed_to_bot=True, ephemeral=True
            )
        return await mt._run_passive_turn(**kwargs, is_private=private)

    async def _await_prompt_in_flight(self, task: asyncio.Task) -> None:
        for _ in range(500):
            if self._entries("session/prompt"):
                return
            if task.done():
                await task
                raise AssertionError("turn ended before the ACP prompt was in flight")
            await asyncio.sleep(0.01)
        raise AssertionError("ACP prompt never became in flight")

    async def _clear(self, route: str) -> None:
        private = route == "private"
        group = None if private else "100"
        target = SimpleNamespace(private=private, id="123" if private else group)
        with (
            patch.object(commands, "session_manager", self.manager),
            patch.object(commands.alconna, "get_target", return_value=target),
            patch.object(commands, "check_isolation", return_value=True),
            patch.object(commands, "get_adapter_name", return_value="onebotv11"),
            patch.object(mcp, "background_report_outbox", None),
        ):
            await commands.handle_clear(
                self.bot,
                SimpleNamespace(get_user_id=lambda: "123"),
                SimpleNamespace(),
            )

    # ------------------------------------------------------------------ cases
    async def test_normal_turn_reaches_platform_once(self) -> None:
        self._client(f"§\n{OLD_BODY}\n§")
        await self._turn("private")
        self.assertEqual(self.platform, [OLD_BODY])

    async def test_private_clear_fences_inflight_old_body(self) -> None:
        await self._assert_clear_fence("private")

    async def test_group_passive_clear_fences_inflight_old_body(self) -> None:
        await self._assert_clear_fence("passive")

    async def test_group_reactive_clear_fences_inflight_old_body(self) -> None:
        await self._assert_clear_fence("reactive")

    async def _assert_clear_fence(self, route: str) -> None:
        private = route == "private"
        group = None if private else "100"
        self._client(f"§\n{OLD_BODY}\n§", gate=True)
        task = asyncio.create_task(self._turn(route))
        await self._await_prompt_in_flight(task)
        self.assertEqual(self.platform, [], "旧代正文不得在对话轮未结束时提前出站")

        await self._clear(route)
        self.assertEqual(
            self.manager.get_delivery_generation("onebotv11", private, "123", group),
            1,
            "真实 /clear 必须已推进交付代际",
        )
        before_release = len(self.platform)
        (self.base / "wire.jsonl.release").touch()
        await task
        self.assertEqual(
            self.platform[before_release:],
            [],
            "clear 完成后仍在途的旧代正文不得到达平台发送边界",
        )

        # 同一路由的下一轮：新 ACP 会话 + 新代正文照常发送。
        await self._turn(route)
        prompts = self._entries("session/prompt")
        news = self._entries("session/new")
        self.assertEqual(len(news), 2, "clear 后必须建立新 ACP 会话")
        self.assertNotEqual(
            prompts[0]["params"]["sessionId"], prompts[1]["params"]["sessionId"]
        )
        self.assertEqual(self.platform[-1], OLD_BODY, "clear 后新代正文仍须正常发送")


if __name__ == "__main__":
    try:
        unittest.main(verbosity=2)
    finally:
        _CONFIG_TMP.cleanup()
