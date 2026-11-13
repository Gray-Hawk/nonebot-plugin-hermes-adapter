"""阶段2A：ACP 流式正文与运行中 steer 接管（离线 TDD）。

链路仍是候选的真实路径：假 `agy acp` NDJSON 子进程 → 真
`AntigravityACPClient` / `HermesClient.chat` → 真 handler
（`_run_passive_turn` / `_run_reactive_turn`）→ 真 `core.outbound.send_text_with_media`。
只替换官方平台发送边界与隔离配置/存储；不发真实 QQ、不启动真实 agy 模型。

本文件先覆盖客户端协议层（RED→GREEN 切片 1-2），handler 链路的用例在同文件后段。
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

FIXTURE = ROOT / "tests" / "fixtures" / "fake_agy_acp.py"


class ACPStreamTestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="acp-stream-")
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log_path = self.tmp / "wire.jsonl"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self._clients = []

    async def asyncTearDown(self) -> None:
        for client in self._clients:
            await client.close()

    def make_client(self, *, chunks, steer_chunks=None, delay=0.03,
                    ack_wait=0.0, late_chunk=None, prompt_timeout=5.0,
                    extra_flags=()):
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            AntigravityACPClient,
        )

        flags = [
            f"--chunks={json.dumps(chunks, ensure_ascii=False)}",
            f"--chunk-delay={delay}",
            *extra_flags,
        ]
        if steer_chunks is not None:
            flags.append(
                f"--steer-chunks={json.dumps(steer_chunks, ensure_ascii=False)}"
            )
        if ack_wait:
            flags.append(f"--ack-wait={ack_wait}")
        if late_chunk is not None:
            flags.append(f"--late-chunk={late_chunk}")
        client = AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=[str(FIXTURE), str(self.log_path), json.dumps(""), *flags],
            workspace=self.tmp,
            prompt_timeout=prompt_timeout,
            rpc_timeout=5.0,
        )
        self._clients.append(client)
        return client

    def wire(self) -> list[dict]:
        if not self.log_path.exists():
            return []
        return [
            json.loads(line)
            for line in self.log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def entries(self, method: str) -> list[dict]:
        return [item for item in self.wire() if item.get("method") == method]


class ACPStreamingTests(ACPStreamTestBase):
    async def test_closed_units_arrive_before_the_prompt_rpc_returns(self) -> None:
        """闭合 § 单位在 RPC 收尾前就进入回调；终态只补未投递的那一份。"""
        units: list[str] = []
        first = asyncio.Event()

        def on_unit(decision: dict) -> None:
            units.append(decision["body"])
            if decision["body"] == "A1":
                first.set()

        client = self.make_client(chunks=["§\nA1\n§\n", "A2\n", "§"])
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, on_unit=on_unit,
            )
        )
        await asyncio.wait_for(first.wait(), 3)
        self.assertFalse(task.done(), "首个闭合单位到达时 prompt 不得已经收尾")

        text = await asyncio.wait_for(task, 5)
        self.assertEqual(units, ["A1", "A2"], "A1 流式到达，A2 只在终态补一次")
        self.assertEqual(text, "§\nA1\n§\nA2\n§")

    async def test_streamed_units_are_not_replayed_at_terminal(self) -> None:
        units: list[str] = []
        client = self.make_client(
            chunks=["§\nA1\n§\n", "§\nA2\n§\n"],
            delay=0.04,
        )
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi",
            store=self.store, on_unit=lambda d: units.append(d["body"]),
        )
        self.assertEqual(units, ["A1", "A2"], "已流式投递的单位不得在终态重发")

    async def test_duplicate_chunks_within_a_turn_are_deduped(self) -> None:
        units: list[str] = []
        client = self.make_client(
            chunks=["§\nA1\n§\n", "§\nA1\n§\n"],
            delay=0.02,
        )
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi",
            store=self.store, on_unit=lambda d: units.append(d["body"]),
        )
        self.assertEqual(units, ["A1"])

    async def test_late_chunk_after_turn_is_not_attributed_to_next_round(self) -> None:
        units: list[str] = []
        client = self.make_client(
            chunks=["§\nA1\n§\n"],
            late_chunk="§\nLATE\n§",
            delay=0.02,
        )
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi",
            store=self.store, on_unit=lambda d: units.append(d["body"]),
        )
        await asyncio.sleep(0.2)
        self.assertEqual(units, ["A1"], "轮次结束后游离 chunk 不得进入任何一轮")


class ACPSteerTakeoverTests(ACPStreamTestBase):
    async def test_steer_early_end_turn_keeps_old_stream_and_new_prompt_owns_terminal(self) -> None:
        units: list[str] = []
        first = asyncio.Event()

        def on_unit(decision: dict) -> None:
            units.append(decision["body"])
            if decision["body"] == "A1":
                first.set()

        client = self.make_client(
            chunks=["§\nA1\n§\n", "A2\n", "§\n"],
            steer_chunks=[["§\nB1\n§\n"]],
            delay=0.05,
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="A",
                store=self.store, on_unit=on_unit,
            )
        )
        await asyncio.wait_for(first.wait(), 3)
        self.assertTrue(await client.steer(session_key="k1", text="STEER-B"))
        text = await asyncio.wait_for(task, 5)

        self.assertEqual(units, ["A1", "A2", "B1"])
        self.assertEqual(text, "§\nA1\n§\nA2\n§\n§\nB1\n§\n")
        prompts = self.entries("session/prompt")
        self.assertEqual(len(prompts), 2)
        self.assertEqual(prompts[1]["params"]["prompt"], [{"type": "text", "text": "STEER-B"}])
        self.assertEqual(
            prompts[0]["params"]["sessionId"], prompts[1]["params"]["sessionId"],
            "steer 必须落在同一个 ACP session",
        )

    async def test_consecutive_b_c_steer_keeps_arrival_order(self) -> None:
        units: list[str] = []
        first = asyncio.Event()
        b1 = asyncio.Event()

        def on_unit(decision: dict) -> None:
            units.append(decision["body"])
            if decision["body"] == "A1":
                first.set()
            if decision["body"] == "B1":
                b1.set()

        client = self.make_client(
            chunks=["§\nA1\n§\n", "A2\n", "§\n"],
            steer_chunks=[
                ["§\nB1\n§\n", "B2\n", "§\n"],
                ["§\nC1\n§\n"],
            ],
            delay=0.05,
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="A",
                store=self.store, on_unit=on_unit,
            )
        )
        await asyncio.wait_for(first.wait(), 3)
        self.assertTrue(await client.steer(session_key="k1", text="STEER-B"))
        await asyncio.wait_for(b1.wait(), 3)
        self.assertTrue(await client.steer(session_key="k1", text="STEER-C"))
        await asyncio.wait_for(task, 5)

        self.assertEqual(units, ["A1", "A2", "B1", "B2", "C1"])
        prompts = self.entries("session/prompt")
        self.assertEqual(
            [p["params"]["prompt"][0]["text"] for p in prompts],
            ["A", "STEER-B", "STEER-C"],
        )
        self.assertEqual(
            len({p["params"]["sessionId"] for p in prompts}), 1,
            "连续 steer 都复用同一 ACP session",
        )

    async def test_same_body_in_a_new_turn_is_delivered_again(self) -> None:
        units: list[str] = []
        first = asyncio.Event()

        def on_unit(decision: dict) -> None:
            units.append(decision["body"])
            if len(units) == 1:
                first.set()

        client = self.make_client(
            chunks=["§\nSAME\n§\n"],
            steer_chunks=[["§\nSAME\n§\n"]],
            delay=0.05,
            ack_wait=0.6,
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="A",
                store=self.store, on_unit=on_unit,
            )
        )
        await asyncio.wait_for(first.wait(), 3)
        self.assertTrue(await client.steer(session_key="k1", text="STEER-B"))
        await asyncio.wait_for(task, 5)
        self.assertEqual(
            units, ["SAME", "SAME"], "段内去重不等于跨轮去重：新一轮同文是两条消息"
        )

    async def test_permission_reject_does_not_block_streamed_body(self) -> None:
        units: list[str] = []
        client = self.make_client(
            chunks=["§\nA1\n§\n"],
            delay=0.02,
            extra_flags=("--permission",),
        )
        text = await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi",
            store=self.store, on_unit=lambda d: units.append(d["body"]),
        )
        self.assertEqual(text, "§\nA1\n§\n")
        self.assertEqual(units, ["A1"])
        responses = [
            item for item in self.wire()
            if item.get("id") == 9000 and "method" not in item
        ]
        self.assertEqual(len(responses), 1)
        self.assertEqual(
            responses[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
        )

    async def test_steer_without_active_turn_returns_false(self) -> None:
        client = self.make_client(chunks=["§\nA1\n§\n"])
        self.assertFalse(await client.steer(session_key="missing", text="B"))


class HandlerStreamTests(unittest.IsolatedAsyncioTestCase):
    """真实 handler→HermesClient→outbound 链路：流式正文与 steer 接管。"""

    def setUp(self) -> None:
        from contextlib import ExitStack
        from unittest.mock import AsyncMock, patch

        from nonebot_plugin_hermes_adapter.config import plugin_config

        self._prev_interface = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__,
            plugin_config,
            "hermes_api_interface",
            self._prev_interface,
        )

        self._tmp = tempfile.TemporaryDirectory(prefix="acp-stream-handler-")
        self.addCleanup(self._tmp.cleanup)
        self.base = Path(self._tmp.name)
        self._prev_custom_dir = plugin_config.hermes_custom_dir
        object.__setattr__(plugin_config, "hermes_custom_dir", str(self.base / "config"))
        self.addCleanup(
            object.__setattr__,
            plugin_config,
            "hermes_custom_dir",
            self._prev_custom_dir,
        )

        from nonebot_plugin_hermes_adapter import mcp
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter.core import outbound
        from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager
        from nonebot_plugin_hermes_adapter.core.chat_config import ChatConfig
        from nonebot_plugin_hermes_adapter.core.inflight import InflightRegistry
        from nonebot_plugin_hermes_adapter.core.media_config import MediaConfig
        from nonebot_plugin_hermes_adapter.core.misc_config import MiscConfig
        from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker
        from nonebot_plugin_hermes_adapter.core.session import SessionManager
        from nonebot_plugin_hermes_adapter.handlers import message_relay as relay
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        self.mcp = mcp
        self.hc = hc
        self.mt = mt
        self.relay = relay
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
                get_bot_nickname=AsyncMock(return_value="StreamBot"),
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

        self.stack.enter_context(
            patch.object(outbound.alconna.UniMessage, "send", platform_send)
        )
        # 只把 3 秒节奏换成即时，队列语义与发送顺序不变。
        async def instant(_delay):
            await asyncio.sleep(0)

        self.stack.enter_context(patch.object(hc, "_runs_reply_sleep", instant))
        self.stack.enter_context(
            patch.object(hc, "_get_http_client", side_effect=AssertionError("HTTP forbidden"))
        )

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    # ---------------------------------------------------------------- helpers
    def _client(self, *, chunks, steer_chunks=None, delay=0.05, ack_wait=0.0):
        from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag

        log = self.base / "wire.jsonl"
        flags = [
            f"--chunks={json.dumps(chunks, ensure_ascii=False)}",
            f"--chunk-delay={delay}",
        ]
        if steer_chunks is not None:
            flags.append(f"--steer-chunks={json.dumps(steer_chunks, ensure_ascii=False)}")
        if ack_wait:
            flags.append(f"--ack-wait={ack_wait}")
        client = ag.AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=[str(FIXTURE), str(log), json.dumps(""), *flags],
            workspace=self.base,
            prompt_timeout=10,
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

    def _prompts(self) -> list[dict]:
        return [item for item in self._wire() if item.get("method") == "session/prompt"]

    def _row(self, *, private: bool, content: str, mid: int = 1):
        from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
        from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin

        return BufferedMessage(
            ts=mid, adapter="onebotv11", group_id=None if private else "100",
            user_id="123", nickname="StreamUser", content=content,
            origin=MessageOrigin.human(), id=mid,
            private_owner_id="123" if private else None,
        )

    async def _turn(self, route: str, row):
        import time

        private = route == "private"
        group = None if private else "100"
        kwargs = dict(
            bot=self.bot,
            target=SimpleNamespace(private=private, id="123" if private else group),
            adapter_name="onebotv11",
            user_id="123",
            group_id=group,
            text=row.content,
            image_urls=[],
            now_ms=int(time.time() * 1000),
            current_buffered=row,
        )
        if route == "reactive":
            return await self.mt._run_reactive_turn(
                **kwargs, is_direct=True, addressed_to_bot=True, ephemeral=True
            )
        return await self.mt._run_passive_turn(**kwargs, is_private=private)

    async def _steer(self, *, private: bool, content: str, mid: int) -> bool:
        import time

        row = self._row(private=private, content=content, mid=mid)
        return await self.relay._maybe_steer_direct(
            target=SimpleNamespace(private=private, id="123" if private else "100"),
            adapter_name="onebotv11",
            user_id="123",
            nickname="StreamUser",
            group_id=None if private else "100",
            msg_text=content,
            is_direct=True,
            now_ms=int(time.time() * 1000),
            origin=row.origin,
            rows=[row],
        )

    async def _wait_for(self, predicate, task=None, timeout=4.0, what="condition"):
        import time

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            if task is not None and task.done():
                await task
                if predicate():
                    return
                raise AssertionError(f"{what} 未出现，turn 已结束: platform={self.platform}")
            await asyncio.sleep(0.01)
        raise AssertionError(f"{what} 未出现: platform={self.platform}")

    async def _clear(self, *, private: bool) -> None:
        from unittest.mock import patch

        from nonebot_plugin_hermes_adapter.handlers import commands

        group = None if private else "100"
        target = SimpleNamespace(private=private, id="123" if private else group)
        with (
            patch.object(commands, "session_manager", self.manager),
            patch.object(commands.alconna, "get_target", return_value=target),
            patch.object(commands, "check_isolation", return_value=True),
            patch.object(commands, "get_adapter_name", return_value="onebotv11"),
            patch.object(self.mcp, "background_report_outbox", None),
        ):
            await commands.handle_clear(
                self.bot,
                SimpleNamespace(get_user_id=lambda: "123"),
                SimpleNamespace(),
            )

    # ------------------------------------------------------------------ cases
    async def test_private_stream_reaches_platform_before_turn_end(self) -> None:
        self._client(chunks=["§\nA1\n§\n", "A2\n", "§\n"], delay=0.1)
        task = asyncio.create_task(self._turn("private", self._row(private=True, content="hi")))
        await self._wait_for(lambda: self.platform == ["A1"], task=task, what="A1")
        self.assertFalse(task.done(), "首份正文到达平台时 turn 不得已经收尾")
        await asyncio.wait_for(task, 10)
        self.assertEqual(self.platform, ["A1", "A2"])

    async def test_private_steer_b_takes_over_the_same_acp_session(self) -> None:
        self._client(
            chunks=["§\nA1\n§\n", "A2\n", "§\n"],
            steer_chunks=[["§\nB1\n§\n"]],
            delay=0.1,
        )
        task = asyncio.create_task(self._turn("private", self._row(private=True, content="A")))
        await self._wait_for(lambda: self.platform == ["A1"], task=task, what="A1")
        self.assertTrue(await self._steer(private=True, content="插一句 B", mid=2))
        await asyncio.wait_for(task, 10)

        self.assertEqual(self.platform, ["A1", "A2", "B1"])
        prompts = self._prompts()
        self.assertEqual(len(prompts), 2)
        steer_text = prompts[1]["params"]["prompt"][0]["text"]
        self.assertIn("插一句 B", steer_text)
        self.assertIn("m:2", steer_text, "steer 呈递必须保留插件 m:id")
        self.assertEqual(
            prompts[0]["params"]["sessionId"], prompts[1]["params"]["sessionId"]
        )

    async def test_group_passive_stream_is_delivered_once(self) -> None:
        self._client(chunks=["§\nP1\n§\n", "P2\n§\n"], delay=0.05)
        task = asyncio.create_task(
            self._turn("passive", self._row(private=False, content="@bot hi"))
        )
        await self._wait_for(lambda: self.platform == ["P1"], task=task, what="P1")
        await asyncio.wait_for(task, 10)
        self.assertEqual(self.platform, ["P1", "P2"])

    async def test_group_reactive_stream_and_b_c_steer_keep_order(self) -> None:
        self._client(
            chunks=["§\nA1\n§\n", "A2\n", "§\n"],
            steer_chunks=[["§\nB1\n§\n", "B2\n", "§\n"], ["§\nC1\n§\n"]],
            delay=0.1,
        )
        task = asyncio.create_task(
            self._turn("reactive", self._row(private=False, content="@bot 开工"))
        )
        await self._wait_for(lambda: self.platform == ["A1"], task=task, what="A1")
        self.assertTrue(await self._steer(private=False, content="插 B", mid=2))
        await self._wait_for(lambda: "B1" in self.platform, task=task, what="B1")
        self.assertTrue(await self._steer(private=False, content="插 C", mid=3))
        await asyncio.wait_for(task, 10)

        self.assertEqual(self.platform, ["A1", "A2", "B1", "B2", "C1"])
        prompts = self._prompts()
        self.assertEqual(len(prompts), 3)
        texts = [p["params"]["prompt"][0]["text"] for p in prompts]
        self.assertIn("插 B", texts[1])
        self.assertIn("插 C", texts[2])
        self.assertEqual(len({p["params"]["sessionId"] for p in prompts}), 1)

    async def test_silent_turn_sends_nothing(self) -> None:
        self._client(chunks=["§\n§"], delay=0.02)
        result = await asyncio.wait_for(
            self._turn("private", self._row(private=True, content="hi")), 10
        )
        self.assertEqual(self.platform, [])
        self.assertEqual(result.structured, {"should_reply": False})
        self.assertEqual(result.terminal_decisions, [])

    async def test_clear_fences_remaining_stream_units_of_old_generation(self) -> None:
        self._client(chunks=["§\nOLD1\n§\n", "OLD2\n", "§\n"], delay=0.35)
        task = asyncio.create_task(
            self._turn("private", self._row(private=True, content="old turn"))
        )
        await self._wait_for(lambda: self.platform == ["OLD1"], task=task, what="OLD1")
        await self._clear(private=True)
        clear_receipt = self.platform[-1]
        self.assertIn("会话已重置", clear_receipt)
        await asyncio.wait_for(task, 10)
        self.assertEqual(
            [item for item in self.platform if item != clear_receipt], ["OLD1"],
            "clear 之后仍在途的旧代正文不得再出站（终态补发同样受代际栅栏）",
        )

        new_client = self._client(chunks=["§\nNEW1\n§\n"], delay=0.02)
        await asyncio.wait_for(
            self._turn("private", self._row(private=True, content="new turn", mid=9)), 10
        )
        self.assertEqual(
            [item for item in self.platform if item != clear_receipt],
            ["OLD1", "NEW1"],
        )
        self.assertEqual(len(new_client._turns), 0, "新代轮次收尾后不得残留接管状态")


if __name__ == "__main__":
    unittest.main()
