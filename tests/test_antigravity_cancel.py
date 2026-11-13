"""阶段2B：ACP 取消（/run-stop）离线 TDD。

客户端协议层用假 `agy acp` NDJSON 子进程；handler 层走真
`_run_passive_turn` / `_run_reactive_turn` → 真 `AntigravityACPClient` →
真 `handle_run_stop` → 真出站。只替换平台发送边界、管理员白名单与隔离配置。

覆盖：有／无在跑轮的 stop、无 id `session/cancel`、`stopReason=cancelled` →
`is_interrupted` 硬静默、reply_queue 未发余量丢弃、steer B/C 途中取消、权限等待中
取消、取消后新 prompt 复用同一 ACP 进程与会话。
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
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from tests.acp_2b_harness import (  # noqa: E402
    ACPHandlerHarness,
    wait_until as _wait_until,
)

from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

FIXTURE = ROOT / "tests" / "fixtures" / "fake_agy_acp.py"


class ACPCancelClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="acp-cancel-")
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log_path = self.tmp / "wire.jsonl"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self.clients = []
        self.units: list[str] = []
        self.payloads: list[dict] = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    def make_client(self, *, chunks, steer_chunks=None, delay=0.2, flags=(), prompt_timeout=10.0, permission_timeout=None):
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            AntigravityACPClient,
        )

        args = [
            str(FIXTURE),
            str(self.log_path),
            json.dumps("§\nA1\n§\nA2\n§", ensure_ascii=False),
            f"--chunks={json.dumps(chunks, ensure_ascii=False)}",
            f"--chunk-delay={delay}",
            *flags,
        ]
        if steer_chunks is not None:
            args.append(f"--steer-chunks={json.dumps(steer_chunks, ensure_ascii=False)}")
        client = AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=args,
            workspace=self.tmp,
            prompt_timeout=prompt_timeout,
            rpc_timeout=5.0,
            permission_timeout=permission_timeout,
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

    def _on_unit(self, decision: dict) -> None:
        self.units.append(decision["body"])

    async def _notify(self, payload: dict) -> None:
        self.payloads.append(payload)

    async def test_cancel_running_turn_sends_id_less_notice_and_reports_cancelled(self) -> None:
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            ACPCancelledError,
        )

        client = self.make_client(chunks=["§\nA1\n§\n", "A2\n", "§\n"])
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, on_unit=self._on_unit,
            )
        )
        await _wait_until(lambda: self.units == ["A1"], what="首份正文")

        self.assertTrue(await client.cancel("k1"))
        with self.assertRaises(ACPCancelledError):
            await asyncio.wait_for(task, 8)

        cancels = [item for item in self.wire() if item.get("method") == "session/cancel"]
        self.assertEqual(len(cancels), 1, cancels)
        self.assertNotIn("id", cancels[0], "session/cancel 必须是无 id 通知")
        self.assertEqual(cancels[0]["params"], {"sessionId": self.store.load_acp_session("k1")[0]})
        self.assertEqual(self.units, ["A1"], "取消后不得继续投递未发余量")

    async def test_cancel_without_a_running_turn_returns_false(self) -> None:
        client = self.make_client(chunks=["§\nA1\n§\n"])
        # 进程都还没起：不能为了发 cancel 去启动 agy。
        self.assertFalse(await client.cancel("k1"))
        self.assertEqual(self.wire(), [])

        await asyncio.wait_for(
            client.prompt(session_key="k1", system_prompt="SYS", user_text="hi", store=self.store),
            8,
        )
        # 轮已收尾：没有在跑对象，同样 False。
        self.assertFalse(await client.cancel("k1"))
        self.assertEqual(
            [item for item in self.wire() if item.get("method") == "session/cancel"], []
        )

    async def test_cancel_while_waiting_for_permission_rejects_and_ends_the_turn(self) -> None:
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            ACPCancelledError,
        )

        client = self.make_client(
            chunks=["§\nA1\n§\n", "A2\n", "§\n"],
            flags=("--permission", "--permission-id=p-cancel"),
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, on_unit=self._on_unit,
                approval_callback=self._notify,
            )
        )
        await _wait_until(lambda: bool(self.payloads), what="权限登记")
        self.assertFalse(task.done())

        self.assertTrue(await client.cancel("k1"))
        with self.assertRaises(ACPCancelledError):
            await asyncio.wait_for(task, 8)

        answers = [
            item
            for item in self.wire()
            if item.get("id") == "p-cancel" and "method" not in item
        ]
        self.assertEqual(len(answers), 1, answers)
        self.assertEqual(
            answers[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
            "权限等待中取消必须按真实 reject_once 关闭原 RPC",
        )

    async def test_steer_b_and_c_in_flight_cancel_ends_the_whole_wait_chain(self) -> None:
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            ACPCancelledError,
        )

        client = self.make_client(
            chunks=["§\nA1\n§\n", "A2\n", "§\n"],
            steer_chunks=[["§\nB1\n§\n"], ["§\nC1\n§\n"]],
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="A",
                store=self.store, on_unit=self._on_unit,
            )
        )
        await _wait_until(lambda: self.units == ["A1"], what="首份正文")
        self.assertTrue(await client.steer(session_key="k1", text="B"))
        self.assertTrue(await client.steer(session_key="k1", text="C"))

        self.assertTrue(await client.cancel("k1"))
        with self.assertRaises(ACPCancelledError):
            await asyncio.wait_for(task, 8)
        self.assertEqual(self.units, ["A1"], "B/C 未开始的正文不得在取消后补发")

    async def test_prompt_after_cancel_reuses_the_same_process_and_session(self) -> None:
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            ACPCancelledError,
        )

        client = self.make_client(chunks=["§\nA1\n§\n", "A2\n", "§\n"])
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="one",
                store=self.store, on_unit=self._on_unit,
            )
        )
        await _wait_until(lambda: self.units == ["A1"], what="首份正文")
        self.assertTrue(await client.cancel("k1"))
        with self.assertRaises(ACPCancelledError):
            await asyncio.wait_for(task, 8)

        self.units.clear()
        text = await asyncio.wait_for(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="two",
                store=self.store, on_unit=self._on_unit,
            ),
            8,
        )
        self.assertEqual(text, "§\nA1\n§\nA2\n§\n")
        self.assertEqual(self.units, ["A1", "A2"])
        self.assertIsNotNone(client._proc, "取消后 ACP 进程必须保持存活可复用")
        prompts = [item for item in self.wire() if item.get("method") == "session/prompt"]
        self.assertEqual(len(prompts), 2)
        self.assertEqual(
            prompts[0]["params"]["sessionId"], prompts[1]["params"]["sessionId"]
        )


    async def test_prompt_timeout_rejects_pending_permission_before_cleanup(self) -> None:
        """prompt 超时收尾必须先用既有安全 reject 结清原权限 RPC。

        根因：prompt 超时与权限到期默认同为 300s，权限 timer 启动更晚；prompt 超时
        只发 cancel，随后 finalize 撤销 timer 并丢权限 → 原权限 RPC 永远没人应答。
        """
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            ACPTimeoutError,
        )

        client = self.make_client(
            chunks=["§\nA1\n§\n"],
            flags=("--permission", "--permission-id=timeout-id"),
            prompt_timeout=0.2,
            permission_timeout=0.2,
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, approval_callback=self._notify,
            )
        )
        await _wait_until(lambda: bool(self.payloads), what="权限登记")
        with self.assertRaises(ACPTimeoutError):
            await asyncio.wait_for(task, 8)

        # 再等一个权限到期窗口：收尾之后再补应答也算不合格（timer 已被撤销）。
        await asyncio.sleep(0.3)
        answers = [
            item
            for item in self.wire()
            if item.get("id") == "timeout-id" and "method" not in item
        ]
        self.assertEqual(len(answers), 1, f"原权限 RPC 必须收到一次安全拒绝: {answers}")
        self.assertEqual(
            answers[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
            "超时收尾只能按真实 reject_once 关闭原权限 RPC",
        )
        self.assertEqual(client._permissions, {})
        self.assertEqual(client._turns, {})


# --- 真 handler 的 /run-stop ------------------------------------------------


class ACPRunStopHandlerTests(ACPHandlerHarness):
    async def test_run_stop_cancels_running_turn_and_hard_silences_it(self) -> None:
        self._client(chunks=["§\nA1\n§\n", "A2\n", "§\n"], delay=0.2)
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.platform == ["A1"], what="首份正文到达平台")

        await self._run_stop()
        result = await asyncio.wait_for(task, 10)

        self.assertTrue(result.is_interrupted)
        receipts = self.receipts()
        self.assertEqual(len(receipts), 1, f"只能有一条命令回执: {self.platform}")
        self.assertIn("已取消", receipts[0])
        self.assertNotIn("run-", receipts[0], "ACP 回执不得出现伪造 run_id")
        self.assertNotIn("A2", self.platform, "取消后 reply_queue 未发余量必须丢弃")
        self.assertNotIn("⚠️", " ".join(self.platform), "被停 turn 走硬静默,不发兜底文本")

        cancels = self.rows("session/cancel")
        self.assertEqual(len(cancels), 1, cancels)
        self.assertNotIn("id", cancels[0])
        self.assertEqual(cancels[0]["params"]["sessionId"], self.store.load_acp_session(self.session_key())[0])

    async def test_group_run_stop_does_not_cancel_a_private_turn(self) -> None:
        self._client(chunks=["§\nA1\n§\n", "A2\n", "§\n"], delay=0.2)
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.platform == ["A1"], what="首份正文到达平台")

        await self._run_stop(private=False)
        self.assertIn("没有", self.receipts()[-1])
        self.assertEqual(self.rows("session/cancel"), [], "别的会话的轮不得被取消")
        self.assertFalse(task.done())

        await self._run_stop()
        await asyncio.wait_for(task, 10)

    async def test_run_stop_without_running_turn_reports_no_object(self) -> None:
        self._client(chunks=["§\nA1\n§\n"])
        await self._run_stop()
        self.assertEqual(len(self.platform), 1, self.platform)
        self.assertIn("没有", self.platform[0])
        self.assertEqual(self.rows("session/cancel"), [], "没有在跑轮不得发 cancel")

    async def test_new_prompt_after_run_stop_reuses_the_acp_session(self) -> None:
        self._client(chunks=["§\nA1\n§\n", "A2\n", "§\n"], delay=0.2)
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.platform == ["A1"], what="首份正文到达平台")
        await self._run_stop()
        await asyncio.wait_for(task, 10)

        self.platform.clear()
        result = await asyncio.wait_for(self._turn(self._row(mid=2, content="after")), 10)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(self.platform, ["A1", "A2"], "取消后新 prompt 必须正常复用会话")

        prompts = self.rows("session/prompt")
        self.assertEqual(len(prompts), 2)
        self.assertEqual(
            prompts[0]["params"]["sessionId"], prompts[1]["params"]["sessionId"]
        )

    async def test_run_stop_after_body_overflow_keeps_interrupted_semantics(self) -> None:
        """正文错误不得覆盖之后的真实 cancelled。

        根因：已记录 body_overflow 后先抛普通 ACPError，取消结果被覆盖成
        interrupted=False/transport_error=True → 被停轮还会发兜底文本。
        """
        client = self._client(
            chunks=["§\n", "X" * 4001 + "\n§\n", "TAIL\n§\n"], delay=0.2
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: bool(client._text_errors), what="流式正文单位不合法")
        self.assertEqual(self.platform, [], "坏正文不得部分出站")

        await self._run_stop()
        result = await asyncio.wait_for(task, 10)

        self.assertTrue(result.is_interrupted, "取消结果必须优先于正文错误")
        self.assertFalse(result.is_transport_error)
        self.assertEqual(
            [text for text in self.platform if not text.startswith("🛑")],
            [],
            "被停轮必须硬静默，不落 transport-fallback",
        )
        # 正文错误日志保留（不是静默吞掉），只是不再改写终态。
        self.assertEqual(client._text_errors, {})

    async def test_run_stop_after_body_overflow_drops_paced_queue(self) -> None:
        """已有 SENT + 被节奏挡住的 QUEUED 时取消：等 ACP 收尾后 QUEUED 仍不得出站。"""
        client = self._client(
            chunks=["§\nSENT\n§\nQUEUED\n§\n", "X" * 4001 + "\n§\n", "TAIL\n§\n"],
            delay=0.2,
        )
        sleeping = asyncio.Event()
        release = asyncio.Event()

        async def pace(_delay):
            sleeping.set()
            await release.wait()

        self.stack.enter_context(patch.object(self.hc, "_runs_reply_sleep", pace))
        task = asyncio.create_task(self._turn())
        await asyncio.wait_for(sleeping.wait(), 5)
        await _wait_until(lambda: bool(client._text_errors), what="流式正文单位不合法")
        self.assertEqual(self.platform, ["SENT"])

        await self._run_stop()
        # 等真实的 cancelled 应答被读到且 ACP 已收尾，再放行节奏队列：不是取消未生效的竞争。
        await _wait_until(lambda: not client._turns, what="cancelled 收尾")
        await asyncio.sleep(0.05)
        release.set()
        result = await asyncio.wait_for(task, 10)

        self.assertTrue(result.is_interrupted)
        self.assertEqual(
            [text for text in self.platform if not text.startswith("🛑")],
            ["SENT"],
            "取消后既有队列余量不得再出站",
        )

    async def test_body_overflow_without_cancel_stays_transport_error(self) -> None:
        """未取消的普通超长正文仍原样 fail-closed（不放宽协议）。"""
        self._client(chunks=["§\n" + "X" * 4001 + "\n§\n"], delay=0.01)
        result = await asyncio.wait_for(self._turn(), 10)
        self.assertTrue(result.is_transport_error)
        self.assertFalse(result.is_interrupted)
        self.assertEqual(self.platform, ["transport-fallback"])
