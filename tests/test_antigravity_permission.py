"""阶段2B：ACP 权限审批（once／deny）离线 TDD。

链路是候选的真实路径：假 `agy acp` NDJSON 子进程 → 真
`AntigravityACPClient` → 真 `HermesClient.chat` → 真 handler
（`_run_passive_turn`）→ 真 `core.approvals` → 真 `handle_approve`。
只替换官方平台发送边界、管理员白名单与隔离配置；不发真实 QQ、不跑真实模型。

覆盖：原 RPC id／类型、按真实 options kind 选 allow_once／reject_once、成功才删单、
无 callback 安全拒绝、过期／重复／未知请求失败关闭、scope 隔离、ACP 命令面只有
once|deny、其他输入零后端调用并留单、通知失败留单、Runs 路径回归、重启不恢复 ACP。
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
    permission_frames as _permission_frames,
    wait_until as _wait_until,
)

from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

FIXTURE = ROOT / "tests" / "fixtures" / "fake_agy_acp.py"


# --- 客户端协议层 ---------------------------------------------------------


class ACPPermissionClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="acp-perm-")
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log_path = self.tmp / "wire.jsonl"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self.clients = []
        self.payloads: list[dict] = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    def make_client(self, *, flags=(), permission_timeout=None):
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

    async def _notify(self, payload: dict) -> None:
        self.payloads.append(payload)

    async def test_once_waits_for_admin_and_answers_allow_once_at_the_original_id(self) -> None:
        client = self.make_client(
            flags=("--permission", "--permission-id=jetski-acp-front:2")
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
        await _wait_until(lambda: bool(self.payloads), what="审批登记")
        payload = self.payloads[0]
        self.assertEqual(payload["backend"], "antigravity")
        self.assertEqual(payload["session_key"], "k1")
        self.assertEqual(payload["request_id"], "jetski-acp-front:2")
        self.assertEqual(payload["title"], "write_file")
        self.assertEqual(payload["raw_input"], {"CommandLine": "pwd"})
        self.assertEqual(
            [option["kind"] for option in payload["options"]],
            ["allow_once", "reject_once"],
        )
        # 管理员还没答：原 RPC 不能被抢先回答。
        self.assertEqual(_permission_frames(self.wire(), "jetski-acp-front:2"), [])
        self.assertFalse(task.done())

        self.assertTrue(
            await client.answer_permission(
                session_key="k1", request_id="jetski-acp-front:2", choice="once"
            )
        )
        text = await asyncio.wait_for(task, 6)
        self.assertEqual(text, "§\n第一句\n§\n第二句\n§")

        frames = _permission_frames(self.wire(), "jetski-acp-front:2")
        self.assertEqual(len(frames), 1, frames)
        self.assertIsInstance(frames[0]["id"], str, "原 RPC id 类型必须保留")
        self.assertEqual(
            frames[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "allow-once"},
        )

    async def test_deny_answers_reject_once(self) -> None:
        client = self.make_client(flags=("--permission", "--permission-id=p-9"))
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, approval_callback=self._notify,
            )
        )
        await _wait_until(lambda: bool(self.payloads), what="审批登记")
        self.assertTrue(
            await client.answer_permission(session_key="k1", request_id="p-9", choice="deny")
        )
        await asyncio.wait_for(task, 6)

        frames = _permission_frames(self.wire(), "p-9")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(
            frames[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
        )

    async def test_missing_callback_is_safely_rejected_without_registration(self) -> None:
        client = self.make_client(flags=("--permission", "--permission-id=p-3"))
        text = await asyncio.wait_for(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
            ),
            6,
        )
        self.assertEqual(text, "§\n第一句\n§\n第二句\n§")
        self.assertEqual(self.payloads, [])
        self.assertEqual(
            _permission_frames(self.wire(), "p-3")[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
        )

    async def test_duplicate_unknown_and_wrong_scope_answers_fail_closed(self) -> None:
        client = self.make_client(flags=("--permission", "--permission-id=p-7"))
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, approval_callback=self._notify,
            )
        )
        await _wait_until(lambda: bool(self.payloads), what="审批登记")

        # scope 隔离：另一个 session_key 不能回答本会话的请求。
        self.assertFalse(
            await client.answer_permission(session_key="k2", request_id="p-7", choice="once")
        )
        # 未知 request：失败关闭且不写任何帧。
        self.assertFalse(
            await client.answer_permission(session_key="k1", request_id="p-unknown", choice="once")
        )
        self.assertEqual(_permission_frames(self.wire(), "p-7"), [])

        self.assertTrue(
            await client.answer_permission(session_key="k1", request_id="p-7", choice="once")
        )
        # 重复应答：第二次失败，且不产生第二条帧。
        self.assertFalse(
            await client.answer_permission(session_key="k1", request_id="p-7", choice="deny")
        )
        await asyncio.wait_for(task, 6)
        self.assertEqual(len(_permission_frames(self.wire(), "p-7")), 1)

    async def test_expired_permission_is_rejected_and_late_answer_fails(self) -> None:
        client = self.make_client(
            flags=("--permission", "--permission-id=p-exp"),
            permission_timeout=0.05,
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, approval_callback=self._notify,
            )
        )
        await _wait_until(lambda: bool(self.payloads), what="审批登记")
        await _wait_until(
            lambda: bool(_permission_frames(self.wire(), "p-exp")), what="过期安全拒绝"
        )
        frames = _permission_frames(self.wire(), "p-exp")
        self.assertEqual(
            frames[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
        )
        self.assertFalse(
            await client.answer_permission(session_key="k1", request_id="p-exp", choice="once")
        )
        await asyncio.wait_for(task, 6)
        self.assertEqual(len(_permission_frames(self.wire(), "p-exp")), 1)

    async def test_options_without_allow_once_kind_cannot_be_answered_as_once(self) -> None:
        options = [{"optionId": "reject-only", "name": "Reject", "kind": "reject_once"}]
        client = self.make_client(
            flags=(
                "--permission",
                "--permission-id=p-k",
                f"--permission-options={json.dumps(options)}",
            )
        )
        task = asyncio.create_task(
            client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi",
                store=self.store, approval_callback=self._notify,
            )
        )
        await _wait_until(lambda: bool(self.payloads), what="审批登记")

        # 真实 options 没有 allow_once：不能硬猜 optionId，条目应留在原处。
        self.assertFalse(
            await client.answer_permission(session_key="k1", request_id="p-k", choice="once")
        )
        self.assertEqual(_permission_frames(self.wire(), "p-k"), [])
        self.assertTrue(
            await client.answer_permission(session_key="k1", request_id="p-k", choice="deny")
        )
        await asyncio.wait_for(task, 6)
        self.assertEqual(
            _permission_frames(self.wire(), "p-k")[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-only"},
        )


# --- approvals 队列与命令面（不启动 ACP 进程） -----------------------------


class ApprovalQueueBackendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.core import approvals

        self.approvals = approvals
        self._tmp = tempfile.TemporaryDirectory(prefix="acp-approvals-")
        self.addCleanup(self._tmp.cleanup)
        self.base = Path(self._tmp.name)
        self._prev_pending = dict(approvals._pending)
        self._prev_store = approvals._store_path
        approvals._pending.clear()
        approvals._store_path = self.base / "pending_approvals.json"
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        self.approvals._pending.clear()
        self.approvals._pending.update(self._prev_pending)
        self.approvals._store_path = self._prev_store

    def test_acp_entry_is_persisted_with_backend_and_scope_but_runs_is_unchanged(self) -> None:
        entry = self.approvals.add_pending(
            run_id="",
            adapter="onebotv11",
            scope_desc="私聊 123",
            command="pwd",
            choices=["once", "deny"],
            request_id="jetski-acp-front:2",
            backend="antigravity",
            session_key="hermes-onebotv11+private+123",
        )
        rows = json.loads((self.base / "pending_approvals.json").read_text(encoding="utf-8"))
        self.assertEqual(rows[0]["backend"], "antigravity")
        self.assertEqual(rows[0]["session_key"], "hermes-onebotv11+private+123")
        self.assertEqual(entry.run_id, "")

        runs = self.approvals.add_pending(
            run_id="run-1", adapter="onebotv11", scope_desc="群 100",
            command="ls", choices=["once", "session", "always", "deny"],
        )
        self.assertEqual(runs.backend, "runs")
        self.assertIsNone(runs.session_key)
        notice = self.approvals.build_admin_notice(runs)
        self.assertIn("/approve session 本会话放行", notice)
        self.assertIn("/approve always 永久放行", notice)

    def test_acp_notice_lists_only_once_and_deny(self) -> None:
        entry = self.approvals.add_pending(
            run_id="", adapter="onebotv11", scope_desc="私聊 123", command="pwd",
            choices=["once", "deny"], backend="antigravity",
            session_key="hermes-onebotv11+private+123",
        )
        notice = self.approvals.build_admin_notice(entry)
        self.assertIn("once / deny", notice)
        self.assertIn("/approve once 批准一次", notice)
        self.assertNotIn("session", notice)
        self.assertNotIn("always", notice)
        self.assertNotIn("run:", notice)

    def test_restart_does_not_restore_acp_entries(self) -> None:
        now = time.time()
        rows = [
            {
                "approval_id": 1,
                "run_id": "",
                "adapter": "onebotv11",
                "scope_desc": "私聊 123",
                "command": "pwd",
                "choices": ["once", "deny"],
                "is_private": True,
                "group_id": None,
                "request_id": "jetski-acp-front:2",
                "backend": "antigravity",
                "session_key": "hermes-onebotv11+private+123",
                "ts": now,
            },
            {
                "approval_id": 2,
                "run_id": "run-2",
                "adapter": "onebotv11",
                "scope_desc": "群 100",
                "command": "ls",
                "choices": ["once", "session", "always", "deny"],
                "is_private": False,
                "group_id": "100",
                "request_id": "",
                "ts": now,
            },
        ]
        path = self.base / "restart.json"
        path.write_text(json.dumps(rows), encoding="utf-8")

        restored = self.approvals.bind_storage(path)
        self.assertEqual(restored, 1, "旧 ACP 请求随旧子进程失效，重启不得装回")
        self.approvals._store_path = self.base / "pending_approvals.json"
        entry = self.approvals.peek_oldest()
        self.assertIsNotNone(entry)
        self.assertEqual(entry.run_id, "run-2")
        self.assertEqual(entry.backend, "runs")


# --- 真 handler → ACP → 审批 → /approve 命令 -------------------------------


class ACPApprovalHandlerTests(ACPHandlerHarness):
    """真 handler → ACP → 真审批队列 → 真 /approve 命令。"""

    async def test_once_answers_allow_once_and_deletes_only_after_success(self) -> None:
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=("--permission", "--permission-id=jetski-acp-front:2"),
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(
            lambda: self.approvals.pending_count() == 1, what="ACP 审批登记"
        )
        entry = self.approvals.peek_oldest()
        self.assertEqual(entry.backend, "antigravity")
        self.assertEqual(entry.run_id, "")
        self.assertEqual(entry.session_key, self.session_key())
        self.assertEqual(entry.request_id, "jetski-acp-front:2")
        self.assertEqual(entry.choices, ["once", "deny"])
        self.assertEqual(entry.command, "pwd")
        self.assertTrue(entry.is_private)

        notices = self.admin_notices()
        self.assertEqual(len(notices), 1, notices)
        self.assertIn("once / deny", notices[0])
        self.assertNotIn("session", notices[0])
        self.assertNotIn("always", notices[0])

        await self._approve("once")
        result = await asyncio.wait_for(task, 10)
        self.assertFalse(result.is_transport_error)
        bodies = [text for text in self.platform if text in ("第一句", "第二句")]
        self.assertEqual(bodies, ["第一句", "第二句"])
        self.assertEqual(self.approvals.pending_count(), 0, "成功应答后才删单")
        self.assertTrue(
            any("审批 #" in text for text in self.receipts()),
            f"管理员命令必须有回执: {self.platform}",
        )

        frames = _permission_frames(self.wire(), "jetski-acp-front:2")
        self.assertEqual(len(frames), 1, frames)
        self.assertIsInstance(frames[0]["id"], str)
        self.assertEqual(
            frames[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "allow-once"},
        )

    async def test_deny_answers_reject_once_and_keeps_body_flowing(self) -> None:
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=("--permission", "--permission-id=p-deny"),
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="ACP 审批登记")
        await self._approve("deny")
        result = await asyncio.wait_for(task, 10)
        bodies = [text for text in self.platform if text in ("第一句", "第二句")]
        self.assertEqual(bodies, ["第一句", "第二句"])
        self.assertFalse(result.is_transport_error)
        self.assertEqual(
            _permission_frames(self.wire(), "p-deny")[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
        )
        self.assertEqual(self.approvals.pending_count(), 0)

    async def test_other_choice_replies_acp_usage_only_and_keeps_entry(self) -> None:
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=("--permission", "--permission-id=p-usage"),
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="ACP 审批登记")
        wire_before = len(self.wire())

        await self._approve("session")
        receipt = self.receipts()[-1]
        self.assertIn("用法", receipt)
        self.assertIn("<once|deny>", receipt)
        self.assertNotIn("session", receipt)
        self.assertNotIn("always", receipt)
        self.assertNotIn("不支持", receipt)
        self.assertEqual(self.approvals.pending_count(), 1, "其他输入不得删单")
        self.assertEqual(len(self.wire()), wire_before, "其他输入零后端调用")
        self.assertFalse(task.done())

        await self._approve("deny")
        await asyncio.wait_for(task, 10)
        self.assertEqual(self.approvals.pending_count(), 0)

    async def test_non_admin_approve_is_silent_and_never_answers(self) -> None:
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=("--permission", "--permission-id=p-silent"),
        )
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="ACP 审批登记")
        wire_before = len(self.wire())
        platform_before = list(self.platform)

        await self._approve("once", user_id="777")
        self.assertEqual(self.platform, platform_before, "非管理员必须隐身")
        self.assertEqual(len(self.wire()), wire_before)
        self.assertEqual(self.approvals.pending_count(), 1)

        await self._approve("deny")
        await asyncio.wait_for(task, 10)

    async def test_notification_failure_keeps_entry_answerable(self) -> None:
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=("--permission", "--permission-id=p-nonotify"),
        )
        self.bot.call_api = AsyncMock(side_effect=RuntimeError("qq down"))
        task = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="ACP 审批登记")
        self.assertFalse(task.done(), "通知失败不能拖垮主 turn")

        await self._approve("once")
        result = await asyncio.wait_for(task, 10)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(self.approvals.pending_count(), 0)
        self.assertEqual(
            _permission_frames(self.wire(), "p-nonotify")[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "allow-once"},
        )

    async def _reactive_turn(self):
        row = self._row(private=False)
        return await self.mt._run_reactive_turn(
            bot=self.bot,
            target=SimpleNamespace(private=False, id="100"),
            adapter_name="onebotv11",
            user_id=self.ADMIN,
            group_id="100",
            text=row.content,
            image_urls=[],
            now_ms=int(time.time() * 1000),
            current_buffered=row,
            is_direct=True,
            addressed_to_bot=True,
            ephemeral=True,
        )

    async def test_group_reactive_permission_uses_the_group_scope_and_answers_once(self) -> None:
        self._client(
            reply="§\n第一句\n§\n第二句\n§",
            flags=("--permission", "--permission-id=p-group"),
        )
        task = asyncio.create_task(self._reactive_turn())
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="群 ACP 审批登记")
        entry = self.approvals.peek_oldest()
        self.assertEqual(entry.backend, "antigravity")
        self.assertEqual(
            entry.session_key,
            self.manager.get_session_key("onebotv11", False, self.ADMIN, "100"),
        )
        self.assertFalse(entry.is_private)
        self.assertIn("群 100", entry.scope_desc)
        self.assertNotIn("session", self.admin_notices()[0])

        await self._approve("once")
        result = await asyncio.wait_for(task, 10)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(
            [text for text in self.platform if text in ("第一句", "第二句")],
            ["第一句", "第二句"],
        )
        self.assertEqual(
            _permission_frames(self.wire(), "p-group")[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "allow-once"},
        )



# --- 返修：取消/超时结清后的共享审批 FIFO -----------------------------------


class ACPApprovalSettleRegressionTests(ACPHandlerHarness):
    """R1：已确认安全结清的 ACP 权限不得继续占共享审批 FIFO 队首。"""

    async def test_cancelled_permission_settles_entry_so_next_scope_can_answer(self) -> None:
        self._client(
            chunks=["§\nA1\n§\n"],
            flags=("--permission", "--permission-id=old-req"),
            delay=0.01,
        )
        first = asyncio.create_task(self._turn())
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="私聊权限登记")
        old = self.approvals.peek_oldest()
        self.assertEqual(old.request_id, "old-req")

        await self._run_stop()
        self.assertTrue((await asyncio.wait_for(first, 10)).is_interrupted)
        self.assertEqual(
            len(_permission_frames(self.wire(), "old-req")), 1, "取消必须已按原 id 安全拒绝"
        )
        self.assertEqual(
            self.approvals.pending_count(), 0, "已结清的旧单不得继续占 FIFO 队首"
        )

        # 另一个 scope 的新权限：第二台夹具进程用不同权限 id，便于分辨应答落在谁身上。
        self._client(
            chunks=["§\nG1\n§\n"],
            flags=("--permission", "--permission-id=group-req"),
            delay=0.01,
        )
        second = asyncio.create_task(self._turn(private=False))
        await _wait_until(lambda: self.approvals.pending_count() == 1, what="群权限登记")
        new = self.approvals.peek_oldest()
        self.assertNotEqual(new.approval_id, old.approval_id, "队首必须是新单")
        self.assertEqual(new.request_id, "group-req")

        await self._approve("once")
        result = await asyncio.wait_for(second, 10)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(self.approvals.pending_count(), 0)
        self.assertTrue(
            any(f"审批 #{new.approval_id} 已应答" in text for text in self.platform),
            f"/approve 必须答到新单而不是已结清旧单: {self.platform}",
        )
        frames = _permission_frames(self.wire(), "group-req")
        self.assertEqual(len(frames), 1, frames)
        self.assertEqual(
            frames[0]["result"]["outcome"],
            {"outcome": "selected", "optionId": "allow-once"},
        )

    async def test_settle_keeps_entry_when_reject_write_fails(self) -> None:
        """真正写帧失败不能当已结清：条目留单可重试，Runs 条目一律不动。"""
        from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag

        # 进程未启动：_send 直接失败 → 这是「真正发送失败」，不是「已结清」。
        client = self._client(reply="§\nOK\n§")
        options = [
            {"optionId": "allow-once", "kind": "allow_once"},
            {"optionId": "reject-once", "kind": "reject_once"},
        ]
        pending = ag.ACPPendingPermission(
            session_key="k1",
            session_id="sess-1",
            request_id="fail-req",
            options=options,
            expires_at=time.monotonic() + 60,
        )
        key = client._permission_key("k1", "fail-req")
        client._permissions[key] = pending
        acp_entry = self.approvals.add_pending(
            run_id="", adapter="onebotv11", scope_desc="私聊 123", command="pwd",
            choices=["once", "deny"], is_private=True, request_id="fail-req",
            backend="antigravity", session_key="k1",
        )

        self.assertFalse(await client._settle_permission(key, pending))
        self.assertEqual(self.approvals.pending_count(), 1, "写帧失败必须留单")
        self.assertIs(self.approvals.peek_oldest(), acp_entry)

        # 同 request_id 的 Runs 条目不受 ACP 结清影响。
        runs_entry = self.approvals.add_pending(
            run_id="run-1", adapter="onebotv11", scope_desc="私聊 123", command="ls",
            choices=["once"], is_private=True, request_id="fail-req", session_key="k1",
        )
        self.assertEqual(
            self.approvals.settle_permission(session_key="k1", request_id="fail-req"), 1
        )
        self.assertEqual(self.approvals.pending_count(), 1)
        self.assertIs(self.approvals.peek_oldest(), runs_entry)
        # 缺键时不删任何条目（Runs 老存档 request_id 为空）。
        self.assertEqual(
            self.approvals.settle_permission(session_key="k1", request_id=""), 0
        )
        self.assertEqual(self.approvals.pending_count(), 1)


# --- Runs 侧回归（审批／停止行为不变） -------------------------------------


class RunsApprovalRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config
        from nonebot_plugin_hermes_adapter.core import approvals
        from nonebot_plugin_hermes_adapter.handlers import commands

        self.commands = commands
        self.approvals = approvals
        self._prev_interface = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._prev_interface
        )
        self._tmp = tempfile.TemporaryDirectory(prefix="runs-approve-")
        self.addCleanup(self._tmp.cleanup)
        self._prev_pending = dict(approvals._pending)
        self._prev_store = approvals._store_path
        approvals._pending.clear()
        approvals._store_path = None
        self.addCleanup(self._restore)
        self.bot = SimpleNamespace(self_id="999", call_api=AsyncMock(return_value={}))
        self.platform: list[str] = []

        async def platform_send(msg, **kwargs):
            self.platform.append(str(msg))
            return SimpleNamespace(msg_ids=["1"])

        self._send_patch = patch.object(commands.alconna.UniMessage, "send", platform_send)
        self._send_patch.start()
        self.addCleanup(self._send_patch.stop)
        self._lists = SimpleNamespace(admin_users={"onebotv11:123"})
        self._lists_patch = patch.object(commands, "load_user_lists", lambda: self._lists)
        self._lists_patch.start()
        self.addCleanup(self._lists_patch.stop)

    def _restore(self) -> None:
        self.approvals._pending.clear()
        self.approvals._pending.update(self._prev_pending)
        self.approvals._store_path = self._prev_store

    async def _run(self, handler, *, raw):
        target = SimpleNamespace(private=True, id="123")
        event = SimpleNamespace(get_user_id=lambda: "123", get_plaintext=lambda: raw)
        with (
            patch.object(self.commands.alconna, "get_target", return_value=target),
            patch.object(self.commands, "check_isolation", return_value=True),
            patch.object(self.commands, "get_adapter_name", return_value="onebotv11"),
        ):
            await handler(self.bot, event, SimpleNamespace(skip=lambda: None))

    async def test_runs_approve_usage_and_failure_keep_entry(self) -> None:
        entry = self.approvals.add_pending(
            run_id="run-1", adapter="onebotv11", scope_desc="私聊 123",
            command="ls", choices=["once", "session", "always", "deny"],
            is_private=True,
        )
        with patch.object(
            self.commands.hermes_client, "respond_approval", AsyncMock(return_value=(True, "ok"))
        ) as respond:
            await self._run(
                self.commands.handle_approve,
                raw=f"/{self.commands._cmd('approve')} maybe",
            )
            self.assertIn("<once|session|always|deny>", self.platform[-1])
            respond.assert_not_called()
            self.assertEqual(self.approvals.pending_count(), 1)

            await self._run(
                self.commands.handle_approve,
                raw=f"/{self.commands._cmd('approve')} once",
            )
            respond.assert_awaited_once()
            kwargs = respond.await_args.kwargs
            self.assertEqual(kwargs["run_id"], "run-1")
            self.assertEqual(kwargs["choice"], "once")
            self.assertEqual(self.approvals.pending_count(), 0)
        self.assertEqual(entry.run_id, "run-1")

    async def test_runs_approve_backend_failure_keeps_entry(self) -> None:
        self.approvals.add_pending(
            run_id="run-2", adapter="onebotv11", scope_desc="私聊 123",
            command="ls", choices=["once", "session", "always", "deny"],
            is_private=True,
        )
        with patch.object(
            self.commands.hermes_client,
            "respond_approval",
            AsyncMock(return_value=(False, "409 无挂中审批")),
        ):
            await self._run(
                self.commands.handle_approve,
                raw=f"/{self.commands._cmd('approve')} deny",
            )
        self.assertEqual(self.approvals.pending_count(), 1, "失败留单可重试")
        self.assertIn("应答失败", self.platform[-1])

    async def test_runs_run_stop_prompt_and_flow_unchanged(self) -> None:
        live = SimpleNamespace(
            run_id="run-7", mode="runs", session_key="k1", started_at=time.time()
        )
        with (
            patch.object(self.commands, "_scope_live_run", return_value=live),
            patch.object(
                self.commands.hermes_client, "stop_run", AsyncMock(return_value=True)
            ) as stop,
        ):
            await self._run(
                self.commands.handle_run_stop,
                raw=f"/{self.commands._cmd('run-stop')}",
            )
        stop.assert_awaited_once()
        self.assertEqual(stop.await_args.kwargs["run_id"], "run-7")
        self.assertIn("🛑 已发送终止: run-7", self.platform[-1])

    async def test_runs_run_stop_without_live_run_keeps_old_prompt(self) -> None:
        with patch.object(self.commands, "_scope_live_run", return_value=None):
            await self._run(
                self.commands.handle_run_stop,
                raw=f"/{self.commands._cmd('run-stop')}",
            )
        self.assertIn("没有在跑的 run", self.platform[-1])
