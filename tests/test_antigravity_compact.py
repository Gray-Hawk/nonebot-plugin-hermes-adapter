"""反重力 ACP 手动会话压缩（复用 `/compress`）离线 TDD。

实测契约（agy 1.5.9）：`available_commands_update` 里有 `compact`，
`session/prompt` 发 `/compact` 可执行；直接 RPC `session/compact` 返回 -32601。
因此本部署只走「已有会话 resume + prompt('/compact')」，不新增压缩体系。

覆盖：
- 客户端：`compact()` 复用 prompt 的会话恢复／MCP／锁／turn 收尾；resume 不带
  系统提示 `_meta`、不改保存的 system hash、不新建会话；缺会话 False 且零 RPC；
  普通 prompt 行为不变；agy 输出不进 QQ 正文流。
- 命令：裸 `/compress` 成功、缺会话、忙碌、附加参数零 RPC、帮助文案。

链路同 2B 夹具：假 `agy acp` NDJSON 子进程 → 真 `AntigravityACPClient` /
`HermesClient` → 真 handler → 真出站（平台发送换进程内替身）。
"""

from __future__ import annotations

import asyncio
import hashlib
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

from tests.acp_2b_harness import (  # noqa: E402
    ACPHandlerHarness,
    wait_until as _wait_until,
)
from nonebot_plugin_agent_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

FIXTURE = ROOT / "tests" / "fixtures" / "fake_agy_acp.py"

# 自造离线占位，不是任何真实凭据。
DUMMY_TOKEN = "sk-offline-compact-dummy"
MCP_HOST = "127.0.0.1"
MCP_PORT = 18643
GROUP = "100"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def prompt_text(row: dict) -> str:
    """`session/prompt` 帧里的 prompt 正文（按块顺序拼）。"""
    blocks = row.get("params", {}).get("prompt") or []
    return "".join(str(block.get("text") or "") for block in blocks)


def expected_mcp_servers() -> list[dict]:
    """本轮 ACP 应挂载的定义（独立字面量，不借用产品 helper）。"""
    return [
        {
            "type": "http",
            "name": "nonebot-bridge",
            "url": f"http://{MCP_HOST}:{MCP_PORT}/mcp",
            "headers": [{"name": "Authorization", "value": f"Bearer {DUMMY_TOKEN}"}],
        }
    ]


def _plugin_config_override(testcase: unittest.TestCase, **overrides) -> None:
    from nonebot_plugin_agent_adapter.config import plugin_config

    saved = {name: getattr(plugin_config, name) for name in overrides}
    for name, value in overrides.items():
        object.__setattr__(plugin_config, name, value)

    def restore() -> None:
        for name, value in saved.items():
            object.__setattr__(plugin_config, name, value)

    testcase.addCleanup(restore)


def mcp_config(testcase: unittest.TestCase) -> None:
    _plugin_config_override(
        testcase,
        hermes_mcp_enabled=True,
        hermes_mcp_host=MCP_HOST,
        hermes_mcp_port=MCP_PORT,
        hermes_api_key=DUMMY_TOKEN,
    )


class _ACPCompactBase(unittest.IsolatedAsyncioTestCase):
    reply = "COMPACT-AGY-OUTPUT-MUST-NOT-SURFACE"

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="acp-compact-")
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log_path = self.tmp / "wire.jsonl"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self.clients = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    def make_client(self, **kwargs):
        from nonebot_plugin_agent_adapter.backends.antigravity.client import (
            AntigravityACPClient,
        )

        args = [str(FIXTURE), str(self.log_path), json.dumps(self.reply, ensure_ascii=False)]
        args += list(kwargs.pop("flags", ()))
        client = AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=args,
            workspace=self.tmp,
            prompt_timeout=kwargs.pop("prompt_timeout", 5.0),
            rpc_timeout=kwargs.pop("rpc_timeout", 5.0),
            **kwargs,
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


class ACPCompactWireTests(_ACPCompactBase):
    """`compact()` 的 ACP wire 契约：复用恢复链，只发裸 `/compact`。"""

    def setUp(self) -> None:
        super().setUp()
        mcp_config(self)

    async def test_compact_resumes_without_system_prompt_and_sends_bare_command(self) -> None:
        client = self.make_client()
        await client.prompt(
            session_key="k1",
            system_prompt="SYS-A",
            user_text="hello",
            store=self.store,
            turn_meta=self.group_meta(),
        )

        ok = await client.compact(
            session_key="k1", store=self.store, turn_meta=self.group_meta()
        )

        self.assertTrue(ok)
        new = self.rows("session/new")
        self.assertEqual(len(new), 1, "已有会话不得新建")
        resume = self.rows("session/resume")
        self.assertEqual(len(resume), 1, resume)
        session_id = self.store.load_acp_session("k1")[0]
        self.assertEqual(resume[0]["params"]["sessionId"], session_id)
        # 控制命令不追加系统提示：resume 帧里不能有系统提示 _meta。
        self.assertNotIn("_meta", resume[0]["params"])
        # MCP 来源与普通轮同源（同一份 mcp_servers_for_turn）。
        self.assertEqual(resume[0]["params"]["mcpServers"], expected_mcp_servers())
        self.assertEqual(
            new[0]["params"]["mcpServers"], resume[0]["params"]["mcpServers"]
        )

        prompts = self.rows("session/prompt")
        self.assertEqual(len(prompts), 2, prompts)
        self.assertEqual(prompts[-1]["params"]["sessionId"], session_id)
        self.assertEqual(prompt_text(prompts[-1]), "/compact")
        self.assertEqual(
            prompts[-1]["params"]["prompt"], [{"type": "text", "text": "/compact"}]
        )
        self.assertEqual(self.store.load_acp_session("k1")[1], sha("SYS-A"))

    async def test_compact_without_any_session_is_false_and_sends_no_rpc(self) -> None:
        client = self.make_client()

        ok = await client.compact(session_key="k-missing", store=self.store)

        self.assertFalse(ok)
        self.assertEqual(self.wire(), [], "缺会话必须零 RPC，也不得拉起进程")

    async def test_stale_stored_session_is_false_and_creates_no_empty_session(self) -> None:
        client = self.make_client()
        self.store.put_acp_session("k1", "gone-session", sha("SYS-A"), "", now=1.0)

        ok = await client.compact(
            session_key="k1", store=self.store, turn_meta=self.group_meta()
        )

        self.assertFalse(ok)
        self.assertEqual(self.rows("session/new"), [], "不得新建空会话")
        self.assertEqual(self.rows("session/prompt"), [], "不得向不存在会话发 prompt")
        self.assertEqual(self.store.load_acp_session("k1")[0], "gone-session")

    async def test_compact_leaves_no_active_turn_and_no_body_text(self) -> None:
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="hello", store=self.store
        )

        ok = await client.compact(session_key="k1", store=self.store)

        self.assertTrue(ok)
        # 控制轮已收尾：不残留 active_turn，agy 输出也没有可用正文出口。
        self.assertIsNone(client.active_turn("k1"))

    async def test_user_message_chunk_never_hands_the_body_stream_to_a_control_turn(
        self,
    ) -> None:
        """控制命令轮回显 `/compact` 时不接管正文流：后续 chunk 一律不进 QQ。"""
        client = self.make_client()
        units: list[dict] = []
        turn = client._new_turn(
            session_key="k1",
            session_id="sess-control",
            on_unit=units.append,
            meta=None,
            stream_here=False,
            collect_text=False,
        )
        client._on_notification(
            {
                "method": "session/update",
                "params": {
                    "sessionId": "sess-control",
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": {"type": "text", "text": "/compact"},
                    },
                },
            }
        )
        client._on_notification(
            {
                "method": "session/update",
                "params": {
                    "sessionId": "sess-control",
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "§\n泄漏\n§"},
                    },
                },
            }
        )

        self.assertEqual(units, [])
        self.assertEqual(turn.text, [])
        self.assertIsNone(client._streaming_turn.get("sess-control"))

    async def test_compact_does_not_touch_the_model_or_resume_meta(self) -> None:
        client = self.make_client(model="gemini-3.8-flash-high")
        await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="hello", store=self.store
        )

        self.assertTrue(await client.compact(session_key="k1", store=self.store))

        self.assertEqual(len(self.rows("session/set_model")), 1, "压缩不得重新下发模型")
        self.assertEqual(self.store.load_acp_session("k1")[2], "gemini-3.8-flash-high")

    async def test_normal_prompt_after_compact_keeps_system_prompt_behaviour(self) -> None:
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="one", store=self.store
        )
        self.assertTrue(await client.compact(session_key="k1", store=self.store))

        text = await client.prompt(
            session_key="k1", system_prompt="SYS-B", user_text="two", store=self.store
        )

        self.assertEqual(text, self.reply)
        self.assertEqual(len(self.rows("session/new")), 1)
        resume = self.rows("session/resume")
        self.assertEqual(len(resume), 2, resume)
        self.assertEqual(
            resume[-1]["params"]["_meta"]["antigravity"]["systemPrompt"]["append"], "SYS-B"
        )
        self.assertEqual(self.store.load_acp_session("k1")[1], sha("SYS-B"))
        prompts = self.rows("session/prompt")
        self.assertEqual(prompt_text(prompts[-1]), "two")


class ACPCompressCommandTests(ACPHandlerHarness):
    """`/compress` 在反重力后端的分支：裸命令成功、缺会话、忙碌、附加参数。"""

    def _compress(self, tail: str = ""):
        raw = f"/{self.commands._cmd('compress')}"
        if tail:
            raw += f" {tail}"
        target = SimpleNamespace(private=True, id=self.ADMIN)
        return self._command(self.commands.handle_compress, target=target, raw=raw)

    def last_receipt(self) -> str:
        self.assertTrue(self.platform, "命令必须有回执")
        return self.platform[-1]

    async def test_bare_compress_succeeds_and_reports_unchanged_session(self) -> None:
        self._client(reply="§\n好的\n§")
        await self._turn()

        await self._compress()

        receipt = self.last_receipt()
        self.assertIn("压缩完成", receipt)
        self.assertIn("session 不变", receipt)
        # 不伪造 before/after 消息数、摘要或进度。
        self.assertNotIn("→", receipt)
        self.assertNotIn("条消息", receipt)

        prompts = self.rows("session/prompt")
        self.assertEqual(prompt_text(prompts[-1]), "/compact")
        self.assertEqual(len(self.rows("session/new")), 1, "压缩不得新建会话")
        self.assertEqual(self.rows("session/resume")[-1]["params"]["sessionId"],
                         self.store.load_acp_session(self.session_key())[0])

    async def test_success_receipt_carries_no_internal_session_key(self) -> None:
        """成功回执只说明「压缩完成、ACP session 不变」，内部 key/UUID 一律不透出。"""
        self._client(reply="§\n好的\n§")
        await self._turn()
        session_key = self.session_key()
        self.assertTrue(session_key, "本用例需要真实内部 key 作为反例")

        await self._compress()

        receipt = "\n".join(self.platform)
        self.assertIn("压缩完成", receipt)
        self.assertIn("ACP session 不变", receipt)
        for leaked in (session_key, "antigravity-", "hermes-", "onebotv11+", self.ADMIN):
            self.assertNotIn(leaked, receipt, f"回执泄漏内部标识: {leaked}")
        self.assertIsNone(
            __import__("re").search(r"[0-9a-f]{8}-[0-9a-f]{4}-", receipt),
            f"回执不得出现 ACP UUID: {receipt!r}",
        )

    async def test_agy_output_never_reaches_the_qq_body(self) -> None:
        marker = "COMPACT-AGY-OUTPUT-MUST-NOT-SURFACE"
        self._client(chunks=[f"§\n{marker}\n§"])
        await self._turn()
        # 普通轮照常出正文（同一份夹具输出）；压缩轮的同一份输出必须被拦下。
        delivered = "\n".join(self.platform)
        self.assertIn(marker, delivered)
        before = list(self.platform)

        await self._compress()

        new_texts = self.platform[len(before):]
        self.assertTrue(new_texts, "压缩必须有回执")
        for text in new_texts:
            self.assertNotIn(marker, text)

    async def test_missing_session_receipt_and_zero_rpc(self) -> None:
        self._client()

        await self._compress()

        receipt = self.last_receipt()
        self.assertIn("会话", receipt)
        self.assertIn("无需压缩", receipt)
        self.assertEqual(self.wire(), [], "缺会话不得发任何 RPC")

    async def test_busy_session_is_reported_without_sending_compact(self) -> None:
        gate = self.base / "release"
        self._client(reply="§\n晚到\n§", flags=(f"--gate-prompt={gate}",))
        task = asyncio.create_task(self._turn())
        self.addCleanup(lambda: gate.write_text("go", encoding="utf-8"))
        await _wait_until(lambda: self.rows("session/prompt"), what="ACP prompt in flight")

        await self._compress()

        self.assertIn("正忙", self.last_receipt())
        self.assertTrue(
            all(prompt_text(row) != "/compact" for row in self.rows("session/prompt")),
            "忙碌时不得发 /compact",
        )
        gate.write_text("go", encoding="utf-8")
        await task

    async def test_extra_arguments_get_a_capability_receipt_and_zero_rpc(self) -> None:
        self._client()
        for tail in ("--preview", "保留配方", "--preview 保留配方", "here 3"):
            with self.subTest(tail=tail):
                self.platform.clear()
                await self._compress(tail)
                receipt = self.last_receipt()
                self.assertIn("反重力", receipt)
                self.assertIn("直接压缩", receipt)
                self.assertNotIn("已压缩", receipt)
                self.assertNotIn("tokens", receipt)
                self.assertEqual(self.wire(), [], "附加参数不得发任何 RPC")


class ACPCompactSteerIsolationTests(_ACPCompactBase):
    """控制命令轮没有正文交付出口，普通用户 steer 必须回退，不得被静默吞掉。"""

    async def _established(self):
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="hello", store=self.store
        )
        return client, self.store.load_acp_session("k1")[0]

    async def test_control_turn_refuses_steer_without_sending_a_prompt(self) -> None:
        client, session_id = await self._established()
        control = client._new_turn(
            session_key="k1",
            session_id=session_id,
            on_unit=None,
            meta=self.group_meta(),
            stream_here=False,
            collect_text=False,
            control=True,
        )
        self.assertIs(client.active_turn("k1"), control)

        ok = await client.steer(session_key="k1", text="§\nLATE-USER\n§")

        self.assertFalse(ok, "控制命令轮不得接管正常用户消息")
        self.assertEqual(
            [prompt_text(row) for row in self.rows("session/prompt")],
            ["hello"],
            "拒绝 steer 时不得发后续 session/prompt",
        )
        self.assertIs(client.active_turn("k1"), control, "控制轮原样保留，不登记接管轮")
        self.assertFalse(control.superseded)

    async def test_ordinary_turn_still_accepts_steer(self) -> None:
        client, session_id = await self._established()
        turn = client._new_turn(
            session_key="k1",
            session_id=session_id,
            on_unit=None,
            meta=self.group_meta(),
            stream_here=True,
        )
        self.assertIs(client.active_turn("k1"), turn)

        ok = await client.steer(session_key="k1", text="§\nSTEER-OK\n§")

        self.assertTrue(ok, "普通在跑轮的 steer 行为不得回归")
        prompts = self.rows("session/prompt")
        self.assertEqual(len(prompts), 2)
        self.assertIn("STEER-OK", prompt_text(prompts[-1]))


class ACPCompactConcurrencyTests(ACPHandlerHarness):
    """压缩进行中到达的正常私聊：回退既有正常正文链，回复不丢、只处理一次。"""

    def _compress(self):
        target = SimpleNamespace(private=True, id=self.ADMIN)
        return self._command(
            self.commands.handle_compress,
            target=target,
            raw=f"/{self.commands._cmd('compress')}",
        )

    async def test_normal_message_during_compact_is_dispatched_once_and_answered(
        self,
    ) -> None:
        from nonebot.matcher import Matcher
        from nonebot_plugin_alconna import UniMessage
        from nonebot_plugin_agent_adapter.core import steer_seen
        from nonebot_plugin_agent_adapter.core.chat_config import ChatConfig
        from nonebot_plugin_agent_adapter.handlers import message, message_relay as relay

        gate = self.base / "release"
        gate.write_text("go", encoding="utf-8")
        client = self._client(
            reply="§\nLATER-USER-ANSWER\n§", flags=(f"--gate-prompt={gate}",)
        )
        # 首个 prompt 直接放行；之后删除 gate，`/compact` 的 prompt 被夹具挂住。
        await self._turn()
        self.assertIn("LATER-USER-ANSWER", "\n".join(self.platform))
        self.platform.clear()
        gate.unlink()

        compress_task = asyncio.create_task(self._compress())
        await _wait_until(
            lambda: any(
                prompt_text(row) == "/compact" for row in self.rows("session/prompt")
            ),
            what="/compact prompt",
        )
        control = client.active_turn(self.session_key())
        self.assertIsNotNone(control)
        self.assertTrue(control.control)
        self.assertFalse(control.collect_text)

        cfg = ChatConfig(reply_debounce_sec=0)
        dispatches: list[dict] = []
        real_dispatch = message._dispatch_chat

        async def counting_dispatch(**payload):
            dispatches.append(payload)
            await real_dispatch(**payload)

        target = SimpleNamespace(private=True, id=self.ADMIN)
        event = SimpleNamespace(
            message=UniMessage("LATER-USER-QUESTION"),
            get_user_id=lambda: self.ADMIN,
            is_tome=lambda: False,
            message_id=902,
        )
        steer_seen._steer_seen_reset_for_test()
        message._debounce_states.clear()
        self.addCleanup(steer_seen._steer_seen_reset_for_test)
        self.addCleanup(message._debounce_states.clear)
        with (
            patch.object(message, "load_chat_config", lambda: cfg),
            patch.object(relay, "load_chat_config", lambda: cfg),
            patch.object(message.alconna, "get_target", lambda *_: target),
            patch.object(
                UniMessage, "generate_without_reply", lambda **kw: kw["event"].message
            ),
            patch.object(message, "get_adapter_name", lambda _: "onebotv11"),
            patch.object(message, "check_isolation", lambda *_: True),
            patch.object(message, "_dispatch_chat", counting_dispatch),
            patch.object(message, "_peek_event_db_id", lambda *_: 902),
            patch.object(message, "session_manager", self.manager),
            patch.object(message.user_lists, "is_known_bot", lambda *_: False),
            patch.object(message.user_lists, "is_blacklisted", lambda *_: False),
            patch.object(relay, "_steer_name_resolver", lambda *_: None),
            patch.object(relay, "steer_reply_push_active", lambda: False),
            # 本离线 venv 未装 apscheduler，tasks 包导入会炸；这里的投递重试旁路与
            # 本条消息的通路无关，只保留 slot 释放这一实际副作用。
            patch.object(
                relay,
                "_release_slot_and_retry",
                lambda key, *a, **k: relay._mcp.inflight.exit(key),
            ),
        ):
            ingress = asyncio.create_task(
                message.handle_message(self.bot, event, Matcher)
            )
            await _wait_until(
                lambda: dispatches, what="压缩期间消息进入正常 dispatch"
            )
            # 压缩仍在进行：消息没有被控制轮 steer 接管，也没有提前发第三条 prompt。
            texts = [prompt_text(row) for row in self.rows("session/prompt")]
            self.assertEqual(texts[1], "/compact")
            self.assertEqual(len(texts), 2)
            self.assertFalse(any("LATER-USER-QUESTION" in text for text in texts))
            gate.write_text("go", encoding="utf-8")
            await ingress
            await compress_task

        texts = [prompt_text(row) for row in self.rows("session/prompt")]
        self.assertEqual(len(texts), 3, texts)
        self.assertEqual(texts[1], "/compact")
        self.assertIn("LATER-USER-QUESTION", texts[2], "压缩收尾后才走正常正文链")
        self.assertEqual(len(dispatches), 1, "后续消息必须且只能正常 dispatch 一次")
        delivered = "\n".join(self.platform)
        self.assertIn("压缩完成", delivered)
        self.assertIn("LATER-USER-ANSWER", delivered, "压缩期间到达的消息回复不得丢失")
        self.assertIsNone(client.active_turn(self.session_key()))

    async def test_second_compress_during_compaction_still_reports_busy(self) -> None:
        gate = self.base / "release"
        gate.write_text("go", encoding="utf-8")
        self._client(reply="§\nOK\n§", flags=(f"--gate-prompt={gate}",))
        await self._turn()
        gate.unlink()

        first = asyncio.create_task(self._compress())
        await _wait_until(
            lambda: any(
                prompt_text(row) == "/compact" for row in self.rows("session/prompt")
            ),
            what="/compact prompt",
        )
        self.platform.clear()

        await self._compress()

        self.assertIn("正忙", self.platform[-1])
        self.assertEqual(
            sum(
                prompt_text(row) == "/compact"
                for row in self.rows("session/prompt")
            ),
            1,
            "压缩进行中不得再发第二条 /compact",
        )
        gate.write_text("go", encoding="utf-8")
        await first


class CompressHelpTextTests(unittest.TestCase):
    """/help 按后端展示：反重力只列直接压缩，不写 `--preview`。"""

    def setUp(self) -> None:
        from nonebot_plugin_agent_adapter.core.chat_config import ChatConfig

        patcher = patch.object(
            self._commands(),
            "load_chat_config",
            lambda: ChatConfig(session_idle_reset_sec=0),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def _commands():
        from nonebot_plugin_agent_adapter.handlers import commands

        return commands

    def test_antigravity_help_lists_direct_compress_without_preview(self) -> None:
        commands = self._commands()
        with patch.object(commands, "hermes_control_plane_enabled", return_value=False):
            text = commands.build_help_text(True, True, backend="antigravity")

        self.assertIn(f"{commands._display_cmd('compress')} - 手动压缩当前会话上下文", text)
        self.assertNotIn("--preview", text)
        # 反重力控制面关闭，不得顺手列出 Hermes 专属的 runs 命令。
        self.assertNotIn(commands._display_cmd("run-status"), text)

    def test_hermes_help_keeps_the_preview_hint(self) -> None:
        commands = self._commands()
        with patch.object(commands, "hermes_control_plane_enabled", return_value=True):
            text = commands.build_help_text(True, True)

        self.assertIn("--preview", text)
        self.assertIn(commands._display_cmd("compress"), text)

    def test_non_admin_never_sees_compress_for_antigravity(self) -> None:
        commands = self._commands()
        with patch.object(commands, "hermes_control_plane_enabled", return_value=False):
            text = commands.build_help_text(True, False, backend="antigravity")

        self.assertNotIn(commands._display_cmd("compress"), text)

    def test_help_backend_follows_the_configured_interface(self) -> None:
        commands = self._commands()
        _plugin_config_override(self, hermes_api_interface="antigravity")
        self.assertEqual(commands._help_backend(), "antigravity")
        _plugin_config_override(self, hermes_api_interface="runs")
        self.assertEqual(commands._help_backend(), "hermes")


if __name__ == "__main__":
    unittest.main()
