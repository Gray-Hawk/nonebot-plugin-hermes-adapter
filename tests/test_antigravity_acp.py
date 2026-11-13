"""阶段1 Antigravity ACP 最小文本后端：离线 TDD。

真实 `agy acp` 不在本机跑模型；这里用 `tests/fixtures/fake_agy_acp.py` 的
NDJSON JSON-RPC 假服务端验证握手、会话、systemPrompt 扩展、模型选择、权限
拒绝、错误归一化与 Hermes HTTP 零调用。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

FIXTURE = ROOT / "tests" / "fixtures" / "fake_agy_acp.py"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ACPSessionMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "session_keys.db"

    def test_acp_session_mapping_roundtrip_survives_reopen(self) -> None:
        store = SessionKeyStore(self.db_path)
        self.assertIsNone(store.load_acp_session("hermes-onebotv11+private+1"))

        store.put_acp_session(
            "hermes-onebotv11+private+1",
            "sess-7",
            "hash-a",
            "gemini-3.8-flash-high",
            now=1.0,
        )
        self.assertEqual(
            store.load_acp_session("hermes-onebotv11+private+1"),
            ("sess-7", "hash-a", "gemini-3.8-flash-high"),
        )

        # 同一 session key 改写 = 系统上下文或模型变化后的原位更新
        store.put_acp_session(
            "hermes-onebotv11+private+1",
            "sess-7",
            "hash-b",
            "gemini-3.8-flash-high",
            now=2.0,
        )
        self.assertEqual(
            store.load_acp_session("hermes-onebotv11+private+1"),
            ("sess-7", "hash-b", "gemini-3.8-flash-high"),
        )
        store.close()

        reopened = SessionKeyStore(self.db_path)
        self.addCleanup(reopened.close)
        self.assertEqual(
            reopened.load_acp_session("hermes-onebotv11+private+1"),
            ("sess-7", "hash-b", "gemini-3.8-flash-high"),
        )

    def test_missing_key_returns_none(self) -> None:
        store = SessionKeyStore(self.db_path)
        self.addCleanup(store.close)
        self.assertIsNone(store.load_acp_session("nope"))


class FakeACPBase(unittest.IsolatedAsyncioTestCase):
    reply = "§\n你好\n§"
    flags: tuple[str, ...] = ()
    model = ""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log_path = self.tmp / "acp.log"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self._clients = []

    async def asyncTearDown(self) -> None:
        for client in self._clients:
            await client.close()

    def make_client(self, **kwargs):
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            AntigravityACPClient,
        )

        args = (
            str(FIXTURE),
            str(self.log_path),
            json.dumps(self.reply, ensure_ascii=False),
            *self.flags,
        )
        client = AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=args,
            workspace=kwargs.pop("workspace", self.tmp),
            model=kwargs.pop("model", self.model),
            prompt_timeout=kwargs.pop("prompt_timeout", 5.0),
            rpc_timeout=kwargs.pop("rpc_timeout", 5.0),
            **kwargs,
        )
        self._clients.append(client)
        return client

    def log(self) -> list[dict]:
        if not self.log_path.exists():
            return []
        return [
            json.loads(line)
            for line in self.log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def entries(self, method: str) -> list[dict]:
        return [item for item in self.log() if item.get("method") == method]


class ACPHandshakeTests(FakeACPBase):
    async def test_lazy_process_then_initialize_new_session_and_prompt(self) -> None:
        client = self.make_client()
        self.assertFalse(self.log_path.exists(), "客户端在首轮前不应启动 agy")

        text = await client.prompt(
            session_key="k1", system_prompt="SYS-ONE", user_text="你好呀", store=self.store
        )

        self.assertEqual(text, self.reply)
        log = self.log()
        self.assertEqual(log[0]["method"], "initialize")
        self.assertEqual(log[0]["params"]["protocolVersion"], 1)

        new = self.entries("session/new")[0]
        self.assertEqual(new["params"]["cwd"], str(self.tmp))
        self.assertEqual(
            new["params"]["_meta"]["antigravity"]["systemPrompt"]["append"], "SYS-ONE"
        )
        # 标准 ACP systemPrompt 字段实测不生效，禁止拿它替代 _meta 扩展。
        self.assertNotIn("systemPrompt", new["params"])

        record = self.store.load_acp_session("k1")
        self.assertIsNotNone(record)
        acp_session_id, system_hash, model = record
        self.assertEqual(system_hash, sha("SYS-ONE"))
        self.assertEqual(model, "")

        prompt = self.entries("session/prompt")[0]
        self.assertEqual(prompt["params"]["sessionId"], acp_session_id)
        self.assertEqual(prompt["params"]["prompt"], [{"type": "text", "text": "你好呀"}])

    async def test_thought_and_plan_updates_never_reach_body(self) -> None:
        client = self.make_client()
        text = await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )
        self.assertEqual(text, self.reply)
        self.assertNotIn("THOUGHT-MUST-NOT-SURFACE", text)
        self.assertNotIn("PLAN-MUST-NOT-SURFACE", text)


class ACPResumeTests(FakeACPBase):
    async def test_same_system_prompt_resumes_without_reappending(self) -> None:
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="one", store=self.store
        )
        await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="two", store=self.store
        )

        self.assertEqual(len(self.entries("session/new")), 1)
        self.assertEqual(len(self.entries("initialize")), 1, "同一插件实例只握手一次")
        resume = self.entries("session/resume")
        self.assertEqual(len(resume), 1)
        session_id = self.store.load_acp_session("k1")[0]
        self.assertEqual(resume[0]["params"]["sessionId"], session_id)
        self.assertNotIn("_meta", resume[0]["params"])

        prompts = self.entries("session/prompt")
        self.assertEqual([p["params"]["sessionId"] for p in prompts], [session_id, session_id])

    async def test_changed_system_prompt_resumes_with_append_and_updates_hash(self) -> None:
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="one", store=self.store
        )
        await client.prompt(
            session_key="k1", system_prompt="SYS-B", user_text="two", store=self.store
        )

        self.assertEqual(len(self.entries("session/new")), 1)
        resume = self.entries("session/resume")[0]
        self.assertEqual(
            resume["params"]["_meta"]["antigravity"]["systemPrompt"]["append"], "SYS-B"
        )
        self.assertEqual(self.store.load_acp_session("k1")[1], sha("SYS-B"))

    async def test_stale_resume_falls_back_to_a_new_session(self) -> None:
        client = self.make_client()
        self.store.put_acp_session("k1", "gone-session", sha("SYS-A"), "", now=1.0)

        text = await client.prompt(
            session_key="k1", system_prompt="SYS-A", user_text="one", store=self.store
        )

        self.assertEqual(text, self.reply)
        self.assertEqual(len(self.entries("session/new")), 1)
        record = self.store.load_acp_session("k1")
        self.assertNotEqual(record[0], "gone-session")
        self.assertEqual(self.entries("session/prompt")[0]["params"]["sessionId"], record[0])


class ACPModelTests(FakeACPBase):
    model = "gemini-3.8-flash-high"

    async def test_configured_model_is_set_through_acp(self) -> None:
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )

        set_model = self.entries("session/set_model")
        self.assertEqual(len(set_model), 1)
        session_id = self.store.load_acp_session("k1")[0]
        self.assertEqual(
            set_model[0]["params"],
            {"sessionId": session_id, "modelId": "gemini-3.8-flash-high"},
        )
        self.assertEqual(self.store.load_acp_session("k1")[2], "gemini-3.8-flash-high")

        # set_model 必须在 prompt 之前完成，prompt 才跑在该模型上。
        methods = [item.get("method") for item in self.log()]
        self.assertLess(methods.index("session/set_model"), methods.index("session/prompt"))

    async def test_empty_model_keeps_agent_default(self) -> None:
        client = self.make_client(model="")
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )
        self.assertEqual(self.entries("session/set_model"), [])
        self.assertEqual(self.store.load_acp_session("k1")[2], "")


class ACPPermissionTests(FakeACPBase):
    flags = ("--permission",)

    async def test_permission_is_rejected_once_at_the_original_rpc_id(self) -> None:
        client = self.make_client()
        text = await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )
        self.assertEqual(text, self.reply)

        responses = [
            item
            for item in self.log()
            if item.get("id") == 9000 and "method" not in item
        ]
        self.assertEqual(len(responses), 1, responses)
        response = responses[0]
        self.assertNotIn("error", response)
        self.assertEqual(
            response["result"]["outcome"],
            {"outcome": "selected", "optionId": "reject-once"},
        )


class HermesAntigravityChatTests(FakeACPBase):
    reply = "§\n第一句\n§\n第二句\n§"

    def setUp(self) -> None:
        super().setUp()
        from unittest.mock import patch

        from nonebot_plugin_hermes_adapter import mcp as mcp_module
        from nonebot_plugin_hermes_adapter.config import plugin_config
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc

        self.patch = patch
        self.hc = hc
        self._previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._previous
        )
        store_patch = patch.object(mcp_module, "session_key_store", self.store)
        store_patch.start()
        self.addCleanup(store_patch.stop)

    async def _chat(self, client, content="你好", **overrides):
        from types import SimpleNamespace

        from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag

        kwargs = dict(
            text="",
            session_key="hermes-onebotv11+private+10001",
            user_id="10001",
            group_id=None,
            adapter_name="onebotv11",
            is_private=True,
            mode="passive",
            expect_structured=True,
            structured_tool_name="submit_decision",
            system_prompt="SYS",
            user_content_override=content,
            target=SimpleNamespace(
                base_url="http://127.0.0.1:1", api_key="", timeout=5, label="fake"
            ),
        )
        kwargs.update(overrides)
        with self.patch.object(ag, "get_client", return_value=client), self.patch.object(
            self.hc, "_get_http_client", side_effect=AssertionError("Hermes HTTP 不得被调用")
        ):
            return await self.hc.hermes_client.chat(**kwargs)

    async def test_chat_returns_all_final_section_units_without_http(self) -> None:
        client = self.make_client()
        result = await self._chat(client)

        self.assertFalse(result.is_transport_error)
        self.assertEqual(result.raw_text, self.reply)
        self.assertIsNone(result.run_id)
        self.assertEqual(
            [decision["body"] for decision in result.terminal_decisions],
            ["第一句", "第二句"],
        )
        self.assertEqual(result.structured, {"body": "第一句", "should_reply": True})

    async def test_hermes_error_text_detectors_do_not_run_on_model_text(self) -> None:
        # 正文里出现 HTTP 403 只是模型措辞，不能触发 Hermes 专属 403 硬标记。
        self.reply = "§\nHTTP 403 只是一个状态码\n§"
        client = self.make_client()
        result = await self._chat(client)
        self.assertFalse(result.is_forbidden_error)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(result.terminal_decisions[0]["body"], "HTTP 403 只是一个状态码")

    async def test_rpc_error_is_normalized_without_replay(self) -> None:
        self.flags = ("--error-prompt",)
        client = self.make_client()
        result = await self._chat(client)

        self.assertTrue(result.is_transport_error)
        self.assertTrue(result.parse_failed)
        self.assertIn("Antigravity", result.raw_text)
        self.assertEqual(len(self.entries("session/prompt")), 1)

    async def test_process_exit_is_normalized_without_replay(self) -> None:
        self.flags = ("--exit-on-prompt",)
        client = self.make_client()
        result = await self._chat(client)

        self.assertTrue(result.is_transport_error)
        self.assertIn("Antigravity", result.raw_text)
        self.assertEqual(len(self.entries("session/prompt")), 1)

    async def test_image_content_is_refused_before_any_acp_call(self) -> None:
        client = self.make_client()
        content = [
            {"type": "text", "text": "看这张图"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AA"}},
        ]
        result = await self._chat(client, content=content)

        self.assertTrue(result.is_local_refusal)
        self.assertIn("仅支持文本", result.raw_text)
        self.assertEqual(self.log(), [], "媒体轮不应启动 ACP 进程或提交 prompt")

    async def test_timeout_is_normalized_without_replay(self) -> None:
        self.flags = ("--hang-prompt",)
        client = self.make_client(prompt_timeout=0.2)
        result = await self._chat(client)

        self.assertTrue(result.is_transport_error)
        self.assertIn("Antigravity", result.raw_text)
        self.assertEqual(len(self.entries("session/prompt")), 1)


class AntigravityPromptTests(unittest.TestCase):
    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config

        self._previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._previous
        )

    def _assert_section_only(self, prompt: str) -> None:
        self.assertIn("§", prompt)
        self.assertIn("</decision_protocol>", prompt)
        for token in (
            "submit_control",
            "push_message",
            "request_continuation",
            "get_background_tasks",
            "pin_anchor",
            "release_anchor",
            "answer_anchor",
            "anchor_merge",
            "pinned_tasks",
            "get_message_files",
            "browse_group_files",
            "get_message_images",
            "search_sticker_library",
            "should_exit_active",
            "工具调用",
            "SOUL",
            "Hermes",
        ):
            self.assertNotIn(token, prompt, f"{token} 不应出现在 antigravity 系统提示里")

    def test_reactive_prompt_keeps_group_rules_without_tools(self) -> None:
        from nonebot_plugin_hermes_adapter.core import prompt_builder as pb

        prompt = pb.build_reactive_system_prompt(
            adapter="onebotv11", group_id="462540549", self_id="2968545011",
            self_nickname="bot",
        )
        self._assert_section_only(prompt)
        self.assertIn("插话原则", prompt)
        self.assertIn("<session_context>", prompt)

    def test_passive_prompt_keeps_private_rules_without_tools(self) -> None:
        from nonebot_plugin_hermes_adapter.core import prompt_builder as pb

        prompt = pb.build_passive_system_prompt(
            adapter="onebotv11", is_private=True, user_id="10001", group_id=None,
            private_decision=True,
        )
        self._assert_section_only(prompt)
        self.assertIn("私聊", prompt)
        self.assertNotIn("write_file", prompt)


class AntigravityConfigGateTests(unittest.TestCase):
    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config

        self._previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._previous
        )

    def test_config_accepts_antigravity_with_minimal_new_fields(self) -> None:
        from typing import get_args

        from nonebot_plugin_hermes_adapter.config import Config

        annotation = Config.model_fields["hermes_api_interface"].annotation
        self.assertIn("antigravity", get_args(annotation))
        defaults = Config()
        self.assertEqual(defaults.hermes_antigravity_cli, "agy")
        self.assertEqual(defaults.hermes_antigravity_model, "")

    def test_control_plane_is_closed_for_antigravity(self) -> None:
        from nonebot_plugin_hermes_adapter.config import (
            hermes_control_plane_enabled,
            steer_reply_push_active,
        )

        self.assertFalse(hermes_control_plane_enabled())
        self.assertFalse(steer_reply_push_active())

    def test_background_reporting_is_closed_for_antigravity(self) -> None:
        from nonebot_plugin_hermes_adapter.config import (
            background_report_enabled,
        )

        self.assertFalse(background_report_enabled())

    def test_antigravity_passive_uses_decision_channel(self) -> None:
        from nonebot_plugin_hermes_adapter.handlers.message_turns import (
            _passive_decision_enabled,
        )

        self.assertTrue(_passive_decision_enabled(is_private=True))
        self.assertTrue(_passive_decision_enabled(is_private=False))


class AntigravityMediaGateTests(unittest.TestCase):
    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config

        self._previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._previous
        )

    def test_history_media_is_not_inlined_for_text_only_backend(self) -> None:
        from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
        from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin
        from nonebot_plugin_hermes_adapter.handlers.message_turns import (
            _inline_media_window,
        )

        row = BufferedMessage(
            ts=1, adapter="onebotv11", group_id="462540549", user_id="10001",
            nickname="小光", content="[图片]", origin=MessageOrigin.human(),
            image_urls=["https://example.invalid/a.png"],
        )
        window = _inline_media_window([row])
        self.assertEqual(window[0].image_urls, [])
        self.assertEqual(row.image_urls, ["https://example.invalid/a.png"])


class HermesAntigravityHealthTests(FakeACPBase):
    async def test_ping_uses_acp_handshake_not_hermes_http(self) -> None:
        from types import SimpleNamespace
        from unittest.mock import patch

        from nonebot_plugin_hermes_adapter.backends.antigravity import client as ag
        from nonebot_plugin_hermes_adapter.config import plugin_config
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", previous
        )
        client = self.make_client()
        target = SimpleNamespace(
            base_url="http://127.0.0.1:1", api_key="", timeout=5, label="fake"
        )
        with patch.object(ag, "get_client", return_value=client), patch.object(
            hc, "_get_http_client", side_effect=AssertionError("Hermes HTTP 不得被调用")
        ):
            self.assertTrue(await hc.hermes_client.health_check(target))

        self.assertEqual(self.entries("initialize")[0]["params"]["protocolVersion"], 1)
        self.assertEqual(self.entries("session/new"), [])


class ACPClearSessionTests(FakeACPBase):
    async def test_clear_generation_opens_a_new_acp_session(self) -> None:
        from nonebot_plugin_hermes_adapter.core.session import SessionManager

        manager = SessionManager()
        manager.bind_store(self.store)
        before = manager.get_session_key(
            adapter_name="onebotv11", is_private=True, user_id="10001"
        )
        client = self.make_client()
        await client.prompt(
            session_key=before, system_prompt="SYS", user_text="one", store=self.store
        )

        manager.clear_session(
            adapter_name="onebotv11", is_private=True, user_id="10001"
        )
        after = manager.get_session_key(
            adapter_name="onebotv11", is_private=True, user_id="10001"
        )
        self.assertNotEqual(after, before)
        self.assertTrue(after.endswith("-g1"))

        await client.prompt(
            session_key=after, system_prompt="SYS", user_text="two", store=self.store
        )

        self.assertEqual(len(self.entries("session/new")), 2)
        old_id = self.store.load_acp_session(before)[0]
        new_id = self.store.load_acp_session(after)[0]
        self.assertNotEqual(old_id, new_id)
        prompts = self.entries("session/prompt")
        self.assertEqual(prompts[-1]["params"]["sessionId"], new_id)
        # 旧代正文栅栏不归 ACP 客户端管；新代必须落在新会话上，旧代不被复用。
        self.assertNotEqual(prompts[-1]["params"]["sessionId"], old_id)


class ACPTimeoutTests(FakeACPBase):
    flags = ("--hang-prompt",)

    async def test_timeout_cancels_the_prompt_and_raises_timeout(self) -> None:
        from nonebot_plugin_hermes_adapter.backends.antigravity.client import (
            ACPTimeoutError,
        )

        client = self.make_client(prompt_timeout=0.2)
        with self.assertRaises(ACPTimeoutError):
            await client.prompt(
                session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
            )

        # cancel 是通知，写入管道即可；给它一点时间被假服务端读到。
        cancel = []
        for _ in range(50):
            cancel = self.entries("session/cancel")
            if cancel:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(cancel), 1)
        self.assertEqual(cancel[0]["params"]["sessionId"], self.store.load_acp_session("k1")[0])


if __name__ == "__main__":
    unittest.main()
