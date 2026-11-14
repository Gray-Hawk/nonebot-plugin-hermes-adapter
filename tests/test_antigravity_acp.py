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
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_agent_adapter.core.storage.session_key_store import (  # noqa: E402
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


class ACPAgentArgvTests(unittest.TestCase):
    """`--agent` 只走独立 argv 项；缺省空串必须保持官方默认命令不变。"""

    def test_default_args_have_no_agent_flag(self) -> None:
        from nonebot_plugin_agent_adapter.backends.antigravity.client import (
            DEFAULT_ACP_ARGS,
            build_acp_args,
        )

        self.assertEqual(
            build_acp_args(DEFAULT_ACP_ARGS, ""), ("acp", "--process-per-session")
        )

    def test_configured_agent_is_appended_as_a_separate_argv_pair(self) -> None:
        from nonebot_plugin_agent_adapter.backends.antigravity.client import (
            DEFAULT_ACP_ARGS,
            build_acp_args,
        )

        self.assertEqual(
            build_acp_args(DEFAULT_ACP_ARGS, "nano"),
            ("acp", "--process-per-session", "--agent", "nano"),
        )

    def test_agent_with_spaces_and_specials_stays_a_single_argv_item(self) -> None:
        from nonebot_plugin_agent_adapter.backends.antigravity.client import (
            DEFAULT_ACP_ARGS,
            build_acp_args,
        )

        # 不拼 shell 字符串：空格、分号、$(...) 都必须原样留在同一个 argv 项里。
        agent = "my agent;$(id) `whoami`"
        args = build_acp_args(DEFAULT_ACP_ARGS, agent)
        self.assertEqual(len(args), 4)
        self.assertEqual(args[-2:], ("--agent", agent))

    def test_blank_agent_config_keeps_official_default(self) -> None:
        from nonebot_plugin_agent_adapter.backends.antigravity.client import (
            DEFAULT_ACP_ARGS,
            build_acp_args,
        )

        # 只写空白（YAML/env 常见手误）等同未配置，不能变成 `--agent "   "`。
        self.assertEqual(
            build_acp_args(DEFAULT_ACP_ARGS, "   "), ("acp", "--process-per-session")
        )


class AntigravityAgentConfigTests(unittest.TestCase):
    """配置面：AGENT_ANTIGRAVITY_PERSONA 缺省空串，并接到 ACP 客户端。"""

    def test_config_exposes_agent_with_empty_default(self) -> None:
        from nonebot_plugin_agent_adapter.config import Config

        defaults = Config()
        self.assertEqual(defaults.hermes_antigravity_persona, "")
        field = Config.model_fields["hermes_antigravity_persona"]
        self.assertEqual(field.alias, "agent_antigravity_persona")
        validated = Config.model_validate({"agent_antigravity_persona": "nano"})
        self.assertEqual(validated.hermes_antigravity_persona, "nano")

    def test_get_client_passes_configured_agent_to_the_acp_client(self) -> None:
        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag
        from nonebot_plugin_agent_adapter.config import plugin_config

        previous = plugin_config.hermes_antigravity_persona
        object.__setattr__(plugin_config, "hermes_antigravity_persona", "nano")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_antigravity_persona", previous
        )
        previous_client = ag._client
        ag._client = None
        self.addCleanup(setattr, ag, "_client", previous_client)

        client = ag.get_client()
        # 命令行由 build_acp_args(cli_args, client._agent) 生成；这里只验证配置接线。
        self.assertEqual(
            ag.build_acp_args(ag.DEFAULT_ACP_ARGS, client._agent),
            ("acp", "--process-per-session", "--agent", "nano"),
        )


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
        from nonebot_plugin_agent_adapter.backends.antigravity.client import (
            AntigravityACPClient,
        )

        args = self.fixture_args()
        client = AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=args,
            workspace=kwargs.pop("workspace", self.tmp),
            model=kwargs.pop("model", self.model),
            agent=kwargs.pop("agent", ""),
            prompt_timeout=kwargs.pop("prompt_timeout", 5.0),
            rpc_timeout=kwargs.pop("rpc_timeout", 5.0),
            **kwargs,
        )
        self._clients.append(client)
        return client

    def fixture_args(self) -> tuple[str, ...]:
        return (
            str(FIXTURE),
            str(self.log_path),
            json.dumps(self.reply, ensure_ascii=False),
            *self.flags,
        )

    def spawn_argv(self) -> list[str]:
        """假服务端启动时记下的真实命令行（含 argv[0] 解释器）。"""
        path = Path(str(self.log_path) + ".argv")
        if not path.exists():
            return []
        return json.loads(path.read_text(encoding="utf-8"))

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


class ACPAgentSpawnTests(FakeACPBase):
    """真实子进程命令行：`--agent` 必须落到 argv，且缺省不改动命令行。"""

    async def test_default_command_line_is_unchanged(self) -> None:
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )

        argv = self.spawn_argv()
        self.assertEqual(argv, list(self.fixture_args()))
        self.assertNotIn("--agent", argv)

    async def test_configured_agent_is_passed_to_the_real_command_line(self) -> None:
        client = self.make_client(agent="nano")
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )

        argv = self.spawn_argv()
        self.assertEqual(argv[-2:], ["--agent", "nano"])
        self.assertEqual(argv[:-2], list(self.fixture_args()))

    async def test_special_agent_stays_one_argv_item_on_the_real_command_line(self) -> None:
        agent = "my agent;$(id) `whoami`"
        client = self.make_client(agent=agent)
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )

        argv = self.spawn_argv()
        self.assertEqual(argv[-2], "--agent")
        self.assertEqual(argv[-1], agent)
        self.assertEqual(len(argv), len(self.fixture_args()) + 2)


class ACPWorkspacePathTests(unittest.TestCase):
    """ACP cwd 会随 session/new 与 resume 发给外部后端：目录名不得带插件/后端身份。"""

    def test_workspace_is_a_neutral_sibling_of_the_plugin_data_dir(self) -> None:
        from nonebot_plugin_agent_adapter import config
        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag

        base = Path("/srv/nb-data/nonebot_plugin_agent_adapter")
        with patch.object(config, "resolve_custom_dir", lambda: base):
            workspace = ag._acp_workspace()
            again = ag._acp_workspace()

        self.assertEqual(workspace, base.parent / "acp-workspace")
        self.assertEqual(again, workspace, "同一数据目录必须给出同一 cwd（跨重启稳定）")
        self.assertFalse(workspace.is_relative_to(base), "cwd 不得落在插件数据目录里")
        self.assertNotIn("hermes", str(workspace).lower())

    def test_workspace_falls_back_to_a_neutral_temp_dir(self) -> None:
        from nonebot_plugin_agent_adapter import config
        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag

        with patch.object(config, "resolve_custom_dir", lambda: None):
            workspace = ag._acp_workspace()

        self.assertEqual(workspace.name, "acp-workspace")
        # 只有插件/后端身份成分要断言：本机 TMPDIR 在 `~/.hermes/…` 下，
        # 对整串做 blanket "hermes" 断言会把家目录误判成泄漏。
        self.assertNotIn("nonebot_plugin_agent_adapter", str(workspace).lower())


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

    async def test_initialize_client_info_is_neutral_and_keeps_the_version(self) -> None:
        """initialize.clientInfo 是插件写给外部 Agent 的可见身份，不得带后端名。"""
        client = self.make_client()
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="hi", store=self.store
        )

        initialize = self.entries("initialize")[0]
        info = initialize["params"]["clientInfo"]
        self.assertEqual(initialize["params"]["protocolVersion"], 1)
        self.assertEqual(info["version"], "0.1.0", "协议版本字段不动")
        self.assertTrue(info["name"], "客户端名不得为空")
        self.assertNotIn("hermes", info["name"].lower(), info["name"])
        self.assertTrue(info["name"].startswith("nonebot"), info["name"])

    async def test_new_and_resume_send_the_neutral_workspace_as_cwd(self) -> None:
        """真 wire：new/resume 的 cwd 都是中性目录，且 ACP 子进程确实在该目录启动。"""
        from nonebot_plugin_agent_adapter import config
        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag

        base = self.tmp / "data" / "nonebot_plugin_agent_adapter"
        base.mkdir(parents=True)
        with patch.object(config, "resolve_custom_dir", lambda: base):
            workspace = ag._acp_workspace()
        client = self.make_client(workspace=workspace)

        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="one", store=self.store
        )
        await client.prompt(
            session_key="k1", system_prompt="SYS", user_text="two", store=self.store
        )

        expected = str(base.parent / "acp-workspace")
        new_cwd = self.entries("session/new")[0]["params"]["cwd"]
        resume_cwd = self.entries("session/resume")[0]["params"]["cwd"]
        self.assertEqual(new_cwd, expected)
        self.assertEqual(resume_cwd, expected)
        self.assertEqual(Path(new_cwd).parent, base.parent, "cwd 必须是数据目录的同级")
        self.assertEqual(Path(new_cwd).name, "acp-workspace")
        self.assertNotIn("nonebot_plugin_agent_adapter", new_cwd.lower())
        self.assertTrue(Path(new_cwd).is_dir(), "ACP 子进程的工作目录必须已创建")

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


class ACPWorkspaceMigrationTests(FakeACPBase):
    """升级换 cwd：旧会话（旧 `antigravity-workspace`）在新 cwd 下仍走 resume。"""

    flags = ("--persist-sessions",)

    async def test_session_created_under_the_old_cwd_resumes_under_the_new_one(self) -> None:
        old = self.tmp / "data" / "nonebot_plugin_agent_adapter" / "antigravity-workspace"
        new = self.tmp / "data" / "acp-workspace"

        first = self.make_client(workspace=old)
        await first.prompt(
            session_key="k1", system_prompt="SYS", user_text="one", store=self.store
        )
        session_id = self.store.load_acp_session("k1")[0]
        await first.close()

        # 升级后：新进程 + 新 cwd，acp_sessions 行仍是同一个 session
        second = self.make_client(workspace=new)
        await second.prompt(
            session_key="k1", system_prompt="SYS", user_text="two", store=self.store
        )

        self.assertEqual(len(self.entries("session/new")), 1, "旧会话不因 cwd 变化被重建")
        resume = self.entries("session/resume")[0]["params"]
        self.assertEqual(resume["sessionId"], session_id)
        self.assertEqual(resume["cwd"], str(new))
        self.assertEqual(Path(resume["cwd"]).name, "acp-workspace")
        self.assertNotIn("nonebot_plugin_agent_adapter", resume["cwd"].lower())
        self.assertNotIn("antigravity-workspace", resume["cwd"])
        self.assertEqual(self.store.load_acp_session("k1")[0], session_id)


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

        from nonebot_plugin_agent_adapter import mcp as mcp_module
        from nonebot_plugin_agent_adapter.config import plugin_config
        from nonebot_plugin_agent_adapter.core import hermes_client as hc

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

    async def _chat(self, client, content: object = "你好", **overrides):
        from types import SimpleNamespace

        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag

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

    async def test_non_image_media_is_refused_before_any_acp_call(self) -> None:
        client = self.make_client()
        content = [
            {"type": "text", "text": "看这个"},
            {"type": "input_audio", "input_audio": {"data": "AAAA", "format": "wav"}},
        ]
        result = await self._chat(client, content=content)

        self.assertTrue(result.is_local_refusal)
        self.assertIn("暂不支持", result.raw_text)
        self.assertEqual(self.log(), [], "媒体轮不应启动 ACP 进程或提交 prompt")

    async def test_remote_image_url_is_refused_before_any_acp_call(self) -> None:
        # 2C2 只放开既有内联链产出的 data URL；远程 URL 不另造下载。
        client = self.make_client()
        content = [
            {"type": "text", "text": "看这张图"},
            {"type": "image_url", "image_url": {"url": "https://example.invalid/a.png"}},
        ]
        result = await self._chat(client, content=content)

        self.assertTrue(result.is_local_refusal)
        self.assertEqual(self.log(), [], "远程图不得启动 ACP 进程或提交 prompt")

    async def test_timeout_is_normalized_without_replay(self) -> None:
        self.flags = ("--hang-prompt",)
        client = self.make_client(prompt_timeout=0.2)
        result = await self._chat(client)

        self.assertTrue(result.is_transport_error)
        self.assertIn("Antigravity", result.raw_text)
        self.assertEqual(len(self.entries("session/prompt")), 1)


class AntigravityPromptTests(unittest.TestCase):
    def setUp(self) -> None:
        from nonebot_plugin_agent_adapter.config import plugin_config

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
        from nonebot_plugin_agent_adapter.core import prompt_builder as pb

        prompt = pb.build_reactive_system_prompt(
            adapter="onebotv11", group_id="462540549", self_id="2968545011",
            self_nickname="bot",
        )
        self._assert_section_only(prompt)
        self.assertIn("插话原则", prompt)
        self.assertIn("<session_context>", prompt)

    def test_passive_prompt_keeps_private_rules_without_tools(self) -> None:
        from nonebot_plugin_agent_adapter.core import prompt_builder as pb

        prompt = pb.build_passive_system_prompt(
            adapter="onebotv11", is_private=True, user_id="10001", group_id=None,
            private_decision=True,
        )
        self._assert_section_only(prompt)
        self.assertIn("私聊", prompt)
        self.assertNotIn("write_file", prompt)


class AntigravityConfigGateTests(unittest.TestCase):
    def setUp(self) -> None:
        from nonebot_plugin_agent_adapter.config import plugin_config

        self._previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._previous
        )

    def test_config_accepts_antigravity_with_minimal_new_fields(self) -> None:
        from typing import get_args

        from nonebot_plugin_agent_adapter.config import Config

        annotation = Config.model_fields["hermes_api_interface"].annotation
        self.assertIn("antigravity", get_args(annotation))
        defaults = Config()
        self.assertEqual(defaults.hermes_antigravity_cli, "agy")
        self.assertEqual(defaults.hermes_antigravity_model, "")

    def test_control_plane_is_closed_for_antigravity(self) -> None:
        from nonebot_plugin_agent_adapter.config import (
            hermes_control_plane_enabled,
            steer_reply_push_active,
        )

        self.assertFalse(hermes_control_plane_enabled())
        self.assertFalse(steer_reply_push_active())

    def test_background_reporting_is_closed_for_antigravity(self) -> None:
        from nonebot_plugin_agent_adapter.config import (
            background_report_enabled,
        )

        self.assertFalse(background_report_enabled())

    def test_antigravity_passive_uses_decision_channel(self) -> None:
        from nonebot_plugin_agent_adapter.handlers.message_turns import (
            _passive_decision_enabled,
        )

        self.assertTrue(_passive_decision_enabled(is_private=True))
        self.assertTrue(_passive_decision_enabled(is_private=False))


class AntigravityMediaGateTests(unittest.TestCase):
    def setUp(self) -> None:
        from nonebot_plugin_agent_adapter.config import plugin_config

        self._previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", self._previous
        )

    def test_history_media_is_not_inlined_for_text_only_backend(self) -> None:
        from nonebot_plugin_agent_adapter.core.message_buffer import BufferedMessage
        from nonebot_plugin_agent_adapter.core.message_origin import MessageOrigin
        from nonebot_plugin_agent_adapter.handlers.message_turns import (
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

        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag
        from nonebot_plugin_agent_adapter.config import plugin_config
        from nonebot_plugin_agent_adapter.core import hermes_client as hc

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
        from nonebot_plugin_agent_adapter.core.session import SessionManager

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
        from nonebot_plugin_agent_adapter.backends.antigravity.client import (
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
