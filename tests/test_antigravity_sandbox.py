"""ACP 沙箱缓存工作区（BASH_ENV 绕行）离线回归。

真实边界（2026-11-16 已用官方 `agy acp --sandbox` 实测）：session cwd 在沙箱里被
挂成只读、`workspaceDirs=[]`；未声明为 cwd／additionalDirectories 的
`~/.cache/<workspace>` 可写；`run_command` 用 `bash -c` 并保留启动环境里的
`BASH_ENV`。所以插件侧的契约是：

- 只有 ACP argv 里带 `--sandbox` 才进入该模式；缺省 argv／env／cwd／提示词全不变。
- session/new／resume 的 cwd 保持原 ACP workspace，resume 语义不变。
- 真正的 shell 工作区固定在运行用户 `~/.cache/agent-adapter/acp-workspace`，启动时创建。
- 受控 BASH_ENV 文件放在原 ACP workspace（沙箱只读 cwd）里，内容只有一行 `cd <路径>`。
- 缓存工作区不得进入 additionalDirectories（声明为工作区根会重新变只读）。

这里只替换进程边界（`tests/fixtures/fake_agy_acp.py`），断言真实 argv、真实子进程
env、真实 wire 与真实文件内容。
"""

from __future__ import annotations

import hashlib
import json
import os
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
REPLY = "§\n你好\n§"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def mod():
    from nonebot_plugin_agent_adapter.backends.antigravity import client as ag

    return ag


class SandboxHelperTests(unittest.TestCase):
    """纯函数契约：开关判定、默认缓存路径、提示词一句话。"""

    def test_sandbox_flag_is_exact_argv_membership(self) -> None:
        ag = mod()
        self.assertFalse(ag.sandbox_enabled(("acp", "--process-per-session")))
        self.assertFalse(ag.sandbox_enabled(()))
        # 近似但不同名的参数不得当作开启
        self.assertFalse(ag.sandbox_enabled(("acp", "--sandbox-mode")))
        self.assertFalse(ag.sandbox_enabled(("acp", "--no-sandbox")))
        self.assertTrue(ag.sandbox_enabled(("acp", "--process-per-session", "--sandbox")))

    def test_default_shell_workspace_is_dedicated_dir_under_user_cache(self) -> None:
        ag = mod()
        path = ag.sandbox_shell_workspace()
        self.assertTrue(path.is_absolute())
        self.assertEqual(path, Path.home() / ".cache" / "agent-adapter" / "acp-workspace")
        # 中文注释：必须是 ~/.cache 下的专用子目录，不能复用 ACP 会话 cwd。
        self.assertNotEqual(path, ag._acp_workspace())

    def test_notice_is_one_neutral_sentence_with_absolute_path(self) -> None:
        ag = mod()
        notice = ag.sandbox_workspace_notice(Path("/home/u/.cache/agent-adapter/acp-workspace"))
        self.assertIn("/home/u/.cache/agent-adapter/acp-workspace", notice)
        self.assertNotIn("\n", notice)
        for banned in ("Hermes", "hermes", "Antigravity", "antigravity"):
            self.assertNotIn(banned, notice)
        self.assertEqual(
            ag.append_workspace_notice("SYS-A", Path("/tmp/acp-ws")),
            "SYS-A\n\n" + ag.sandbox_workspace_notice(Path("/tmp/acp-ws")),
        )


class SandboxClientTests(unittest.IsolatedAsyncioTestCase):
    """真客户端 + 假 `agy acp` 子进程：argv／env／cwd／文件／提示词。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="acp-sandbox-")
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.workspace = self.tmp / "acp-workspace"
        self.workspace.mkdir()
        self.shell = self.tmp / "cache" / "agent-adapter" / "acp-workspace"
        self.log_path = self.tmp / "wire.jsonl"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self.clients = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    def make_client(self, *extra: str):
        ag = mod()
        client = ag.AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=[str(FIXTURE), str(self.log_path), json.dumps(REPLY, ensure_ascii=False), *extra],
            workspace=self.workspace,
            prompt_timeout=6.0,
            rpc_timeout=5.0,
            sandbox_shell_dir=self.shell,
        )
        self.clients.append(client)
        return client

    # ---------------------------------------------------------------- helpers
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

    def spawn_argv(self) -> list[str]:
        return json.loads(Path(str(self.log_path) + ".argv").read_text(encoding="utf-8"))

    def spawn_env(self) -> dict:
        return json.loads(Path(str(self.log_path) + ".env").read_text(encoding="utf-8"))

    def bash_env_file(self) -> Path:
        return self.workspace / ".acp-sandbox-bash-env"

    async def prompt(self, client, system_prompt, text="hi"):  # noqa: ANN001
        return await client.prompt(
            session_key="k1", system_prompt=system_prompt, user_text=text, store=self.store
        )

    # ------------------------------------------------------- 缺省零变化
    async def test_default_mode_keeps_argv_env_cwd_and_prompt(self) -> None:
        client = self.make_client()
        await self.prompt(client, "SYS-A")

        self.assertEqual(
            self.spawn_argv(),
            [str(FIXTURE), str(self.log_path), json.dumps(REPLY, ensure_ascii=False)],
        )
        # 未开启：不给子进程 env 覆盖，BASH_ENV 与父进程一致（这里父进程通常为空）
        self.assertEqual(self.spawn_env().get("BASH_ENV"), os.environ.get("BASH_ENV"))
        self.assertFalse(self.bash_env_file().exists(), "缺省不得生成 BASH_ENV 文件")
        self.assertFalse(self.shell.exists(), "缺省不得创建缓存工作区")

        new = self.rows("session/new")[0]["params"]
        self.assertEqual(new["cwd"], str(self.workspace))
        self.assertEqual(new["_meta"]["antigravity"]["systemPrompt"]["append"], "SYS-A")
        self.assertNotIn("additionalDirectories", new)

    async def test_default_mode_inherits_existing_bash_env(self) -> None:
        with patch.dict(os.environ, {"BASH_ENV": str(self.tmp / "parent-bash-env")}):
            client = self.make_client()
            await self.prompt(client, "SYS-A")
        self.assertEqual(self.spawn_env().get("BASH_ENV"), str(self.tmp / "parent-bash-env"))
        self.assertFalse(self.bash_env_file().exists())

    # --------------------------------------------------------- sandbox 模式
    async def test_sandbox_sets_bash_env_and_creates_shell_workspace(self) -> None:
        client = self.make_client("--sandbox")
        await self.prompt(client, "SYS-A")

        self.assertIn("--sandbox", self.spawn_argv())
        # 实际 shell 工作区在启动时创建（BASH_ENV 里 cd 的目标必须已存在）
        self.assertTrue(self.shell.is_dir())
        # 受控 BASH_ENV 文件位于沙箱只读的原 ACP cwd 内，不在可写缓存里
        env_file = self.bash_env_file()
        self.assertTrue(env_file.is_file())
        self.assertEqual(env_file.parent, self.workspace)
        self.assertEqual(env_file.read_text(encoding="utf-8"), f"cd {self.shell}\n")
        self.assertEqual(self.spawn_env().get("BASH_ENV"), str(env_file))

        # session/new 的 cwd 仍是原 ACP workspace，缓存路径不得进 additionalDirectories
        new = self.rows("session/new")[0]["params"]
        self.assertEqual(new["cwd"], str(self.workspace))
        self.assertNotIn("additionalDirectories", new)
        self.assertNotEqual(new["cwd"], str(self.shell))
        # 缓存路径只出现在提示词那一句说明里，不能作为工作区声明出现
        append = new["_meta"]["antigravity"]["systemPrompt"]["append"]
        self.assertIn(str(self.shell), append)
        self.assertEqual(
            append, f"SYS-A\n\n{mod().sandbox_workspace_notice(self.shell)}"
        )

    async def test_sandbox_notice_on_new_and_resume_matches(self) -> None:
        ag = mod()
        client = self.make_client("--sandbox")
        notice = ag.sandbox_workspace_notice(self.shell)

        await self.prompt(client, "SYS-A")
        new = self.rows("session/new")[0]["params"]
        self.assertEqual(
            new["_meta"]["antigravity"]["systemPrompt"]["append"], f"SYS-A\n\n{notice}"
        )
        self.assertIn(str(self.shell), new["_meta"]["antigravity"]["systemPrompt"]["append"])

        # 同一系统上下文 resume：哈希不变就不重复 append
        await self.prompt(client, "SYS-A", text="again")
        first_resume = self.rows("session/resume")[0]["params"]
        self.assertNotIn("_meta", first_resume)
        self.assertEqual(first_resume["cwd"], str(self.workspace))
        self.assertNotIn("additionalDirectories", first_resume)

        # 换系统上下文时 resume 也要带同一句工作区说明
        await self.prompt(client, "SYS-B", text="third")
        resume = self.rows("session/resume")[-1]["params"]
        self.assertEqual(
            resume["_meta"]["antigravity"]["systemPrompt"]["append"], f"SYS-B\n\n{notice}"
        )
        self.assertEqual(self.store.load_acp_session("k1")[1], sha(f"SYS-B\n\n{notice}"))

    async def test_sandbox_control_turn_keeps_hash_and_no_meta(self) -> None:
        ag = mod()
        client = self.make_client("--sandbox")
        await self.prompt(client, "SYS-A")
        expected_hash = sha("SYS-A\n\n" + ag.sandbox_workspace_notice(self.shell))
        self.assertEqual(self.store.load_acp_session("k1")[1], expected_hash)

        self.assertTrue(await client.compact(session_key="k1", store=self.store))
        control_resume = self.rows("session/resume")[-1]["params"]
        self.assertNotIn("_meta", control_resume)
        self.assertEqual(self.store.load_acp_session("k1")[1], expected_hash)
        self.assertEqual(
            self.rows("session/prompt")[-1]["params"]["prompt"][0]["text"], "/compact"
        )

    async def test_sandbox_workspace_still_created_with_existing_env(self) -> None:
        with patch.dict(os.environ, {"BASH_ENV": str(self.tmp / "parent-bash-env")}):
            client = self.make_client("--sandbox")
            await self.prompt(client, "SYS-A")
        # sandbox 模式覆盖继承值，指向受控文件
        self.assertEqual(self.spawn_env().get("BASH_ENV"), str(self.bash_env_file()))

    async def test_bash_env_file_is_regenerated_at_start(self) -> None:
        stale = self.bash_env_file()
        stale.write_text("cd /wrong/place\n", encoding="utf-8")
        client = self.make_client("--sandbox")
        await self.prompt(client, "SYS-A")
        self.assertEqual(stale.read_text(encoding="utf-8"), f"cd {self.shell}\n")


class SandboxConfigTests(unittest.TestCase):
    """配置面：ACP CLI 参数载体缺省空串，且接到客户端 argv。"""

    def test_config_exposes_args_with_empty_default(self) -> None:
        from nonebot_plugin_agent_adapter.config import Config

        defaults = Config()
        self.assertEqual(defaults.hermes_antigravity_args, "")
        field = Config.model_fields["hermes_antigravity_args"]
        self.assertEqual(field.alias, "agent_antigravity_args")
        validated = Config.model_validate({"agent_antigravity_args": "--sandbox"})
        self.assertEqual(validated.hermes_antigravity_args, "--sandbox")

    def test_get_client_passes_args_and_default_argv_is_unchanged(self) -> None:
        ag = mod()
        from nonebot_plugin_agent_adapter.config import plugin_config

        previous_client = ag._client
        ag._client = None
        self.addCleanup(setattr, ag, "_client", previous_client)
        previous = plugin_config.hermes_antigravity_args
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_antigravity_args", previous
        )

        object.__setattr__(plugin_config, "hermes_antigravity_args", "")
        client = ag.get_client()
        self.assertEqual(client._cli_args, ag.DEFAULT_ACP_ARGS)
        self.assertFalse(client._sandbox)

        object.__setattr__(plugin_config, "hermes_antigravity_args", "--sandbox")
        ag._client = None
        client = ag.get_client()
        self.assertEqual(
            client._cli_args, (*ag.DEFAULT_ACP_ARGS, "--sandbox")
        )
        self.assertTrue(client._sandbox)
        self.assertEqual(client._sandbox_shell, ag.sandbox_shell_workspace())


if __name__ == "__main__":
    unittest.main()
