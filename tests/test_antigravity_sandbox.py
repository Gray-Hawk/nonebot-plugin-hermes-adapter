"""ACP 终端沙箱常开（缓存工作区 + BASH_ENV 绕行）离线回归。

真实边界（2026-11-16 已用官方 `agy acp --sandbox` 实测）：session cwd 在沙箱里被
挂成只读、`workspaceDirs=[]`；未声明为 cwd／additionalDirectories 的
`~/.cache/<workspace>` 可写；`run_command` 用 `bash -c` 并保留启动环境里的
`BASH_ENV`。主人 2026-11-16 定案的契约是**沙箱在插件 ACP 侧常开、没有关闭分支**：

- 不新增沙箱专用配置开关；`AGENT_ANTIGRAVITY_ARGS` 仍只是通用 argv 入口。
- 读 agy 持久设置 `enableTerminalSandbox`：true 时复用、不追加 `--sandbox`；
  false／缺省／读不到时由插件追加一项。两种情况都不重复追加。
- 启动时始终创建缓存工作区、重写受控 BASH_ENV、始终把 BASH_ENV 传给子进程。
- session/new／resume 的 cwd 保持原 ACP workspace；缓存目录不进 additionalDirectories；
  只读 agy settings，不改全局设置、不设 toolPermission、不启用 dangerously-skip-permissions。

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


class SandboxResolutionTests(unittest.TestCase):
    """纯函数契约：持久设置读取、argv 收敛、默认缓存路径、提示词一句话。"""

    def test_effective_args_reuses_persisted_true(self) -> None:
        ag = mod()
        argv, reused = ag.effective_sandbox_args(("acp", "--process-per-session"), True)
        self.assertEqual(argv, ("acp", "--process-per-session"))
        self.assertNotIn(ag.SANDBOX_FLAG, argv)
        self.assertTrue(reused)

    def test_effective_args_appends_when_persisted_false(self) -> None:
        ag = mod()
        argv, reused = ag.effective_sandbox_args(("acp",), False)
        self.assertEqual(argv, ("acp", ag.SANDBOX_FLAG))
        self.assertFalse(reused)

    def test_effective_args_never_duplicates_explicit_flag(self) -> None:
        ag = mod()
        argv, reused = ag.effective_sandbox_args(("acp", ag.SANDBOX_FLAG), True)
        self.assertEqual(argv.count(ag.SANDBOX_FLAG), 1)
        self.assertFalse(reused)
        argv, reused = ag.effective_sandbox_args(("acp", ag.SANDBOX_FLAG), False)
        self.assertEqual(argv.count(ag.SANDBOX_FLAG), 1)
        self.assertFalse(reused)

    def test_effective_args_overrides_explicit_off(self) -> None:
        """沙箱没有关闭分支：通用 argv 里写 `--sandbox=false` 也照样补一项（后者生效）。"""
        ag = mod()
        for persisted in (True, False):
            argv, reused = ag.effective_sandbox_args(
                ("acp", f"{ag.SANDBOX_FLAG}=false"), persisted
            )
            self.assertEqual(argv.count(ag.SANDBOX_FLAG), 1)
            self.assertEqual(argv[-1], ag.SANDBOX_FLAG)
            self.assertFalse(reused)

    def test_persisted_setting_only_accepts_exact_true(self) -> None:
        ag = mod()
        with tempfile.TemporaryDirectory(prefix="agy-settings-") as tmp:
            path = Path(tmp) / "settings.json"
            path.write_text(json.dumps({"enableTerminalSandbox": True}), encoding="utf-8")
            self.assertTrue(ag.persisted_sandbox_enabled(path))

            path.write_text(json.dumps({"enableTerminalSandbox": False}), encoding="utf-8")
            self.assertFalse(ag.persisted_sandbox_enabled(path))

            path.write_text(json.dumps({"enableTerminalSandbox": "true"}), encoding="utf-8")
            self.assertFalse(ag.persisted_sandbox_enabled(path), "字符串不是设置里的 true")

            path.write_text(json.dumps({"trustedWorkspaces": ["/home/u"]}), encoding="utf-8")
            self.assertFalse(ag.persisted_sandbox_enabled(path), "缺键按未开启")

            path.write_text("{not json", encoding="utf-8")
            self.assertFalse(ag.persisted_sandbox_enabled(path), "坏 JSON 按未开启")

            path.write_text("[]", encoding="utf-8")
            self.assertFalse(ag.persisted_sandbox_enabled(path), "非对象按未开启")

            path.unlink()
            self.assertFalse(ag.persisted_sandbox_enabled(path), "文件缺失按未开启")

    def test_settings_path_follows_agy_dirs(self) -> None:
        ag = mod()
        self.assertEqual(
            ag.agy_settings_path(()),
            Path.home() / ".gemini" / "antigravity-cli" / "settings.json",
        )
        # 目录参数（用户可能经 AGENT_ANTIGRAVITY_ARGS 传）与官方环境变量都要跟。
        self.assertEqual(
            ag.agy_settings_path(("acp", "--gemini-dir", "/srv/g", "--app-data-dir", "cli-x")),
            Path("/srv/g/cli-x/settings.json"),
        )
        self.assertEqual(
            ag.agy_settings_path(("acp", "--gemini_dir=/srv/g2")),
            Path("/srv/g2/antigravity-cli/settings.json"),
        )
        with patch.dict(
            os.environ,
            {"ANTIGRAVITY_GEMINI_DIR": "/srv/env-g", "ANTIGRAVITY_APP_DATA_DIR": "env-cli"},
        ):
            self.assertEqual(
                ag.agy_settings_path(()), Path("/srv/env-g/env-cli/settings.json")
            )
        # 相对目录按 agy 口径落在 home 下
        self.assertEqual(
            ag.agy_settings_path(("acp", "--gemini-dir", "rel-g")),
            Path.home() / "rel-g" / "antigravity-cli" / "settings.json",
        )

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
        self.settings = self.tmp / "gemini" / "antigravity-cli" / "settings.json"
        self.store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(self.store.close)
        self.clients = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close()

    def write_settings(self, data: dict) -> None:
        self.settings.parent.mkdir(parents=True, exist_ok=True)
        self.settings.write_text(json.dumps(data), encoding="utf-8")

    def make_client(self, *extra: str, settings: dict | None = None):
        ag = mod()
        if settings is not None:
            self.write_settings(settings)
        client = ag.AntigravityACPClient(
            cli_path=sys.executable,
            cli_args=[str(FIXTURE), str(self.log_path), json.dumps(REPLY, ensure_ascii=False), *extra],
            workspace=self.workspace,
            prompt_timeout=6.0,
            rpc_timeout=5.0,
            sandbox_shell_dir=self.shell,
            agy_settings_file=self.settings,
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

    def appended_prompt(self, params: dict) -> str:
        return params["_meta"]["antigravity"]["systemPrompt"]["append"]

    async def prompt(self, client, system_prompt, text="hi"):  # noqa: ANN001
        return await client.prompt(
            session_key="k1", system_prompt=system_prompt, user_text=text, store=self.store
        )

    def assert_sandbox_prepared(self, client) -> None:  # noqa: ANN001
        """两种沙箱来源共同的部分：缓存工作区、BASH_ENV 文件与子进程 env。"""
        self.assertTrue(self.shell.is_dir(), "启动时必须准备缓存工作区")
        env_file = self.bash_env_file()
        self.assertTrue(env_file.is_file(), "启动时必须重写受控 BASH_ENV")
        self.assertEqual(env_file.parent, self.workspace)
        self.assertEqual(env_file.read_text(encoding="utf-8"), f"cd {self.shell}\n")
        self.assertEqual(self.spawn_env().get("BASH_ENV"), str(env_file))
        # 缓存路径只出现在提示词那一句说明里，不能作为工作区声明出现
        new = self.rows("session/new")[0]["params"]
        self.assertEqual(new["cwd"], str(self.workspace))
        self.assertNotIn("additionalDirectories", new)
        self.assertNotEqual(new["cwd"], str(self.shell))
        self.assertEqual(
            self.appended_prompt(new), f"SYS-A\n\n{mod().sandbox_workspace_notice(self.shell)}"
        )

    # --------------------------------------------- agy 持久设置 true：复用
    async def test_persisted_true_reuses_sandbox_without_flag(self) -> None:
        client = self.make_client(settings={"enableTerminalSandbox": True})
        await self.prompt(client, "SYS-A")

        self.assertNotIn(mod().SANDBOX_FLAG, self.spawn_argv(), "持久 true 不得重复补 --sandbox")
        self.assertTrue(client._sandbox_reused)
        self.assert_sandbox_prepared(client)

    async def test_persisted_true_does_not_modify_settings_file(self) -> None:
        original = {"enableTerminalSandbox": True, "trustedWorkspaces": ["/home/u"]}
        client = self.make_client(settings=original)
        await self.prompt(client, "SYS-A")
        self.assertEqual(json.loads(self.settings.read_text(encoding="utf-8")), original)

    # ------------------------------------- 持久 false／缺省：插件补 flag
    async def test_persisted_false_appends_sandbox_flag(self) -> None:
        client = self.make_client(settings={"enableTerminalSandbox": False})
        await self.prompt(client, "SYS-A")

        self.assertEqual(self.spawn_argv().count(mod().SANDBOX_FLAG), 1)
        self.assertFalse(client._sandbox_reused)
        self.assert_sandbox_prepared(client)

    async def test_missing_settings_appends_sandbox_flag(self) -> None:
        # 设置文件不存在（本机现状：只有 trustedWorkspaces，没有该键）→ 插件补 flag。
        client = self.make_client()
        await self.prompt(client, "SYS-A")

        self.assertEqual(self.spawn_argv().count(mod().SANDBOX_FLAG), 1)
        self.assertFalse(client._sandbox_reused)
        self.assert_sandbox_prepared(client)

    async def test_other_keys_only_appends_sandbox_flag(self) -> None:
        client = self.make_client(settings={"trustedWorkspaces": ["/home/u"]})
        await self.prompt(client, "SYS-A")
        self.assertEqual(self.spawn_argv().count(mod().SANDBOX_FLAG), 1)
        self.assert_sandbox_prepared(client)

    # ------------------------------------------------ flag 不重复 / 显式参数
    async def test_explicit_flag_with_persisted_true_is_not_duplicated(self) -> None:
        client = self.make_client("--sandbox", settings={"enableTerminalSandbox": True})
        await self.prompt(client, "SYS-A")
        self.assertEqual(self.spawn_argv().count(mod().SANDBOX_FLAG), 1)
        self.assertFalse(client._sandbox_reused)
        self.assert_sandbox_prepared(client)

    async def test_explicit_flag_with_persisted_false_is_not_duplicated(self) -> None:
        client = self.make_client("--sandbox", settings={"enableTerminalSandbox": False})
        await self.prompt(client, "SYS-A")
        self.assertEqual(self.spawn_argv().count(mod().SANDBOX_FLAG), 1)
        self.assert_sandbox_prepared(client)

    # ------------------------------------------------ 常开的文件／env 行为
    async def test_bash_env_overrides_inherited_value(self) -> None:
        with patch.dict(os.environ, {"BASH_ENV": str(self.tmp / "parent-bash-env")}):
            client = self.make_client(settings={"enableTerminalSandbox": True})
            await self.prompt(client, "SYS-A")
        self.assertEqual(self.spawn_env().get("BASH_ENV"), str(self.bash_env_file()))
        self.assert_sandbox_prepared(client)

    async def test_bash_env_file_is_regenerated_at_start(self) -> None:
        stale = self.bash_env_file()
        stale.write_text("cd /wrong/place\n", encoding="utf-8")
        client = self.make_client(settings={"enableTerminalSandbox": False})
        await self.prompt(client, "SYS-A")
        self.assertEqual(stale.read_text(encoding="utf-8"), f"cd {self.shell}\n")

    # ------------------------------------------------ new／resume 与提示词
    async def test_notice_on_new_and_resume_matches(self) -> None:
        ag = mod()
        client = self.make_client(settings={"enableTerminalSandbox": True})
        notice = ag.sandbox_workspace_notice(self.shell)

        await self.prompt(client, "SYS-A")
        new = self.rows("session/new")[0]["params"]
        self.assertEqual(self.appended_prompt(new), f"SYS-A\n\n{notice}")

        # 同一系统上下文 resume：哈希不变就不重复 append
        await self.prompt(client, "SYS-A", text="again")
        first_resume = self.rows("session/resume")[0]["params"]
        self.assertNotIn("_meta", first_resume)
        self.assertEqual(first_resume["cwd"], str(self.workspace))
        self.assertNotIn("additionalDirectories", first_resume)

        # 换系统上下文时 resume 也要带同一句工作区说明
        await self.prompt(client, "SYS-B", text="third")
        resume = self.rows("session/resume")[-1]["params"]
        self.assertEqual(self.appended_prompt(resume), f"SYS-B\n\n{notice}")
        self.assertEqual(self.store.load_acp_session("k1")[1], sha(f"SYS-B\n\n{notice}"))

    async def test_control_turn_keeps_hash_and_no_meta(self) -> None:
        ag = mod()
        client = self.make_client(settings={"enableTerminalSandbox": False})
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


class SandboxConfigTests(unittest.TestCase):
    """配置面：AGENT_ANTIGRAVITY_ARGS 只是通用 argv 载体，不是沙箱开关。"""

    def test_config_exposes_args_with_empty_default(self) -> None:
        from nonebot_plugin_agent_adapter.config import Config

        defaults = Config()
        self.assertEqual(defaults.hermes_antigravity_args, "")
        field = Config.model_fields["hermes_antigravity_args"]
        self.assertEqual(field.alias, "agent_antigravity_args")
        validated = Config.model_validate({"agent_antigravity_args": "--sandbox"})
        self.assertEqual(validated.hermes_antigravity_args, "--sandbox")

    def test_get_client_resolves_sandbox_from_settings_and_args(self) -> None:
        ag = mod()
        from nonebot_plugin_agent_adapter.config import plugin_config

        previous_client = ag._client
        ag._client = None

        def restore_client() -> None:
            ag._client = previous_client

        self.addCleanup(restore_client)
        previous = plugin_config.hermes_antigravity_args
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_antigravity_args", previous
        )

        settings = self.tmp_join("settings.json")
        settings.parent.mkdir(parents=True, exist_ok=True)
        settings.write_text("{}", encoding="utf-8")
        path_patch = patch.object(ag, "agy_settings_path", return_value=settings)
        path_patch.start()
        self.addCleanup(path_patch.stop)

        # 持久设置缺省 → 插件补 --sandbox；配置的通用 argv 本身不被改写
        object.__setattr__(plugin_config, "hermes_antigravity_args", "")
        client = ag.get_client()
        self.assertEqual(client._cli_args, ag.DEFAULT_ACP_ARGS)
        self.assertEqual(
            client._effective_cli_args, (*ag.DEFAULT_ACP_ARGS, ag.SANDBOX_FLAG)
        )
        self.assertFalse(client._sandbox_reused)
        self.assertEqual(client._sandbox_shell, ag.sandbox_shell_workspace())
        self.assertEqual(client._agy_settings_file, settings)

        # 持久设置 true → 复用，不追加
        settings.write_text('{"enableTerminalSandbox": true}', encoding="utf-8")
        ag._client = None
        client = ag.get_client()
        self.assertEqual(client._effective_cli_args, ag.DEFAULT_ACP_ARGS)
        self.assertTrue(client._sandbox_reused)

        # 通用 argv 显式给 --sandbox：原样保留，不重复
        settings.write_text("{}", encoding="utf-8")
        object.__setattr__(plugin_config, "hermes_antigravity_args", "--sandbox")
        ag._client = None
        client = ag.get_client()
        self.assertEqual(
            client._effective_cli_args, (*ag.DEFAULT_ACP_ARGS, ag.SANDBOX_FLAG)
        )
        self.assertEqual(client._effective_cli_args.count(ag.SANDBOX_FLAG), 1)
        self.assertFalse(client._sandbox_reused)

    def tmp_join(self, name: str) -> Path:
        self._settings_tmp = tempfile.TemporaryDirectory(prefix="agy-get-client-")
        self.addCleanup(self._settings_tmp.cleanup)
        return Path(self._settings_tmp.name) / name


if __name__ == "__main__":
    unittest.main()
