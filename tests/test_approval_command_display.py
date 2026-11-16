"""审批通知里的命令必须与指令寻址前缀一致,管理员可照抄执行。

文案在 misc_config.bot_name 非空时是 ``/feiyu approve once``(与 handlers.commands
注册的 CMD_PREFIX 同源);空 bot_name 保持旧的 ``/approve ...`` 直连形态。

``bot_name`` 规定重启生效:命令 matcher 在插件加载期定型,``/reload-config`` 只换
快照。因此通知必须读同一个启动期前缀,热重载后重启前仍给旧命令名(旧命令才是
真正注册着的那条),重启后才采用新名字。
"""

import ast
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import approvals, misc_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core.config_reload import reload_stores  # noqa: E402
from nonebot_plugin_hermes_adapter.core.config_store import (  # noqa: E402
    ConfigSnapshotStore,
)
from nonebot_plugin_hermes_adapter.handlers import commands  # noqa: E402


class ApprovalCommandDisplayTests(unittest.TestCase):
    def setUp(self) -> None:
        # 启动期前缀是进程级的一次性定型:每个用例重新定型,跑完清掉缓存,
        # 免得多份用例互相看到对方冻结的前缀。
        misc_config.startup_command_prefix.cache_clear()
        self.addCleanup(misc_config.startup_command_prefix.cache_clear)

    def _entry(self) -> approvals.PendingApproval:
        return approvals.PendingApproval(
            approval_id=1,
            run_id="run-1",
            adapter="onebotv11",
            scope_desc="私聊 123",
            command="pwd",
            choices=["once", "session", "always", "deny"],
        )

    def _temp_store(self) -> tuple[Path, ConfigSnapshotStore]:
        directory = tempfile.TemporaryDirectory(prefix="approval-prefix-")
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "misc_config.yaml"
        store = ConfigSnapshotStore(
            label="misc_config",
            path_provider=lambda: path,
            parser=misc_config.parse_misc_config,
            default_factory=misc_config.MiscConfig,
        )
        return path, store

    def test_notice_uses_instance_command_prefix(self) -> None:
        config = SimpleNamespace(bot_name="feiyu", approval_expiry_sec=300)
        with patch.object(misc_config, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry())
        for choice in ("once", "deny", "session", "always"):
            self.assertIn(f"/feiyu approve {choice}", text)
        self.assertNotIn("回复 /approve", text)

    def test_prefix_takes_first_word_only(self) -> None:
        # 与 handlers.commands 同名口径:bot_name 含空白只取首段。
        config = SimpleNamespace(bot_name="feiyu bot", approval_expiry_sec=300)
        with patch.object(misc_config, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry())
        self.assertIn("/feiyu approve once 批准一次", text)
        self.assertNotIn("/feiyu bot", text)
        # 空白回退:空/纯空白 bot_name 仍走旧的裸命令形态。
        self.assertEqual(misc_config.bot_name_prefix("feiyu bot"), "feiyu")
        self.assertEqual(misc_config.bot_name_prefix("   "), "")
        self.assertEqual(misc_config.bot_name_prefix(""), "")

    def test_empty_prefix_preserves_original_commands(self) -> None:
        config = SimpleNamespace(bot_name="", approval_expiry_sec=300)
        with patch.object(misc_config, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry())
        self.assertIn("回复 /approve once 批准一次", text)
        self.assertIn("/approve deny 拒绝", text)
        self.assertNotIn("feiyu", text)

    def test_reload_config_keeps_notice_prefix_until_restart(self) -> None:
        """R1/P2 回归:热重载换了 bot_name,通知仍须给出实际注册着的旧命令。

        真实临时 ConfigSnapshotStore + reload_stores:``bot_name: old`` 启动定型后
        改成 ``new``/空串并重载,快照确实换了,但通知前缀必须原地不动;只有模拟
        重启(新进程重新定型)才允许采用新名字。
        """
        path, store = self._temp_store()
        path.write_text("bot_name: old\n", encoding="utf-8")
        with patch.object(misc_config, "_store", store):
            # 启动:matcher 与通知共用这一份定型值。
            self.assertEqual(misc_config.startup_command_prefix(), "old")
            before = approvals.build_admin_notice(self._entry())
            self.assertIn("回复 /old approve once", before)

            # /reload-config 换成 new / 空串:快照换了,前缀和通知都不许动。
            for new in ("new", ""):
                path.write_text(f"bot_name: {new!r}\n", encoding="utf-8")
                result = reload_stores((store,))
                self.assertTrue(result.ok, result.message)
                self.assertEqual(misc_config.load_misc_config().bot_name, new)
                self.assertEqual(misc_config.startup_command_prefix(), "old")
                self.assertEqual(approvals.build_admin_notice(self._entry()), before)

            # 重启=新进程重新定型:这时才轮到快照里的名字(此刻是空串,旧直连形态)。
            misc_config.startup_command_prefix.cache_clear()
            self.assertEqual(misc_config.startup_command_prefix(), "")
            self.assertIn("回复 /approve once 批准一次", approvals.build_admin_notice(self._entry()))

            # 空串再改回 new 后重启:新名字生效。
            path.write_text("bot_name: 'new'\n", encoding="utf-8")
            self.assertTrue(reload_stores((store,)).ok)
            misc_config.startup_command_prefix.cache_clear()
            self.assertEqual(misc_config.startup_command_prefix(), "new")
            self.assertIn("回复 /new approve once", approvals.build_admin_notice(self._entry()))

    def test_matcher_and_notice_share_one_startup_prefix_source(self) -> None:
        """handlers.commands 的注册前缀与审批通知读同一处,防止再次分叉。"""
        source = (ROOT / "handlers" / "commands.py").read_text(encoding="utf-8")
        assigned: list[ast.expr] = []
        imported = False
        for node in ast.parse(source).body:
            if isinstance(node, ast.ImportFrom) and (node.module or "").endswith(
                "core.misc_config"
            ):
                imported = imported or any(
                    alias.name == "startup_command_prefix" for alias in node.names
                )
            elif isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "CMD_PREFIX"
                for target in node.targets
            ):
                assigned.append(node.value)
        self.assertTrue(imported, "commands.py 须从 misc_config 导入 startup_command_prefix")
        self.assertEqual(len(assigned), 1, f"CMD_PREFIX 只应有启动期一处赋值, 实际={len(assigned)}")
        value = assigned[0]
        self.assertIsInstance(value, ast.Call)
        func_id = (
            value.func.id
            if isinstance(value, ast.Call) and isinstance(value.func, ast.Name)
            else None
        )
        self.assertEqual(func_id, "startup_command_prefix")

        # 通知侧不得再直接读 bot_name 实时快照(只允许经 startup_command_prefix)。
        self.assertNotIn(".bot_name", Path(approvals.__file__).read_text(encoding="utf-8"))
        # 同一进程内两侧读到的是同一个启动期值。
        self.assertEqual(commands.CMD_PREFIX, misc_config.startup_command_prefix())


if __name__ == "__main__":
    unittest.main()
