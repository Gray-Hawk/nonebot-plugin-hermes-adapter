"""插件更名 Agent Adapter：新名七处一致、旧名退役、无旧运行兜底。

改名是一次性破坏性更名（发布 ``nonebot-plugin-agent-adapter``、导入
``nonebot_plugin_agent_adapter``、显示 ``Agent Adapter``）：不保留双导入兼容层，
也不允许 site-packages 里装着的旧包把漏改的 import 掩盖成假绿。

本套件锁四件事：

1. 七处名字字面量一致（pyproject 四处 + ``setup.py`` 守卫 + ``config.PLUGIN_MODULE_NAME``
   + ``tests/_bootstrap.py``），版本号与 ``__plugin_meta__`` 显示名同步；
2. 旧模块名 import 必须失败——引导器主动阻断，不靠 site-packages 碰巧没装；
3. 运行期没有旧运行兜底：``PLUGIN_MODULE_NAME`` 是唯一按名解析读点，解析出的目录名是新名；
4. 新名解析到本工作树（flat 开发树），不被已安装的同名包抢走。

旧名的唯一字面量来源是 ``tests._bootstrap._OLD_PACKAGE_NAMES``，本模块不另写死，
避免"为了禁旧名反而到处留旧名"。
"""

from __future__ import annotations

import importlib
import os
import re
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

_SCRATCH = tempfile.TemporaryDirectory()
os.environ.setdefault("LOCALSTORE_DATA_DIR", str(Path(_SCRATCH.name) / "data"))

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_agent_adapter import config as plugin_config_module  # noqa: E402

NEW = "nonebot_plugin_agent_adapter"
NEW_DIST = "nonebot-plugin-agent-adapter"
DISPLAY_NAME = "Agent Adapter"


class NameConsistencyTests(unittest.TestCase):
    """七处字面量必须同源：改单边会产出空 wheel 或插件声明失效。"""

    def setUp(self) -> None:
        self.pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    def test_seven_name_points_agree(self) -> None:
        setup_src = (ROOT / "setup.py").read_text(encoding="utf-8")
        setuptools = self.pyproject["tool"]["setuptools"]
        points = {
            "pyproject.name": self.pyproject["project"]["name"] == NEW_DIST,
            "[tool.nonebot].plugins": self.pyproject["tool"]["nonebot"]["plugins"] == [NEW],
            "packages.find.include": setuptools["packages"]["find"]["include"] == [NEW + "*"],
            "package-data 键": list(setuptools["package-data"]) == [NEW],
            "setup.py 守卫": f'_PACKAGE = _ROOT / "{NEW}"' in setup_src,
            "config.PLUGIN_MODULE_NAME": plugin_config_module.PLUGIN_MODULE_NAME == NEW,
            "tests/_bootstrap.py": _bootstrap.package_name == NEW,
        }
        failed = [name for name, ok in points.items() if not ok]
        self.assertEqual(failed, [], f"名字单点不一致：{failed}")

    def test_version_and_display_name_follow_the_release(self) -> None:
        # 引导器给的是合成包对象（不执行 __init__），所以版本/元数据按源码断言。
        init_src = (ROOT / "__init__.py").read_text(encoding="utf-8")
        match = re.search(r'^__version__ = "([^"]+)"', init_src, re.MULTILINE)
        assert match is not None, "找不到 __version__"
        self.assertEqual(match.group(1), self.pyproject["project"]["version"])
        self.assertEqual(match.group(1), "0.2.0")
        self.assertIn(f'name="{DISPLAY_NAME}"', init_src)
        # 描述不再把插件自身标成某个后端；上游分支说明与 Hermes 后端指称不受影响。
        self.assertNotIn("Hermes Agent API Server", init_src)


class OldNameRetirementTests(unittest.TestCase):
    """旧名必须真的不可导入，且代码里没有旧运行兜底。"""

    def test_old_module_names_are_not_importable(self) -> None:
        self.assertTrue(_bootstrap._OLD_PACKAGE_NAMES, "引导器必须显式列出被退役的旧名")
        for old in _bootstrap._OLD_PACKAGE_NAMES:
            self.assertNotEqual(old, NEW)
            with self.assertRaises(ModuleNotFoundError):
                importlib.import_module(old)

    def test_old_name_does_not_survive_as_a_runtime_fallback(self) -> None:
        # 按名解析的唯一读点就是 PLUGIN_MODULE_NAME：解析出的目录名必须已经是新名。
        module_name = plugin_config_module.PLUGIN_MODULE_NAME
        self.assertEqual(module_name, NEW)
        for old in _bootstrap._OLD_PACKAGE_NAMES:
            self.assertNotIn(old, module_name)
        # 其它套件会就地改共享的 plugin_config，这里钉住"未配置定制目录"的形态。
        with patch.object(
            plugin_config_module, "plugin_config", SimpleNamespace(hermes_custom_dir="")
        ):
            data_dir = plugin_config_module.resolve_custom_dir()
        self.assertIsNotNone(data_dir)
        self.assertEqual(Path(data_dir).name, NEW, str(data_dir))

    def test_new_package_resolves_to_this_worktree(self) -> None:
        package = importlib.import_module(NEW)
        roots = [Path(entry).resolve() for entry in package.__path__]
        self.assertIn(ROOT.resolve(), roots, f"新名被别的安装树抢走：{roots}")


class NeutralIdentityTests(unittest.TestCase):
    """脱敏后的对外标识保持中性：改名不回退成任何一侧的插件名。"""

    def test_acp_client_info_carries_no_plugin_name(self) -> None:
        from nonebot_plugin_agent_adapter.backends.antigravity import client as acp

        name = acp._CLIENT_INFO["name"]
        for forbidden in (NEW, *_bootstrap._OLD_PACKAGE_NAMES):
            self.assertNotIn(forbidden, name)
        self.assertNotIn("hermes", name.lower())

    def test_log_prefix_stays_hermes_while_plugin_line_is_renamed(self) -> None:
        init_src = (ROOT / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("[HERMES]", init_src, "日志前缀是运维检索锚，本轮不改")
        self.assertIn("Agent Adapter loaded", init_src)
        self.assertNotIn("Hermes Plugin loaded", init_src)


if __name__ == "__main__":
    unittest.main()
