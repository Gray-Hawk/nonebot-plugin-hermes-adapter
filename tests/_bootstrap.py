"""让扁平发布暂存树可按真实插件包名导入，同时避免本地 mcp/ 遮蔽 SDK。"""

from __future__ import annotations

import importlib.abc
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
root = str(ROOT)
saved_path = list(sys.path)
try:
    sys.path = [
        item for item in sys.path if Path(item or ".").resolve() != ROOT
    ]
    for name, module in list(sys.modules.items()):
        if name != "mcp" and not name.startswith("mcp."):
            continue
        file = getattr(module, "__file__", "") or ""
        if file and Path(file).resolve().is_relative_to(ROOT):
            sys.modules.pop(name, None)
    import mcp.types  # noqa: F401
finally:
    sys.path = saved_path

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init()

package_name = "nonebot_plugin_agent_adapter"
if package_name not in sys.modules:
    package = types.ModuleType(package_name)
    package.__path__ = [root]
    package.__package__ = package_name
    sys.modules[package_name] = package

# 旧模块名阻断：生产 venv 的 site-packages 里仍装着改名前的包（生产冷迁移未做），
# 只把 shim 换成新名不够——漏改的套件会解析到已安装的旧包并假绿。这里让旧名的
# import 显式失败，并在失败信息里直说改名事实。
_OLD_PACKAGE_NAMES = ("nonebot_plugin_hermes_adapter",)


class _BlockOldPackageName(importlib.abc.MetaPathFinder):
    """旧模块名一律 ModuleNotFoundError，不给 site-packages 旧包遮罩的机会。"""

    def find_spec(self, fullname, path=None, target=None):
        for old in _OLD_PACKAGE_NAMES:
            if fullname == old or fullname.startswith(old + "."):
                raise ModuleNotFoundError(
                    f"{fullname}: 插件已更名为 {package_name}，旧模块名不再可用",
                    name=fullname,
                )
        return None


for _name in list(sys.modules):
    if _name in _OLD_PACKAGE_NAMES or _name.startswith(
        tuple(old + "." for old in _OLD_PACKAGE_NAMES)
    ):
        sys.modules.pop(_name, None)

if not any(isinstance(finder, _BlockOldPackageName) for finder in sys.meta_path):
    sys.meta_path.insert(0, _BlockOldPackageName())
