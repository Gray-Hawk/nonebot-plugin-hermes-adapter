"""让扁平发布暂存树可按真实插件包名导入，同时避免本地 mcp/ 遮蔽 SDK。"""

from __future__ import annotations

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

package_name = "nonebot_plugin_hermes_adapter"
if package_name not in sys.modules:
    package = types.ModuleType(package_name)
    package.__path__ = [root]
    package.__package__ = package_name
    sys.modules[package_name] = package
