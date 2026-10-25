"""setuptools 入口；扁平开发树必须先组出标准发布暂存区。"""

from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_PACKAGE = _ROOT / "nonebot_plugin_hermes_adapter"
if not (_PACKAGE / "__init__.py").is_file():
    raise SystemExit(
        "找不到标准包目录 nonebot_plugin_hermes_adapter；"
        "请先用发布脚本生成暂存区，再在暂存区构建，避免产出空 wheel"
    )

from setuptools import setup

setup()
