"""用户可见文本的中性措辞回归：`/ping` 与内置 `/help`。

产品口径：用户只看得到「AI 助手」，不写死某个后端名——换后端时 `/ping` 与
`/help` 不能继续自称别的后端。包名、配置键、日志前缀与 Hermes 主后端是结构
兼容面，**不在此断言范围**（也不得被本模块的措辞要求反向改掉）。

只测两个纯拼装函数（`build_ping_message` / `build_help_text`），不启 bot、
不发真实消息。
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ["LOCALSTORE_DATA_DIR"] = str(_ROOT / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_ROOT / "cfg")
(_ROOT / "cfg").mkdir()

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.chat_config import ChatConfig  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import commands  # noqa: E402


class PingTextTests(unittest.TestCase):
    def test_ping_texts_are_backend_neutral(self) -> None:
        ok_text = commands.build_ping_message(True)
        fail_text = commands.build_ping_message(False)

        for text in (ok_text, fail_text):
            self.assertNotIn("hermes", text.lower(), text)
            self.assertIn("AI 助手", text)

        # 既有可读形态与排障指向不变，只换称谓。
        self.assertIn("pong", ok_text)
        self.assertIn("Gateway", fail_text)


class HelpTextTests(unittest.TestCase):
    def setUp(self) -> None:
        # 命令清单依赖 chat_config：钉住「空闲重置关闭」，命令表才确定。
        patcher = patch.object(
            commands, "load_chat_config", lambda: ChatConfig(session_idle_reset_sec=0)
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_help_texts_are_backend_neutral(self) -> None:
        private = commands.build_help_text(True, False)
        group = commands.build_help_text(False, False)

        for text in (private, group):
            self.assertNotIn("hermes", text.lower(), text)
            self.assertIn("AI 助手帮助", text)
            for name, title in (
                ("clear", "重置对话"),
                ("ping", "检查连接状态"),
                ("help", "显示本帮助"),
            ):
                self.assertIn(f"{commands._display_cmd(name)} - {title}", text)
            self.assertNotIn(commands._display_cmd("delay"), text)
            self.assertNotIn("管理员", text)

        self.assertIn("直接发送消息即可与 AI 对话", private)
        self.assertIn("@我 发送消息即可与 AI 对话", group)


if __name__ == "__main__":
    unittest.main()
