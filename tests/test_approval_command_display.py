"""审批通知里的命令必须与指令寻址前缀一致,管理员可照抄执行。

文案在 misc_config.bot_name 非空时是 ``/feiyu approve once``(与 handlers.commands
注册的 CMD_PREFIX 同源);空 bot_name 保持旧的 ``/approve ...`` 直连形态。
"""

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import approvals  # noqa: E402


class ApprovalCommandDisplayTests(unittest.TestCase):
    def _entry(self) -> approvals.PendingApproval:
        return approvals.PendingApproval(
            approval_id=1,
            run_id="run-1",
            adapter="onebotv11",
            scope_desc="私聊 123",
            command="pwd",
            choices=["once", "session", "always", "deny"],
        )

    def test_notice_uses_instance_command_prefix(self) -> None:
        config = SimpleNamespace(bot_name="feiyu", approval_expiry_sec=300)
        with patch.object(approvals, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry())
        for choice in ("once", "deny", "session", "always"):
            self.assertIn(f"/feiyu approve {choice}", text)
        self.assertNotIn("回复 /approve", text)

    def test_prefix_takes_first_word_only(self) -> None:
        # 与 handlers.commands 同名口径:bot_name 含空白只取首段。
        config = SimpleNamespace(bot_name="feiyu bot", approval_expiry_sec=300)
        with patch.object(approvals, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry())
        self.assertIn("/feiyu approve once 批准一次", text)
        self.assertNotIn("/feiyu bot", text)

    def test_empty_prefix_preserves_original_commands(self) -> None:
        config = SimpleNamespace(bot_name="", approval_expiry_sec=300)
        with patch.object(approvals, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry())
        self.assertIn("回复 /approve once 批准一次", text)
        self.assertIn("/approve deny 拒绝", text)
        self.assertNotIn("feiyu", text)


if __name__ == "__main__":
    unittest.main()
