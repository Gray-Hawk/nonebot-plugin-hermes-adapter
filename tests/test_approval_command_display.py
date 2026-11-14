import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_agent_adapter.core import approvals  # noqa: E402


class ApprovalCommandDisplayTests(unittest.TestCase):
    def _entry(self, backend: str) -> approvals.PendingApproval:
        return approvals.PendingApproval(
            approval_id=1,
            run_id="run-1" if backend == "runs" else "",
            adapter="onebotv11",
            scope_desc="私聊 123",
            command="pwd",
            choices=["once", "deny"] if backend == "antigravity" else ["once", "session", "always", "deny"],
            backend=backend,
            session_key="session-1" if backend == "antigravity" else None,
        )

    def test_acp_notice_uses_instance_command_prefix(self) -> None:
        config = SimpleNamespace(bot_name="feiyu", approval_expiry_sec=300)
        with patch.object(approvals, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry("antigravity"))
        self.assertIn("/feiyu approve once 批准一次", text)
        self.assertIn("/feiyu approve deny 拒绝", text)
        self.assertNotIn("回复 /approve", text)
        self.assertNotIn("session", text)
        self.assertNotIn("always", text)

    def test_runs_notice_uses_same_instance_prefix(self) -> None:
        config = SimpleNamespace(bot_name="feiyu", approval_expiry_sec=300)
        with patch.object(approvals, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry("runs"))
        for choice in ("once", "deny", "session", "always"):
            self.assertIn(f"/feiyu approve {choice}", text)
        self.assertNotIn("回复 /approve", text)

    def test_empty_prefix_preserves_original_commands(self) -> None:
        config = SimpleNamespace(bot_name="", approval_expiry_sec=300)
        with patch.object(approvals, "load_misc_config", return_value=config):
            text = approvals.build_admin_notice(self._entry("runs"))
        self.assertIn("回复 /approve once 批准一次", text)
        self.assertIn("/approve deny 拒绝", text)
        self.assertNotIn("feiyu", text)


if __name__ == "__main__":
    unittest.main()
