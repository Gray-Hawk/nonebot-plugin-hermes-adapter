"""看板「双路通知」告警按会话身份命中，不按 session_id 字符串等值。

同一 QQTPC 会话的 api_server 订阅可能还写着迁移前的 ``hermes-onebotv11+…``，
而卡片 session_id 已是 ``hermes-qqtpc+…`` —— 字符串不等但仍是同一目标，必须
照常告警；兄弟群、官方 QQ 域与其它后端前缀不得误报。
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import kanban_report_source as krs  # noqa: E402

GROUP = "253384307"
SIBLING = "2533843070"
TASK = "t_dual"


class DuplicatePathIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        krs._DUPLICATE_WARNED.clear()
        self.addCleanup(krs._DUPLICATE_WARNED.clear)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.conn = sqlite3.connect(Path(tmp.name) / "board.db")
        self.addCleanup(self.conn.close)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE kanban_notify_subs (task_id TEXT, platform TEXT,
                                             chat_id TEXT, thread_id TEXT);
            """
        )
        self.conn.commit()

    def _sub(self, chat_id: str, *, platform: str = "api_server") -> None:
        self.conn.execute(
            "INSERT INTO kanban_notify_subs VALUES (?,?,?,NULL)",
            (TASK, platform, chat_id),
        )
        self.conn.commit()

    def _warn(self, session_id: str) -> bool:
        with patch.object(krs, "logger") as logging:
            krs.KanbanReportSource._warn_duplicate_path_once(self.conn, TASK, session_id)
        return logging.warning.called

    def test_legacy_transport_subscription_still_matches(self) -> None:
        self._sub(f"hermes-onebotv11+group+{GROUP}-g1057")

        self.assertTrue(
            self._warn(f"hermes-qqtpc+group+{GROUP}-g3"),
            "同一会话的旧 OneBot 订阅与 canonical key 必须按 identity 命中",
        )

    def test_registered_icqq_transport_subscription_still_matches(self) -> None:
        self._sub(f"hermes-icqq+group+{GROUP}-g1")

        self.assertTrue(
            self._warn(f"hermes-qqtpc+group+{GROUP}-g3"),
            "已登记 transport（icqq）与 canonical key 必须按 identity 命中",
        )

    def test_unknown_transport_subscription_does_not_match(self) -> None:
        self._sub(f"hermes-icqq9+group+{GROUP}-g1")

        self.assertFalse(
            self._warn(f"hermes-qqtpc+group+{GROUP}-g3"),
            "未登记 transport 不是 QQTPC，不得被同域清场/订阅命中",
        )

    def test_sibling_group_is_not_matched(self) -> None:
        self._sub(f"hermes-onebotv11+group+{SIBLING}-g7")

        self.assertFalse(self._warn(f"hermes-qqtpc+group+{GROUP}-g3"))

    def test_official_qq_domain_is_not_matched(self) -> None:
        self._sub(f"hermes-qq+group+{GROUP}")

        self.assertFalse(self._warn(f"hermes-qqtpc+group+{GROUP}-g3"))

    def test_other_backend_prefix_is_not_matched(self) -> None:
        self._sub(f"astrbot-qqtpc+group+{GROUP}")

        self.assertFalse(self._warn(f"hermes-qqtpc+group+{GROUP}-g3"))

    def test_warns_only_once_per_target(self) -> None:
        self._sub(f"hermes-milky+group+{GROUP}-g5")

        self.assertTrue(self._warn(f"hermes-qqtpc+group+{GROUP}-g3"))
        self.assertFalse(self._warn(f"hermes-qqtpc+group+{GROUP}-g3"), "同一目标只提示一次")


if __name__ == "__main__":
    unittest.main()
