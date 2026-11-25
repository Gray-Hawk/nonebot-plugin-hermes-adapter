"""同域归属：后台状态查询按 parse_session_key 的 scope+target 归属，不靠 transport 枚举。

同一 QQTPC 群的历史 key（canonical / onebotv11 / milky / 未来未登记 transport）
都必须归属同一会话；兄弟群、官方 QQ 域与其它后端前缀必须隔离。运行态与待汇报
两条读路径用同一份身份判定。
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.background_report_outbox import (  # noqa: E402
    BackgroundReportOutbox,
)
from nonebot_plugin_hermes_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_hermes_adapter.core.running_tasks import (  # noqa: E402
    DelegationRunningSource,
    KanbanRunningSource,
)
from nonebot_plugin_hermes_adapter.mcp.tools.background_status import (  # noqa: E402
    GetBackgroundTasksInput,
    get_background_tasks_impl,
)

GROUP = "253384307"
SIBLING = "2533843070"


def _build_board(path: Path, sessions: dict[str, str]) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, title TEXT, status TEXT,
                                          assignee TEXT, result TEXT, session_id TEXT);
        """
    )
    now = time.time()
    for task_id, session_id in sessions.items():
        con.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?)",
            (task_id, f"卡片 {task_id}", "running", "coder", None, session_id),
        )
        now += 0.001
    con.commit()
    con.close()


class BackgroundIdentityAttributionTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.origins = {
            "canonical": f"hermes-qqtpc+group+{GROUP}-g3",
            "onebot": f"hermes-onebotv11+group+{GROUP}-g1057",
            "milky": f"hermes-milky+group+{GROUP}-g5",
            "future": f"hermes-icqq9+group+{GROUP}-g1",
            "sibling": f"hermes-onebotv11+group+{SIBLING}-g7",
            "official": f"hermes-qq+group+{GROUP}",
            "otherbackend": f"astrbot-qqtpc+group+{GROUP}",
        }
        self.outbox = BackgroundReportOutbox(self.dir / "outbox.db")
        self.addCleanup(self.outbox.close)
        for name, origin in self.origins.items():
            self.outbox.upsert_event({
                "event_id": name,
                "origin_session_id": origin,
                "state": "completed",
                "result": "done",
                "source": "subagent",
            })
        self.board = self.dir / "board.db"
        _build_board(self.board, {name: origin for name, origin in self.origins.items()})

    def _query(self, **kwargs):
        return get_background_tasks_impl(
            GetBackgroundTasksInput(adapter="milky", group_id=GROUP),
            scope=CallerScope.dev(),
            running_sources=[KanbanRunningSource(self.board)],
            **kwargs,
        )

    def test_reports_attributed_by_identity(self) -> None:
        result = self._query(outbox=self.outbox)

        self.assertTrue(result.ok, result.error)
        self.assertEqual(
            sorted(item.event_id for item in result.reports),
            ["canonical", "future", "milky", "onebot"],
        )

    def test_running_attributed_by_identity(self) -> None:
        result = self._query(outbox=self.outbox)

        self.assertTrue(result.ok, result.error)
        self.assertEqual(
            sorted(item.task_id for item in result.running),
            ["canonical", "future", "milky", "onebot"],
        )
        self.assertEqual(result.running_sources, ["kanban"])

    def test_running_limit_applies_after_identity_filtering(self) -> None:
        result = self._query(outbox=self.outbox, limit=1)

        self.assertTrue(result.ok, result.error)
        self.assertEqual([item.task_id for item in result.running], ["future"])

    def test_delegation_limit_applies_after_identity_filtering(self) -> None:
        path = self.dir / "delegation.db"
        con = sqlite3.connect(path)
        con.execute(
            "CREATE TABLE bg_completions (delegation_id TEXT, origin_session_id TEXT,"
            " state TEXT, acked INTEGER, dispatched_at REAL, created_at REAL)"
        )
        for index, (name, origin) in enumerate(self.origins.items()):
            con.execute(
                "INSERT INTO bg_completions VALUES (?, ?, 'registered', 0, ?, ?)",
                (name, origin, float(index), float(index)),
            )
        con.commit()
        con.close()

        items = DelegationRunningSource(path).running(
            identity=("qqtpc", "group", GROUP), prefixes=("hermes",), limit=1
        )

        self.assertEqual([item["task_id"] for item in items], ["future"])

    def test_official_qq_caller_is_isolated(self) -> None:
        result = get_background_tasks_impl(
            GetBackgroundTasksInput(adapter="qq", group_id=GROUP),
            scope=CallerScope.dev(),
            outbox=self.outbox,
            running_sources=[KanbanRunningSource(self.board)],
        )

        self.assertTrue(result.ok, result.error)
        self.assertEqual([item.event_id for item in result.reports], ["official"])
        self.assertEqual([item.task_id for item in result.running], ["official"])


if __name__ == "__main__":
    unittest.main()
