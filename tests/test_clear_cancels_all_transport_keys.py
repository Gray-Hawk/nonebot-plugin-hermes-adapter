"""QQ-08：/clear 必须清掉同一 QQTPC 会话**全部** key 变体下的待汇报事件。

身份判定复用 ``parse_session_key``：同一平台族 + scope + target 的
canonical / OneBot / Milky / 未来未登记 transport 都是同一会话；兄弟群与
其它平台域不得误伤。旧事件复活 = 清场后消费器又把旧 key 的汇报唤醒来。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.background_report_outbox import (  # noqa: E402
    BackgroundReportOutbox,
)
from nonebot_plugin_hermes_adapter.core.background_report_runtime import (  # noqa: E402
    cancel_reports_for_conversation,
)

GROUP = "253384307"
SIBLING = "2533843070"


class ClearCancelsAllTransportKeysTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.outbox = BackgroundReportOutbox(Path(tmp.name) / "outbox.db")
        self.addCleanup(self.outbox.close)
        self.origins = {
            "canonical": f"hermes-qqtpc+group+{GROUP}-g3",
            "onebot": f"hermes-onebotv11+group+{GROUP}-g1057",
            "milky": f"hermes-milky+group+{GROUP}-g5",
            "icqq": f"hermes-icqq+group+{GROUP}-g1",
            "unknown": f"hermes-icqq9+group+{GROUP}-g1",
            "sibling": f"hermes-onebotv11+group+{SIBLING}-g7",
        }
        for name, origin in self.origins.items():
            self.outbox.upsert_event({
                "event_id": name,
                "origin_session_id": origin,
                "state": "completed",
                "result": "done",
                "source": "subagent",
            })

    def _states(self) -> dict[str, str]:
        return {
            name: self.outbox.get_event(name)["report_state"]
            for name in self.origins
        }

    def test_clear_cancels_every_transport_variant_of_the_same_group(self) -> None:
        cancelled = cancel_reports_for_conversation(
            self.outbox,
            adapter="milky",
            is_private=False,
            user_id="10001",
            group_id=GROUP,
            current_session_key=self.origins["canonical"],
            now=1000.0,
        )

        states = self._states()
        for name in ("canonical", "onebot", "milky", "icqq"):
            self.assertEqual(states[name], "cancelled", name)
        self.assertEqual(states["unknown"], "pending",
                         "未登记 transport（icqq9）不是 QQTPC，不被同域清场误伤")
        self.assertEqual(states["sibling"], "pending", "兄弟群不得被同一 /clear 清掉")
        self.assertEqual(
            sorted(cancelled),
            ["canonical", "icqq", "milky", "onebot"],
            "返回的是被取消的事件号（与既有 cancel_session 同语义）",
        )
        cancels = set(self.outbox.pending_session_cancels())
        for name in ("canonical", "onebot", "milky", "icqq"):
            self.assertIn(self.origins[name], cancels)

    def test_official_qq_key_is_not_swept_by_qqtpc_clear(self) -> None:
        self.outbox.upsert_event({
            "event_id": "official",
            "origin_session_id": f"hermes-qq+group+{GROUP}",
            "state": "completed",
            "result": "done",
            "source": "subagent",
        })

        cancel_reports_for_conversation(
            self.outbox,
            adapter="milky",
            is_private=False,
            user_id="10001",
            group_id=GROUP,
            current_session_key=self.origins["canonical"],
            now=1000.0,
        )

        self.assertEqual(self.outbox.get_event("official")["report_state"], "pending")

    def test_private_clear_does_not_touch_group_scope(self) -> None:
        self.outbox.upsert_event({
            "event_id": "priv-canonical",
            "origin_session_id": "hermes-qqtpc+private+10001-g2",
            "state": "completed",
            "result": "done",
            "source": "subagent",
        })

        cancel_reports_for_conversation(
            self.outbox,
            adapter="milky",
            is_private=True,
            user_id="10001",
            group_id=None,
            current_session_key="hermes-qqtpc+private+10001-g2",
            now=1000.0,
        )

        self.assertEqual(self.outbox.get_event("priv-canonical")["report_state"], "cancelled")
        self.assertEqual(self._states()["canonical"], "pending", "群 scope 不受私聊 /clear 影响")


if __name__ == "__main__":
    unittest.main()
