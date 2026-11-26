"""QQTPC 交付身份：scope / inflight / 代际栅栏按业务会话域，不按 transport。

- ``make_scope_key`` / ``make_inflight_key`` 产出 canonical ``qqtpc:*``；官方 QQ 与
  未知平台保持自己的标签。
- 跨协议 ``/clear``（任意 transport 触发）必须取消同一 qqtpc 域下**全部** transport
  写过的 ready 交付，且只落一条 canonical fence（迁移后不再生成 transport fence）。
- 代际水位单调：旧代际/病态 0 不得把已有 fence generation 改小或归零。
- ``runs_delivery.yaml`` 的 repair_scopes 接受 ``qqtpc:*`` 新写法并兼容旧
  ``onebotv11:*`` / ``milky:*`` 条目；实际发送 adapter 仍是台账里的真实值。
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.inflight import (  # noqa: E402
    InflightRegistry,
    make_inflight_key,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import (  # noqa: E402
    RunTracker,
    make_scope_key,
)
from nonebot_plugin_hermes_adapter.core.runs_delivery_config import (  # noqa: E402
    RunsDeliveryConfig,
    effective_mode,
    stable_scope,
)

GROUP = "462540549"
PRIVATE = "10001"


class ScopeKeyTests(unittest.TestCase):
    def test_scope_key_is_domain_based(self) -> None:
        self.assertEqual(make_scope_key("onebotv11", False, "u", GROUP), f"qqtpc:group:{GROUP}")
        self.assertEqual(make_scope_key("Milky", True, PRIVATE, None), f"qqtpc:private:{PRIVATE}")
        self.assertEqual(make_scope_key("qqtpc", False, "u", GROUP), f"qqtpc:group:{GROUP}")
        self.assertEqual(make_scope_key("qq", False, "u", "OPENID"), "qq:group:OPENID")
        self.assertEqual(make_scope_key("astrbot", False, "u", GROUP), f"astrbot:group:{GROUP}")

    def test_inflight_key_is_domain_based(self) -> None:
        self.assertEqual(
            make_inflight_key("onebotv11", False, "", GROUP), ("qqtpc", f"group:{GROUP}")
        )
        self.assertEqual(
            make_inflight_key("milky", True, PRIVATE, None), ("qqtpc", f"private:{PRIVATE}")
        )
        self.assertEqual(make_inflight_key("qq", False, "", "OPENID"), ("qq", "group:OPENID"))
        self.assertEqual(
            make_inflight_key("astrbot", False, "", GROUP), ("astrbot", f"group:{GROUP}")
        )

    def test_registry_sees_same_key_across_transports(self) -> None:
        registry = InflightRegistry()
        self.assertEqual(
            registry.try_enter(
                make_inflight_key("onebotv11", False, "", GROUP),
                current_msg=None,
                is_direct=True,
                original_msg_id=None,
                now_ms=1,
                commission=True,
            ),
            "entered",
        )
        self.assertTrue(
            registry.is_inflight(make_inflight_key("milky", False, "", GROUP)),
            "同一 QQ 群在协议切换后仍是同一个 in-flight 槽",
        )
        self.assertFalse(registry.is_inflight(make_inflight_key("qq", False, "", GROUP)))


class StableScopeTests(unittest.TestCase):
    def test_stable_scope_uses_domain(self) -> None:
        self.assertEqual(stable_scope("milky", False, None, GROUP), f"qqtpc:group:{GROUP}")
        self.assertEqual(stable_scope("qq", True, "OPENID", None), "qq:private:OPENID")

    def test_qqtpc_repair_scope_promotes_any_transport_row(self) -> None:
        cfg = RunsDeliveryConfig(mode="observe", repair_scopes=(f"qqtpc:group:{GROUP}",))
        self.assertEqual(
            effective_mode(cfg, adapter="onebotv11", is_private=False, group_id=GROUP),
            "repair",
        )
        self.assertEqual(
            effective_mode(cfg, adapter="milky", is_private=False, group_id=GROUP),
            "repair",
        )
        self.assertEqual(
            effective_mode(cfg, adapter="qq", is_private=False, group_id=GROUP),
            "observe",
            "官方 QQ 不是 QQTPC 域，不得被 qqtpc 条目提升",
        )

    def test_legacy_transport_repair_scope_still_matches_after_switch(self) -> None:
        cfg = RunsDeliveryConfig(mode="observe", repair_scopes=(f"onebotv11:group:{GROUP}",))
        self.assertEqual(
            effective_mode(cfg, adapter="milky", is_private=False, group_id=GROUP), "repair"
        )
        cfg_private = RunsDeliveryConfig(mode="observe", repair_scopes=(f"milky:private:{PRIVATE}",))
        self.assertEqual(
            effective_mode(cfg_private, adapter="onebotv12", is_private=True, user_id=PRIVATE),
            "repair",
        )


class FenceIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Path(tmp.name) / "active_runs.db"
        self.tracker = RunTracker(self.db)
        self.addCleanup(self.tracker._conn.close)

    def _legacy_ready(self, run_id: str, *, adapter: str = "onebotv11",
                      group_id: str = GROUP, generation: int | None = None) -> str:
        scope = f"{adapter}:group:{group_id}"
        key = f"idem-{run_id}"
        self.tracker.record_submission(
            idempotency_key=key, request_body={}, target_fingerprint=f"{adapter}|http://x",
            scope_key=scope, session_key="s", mode="reactive",
            expect_structured=False, bot_self_id="999",
            delivery_generation=generation,
        )
        self.tracker.bind_submission_and_register(
            idempotency_key=key, run_id=run_id, scope_key=scope, adapter=adapter,
            is_private=False, user_id="u1", group_id=group_id, bot_self_id="999",
            session_key="s", mode="reactive", expect_structured=False,
        )
        self.tracker.record_expected_delivery(
            run_id=run_id, output="正文", expect_structured=False,
        )
        return scope

    def _fence_rows(self) -> list[tuple[str, int | None]]:
        return self.tracker._conn.execute(
            "SELECT scope_key, fence_generation FROM delivery_generation_fences ORDER BY scope_key"
        ).fetchall()

    def test_cross_transport_clear_cancels_all_qqtpc_ready_deliveries(self) -> None:
        self._legacy_ready("run-onebot", adapter="onebotv11")
        self._legacy_ready("run-icqq", adapter="icqq")

        result = self.tracker.fence_scope_generation(
            make_scope_key("milky", False, "u1", GROUP), fence_generation=4,
        )

        self.assertEqual(result["deliveries"], 2)
        self.assertEqual(self.tracker.get_delivery("run-onebot")["expected_state"], "cancelled")
        self.assertEqual(self.tracker.get_delivery("run-icqq")["expected_state"], "cancelled")
        rows = self._fence_rows()
        self.assertEqual([scope for scope, _ in rows], [f"qqtpc:group:{GROUP}"],
                         "新栅栏只落 canonical 域，不再生成 transport fence")

    def test_official_qq_domain_is_not_swept(self) -> None:
        self._legacy_ready("run-qq", adapter="qq")

        self.tracker.fence_scope_generation(
            make_scope_key("milky", False, "u1", GROUP), fence_generation=1,
        )

        self.assertEqual(self.tracker.get_delivery("run-qq")["expected_state"], "ready")

    def test_fence_generation_never_goes_backwards_or_zero(self) -> None:
        key = make_scope_key("milky", False, "u1", GROUP)
        self.tracker.fence_scope_generation(key, fence_generation=5)
        self.tracker.fence_scope_generation(key, fence_generation=0)

        rows = self._fence_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 5, "病态 0 不得覆盖已有代际水位")

    def test_legacy_delivery_reads_canonical_fence(self) -> None:
        self._legacy_ready("run-legacy", adapter="onebotv11", generation=5)
        self.assertTrue(self.tracker.is_delivery_current("run-legacy"),
                        "无栅栏时旧行照常可投")

        self.tracker.fence_scope_generation(
            make_scope_key("milky", False, "u1", GROUP), fence_generation=3,
        )
        self.assertTrue(self.tracker.is_delivery_current("run-legacy"),
                        "旧 scope 行必须读到同域 canonical 栅栏：5 >= 3")

        self.tracker.fence_scope_generation(
            make_scope_key("milky", False, "u1", GROUP), fence_generation=7,
        )
        self.assertFalse(self.tracker.is_delivery_current("run-legacy"),
                         "更高代际后旧交付不得再发送")

    def test_legacy_scope_fence_is_read_back_compat(self) -> None:
        """迁移前落下的 transport fence 仍要被旧行读到（不删除、不重写）。"""
        self._legacy_ready("run-old-fence", adapter="onebotv11", generation=2)
        with self.tracker._conn:
            self.tracker._conn.execute(
                "INSERT INTO delivery_generation_fences(scope_key, fence_seq, fence_generation, updated_at)"
                " VALUES(?,?,?,?)",
                (f"onebotv11:group:{GROUP}", 0, 9, 0.0),
            )

        self.assertFalse(self.tracker.is_delivery_current("run-old-fence"))
        self.assertEqual(
            [scope for scope, _ in self._fence_rows()],
            [f"onebotv11:group:{GROUP}"],
            "读取兼容不产生新 transport 栅栏",
        )

    def test_legacy_delivery_reads_other_legacy_transport_fence(self) -> None:
        """OneBot 交付也必须读到仅存在 Milky 名下的旧栅栏。"""
        self._legacy_ready("run-cross-legacy-fence", adapter="onebotv11", generation=2)
        with self.tracker._conn:
            self.tracker._conn.execute(
                "INSERT INTO delivery_generation_fences(scope_key, fence_seq, fence_generation, updated_at)"
                " VALUES(?,?,?,?)",
                (f"milky:group:{GROUP}", 0, 9, 0.0),
            )

        self.assertFalse(self.tracker.is_delivery_current("run-cross-legacy-fence"))

    def test_legacy_fence_generation_carries_into_canonical_fence(self) -> None:
        key = make_scope_key("milky", False, "u1", GROUP)
        with self.tracker._conn:
            self.tracker._conn.execute(
                "INSERT INTO delivery_generation_fences(scope_key, fence_seq, fence_generation, updated_at)"
                " VALUES(?,?,?,?)",
                (f"onebotv11:group:{GROUP}", 3, 9, 0.0),
            )
        self.tracker.fence_scope_generation(key, fence_generation=0)

        rows = dict(self._fence_rows())
        self.assertEqual(rows[f"onebotv11:group:{GROUP}"], 9, "旧栅栏行不改写")
        self.assertEqual(rows[f"qqtpc:group:{GROUP}"], 9,
                         "新 canonical 栅栏必须继承同域旧代际，不得归零")

    def test_get_by_scope_reads_legacy_transport_run(self) -> None:
        self._legacy_ready("run-steer", adapter="onebotv11")

        live = self.tracker.get_by_scope(make_scope_key("milky", False, "u1", GROUP))

        self.assertIsNotNone(live)
        self.assertEqual(live.run_id, "run-steer")
        self.assertEqual(live.adapter, "onebotv11", "行上的发送 adapter 保持到达事实")
        self.assertTrue(
            self.tracker.has_active_run_for_target(
                adapter="milky", is_private=False, group_id=GROUP
            )
        )


if __name__ == "__main__":
    unittest.main()
