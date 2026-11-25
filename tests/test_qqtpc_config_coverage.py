"""QQ-09：QQTPC 归一后的配置覆盖检查——只告警，不改权限/交付语义。

归一后同一会话会在 OneBot / Milky 间切换，而 user_lists 的复合键
（``adapter:id``）与 runs_delivery 的 repair_scopes（``adapter:scope:id``）仍按
真实 adapter 分裂。某个面只覆盖 QQTPC transport 的真子集时，切协议后会静默
降级（管理命令 skip、私聊唤醒被拒、自动补发落回 observe）——这里只把缺口
打出来，判定逻辑一行不动。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import qqtpc_config_coverage as cov  # noqa: E402


class CoverageGapTests(unittest.TestCase):
    def test_partial_admin_coverage_reports_missing_transports(self) -> None:
        gaps = cov.collect_coverage_gaps(
            admin_users={"onebotv11:10001"},
            repair_scopes=(),
        )

        self.assertEqual(len(gaps), 1)
        self.assertIn("admin_users", gaps[0])
        self.assertIn("milky", gaps[0])
        self.assertIn("onebotv11", gaps[0])

    def test_full_coverage_has_no_gap(self) -> None:
        gaps = cov.collect_coverage_gaps(
            admin_users={
                "onebotv11:10001", "onebotv12:10001", "milky:10001", "icqq:10001",
            },
            repair_scopes=(
                "onebotv11:group:1", "onebotv12:group:1", "milky:group:1", "icqq:group:1",
            ),
        )

        self.assertEqual(gaps, [])

    def test_official_qq_only_surface_is_not_a_qqtpc_gap(self) -> None:
        gaps = cov.collect_coverage_gaps(
            admin_users={"qq:OPENID_A"},
            repair_scopes=("qq:group:GROUP_OPENID",),
        )

        self.assertEqual(gaps, [], "官方 QQ 不是 QQTPC transport，不该报缺口")

    def test_partial_repair_scopes_report_gap(self) -> None:
        gaps = cov.collect_coverage_gaps(
            admin_users=set(),
            repair_scopes=("onebotv11:group:462540549",),
        )

        self.assertEqual(len(gaps), 1)
        self.assertIn("repair_scopes", gaps[0])
        self.assertIn("milky", gaps[0])

    def test_empty_surfaces_are_not_gaps(self) -> None:
        self.assertEqual(cov.collect_coverage_gaps(admin_users=set()), [])

    def test_allow_users_and_allow_groups_are_checked_too(self) -> None:
        gaps = cov.collect_coverage_gaps(
            allow_users={"milky:10001"},
            allow_groups={"onebotv11:462540549"},
        )

        self.assertEqual(len(gaps), 2)
        self.assertTrue(any("allow_users" in gap for gap in gaps))
        self.assertTrue(any("allow_groups" in gap for gap in gaps))


class WarnWiringTests(unittest.TestCase):
    def test_warn_config_coverage_logs_each_gap(self) -> None:
        fake_lists = type(
            "Lists",
            (),
            {
                "admin_users": {"onebotv11:10001"},
                "master_users": None,
                "allow_users": set(),
                "allow_groups": set(),
            },
        )()
        fake_delivery = type("Cfg", (), {"repair_scopes": ()})()

        with (
            patch.object(cov, "load_user_lists", lambda: fake_lists),
            patch.object(cov, "load_runs_delivery_config", lambda: fake_delivery),
            patch.object(cov, "logger") as logging,
        ):
            gaps = cov.warn_config_coverage()

        self.assertEqual(len(gaps), 1)
        logging.warning.assert_called_once()
        message = logging.warning.call_args.args[0]
        self.assertIn("milky", message)
        self.assertIn("不改判定", message)


if __name__ == "__main__":
    unittest.main()
