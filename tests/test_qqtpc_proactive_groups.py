"""主动发言群范围：``proactive_groups`` 的复合键按业务会话域匹配。

模板一直写 ``adapter:group_id``，但巡查执行侧曾拿裸 ``scope_id`` 比对，导致
按协议写的范围永远不命中。adapter 段按 session_domain 比较：新旧写法都生效，
官方 QQ 条目不会命中 QQTPC 群；无冒号的裸群号保留为旧配置兼容。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.chat_config import ChatConfig  # noqa: E402

GROUP = "462540549"
OTHER = "253384307"


class ProactiveGroupsIdentityTests(unittest.TestCase):
    def test_empty_set_means_all_groups(self) -> None:
        cfg = ChatConfig(proactive_groups=set())
        self.assertTrue(cfg.proactive_group_enabled("milky", GROUP))

    def test_qqtpc_key_matches_any_transport(self) -> None:
        cfg = ChatConfig(proactive_groups={f"qqtpc:{GROUP}"})
        self.assertTrue(cfg.proactive_group_enabled("onebotv11", GROUP))
        self.assertTrue(cfg.proactive_group_enabled("milky", GROUP))
        self.assertFalse(cfg.proactive_group_enabled("milky", OTHER))

    def test_legacy_transport_key_still_matches_after_switch(self) -> None:
        cfg = ChatConfig(proactive_groups={f"onebotv11:{GROUP}"})
        self.assertTrue(cfg.proactive_group_enabled("milky", GROUP))
        self.assertFalse(cfg.proactive_group_enabled("qq", GROUP))

    def test_official_qq_group_is_isolated(self) -> None:
        cfg = ChatConfig(proactive_groups={f"qqtpc:{GROUP}"})
        self.assertFalse(cfg.proactive_group_enabled("qq", GROUP))
        official = ChatConfig(proactive_groups={f"qq:{GROUP}"})
        self.assertTrue(official.proactive_group_enabled("qq", GROUP))
        self.assertFalse(official.proactive_group_enabled("milky", GROUP))

    def test_bare_group_id_remains_supported(self) -> None:
        cfg = ChatConfig(proactive_groups={GROUP})
        self.assertTrue(cfg.proactive_group_enabled("milky", GROUP))
        self.assertFalse(cfg.proactive_group_enabled("milky", OTHER))


if __name__ == "__main__":
    unittest.main()
