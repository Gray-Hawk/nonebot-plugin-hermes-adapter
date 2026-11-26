"""QQTPC 权限/白名单身份：复合键按业务会话域匹配，官方 QQ 与其它平台隔离。

user_lists 的四类复合键（admin_users / master_users / allow_users / allow_groups）
语义仍是 ``adapter:id``，但 adapter 段是**身份域**而不是当前 transport：OneBot v11 /
v12 / Milky / icqq 下配置的同一账号在协议切换后照常生效。官方 QQ（``qq``）是另一个
域，不得被 QQTPC 的条目命中；未知平台保持逐字同名匹配。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter import utils as plugin_utils  # noqa: E402
from nonebot_plugin_hermes_adapter.core import approvals  # noqa: E402
from nonebot_plugin_hermes_adapter.core import user_lists  # noqa: E402
from nonebot_plugin_hermes_adapter.core.user_lists import UserLists  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import commands  # noqa: E402

GROUP = "462540549"
USER = "10001"
OTHER = "10002"


def _lists(**kwargs) -> UserLists:
    return UserLists(**kwargs)


class _Event:
    def __init__(self, user_id: str) -> None:
        self._user_id = user_id

    def get_user_id(self) -> str:
        return self._user_id


class _Target:
    def __init__(self, adapter: str, *, private: bool, target_id: str) -> None:
        self.adapter = adapter
        self.private = private
        self.id = target_id


class QqtpcAuthorityTests(unittest.TestCase):
    def test_admin_entry_from_another_qqtpc_transport_still_matches(self) -> None:
        lists = _lists(admin_users={"onebotv11:10001", "qqtpc:10002"})
        with patch.object(user_lists, "load_user_lists", lambda: lists):
            self.assertEqual(user_lists.authority_role("milky", USER), "master")
            self.assertEqual(user_lists.authority_role("OneBot V12", OTHER), "master")
            ids = approvals.admin_user_ids(set(lists.admin_users), "milky")
        self.assertEqual(ids, sorted([USER, OTHER]))

    def test_official_qq_is_not_matched_by_qqtpc_entries(self) -> None:
        lists = _lists(admin_users={"onebotv11:10001"})
        with patch.object(user_lists, "load_user_lists", lambda: lists):
            self.assertIsNone(user_lists.authority_role("qq", USER))

    def test_master_subset_is_matched_by_domain_too(self) -> None:
        lists = _lists(
            admin_users={"onebotv11:10001", "onebotv11:10002"},
            master_users={"milky:10002"},
        )
        with patch.object(user_lists, "load_user_lists", lambda: lists):
            self.assertEqual(user_lists.authority_role("milky", OTHER), "master")
            self.assertEqual(
                user_lists.authority_role("qqtpc", USER), "admin",
                "不在 master 子集里的管理员仍是 admin",
            )
            self.assertIsNone(user_lists.authority_role("qq", OTHER))

    def test_private_allow_fallback_uses_domain_admin_list(self) -> None:
        lists = _lists(admin_users={"onebotv11:10001"})
        with patch.object(user_lists, "load_user_lists", lambda: lists):
            self.assertTrue(user_lists.private_allowed("milky", USER))
            self.assertFalse(user_lists.private_allowed("qq", USER))

    def test_commands_is_admin_uses_domain(self) -> None:
        lists = _lists(admin_users={"onebotv11:10001"})
        with patch.object(commands, "load_user_lists", lambda: lists):
            self.assertTrue(commands._is_admin("milky", USER))
            self.assertFalse(commands._is_admin("qq", USER))

    def test_check_isolation_allow_users_cross_transport(self) -> None:
        lists = _lists(allow_users={"onebotv11:10001"})
        event = _Event(USER)
        target = _Target("milky", private=True, target_id=USER)
        with (
            patch.object(plugin_utils, "load_user_lists", lambda: lists),
            patch.object(
                plugin_utils, "load_chat_config",
                lambda: SimpleNamespace(private_trigger="allowlist"),
            ),
        ):
            self.assertTrue(plugin_utils.check_isolation(event, target))
            qq_target = _Target("qq", private=True, target_id=USER)
            self.assertFalse(plugin_utils.check_isolation(event, qq_target))

    def test_check_isolation_allow_groups_cross_transport(self) -> None:
        lists = _lists(allow_groups={"milky:462540549"})
        event = _Event(USER)
        target = _Target("onebotv11", private=False, target_id=GROUP)
        with (
            patch.object(plugin_utils, "load_user_lists", lambda: lists),
            patch.object(
                plugin_utils, "load_chat_config",
                lambda: SimpleNamespace(private_trigger="all"),
            ),
        ):
            self.assertTrue(plugin_utils.check_isolation(event, target))
            qq_target = _Target("qq", private=False, target_id=GROUP)
            self.assertFalse(plugin_utils.check_isolation(event, qq_target))

    def test_unknown_platform_requires_exact_tag(self) -> None:
        lists = _lists(admin_users={"astrbot:10001"})
        with patch.object(user_lists, "load_user_lists", lambda: lists):
            self.assertEqual(user_lists.authority_role("astrbot", USER), "master")
            self.assertIsNone(user_lists.authority_role("milky", USER))


if __name__ == "__main__":
    unittest.main()
