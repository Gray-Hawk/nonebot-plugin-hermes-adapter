"""session key 按真实后端命名、后台登记按前缀匹配的离线回归（无 ACP 主线）。

契约：
- 主线现有后端（completions / runs / astrbot）key 仍是 ``hermes-{internal_id}[-gN]``，
  已缓存 key（含上游轮换回来的 uuid）完全不动；
- 前缀来自 ``core.session`` 的接口登记表：换后端前缀时只改**本地派生** key 的名字，
  上游轮换回传的 id 永不改名；旧上游会话映射不迁移、不别名；
- 后台状态匹配认本会话可能出现的全部前缀（当前在前、升级前的 ``hermes-`` 在后）。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import session as session_mod  # noqa: E402
from nonebot_plugin_hermes_adapter.core.session import SessionManager  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

ADAPTER = "onebotv11"
USER = "10001"
# QQTPC 业务会话域：OneBot v11 / Milky / icqq 共用同一个 canonical internal_id。
INTERNAL_ID = f"qqtpc+private+{USER}"
LEGACY_INTERNAL_ID = f"{ADAPTER}+private+{USER}"
# 假后端前缀：只在本测试里登记，主线真实配置里不存在。
OTHER_PREFIX = "otherbackend"


class _Case(unittest.TestCase):
    def set_interface(self, value: str) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", value)
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", previous
        )

    def use_prefix(self, interface: str, prefix: str) -> None:
        self.set_interface(interface)
        patcher = patch.dict(
            session_mod._KEY_PREFIX_BY_INTERFACE, {interface: prefix}
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def make_store(self) -> SessionKeyStore:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = SessionKeyStore(Path(tmp.name) / "session_keys.db")
        self.addCleanup(store.close)
        return store

    def key(self, manager: SessionManager) -> str:
        return manager.get_session_key(ADAPTER, True, USER, None)


class HermesBackendKeyTests(_Case):
    """Hermes 后端：新 key 仍是 hermes-；已缓存 key（含上游轮换 id）不动。"""

    def test_new_session_key_keeps_hermes_prefix(self) -> None:
        self.set_interface("runs")
        manager = SessionManager()
        self.assertEqual(self.key(manager), f"hermes-{INTERNAL_ID}")
        self.assertEqual(manager.key_prefix, "hermes")

    def test_all_mainline_interfaces_resolve_to_hermes(self) -> None:
        for interface in ("completions", "runs", "astrbot"):
            with self.subTest(interface=interface):
                self.set_interface(interface)
                self.assertEqual(
                    session_mod.resolve_session_key_prefix(), session_mod.KEY_PREFIX_HERMES
                )

    def test_cached_key_survives_rebind_and_continues_upstream_history(self) -> None:
        self.set_interface("completions")
        store = self.make_store()
        store.put(INTERNAL_ID, f"hermes-{INTERNAL_ID}", 0, now=1.0)
        manager = SessionManager()
        manager.bind_store(store)
        self.assertEqual(self.key(manager), f"hermes-{INTERNAL_ID}")

        # 上游压缩轮换：采纳上游回来的新 id，重启后仍是它（历史连续）。
        self.assertTrue(manager.adopt_session_key(f"hermes-{INTERNAL_ID}", "upstream-uuid-1"))
        restarted = SessionManager()
        restarted.bind_store(store)
        self.assertEqual(self.key(restarted), "upstream-uuid-1")

    def test_clear_and_t1_generation_stay_hermes(self) -> None:
        self.set_interface("runs")
        manager = SessionManager()
        manager.get_session_key(ADAPTER, True, USER, None)
        manager.clear_session(ADAPTER, True, USER, None)
        self.assertEqual(self.key(manager), f"hermes-{INTERNAL_ID}-g1")
        manager.clear_internal(INTERNAL_ID)
        self.assertEqual(self.key(manager), f"hermes-{INTERNAL_ID}-g2")


class DerivedKeyRenameTests(_Case):
    """换后端前缀：只改本地派生 key 名字，上游 id 不动，旧映射不迁移。"""

    def test_new_session_clear_and_t1_use_registered_prefix(self) -> None:
        self.use_prefix("otherbackend", OTHER_PREFIX)
        manager = SessionManager()
        self.assertEqual(self.key(manager), f"{OTHER_PREFIX}-{INTERNAL_ID}")
        manager.clear_session(ADAPTER, True, USER, None)
        self.assertEqual(self.key(manager), f"{OTHER_PREFIX}-{INTERNAL_ID}-g1")
        manager.clear_internal(INTERNAL_ID)
        self.assertEqual(self.key(manager), f"{OTHER_PREFIX}-{INTERNAL_ID}-g2")

    def test_legacy_derived_key_renamed_and_persisted(self) -> None:
        store = self.make_store()
        store.put(INTERNAL_ID, f"hermes-{INTERNAL_ID}-g3", 3, now=1.0)

        self.use_prefix("otherbackend", OTHER_PREFIX)
        manager = SessionManager()
        manager.bind_store(store)

        self.assertEqual(self.key(manager), f"{OTHER_PREFIX}-{INTERNAL_ID}-g3")
        # 新名字落库，重启不退回 hermes-。
        restarted = SessionManager()
        restarted.bind_store(store)
        self.assertEqual(self.key(restarted), f"{OTHER_PREFIX}-{INTERNAL_ID}-g3")

    def test_upstream_rotated_id_never_renamed(self) -> None:
        store = self.make_store()
        store.put(INTERNAL_ID, "upstream-uuid-9", 0, now=1.0)

        self.use_prefix("otherbackend", OTHER_PREFIX)
        manager = SessionManager()
        manager.bind_store(store)

        # 上游 session id 不可本地强改，换前缀也必须原样续用。
        self.assertEqual(self.key(manager), "upstream-uuid-9")

    def test_is_derived_key_only_matches_own_prefixed_forms(self) -> None:
        is_derived = SessionManager._is_derived_key
        self.assertTrue(is_derived(f"hermes-{INTERNAL_ID}", INTERNAL_ID, "hermes"))
        self.assertTrue(is_derived(f"hermes-{INTERNAL_ID}-g12", INTERNAL_ID, "hermes"))
        self.assertFalse(is_derived(f"hermes-{INTERNAL_ID}-gx", INTERNAL_ID, "hermes"))
        self.assertFalse(is_derived("upstream-uuid-1", INTERNAL_ID, "hermes"))
        self.assertFalse(
            is_derived(f"other-{INTERNAL_ID}", INTERNAL_ID, "hermes"),
            "别的前缀不是本次要改的派生形态",
        )

    def test_prefix_switch_back_renames_local_keys_again(self) -> None:
        manager = SessionManager()
        self.key(manager)
        self.use_prefix("otherbackend", OTHER_PREFIX)
        self.assertEqual(self.key(manager), f"{OTHER_PREFIX}-{INTERNAL_ID}")
        # 配置回落到 Hermes 形态（登记表拿掉）后，本地派生 key 再改回 hermes-。
        session_mod._KEY_PREFIX_BY_INTERFACE.pop("otherbackend", None)
        self.assertEqual(self.key(manager), f"hermes-{INTERNAL_ID}")

    def test_prefix_candidates_current_first_then_legacy(self) -> None:
        manager = SessionManager()
        self.set_interface("runs")
        self.assertEqual(manager.session_key_prefixes(), ("hermes",))
        self.use_prefix("otherbackend", OTHER_PREFIX)
        self.assertEqual(
            manager.session_key_prefixes(), (OTHER_PREFIX, "hermes"),
            "换前缀后升级前登记的 hermes-* 仍要认",
        )


class BackgroundPrefixMatchTests(_Case):
    """后台登记：前缀列表按后端走，匹配同时认当前与升级前前缀。"""

    def test_route_resolution_accepts_configured_prefixes(self) -> None:
        from nonebot_plugin_hermes_adapter.core.background_report_runtime import (
            resolve_background_route,
        )

        for prefixes in (("hermes",), ("hermes", OTHER_PREFIX)):
            with self.subTest(prefixes=prefixes):
                route = resolve_background_route(
                    f"{prefixes[-1]}-{ADAPTER}+group+462540549-g2", (), prefixes=prefixes
                )
                self.assertEqual(
                    route,
                    {
                        "adapter_name": ADAPTER,
                        "is_private": False,
                        "user_id": None,
                        "group_id": "462540549",
                    },
                )
        # 默认仍是旧的单前缀行为。
        self.assertIsNone(
            resolve_background_route(f"{OTHER_PREFIX}-{INTERNAL_ID}", ())
        )
        self.assertIsNone(resolve_background_route(f"agent-{INTERNAL_ID}", ()))

    def test_identity_covers_all_transports_within_backend_prefix(self) -> None:
        from nonebot_plugin_hermes_adapter.core.background_report_runtime import (
            conversation_identity,
            identity_matches,
        )
        from nonebot_plugin_hermes_adapter.core.session import session_manager

        # 归属按会话身份（平台族 + scope + target），不枚举 transport：
        # canonical 域、全部已登记 transport、以及将来未登记的新驱动名都命中。
        identity = conversation_identity(ADAPTER, True, USER, None)
        self.set_interface("runs")
        prefixes = session_manager.session_key_prefixes()
        for name in ("qqtpc", ADAPTER, "milky", "icqq"):
            with self.subTest(name=name):
                self.assertTrue(
                    identity_matches(f"hermes-{name}+private+{USER}-g4", identity, prefixes=prefixes)
                )
        self.assertFalse(
            identity_matches(f"hermes-somefuture+private+{USER}-g4", identity, prefixes=prefixes),
            "未登记 transport 不是 QQTPC 身份",
        )
        self.assertFalse(
            identity_matches(f"{OTHER_PREFIX}-{INTERNAL_ID}", identity, prefixes=prefixes),
            "其它后端前缀不在本会话前缀集合内",
        )
        self.assertFalse(
            identity_matches(f"hermes-qq+private+{USER}", identity, prefixes=prefixes),
            "官方 QQ 属另一平台族",
        )
        self.assertFalse(
            identity_matches(f"hermes-{ADAPTER}+private+99999", identity, prefixes=prefixes),
            "兄弟 target 不得命中",
        )

        # 换后端前缀：当前前缀与升级前 hermes- 登记都要认；agent- 永远不认。
        self.use_prefix("otherbackend", OTHER_PREFIX)
        prefixes = session_manager.session_key_prefixes()
        self.assertTrue(
            identity_matches(f"{OTHER_PREFIX}-{INTERNAL_ID}-g4", identity, prefixes=prefixes)
        )
        self.assertTrue(
            identity_matches(f"hermes-{LEGACY_INTERNAL_ID}-g2", identity, prefixes=prefixes),
            "升级前遗留登记",
        )
        self.assertFalse(identity_matches(f"agent-{INTERNAL_ID}", identity, prefixes=prefixes))

    def test_collect_running_dedupes_multiple_prefixes(self) -> None:
        from nonebot_plugin_hermes_adapter.mcp.tools.background_status import _collect_running

        class _Source:
            name = "kanban"

            def __init__(self) -> None:
                self.seen: list[str] = []

            def running(self, *, session_prefix: str, limit: int):
                self.seen.append(session_prefix)
                return [
                    {"task_id": "t1", "source": "kanban"},
                    {"task_id": "t2", "source": "kanban"},
                ]

        source = _Source()
        items, ok_sources, errors = _collect_running(
            [source], prefixes=[f"{OTHER_PREFIX}-{INTERNAL_ID}", f"hermes-{INTERNAL_ID}"],
            limit=20,
        )
        self.assertEqual(source.seen, [f"{OTHER_PREFIX}-{INTERNAL_ID}", f"hermes-{INTERNAL_ID}"])
        self.assertEqual([item["task_id"] for item in items], ["t1", "t2"], "同一任务不重复计")
        self.assertEqual(ok_sources, ["kanban"], "来源名去重")
        self.assertEqual(errors, [])

    def test_report_filter_accepts_current_and_legacy_origin_ids(self) -> None:
        from nonebot_plugin_hermes_adapter.core.routing import CallerScope
        from nonebot_plugin_hermes_adapter.mcp.tools.background_status import (
            GetBackgroundTasksInput,
            get_background_tasks_impl,
        )

        class _Outbox:
            def active_events(self):
                return [
                    {
                        "event_id": "new",
                        "source": "subagent",
                        "task_state": "completed",
                        "report_state": "pending",
                        "origin_session_id": f"{OTHER_PREFIX}-{INTERNAL_ID}",
                    },
                    {
                        "event_id": "legacy",
                        "source": "subagent",
                        "task_state": "completed",
                        "report_state": "pending",
                        "origin_session_id": f"hermes-{INTERNAL_ID}-g1",
                    },
                    {
                        "event_id": "other",
                        "source": "subagent",
                        "task_state": "completed",
                        "report_state": "pending",
                        "origin_session_id": "hermes-onebotv11+private+99999",
                    },
                ]

        self.use_prefix("otherbackend", OTHER_PREFIX)
        result = get_background_tasks_impl(
            GetBackgroundTasksInput(adapter=ADAPTER, user_id=USER),
            outbox=_Outbox(),
            scope=CallerScope.dev(),
            running_sources=[],
        )
        self.assertTrue(result.ok)
        self.assertEqual(sorted(item.event_id for item in result.reports), ["legacy", "new"])

    def test_kanban_routable_uses_configured_prefixes(self) -> None:
        from nonebot_plugin_hermes_adapter.core.kanban_report_source import _routable

        self.set_interface("runs")
        # 语法闸：canonical qqtpc 键与归一前 transport 键都算可登记，真实 adapter
        # 由消费器在唤醒时按当前连接解析；等待不等于丢弃。
        self.assertTrue(_routable(f"hermes-{INTERNAL_ID}"))
        self.assertTrue(_routable(f"hermes-{LEGACY_INTERNAL_ID}-g2"))
        self.assertFalse(_routable(f"{OTHER_PREFIX}-{INTERNAL_ID}"))
        self.assertFalse(_routable(""))
        self.assertFalse(_routable("hermes-not-a-session"))

        self.use_prefix("otherbackend", OTHER_PREFIX)
        self.assertTrue(_routable(f"{OTHER_PREFIX}-{INTERNAL_ID}"))
        self.assertTrue(_routable(f"hermes-{INTERNAL_ID}"), "升级前登记的看板卡")


if __name__ == "__main__":
    unittest.main()
