"""session key 按真实后端命名 + ACP 旧 key 一次性迁移的离线回归。

契约（主人口径）：
- Hermes 后端（completions / runs / astrbot）沿用 ``hermes-{internal_id}[-gN]``，
  现有 key 与上游历史完全不动；
- Antigravity ACP 后端用 ``antigravity-{internal_id}[-gN]``，新建 / clear / T1 换代
  全部如此；
- 旧 ACP 会话的 ``hermes-*`` key 只改本地名字到 ``antigravity-*``，不迁移 / 不别名
  ``acp_sessions`` 映射：换名后直接新建 ACP 会话，绝不错误续读旧映射；
- 后台状态匹配同时认两种真实后端前缀（升级前登记的事件仍是 ``hermes-*``）。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_agent_adapter.core.session import SessionManager  # noqa: E402
from nonebot_plugin_agent_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

ADAPTER = "onebotv11"
USER = "10001"
INTERNAL_ID = f"{ADAPTER}+private+{USER}"


class _BackendCase(unittest.TestCase):
    """按后端切 ``plugin_config.hermes_api_interface``（既有夹具同一手法）。"""

    def set_interface(self, value: str) -> None:
        from nonebot_plugin_agent_adapter.config import plugin_config

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", value)
        self.addCleanup(
            object.__setattr__, plugin_config, "hermes_api_interface", previous
        )

    def make_store(self) -> SessionKeyStore:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = SessionKeyStore(Path(tmp.name) / "session_keys.db")
        self.addCleanup(store.close)
        return store

    def key(self, manager: SessionManager) -> str:
        return manager.get_session_key(ADAPTER, True, USER, None)


class HermesBackendKeyTests(_BackendCase):
    """Hermes 后端：新 key 仍是 hermes-；已缓存 key（含上游轮换 id）不动。"""

    def test_new_session_key_keeps_hermes_prefix(self) -> None:
        self.set_interface("runs")
        manager = SessionManager()
        self.assertEqual(self.key(manager), f"hermes-{INTERNAL_ID}")

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
        cleared = self.key(manager)
        self.assertEqual(cleared, f"hermes-{INTERNAL_ID}-g1")
        manager.clear_internal(INTERNAL_ID)
        self.assertEqual(self.key(manager), f"hermes-{INTERNAL_ID}-g2")


class AntigravityBackendKeyTests(_BackendCase):
    """ACP 后端：新建 / clear / T1 都用 antigravity-。"""

    def test_new_session_clear_and_t1_use_antigravity_prefix(self) -> None:
        self.set_interface("antigravity")
        manager = SessionManager()
        self.assertEqual(self.key(manager), f"antigravity-{INTERNAL_ID}")

        manager.clear_session(ADAPTER, True, USER, None)
        self.assertEqual(self.key(manager), f"antigravity-{INTERNAL_ID}-g1")

        manager.clear_internal(INTERNAL_ID)
        self.assertEqual(self.key(manager), f"antigravity-{INTERNAL_ID}-g2")

    def test_legacy_hermes_key_switches_to_antigravity_without_reading_old_mapping(self) -> None:
        store = self.make_store()
        old_key = f"hermes-{INTERNAL_ID}"
        store.put(INTERNAL_ID, old_key, 0, now=1.0)
        store.put_acp_session(old_key, "acp-sess-1", "hash-1", "model-a", now=1.0)

        self.set_interface("antigravity")
        manager = SessionManager()
        manager.bind_store(store)

        new_key = self.key(manager)
        self.assertEqual(new_key, f"antigravity-{INTERNAL_ID}")
        # 不迁移、不别名：新 key 读不到旧 ACP 映射，下一轮按真实行为新建会话。
        self.assertIsNone(store.load_acp_session(new_key))
        # 旧行可以留作无引用历史（也可由既有清理路径淘汰），但绝不与新 key 串起来。
        self.assertEqual(store.load_acp_session(old_key)[0], "acp-sess-1")

        # 本地 key 改名后重启保持一致（不会退回 hermes-）。
        restarted = SessionManager()
        restarted.bind_store(store)
        self.assertEqual(self.key(restarted), new_key)
        self.assertIsNone(store.load_acp_session(new_key))

    def test_rename_keeps_generation_suffix(self) -> None:
        store = self.make_store()
        old_key = f"hermes-{INTERNAL_ID}-g3"
        store.put(INTERNAL_ID, old_key, 3, now=1.0)
        store.put_acp_session(old_key, "acp-sess-g3", "h", "", now=1.0)

        self.set_interface("antigravity")
        manager = SessionManager()
        manager.bind_store(store)

        self.assertEqual(self.key(manager), f"antigravity-{INTERNAL_ID}-g3")
        self.assertIsNone(store.load_acp_session(f"antigravity-{INTERNAL_ID}-g3"))


class BackgroundPrefixCompatTests(_BackendCase):
    """后台状态：两种真实后端前缀都要认，升级前遗留的 hermes-* 不能丢。"""

    def test_route_resolution_accepts_both_backends_and_rejects_other_names(self) -> None:
        from nonebot_plugin_agent_adapter.core.background_report_runtime import (
            resolve_background_route,
        )

        for prefix in ("hermes", "antigravity"):
            with self.subTest(prefix=prefix):
                route = resolve_background_route(f"{prefix}-{ADAPTER}+group+462540549-g2", ())
                self.assertIsNotNone(route)
                self.assertEqual(
                    route,
                    {
                        "adapter_name": ADAPTER,
                        "is_private": False,
                        "user_id": None,
                        "group_id": "462540549",
                    },
                )
                private = resolve_background_route(f"{prefix}-{ADAPTER}+private+10001", ())
                self.assertEqual(private["user_id"], "10001")

        self.assertIsNone(resolve_background_route(f"agent-{INTERNAL_ID}", ()))

    def test_prefix_candidates_follow_backend(self) -> None:
        from nonebot_plugin_agent_adapter.mcp.tools.background_status import (
            _belongs,
            _session_prefixes,
        )

        self.set_interface("antigravity")
        prefixes = _session_prefixes(ADAPTER, True, USER)
        self.assertEqual(
            prefixes, [f"antigravity-{INTERNAL_ID}", f"hermes-{INTERNAL_ID}"]
        )
        self.assertTrue(_belongs(f"antigravity-{INTERNAL_ID}-g4", prefixes))
        self.assertTrue(_belongs(f"hermes-{INTERNAL_ID}-g2", prefixes), "升级前遗留登记")
        self.assertFalse(_belongs(f"agent-{INTERNAL_ID}", prefixes))

        self.set_interface("runs")
        hermes_only = _session_prefixes(ADAPTER, True, USER)
        self.assertEqual(hermes_only, [f"hermes-{INTERNAL_ID}"])
        self.assertTrue(_belongs(f"hermes-{INTERNAL_ID}", hermes_only))
        self.assertFalse(_belongs(f"antigravity-{INTERNAL_ID}", hermes_only))

    def test_report_filter_accepts_legacy_and_new_origin_session_ids(self) -> None:
        from nonebot_plugin_agent_adapter.core.routing import CallerScope
        from nonebot_plugin_agent_adapter.mcp.tools.background_status import (
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
                        "origin_session_id": f"antigravity-{INTERNAL_ID}",
                        "summary": "新 key",
                    },
                    {
                        "event_id": "legacy",
                        "source": "subagent",
                        "task_state": "completed",
                        "report_state": "pending",
                        "origin_session_id": f"hermes-{INTERNAL_ID}-g1",
                        "summary": "升级前",
                    },
                    {
                        "event_id": "other",
                        "source": "subagent",
                        "task_state": "completed",
                        "report_state": "pending",
                        "origin_session_id": "hermes-onebotv11+private+99999",
                        "summary": "别人的",
                    },
                ]

        self.set_interface("antigravity")
        result = get_background_tasks_impl(
            GetBackgroundTasksInput(adapter=ADAPTER, user_id=USER),
            outbox=_Outbox(),
            scope=CallerScope.dev(),
            running_sources=[],
        )
        self.assertTrue(result.ok)
        self.assertEqual(sorted(item.event_id for item in result.reports), ["legacy", "new"])


if __name__ == "__main__":
    unittest.main()
