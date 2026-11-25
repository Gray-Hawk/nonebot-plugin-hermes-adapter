"""QQTPC 会话域兼容：canonical internal_id ＋ 运行时真实 adapter 解析。

QQTPC＝QQ Third-Party Client。OneBot v11/v12、Milky、icqq 是用户侧同一整体在不同
驱动下的形态：用户以后会来回切协议，所以

- ``internal_id`` 用**业务会话域** ``qqtpc+…``：同一 QQ 群/人跨协议就是同一会话，
  session_key / generation 原样沿用，**不重命名现有 Hermes session id**，也不复制／
  合并上游转录；
- 真实 adapter 是**运行时**事实（当前 Bot 连接／事件／BotRegistry 路由），**不落库**：
  切换协议零人工改库、零迁移；
- 重启后真实 adapter 尚未就绪时，需要它的后台动作（``known_sessions`` / T1 清理 /
  交付代际栅栏）按既有等待／跳过语义处理——绝不把业务域 ``qqtpc`` 当 adapter 去查
  路由或调平台 API。

QQ 官方（adapter=``qq``）及其它平台不归一，行为与改名前逐字一致。
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

from nonebot_plugin_hermes_adapter.core import session as session_mod  # noqa: E402
from nonebot_plugin_hermes_adapter.core.bot_registry import BotRegistry  # noqa: E402
from nonebot_plugin_hermes_adapter.core.session import (  # noqa: E402
    SESSION_DOMAIN_QQTPC,
    SESSION_DOMAIN_TRANSPORTS,
    SessionManager,
    is_session_domain,
    session_domain,
)
from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionDomainMigrationConflict,
    SessionKeyStore,
)

GROUP = "462540549"
USER = "10001"
PRIVATE_ID = f"{SESSION_DOMAIN_QQTPC}+private+{USER}"
GROUP_ID = f"{SESSION_DOMAIN_QQTPC}+group+{GROUP}"
TRANSITIONS = SESSION_DOMAIN_TRANSPORTS  # {"onebotv11": "qqtpc", "milky": "qqtpc", ...}


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "session_keys.db"
        self.store = SessionKeyStore(self.db_path)
        self.addCleanup(self.store.close)
        self.registry = BotRegistry()
        self._registry_patch = patch.object(
            session_mod, "_runtime_bot_registry", lambda: self.registry
        )
        self._registry_patch.start()
        self.addCleanup(self._registry_patch.stop)

    def route(self, adapter: str, *, group_id: str = GROUP, synthetic: bool = False) -> None:
        """登记一条真实路由（等价于真实事件／bot 连接恢复写进 BotRegistry）。"""
        self.registry.upsert(
            adapter=adapter,
            scope="group",
            scope_id=group_id,
            bot_self_id="10000",
            target=object(),
            ts=0,
            synthetic=synthetic,
        )

    def new_manager(self) -> SessionManager:
        manager = SessionManager()
        manager.bind_store(self.store)
        return manager

    def _dump(self) -> tuple:
        """整库快照（两张业务表的全部列），用于「不改库」比对。"""
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            keys = sorted(
                tuple(row)
                for row in conn.execute(
                    "SELECT internal_id, session_key, generation, updated_at FROM session_keys"
                )
            )
            paused = sorted(
                tuple(row) for row in conn.execute("SELECT internal_id FROM idle_paused")
            )
        finally:
            conn.close()
        return (keys, paused)

    def integrity_ok(self) -> bool:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            conn.close()

    def columns(self, table: str) -> list[str]:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
        finally:
            conn.close()


class CanonicalInternalIdTests(_Base):
    """双协议生成同一 internal_id；QQ 官方与其它平台不变。"""

    def test_all_qqtpc_transports_share_one_session(self) -> None:
        manager = SessionManager()
        expected = f"hermes-{GROUP_ID}"
        for adapter in TRANSITIONS:
            with self.subTest(adapter=adapter):
                self.assertEqual(manager.get_session_key(adapter, False, "u1", GROUP), expected)
        self.assertEqual(
            manager.get_session_key("onebotv11", True, USER, None),
            manager.get_session_key("milky", True, USER, None),
        )
        self.assertEqual(
            manager.get_session_key("milky", True, USER, None), f"hermes-{PRIVATE_ID}"
        )

    def test_official_qq_and_other_platforms_keep_adapter_in_id(self) -> None:
        manager = SessionManager()
        self.assertEqual(
            manager.get_session_key("qq", True, "openid-1", None), "hermes-qq+private+openid-1"
        )
        self.assertEqual(
            manager.get_session_key("qq", False, "u1", GROUP), f"hermes-qq+group+{GROUP}"
        )
        self.assertEqual(
            manager.get_session_key("telegram", False, "u1", GROUP),
            f"hermes-telegram+group+{GROUP}",
        )

    def test_session_domain_helpers(self) -> None:
        for adapter, domain in TRANSITIONS.items():
            with self.subTest(adapter=adapter):
                self.assertEqual(session_domain(adapter), domain)
                self.assertTrue(is_session_domain(domain))
        self.assertEqual(session_domain("qq"), "qq")
        self.assertFalse(is_session_domain("qq"))
        self.assertEqual(session_domain("OneBot V11"), SESSION_DOMAIN_QQTPC)
        self.assertFalse(is_session_domain("onebotv11"), "真实 adapter 不是会话域")


class CrossTransportSwitchTests(_Base):
    """同一 QQTPC 映射 OneBot→Milky→OneBot：key/generation 不变且不改库。"""

    def test_round_trip_switch_keeps_key_generation_and_db(self) -> None:
        manager = self.new_manager()
        first = manager.get_session_key("onebotv11", False, "u1", GROUP)
        manager.clear_session("onebotv11", False, "u1", GROUP)
        key_gen1 = manager.get_session_key("onebotv11", False, "u1", GROUP)
        self.assertEqual(key_gen1, f"{first}-g1")
        before = self._dump()

        for adapter in ("milky", "onebotv11", "icqq", "milky"):
            with self.subTest(adapter=adapter):
                self.assertEqual(manager.get_session_key(adapter, False, "u1", GROUP), key_gen1)
                self.assertEqual(
                    manager.get_delivery_generation(adapter, False, "u1", GROUP), 1
                )
                self.assertEqual(self._dump(), before, "换协议不得改库（无迁移、无新写入）")
        self.assertEqual(
            self.columns("session_keys"),
            ["internal_id", "session_key", "generation", "updated_at"],
            "不新增 transport 列：真实 adapter 是运行时事实，不落库",
        )

        # 重启后仍续用同一 key/generation。
        self.route("milky")
        restarted = self.new_manager()
        self.assertEqual(restarted.get_session_key("milky", False, "u1", GROUP), key_gen1)
        self.assertEqual(restarted.get_delivery_generation("milky", False, "u1", GROUP), 1)

    def test_switch_does_not_touch_other_platforms(self) -> None:
        manager = self.new_manager()
        qq_key = manager.get_session_key("qq", False, "u1", GROUP)
        manager.get_session_key("milky", False, "u1", GROUP)
        manager.get_session_key("onebotv11", False, "u1", GROUP)
        self.assertEqual(manager.get_session_key("qq", False, "u1", GROUP), qq_key)
        self.assertEqual(qq_key, f"hermes-qq+group+{GROUP}")


class RestartRuntimeResolutionTests(_Base):
    """重启后真实 adapter 由当前连接/路由恢复；就绪前按等待/跳过处理。"""

    def test_restart_with_live_route_restores_real_adapter(self) -> None:
        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 2, now=1.0)
        self.route("milky")
        manager = self.new_manager()

        rows = manager.known_sessions()
        self.assertEqual(len(rows), 1, "iter_sessions 必须看得见重启前的会话")
        self.assertEqual(rows[0][1], "milky", "adapter 是当前真实连接，不是 qqtpc")
        self.assertEqual(rows[0][5], f"hermes-{GROUP_ID}", "session key 原样续用")

        expired = manager.idle_expired(0, now=1e12)
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0][1][0], "milky")

    def test_restart_without_route_waits_then_resolves_on_real_event(self) -> None:
        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 0, now=1.0)
        with patch.object(session_mod, "_live_transports", lambda domain: []):
            manager = self.new_manager()
            # 路由/连接都没就绪：不猜协议，条目按等待语义缺席。
            self.assertEqual(manager.known_sessions(), [])
            self.assertEqual(manager.idle_expired(0, now=1e12), [])
            # 真实事件到达：登记真实 adapter，此后即可参与 T1/投递轮询。
            self.assertEqual(manager.get_session_key("milky", False, "u1", GROUP), f"hermes-{GROUP_ID}")
            rows = manager.known_sessions()
        self.assertEqual([row[1] for row in rows], ["milky"])
        self.assertEqual([c[0] for _, c in manager.idle_expired(0, now=1e12)], ["milky"])

    def test_live_bot_of_single_driver_is_used_when_no_route(self) -> None:
        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 0, now=1.0)
        with patch.object(session_mod, "_live_transports", lambda domain: ["onebotv11"]):
            manager = self.new_manager()
            rows = manager.known_sessions()
        self.assertEqual([row[1] for row in rows], ["onebotv11"])

    def test_two_drivers_without_route_is_ambiguous_and_skipped(self) -> None:
        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 0, now=1.0)
        with patch.object(session_mod, "_live_transports", lambda domain: ["onebotv11", "milky"]):
            manager = self.new_manager()
            self.assertEqual(manager.known_sessions(), [], "两个驱动同时在线不猜")
            self.assertEqual(manager.idle_expired(0, now=1e12), [])

    def test_two_routes_for_same_scope_is_ambiguous_and_skipped(self) -> None:
        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 0, now=1.0)
        self.route("onebotv11")
        self.route("milky")
        manager = self.new_manager()
        self.assertEqual(manager.known_sessions(), [])

    def test_real_route_wins_over_synthetic(self) -> None:
        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 0, now=1.0)
        self.route("onebotv11", synthetic=True)
        self.route("milky")
        manager = self.new_manager()
        self.assertEqual([row[1] for row in manager.known_sessions()], ["milky"])

    def test_restart_waits_for_route_recovery_then_uses_real_adapter(self) -> None:
        """重启后路由未就绪先跳过；bot 连接恢复路由后同一进程内立即可用，且不改库。"""
        key = f"hermes-{GROUP_ID}"
        self.store.put(GROUP_ID, key, 0, now=1.0)
        with patch.object(session_mod, "_live_transports", lambda domain: []):
            manager = self.new_manager()
            self.assertEqual(manager.known_sessions(), [])
            self.assertEqual(manager.idle_expired(0, now=1e12), [])
            # 等价于 bot 连接钩子 restore_bot_routes() 写入的 synthetic 路由。
            self.route("onebotv11", synthetic=True)
            rows = manager.known_sessions()
            expired = manager.idle_expired(0, now=1e12)
        self.assertEqual([row[1] for row in rows], ["onebotv11"])
        self.assertEqual([comp[0] for _, comp in expired], ["onebotv11"])
        self.assertEqual(self.store.load_all()[GROUP_ID], (key, 0), "路由恢复不写会话库")

    def test_push_watermark_is_session_scoped_not_transport_scoped(self) -> None:
        manager = SessionManager()
        key = manager.get_session_key("onebotv11", False, "u1", GROUP)
        manager.advance_push_watermark("onebotv11", "u1", GROUP, 42, key)
        self.assertEqual(
            manager.get_push_watermark("milky", "u1", GROUP), 42,
            "增量推送水位跟 canonical 会话走；真实 adapter 由当前事件提供",
        )

    def test_group_activity_mark_uses_canonical_id(self) -> None:
        manager = SessionManager()
        manager.get_session_key("onebotv11", False, "u1", GROUP)
        manager.mark_activity("onebotv11", False, "u1", GROUP)
        self.assertIn(GROUP_ID, manager._last_activity, "活动基准落在业务会话域 id 上")
        manager._last_activity[GROUP_ID] = 1.0
        manager.mark_activity_group("milky", GROUP)  # 换协议后 push_message 的群级标记
        self.assertGreater(manager._last_activity[GROUP_ID], 1.0)


class NoDomainLeakTests(_Base):
    """T1 / 增量推送 / 交付代际不得拿 qqtpc 去调平台 API。"""

    def test_t1_clear_fences_with_real_adapter_only(self) -> None:
        class _Tracker:
            def __init__(self) -> None:
                self.scopes: list[str] = []

            def fence_scope_generation(self, scope_key: str, fence_generation: int):
                self.scopes.append(scope_key)
                return {"deliveries": 0, "submissions": 0}

        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 0, now=1.0)
        self.route("milky")
        manager = self.new_manager()
        tracker = _Tracker()
        manager.bind_run_tracker(tracker)

        manager.clear_internal(GROUP_ID)
        self.assertEqual(tracker.scopes, [f"milky:group:{GROUP}"])
        self.assertEqual(manager.get_session_key("milky", False, "u1", GROUP), f"hermes-{GROUP_ID}-g1")

    def test_unresolved_transport_skips_fence_instead_of_using_domain(self) -> None:
        class _Tracker:
            def __init__(self) -> None:
                self.scopes: list[str] = []

            def fence_scope_generation(self, scope_key: str, fence_generation: int):
                self.scopes.append(scope_key)
                return {"deliveries": 0, "submissions": 0}

        self.store.put(GROUP_ID, f"hermes-{GROUP_ID}", 0, now=1.0)
        with patch.object(session_mod, "_live_transports", lambda domain: []):
            manager = self.new_manager()
        tracker = _Tracker()
        manager.bind_run_tracker(tracker)

        manager.clear_internal(GROUP_ID)
        self.assertEqual(tracker.scopes, [], "没有真实 adapter 就不落栅栏，绝不写 qqtpc")
        self.assertEqual(self.store.load_all()[GROUP_ID], (f"hermes-{GROUP_ID}-g1", 1))

    def test_known_sessions_never_reports_session_domain(self) -> None:
        manager = self.new_manager()
        manager.get_session_key("onebotv11", False, "u1", GROUP)
        manager.get_session_key("onebotv11", True, USER, None)
        for row in manager.known_sessions():
            with self.subTest(internal_id=row[0]):
                self.assertFalse(is_session_domain(row[1]), "adapter 位置不许是业务会话域")
        for _internal_id, comp in manager.idle_expired(0, now=1e12):
            self.assertFalse(is_session_domain(comp[0]))


class MigrationTests(_Base):
    """一次性归一：保留 key/generation/idle_paused；冲突失败关闭且零写入。"""

    def test_migrates_rows_and_preserves_everything(self) -> None:
        self.store.put(
            f"onebotv11+group+{GROUP}", f"hermes-onebotv11+group+{GROUP}-g3", 3, now=1.0
        )
        self.store.put(f"milky+private+{USER}", "upstream-uuid-7", 5, now=2.0)
        self.store.set_idle_paused(f"onebotv11+group+{GROUP}", True)
        self.store.set_idle_paused("telegram+group+42", True)

        report = self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS)

        self.assertEqual(report.session_keys_migrated, 2)
        self.assertEqual(report.idle_paused_migrated, 1)
        self.assertTrue(report.integrity_ok)
        self.assertEqual(report.conflicts, ())
        self.assertTrue(self.integrity_ok())

        loaded = self.store.load_all()
        self.assertEqual(
            loaded[GROUP_ID], (f"hermes-onebotv11+group+{GROUP}-g3", 3), "key 与 generation 原样保留"
        )
        self.assertEqual(loaded[PRIVATE_ID], ("upstream-uuid-7", 5))
        self.assertNotIn(f"onebotv11+group+{GROUP}", loaded)
        self.assertEqual(
            self.store.load_idle_paused(), {GROUP_ID, "telegram+group+42"},
            "idle_paused 跟着归一，别的平台不动",
        )

        # 幂等：再跑一次什么都不做。
        again = self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS)
        self.assertEqual(again.session_keys_migrated, 0)
        self.assertEqual(again.idle_paused_migrated, 0)
        self.assertEqual(self.store.load_all(), loaded)

    def test_bind_store_normalizes_before_use(self) -> None:
        self.store.put(
            f"onebotv11+group+{GROUP}", f"hermes-onebotv11+group+{GROUP}", 0, now=1.0
        )
        self.store.set_idle_paused(f"onebotv11+group+{GROUP}", True)
        # 一次性归一是运维显式动作；启动只消费已归一结果。
        self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS)
        self.route("onebotv11")
        manager = self.new_manager()

        self.assertEqual([row[1] for row in manager.known_sessions()], ["onebotv11"])
        self.assertEqual(
            manager.get_session_key("milky", False, "u1", GROUP), f"hermes-onebotv11+group+{GROUP}",
            "归一后 key 续用（且不重命名现有 Hermes session id）",
        )
        # 真实 Milky 事件到达后，运行时解析立刻切到当前驱动，库仍不动。
        self.assertEqual([row[1] for row in manager.known_sessions()], ["milky"])
        self.assertEqual(manager.set_idle_paused("milky", False, "u1", GROUP, False), GROUP_ID)
        self.assertEqual(self.store.load_idle_paused(), set())

    def test_milky_only_mapping_for_main_instance(self) -> None:
        """主实例以后 milky→qqtpc 用同一入口（只换映射）。"""
        self.store.put(f"milky+group+{GROUP}", f"hermes-milky+group+{GROUP}", 2, now=1.0)
        report = self.store.migrate_session_domain({"milky": SESSION_DOMAIN_QQTPC})
        self.assertEqual(report.session_keys_migrated, 1)
        self.assertEqual(self.store.load_all()[GROUP_ID], (f"hermes-milky+group+{GROUP}", 2))

    def test_conflict_between_transports_fails_closed(self) -> None:
        self.store.put(f"onebotv11+group+{GROUP}", "key-a", 1, now=1.0)
        self.store.put(f"milky+group+{GROUP}", "key-b", 7, now=2.0)
        before = self._dump()

        with self.assertRaises(SessionDomainMigrationConflict) as ctx:
            self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS)

        self.assertEqual(
            sorted(ctx.exception.conflicts),
            sorted(
                [
                    (f"onebotv11+group+{GROUP}", GROUP_ID),
                    (f"milky+group+{GROUP}", GROUP_ID),
                ]
            ),
        )
        self.assertEqual(self._dump(), before, "冲突必须零写入")
        self.assertTrue(self.integrity_ok())

    def test_conflict_with_existing_canonical_row_fails_closed(self) -> None:
        self.store.put(GROUP_ID, "key-new", 4, now=3.0)
        self.store.put(f"onebotv11+group+{GROUP}", "key-old", 1, now=1.0)
        self.store.set_idle_paused(GROUP_ID, True)
        self.store.set_idle_paused(f"onebotv11+group+{GROUP}", True)
        before = self._dump()

        with self.assertRaises(SessionDomainMigrationConflict):
            self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS)

        self.assertEqual(self._dump(), before)
        self.assertTrue(self.integrity_ok())

    def test_conflict_fails_closed_on_bind_store(self) -> None:
        self.store.put(f"onebotv11+group+{GROUP}", "key-a", 1, now=1.0)
        self.store.put(f"milky+group+{GROUP}", "key-b", 7, now=2.0)
        before = self._dump()

        with self.assertRaises(SessionDomainMigrationConflict):
            self.new_manager()

        self.assertEqual(self._dump(), before, "冲突必须零写入")
        # 旧行仍在库里，运维可用 --exclude 显式处理后重启。
        self.assertEqual(set(self.store.load_all()), {
            f"onebotv11+group+{GROUP}", f"milky+group+{GROUP}",
        })

    def test_main_instance_mixed_history_excludes_milky_test_session(self) -> None:
        """主实例定案：OneBot 主会话已混合 Milky 对话，继续用同一 key/generation；
        独立 milky 测试小会话显式排除（不收录、不删除），不做转录合并。"""
        main_old = f"onebotv11+private+{USER}"
        test_old = f"milky+private+{USER}"
        main_key = f"hermes-{main_old}-g404"
        self.store.put(main_old, main_key, 404, now=1.0)
        self.store.put(test_old, f"hermes-{test_old}", 0, now=2.0)
        self.store.set_idle_paused(main_old, True)
        before = self._dump()

        # 不排除 → 两条旧行争同一目标，失败关闭且零写入。
        with self.assertRaises(SessionDomainMigrationConflict):
            self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS)
        self.assertEqual(self._dump(), before)

        # 显式排除 milky 测试小会话后：主行归一，key/generation 原样保留。
        report = self.store.migrate_session_domain(
            SESSION_DOMAIN_TRANSPORTS, exclude=(test_old,)
        )
        self.assertEqual(report.session_keys_migrated, 1)
        self.assertEqual(report.excluded, (test_old,))
        self.assertTrue(report.integrity_ok)
        loaded = self.store.load_all()
        self.assertEqual(loaded[PRIVATE_ID], (main_key, 404), "主会话 key/generation 完整保留")
        self.assertEqual(loaded[test_old], (f"hermes-{test_old}", 0), "测试小会话原样保留")
        self.assertEqual(self.store.load_idle_paused(), {PRIVATE_ID})
        self.assertFalse((Path(self._tmp.name) / "state.db").exists(), "不碰转录库")
        self.assertTrue(self.integrity_ok())

        # 主实例以后切到 milky：同一 session_key/generation，库不需要再迁移。
        self.registry.upsert(
            adapter="milky", scope="private", scope_id=USER, bot_self_id="10000",
            target=object(), ts=0,
        )
        manager = self.new_manager()
        self.assertEqual(manager.get_session_key("milky", True, USER, None), main_key)
        self.assertEqual(manager.get_delivery_generation("milky", True, USER, None), 404)
        self.assertEqual([row[1] for row in manager.known_sessions()], ["milky"])

    def test_custom_merged_target_for_group(self) -> None:
        """群 253384307 定制案例：先在上游合并转录，再把 canonical 指向合并目标；
        不自行选 milky g9 / onebot g1076，也不复制或重写 state.db。"""
        group = "253384307"
        onebot_old = f"onebotv11+group+{group}"
        milky_old = f"milky+group+{group}"
        merged_key = f"hermes-merged+group+{group}-g1060"
        self.store.put(onebot_old, f"hermes-{onebot_old}-g1060", 1060, now=1.0)
        self.store.put(milky_old, f"hermes-{milky_old}-g9", 9, now=2.0)
        before = self._dump()

        report = self.store.migrate_session_domain(
            SESSION_DOMAIN_TRANSPORTS,
            exclude=(milky_old,),
            overrides={onebot_old: (merged_key, 1060)},
        )

        self.assertEqual(report.session_keys_migrated, 1)
        self.assertEqual(report.overrides_applied, 1)
        self.assertEqual(report.overrides_ignored, ())
        loaded = self.store.load_all()
        self.assertEqual(loaded[f"qqtpc+group+{group}"], (merged_key, 1060))
        self.assertEqual(loaded[milky_old], (f"hermes-{milky_old}-g9", 9), "Milky 旧行保留待合并卡处理")
        self.assertNotEqual(loaded[f"qqtpc+group+{group}"][0], f"hermes-{milky_old}-g9")
        self.assertNotEqual(loaded[f"qqtpc+group+{group}"][0], f"hermes-{onebot_old}-g1076")
        self.assertNotEqual(self._dump(), before)
        self.assertFalse((Path(self._tmp.name) / "state.db").exists(), "本卡不碰转录")

        # 重放：已归一行不再重复写，override 如实报告为 ignored 而不是改库。
        again = self.store.migrate_session_domain(
            SESSION_DOMAIN_TRANSPORTS,
            exclude=(milky_old,),
            overrides={onebot_old: (merged_key, 1060)},
        )
        self.assertEqual(again.session_keys_migrated, 0)
        self.assertEqual(again.overrides_ignored, (onebot_old,))
        self.assertEqual(self.store.load_all()[f"qqtpc+group+{group}"], (merged_key, 1060))

    def test_reusable_script_dry_run_and_apply(self) -> None:
        """可复用的一次性迁移入口：dry-run 零写入；冲突退出码 2；apply 保留 key。"""
        from scripts.migrate_session_domain import main as migrate_main

        main_old = f"onebotv11+group+{GROUP}"
        test_old = f"milky+group+{GROUP}"
        self.store.put(main_old, f"hermes-{main_old}-g9", 9, now=1.0)
        self.store.put(test_old, f"hermes-{test_old}", 0, now=2.0)
        before = self._dump()

        self.assertEqual(migrate_main(["--db", str(self.db_path)]), 2, "冲突失败关闭")
        self.assertEqual(self._dump(), before)
        self.assertEqual(
            migrate_main(["--db", str(self.db_path), "--dry-run", "--exclude", test_old]), 0
        )
        self.assertEqual(self._dump(), before, "dry-run 不写库")
        self.assertEqual(migrate_main(["--db", str(self.db_path), "--exclude", test_old]), 0)
        loaded = self.store.load_all()
        self.assertEqual(loaded[GROUP_ID], (f"hermes-{main_old}-g9", 9))
        self.assertEqual(loaded[test_old], (f"hermes-{test_old}", 0))
        self.assertTrue(self.integrity_ok())

    def test_dry_run_reports_without_writing(self) -> None:
        self.store.put(f"onebotv11+group+{GROUP}", "key-a", 1, now=1.0)
        before = self._dump()

        report = self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS, dry_run=True)

        self.assertEqual(report.session_keys_migrated, 1)
        self.assertEqual(self._dump(), before, "dry_run 不写库")

    def test_unknown_platform_rows_are_left_alone(self) -> None:
        self.store.put("telegram+group+42", "hermes-telegram+group+42", 0, now=1.0)
        self.store.migrate_session_domain(SESSION_DOMAIN_TRANSPORTS)
        self.assertEqual(
            self.store.load_all(), {"telegram+group+42": ("hermes-telegram+group+42", 0)}
        )


class BackgroundRoutingTests(_Base):
    """后台汇报/看板登记的会话前缀：会话域与旧 transport 形态都认。"""

    def test_canonical_key_waits_without_route_instead_of_using_domain(self) -> None:
        from nonebot_plugin_hermes_adapter.core.background_report_runtime import (
            resolve_background_route,
        )

        self.assertIsNone(
            resolve_background_route(f"hermes-{GROUP_ID}", (), prefixes=("hermes",)),
            "路由未就绪：事件应等待（route_unavailable），不能拿 qqtpc 去唤醒",
        )

    def test_canonical_key_resolves_from_known_sessions(self) -> None:
        from nonebot_plugin_hermes_adapter.core.background_report_runtime import (
            resolve_background_route,
        )

        key = f"hermes-{GROUP_ID}"
        route = resolve_background_route(
            key,
            [(GROUP_ID, "milky", False, "", GROUP, key)],
            prefixes=("hermes",),
        )
        self.assertEqual(
            route,
            {"adapter_name": "milky", "is_private": False, "user_id": None, "group_id": GROUP},
        )

    def test_legacy_transport_key_still_parses_to_real_adapter(self) -> None:
        from nonebot_plugin_hermes_adapter.core.background_report_runtime import (
            resolve_background_route,
        )

        route = resolve_background_route(
            f"hermes-onebotv11+group+{GROUP}-g2", (), prefixes=("hermes",)
        )
        self.assertEqual(route["adapter_name"], "onebotv11")

    def test_session_prefixes_cover_domain_and_legacy_transport(self) -> None:
        from nonebot_plugin_hermes_adapter.mcp.tools.background_status import _session_prefixes

        prefixes = _session_prefixes("onebotv11", True, USER)
        self.assertIn(f"hermes-{PRIVATE_ID}", prefixes, "新派生 key（hermes-qqtpc…）")
        self.assertIn(f"hermes-onebotv11+private+{USER}", prefixes, "归一前登记的旧 key")
        self.assertEqual(_session_prefixes("qq", True, "openid-1"), ["hermes-qq+private+openid-1"])


if __name__ == "__main__":
    unittest.main()
