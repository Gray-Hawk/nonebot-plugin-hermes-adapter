"""QQTPC 状态面身份：活跃窗口、recent 查询、群记忆/身份档案、后台平台族、图片归属。

身份统一后，运行态按业务会话域聚合，但 messages.db.adapter 仍是到达事实（保留原始
transport 供排障）。群记忆写向 ``memory/qqtpc``，旧 ``memory/<transport>`` 只做兼容
读取；离线迁移不在运行时自动执行。
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import group_profile  # noqa: E402
from nonebot_plugin_hermes_adapter.core import identity  # noqa: E402
from nonebot_plugin_hermes_adapter.core.active_session import (  # noqa: E402
    ActiveSessionManager,
)
from nonebot_plugin_hermes_adapter.core.background_report_runtime import (  # noqa: E402
    platform_family,
)
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.window_store import WindowStore  # noqa: E402

GROUP = "462540549"
USER = "10001"


def _msg(*, adapter: str, ts: int, text: str, group_id: str | None = GROUP) -> BufferedMessage:
    return BufferedMessage(
        ts=ts, adapter=adapter, group_id=group_id, user_id=USER, nickname="u",
        content=text, origin=MessageOrigin.human(),
        private_owner_id=None if group_id else USER,
    )


class ActiveWindowDomainTests(unittest.TestCase):
    def test_window_is_shared_across_transports(self) -> None:
        manager = ActiveSessionManager(default_ttl_sec=300)
        session = manager.trigger("onebotv11", GROUP, USER, now_ms=1000)

        self.assertIs(manager.get("milky", GROUP), session)
        self.assertTrue(manager.is_alive_for_trigger("milky", GROUP, 1001))
        self.assertIsNone(manager.get("qq", GROUP), "官方 QQ 是另一个窗口")
        self.assertEqual([s.adapter for s in manager.list("milky")], ["qqtpc"],
                         "窗口 identity 是业务会话域；真实 adapter 由运行时路由承载")

        manager.end("milky", GROUP)
        self.assertIsNone(manager.get("onebotv11", GROUP))

    def test_persisted_window_is_restored_for_current_transport(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = WindowStore(Path(tmp.name) / "windows.db")
        self.addCleanup(store.close)

        first = ActiveSessionManager(default_ttl_sec=300)
        first.bind_store(store, now_ms=1000)
        first.trigger("onebotv11", GROUP, USER, now_ms=1000)

        second = ActiveSessionManager(default_ttl_sec=300)
        restored = second.bind_store(store, now_ms=1001)
        self.assertEqual(restored, 1)
        self.assertTrue(
            second.is_alive_for_trigger("milky", GROUP, 1001),
            "重启后装回的窗口按业务域服务当前驱动",
        )

    def test_legacy_transport_window_row_is_readable_after_switch(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = WindowStore(Path(tmp.name) / "windows.db")
        self.addCleanup(store.close)
        from nonebot_plugin_hermes_adapter.core.storage.window_store import session_to_payload

        legacy = ActiveSessionManager(default_ttl_sec=300)
        legacy.bind_store(store, now_ms=1000)
        session = legacy.trigger("onebotv11", GROUP, USER, now_ms=1000)
        # 模拟旧版本落库形态：adapter 列就是 transport
        store.upsert("onebotv11", GROUP, session_to_payload(session), session.expires_at)

        current = ActiveSessionManager(default_ttl_sec=300)
        current.bind_store(store, now_ms=1001)
        self.assertTrue(current.is_alive_for_trigger("milky", GROUP, 1001))


class MessageQueryDomainTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = MessageStore(Path(tmp.name) / "messages.db")
        self.addCleanup(self.store._conn.close)

    def test_group_recent_reads_all_qqtpc_transports_only(self) -> None:
        ids = {}
        for adapter, ts, text in (
            ("onebotv11", 1, "旧协议"),
            ("milky", 2, "新协议"),
            ("qq", 3, "官方QQ"),
            ("astrbot", 4, "其它平台"),
        ):
            msg = _msg(adapter=adapter, ts=ts, text=text)
            ids[adapter] = self.store.append(msg)

        rows = self.store.get_recent("milky", GROUP, 10)

        self.assertEqual([r.content for r in rows], ["新协议", "旧协议"])
        self.assertEqual({r.adapter for r in rows}, {"onebotv11", "milky"},
                         "读跨协议，但行上的 adapter 仍是到达事实")

    def test_private_recent_reads_all_qqtpc_transports_only(self) -> None:
        self.store.append(_msg(adapter="onebotv11", ts=1, text="旧", group_id=None))
        self.store.append(_msg(adapter="milky", ts=2, text="新", group_id=None))
        self.store.append(_msg(adapter="qq", ts=3, text="官方", group_id=None))

        rows = self.store.get_recent("milky", None, 10, owner_user_id=USER)

        self.assertEqual([r.content for r in rows], ["新", "旧"])

    def test_reply_target_accepts_same_domain_history(self) -> None:
        mid = self.store.append(_msg(adapter="onebotv11", ts=1, text="旧协议"))
        self.store._conn.execute(
            "UPDATE messages SET qq_message_id=? WHERE id=?", ("555", mid)
        )
        self.store._conn.commit()

        self.assertEqual(
            self.store.get_reply_target("milky", GROUP, mid), ("555", USER)
        )
        self.assertIsNone(self.store.get_reply_target("qq", GROUP, mid))


class MemoryLayoutDomainTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.addCleanup(identity.clear_cache)
        self.patches = [
            patch.object(group_profile, "resolve_custom_dir", lambda: self.root),
            patch.object(
                group_profile, "load_misc_config",
                lambda: SimpleNamespace(group_profile_enabled=True),
            ),
            patch.object(
                identity, "load_misc_config",
                lambda: SimpleNamespace(group_profile_enabled=True),
            ),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_memory_root_uses_domain(self) -> None:
        self.assertEqual(group_profile.memory_root("milky"), self.root / "memory" / "qqtpc")
        self.assertEqual(group_profile.memory_root("qq"), self.root / "memory" / "qq")

    def test_new_profile_is_written_under_qqtpc(self) -> None:
        block = group_profile.build_group_profile_block("milky", GROUP)

        self.assertIsNotNone(block)
        self.assertTrue((self.root / "memory" / "qqtpc" / "groups" / f"{GROUP}.md").exists())
        self.assertFalse((self.root / "memory" / "onebotv11" / "groups" / f"{GROUP}.md").exists())

    def test_legacy_profile_is_read_but_canonical_wins(self) -> None:
        legacy = self.root / "memory" / "onebotv11" / "groups" / f"{GROUP}.md"
        legacy.parent.mkdir(parents=True)
        legacy.write_text("旧档案正文\n群友: \n", encoding="utf-8")

        block = group_profile.build_group_profile_block("milky", GROUP)
        self.assertIn("旧档案正文", block or "", "旧 transport 档案兼容读取")

        canonical = self.root / "memory" / "qqtpc" / "groups" / f"{GROUP}.md"
        canonical.parent.mkdir(parents=True, exist_ok=True)
        canonical.write_text("新档案正文\n群友: \n", encoding="utf-8")
        block = group_profile.build_group_profile_block("onebotv12", GROUP)
        self.assertIn("新档案正文", block or "")
        self.assertNotIn("旧档案正文", block or "")

    def test_identity_table_reads_legacy_and_writes_canonical(self) -> None:
        legacy = self.root / "memory" / "onebotv11" / "identities.csv"
        legacy.parent.mkdir(parents=True)
        legacy.write_text(
            "user_id,nickname,aliases,merged_into,note\n"
            f"{USER},旧名字,,,\n",
            encoding="utf-8",
        )

        self.assertEqual(identity.resolve_display_name(self.root, "milky", USER), "旧名字")
        self.assertFalse(
            (self.root / "memory" / "qqtpc" / "identities.csv").exists(),
            "存在旧表时读取不强制复制",
        )

        # 没有可读旧表时，缺档自动建档写到 canonical 目录（旧表存在时不复制/不建空表）
        self.assertEqual(identity.resolve_display_name(self.root, "milky", "20002"), None)
        self.assertFalse((self.root / "memory" / "qqtpc" / "identities.csv").exists())

        fresh = self.root / "fresh"
        self.assertIsNone(identity.resolve_display_name(fresh, "milky", "20002"))
        self.assertTrue((fresh / "memory" / "qqtpc" / "identities.csv").exists())


class BackgroundPlatformFamilyTests(unittest.TestCase):
    def test_only_registered_qqtpc_transports_share_family(self) -> None:
        for tag in ("onebotv11", "OneBot V12", "milky", "icqq", "qqtpc"):
            self.assertEqual(platform_family(tag), "qqtpc", tag)
        self.assertEqual(platform_family("qq"), "qq")
        self.assertEqual(platform_family("astrbot"), "astrbot")
        self.assertEqual(platform_family("icqq9"), "icqq9")


class _ImgStore:
    def __init__(self, owners):
        self._owners = owners

    def get_message_owners(self, ids):
        return {mid: self._owners[mid] for mid in ids if mid in self._owners}

    def get_message_images_meta(self, ids):
        return {
            mid: [{"idx": 0, "kind": "image", "sha256": None, "mime_type": "image/jpeg"}]
            for mid in ids if mid in self._owners
        }


class _ImgCache:
    def get_bytes(self, sha):
        return None


class ImageOwnershipDomainTests(unittest.TestCase):
    def _reasons(self, owners, *, adapter):
        from nonebot_plugin_hermes_adapter.mcp.tools.get_message_images import (
            GetMessageImagesInput,
            get_message_images_impl,
        )

        inp = GetMessageImagesInput(message_ids=list(owners), adapter=adapter, group_id=GROUP)
        with patch(
            "nonebot_plugin_hermes_adapter.mcp.tools.get_message_images.load_media_config",
            lambda: SimpleNamespace(),
        ):
            blocks = asyncio.run(
                get_message_images_impl(
                    inp, store=_ImgStore(owners), cache=_ImgCache(), scope=CallerScope.dev()
                )
            )
        import json

        return [r["reason"] for r in json.loads(blocks[0].text)["results"]]

    def test_same_domain_owner_is_not_denied(self) -> None:
        self.assertEqual(self._reasons({1: ("onebotv11", GROUP)}, adapter="milky"), ["cache_miss"])

    def test_official_qq_owner_is_denied(self) -> None:
        self.assertEqual(self._reasons({1: ("qq", GROUP)}, adapter="milky"), ["not_found"])


if __name__ == "__main__":
    unittest.main()
