from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


_SCRATCH = tempfile.TemporaryDirectory()
_ROOT = Path(_SCRATCH.name)
os.environ["LOCALSTORE_DATA_DIR"] = str(_ROOT / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_ROOT / "cfg")
(_ROOT / "cfg").mkdir()

from tests import _bootstrap  # noqa: F401,E402
from nonebot_plugin_hermes_adapter.core.sticker_library import StickerLibrary  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import commands  # noqa: E402


_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c63606060f80f0001040100b51c0c020000000049454e44ae426082"
)


class StickerReloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "one.png").write_bytes(_PNG)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_index(self, sticker_id: str) -> None:
        path = self.root / "index.csv"
        path.write_text(
            f"id,kind,file,note\n{sticker_id},测试,one.png,测试\n",
            encoding="utf-8",
        )

    def test_reload_forces_csv_reread_even_with_same_signature(self) -> None:
        self._write_index("before")
        library = StickerLibrary(self.root)
        self.assertIsNotNone(library.get("before"))
        index = self.root / "index.csv"
        stat = index.stat()
        self._write_index("after_")
        self.assertEqual(index.stat().st_size, stat.st_size)
        os.utime(index, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assertEqual(library.reload(), 1)
        self.assertIsNone(library.get("before"))
        self.assertIsNotNone(library.get("after_"))

    def test_admin_config_reload_refreshes_sticker_library(self) -> None:
        fake = SimpleNamespace(calls=0)

        def reload_library() -> int:
            fake.calls += 1
            return 7

        fake.reload = reload_library
        result = SimpleNamespace(ok=True, reloaded=["media"], message="配置已重载")
        with (
            patch.object(commands, "_is_admin", return_value=True),
            patch.object(commands, "reload_runtime_config", return_value=result),
            patch.object(commands, "default_sticker_library", return_value=fake),
        ):
            message = commands._config_reload_message("onebotv11", "admin")
        self.assertEqual(fake.calls, 1)
        self.assertIn("表情 7", message)

    def test_failed_config_reload_does_not_refresh_sticker_library(self) -> None:
        fake = SimpleNamespace(reload=lambda: 7)
        result = SimpleNamespace(ok=False, reloaded=[], message="配置坏档")
        with (
            patch.object(commands, "_is_admin", return_value=True),
            patch.object(commands, "reload_runtime_config", return_value=result),
            patch.object(commands, "default_sticker_library", return_value=fake),
            patch.object(fake, "reload") as reload_mock,
        ):
            message = commands._config_reload_message("onebotv11", "admin")
        reload_mock.assert_not_called()
        self.assertIn("配置坏档", message)


if __name__ == "__main__":
    unittest.main()
