from __future__ import annotations

import base64
import csv
import tempfile
import unittest
from pathlib import Path

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.core.sticker_library import StickerLibrary


PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


class StickerLibraryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "speechless.png").write_bytes(PNG)
        (self.root / "happy.png").write_bytes(PNG)
        with (self.root / "index.csv").open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "kind", "file", "note"])
            writer.writerow(["speechless", "无语|无奈", "speechless.png", "被离谱消息整无语"])
            writer.writerow(["happy", "开心", "happy.png", "真诚地开心"])
        self.library = StickerLibrary(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_loads_csv_and_searches_id_kind_and_note(self) -> None:
        rows, total = self.library.search("无语")
        self.assertEqual(total, 1)
        self.assertEqual([row.id for row in rows], ["speechless"])
        self.assertEqual(rows[0].kind, "无语|无奈")
        self.assertEqual(self.library.get("happy").note, "真诚地开心")
        self.assertEqual(self.library.search("SPEECHLESS")[1], 1)
        self.assertEqual(self.library.search("不存在")[1], 0)

    def test_search_limit_preserves_total(self) -> None:
        rows, total = self.library.search("", limit=1)
        self.assertEqual(total, 2)
        self.assertEqual(len(rows), 1)

    def test_read_returns_original_bytes_and_rejects_path_escape(self) -> None:
        raw, mime = self.library.read("speechless")
        self.assertEqual(raw, PNG)
        self.assertEqual(mime, "image/png")
        with (self.root / "index.csv").open("a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["escape", "测试", "../outside.png", "越库"])
        self.assertIsNone(self.library.get("escape"))

    def test_duplicate_and_invalid_ids_are_not_selectable(self) -> None:
        with (self.root / "index.csv").open("a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["happy", "重复", "speechless.png", "重复 ID"])
            writer.writerow(["坏 ID", "非法", "speechless.png", "非法 ID"])
        self.assertEqual(self.library.get("happy").file, "happy.png")
        self.assertIsNone(self.library.get("坏 ID"))


if __name__ == "__main__":
    unittest.main()
