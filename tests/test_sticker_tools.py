from __future__ import annotations

import asyncio
import base64
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from tests import _bootstrap  # noqa: F401
from mcp.types import ImageContent

from nonebot_plugin_hermes_adapter.core.media_config import MediaConfig
from nonebot_plugin_hermes_adapter.core.sticker_library import StickerLibrary
from nonebot_plugin_hermes_adapter.mcp.tools.sticker_library import (
    preview_sticker_impl,
    search_sticker_library_impl,
)


PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


class StickerToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "speechless.png").write_bytes(PNG)
        with (self.root / "index.csv").open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "kind", "file", "note"])
            writer.writerow(["speechless", "无语", "speechless.png", "被离谱消息整无语"])
        self.library = StickerLibrary(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_search_returns_bounded_csv_without_host_paths(self) -> None:
        result = search_sticker_library_impl("无语", 20, library=self.library)
        self.assertTrue(result.ok)
        self.assertEqual(result.total, 1)
        self.assertIn("speechless,无语,speechless.png", result.csv)
        self.assertNotIn(str(self.root), result.csv)

    def test_preview_reuses_image_content_and_original_bytes(self) -> None:
        blocks = asyncio.run(
            preview_sticker_impl("speechless", library=self.library, cfg=MediaConfig())
        )
        header = json.loads(blocks[0].text)
        self.assertTrue(header["available"])
        images = [block for block in blocks if isinstance(block, ImageContent)]
        self.assertEqual(len(images), 1)
        self.assertEqual(base64.b64decode(images[0].data), PNG)

    def test_preview_animated_gif_reuses_frame_extraction(self) -> None:
        frames = [Image.new("RGB", (8, 8), (i * 40, 0, 0)) for i in range(4)]
        buf = io.BytesIO()
        frames[0].save(buf, format="GIF", save_all=True, append_images=frames[1:], duration=80)
        (self.root / "animated.gif").write_bytes(buf.getvalue())
        with (self.root / "index.csv").open("a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["animated", "测试", "animated.gif", "动图"])
        blocks = asyncio.run(
            preview_sticker_impl(
                "animated",
                library=self.library,
                cfg=MediaConfig(fetch_gif_max_frames=3),
            )
        )
        header = json.loads(blocks[0].text)
        self.assertTrue(header["available"])
        self.assertEqual(header["frames"], 3)
        self.assertEqual(sum(isinstance(block, ImageContent) for block in blocks), 3)

    def test_preview_unknown_id_is_structured_not_found(self) -> None:
        blocks = asyncio.run(
            preview_sticker_impl("missing", library=self.library, cfg=MediaConfig())
        )
        self.assertEqual(json.loads(blocks[0].text)["reason"], "not_found")
        self.assertEqual(len(blocks), 1)


if __name__ == "__main__":
    unittest.main()
