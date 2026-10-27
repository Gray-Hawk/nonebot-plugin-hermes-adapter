from __future__ import annotations

import asyncio
import base64
import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.core.sticker_library import StickerLibrary
from nonebot_plugin_hermes_adapter.core import outbound


PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


class StickerOutboundTests(unittest.TestCase):
    class FakeBot:
        def __init__(self) -> None:
            self.api_calls = []

        async def call_api(self, name: str, **kwargs):
            self.api_calls.append((name, kwargs))
            return {"message_id": 123}

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "speechless.png").write_bytes(PNG)
        with (self.root / "index.csv").open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "kind", "file", "note"])
            writer.writerow(["speechless", "无语", "speechless.png", "被离谱消息整无语"])
        self.library = StickerLibrary(self.root)
        self.sent = []
        self.bot = self.FakeBot()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def _fake_send(self, uni, *, bot, target) -> list[str]:
        self.sent.append(uni)
        return ["123"]

    def _send(self, text: str):
        with patch.object(outbound, "_send_unimsg", new=self._fake_send):
            return asyncio.run(
                outbound.send_text_with_media(
                    bot=self.bot,
                    target=SimpleNamespace(private=True, id="10001"),
                    text=text,
                    media_urls=[],
                    adapter_name="onebotv11",
                    sticker_library=self.library,
                )
            )

    def test_legacy_result_objects_keep_original_history_and_zero_sticker_count(self) -> None:
        for legacy in (SimpleNamespace(), True, False):
            self.assertEqual(outbound.send_result_history(legacy, "原文"), "原文")
            self.assertEqual(outbound.send_result_sticker_count(legacy), 0)

    def test_pure_marker_sends_original_image_as_sticker(self) -> None:
        result = self._send("表情=speechless")
        self.assertTrue(result.ok)
        self.assertEqual(result.sticker_id, "speechless")
        self.assertEqual(result.msg_ids, ["123"])
        self.assertEqual(result.history_content("表情=speechless"), "[表情包]")
        self.assertEqual(result.sticker_media_count, 1)
        self.assertEqual(self.sent, [])
        self.assertEqual(len(self.bot.api_calls), 1)
        api_name, payload = self.bot.api_calls[0]
        self.assertEqual(api_name, "send_private_msg")
        self.assertEqual(payload["user_id"], 10001)
        image = payload["message"][0]
        self.assertEqual(image["type"], "image")
        self.assertEqual(image["data"]["sub_type"], 1)
        self.assertNotIn("subType", image["data"])
        self.assertEqual(
            base64.b64decode(image["data"]["file"].removeprefix("base64://")),
            PNG,
        )

    def test_unknown_id_fails_without_sending_literal_marker(self) -> None:
        result = self._send("表情=missing")
        self.assertFalse(result.ok)
        self.assertEqual(self.sent, [])

    def test_ordinary_text_keeps_existing_behavior(self) -> None:
        result = self._send("普通回复")
        self.assertTrue(result.ok)
        self.assertIsNone(result.sticker_id)
        self.assertEqual(str(self.sent[0][0]), "普通回复")


if __name__ == "__main__":
    unittest.main()
