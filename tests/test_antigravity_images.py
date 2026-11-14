"""阶段2C2：Antigravity 图片输入（ACP `image` 内容块）离线 TDD。

链路与 2B/2C1 同源：真 `_assemble_media_parts`／真 `_label_groups`／真 handler
（`_run_passive_turn`）→ 真 `HermesClient.chat` → 真 `AntigravityACPClient` →
假 `agy acp` NDJSON 子进程；只有内联抓图（`inline_image_frames`）与平台发送换成
进程内替身。不发真实 QQ、不跑真实模型、不读真实图片。

覆盖：
- data URL 拆成 `data`＋`mimeType`；纯文本（str／全文本 parts）仍是单 text 块；
- 文字／图片／`m:id#idx` 标签顺序与既有帧组一致；多图、动图多帧；
- 单轮 10 张内联图上限（保留最新）；
- 远程 URL、坏 base64、非图片 part 一律 fail-closed（本地拒绝，不进 ACP）；
- 真 handler → 假 ACP 的 wire 里出现 image 块（不只测 helper）；
- media off 时纯文本行为不变。
"""

from __future__ import annotations

import asyncio
import base64
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from tests.acp_2b_harness import ACPHandlerHarness  # noqa: E402
from nonebot_plugin_hermes_adapter.backends.antigravity.client import (  # noqa: E402
    prompt_blocks,
)
from nonebot_plugin_hermes_adapter.core.hermes_client import (  # noqa: E402
    _antigravity_content_blocks,
)
from nonebot_plugin_hermes_adapter.core.prompt_builder import (  # noqa: E402
    _assemble_media_parts,
)

PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"acp2c2-canary"
PNG_B64 = base64.b64encode(PNG_BYTES).decode("ascii")


def data_url(payload: bytes = PNG_BYTES, mime: str = "image/png") -> str:
    """既有内联链形态的 data URL（base64 由 image_inline 产出，带 padding）。"""
    return f"data:{mime};base64,{base64.b64encode(payload).decode('ascii')}"


def image_part(url: str) -> dict:
    return {"type": "image_url", "image_url": {"url": url}}


# --- 1. 内容块转换（纯函数层） ---------------------------------------------


class ContentBlockConversionTests(unittest.TestCase):
    def test_plain_text_string_stays_one_unchanged_text_block(self) -> None:
        self.assertEqual(
            _antigravity_content_blocks("你好呀"),
            [{"type": "text", "text": "你好呀"}],
        )

    def test_all_text_parts_still_join_into_one_block_like_2b(self) -> None:
        content = [
            {"type": "text", "text": "第一段"},
            {"type": "text", "text": ""},
            {"type": "text", "text": "第二段"},
        ]
        self.assertEqual(
            _antigravity_content_blocks(content),
            [{"type": "text", "text": "第一段\n第二段"}],
        )

    def test_empty_part_list_keeps_the_previous_empty_text(self) -> None:
        self.assertEqual(
            _antigravity_content_blocks([]), [{"type": "text", "text": ""}]
        )

    def test_data_url_is_split_into_data_and_mime_type(self) -> None:
        blocks = _antigravity_content_blocks(
            [{"type": "text", "text": "看图"}, image_part(data_url())]
        )
        self.assertEqual(
            blocks,
            [
                {"type": "text", "text": "看图"},
                {"type": "image", "data": PNG_B64, "mimeType": "image/png"},
            ],
        )
        # 不允许把 data URL 形态或 resource 形态漏到 ACP 块里。
        dumped = json.dumps(blocks, ensure_ascii=False)
        self.assertNotIn("data:image", dumped)
        self.assertNotIn("resource", dumped)
        self.assertNotIn("image_url", dumped)

    def test_text_image_order_and_mid_label_follow_the_assembled_parts(self) -> None:
        parts = _assemble_media_parts(
            "聊天区正文", [(("image", 7, 0), [data_url()])], []
        )
        blocks = _antigravity_content_blocks(parts)
        self.assertEqual([block["type"] for block in blocks], ["text", "text", "image"])
        self.assertEqual(blocks[0]["text"], "聊天区正文")
        self.assertEqual(blocks[1]["text"], "[图 m:7#0]")
        self.assertEqual(blocks[2]["data"], PNG_B64)

    def test_multiple_images_and_gif_frame_group_keep_their_order(self) -> None:
        frames = [data_url(f"frame-{index}".encode(), "image/jpeg") for index in range(3)]
        parts = _assemble_media_parts(
            "聊天区正文",
            [(("image", 9, 0), [data_url()]), (("image", 9, 1), frames)],
            [],
        )
        blocks = _antigravity_content_blocks(parts)
        self.assertEqual(
            [block["type"] for block in blocks],
            ["text", "text", "image", "text", "image", "text", "image", "text", "image"],
        )
        labels = [block["text"] for block in blocks if block["type"] == "text"][1:]
        self.assertEqual(
            labels,
            [
                "[图 m:9#0]",
                "[动图 m:9#1 第1/3帧，同一动图按时间顺序]",
                "[动图 m:9#1 第2/3帧，同一动图按时间顺序]",
                "[动图 m:9#1 第3/3帧，同一动图按时间顺序]",
            ],
        )
        self.assertEqual(
            [block["mimeType"] for block in blocks if block["type"] == "image"],
            ["image/png", "image/jpeg", "image/jpeg", "image/jpeg"],
        )

    def test_more_than_ten_inline_images_keep_the_latest_ten(self) -> None:
        urls = [data_url(f"image-{index:02d}".encode()) for index in range(12)]
        content = [{"type": "text", "text": "很多图"}] + [image_part(url) for url in urls]
        blocks = _antigravity_content_blocks(content)
        images = [block for block in blocks if block["type"] == "image"]
        self.assertEqual(len(images), 10)
        self.assertEqual(images[0]["data"], base64.b64encode(b"image-02").decode())
        self.assertEqual(images[-1]["data"], base64.b64encode(b"image-11").decode())

    def test_non_image_or_bad_data_fails_closed(self) -> None:
        cases = {
            "remote http url": image_part("https://example.invalid/a.png"),
            "no base64 marker": image_part("data:image/png,AAAA"),
            "empty payload": image_part("data:image/png;base64,"),
            "illegal base64 chars": image_part("data:image/png;base64,AA!!"),
            "bad padding": image_part("data:image/png;base64,A"),
            "not an image mime": image_part(
                f"data:application/octet-stream;base64,{PNG_B64}"
            ),
            "image_url not a dict": {"type": "image_url", "image_url": "x"},
            "audio part": {"type": "input_audio", "input_audio": {"data": "AA", "format": "wav"}},
            "video part": {"type": "video_url", "video_url": {"url": data_url()}},
            "file part": {"type": "file", "file": {"file_id": "f1"}},
            "resource part": {"type": "resource", "resource": {"uri": "file:///tmp/a"}},
            "plain string part": "not-a-part",
        }
        for name, part in cases.items():
            with self.subTest(case=name):
                self.assertIsNone(
                    _antigravity_content_blocks([{"type": "text", "text": "hi"}, part]),
                    name,
                )

    def test_non_list_non_str_content_is_refused(self) -> None:
        for value in (None, 42, {"type": "text", "text": "x"}):
            with self.subTest(value=value):
                self.assertIsNone(_antigravity_content_blocks(value))


class ClientPromptBlocksTests(unittest.TestCase):
    def test_string_prompt_is_still_byte_identical(self) -> None:
        self.assertEqual(
            prompt_blocks("纯文本"), [{"type": "text", "text": "纯文本"}]
        )

    def test_block_list_is_forwarded_in_order_without_mutation(self) -> None:
        blocks = [
            {"type": "text", "text": "看图"},
            {"type": "image", "data": PNG_B64, "mimeType": "image/png"},
        ]
        sent = prompt_blocks(blocks)
        self.assertEqual(sent, blocks)
        self.assertIsNot(sent[0], blocks[0], "不共享可变 dict")
        self.assertIsNot(sent[1], blocks[1], "不共享可变 dict")


# --- 2. 真 handler → 假 ACP：wire 里必须是 image 块 ------------------------


class ImageWireHandlerTests(ACPHandlerHarness):
    def _patch_inline(self, inline):
        mock = AsyncMock(return_value=inline)
        self.stack.enter_context(patch.object(self.mt, "inline_image_frames", mock))
        return mock

    def _prompt_blocks(self) -> list[dict]:
        prompts = self.rows("session/prompt")
        self.assertEqual(len(prompts), 1, prompts)
        return prompts[0]["params"]["prompt"]

    async def test_single_still_image_reaches_the_wire_as_an_image_block(self) -> None:
        url = "http://example.invalid/still.png"
        self._patch_inline([(url, [data_url()])])
        self._client(reply="§\n看到图里的字了\n§")
        row = self._row(content="看看这张图", image_urls=[url])

        result = await asyncio.wait_for(self._turn(row, image_urls=[url]), 10)

        self.assertFalse(result.is_local_refusal)
        blocks = self._prompt_blocks()
        self.assertEqual([block["type"] for block in blocks], ["text", "text", "image"])
        self.assertIn("当前时间", blocks[0]["text"])
        self.assertEqual(blocks[1]["text"], "[图 m:1#0]")
        self.assertEqual(blocks[2], {"type": "image", "data": PNG_B64, "mimeType": "image/png"})
        self.assertIn("看到图里的字了", self.platform)

    async def test_multiple_images_and_a_gif_frame_group_keep_wire_order(self) -> None:
        still = "http://example.invalid/a.png"
        gif = "http://example.invalid/b.gif"
        frames = [data_url(f"gif-{index}".encode(), "image/jpeg") for index in range(3)]
        self._patch_inline([(still, [data_url()]), (gif, frames)])
        self._client(reply="§\n三帧动图\n§")
        row = self._row(content="两张图", image_urls=[still, gif])

        result = await asyncio.wait_for(
            self._turn(row, image_urls=[still, gif]), 10
        )

        self.assertFalse(result.is_local_refusal)
        blocks = self._prompt_blocks()
        self.assertEqual(
            [block["type"] for block in blocks],
            ["text", "text", "image", "text", "image", "text", "image", "text", "image"],
        )
        labels = [block["text"] for block in blocks if block["type"] == "text"][1:]
        self.assertEqual(
            labels,
            [
                "[图 m:1#0]",
                "[动图 m:1#1 第1/3帧，同一动图按时间顺序]",
                "[动图 m:1#1 第2/3帧，同一动图按时间顺序]",
                "[动图 m:1#1 第3/3帧，同一动图按时间顺序]",
            ],
        )
        self.assertEqual(
            [block["mimeType"] for block in blocks if block["type"] == "image"],
            ["image/png", "image/jpeg", "image/jpeg", "image/jpeg"],
        )

    async def test_plain_text_turn_still_sends_exactly_one_text_block(self) -> None:
        inline = self._patch_inline([])
        self._client(reply="§\n纯文本\n§")

        result = await asyncio.wait_for(self._turn(), 10)

        self.assertFalse(result.is_local_refusal)
        inline.assert_not_called()
        blocks = self._prompt_blocks()
        self.assertEqual(len(blocks), 1, blocks)
        self.assertEqual(blocks[0]["type"], "text")
        self.assertIn("当前时间", blocks[0]["text"])

    async def test_media_off_group_turn_keeps_text_only_wire(self) -> None:
        # harness 已把 load_media_config 固定为 mode="off"，与 2B/2C1 同口径。
        inline = self._patch_inline([])
        self._client(reply="§\n群里纯文本\n§")
        url = "http://example.invalid/off.png"
        row = self._row(private=False, image_urls=[url])

        result = await asyncio.wait_for(
            self._turn(row, private=False, image_urls=[url]), 10
        )

        self.assertFalse(result.is_local_refusal)
        inline.assert_not_called()
        blocks = self._prompt_blocks()
        self.assertEqual([block["type"] for block in blocks], ["text"])
        self.assertNotIn("data:image", blocks[0]["text"])

    async def test_remote_image_url_fails_closed_before_any_acp_frame(self) -> None:
        # 内联链没有产出 data URL（例如抓图失败后回退远程 URL）：不得自己下载。
        url = "http://example.invalid/raw.png"
        self._patch_inline([(url, [url])])
        self._client(reply="§\n不应到达模型\n§")
        row = self._row(image_urls=[url])

        result = await asyncio.wait_for(self._turn(row, image_urls=[url]), 10)

        self.assertTrue(result.is_local_refusal)
        self.assertIn("暂不支持", result.raw_text)
        self.assertEqual(self.wire(), [], "坏数据轮不得启动 ACP 进程或提交 prompt")

    async def test_malformed_inline_data_url_fails_closed(self) -> None:
        url = "http://example.invalid/broken.png"
        self._patch_inline([(url, ["data:image/png;base64,@@not-base64@@"])])
        self._client(reply="§\n不应到达模型\n§")
        row = self._row(image_urls=[url])

        result = await asyncio.wait_for(self._turn(row, image_urls=[url]), 10)

        self.assertTrue(result.is_local_refusal)
        self.assertEqual(self.wire(), [])


if __name__ == "__main__":
    unittest.main()
