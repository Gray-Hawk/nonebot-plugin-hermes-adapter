from __future__ import annotations

import io
import unittest

from PIL import Image

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.core.image_inline import encode_image_frames_bytes


def make_gif(colors: list[tuple[int, int, int]]) -> bytes:
    frames = [Image.new("RGB", (8, 8), color) for color in colors]
    buf = io.BytesIO()
    frames[0].save(buf, format="GIF", save_all=True, append_images=frames[1:], duration=80)
    return buf.getvalue()


def decoded_colors(blocks: list[tuple[bytes, str]]) -> list[tuple[int, int, int]]:
    colors = []
    for raw, _mime in blocks:
        with Image.open(io.BytesIO(raw)) as im:
            colors.append(im.convert("RGB").getpixel((0, 0)))
    return colors


class GifPreviewFrameTests(unittest.TestCase):
    def test_multi_frame_gif_discards_preview_frame_before_sampling(self) -> None:
        colors = [(255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0)]
        blocks = encode_image_frames_bytes(
            make_gif(colors), ((4096, 95),), 2_000_000, 10
        )
        self.assertIsNotNone(blocks)
        sampled = decoded_colors(blocks)
        self.assertEqual(len(sampled), 3)
        self.assertNotEqual(sampled[0], colors[0])
        for actual, expected in zip(sampled, colors[1:], strict=True):
            for channel_actual, channel_expected in zip(actual, expected, strict=True):
                self.assertAlmostEqual(channel_actual, channel_expected, delta=5)

    def test_single_frame_gif_keeps_its_only_frame(self) -> None:
        color = (255, 0, 0)
        blocks = encode_image_frames_bytes(
            make_gif([color]), ((4096, 95),), 2_000_000, 10
        )
        self.assertIsNotNone(blocks)
        self.assertEqual(len(blocks), 1)
        actual = decoded_colors(blocks)[0]
        for channel_actual, channel_expected in zip(actual, color, strict=True):
            self.assertAlmostEqual(channel_actual, channel_expected, delta=5)


if __name__ == "__main__":
    unittest.main()
