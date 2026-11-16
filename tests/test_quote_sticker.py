"""引用媒体接线：纯表情引用也要有可呈递内容 `(引用表情#k)`。

事故面：``_RepliedMedia`` 只有 ``image_urls``，``_extract_replied_media`` 漏抽
``Image.sticker=True``，于是「引用了一条纯表情消息」在 ``_compose_quote_prefix`` 里
得到空内容 → 返回空前缀 → 模型完全不知道被引的是什么。

本文件锁定：
- 引用表情与引用图分开抽、分开入库（kind='quote_sticker'），idx 续排在引用图之后；
- 呈递标记是 `(引用表情#k)`，号与 message_images 的全局 idx 一致；
- 纯表情、文本+表情、普通图片、混合图片+表情四种引用都有内容；
- 历史入库（perception 构造 → MessageStore.append → get_recent）与主路径同一份枚举序。
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests import _bootstrap  # noqa: F401

import nonebot_plugin_alconna as alconna  # noqa: E402

from nonebot_plugin_hermes_adapter.core.message_buffer import (  # noqa: E402
    BufferedMessage,
    MessageBuffer,
    iter_message_media,
    render_media_summary,
)
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_media as media  # noqa: E402

STICKER_URL = "https://example.invalid/quote-sticker.png"
IMAGE_URL = "https://example.invalid/quote-image.png"


def _replied(**kwargs) -> media._RepliedMedia:
    return media._RepliedMedia(**kwargs)


class RepliedStickerExtractionTests(unittest.TestCase):
    def _extract(self, uni):
        async def _generate(*_args, **_kwargs):
            return uni

        with patch.object(media.alconna.UniMessage, "generate", new=_generate):
            return asyncio.run(
                media._extract_replied_media(
                    SimpleNamespace(
                        reply=SimpleNamespace(message=uni, message_id=41),
                    ),
                    bot=SimpleNamespace(self_id="999"),
                    adapter_name="onebotv11",
                )
            )

    def test_sticker_only_reply_is_extracted(self) -> None:
        """纯表情引用：sticker 单独抽出，image_urls 仍为空。"""
        replied = self._extract(
            alconna.UniMessage(alconna.Image(url=STICKER_URL, sticker=True))
        )
        self.assertEqual(replied.sticker_urls, [STICKER_URL])
        self.assertEqual(replied.image_urls, [])
        self.assertEqual(replied.msg_id, 41)

    def test_sticker_and_text_reply_keeps_both(self) -> None:
        replied = self._extract(
            alconna.UniMessage(
                [alconna.Text("看这个"), alconna.Image(url=STICKER_URL, sticker=True)]
            )
        )
        self.assertEqual(replied.text, "看这个")
        self.assertEqual(replied.sticker_urls, [STICKER_URL])
        self.assertEqual(replied.image_urls, [])


class QuotePrefixStickerTests(unittest.TestCase):
    def test_pure_sticker_reference_has_a_prefix(self) -> None:
        prefix = media._compose_quote_prefix(_replied(sticker_urls=[STICKER_URL]), 0)
        self.assertEqual(prefix, "(引用: (引用表情#0))")

    def test_text_plus_sticker_reference(self) -> None:
        prefix = media._compose_quote_prefix(
            _replied(sender_label="小光(1001)", text="看这个", sticker_urls=[STICKER_URL]), 0
        )
        self.assertEqual(prefix, "(引用 小光(1001): 看这个 (引用表情#0))")

    def test_image_reference_unchanged(self) -> None:
        prefix = media._compose_quote_prefix(_replied(image_urls=[IMAGE_URL]), 0)
        self.assertEqual(prefix, "(引用: (引用图#0))")

    def test_mixed_image_and_sticker_reference_keeps_global_idx(self) -> None:
        # quote_base = 本条消息 own+sticker 数：引用图紧随其后，引用表情再接排。
        prefix = media._compose_quote_prefix(
            _replied(image_urls=[IMAGE_URL], sticker_urls=[STICKER_URL]), 2
        )
        self.assertEqual(prefix, "(引用: (引用图#2) (引用表情#3))")

    def test_no_media_reference_still_returns_empty_prefix(self) -> None:
        self.assertEqual(media._compose_quote_prefix(_replied(), 0), "")


class QuoteStickerStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.store = MessageStore(self.root / "messages.db")
        self.addCleanup(self.store.close)
        self.fetcher = MagicMock()
        self.buffer = MessageBuffer(store=self.store, fetcher=self.fetcher)

    def _append(self, **kwargs) -> BufferedMessage:
        msg = BufferedMessage(
            ts=1, adapter="onebotv11", group_id="g1", user_id="u1", nickname="小光",
            content="看这个", origin=MessageOrigin.human(), **kwargs,
        )
        self.buffer.append(msg)
        return msg

    def test_enumeration_order_puts_quote_sticker_after_quote_image(self) -> None:
        msg = BufferedMessage(
            ts=1, adapter="onebotv11", group_id="g1", user_id="u1", nickname="小光",
            content="x", origin=MessageOrigin.human(),
            image_urls=["https://example.invalid/own.png"],
            sticker_urls=["https://example.invalid/own-sticker.png"],
            quote_image_urls=[IMAGE_URL],
            quote_sticker_urls=[STICKER_URL],
            forward_image_urls=["https://example.invalid/fwd.png"],
        )
        self.assertEqual(
            [kind for kind, _url in iter_message_media(msg)],
            ["image", "sticker", "quote", "quote_sticker", "forward_image"],
        )
        self.assertEqual(
            render_media_summary(msg),
            "(图#0 表情#1 引用#2 引用表情#3 转发#4)",
        )

    def test_historical_row_roundtrip_keeps_kind_and_index(self) -> None:
        self._append(quote_image_urls=[IMAGE_URL], quote_sticker_urls=[STICKER_URL])
        rows = self.buffer.get_recent("onebotv11", "g1", 10)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.quote_image_urls, [IMAGE_URL])
        self.assertEqual(row.quote_sticker_urls, [STICKER_URL])
        meta = self.store.get_message_images_meta([row.id])[row.id]
        self.assertEqual([(m["kind"], m["idx"]) for m in meta], [("quote", 0), ("quote_sticker", 1)])
        self.assertEqual(render_media_summary(row), "(引用#0 引用表情#1)")

    def test_get_media_urls_exposes_quote_sticker_bucket(self) -> None:
        msg = self._append(quote_sticker_urls=[STICKER_URL])
        self.assertEqual(
            self.buffer.get_media_urls(msg.id)["quote_sticker"], [STICKER_URL]
        )

    def test_fetcher_gets_quote_sticker_offsets(self) -> None:
        self._append(
            image_urls=["https://example.invalid/own.png"],
            quote_image_urls=[IMAGE_URL],
            quote_sticker_urls=[STICKER_URL],
        )
        calls = [
            (c.kwargs["start_index"], c.args[1])
            for c in self.fetcher.submit.call_args_list
        ]
        # 枚举序不变量：own(0) → quote(1) → quote_sticker(2)，每段都带自己的 start_index。
        self.assertEqual(calls, [(0, ["https://example.invalid/own.png"]),
                                 (1, [IMAGE_URL]),
                                 (2, [STICKER_URL])])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
