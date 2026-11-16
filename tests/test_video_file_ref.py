"""视频 m:id 接线：入站登记视频文件引用，占位复用现有文件 ref 与 get_message_files。

当前 handlers/message_media.py 对 alconna.Video / 合并转发 video 只写 ``[视频]``，
模型拿不到可寻址引用，也就没法用现有工具按 ref 取视频。本文件锁定最小接线：
- 视频段（普通消息与合并转发）登记成 message_files 行（复用 file_store 表与
  ``m:<mid>#f<idx>`` 编号，不新增视频专用下载工具）；
- 占位写成可寻址形态（``[视频 m:<mid>#f<idx>]``），拿不到文件引用时保留 ``[视频]``；
- 现有 ``get_message_files`` 按 ref 能拉到视频文件，普通文件回归不变；
- 不自动下载、不自动抽帧。
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

import nonebot_plugin_alconna as alconna  # noqa: E402

from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers.message_media import (  # noqa: E402
    _collect_file_placeholders,
    _collect_nontext_placeholders,
    _extract_file_entries,
    _extract_forward_full,
)
from nonebot_plugin_hermes_adapter.mcp.tools.get_message_files import (  # noqa: E402
    FileRef,
    GetMessageFilesInput,
    get_message_files_impl,
)

VIDEO_URL = "https://example.invalid/clip.mp4"


def _msg(*segments) -> alconna.UniMessage:
    return alconna.UniMessage(list(segments))


class VideoEntryTests(unittest.TestCase):
    def test_video_segment_registers_file_ref(self) -> None:
        entries = _extract_file_entries(_msg(alconna.Video(url=VIDEO_URL)))
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["kind"], "video")
        self.assertEqual(entry["url"], VIDEO_URL)
        self.assertEqual(entry["name"], "video.mp4")
        self.assertEqual(_collect_file_placeholders(entries, 7), ["[视频 m:7#f0]"])

    def test_video_without_url_keeps_plain_placeholder(self) -> None:
        msg = _msg(alconna.Video(id="telegram-file-id"))
        self.assertEqual(_extract_file_entries(msg), [])
        # 拿不到可下载引用：普通消息与触发轮都保留旧的 [视频] 占位。
        self.assertEqual(
            _collect_nontext_placeholders(msg, include_files=False), ["[视频]"]
        )
        self.assertEqual(_collect_nontext_placeholders(msg), ["[视频]"])

    def test_file_then_video_share_one_index_space(self) -> None:
        msg = _msg(
            alconna.File(name="报告.pdf", url="https://example.invalid/r.pdf"),
            alconna.Video(url=VIDEO_URL),
        )
        entries = _extract_file_entries(msg)
        self.assertEqual([e["kind"] for e in entries], ["file", "video"])
        self.assertEqual(
            _collect_file_placeholders(entries, 9),
            ["[文件 m:9#f0:报告.pdf]", "[视频 m:9#f1]"],
        )
        # 触发轮正文：视频已可寻址，不再退化成裸 [视频]。
        placeholders = _collect_nontext_placeholders(msg, file_message_id=9)
        self.assertIn("[文件 m:9#f0:报告.pdf]", placeholders)
        self.assertIn("[视频 m:9#f1]", placeholders)
        self.assertNotIn("[视频]", placeholders)

    def test_missing_message_id_falls_back_to_plain_labels(self) -> None:
        msg = _msg(
            alconna.File(name="报告.pdf", url="https://example.invalid/r.pdf"),
            alconna.Video(url=VIDEO_URL),
        )
        entries = _extract_file_entries(msg)
        self.assertEqual(
            _collect_file_placeholders(entries, None), ["[文件:报告.pdf]", "[视频]"]
        )


class _FakeBot:
    def __init__(self, payload: dict) -> None:
        self.self_id = "999"
        self.payload = payload
        self.calls: list[tuple[str, dict]] = []

    async def call_api(self, name: str, **kwargs):
        self.calls.append((name, kwargs))
        return self.payload


def _forward_uni():
    return _msg(alconna.Reference(id="fwd-1"))


def _forward_payload(*segments) -> dict:
    return {
        "messages": [
            {"sender": {"nickname": "阿光"}, "content": list(segments)},
        ]
    }


def _extract_forward(uni, bot, **kwargs):
    return asyncio.run(
        _extract_forward_full(uni, bot, adapter_name="onebotv11", **kwargs)
    )


class ForwardVideoTests(unittest.TestCase):
    def test_forward_video_registers_entry_and_addressable_label(self) -> None:
        bot = _FakeBot(
            _forward_payload(
                {"type": "text", "data": {"text": "看这个"}},
                {"type": "video", "data": {"file": "fv.mp4", "url": VIDEO_URL}},
            )
        )
        block, images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_message_id=12, file_idx_base=2
        )
        self.assertEqual(images, [])
        self.assertEqual(len(videos), 1)
        self.assertEqual(videos[0]["kind"], "video")
        self.assertEqual(videos[0]["url"], VIDEO_URL)
        # 主路径知道本条 m:id，块内直接给可寻址占位。
        self.assertIn("[视频 m:12#f2]", block)

    def test_forward_video_without_mid_keeps_plain_placeholder(self) -> None:
        bot = _FakeBot(
            _forward_payload({"type": "video", "data": {"url": VIDEO_URL}})
        )
        block, _images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_idx_base=2
        )
        self.assertEqual(len(videos), 1)
        # 入库侧（perception）拿不到 m:id：块内保留普通 [视频]，引用在落库后补。
        self.assertIn("[视频]", block)
        self.assertNotIn("#f", block)

    def test_forward_video_without_url_keeps_plain_placeholder(self) -> None:
        bot = _FakeBot(_forward_payload({"type": "video", "data": {"file": "x.mp4"}}))
        block, _images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_message_id=12
        )
        self.assertEqual(videos, [])
        self.assertIn("[视频]", block)


class _Downloader:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls: list[tuple[str, str]] = []

    async def download(self, url: str, name: str, expected_size: int = 0):
        self.calls.append((url, name))
        path = self.root / name
        path.write_bytes(b"video-bytes")
        return SimpleNamespace(sha256="sha-video", path=path, size=len(b"video-bytes"))


class _Cache:
    def get_path(self, sha):
        return None

    def get_bytes(self, sha):  # pragma: no cover - 依赖磁盘路径即可
        return None


class VideoFetchTests(unittest.TestCase):
    """入站登记 → get_message_files 按 ref 拉取的成功路径与普通文件回归。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = MessageStore(self.root / "messages.db")
        self.addCleanup(self._tmp.cleanup)

    def _ingest(self) -> tuple[int, list[dict]]:
        entries = _extract_file_entries(
            _msg(
                alconna.File(name="报告.pdf", url="https://example.invalid/r.pdf"),
                alconna.Video(url=VIDEO_URL),
            )
        )
        msg = BufferedMessage(
            ts=1, adapter="onebotv11", group_id="g1", user_id="u1",
            nickname="小光", content="看这个",
            origin=MessageOrigin.human(), file_entries=entries,
        )
        message_id = self.store.append(msg)
        self.assertIsNotNone(message_id)
        return message_id, self.store.get_message_files([message_id])[message_id]

    def test_video_metadata_is_registered_with_real_ref(self) -> None:
        message_id, rows = self._ingest()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["idx"], 1)
        self.assertEqual(rows[1]["url"], VIDEO_URL)
        self.assertIsNone(rows[1]["local_path"])
        self.assertEqual(rows[1]["status"], "pending")

    def test_get_message_files_fetches_video_and_file(self) -> None:
        message_id, _rows = self._ingest()
        downloader = _Downloader(self.root)
        result = asyncio.run(
            get_message_files_impl(
                GetMessageFilesInput(
                    refs=[
                        FileRef(message_id=message_id, idx=1),
                        FileRef(message_id=message_id, idx=0),
                    ]
                ),
                store=self.store,
                cache=_Cache(),
                downloader=downloader,
                bot_resolver=lambda *a, **kw: None,
                scope=CallerScope.uniform(),
            )
        )
        video, document = result.results
        self.assertTrue(video.available, video.error)
        self.assertEqual(video.file_name, "video.mp4")
        self.assertTrue(Path(video.local_path).is_file())
        self.assertEqual(video.sha256, "sha-video")
        # 普通文件回归：同一编号空间里的下一个 idx 仍按原逻辑可取。
        self.assertTrue(document.available, document.error)
        self.assertEqual(document.file_name, "报告.pdf")
        self.assertEqual([name for _url, name in downloader.calls], ["video.mp4", "报告.pdf"])

    def test_ingress_composition_registers_forward_video_after_own_entries(self) -> None:
        """按 perception 的组装顺序：own(文件+视频) → 转发视频接排 → 落库后出占位。"""
        uni = _msg(
            alconna.File(name="报告.pdf", url="https://example.invalid/r.pdf"),
            alconna.Video(url=VIDEO_URL),
            alconna.Reference(id="fwd-2"),
        )
        bot = _FakeBot(
            _forward_payload({"type": "video", "data": {"url": "https://example.invalid/fv.mp4"}})
        )
        entries = _extract_file_entries(uni)
        _block, _images, forward_videos = _extract_forward(
            uni, bot, number_base=0, file_idx_base=len(entries)
        )
        entries.extend(forward_videos)
        message_id = self.store.append(
            BufferedMessage(
                ts=2, adapter="onebotv11", group_id="g1", user_id="u1",
                nickname="小光", content="看这个", origin=MessageOrigin.human(),
                file_entries=entries,
            )
        )
        labels = _collect_file_placeholders(entries, message_id)
        self.assertEqual(
            labels,
            ["[文件 m:%d#f0:报告.pdf]" % message_id,
             "[视频 m:%d#f1]" % message_id,
             "[视频 m:%d#f2]" % message_id],
        )
        downloader = _Downloader(self.root)
        fetched = asyncio.run(
            get_message_files_impl(
                GetMessageFilesInput(refs=[FileRef(message_id=message_id, idx=2)]),
                store=self.store, cache=_Cache(), downloader=downloader,
                bot_resolver=lambda *a, **kw: None, scope=CallerScope.uniform(),
            )
        )
        self.assertTrue(fetched.results[0].available, fetched.results[0].error)
        self.assertEqual(fetched.results[0].file_name, "video.mp4")

    def test_cached_video_does_not_download_again(self) -> None:
        message_id, _rows = self._ingest()
        downloader = _Downloader(self.root)
        first = asyncio.run(
            get_message_files_impl(
                GetMessageFilesInput(refs=[FileRef(message_id=message_id, idx=1)]),
                store=self.store, cache=_Cache(), downloader=downloader,
                bot_resolver=lambda *a, **kw: None, scope=CallerScope.uniform(),
            )
        )
        self.assertTrue(first.results[0].available)
        second = asyncio.run(
            get_message_files_impl(
                GetMessageFilesInput(refs=[FileRef(message_id=message_id, idx=1)]),
                store=self.store, cache=_Cache(), downloader=downloader,
                bot_resolver=lambda *a, **kw: None, scope=CallerScope.uniform(),
            )
        )
        self.assertTrue(second.results[0].available)
        self.assertEqual(len(downloader.calls), 1)


if __name__ == "__main__":
    unittest.main()
