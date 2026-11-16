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
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

import nonebot_plugin_alconna as alconna  # noqa: E402

from nonebot_plugin_hermes_adapter import mcp  # noqa: E402
from nonebot_plugin_hermes_adapter.core.file_config import FileConfig  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import (  # noqa: E402
    BufferedMessage,
    MessageBuffer,
)
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_media as media  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_perception as perception  # noqa: E402
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


class _CacheClearingTest(unittest.TestCase):
    """_extract_forward_full 有 60s 模块级缓存：每个用例从干净缓存起步。"""

    def setUp(self) -> None:
        media._forward_full_cache.clear()


class ForwardVideoTests(_CacheClearingTest):
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


SECOND_URL = "https://example.invalid/second.mp4"
THIRD_URL = "https://example.invalid/third.mp4"


def _video_seg(url: str | None, name: str = "clip.mp4") -> dict:
    data = {"file": name}
    if url:
        data["url"] = url
    return {"type": "video", "data": data}


class ForwardVideoPositionTests(_CacheClearingTest):
    """R1：块内标签严格按原 video 段位置；只有登记成功的段才递增文件 idx。"""

    def test_url_less_video_before_url_video_keeps_its_position(self) -> None:
        bot = _FakeBot(_forward_payload(_video_seg(None), _video_seg(SECOND_URL)))
        block, _images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_message_id=12
        )
        self.assertEqual([e["url"] for e in videos], [SECOND_URL])
        # 第一段无 url 只能 [视频]；ref 必须贴在第二段上，不能前移。
        self.assertIn("[视频][视频 m:12#f0]", block)

    def test_url_video_before_url_less_video_keeps_its_position(self) -> None:
        bot = _FakeBot(_forward_payload(_video_seg(SECOND_URL), _video_seg(None)))
        block, _images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_message_id=12
        )
        self.assertEqual([e["url"] for e in videos], [SECOND_URL])
        self.assertIn("[视频 m:12#f0][视频]", block)

    def test_interleaved_videos_number_only_registered_segments(self) -> None:
        bot = _FakeBot(
            _forward_payload(
                _video_seg(None), _video_seg(SECOND_URL), _video_seg(None), _video_seg(THIRD_URL)
            )
        )
        block, _images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_message_id=12
        )
        self.assertEqual([e["url"] for e in videos], [SECOND_URL, THIRD_URL])
        self.assertIn("[视频][视频 m:12#f0][视频][视频 m:12#f1]", block)


class ForwardVideoBudgetTests(_CacheClearingTest):
    """R2：转发视频与文件/普通视频共用剩余登记预算，超预算段保留普通 [视频]。"""

    def test_forward_video_respects_remaining_file_slots(self) -> None:
        bot = _FakeBot(_forward_payload(_video_seg(SECOND_URL), _video_seg(THIRD_URL)))
        block, _images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_message_id=12, file_slots=1
        )
        self.assertEqual([e["url"] for e in videos], [SECOND_URL])
        self.assertIn("[视频 m:12#f0][视频]", block)
        self.assertNotIn("#f1", block)

    def test_zero_file_slots_keep_plain_video_labels(self) -> None:
        bot = _FakeBot(_forward_payload(_video_seg(SECOND_URL)))
        block, _images, videos = _extract_forward(
            _forward_uni(), bot, number_base=0, file_message_id=12, file_slots=0
        )
        self.assertEqual(videos, [])
        self.assertIn("[视频]", block)
        self.assertNotIn("#f", block)


class IngressBudgetTests(_CacheClearingTest):
    """真实 handle_perception → MessageBuffer → 临时 SQLite 的总预算与占位回归。"""

    def setUp(self) -> None:
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self._seq = 0
        self.addCleanup(self._tmp.cleanup)

    def _ingest(self, uni, bot, *, max_files: int) -> dict:
        self._seq += 1
        store = MessageStore(self.root / f"ingress-{self._seq}.db")
        fetcher = MagicMock()
        buffer = MessageBuffer(store=store, fetcher=fetcher)
        event = SimpleNamespace(message=uni, message_id=101, get_user_id=lambda: "12345")
        target = SimpleNamespace(id="54321", private=False)

        with ExitStack() as stack:
            for obj, key, value in (
                (mcp, "message_buffer", buffer),
                (mcp, "bot_registry", MagicMock()),
                (alconna, "get_target", lambda: target),
                (
                    alconna.UniMessage,
                    "generate_without_reply",
                    lambda **kwargs: kwargs["event"].message,
                ),
                (perception, "get_adapter_name", lambda _adapter: "onebotv11"),
                (perception, "sync_buddy_to_group", lambda *args: None),
                (
                    media,
                    "load_file_config",
                    lambda: FileConfig(max_files_per_message=max_files),
                ),
            ):
                stack.enter_context(patch.object(obj, key, value))
            asyncio.run(perception.handle_perception(bot, event))
        rows = store.get_recent("onebotv11", "54321", 10)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        files = store.get_message_files([row.id]).get(row.id, [])
        result = {
            "id": row.id,
            "content": row.content,
            "files": files,
            "automatic_downloads": fetcher.submit.call_count,
        }
        store.close()
        return result

    def test_file_and_video_share_one_total_budget(self) -> None:
        uni = _msg(
            alconna.File(name="a.pdf", url="https://example.invalid/a.pdf"),
            alconna.Video(url=SECOND_URL),
        )
        result = self._ingest(uni, _FakeBot({}), max_files=1)
        self.assertEqual(len(result["files"]), 1)
        self.assertEqual(result["files"][0]["url"], "https://example.invalid/a.pdf")
        # 超预算视频不拿 ref，占位退回普通 [视频]，且排在 ref 之后（段序一致）。
        self.assertEqual(result["content"], f"[文件 m:{result['id']}#f0:a.pdf] [视频]")
        self.assertEqual(result["automatic_downloads"], 0)

    def test_overflow_plain_videos_keep_plain_placeholder(self) -> None:
        uni = _msg(
            alconna.Video(url=SECOND_URL),
            alconna.Video(url=THIRD_URL),
        )
        result = self._ingest(uni, _FakeBot({}), max_files=1)
        self.assertEqual(len(result["files"]), 1)
        self.assertEqual(result["files"][0]["url"], SECOND_URL)
        self.assertEqual(result["content"], f"[视频 m:{result['id']}#f0] [视频]")

    def test_forward_videos_share_remaining_budget(self) -> None:
        bot = _FakeBot(
            _forward_payload(_video_seg(SECOND_URL), _video_seg(THIRD_URL))
        )
        result = self._ingest(_msg(alconna.Reference(id="fwd-cap")), bot, max_files=1)
        self.assertEqual(len(result["files"]), 1)
        self.assertEqual(result["files"][0]["url"], SECOND_URL)
        self.assertTrue(result["content"].endswith(f"[视频 m:{result['id']}#f0]"))
        self.assertNotIn("#f1", result["content"])

    def test_video_without_url_only_message_still_stored(self) -> None:
        # 无 url 的视频只能出普通 [视频]；空内容闸门不能把这类消息丢掉。
        result = self._ingest(_msg(alconna.Video(id="tg-file-id")), _FakeBot({}), max_files=1)
        self.assertEqual(result["files"], [])
        self.assertEqual(result["content"], "[视频]")

    def test_ordinary_file_control_still_registers(self) -> None:
        uni = _msg(alconna.File(name="a.pdf", url="https://example.invalid/a.pdf"))
        result = self._ingest(uni, _FakeBot({}), max_files=1)
        self.assertEqual(len(result["files"]), 1)
        self.assertIn(f"[文件 m:{result['id']}#f0:a.pdf]", result["content"])

    def test_ingress_and_main_path_agree_on_forward_video_positions(self) -> None:
        """R1 真实链路：入库行的 url 与主路径块内 ref 位置必须指同一段。"""
        bot = _FakeBot(_forward_payload(_video_seg(None), _video_seg(SECOND_URL)))
        uni = _msg(alconna.Reference(id="fwd-position"))
        result = self._ingest(uni, bot, max_files=4)
        self.assertEqual([row["url"] for row in result["files"]], [SECOND_URL])
        self.assertEqual(result["files"][0]["idx"], 0)

        block, _images, videos = _extract_forward(
            _msg(alconna.Reference(id="fwd-position")),
            _FakeBot(_forward_payload(_video_seg(None), _video_seg(SECOND_URL))),
            number_base=0,
            file_message_id=result["id"],
        )
        self.assertEqual([e["url"] for e in videos], [SECOND_URL])
        self.assertIn(f"[视频][视频 m:{result['id']}#f0]", block)


if __name__ == "__main__":
    unittest.main()
