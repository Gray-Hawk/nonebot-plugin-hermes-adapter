"""Runs 内联图片超限单轮降级：保留最新 10 张，被丢图片的召回标记不动。

生产故障（2026-11-12 14:07 群 253384307 m:71446）：窗口里有 12 张普通图，上游
``gateway/session_api_media.commit_api_images`` 对 ``data:image`` part 设
``_IMAGE_LIMIT=10``，13 张即 ``RuntimeStoreError('invalid_params')`` → POST /v1/runs
400 → 静默兜底、群里无提示。

口径（主人定案）：不拆轮、不改上游。在被动/主动出站的 user_content 完成后、提交
Runs 前把 ``data:image`` 内联 part 截到最多 10 个，按 parts 旧→新顺序保留**最后**
10 个（当前/最新媒体优先）；只移除超限的图片 part，文本、寻址标签
``[图 m:id#idx]`` / ``[表情包 m:id#idx]``、远程 URL part 与其他 part 一律不动。
纯文本、恰好 10 张走原路径（同一个对象原样返回，不记 WARNING）。

本文件用真实 ``build_reactive_user_content`` / ``build_passive_user_content`` 产出
content，再走真实 ``HermesClient.chat`` → 真实 ``_chat_via_runs``，只在 HTTP 客户端
处截获真正要 POST /v1/runs 的请求体。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from tests.test_runs_stream import _ClientChain, sse_lines  # noqa: E402

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402

ADAPTER = "onebotv11"
SELF = "2968545011"
GROUP = "253384307"


def data_image(i: int) -> str:
    """第 i 张图的内联形态；payload 长度不重要，只要像真 data URL。"""
    return f"data:image/png;base64,IMG{i:02d}"


def human(mid: int, *, group: str | None = GROUP) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id="10001", nickname="小光",
        content="看这些图", origin=MessageOrigin.human(), id=mid,
    )


def image_groups(mids):
    """按 handler 的入参形状造内联帧组：((kind, mid, idx), [帧...])。"""
    return [(("image", mid, 0), [data_image(i)]) for i, mid in enumerate(mids)]


def sticker_groups(mids, *, start: int = 0):
    return [(("sticker", mid, 0), [data_image(start + i)]) for i, mid in enumerate(mids)]


def inline_images(content) -> list[str]:
    """content 里 data:image 内联 part 的 url，按 parts 顺序。"""
    if not isinstance(content, list):
        return []
    out = []
    for part in content:
        if isinstance(part, dict) and part.get("type") == "image_url":
            url = (part.get("image_url") or {}).get("url", "")
            if isinstance(url, str) and url.startswith("data:image/"):
                out.append(url)
    return out


def part_texts(content) -> list[str]:
    if not isinstance(content, list):
        return []
    return [p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text"]


class _RunsSubmit:
    """真实 chat → 真实 _chat_via_runs，只在 HTTP 处截获 POST /v1/runs 的 body。"""

    def __init__(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.add_cleanup = tmp.cleanup
        self.tracker = RunTracker(Path(tmp.name) / "t.db")
        self.fake = _ClientChain(
            "run-inline-cap",
            sse_lines([{"event": "run.completed", "output": "§\n收到\n§", "seq": 0}]),
        )

    def close(self) -> None:
        self.tracker.close()
        self.add_cleanup()

    async def submit(self, content):
        from nonebot_plugin_hermes_adapter import mcp as mcp_module

        with patch.object(hc, "_get_http_client", return_value=self.fake), \
                patch.object(mcp_module, "run_tracker", self.tracker):
            await hc.hermes_client.chat(
                text="看这些图",
                session_key="s",
                user_id="10001",
                group_id=GROUP,
                adapter_name=ADAPTER,
                is_private=False,
                mode="reactive",
                expect_structured=True,
                structured_tool_name="submit_decision",
                system_prompt="SYS",
                user_content_override=content,
                target=SimpleNamespace(base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t"),
                bot_self_id=SELF,
            )
        self.assert_submitted_once()
        return self.fake.payloads[-1]

    def assert_submitted_once(self) -> None:
        assert len(self.fake.payloads) == 1, self.fake.payloads


class InlineImageCapTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)
        self.submit_ctx = _RunsSubmit()
        self.addCleanup(self.submit_ctx.close)

    def _built(self, image_urls=None, sticker_urls=None):
        return pb.build_reactive_user_content(
            addressed_to_bot=True,
            recent_messages=[],
            rows=[human(71429)],
            current_image_urls=image_urls or [],
            current_sticker_urls=sticker_urls or [],
            self_id=SELF, adapter=ADAPTER,
        ).content

    async def test_twelve_inline_images_keep_the_last_ten(self) -> None:
        mids = [71420 + i for i in range(12)]
        built = self._built(image_groups(mids))
        # 构造前提：真实 builder 确实吐出 12 个 data 图片 part（上游 _IMAGE_LIMIT=10 会拒）
        self.assertEqual(inline_images(built), [data_image(i) for i in range(12)])

        body = await self.submit_ctx.submit(built)
        content = body["input"][0]["content"]

        # 旧→新排列：保留最后 10 个，最早的两张被移除，顺序不变
        self.assertEqual(inline_images(content), [data_image(i) for i in range(2, 12)])
        self.assertLessEqual(len(inline_images(content)), 10)

    async def test_dropped_images_keep_recall_labels(self) -> None:
        """被移除的图片仍留 `[图 m:id#idx]` 文本标记：用户与模型可按 m:id 召回。"""
        mids = [71420 + i for i in range(12)]
        built = self._built(image_groups(mids))

        body = await self.submit_ctx.submit(built)
        content = body["input"][0]["content"]

        self.assertEqual(inline_images(content), [data_image(i) for i in range(2, 12)])
        labels = "\n".join(part_texts(content))
        for mid in mids:  # 含被丢的最早两张
            self.assertIn(f"[图 m:{mid}#0]", labels)
        # 文本 part 一个不少：12 个寻址标签 + 开头总文本块
        self.assertEqual(sum(1 for text in part_texts(content) if "[图 m:" in text), 12)

    async def test_over_limit_logs_counts_without_payload(self) -> None:
        """WARNING 只报丢弃/保留数量；不打印 data URL、图片字节或提示词正文。"""
        mids = [71420 + i for i in range(12)]
        built = self._built(image_groups(mids))

        with patch.object(hc.logger, "warning") as warn:
            body = await self.submit_ctx.submit(built)

        self.assertEqual(len(inline_images(body["input"][0]["content"])), 10)
        warn.assert_called_once()
        message = warn.call_args[0][0]
        self.assertIn("2", message)
        self.assertIn("10", message)
        for forbidden in ("data:image", "IMG", "看这些图", "base64"):
            self.assertNotIn(forbidden, message)

    async def test_exactly_ten_images_pass_through_untouched(self) -> None:
        mids = [71420 + i for i in range(10)]
        built = self._built(image_groups(mids))

        with patch.object(hc.logger, "warning") as warn:
            body = await self.submit_ctx.submit(built)

        content = body["input"][0]["content"]
        self.assertIs(content, built, "恰好 10 张不该被重建")
        self.assertEqual(len(inline_images(content)), 10)
        warn.assert_not_called()

    async def test_plain_text_passes_through_untouched(self) -> None:
        built = self._built()

        with patch.object(hc.logger, "warning") as warn:
            body = await self.submit_ctx.submit(built)

        self.assertIsInstance(built, str)
        self.assertEqual(body["input"][0]["content"], built)
        warn.assert_not_called()

    async def test_mixed_images_and_stickers_keep_the_last_ten(self) -> None:
        """普通图与表情包混合超限时按 parts 旧→新统一截断，两类寻址标签都保留。"""
        image_mids = [71420 + i for i in range(7)]
        sticker_mids = [71500 + i for i in range(6)]
        built = self._built(
            image_groups(image_mids), sticker_groups(sticker_mids, start=7)
        )
        self.assertEqual(len(inline_images(built)), 13)

        body = await self.submit_ctx.submit(built)
        content = body["input"][0]["content"]

        # 普通图 7 张在前、表情 6 张在后：保留最后 10 个 → 丢最早 3 张普通图
        self.assertEqual(
            inline_images(content), [data_image(i) for i in range(3, 13)]
        )
        labels = "\n".join(part_texts(content))
        for mid in image_mids:
            self.assertIn(f"[图 m:{mid}#0]", labels)
        for mid in sticker_mids:
            self.assertIn(f"[表情包 m:{mid}#0]", labels)

    async def test_remote_url_parts_are_not_counted_or_dropped(self) -> None:
        """远程 URL 图片 part 不受内联数量闸约束（上限只管 data: 内联字节）。"""
        mids = [71420 + i for i in range(12)]
        remote = [("image", 71900 + i, 0) for i in range(2)]
        groups = image_groups(mids) + [
            (addr, [f"https://example.invalid/{i}.png"]) for i, addr in enumerate(remote)
        ]
        built = self._built(groups)

        body = await self.submit_ctx.submit(built)
        content = body["input"][0]["content"]

        self.assertEqual(inline_images(content), [data_image(i) for i in range(2, 12)])
        urls = [
            (p.get("image_url") or {}).get("url", "")
            for p in content
            if isinstance(p, dict) and p.get("type") == "image_url"
        ]
        self.assertEqual(
            [u for u in urls if u.startswith("https://")],
            ["https://example.invalid/0.png", "https://example.invalid/1.png"],
        )


if __name__ == "__main__":
    unittest.main()
