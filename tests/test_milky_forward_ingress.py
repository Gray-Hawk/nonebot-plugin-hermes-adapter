"""QQ-03：Milky 合并转发入站——不为空、可入库、可触发。

真实 Milky ``Bot``（只替换线形 I/O）与真实 alconna ``Reference`` 结构：
  - ``forward_id`` 形态走 ``get_forwarded_messages``；
  - 纯节点形态（alconna 已带 ``CustomNode``）直接渲染，不发 API；
  - 取不到内容/协议未支持时给**明确占位**，绝不再返回 None 让空消息闸门吞消息。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests._qqtpc_fixtures import MilkyHarness  # noqa: E402

import nonebot_plugin_alconna as alconna  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_media as media  # noqa: E402


class _RecorderBot:
    self_id = "999"

    def __init__(self, payload=None) -> None:
        self.payload = payload or {"messages": []}
        self.calls: list[tuple[str, dict]] = []

    async def call_api(self, action, **kwargs):
        self.calls.append((action, kwargs))
        return self.payload


def _uni(*segments) -> alconna.UniMessage:
    msg = alconna.UniMessage()
    for seg in segments:
        msg.append(seg)
    return msg


def _milky_node(sender: str, *segments) -> dict:
    return {
        "sender_name": sender,
        "avatar_url": "",
        "time": 1795538642,
        "segments": list(segments),
        "message_seq": None,
    }


def _text(value: str) -> dict:
    return {"type": "text", "data": {"text": value}}


class MilkyForwardIngressTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        media._forward_full_cache.clear()
        self.addCleanup(media._forward_full_cache.clear)

    async def test_forward_id_expands_through_native_api(self) -> None:
        harness = MilkyHarness()
        harness.replies["get_forwarded_messages"] = {
            "messages": [
                _milky_node("老张", _text("第一条")),
                _milky_node(
                    "小李",
                    {"type": "mention", "data": {"user_id": 10001, "name": "阿光"}},
                    _text(" 看这个"),
                    {
                        "type": "image",
                        "data": {
                            "resource_id": "r1", "temp_url": "https://example.invalid/a.png",
                            "width": 1, "height": 1, "sub_type": "normal",
                        },
                    },
                ),
            ]
        }

        block, images, videos = await media._extract_forward_full(
            _uni(alconna.Reference(id="fwd-1")), harness.bot,
            adapter_name="milky", number_base=0,
        )

        self.assertEqual(harness.actions(), ["get_forwarded_messages"])
        self.assertEqual(harness.params_for("get_forwarded_messages")[0]["forward_id"], "fwd-1")
        self.assertIsNotNone(block, "纯转发不得被判为空消息")
        self.assertIn('count="2"', block)
        self.assertIn("老张: 第一条", block)
        self.assertIn("@10001", block)
        self.assertIn("[转发图#0]", block)
        self.assertEqual(images, ["https://example.invalid/a.png"])
        self.assertEqual(videos, [])
        # 入库摘要必须非空（perception 用 _summarize_forward 存 content）。
        summary = media._summarize_forward(block)
        self.assertIn("<forwarded_messages", summary)
        self.assertNotIn("\n", summary)

    async def test_children_only_reference_renders_without_api_call(self) -> None:
        harness = MilkyHarness()
        ref = alconna.Reference(
            nodes=[
                alconna.CustomNode(
                    "10001", "阿光", alconna.UniMessage([alconna.Text("你好")])
                )
            ]
        )

        block, _images, _videos = await media._extract_forward_full(
            _uni(ref), harness.bot, adapter_name="milky",
        )

        self.assertEqual(harness.actions(), [], "alconna 已带节点时不得再打平台 API")
        self.assertIsNotNone(block)
        self.assertIn("阿光: 你好", block)

    async def test_plain_message_has_no_forward_failure_placeholder(self) -> None:
        harness = MilkyHarness()

        block, images, videos = await media._extract_forward_full(
            _uni(alconna.Text("普通文本")), harness.bot, adapter_name="milky",
        )

        self.assertIsNone(block)
        self.assertEqual(images, [])
        self.assertEqual(videos, [])
        self.assertEqual(harness.actions(), [])

    async def test_fetch_failure_yields_explicit_placeholder(self) -> None:
        harness = MilkyHarness()
        harness.failures["get_forwarded_messages"] = {
            "status": "failed", "retcode": 1, "message": "boom",
        }

        block, images, videos = await media._extract_forward_full(
            _uni(alconna.Reference(id="fwd-2")), harness.bot, adapter_name="milky",
        )

        self.assertIsNotNone(block)
        self.assertIn('status="fetch_failed"', block)
        self.assertEqual(images, [])
        self.assertEqual(videos, [])

    async def test_unknown_adapter_gets_unsupported_placeholder(self) -> None:
        bot = _RecorderBot()

        block, _images, _videos = await media._extract_forward_full(
            _uni(alconna.Reference(id="fwd-3")), bot, adapter_name="astrbot",
        )

        self.assertIsNotNone(block, "不支持展开也不能返回 None（否则整条消息被吞）")
        self.assertIn('status="unsupported"', block)
        self.assertEqual(bot.calls, [])

    async def test_onebot_forward_path_is_unchanged(self) -> None:
        bot = _RecorderBot(
            {"messages": [{"sender": {"nickname": "阿光"},
                           "content": [{"type": "text", "data": {"text": "旧路"}}]}]}
        )

        block, _images, _videos = await media._extract_forward_full(
            _uni(alconna.Reference(id="123")), bot, adapter_name="onebotv11",
        )

        self.assertEqual(bot.calls, [("get_forward_msg", {"id": "123"})])
        self.assertIn("阿光: 旧路", block)


if __name__ == "__main__":
    unittest.main()
