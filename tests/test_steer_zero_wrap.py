"""正文零包装第一阶段：steer 呈递同样只发正文，台账身份不依赖行头。

覆盖：
  1. steer 整批文本 = 各行正文，按原顺序单换行拼接，不再有区域标／固定说明／m:id 行头；
  2. ``SteerRowPresentation.source_row`` 继续承载身份，``rendered_line`` 是实发文本的连续子串；
  3. 对账按台账既有的 ``steer_text`` 命中，不再从转录行首找 m:id；
  4. 呈递标记改为 ``plain_body``，网关薄插件与 NB 逐字同源。
"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_presentation  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_reconcile  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402

ADAPTER = "onebotv11"
SELF = "999"
GROUP = "g-1"


def human(user="10001", nick="小光", *, mid=41, content="插一句", images=()) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=GROUP, user_id=user, nickname=nick,
        content=content, origin=MessageOrigin.human(), id=mid, image_urls=list(images),
    )


def _load_plugin():
    plugin = ROOT / "gateway_plugins" / "steer-presentation-thin" / "patches.py"
    spec = importlib.util.spec_from_file_location("steer_thin_zero_wrap", plugin)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SteerPresentationTests(unittest.TestCase):
    def test_single_row_is_body_only(self) -> None:
        presentation = pb.render_steer_rows(
            [human(mid=41, content="插一句")], self_id=SELF, adapter=ADAPTER, is_private=False,
        )
        self.assertEqual(presentation.text, "插一句")
        self.assertEqual(presentation.rows[0].source_row.id, 41)
        self.assertEqual(presentation.rows[0].rendered_line, "插一句")
        for dead in ("<聊天消息区域", "聊天消息区域仅含", "[m:41]", "[id=10001", "有人@我"):
            self.assertNotIn(dead, presentation.text)

    def test_multiple_rows_preserve_order_with_single_newlines(self) -> None:
        presentation = pb.render_steer_rows(
            [human(mid=41, content="第一句\n续行"), human(mid=42, content="第二句")],
            self_id=SELF, adapter=ADAPTER, is_private=False,
        )
        self.assertEqual(presentation.text, "第一句\n续行\n第二句")
        self.assertNotIn("\n\n", presentation.text)
        for item in presentation.rows:
            self.assertIn(item.rendered_line, presentation.text)

    def test_no_batch_decorations_left(self) -> None:
        self.assertEqual(pb.steer_batch_decorations(), ())

    def test_media_is_semantic_placeholder_without_addressing(self) -> None:
        presentation = pb.render_steer_rows(
            [human(mid=43, content="", images=["data:image/png;base64,IMG"])],
            self_id=SELF, adapter=ADAPTER, is_private=False,
        )
        self.assertEqual(presentation.text, "[图片]")
        self.assertNotIn("m:43", presentation.text)
        self.assertNotIn("get_message_images", presentation.text)


class SteerReconcileTests(unittest.TestCase):
    def test_hit_uses_ledger_text_not_m_id(self) -> None:
        rows = [{"role": "user", "content": "插一句", "display_kind": "steer"}]
        self.assertTrue(steer_reconcile._steer_text_present("插一句", rows))
        self.assertFalse(steer_reconcile._steer_text_present("没送达的话", rows))

    def test_empty_ledger_text_has_no_searchable_anchor(self) -> None:
        rows = [{"role": "user", "content": "", "display_kind": "steer"}]
        self.assertFalse(steer_reconcile._steer_text_present("", rows))


class PresentationContractTests(unittest.TestCase):
    def test_format_is_plain_body_on_both_sides(self) -> None:
        plugin = _load_plugin()
        self.assertEqual(steer_presentation.PRESENTATION_FORMAT, "plain_body")
        self.assertEqual(plugin.PRESENTATION_FORMAT, steer_presentation.PRESENTATION_FORMAT)
        marker = steer_presentation.presentation_marker()
        self.assertTrue(steer_presentation.declares_plain_body(marker))
        self.assertTrue(plugin.declares_plain_body(marker))
        self.assertFalse(plugin.declares_plain_body({"schema": "hermes.steer.presentation/1",
                                                     "format": "chat_region"}))

    def test_gateway_note_describes_plain_row(self) -> None:
        plugin = _load_plugin()
        note = plugin.make_channel_note("ORIGINAL")
        self.assertTrue(note.startswith("ORIGINAL"))
        self.assertIn("plain user row", note)
        self.assertNotIn("chat-message region block", note)


if __name__ == "__main__":
    unittest.main()
