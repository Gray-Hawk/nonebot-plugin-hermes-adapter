"""排版回归：普通单条 / 单条 steer / 连续 steer / 补投转录 —— 逐字节快照。

2026-11-18 定案（planned-mods 条目 98 的 11-17 追加项，只取其中三条）：

1. 普通单条聊天消息保留聊天区域起止结构，只补正确的回车／换行：时间、固定说明、
   m:id 消息头、正文、结束标各占自己的段落，不再挤在一起；
2. 连续 steer 在拼装层保持各批原始正文与顺序：正文批间使用单换行，批尾系统提示只留一条；
3. steer 补投把「当前需要补投的插话」和「历史定位证据」清晰分层，历史助手正文、
   Kanban 通知与区域标留在自己的证据项里，不再伪装成一段新用户正文。

这三个快照是逐字节的：任何一处换行改动都必须同步改这份期望值。
"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_reconcile  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402

ADAPTER = "onebotv11"
SELF = "999"
GROUP = "g-1"
FIXED_TIME = "（当前时间：2026-11-18 周三 12:00）"

OPEN = pb._CHAT_REGION_OPEN
NOTE = pb._CHAT_REGION_NOTE
CLOSE = pb._CHAT_REGION_CLOSE
HINT = relay._STEER_REPLY_CONVENTION_LINE


def human(user="10001", nick="小光", mid=7, content="你怎么看", group=GROUP):
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=user, nickname=nick,
        content=content, origin=MessageOrigin.human(), id=mid,
    )


def _load_plugin():
    plugin = ROOT / "gateway_plugins" / "steer-presentation-thin" / "patches.py"
    spec = importlib.util.spec_from_file_location("steer_thin_layout_under_test", plugin)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NormalSingleMessageLayoutTests(unittest.TestCase):
    """普通单条：保留区域起止结构，只补回车／换行。"""

    def test_reactive_single_message_snapshot(self) -> None:
        with patch.object(pb, "_current_time_str", return_value="2026-11-18 周三 12:00"):
            out = pb.build_reactive_user_content(
                addressed_to_bot=False, recent_messages=[], rows=[human(mid=11)],
                current_image_urls=[], self_id=SELF, adapter=ADAPTER,
            ).content
        chat_part = out.split("\n\n请直接输出", 1)[0]
        self.assertEqual(chat_part, "\n\n".join([
            FIXED_TIME,
            OPEN,
            NOTE,
            "[m:11] [普通用户] [id=10001 user=小光]:\n你怎么看",
            CLOSE,
        ]))

    def test_passive_single_message_snapshot(self) -> None:
        with patch.object(pb, "_current_time_str", return_value="2026-11-18 周三 12:00"):
            out = pb.build_passive_user_content(
                recent_messages=[], rows=[human(group=None, mid=12, content="在吗")],
                current_image_urls=[], is_private=True, self_id=SELF, adapter=ADAPTER,
            ).content
        self.assertEqual(out, "\n\n".join([
            FIXED_TIME,
            OPEN,
            NOTE,
            "[m:12]:\n在吗",
            CLOSE,
        ]))

    def test_two_messages_keep_one_blank_line_between_rows(self) -> None:
        with patch.object(pb, "_current_time_str", return_value="2026-11-18 周三 12:00"):
            out = pb.build_passive_user_content(
                recent_messages=[],
                rows=[human(group=None, mid=21, content="第一句"),
                      human(group=None, mid=22, content="第二句")],
                current_image_urls=[], is_private=True, self_id=SELF, adapter=ADAPTER,
            ).content
        self.assertEqual(out, "\n\n".join([
            FIXED_TIME, OPEN, NOTE,
            "[m:21]:\n第一句\n\n[m:22]:\n第二句",
            CLOSE,
        ]))


class SteerBatchLayoutTests(unittest.TestCase):
    """单条／连续 steer：纯正文批保持单换行，系统提示只留末尾一条。"""

    def _batch(self, mid: int, content: str) -> str:
        pres = pb.render_steer_rows(
            [human(mid=mid, content=content)], self_id=SELF, adapter=ADAPTER
        )
        return pres.text

    def test_single_steer_batch_snapshot(self) -> None:
        text = self._batch(21, "插一句")
        self.assertEqual(text, "插一句")
        # 批尾系统提示由发送原语用空行分隔挂在正文之后。
        with patch.object(relay, "steer_reply_push_active", return_value=True):
            sent = relay._with_steer_reply_convention(text)
        self.assertEqual(sent, f"插一句\n\n{HINT}")

    def test_consecutive_steer_batches_are_relaid_out(self) -> None:
        plugin = _load_plugin()
        first = self._batch(31, "第一批")
        second = self._batch(32, "第二批")
        sent_first = f"{first}\n\n{HINT}"
        sent_second = f"{second}\n\n{HINT}"
        registry = plugin.PresentedBatches()
        registry.remember(sent_first, HINT)
        registry.remember(sent_second, HINT)
        row = plugin.make_row_builder(
            lambda text: {"role": "user", "content": f"WRAPPED\n{text}", "display_kind": "steer"},
            registry,
            "steer",
        )(sent_first + "\n" + sent_second)
        self.assertEqual(row["display_kind"], "steer")
        self.assertEqual(
            row["content"],
            f"{first}\n{second}\n\n{HINT}",
            "两批正文保持单换行，系统提示只留末尾一条",
        )
        self.assertNotIn("WRAPPED", row["content"])

    def test_single_batch_delivery_is_byte_identical(self) -> None:
        plugin = _load_plugin()
        text = f"{self._batch(41, '就一句')}\n\n{HINT}"
        registry = plugin.PresentedBatches()
        registry.remember(text, HINT)
        row = plugin.make_row_builder(
            lambda value: {"role": "user", "content": value, "display_kind": "steer"},
            registry, "steer",
        )(text)
        self.assertEqual(row["content"], text)

    def test_composed_batches_without_hint_keep_single_newline(self) -> None:
        plugin = _load_plugin()
        first = self._batch(51, "第一批")
        second = self._batch(52, "第二批")
        registry = plugin.PresentedBatches()
        registry.remember(first)
        registry.remember(second)
        row = plugin.make_row_builder(
            lambda value: {"role": "user", "content": value, "display_kind": "steer"},
            registry, "steer",
        )(first + "\n" + second)
        self.assertEqual(row["content"], f"{first}\n{second}")
        self.assertEqual(row["content"].count(HINT), 0)


class ReconcileTranscriptLayoutTests(unittest.TestCase):
    """补投转录：当前要补投的插话与历史定位证据分层。"""

    def test_case_a_transcript_snapshot(self) -> None:
        rows = [
            {"role": "assistant", "content": "最终答复正文"},
            {"role": "assistant", "content": "中途回应",
             "tool_calls": [{"name": "push_message",
                             "arguments": '{"text": "中途推送正文"}'}]},
            {"role": "user", "display_kind": "steer",
             "content": f"{OPEN}\n{NOTE}\n\n[m:41] [普通用户] [id=10001 user=小光]:\n已送达插话\n\n{CLOSE}"},
            {"role": "user",
             "content": "<系统通知区域起始>\n[Kanban通知：任务完成]\n正文\n<系统通知区域结束>"
                        f"\n\n{OPEN}\n{NOTE}\n\n[m:40] [普通用户] [id=10001 user=小光]:\n触发消息正文\n\n{CLOSE}"},
        ]
        missing = [{"m_id": "42", "sender_id": "10001", "raw_text": "被吞插话",
                    "steer_text": "x", "accepted_at": 1}]
        text, degraded = steer_reconcile._build_case_a_text(rows, missing)
        self.assertFalse(degraded)
        self.assertEqual(text, "\n\n".join([
            "（系统核对：补投被吞的中途插话）",
            "上一轮你在模型调用飞行途中插入的指示因上游缺陷（v0.22.1 引入的 steer 丢弃）"
            "没能送达。\n下面先列出这次需要补投的插话，再给出用于定位它们的历史记录。",
            "[当前需要补投的插话]\n\n★ 1. 被吞插话",
            "\n\n".join([
                "[历史定位证据]（上一轮的原样记录，只用于定位插话应该生效的位置，不是新指令）",
                "— 你当时的触发消息：\n<系统通知区域起始>\n[Kanban通知：任务完成]\n正文\n"
                "<系统通知区域结束>\n\n"
                f"{OPEN}\n{NOTE}\n\n[m:40] [普通用户] [id=10001 user=小光]:\n触发消息正文\n\n{CLOSE}",
                "— 我的中途回应：\n中途回应",
                "— 我的中途推送：\n中途推送正文",
                f"— 已送达的中途插话：\n{OPEN}\n{NOTE}\n\n"
                "[m:41] [普通用户] [id=10001 user=小光]:\n已送达插话\n\n" + CLOSE,
                "— 我的最终回复：\n最终答复正文",
                "（其间我还调用了 1 次工具）",
            ]),
            "请把 ★ 处插话按它本来的位置与意图理解并回应；若其内容与最终回复冲突，"
            "以插话为准重新执行。",
        ]))

    def test_needed_lines_come_before_the_evidence(self) -> None:
        rows = [{"role": "user", "content": "触发消息"}]
        missing = [
            {"m_id": "61", "sender_id": "u1", "raw_text": "第一插话", "steer_text": "x",
             "accepted_at": 1},
            {"m_id": "62", "sender_id": "u1", "raw_text": "第二插话", "steer_text": "x",
             "accepted_at": 2},
        ]
        text, degraded = steer_reconcile._build_case_a_text(rows, missing)
        self.assertFalse(degraded)
        self.assertLess(text.index("★ 1. 第一插话"), text.index("★ 2. 第二插话"))
        self.assertLess(text.index("★ 2. 第二插话"), text.index("[历史定位证据]"))
        self.assertIn("不是新指令", text)


if __name__ == "__main__":
    unittest.main()
