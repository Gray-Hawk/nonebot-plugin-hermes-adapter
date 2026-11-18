"""正文内联 @：模型只写 ``@QQ号``，插件在原位置转成 At 段。

协议（2026-11-18 定案）：正文里只认 ``@`` 紧跟的纯数字 QQ（5～12 位、首位非 0）。
昵称、``@all``／``@全体成员``、邮箱样式与任何非数字形态都不转换，原样当普通文字。
旧的 ``[CQ:at,qq=…]`` 写法**不再识别**；戳一戳也不再是正文标记（见
``tests/test_poke_tool.py``：poke 只能通过 ``poke_user`` 工具单独使用）。
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401

import nonebot_plugin_alconna as alconna  # noqa: E402

from nonebot_plugin_hermes_adapter.core import outbound  # noqa: E402


def _text_parts(dump: list[dict]) -> str:
    return "".join(part.get("text", "") for part in dump if part.get("type") == "text")


class _Harness:
    """只替换统一出站出口，记录每次真正要发出去的 UniMessage。"""

    def __init__(self) -> None:
        self.sent: list[list[dict]] = []

    async def _send_unimsg(self, uni, *, bot=None, target=None, delivery=None):
        self.sent.append(uni.dump())
        return [str(1000 + len(self.sent))]

    def send(self, text: str, *, private: bool = False, media_urls=(),
             adapter_name: str = "onebotv11"):
        target = SimpleNamespace(private=private, id=("12345" if private else "54321"))
        bot = SimpleNamespace(self_id="999", call_api=self._forbidden_api)
        with patch.object(outbound, "_send_unimsg", new=self._send_unimsg):
            return asyncio.run(
                outbound.send_text_with_media(
                    bot=bot, target=target, text=text,
                    media_urls=list(media_urls), adapter_name=adapter_name,
                )
            )

    async def _forbidden_api(self, api: str, **kwargs):  # pragma: no cover - 守卫
        raise AssertionError(f"正文路径不调平台动作: {api} {kwargs}")


def _ats(dump: list[dict]) -> list[str]:
    return [p.get("target") for p in dump if p.get("type") == "at"]


class MentionPositionTests(unittest.TestCase):
    def test_at_at_body_start(self) -> None:
        h = _Harness()
        result = h.send("@10001 你看")
        self.assertTrue(result.ok)
        dump = h.sent[0]
        self.assertEqual(_ats(dump), ["10001"])
        self.assertEqual(dump[0]["type"], "at")
        self.assertEqual(_text_parts(dump), " 你看")

    def test_at_in_the_middle(self) -> None:
        h = _Harness()
        h.send("你好 @10001 在吗")
        dump = h.sent[0]
        self.assertEqual(_ats(dump), ["10001"])
        self.assertEqual([p["type"] for p in dump], ["text", "at", "text"])
        self.assertEqual(_text_parts(dump), "你好  在吗")

    def test_at_at_body_end(self) -> None:
        h = _Harness()
        h.send("你过来 @10001")
        dump = h.sent[0]
        self.assertEqual([p["type"] for p in dump], ["text", "at"])
        self.assertEqual(_text_parts(dump), "你过来 ")

    def test_multiple_mentions_keep_order(self) -> None:
        h = _Harness()
        h.send("@10001 和 @10002 都看下，@10003 也来")
        dump = h.sent[0]
        self.assertEqual(_ats(dump), ["10001", "10002", "10003"])
        self.assertEqual([p["type"] for p in dump],
                         ["at", "text", "at", "text", "at", "text"])

    def test_adjacent_mentions_need_a_separator(self) -> None:
        """`@10001@10002` 紧贴时只认前一个：@ 前面紧跟数字/字母属于邮箱样式，不转换。"""
        self.assertEqual(
            outbound._parse_inline_markers("@10001@10002"),
            [outbound._InlineAt("10001"), "@10002"],
        )
        self.assertEqual(
            outbound._parse_inline_markers("@10001 @10002"),
            [outbound._InlineAt("10001"), " ", outbound._InlineAt("10002")],
        )


class NotAMentionTests(unittest.TestCase):
    def test_nickname_at_stays_text(self) -> None:
        h = _Harness()
        h.send("@小光 你看")
        self.assertEqual(_ats(h.sent[0]), [])
        self.assertEqual(_text_parts(h.sent[0]), "@小光 你看")

    def test_at_all_and_everyone_stay_text(self) -> None:
        for text in ("@all 集合", "@全体成员 集合"):
            with self.subTest(text=text):
                h = _Harness()
                h.send(text)
                self.assertEqual(_ats(h.sent[0]), [])
                self.assertEqual(_text_parts(h.sent[0]), text)

    def test_plain_and_email_like_at_stay_text(self) -> None:
        for text in ("@ 空格后面没数字", "mail me at a@10001.example", "@10001abc"):
            with self.subTest(text=text):
                h = _Harness()
                h.send(text)
                self.assertEqual(_ats(h.sent[0]), [], text)
                self.assertEqual(_text_parts(h.sent[0]), text)

    def test_out_of_range_digit_runs_stay_text(self) -> None:
        for text in ("@0 集合", "@1234 太短", "@100011000110001 太长", "@00012345 前导零"):
            with self.subTest(text=text):
                h = _Harness()
                h.send(text)
                self.assertEqual(_ats(h.sent[0]), [], text)
                self.assertEqual(_text_parts(h.sent[0]), text)


class NoLegacyCqTests(unittest.TestCase):
    def test_cq_at_is_no_longer_converted(self) -> None:
        h = _Harness()
        text = "[CQ:at,qq=10001] 你看"
        h.send(text)
        self.assertEqual(_ats(h.sent[0]), [])
        self.assertEqual(_text_parts(h.sent[0]), text)

    def test_poke_cq_no_longer_triggers_an_action(self) -> None:
        h = _Harness()
        result = h.send("[CQ:poke,id=10001]")
        self.assertTrue(result.ok)
        self.assertEqual(len(h.sent), 1)
        self.assertEqual(_text_parts(h.sent[0]), "[CQ:poke,id=10001]")

    def test_mixed_text_with_poke_cq_sends_one_message_only(self) -> None:
        h = _Harness()
        h.send("先说一句 [CQ:poke,id=10001] 再补一句")
        self.assertEqual(len(h.sent), 1)
        self.assertEqual(
            _text_parts(h.sent[0]), "先说一句 [CQ:poke,id=10001] 再补一句"
        )


class MentionTruncationAndForwardTests(unittest.TestCase):
    def test_truncation_keeps_mentions_before_the_cut(self) -> None:
        tokens = outbound._parse_inline_markers("@10001 前面很长很长后面也长")
        kept, truncated = outbound._truncate_tokens(tokens, 4)
        self.assertTrue(truncated)
        self.assertEqual([t.qq for t in kept if isinstance(t, outbound._InlineAt)],
                         ["10001"])

    def test_long_forward_packs_at_segments(self) -> None:
        tokens = outbound._parse_inline_markers("@10001 前文 后文")
        nodes = outbound._pack_forward_nodes(
            tokens, bot_self_id="999", bot_nickname="Bot", max_chars_per_node=2
        )
        self.assertTrue(any(isinstance(seg, alconna.At) for node in nodes for seg in node.content))
        self.assertFalse(
            any("[CQ:" in str(seg) for node in nodes for seg in node.content)
        )


if __name__ == "__main__":
    unittest.main()
