"""出站戳一戳：正文内联 ``[CQ:poke,type=1,id=<QQ>]`` 转 OneBot poke 段。

现有出站只认 ``[CQ:at,qq=<数字>]`` 一种内联 CQ。本文件锁定 poke 的最小扩展：
- 只认 ``[CQ:poke,type=<数字>,id=<数字>]`` 与省略 type 的 ``[CQ:poke,id=<数字>]``；
- 转成 OneBot poke 消息段走同一条出站，标记绝不留进正文文本；
- 参数非法（非数字/顺序不符/缺 id）按现有 inline 解析失败语义留在正文里；
- 自戳视为非法目标（不做真实自戳验收），保留原样文本；
- @ 回归不变。
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401

import nonebot_plugin_alconna as alconna  # noqa: E402

from nonebot_plugin_hermes_adapter.core import outbound  # noqa: E402

SELF_ID = "999"


def _text_parts(dump: list[dict]) -> str:
    return "".join(part.get("text", "") for part in dump if part.get("type") == "text")


def _poke_parts(dump: list[dict]) -> list[dict]:
    out: list[dict] = []
    for part in dump:
        origin = part.get("origin")
        if part.get("type") == "other" and isinstance(origin, dict) and origin.get("type") == "poke":
            out.append(origin.get("data") or {})
    return out


class OutboundPokeTests(unittest.TestCase):
    def _send(self, text: str, *, self_id: str = SELF_ID):
        captured: list = []

        async def fake_send(uni, *, bot, target, delivery=None):
            captured.append(uni)
            return ["m1"]

        with patch.object(outbound, "_send_unimsg", new=fake_send):
            result = asyncio.run(
                outbound.send_text_with_media(
                    bot=SimpleNamespace(self_id=self_id),
                    target=SimpleNamespace(private=False, id="g1"),
                    text=text,
                    media_urls=[],
                    adapter_name="onebotv11",
                )
            )
        self.assertTrue(result.ok, text)
        self.assertEqual(len(captured), 1, text)
        return captured[0].dump()

    def test_mixed_text_converts_poke_segment(self) -> None:
        dump = self._send("先戳你 [CQ:poke,type=1,id=10001] 一下")
        pokes = _poke_parts(dump)
        self.assertEqual(pokes, [{"type": "1", "id": "10001"}])
        self.assertNotIn("[CQ:poke", _text_parts(dump))
        self.assertEqual(_text_parts(dump), "先戳你  一下")

    def test_poke_alone_is_a_real_message(self) -> None:
        dump = self._send("[CQ:poke,id=12345]")
        self.assertEqual(_poke_parts(dump), [{"type": "1", "id": "12345"}])
        self.assertEqual(_text_parts(dump), "")

    def test_segment_order_is_preserved_with_at(self) -> None:
        dump = self._send("[CQ:at,qq=20002] 前 [CQ:poke,type=2,id=10001] 后")
        kinds = []
        for part in dump:
            if part.get("type") == "text":
                kinds.append(("text", part.get("text")))
            elif part.get("type") == "other":
                kinds.append(("poke", (part.get("origin") or {}).get("data")))
            else:
                kinds.append((part.get("type"), None))
        self.assertEqual(
            kinds,
            [
                ("at", None),
                ("text", " 前 "),
                ("poke", {"type": "2", "id": "10001"}),
                ("text", " 后"),
            ],
        )

    def test_invalid_params_stay_literal_text(self) -> None:
        for text in (
            "[CQ:poke,id=abc]",
            "[CQ:poke,type=x,id=10001]",
            "[CQ:poke,id=10001,type=1]",
            "[CQ:poke]",
            "[CQ:poke,id=]",
        ):
            with self.subTest(text=text):
                dump = self._send(text)
                self.assertEqual(_poke_parts(dump), [])
                self.assertEqual(_text_parts(dump), text)

    def test_self_poke_is_not_converted(self) -> None:
        dump = self._send("[CQ:poke,id=999]", self_id=SELF_ID)
        self.assertEqual(_poke_parts(dump), [])
        self.assertEqual(_text_parts(dump), "[CQ:poke,id=999]")

    def test_at_regression_unchanged(self) -> None:
        dump = self._send("[CQ:at,qq=12345] 你看")
        ats = [p for p in dump if p.get("type") == "at"]
        self.assertEqual(len(ats), 1)
        self.assertEqual(ats[0].get("target"), "12345")
        self.assertEqual(_poke_parts(dump), [])

    def test_poke_survives_forward_node_packing(self) -> None:
        # 长文合并转发分支复用同一批片段：poke 段不参与字符预算、也不留在文本里。
        tokens = outbound._parse_inline_markers("前文 [CQ:poke,type=1,id=10001] 后文")
        nodes = outbound._pack_forward_nodes(
            tokens, bot_self_id="999", bot_nickname="Bot", max_chars_per_node=2
        )
        contents = [seg for node in nodes for seg in node.content]
        pokes = [
            seg.origin.data for seg in contents
            if isinstance(seg, alconna.Other) and getattr(seg.origin, "type", "") == "poke"
        ]
        self.assertEqual(pokes, [{"type": "1", "id": "10001"}])
        text = "".join(str(seg) for seg in contents if isinstance(seg, alconna.Text))
        self.assertNotIn("[CQ:poke", text)


if __name__ == "__main__":
    unittest.main()
