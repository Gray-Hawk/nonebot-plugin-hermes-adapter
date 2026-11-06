import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.npha_terminal import (  # noqa: E402
    TerminalFormatError, decode_terminal, structured_from_terminal,
)


def unit(control: dict | None = None, body: str = "正文") -> str:
    ctrl = json.dumps(control or {}, ensure_ascii=False)
    return ctrl + "\n" + json.dumps({"body": body}, ensure_ascii=False)


class TerminalDecodeTests(unittest.TestCase):
    def test_two_line_envelope(self) -> None:
        dec = decode_terminal(unit({"reply_to": "m:1", "not_final": True}, "你好"))
        self.assertEqual(dec.body, "你好")
        self.assertEqual(dec.control["reply_to"], "m:1")
        self.assertIs(dec.control["not_final"], True)

    def test_valid_json_has_no_broken_hint(self) -> None:
        dec = decode_terminal(unit({"reply_to": "m:1"}, "正常正文"))
        self.assertEqual(dec.reminder, "")
        self.assertEqual(dec.body, "正常正文")

    def test_plain_text_goes_whole(self) -> None:
        for text in ("普通一句话", "第一行\n第二行", "看 {not_final:true} 这个字段"):
            dec = decode_terminal(text)
            self.assertEqual(dec.body, text)
            self.assertTrue(dec.reminder)

    def test_missing_fields_fall_back_to_defaults(self) -> None:
        dec = decode_terminal('{"reply_to":"m:7"}\n正文照发')
        self.assertEqual(dec.body, "正文照发")
        self.assertEqual(dec.control["reply_to"], "m:7")
        self.assertIs(dec.control["not_final"], False)

    def test_fullwidth_and_escaped_shell_still_reads_fields(self) -> None:
        dec = decode_terminal('｛"reply_to"："m:7"，"not_final"：true｝\n｛"body"："正文"｝')
        self.assertEqual(dec.body, "正文")
        self.assertEqual(dec.control["reply_to"], "m:7")
        self.assertIs(dec.control["not_final"], True)

    def test_body_with_quotes_kept(self) -> None:
        dec = decode_terminal(unit({}, '他说："你好"，然后走了'))
        self.assertIn("然后走了", dec.body)

    def test_empty_body_is_silent(self) -> None:
        dec = decode_terminal(unit({}, ""))
        self.assertTrue(dec.silent)
        self.assertEqual(dec.reminder, "")
        self.assertNotIn("should_reply", dec.control)

    def test_limits(self) -> None:
        with self.assertRaises(TerminalFormatError):
            decode_terminal("abcdef", max_body_chars=4)
        for text in ("", "   ", "\n\n"):
            with self.assertRaises(TerminalFormatError):
                decode_terminal(text)

    def test_structured_payload(self) -> None:
        payload = structured_from_terminal(decode_terminal(unit({}, "")))
        self.assertEqual(payload["body"], "")
        self.assertFalse(payload["should_reply"])
        self.assertNotIn("_self_heal_hint", payload)
