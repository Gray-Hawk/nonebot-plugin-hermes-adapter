"""分节符正文信封协议（t_0ecd8f77）：解析矩阵、流式边界、提示词示例。

协议：每份回复以独立一行 § 开头、以独立一行 § 结尾；多条之间也共享一个独立整行 §。
只有完整闭合的段可发送；静默 = 整段只含 § 与空白；旧裸正文不保留兼容层。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import prompt_protocol as pp  # noqa: E402
from nonebot_plugin_hermes_adapter.core import runs_text  # noqa: E402
from nonebot_plugin_hermes_adapter.core.hermes_client import RunsEventParser  # noqa: E402

SEP = runs_text.SEPARATOR


def unit(body: str) -> str:
    return f"{SEP}\n{body}\n{SEP}"


def batch(*bodies: str) -> str:
    return SEP + "\n" + ("\n" + SEP + "\n").join(bodies) + "\n" + SEP


def feed(parser: RunsEventParser, events: list[dict]) -> list[dict]:
    import json

    drained: list[dict] = []
    for event in events:
        parser.feed("data: " + json.dumps(event, ensure_ascii=False))
        drained.extend(parser.drain_stream_decisions())
    return drained


class EnvelopeParseTests(unittest.TestCase):
    """纯解析矩阵：闭合才发、静默只有一条规则、标记严格整行。"""

    def test_single_body(self) -> None:
        self.assertEqual(runs_text.split_text_units(unit("正文")), ["正文"])

    def test_multiple_bodies_share_one_marker_each_side(self) -> None:
        self.assertEqual(runs_text.split_text_units(batch("A", "B")), ["A", "B"])
        self.assertEqual(
            runs_text.split_text_units(batch("甲", "乙", "丙")), ["甲", "乙", "丙"]
        )

    def test_silence_is_two_markers(self) -> None:
        self.assertEqual(runs_text.split_text_units(f"{SEP}\n{SEP}"), [])

    def test_silence_is_only_markers_and_whitespace(self) -> None:
        for text in (f"{SEP}", f"{SEP}{SEP}", f"{SEP} {SEP}", f"{SEP}\n\n{SEP}", f" {SEP} \n {SEP} "):
            self.assertEqual(runs_text.split_text_units(text), [], repr(text))
        # 去掉 § 与空白后还有内容就不是静默。
        self.assertEqual(runs_text.split_text_units(f"{SEP}\n好\n{SEP}"), ["好"])

    def test_empty_segments_are_dropped_without_complaint(self) -> None:
        text = f"{SEP}\nA\n{SEP}\n{SEP}\nB\n{SEP}"
        self.assertEqual(runs_text.split_text_units(text), ["A", "B"])

    def test_missing_open_drops_only_the_head(self) -> None:
        self.assertEqual(runs_text.split_text_units(f"裸开头\n{SEP}\nB\n{SEP}"), ["B"])

    def test_missing_close_drops_the_tail(self) -> None:
        self.assertEqual(runs_text.split_text_units(f"{SEP}\nA\n{SEP}\n裸尾巴"), ["A"])

    def test_no_marker_is_not_a_body(self) -> None:
        for text in ("你好", "第一行\n第二段", '{"body":"旧信封"}'):
            self.assertEqual(runs_text.split_text_units(text), [], text)

    def test_half_line_markers_are_plain_text(self) -> None:
        for line in (f"{SEP}内容", f"内容{SEP}", f"a{SEP}b"):
            body = f"甲\n{line}\n乙"
            self.assertEqual(runs_text.split_text_units(unit(body)), [body], line)

    def test_spaced_markers_are_plain_text(self) -> None:
        for line in (f" {SEP}", f"{SEP} ", f" {SEP} "):
            body = f"甲\n{line}\n乙"
            self.assertEqual(runs_text.split_text_units(unit(body)), [body], line)

    def test_fenced_marker_splits(self) -> None:
        # 代码围栏没有豁免：围栏内独立整行 § 同样分隔。
        self.assertEqual(
            runs_text.split_text_units(unit("```\n§\n```")), ["```", "```"]
        )

    def test_over_limit_is_fail_closed(self) -> None:
        with self.assertRaises(runs_text.TextUnitsError) as ctx:
            runs_text.split_text_units(unit("长" * (runs_text.MAX_BODY_CHARS + 1)))
        self.assertEqual(ctx.exception.reason, "body_overflow")

    def test_decision_and_unit_helpers_share_the_parser(self) -> None:
        text = batch("甲", "乙")
        self.assertEqual([d["body"] for d in runs_text.decisions_from_text(text)], ["甲", "乙"])
        self.assertEqual([u["body"] for u in runs_text.units_from_text(text)], ["甲", "乙"])


class EnvelopeStreamTests(unittest.TestCase):
    """Runs 流式边界：段内累计、段边界才发、闭合才发、终态只认 output。"""

    def test_char_by_char_delta(self) -> None:
        source = batch("甲", "乙")
        events = [
            {"event": "message.delta", "delta": char, "seq": index}
            for index, char in enumerate(source)
        ]
        events.append({"event": "tool.started", "tool": "todo_list", "seq": 99})
        parser = RunsEventParser()
        self.assertEqual(
            [d["body"] for d in feed(parser, events)], ["甲", "乙"]
        )

    def test_marker_split_across_frames(self) -> None:
        parser = RunsEventParser()
        drained = feed(parser, [
            {"event": "message.delta", "delta": f"{SEP}\n甲\n", "seq": 0},
            {"event": "message.delta", "delta": SEP, "seq": 1},
            {"event": "message.delta", "delta": f"\n乙\n{SEP}\n", "seq": 2},
            {"event": "tool.started", "tool": "todo_list", "seq": 3},
        ])
        self.assertEqual([d["body"] for d in drained], ["甲", "乙"])

    def test_dangling_marker_is_not_sent_until_closed(self) -> None:
        parser = RunsEventParser()
        drained = feed(parser, [
            {"event": "message.delta", "delta": f"{SEP}\n还没闭合", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
        ])
        self.assertEqual(drained, [])
        self.assertEqual(parser.text_error, "")

    def test_interim_only_sends_closed_segments(self) -> None:
        parser = RunsEventParser()
        drained = feed(parser, [
            {"event": "message.interim", "text": f"{SEP}\n写到一半", "seq": 0},
            {"event": "message.interim", "text": unit("进度"), "seq": 1},
        ])
        self.assertEqual([d["body"] for d in drained], ["进度"])

    def test_run_completed_only_uses_the_final_output(self) -> None:
        parser = RunsEventParser()
        output = unit("最终答复")
        drained = feed(parser, [
            {"event": "message.delta", "delta": f"{SEP}\n未闭合的尾巴", "seq": 0},
            {"event": "run.completed", "output": output, "seq": 1},
        ])
        # delta 里的半成品不发送；最后一段只认 output。
        self.assertEqual(drained, [])
        self.assertEqual(parser.output, output)
        self.assertEqual(
            [d["body"] for d in runs_text.decisions_from_text(parser.output)], ["最终答复"]
        )


class EnvelopePromptTests(unittest.TestCase):
    """提示词与解析器同批：四处渲染都要写明信封，示例能被真解析器读成正文。"""

    BLOCKS = ("group_runs", "group_completions", "private_runs", "private_completions")

    def renders(self) -> dict[str, str]:
        return {
            "group_runs": pp.render_decision_protocol(runs=True, group_rules="群规"),
            "group_completions": pp.render_decision_protocol(runs=False, group_rules="群规"),
            "private_runs": pp.render_decision_protocol(runs=True, group_rules=None),
            "private_completions": pp.render_decision_protocol(runs=False, group_rules=None),
        }

    def test_all_four_prompts_state_the_envelope(self) -> None:
        for name, prompt in self.renders().items():
            with self.subTest(block=name):
                self.assertIn("以独立一行 § 开头，以独立一行 § 结尾", prompt)
                self.assertIn("只有被两个 § 完整包住的段才会发出去", prompt)
                self.assertIn("缺开头或缺结尾 § 的段一律不发", prompt)
                self.assertIn("只输出两行 §", prompt)
                self.assertIn("空输出", prompt)  # 明确空输出不再表示静默
                self.assertNotIn("没有 § 就是一条消息", prompt)
                self.assertNotIn("什么都没有输出", prompt)

    def test_prompt_three_body_example_parses_to_three(self) -> None:
        for name, prompt in self.renders().items():
            with self.subTest(block=name):
                block = _example_block(prompt, "- 三条连发")
                self.assertEqual(runs_text.split_text_units(block), ["第一句", "第二句", "第三句"])

    def test_prompt_silence_example_is_silent(self) -> None:
        for name, prompt in self.renders().items():
            with self.subTest(block=name):
                block = _example_block(prompt, "- 静默（本轮什么都不说）")
                self.assertEqual(block, f"{SEP}\n{SEP}")
                self.assertEqual(runs_text.split_text_units(block), [])

    def test_prompt_literal_marker_example_is_plain_text(self) -> None:
        prompt = self.renders()["group_runs"]
        block = _example_block(prompt, "- 正文里要显示一行 §")
        # 中间那一行末尾多一个空格 → 不是标记，整段是一份正文。
        self.assertEqual(runs_text.split_text_units(block), ["第一行\n§ \n第三行"])

    def test_prompt_uses_real_multiline_examples_not_escapes(self) -> None:
        for name, prompt in self.renders().items():
            with self.subTest(block=name):
                self.assertNotIn("§\\n", prompt)
                self.assertNotIn("\\n§", prompt)


def _example_block(prompt: str, header: str) -> str:
    """取「正误示例」里某一节的正确示例（从第一行独立 § 开始到空行结束）。"""
    start = prompt.index(header)
    body = prompt[start:]
    body = body[body.index("正确："):]
    body = body.split("\n\n")[0]
    lines = [line for line in body.split("\n")[1:]]
    # 丢掉 § 之前的说明文字
    while lines and lines[0].strip() != SEP:
        lines.pop(0)
    return "\n".join(lines)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
