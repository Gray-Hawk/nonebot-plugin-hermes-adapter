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


class EnvelopeIncrementalQueueTests(unittest.TestCase):
    """delta 增量按闭合拆成正文单位（t_049df793 主人定案的结构化队列）。

    闭合即拆：每份正文的第二个标记一被换行终结，它就成为独立单位进现有
    ``_stream_pending``；``_seg_text`` 只留未闭合碎片，不再承担完整待发正文缓冲。
    """

    def test_closed_units_are_queued_before_the_segment_ends(self) -> None:
        parser = RunsEventParser()
        drained = feed(parser, [
            {"event": "message.delta", "delta": batch("甲", "乙") + "\n", "seq": 0},
        ])
        # 段还没封（没有 tool.started），两份正文已经各自成为发送单位。
        self.assertEqual([d["body"] for d in drained], ["甲", "乙"])
        self.assertEqual([d["kind"] for d in drained], ["text", "text"])
        self.assertTrue(all(d["should_reply"] for d in drained))
        # 段缓冲只剩最后那个共享标记（下一份正文的开头），没有完整正文。
        self.assertEqual(parser._seg_text, f"{SEP}\n")

    def test_frame_tail_marker_without_newline_is_not_a_marker(self) -> None:
        # 帧尾的 § 还没有被换行终结：可能是下一帧 §B 的行首字符，不能当标记。
        self.assertEqual(
            runs_text.take_closed_units(f"{SEP}B\n{SEP}"), ([], f"{SEP}B\n{SEP}")
        )
        parser = RunsEventParser()
        self.assertEqual(feed(parser, [
            {"event": "message.delta", "delta": f"{SEP}B\n{SEP}", "seq": 0},
            {"event": "message.interim", "text": SEP, "seq": 1},
            {"event": "tool.started", "tool": "lookup", "seq": 2},
        ]), [])

    def test_shared_marker_is_reused_instead_of_restored(self) -> None:
        units, remainder = runs_text.take_closed_units(batch("甲", "乙") + "\n")
        self.assertEqual(units, ["甲", "乙"])
        # 尾标记同时是下一份正文的开头：拆分时一次处理完，不需要“接回共享标记”。
        self.assertEqual(remainder, f"{SEP}\n")
        units, remainder = runs_text.take_closed_units(batch("甲", "乙"))
        # 乙 的闭合标记在帧尾、还没终结：只交付甲，乙留在碎片里。
        self.assertEqual(units, ["甲"])
        self.assertEqual(remainder, f"{SEP}\n乙\n{SEP}")

    def test_crlf_and_cr_are_the_same_line_ending(self) -> None:
        for newline in ("\r\n", "\r"):
            with self.subTest(newline=repr(newline)):
                text = batch("甲", "乙").replace("\n", newline) + newline
                self.assertEqual(
                    runs_text.take_closed_units(text), (["甲", "乙"], f"{SEP}\n")
                )

    def test_head_before_first_marker_is_missing_open(self) -> None:
        from tests import test_runs_terminal as _e2

        with _e2._CapturedWarnings() as logs:
            units, remainder = runs_text.take_closed_units(f"裸开头\n{SEP}\nB\n{SEP}\n")
        self.assertEqual(units, ["B"])
        self.assertEqual(remainder, f"{SEP}\n")
        self.assertIn("reason=missing_open", "\n".join(logs.messages))

    def test_open_fragment_is_not_a_unit_until_it_closes(self) -> None:
        parser = RunsEventParser()
        drained = feed(parser, [
            {"event": "message.delta", "delta": f"{SEP}\n甲\n{SEP}\n乙\n", "seq": 0},
        ])
        self.assertEqual([d["body"] for d in drained], ["甲"])
        self.assertEqual(parser._seg_text, f"{SEP}\n乙\n")

    def test_interim_completing_partial_fragment_does_not_glue_the_next_unit(self) -> None:
        """interim 把半份正文补全后，碎片换成 interim 自己的结构剩余。

        否则半份 ``§\\n甲`` 会和下一帧拼成 ``§\\n甲\\n乙\\n§``，把原文里并不存在的
        「甲+乙」当成一份正文发出去。
        """
        parser = RunsEventParser()
        drained = feed(parser, [
            {"event": "message.delta", "delta": f"{SEP}\n甲", "seq": 0},
            {"event": "message.interim", "text": unit("甲"), "seq": 1},
            {"event": "message.delta", "delta": f"\n乙\n{SEP}\n", "seq": 2},
            {"event": "tool.started", "tool": "lookup", "seq": 3},
        ])
        self.assertEqual([d["body"] for d in drained], ["甲", "乙"])

    def test_text_units_carry_lightweight_kind(self) -> None:
        """发送单元沿用现有 dict，只多一个轻量 ``kind``（当前固定 text）。"""
        self.assertEqual(
            [u["kind"] for u in runs_text.units_from_text(batch("甲", "乙"))],
            ["text", "text"],
        )
        parser = RunsEventParser()
        drained = feed(parser, [{"event": "message.delta", "delta": batch("甲") + "\n", "seq": 0}])
        self.assertEqual([d["kind"] for d in drained], ["text"])
        parser = RunsEventParser()
        feed(parser, [{"event": "run.completed", "output": batch("乙"), "seq": 0}])
        self.assertEqual([d["kind"] for d in parser.terminal_decisions()], ["text"])

    def test_terminal_same_segment_dedupe_is_not_the_segment_duplicate_signal(self) -> None:
        """终态重复本段已发正文记独立原因：不是段内重复吞掉，不触发贴表情观察口。"""
        parser = RunsEventParser()
        seen: list[tuple[str, str, str]] = []
        parser.on_suppressed = lambda reason, text, source: seen.append((reason, text, source))
        drained = feed(parser, [
            {"event": "message.delta", "delta": batch("甲", "乙") + "\n", "seq": 0},
            {"event": "run.completed", "output": batch("甲", "乙"), "seq": 1},
        ])
        self.assertEqual([d["body"] for d in drained], ["甲", "乙"])
        self.assertEqual(parser.terminal_decisions(), [])
        self.assertEqual(seen, [
            ("terminal_same_segment", "甲", "terminal"),
            ("terminal_same_segment", "乙", "terminal"),
        ])
        self.assertNotIn("segment_duplicate", [reason for reason, _text, _source in seen])

    def test_terminal_does_not_repeat_the_same_segment(self) -> None:
        parser = RunsEventParser()
        feed(parser, [
            {"event": "message.delta", "delta": batch("甲", "乙") + "\n", "seq": 0},
            {"event": "run.completed", "output": batch("甲", "乙"), "seq": 1},
        ])
        # 同一段文本的两种切帧：delta 已发过的两份不在终态重发。
        self.assertEqual(parser.terminal_decisions(), [])

    def test_terminal_after_seal_still_resends_cross_segment_text(self) -> None:
        parser = RunsEventParser()
        feed(parser, [
            {"event": "message.delta", "delta": batch("甲") + "\n", "seq": 0},
            {"event": "tool.started", "tool": "lookup", "seq": 1},
            {"event": "run.completed", "output": batch("甲"), "seq": 2},
        ])
        # 工具边界封段后是本段同文照发：跨段不去重。
        self.assertEqual([d["body"] for d in parser.terminal_decisions()], ["甲"])


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
