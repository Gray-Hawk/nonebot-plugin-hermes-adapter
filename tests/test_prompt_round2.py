"""第二轮提示词收敛：纯文本正文协议下的残句、字段动词与历史口吻回归。

只钉可判定的文案事实（不重开协议）：
- Runs/completions 提示词里没有 body/信封式字段教学，只剩正误示例里的反例；
- submit_control 说明写明「多字段合并算一份」，并给引用纪律与引用示例；
- 私聊提示词声明只用到 reply_to，不再把退窗/任务锚当私聊能力；
- 旧字段动词（带 pin_anchor / 带 answer_anchor / 设 true）改成工具调用口径；
- 已退役的「任务便签」与带日期的历史口吻清掉；
- reply_text→正文 的替换不留下夹空格的残句；
- skill/SKILL.md 不再教 two-line / submit_decision / topic_hint / background_task；
- legacy（astrbot）协议保持原样，不被本轮文案改动波及。
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core import prompt_builder  # noqa: E402

RETIRED = ("topic_hint", "should_reply", "reply_text", "not_final")
# `background_task` 只认退役字段本身（后面不能跟 s），工具名 get_background_tasks 不算。
RETIRED_FIELD_RE = re.compile(r"background_task(?!s)")


def render(interface: str, *, private: bool) -> str:
    previous = plugin_config.hermes_api_interface
    try:
        object.__setattr__(plugin_config, "hermes_api_interface", interface)
        if private:
            return prompt_builder.build_passive_system_prompt(
                adapter="onebotv11", is_private=True, user_id="10001",
                group_id=None, private_decision=True,
            )
        return prompt_builder.build_reactive_system_prompt(
            adapter="onebotv11", group_id="462540549",
            self_id="2968545011", self_nickname="Bot",
        )
    finally:
        object.__setattr__(plugin_config, "hermes_api_interface", previous)


class PlainTextPromptResidueTests(unittest.TestCase):
    def test_no_retired_fields_in_runs_prompts(self) -> None:
        for private in (False, True):
            prompt = render("runs", private=private)
            with self.subTest(private=private):
                for name in RETIRED:
                    self.assertNotIn(name, prompt)
                self.assertIsNone(RETIRED_FIELD_RE.search(prompt))
                self.assertNotIn("任务便签", prompt)
                self.assertNotRegex(prompt, r"20\d\d-\d\d-\d\d")

    def test_body_only_appears_as_the_negative_example(self) -> None:
        prompt = render("runs", private=False)
        body_lines = [line for line in prompt.splitlines() if '"body"' in line]
        self.assertEqual(len(body_lines), 1, body_lines)
        self.assertIn("错误：", body_lines[0])
        # 正文不再是字段：任何“原文里写 reply_text/body 字段”的教学都算回归。
        self.assertNotIn("写 body 字段", prompt)

    def test_control_bundling_is_documented(self) -> None:
        for interface in ("runs", "completions"):
            prompt = render(interface, private=False)
            with self.subTest(interface=interface):
                self.assertIn("一次调用可以同时给多个字段，合并算一份控制", prompt)
                self.assertIn("配给接下来的一份正文", prompt)
        self.assertIn("没有登记就用缺省控制", render("runs", private=False))

    def test_quote_discipline_and_example(self) -> None:
        prompt = render("runs", private=False)
        self.assertIn("只在确需指向原文时用（日常接话不必引用）", prompt)
        self.assertIn("- 引用某条原文（只在确需指向它时）", prompt)
        self.assertIn("先调用 submit_control 登记 reply_to=m:123", prompt)

    def test_private_scope_limits_controls_to_reply_to(self) -> None:
        prompt = render("runs", private=True)
        self.assertIn("私聊里的控制只用到 reply_to；退窗与任务锚只在群活跃态生效", prompt)
        self.assertNotIn("私聊里的控制只用到 reply_to", render("runs", private=False))

    def test_anchor_fields_use_tool_verbs(self) -> None:
        prompt = render("runs", private=False)
        self.assertIn("调用 submit_control 传 pin_anchor=true", prompt)
        self.assertIn("调用 submit_control 传 release_anchor", prompt)
        self.assertIn("调用 submit_control 传 answer_anchor=发起者的", prompt)
        self.assertIn("不可用 submit_control 引用或钉锚", prompt)
        self.assertIn("只在以下情况登记 should_exit_active=true：", prompt)
        # 旧信封式动词不该再出现。
        self.assertNotIn("显式 pin_anchor=true 钉入", prompt)
        self.assertNotIn("时带 release_anchor", prompt)
        self.assertNotIn("带 answer_anchor=发起者的", prompt)

    def test_reply_text_replacement_leaves_no_stray_spaces(self) -> None:
        for interface in ("runs", "completions"):
            prompt = render(interface, private=False)
            with self.subTest(interface=interface):
                self.assertNotRegex(prompt, r"正文\s+[里就]")
                self.assertNotRegex(prompt, r"在\s+正文")
                for stray in ("理由 不输出正文", "默认 不输出正文", "仍应 不输出正文", "直接 不输出正文"):
                    self.assertNotIn(stray, prompt)

    def test_completions_keeps_push_message_and_request_continuation(self) -> None:
        prompt = render("completions", private=False)
        self.assertIn("push_message", prompt)
        self.assertIn("request_continuation", prompt)
        self.assertIn("轮中进度用 push_message 工具推送", prompt)

    def test_legacy_astrbot_contract_is_untouched(self) -> None:
        previous = plugin_config.hermes_api_interface
        try:
            object.__setattr__(plugin_config, "hermes_api_interface", "astrbot")
            prompt = prompt_builder.build_reactive_system_prompt()
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        # 旧接口仍走 JSON 决策信封，不能被纯文本文案改坏。
        self.assertIn("submit_decision", prompt)
        self.assertIn("reply_text", prompt)


class SkillDocPlainTextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = (ROOT / "skill" / "SKILL.md").read_text(encoding="utf-8")

    def test_no_retired_contract_language(self) -> None:
        for phrase in ("two-line", "submit_decision", "topic_hint"):
            self.assertNotIn(phrase, self.text)
        self.assertIsNone(RETIRED_FIELD_RE.search(self.text))
        self.assertNotIn("not_final: true", self.text)
        self.assertNotIn("{\"body\"", self.text)

    def test_plain_text_contract_is_documented(self) -> None:
        self.assertIn("§", self.text)
        self.assertIn("submit_control", self.text)
        self.assertIn("request_continuation", self.text)
        self.assertIn("no JSON wrapper", self.text)

    def test_private_scope_note_present(self) -> None:
        self.assertIn("Private chats only ever use `reply_to`", self.text)


if __name__ == "__main__":
    unittest.main()
