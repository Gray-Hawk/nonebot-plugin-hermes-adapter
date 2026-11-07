"""提示词契约回归；验证实际 builder 输出，不假装静态断言能证明模型服从。"""
from __future__ import annotations

import re
import unittest
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin


class EnvelopePromptTests(unittest.TestCase):
    def prompts(self, interface):
        with patch.object(plugin_config, "hermes_api_interface", interface):
            return {
                "group_reactive": pb.build_reactive_system_prompt(),
                "private_passive": pb.build_passive_system_prompt(
                    adapter="onebotv11", is_private=True, user_id="u1",
                    group_id=None, private_decision=True,
                ),
            }

    def test_all_user_visible_content_requires_envelope(self):
        for interface in ("runs", "completions"):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn("只有决策信封中的 body 才能作为用户可见回复", prompt)
                    self.assertIn("受理回执、进度、结果和最终答复", prompt)
                    self.assertIn("不能用普通旁白或最终文本代替", prompt)
                    self.assertIn("每份固定两行", prompt)

    def test_noninstant_tasks_receive_ack_before_lengthy_work(self):
        for interface in ("runs", "completions"):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn("不能即时给出完整答复的任务，必须先发一条明确的受理回执", prompt)
                    self.assertIn("说明消息已收到、任务正在处理", prompt)
                    self.assertIn("回执也必须使用双单行 JSON 信封，正文放在 body", prompt)
                    self.assertNotIn("发出后就是终态", prompt)

    def test_short_tasks_do_not_require_extra_ack(self):
        for interface in ("runs", "completions"):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn("能当轮迅速完成的短任务直接给结果，不强制另发受理回执", prompt)
                    self.assertIn("不要为回执拆轮或重复刷屏", prompt)

    def test_ack_cannot_be_an_empty_promise(self):
        for interface in ("runs", "completions"):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn("只有确实存在后续执行路径时才可承诺处理", prompt)
                    self.assertIn("Runs 同一 run 内", prompt)
                    self.assertIn("not_final=true", prompt)
                    self.assertIn("已成功启动且有完成通知的后台任务", prompt)
                    self.assertIn("background_task 便签本身不会启动任务", prompt)
                    self.assertIn("没有可执行路径时，必须如实说明阻碍或请求必要信息", prompt)
                    self.assertIn("禁止只说“稍等”“我去处理”后静默结束", prompt)

    def test_runs_user_visible_envelopes_are_allowed_between_tools(self):
        prompts = self.prompts("runs")
        row = BufferedMessage(
            ts=1, adapter="onebotv11", group_id="g1", user_id="u1",
            nickname="小光", content="请核对这个项目", origin=MessageOrigin.human(),
        )
        with patch.object(plugin_config, "hermes_api_interface", "runs"):
            prompts["reactive_reminder"] = pb.build_reactive_user_content(
                addressed_to_bot=True, triggered_by="u1", triggered_by_nickname="小光",
                recent_messages=[], rows=[row], current_image_urls=[],
            ).split("</current_message>")[-1]
        for path, prompt in prompts.items():
            with self.subTest(path=path):
                self.assertIn("工具间隙也可输出用户可见的两行 JSON 决策信封", prompt)
                self.assertIn("受理回执、进度、结果和最终答复", prompt)
                self.assertNotIn("工具间隙不要输出决策", prompt)
                self.assertNotIn("工具间隙不要写成决策 JSON", prompt)
                self.assertNotIn("回复最终答复:", prompt)

    def test_runs_ack_example_has_both_lines_for_each_message(self):
        from nonebot_plugin_hermes_adapter.core.npha_terminal import decode_terminal_batch

        prompt = self.prompts("runs")["group_reactive"]
        example = prompt.split("例 D —", 1)[1].split("</decision_protocol>", 1)[0]
        json_lines = []
        for line in example.splitlines():
            candidate = re.sub(r"^(先说|再说):\s*", "", line.strip())
            if candidate.startswith("{"):
                json_lines.append(candidate)
        decisions = decode_terminal_batch("\n".join(json_lines))
        self.assertEqual(len(decisions), 2)
        self.assertTrue(all(not decision.reminder for decision in decisions))
        self.assertIn("收到", decisions[0].body)
        self.assertIn("正在", decisions[0].body)
        # 同一 run 中间回执后继续工具，无需另触发 not_final 续跑轮。
        self.assertFalse(decisions[0].control["not_final"])

    def test_json_structure_does_not_fullwidth_the_chinese_body(self):
        for interface in ("runs", "completions"):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn("JSON 结构符号用半角，中文正文沿用全角标点约定", prompt)
                    self.assertIn("代码、路径、ID 和 CQ 标记保持原样", prompt)
                    self.assertNotIn('"body":"补充一句:', prompt)

    def test_missing_envelope_notes_do_not_offer_narration_as_reply(self):
        import ast
        import inspect
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        for turn in (mt._run_passive_turn, mt._run_reactive_turn):
            tree = ast.parse(inspect.getsource(turn))
            notes = [
                node.value for node in ast.walk(tree)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)
                and node.value.startswith("系统提示:你上一轮的最终回复没有包双单行 JSON")
            ]
            with self.subTest(turn=turn.__name__):
                self.assertEqual(len(notes), 1)
                self.assertIn("受理回执、进度、结果和最终答复", notes[0])
                self.assertIn("不能用普通旁白或最终文本代替", notes[0])
                self.assertNotIn("过程想法走旁白即可", notes[0])


if __name__ == "__main__":
    unittest.main()
