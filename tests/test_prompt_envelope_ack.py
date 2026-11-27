"""提示词契约回归：正文即消息（无 JSON 信封）、§ 严格整行、续跑工具口径。

静态断言只锁提示词文字，不假装能证明模型服从。
"""
from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin


class PlainTextPromptTests(unittest.TestCase):
    def prompts(self, interface):
        with patch.object(plugin_config, 'hermes_api_interface', interface):
            return {
                'group_reactive': pb.build_reactive_system_prompt(),
                'private_passive': pb.build_passive_system_prompt(
                    adapter='onebotv11', is_private=True, user_id='u1',
                    group_id=None, private_decision=True,
                ),
            }

    def test_body_is_the_visible_channel(self):
        for interface in ('runs', 'completions'):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn('用户能看到你输出的正文', prompt)
                    self.assertIn('不要写 JSON、不要写 body 之类的字段', prompt)
                    self.assertIn('整行只有一个 §', prompt)
                    self.assertIn('没有要说的就只输出两行 §（静默）', prompt)
                    self.assertIn('错误：{"body":"你好"}', prompt)  # 反例保留，正例是分节符正文
                    self.assertNotIn('决策信封', prompt)

    def test_separator_rule_is_strict(self):
        for interface in ('runs', 'completions'):
            prompt = self.prompts(interface)['group_reactive']
            # 分节符：每份以独立一行 § 开头／结尾，只有完整闭合的段才会发出去。
            self.assertIn('以独立一行 § 开头，以独立一行 § 结尾', prompt)
            self.assertIn('只有被两个 § 完整包住的段才会发出去', prompt)
            self.assertIn('缺开头或缺结尾 § 的段一律不发', prompt)
            self.assertIn('行内', prompt)
            self.assertIn('带空格的 §', prompt)

    def test_noninstant_tasks_receive_ack_before_lengthy_work(self):
        for interface in ('runs', 'completions'):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn('对已决定承接、不能即时给出完整答复的任务', prompt)
                    self.assertIn('说明消息已收到、任务正在处理', prompt)
                    self.assertNotIn('发出后就是终态', prompt)
                    if interface == 'runs':
                        self.assertIn('必须先用正文明确说明', prompt)
                        self.assertIn('再执行耗时工作', prompt)
                    else:
                        self.assertIn('再调用 request_continuation 进入继续执行轮', prompt)

    def test_short_tasks_do_not_require_extra_ack(self):
        for interface in ('runs', 'completions'):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn('能当轮迅速完成的短任务直接给结果，不要另发受理回执。', prompt)

    def test_ack_cannot_be_an_empty_promise(self):
        for interface in ('runs', 'completions'):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn('只有确实存在后续执行路径时才可承诺处理', prompt)
                    self.assertIn('已成功启动并且有完成通知的后台任务', prompt)
                    self.assertNotIn('background_task 便签', prompt)
                    self.assertIn('没有可执行路径时，必须如实说明阻碍', prompt)
                    self.assertIn('禁止只说“稍等”“我去处理”后静默结束', prompt)
                    if interface == 'runs':
                        self.assertIn('同一 run 的实际后续工具调用', prompt)
                        self.assertIn('get_background_tasks', prompt)
                        self.assertNotIn('request_continuation', prompt)
                    else:
                        self.assertIn('request_continuation', prompt)
                        self.assertIn('push_message', prompt)

    def test_progress_between_tools_is_body(self):
        prompts = self.prompts('runs')
        row = BufferedMessage(ts=1, adapter='onebotv11', group_id='g1', user_id='u1',
                              nickname='小光', content='请核对这个项目', origin=MessageOrigin.human())
        with patch.object(plugin_config, 'hermes_api_interface', 'runs'):
            built = pb.build_reactive_user_content(
                addressed_to_bot=True, recent_messages=[], rows=[row], current_image_urls=[],
            )
        for path in ('group_reactive', 'private_passive'):
            prompt = prompts[path]
            self.assertIn('轮中进度直接输出正文', prompt)
            self.assertIn('不要攒到整轮结束', prompt)
            self.assertNotIn('决策信封', prompt)
        # 近因 reminder 已移出正文，走 hermes_context.closing_reminder（N 次提醒的批尾）。
        self.assertEqual(built.content, '请核对这个项目')
        reminder = built.closing_reminder
        self.assertIn('工具调用之间想对用户说的话也直接输出正文', reminder)
        self.assertIn('不要写 JSON 信封', reminder)

    def test_ack_example_has_no_json(self):
        for interface in ('runs', 'completions'):
            prompt = self.prompts(interface)['group_reactive']
            example = prompt.split('- 长任务受理回执\n正确：', 1)[1]
            self.assertIn('收到，我马上继续处理', example)
            self.assertNotIn('{"body"', example)
            self.assertNotIn('not_final', example)

    def test_no_envelope_self_heal_left(self):
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        for name in ('_missing_envelope_note', '_handle_envelope_rejection',
                     '_push_self_heal_note', '_should_hard_silence_envelope'):
            self.assertFalse(hasattr(mt, name), name)
        for turn in (mt._run_passive_turn, mt._run_reactive_turn):
            source = inspect.getsource(turn)
            self.assertNotIn('_missing_envelope_note', source)
            self.assertNotIn('envelope_missing', source)
            self.assertNotIn('_self_heal_hint', source)


if __name__ == '__main__':
    unittest.main()
