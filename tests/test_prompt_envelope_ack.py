"""提示词契约回归；断言用户第三版定稿，不假装静态断言能证明模型服从。"""
from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin


class EnvelopePromptTests(unittest.TestCase):
    def prompts(self, interface):
        with patch.object(plugin_config, 'hermes_api_interface', interface):
            return {
                'group_reactive': pb.build_reactive_system_prompt(),
                'private_passive': pb.build_passive_system_prompt(
                    adapter='onebotv11', is_private=True, user_id='u1',
                    group_id=None, private_decision=True,
                ),
            }

    def test_all_user_visible_content_requires_envelope(self):
        for interface in ('runs', 'completions'):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn('用户只能看到决策信封中的 body 或 push_message 工具推送的消息。', prompt)
                    self.assertIn('想对用户说的话都写进 body，不要写在信封外', prompt)
                    self.assertIn('不要假设普通旁白会被转发', prompt)
                    if interface == 'runs':
                        self.assertIn('每份决策就是一行 JSON', prompt)
                        self.assertIn('submit_control', prompt)
                    else:
                        self.assertIn('每份决策固定两行', prompt)

    def test_noninstant_tasks_receive_ack_before_lengthy_work(self):
        for interface in ('runs', 'completions'):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn('对已决定承接、不能即时给出完整答复的任务', prompt)
                    self.assertIn('说明消息已收到、任务正在处理', prompt)
                    if interface == 'runs':
                        self.assertIn('必须先用决策信封明确', prompt)
                        self.assertIn('再执行耗时工作', prompt)
                    else:
                        self.assertIn('必须使用 not_final=true 触发继续执行轮的决策信封', prompt)
                        self.assertIn('再用续跑执行耗时工作', prompt)
                    self.assertNotIn('发出后就是终态', prompt)

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
                        self.assertNotIn('not_final', prompt)
                        self.assertIn('同一 run 的实际后续工具调用', prompt)
                        self.assertIn('get_background_tasks', prompt)
                    else:
                        self.assertIn('not_final=true', prompt)

    def test_runs_user_visible_envelopes_are_allowed_between_tools(self):
        prompts = self.prompts('runs')
        row = BufferedMessage(ts=1, adapter='onebotv11', group_id='g1', user_id='u1',
                              nickname='小光', content='请核对这个项目', origin=MessageOrigin.human())
        with patch.object(plugin_config, 'hermes_api_interface', 'runs'):
            prompts['reactive_reminder'] = pb.build_reactive_user_content(
                addressed_to_bot=True, triggered_by='u1', triggered_by_nickname='小光',
                recent_messages=[], rows=[row], current_image_urls=[],
            ).split('</current_message>')[-1]
        for path, prompt in prompts.items():
            with self.subTest(path=path):
                self.assertIn('当下', prompt)
                self.assertIn('完整决策信封', prompt)
                self.assertIn('不要攒到整轮结束', prompt)
                self.assertNotIn('工具间隙不要输出决策', prompt)
                self.assertNotIn('工具间隙不要写成决策 JSON', prompt)

    def test_runs_ack_example_has_both_lines_for_each_message(self):
        from nonebot_plugin_hermes_adapter.core.npha_terminal import decode_terminal_batch
        for interface in ('runs', 'completions'):
            prompt = self.prompts(interface)['group_reactive']
            example = prompt.split('- 长任务受理回执\n正确：\n', 1)[1]
            lines = [line for line in example.splitlines() if line.startswith('{')][:2]
            decisions = decode_terminal_batch('\n'.join(lines))
            self.assertEqual(len(decisions), 1)
            self.assertFalse(decisions[0].reminder)
            self.assertIn('收到', decisions[0].body)
            self.assertEqual(decisions[0].control['not_final'], interface == 'completions')

    def test_json_structure_does_not_fullwidth_the_chinese_body(self):
        for interface in ('runs', 'completions'):
            for path, prompt in self.prompts(interface).items():
                with self.subTest(interface=interface, path=path):
                    self.assertIn('JSON 结构符号使用半角标点符号，中文正文则使用全角标点符号', prompt)
                    self.assertIn('代码、路径、ID 和 CQ 标记不要修改，保持原样', prompt)
                    self.assertNotIn('"body":"补充一句:', prompt)

    def test_missing_envelope_notes_do_not_offer_narration_as_reply(self):
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt
        for turn in (mt._run_passive_turn, mt._run_reactive_turn):
            self.assertIn('_missing_envelope_note()', inspect.getsource(turn))
        for interface in ('runs', 'completions'):
            with patch.object(plugin_config, 'hermes_api_interface', interface):
                note = mt._missing_envelope_note()
            self.assertIn('正文放在 body', note)
            self.assertIn('不能用普通旁白或最终裸文本代替', note)
            if interface == 'runs':
                self.assertNotIn('push_message', note)
                self.assertNotIn('not_final', note)
            else:
                self.assertIn('轮中进度使用 push_message', note)
                self.assertIn('最终答复必须是控制 JSON 行', note)


if __name__ == '__main__':
    unittest.main()
