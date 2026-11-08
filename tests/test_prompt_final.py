"""用户第三版定稿：验证实际四路转录与模式隔离。"""
from __future__ import annotations

import json
import re
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb


class FinalPromptTests(unittest.TestCase):
    def test_dynamic_frame_prose_uses_fullwidth_comma_without_breaking_addresses(self):
        for addr in (None, ('image', 123, 4), ('sticker', 123, 4)):
            for kind in ('image', 'sticker'):
                label = pb._media_frame_label(addr, 0, 2, kind)
                self.assertIn('第1/2帧，同一动图按时间顺序', label)
                self.assertNotIn('帧,', label)
                if addr is not None:
                    self.assertIn('m:123#4', label)
        tool = Path(__file__).resolve().parents[1] / 'mcp/tools/get_message_images.py'
        self.assertNotIn('帧,同一动图按时间顺序', tool.read_text())

    def prompts(self, mode):
        library = type('Library', (), {'kinds': lambda self: ['开心', '疑惑']})()
        with patch.object(plugin_config, 'hermes_api_interface', mode), \
                patch.object(pb, 'sticker_feature_enabled', return_value=True), \
                patch.object(pb, 'default_sticker_library', return_value=library):
            return {
                'private': pb.build_passive_system_prompt(
                    adapter='onebotv11', is_private=True, user_id='u1', group_id=None,
                    private_decision=True, group_profile_enabled=True,
                    memory_root='/test/memory/onebotv11',
                ),
                'group': pb.build_reactive_system_prompt(
                    profile_block='<group_profile>测试群。</group_profile>',
                    memory_root='/test/memory/onebotv11', adapter='onebotv11',
                    group_id='g1', self_id='999',
                ),
            }

    def test_final_visibility_and_mode_rules(self):
        for mode in ('runs', 'completions'):
            for scope, prompt in self.prompts(mode).items():
                with self.subTest(mode=mode, scope=scope):
                    self.assertIn('用户只能看到决策信封中的 body 或 push_message 工具推送的消息。', prompt)
                    self.assertIn('用户明显还要继续说话、说话说一半、意思没表达完整时，本轮不回答、不调用工具，直接输出空 body', prompt)
                    self.assertIn('如果上一轮回复已经涵盖用户后续补充的内容，本轮保持静默，不要重复回复。', prompt)
                    self.assertIn('闲聊时输出尽可能单句不分段，简短，不超过15字。', prompt)
                    self.assertNotRegex(prompt, r'(?m)^[%#]|[%#]$')
                    if mode == 'runs':
                        self.assertNotIn('not_final', prompt)
                        self.assertNotIn('使用 push_message 工具', prompt)
                        self.assertIn('依段落拆分为多个决策信封一次发送', prompt)
                        self.assertIn('随后在当前 run 调用工具继续任务。', prompt)
                    else:
                        self.assertIn('not_final=true', prompt)
                        self.assertIn('先使用 push_message 工具输出消息', prompt)
                        self.assertIn('最终答复必须写成完整决策信封。', prompt)
                        self.assertIn('由 not_final 续跑后，调用工具继续任务。', prompt)
                        self.assertNotIn('随后在当前 run', prompt)
                    if scope == 'private':
                        self.assertIn('私聊只有对方一个人，不必 @ 任何人', prompt)
                        self.assertNotIn('正文内联 @', prompt)
                    else:
                        self.assertNotIn('私聊只有对方一个人', prompt)
                        self.assertIn('[CQ:at,qq=<QQ号>]', prompt)
                        self.assertIn('退出门槛', prompt)
                        self.assertIn('引用后不要再手写同一个首位 @', prompt)
                    self.assertIn('第一次使用某个表情、还不清楚内容时', prompt)
                    self.assertIn('当前可用表情类型：开心／疑惑。', prompt)
                    self.assertIn('普通用户个人画像', prompt)
                    self.assertIn('主人/管理员的通用偏好', prompt)
                    self.assertIn('refs=[{"message_id": X, "idx": N}]', prompt)
                    self.assertIn('/test/memory/onebotv11/people/<QQ号>.md', prompt)
                    self.assertNotIn('get_message_files ，', prompt)
                    self.assertNotIn('CQ 标记 不要修改', prompt)
                    for body in re.findall(r'(?m)^\s*(\{"body":.*\})$', prompt):
                        self.assertIsInstance(json.loads(body)['body'], str)

    def test_chinese_prose_punctuation_protects_technical_examples(self):
        for mode in ('runs', 'completions'):
            for scope, prompt in self.prompts(mode).items():
                with self.subTest(mode=mode, scope=scope):
                    self.assertNotIn('「', prompt)
                    self.assertNotIn('」', prompt)
                    # 技术片段按字节保护，不把 JSON／CQ／路径／消息地址纳入中文标点。
                    masked = re.sub(r'`[^`]*`|\[[^\]\n]*\]|\{[^\n]*\}|<[^>]*>|\(引用 昵称\(id\): …\)|X: \.\.\.Y\.\.\.|\bm:[^\s，。；]*|/test/[^\s。]+', '', prompt)
                    self.assertNotRegex(masked, r'[\u4e00-\u9fff][,;:?!()]|[,;:?!()][\u4e00-\u9fff]')


if __name__ == '__main__':
    unittest.main()
