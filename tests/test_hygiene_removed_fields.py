"""卫生专项：已移除字段与死写点的回归断言。

覆盖本批明确删除的 observable 合同：
- `background_task` 退出决策字段表与各接口 prompt：后台状态改由插件从真实登记
  查询（get_background_tasks），模型不再自报任务便签；
- 旧信封/旧持久化行里残留的未知旧字段按未知字段忽略，不恢复业务状态，也不会
  被重新写成现行或专用兼容字段；
- 离场任务便签的只写不弹登记点（task_notes.set_task_note / pop_task_note /
  message_feedback._record_background_note）已删除，插件提示便签仍然保留。
"""

from __future__ import annotations

import unittest
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import decision_contract, prompt_builder  # noqa: E402
from nonebot_plugin_hermes_adapter.core import task_notes  # noqa: E402
from nonebot_plugin_hermes_adapter.core.npha_terminal import decode_terminal  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.window_store import (  # noqa: E402
    session_from_payload,
    session_to_payload,
)
from nonebot_plugin_hermes_adapter.handlers import message_feedback  # noqa: E402


class RemovedFieldHygieneTests(unittest.TestCase):
    def test_decision_contract_has_no_background_task(self) -> None:
        names = [field.name for field in decision_contract.DECISION_FIELDS]
        self.assertNotIn("background_task", names)
        for rendered in (
            decision_contract.render_cn_field_lines(),
            decision_contract.render_en_field_lines(),
            decision_contract.render_field_name_list(),
        ):
            self.assertNotIn("background_task", rendered)

    def test_prompts_do_not_offer_background_task(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config

        previous = plugin_config.hermes_api_interface
        try:
            for interface in ("completions", "runs", "astrbot"):
                object.__setattr__(plugin_config, "hermes_api_interface", interface)
                prompts = {
                    "reactive": prompt_builder.build_reactive_system_prompt(),
                    "private": prompt_builder.build_passive_system_prompt(
                        adapter="onebotv11", is_private=True, user_id="u1",
                        group_id=None, private_decision=True,
                    ),
                }
                for path, prompt in prompts.items():
                    with self.subTest(interface=interface, path=path):
                        self.assertNotIn("background_task 便签", prompt)
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)

    def test_departure_prompt_does_not_ask_for_a_task_note(self) -> None:
        # 字段口径（便签）不得再出现；工具名 get_background_tasks 里的子串不算字段。
        self.assertNotIn("background_task 便签", message_feedback._DEPARTURE_CONFIRM_PROMPT)
        self.assertNotIn("background_task=", message_feedback._DEPARTURE_CONFIRM_PROMPT)
        # 离场提示改为指向真实登记查询，而不是让模型自报。
        self.assertIn("get_background_tasks", message_feedback._DEPARTURE_CONFIRM_PROMPT)

    def test_legacy_control_line_unknown_field_is_ignored(self) -> None:
        decision = decode_terminal('{"legacy_unknown":"收尾"}\n{"body":"正文"}')
        self.assertEqual(decision.body, "正文")
        self.assertNotIn("legacy_unknown", decision.control)

    def test_retired_background_task_field_is_ignored(self) -> None:
        decision = decode_terminal('{"background_task":"旧便签"}\n{"body":"正文"}')
        self.assertEqual(decision.body, "正文")
        self.assertNotIn("background_task", decision.control)

    def test_old_window_payload_unknown_field_is_ignored(self) -> None:
        session = session_from_payload(
            "onebotv11",
            "g1",
            {
                "triggered_by": "u1",
                "started_at": 1,
                "last_active_at": 2,
                "expires_at": 3,
                "legacy_unknown": "旧字段",
            },
        )
        self.assertFalse(hasattr(session, "legacy_unknown"))
        self.assertNotIn("legacy_unknown", session_to_payload(session))

    def test_background_note_dead_write_api_removed(self) -> None:
        self.assertFalse(hasattr(task_notes, "set_task_note"))
        self.assertFalse(hasattr(task_notes, "pop_task_note"))
        self.assertFalse(hasattr(message_feedback, "_record_background_note"))
        # 现行插件提示便签仍读写：清理不得误删真实提示链。
        self.assertTrue(callable(task_notes.push_plugin_note))
        self.assertTrue(callable(task_notes.pop_plugin_notes))


if __name__ == "__main__":
    unittest.main()
