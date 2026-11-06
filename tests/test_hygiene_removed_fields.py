"""卫生专项：已移除字段与死写点的回归断言。

覆盖本批明确删除的 observable 合同：
- topic_hint 不再出现在决策字段表、reactive prompt、runtime_state 形参、
  list_active_sessions 输出与 /hermes-status 展示里；
- 旧会话/旧持久化行里残留的 topic_hint 按未知字段忽略，不恢复业务状态；
- 离场任务便签的只写不弹登记点（task_notes.set_task_note / pop_task_note /
  message_feedback._record_background_note）已删除，插件提示便签仍然保留。
"""

from __future__ import annotations

import inspect
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import decision_contract, prompt_builder  # noqa: E402
from nonebot_plugin_hermes_adapter.core import task_notes  # noqa: E402
from nonebot_plugin_hermes_adapter.core.active_session import ActiveSessionManager  # noqa: E402
from nonebot_plugin_hermes_adapter.core.npha_terminal import decode_terminal  # noqa: E402
from nonebot_plugin_hermes_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.window_store import (  # noqa: E402
    session_from_payload,
    session_to_payload,
)
from nonebot_plugin_hermes_adapter.handlers import commands, message_feedback  # noqa: E402
from nonebot_plugin_hermes_adapter.mcp.tools import list_active_sessions  # noqa: E402


class TopicHintRemovalTests(unittest.TestCase):
    def test_decision_contract_has_no_topic_hint(self) -> None:
        names = [field.name for field in decision_contract.DECISION_FIELDS]
        self.assertNotIn("topic_hint", names)
        self.assertNotIn("topic_hint", decision_contract.render_cn_field_lines())
        self.assertNotIn("topic_hint", decision_contract.render_en_field_lines())
        self.assertNotIn("topic_hint", decision_contract.render_field_name_list())

    def test_reactive_prompts_have_no_topic_hint(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config

        previous = plugin_config.hermes_api_interface
        try:
            for interface in ("completions", "runs", "astrbot"):
                object.__setattr__(plugin_config, "hermes_api_interface", interface)
                prompt = prompt_builder.build_reactive_system_prompt()
                self.assertNotIn("topic_hint", prompt, msg=interface)
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)

    def test_runtime_state_builder_has_no_topic_hint_parameter(self) -> None:
        params = inspect.signature(prompt_builder.build_reactive_user_content).parameters
        self.assertNotIn("topic_hint", params)

    def test_legacy_control_line_topic_hint_is_ignored(self) -> None:
        decision = decode_terminal('{"topic_hint":"收尾"}\n{"body":"正文"}')
        self.assertEqual(decision.body, "正文")
        self.assertNotIn("topic_hint", decision.control)

    def test_active_session_has_no_topic_hint(self) -> None:
        manager = ActiveSessionManager()
        session = manager.trigger("onebotv11", "g1", "u1", now_ms=1000)
        self.assertFalse(hasattr(session, "topic_hint"))
        self.assertFalse(hasattr(manager, "update_topic"))

    def test_old_window_payload_topic_hint_is_ignored(self) -> None:
        session = session_from_payload(
            "onebotv11",
            "g1",
            {
                "triggered_by": "u1",
                "started_at": 1,
                "last_active_at": 2,
                "expires_at": 3,
                "topic_hint": "旧话题",
            },
        )
        self.assertFalse(hasattr(session, "topic_hint"))
        self.assertNotIn("topic_hint", session_to_payload(session))

    def test_status_lines_do_not_render_topic_hint(self) -> None:
        manager = ActiveSessionManager()
        session = manager.trigger("onebotv11", "g1", "u1", now_ms=1000)
        # 旧实例可能在内存里带 topic_hint 属性；展示端不得再读它。
        session.topic_hint = "旧话题"
        with patch.object(commands._mcp, "active_sessions", manager), \
                patch.object(commands._mcp, "message_buffer", None), \
                patch.object(commands._mcp, "bot_registry", None):
            text = "\n".join(commands.build_status_lines(1000))
        self.assertNotIn("topic=", text)
        self.assertIn("onebotv11/g1", text)

    def test_background_note_dead_write_api_removed(self) -> None:
        self.assertFalse(hasattr(task_notes, "set_task_note"))
        self.assertFalse(hasattr(task_notes, "pop_task_note"))
        self.assertFalse(hasattr(message_feedback, "_record_background_note"))
        # 现行插件提示便签仍读写：清理不得误删真实提示链。
        self.assertTrue(callable(task_notes.push_plugin_note))
        self.assertTrue(callable(task_notes.pop_plugin_notes))


class ListActiveSessionsTopicHintTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_active_sessions_has_no_topic_hint(self) -> None:
        manager = ActiveSessionManager()
        session = manager.trigger("onebotv11", "g1", "u1", now_ms=1000)
        session.topic_hint = "旧话题"
        result = await list_active_sessions.list_active_sessions_impl(
            list_active_sessions.ListActiveSessionsInput(),
            active_sessions=manager,
            scope=CallerScope.dev(),
            now_ms=1000,
        )
        payload = result.model_dump()
        self.assertEqual(len(payload["sessions"]), 1)
        self.assertNotIn("topic_hint", payload["sessions"][0])


if __name__ == "__main__":
    unittest.main()
