"""AstrBot 旧 JSON 决策接入纯文本投递链（Runs/completions 不加兼容分叉）。

AstrBot 后端仍按旧提示词返回 ``{"should_reply": ..., "reply_text": ...}``。
Runs/completions 的 JSON 信封已退役，但 handler 投递只认 ``body`` 与 ``_control``；
归一化只发生在 AstrBot 的路径 B（client 解析分支）与本地拒答结果上，
保证私聊/群决策轮的正文、reply_to、退窗与锚仍按原语义生效。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests import test_runs_terminal as e2  # noqa: E402

from nonebot_plugin_agent_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_agent_adapter.core import control_intent  # noqa: E402
from nonebot_plugin_agent_adapter.core.hermes_client import (  # noqa: E402
    ChatResult,
    _normalize_astrbot_decision,
)
from nonebot_plugin_agent_adapter.core.inflight import make_inflight_key  # noqa: E402


def _astrbot_decision(**fields) -> dict:
    """把旧信封字段归一成 handler 消费的决策形状（与路径 B 同一函数）。"""
    parsed = {"should_reply": True, "reply_text": "你好", **fields}
    return _normalize_astrbot_decision(parsed)


class AstrbotNormalizationTests(unittest.TestCase):
    def test_reply_text_becomes_body_and_control_fields_private(self) -> None:
        parsed = {
            "should_reply": True,
            "reply_text": "你好",
            "reply_to": "m:5",
            "should_exit_active": True,
            "pin_anchor": False,
            "not_final": True,
        }
        decision = _normalize_astrbot_decision(parsed)
        self.assertEqual(decision["body"], "你好")
        self.assertEqual(
            decision["_control"], {"reply_to": "m:5", "should_exit_active": True}
        )
        # 未迁移后端自己的字段保留：不是给 Runs/completions 加的新协议。
        self.assertTrue(decision["not_final"])
        self.assertTrue(decision["should_reply"])
        # 不就地修改解析结果。
        self.assertNotIn("body", parsed)
        self.assertNotIn("_control", parsed)

    def test_existing_body_wins_and_false_controls_are_not_registered(self) -> None:
        decision = _normalize_astrbot_decision(
            {"should_reply": True, "body": "正文优先", "reply_text": "旧文",
             "should_exit_active": False, "release_anchor": ""}
        )
        self.assertEqual(decision["body"], "正文优先")
        self.assertNotIn("_control", decision)

    def test_resolve_control_reads_normalized_control(self) -> None:
        decision = _astrbot_decision(reply_to="m:7", answer_anchor="all")
        effective = control_intent.resolve_control(decision)
        self.assertEqual(effective["reply_to"], "m:7")
        self.assertEqual(effective["answer_anchor"], "all")
        self.assertFalse(effective["should_exit_active"])


class AstrbotClientPathBTests(unittest.IsolatedAsyncioTestCase):
    """真实 HermesClient.chat 的 AstrBot 分支（只换掉 astrbot 传输调用）。"""

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "astrbot")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    async def _chat(self, text: str) -> ChatResult:
        from nonebot_plugin_agent_adapter.core import hermes_client as hc

        completion = SimpleNamespace(text=text, attachments=[])
        with patch.object(
            hc.astrbot_client, "complete_chat_result",
            AsyncMock(return_value=completion),
        ):
            return await hc.hermes_client.chat(
                text="hi", session_key="s", user_id="u1", group_id="g1",
                adapter_name="onebotv11", is_private=False, mode="reactive",
                expect_structured=True, structured_tool_name="submit_decision",
                system_prompt="SYS",
                target=SimpleNamespace(
                    base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t",
                ),
            )

    async def test_path_b_normalizes_reply_text(self) -> None:
        result = await self._chat(
            '{"should_reply": true, "reply_text": "你好", "reply_to": "m:5"}'
        )
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], "你好")
        self.assertEqual(result.structured["_control"]["reply_to"], "m:5")

    async def test_path_b_silent_decision_stays_silent(self) -> None:
        result = await self._chat('{"should_reply": false}')
        self.assertFalse(result.parse_failed)
        self.assertEqual(result.structured["body"], "")
        self.assertFalse(result.structured["should_reply"])


class AstrbotPassiveTests(e2._PassiveHarness):
    """真实 _run_passive_turn：AstrBot 私聊决策轮必须真实送出正文与引用。"""

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "astrbot")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    async def test_astrbot_private_reply_text_delivered(self) -> None:
        recorded = []

        def _resolve(*args, **kwargs):
            recorded.append(kwargs.get("reply_to"))
            return None

        result = ChatResult(
            raw_text='{"should_reply": true, "reply_text": "你好", "reply_to": "m:5"}',
            structured=_astrbot_decision(reply_to="m:5"),
        )
        calls = await self._run(
            result, tracker=self._tracker(record_delivery=False),
            resolve_reply=MagicMock(side_effect=_resolve),
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["你好"])
        self.assertEqual(recorded, ["m:5"])
        self.assertEqual(calls["settle"], [])


class AstrbotReactiveTests(e2._ReactiveHarness):
    """真实 _run_reactive_turn：AstrBot 群决策轮同样按正文与引用投递。"""

    def setUp(self) -> None:
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "astrbot")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    async def test_astrbot_group_reply_text_delivered(self) -> None:
        recorded = []

        def _resolve(*args, **kwargs):
            recorded.append(kwargs.get("reply_to"))
            return None

        result = ChatResult(
            raw_text='{"should_reply": true, "reply_text": "在的", "reply_to": "m:9"}',
            structured=_astrbot_decision(reply_text="在的", reply_to="m:9"),
        )
        calls = await self._run(
            result, tracker=self._tracker(),
            resolve_reply=MagicMock(side_effect=_resolve),
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["在的"])
        self.assertEqual(recorded, ["m:9"])
        self.assertEqual(calls["settle"], [])


class AstrbotGroupScopeTests(unittest.TestCase):
    """群聊决策的控制键与私聊隔离（同一归一化结果不串会话）。"""

    def test_private_and_group_keys_are_distinct(self) -> None:
        decision = _astrbot_decision(reply_to="m:5")
        self.assertEqual(control_intent.resolve_control(decision)["reply_to"], "m:5")
        self.assertNotEqual(
            make_inflight_key("onebotv11", True, "u1", None),
            make_inflight_key("onebotv11", False, "u1", "g1"),
        )


if __name__ == "__main__":
    unittest.main()
