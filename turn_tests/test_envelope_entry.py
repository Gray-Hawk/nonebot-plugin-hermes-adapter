"""passive turn 入口：正文即消息，协议失败 fail-closed，不再有信封打回/自愈便签。"""
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from nonebot_plugin_hermes_adapter.core import prompt_builder  # noqa: E402
from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt  # noqa: E402


def _stub_everything(calls, result):
    """把 passive turn 的依赖全换成可用的 stub，只为走到正文投递分支。"""
    cfg = MagicMock()
    cfg.group_profile_enabled = False
    cfg.sticker_vision_enabled = False
    cfg.history_message_limit = 5
    return {
        "load_misc_config": MagicMock(return_value=cfg),
        "load_chat_config": MagicMock(return_value=cfg),
        "load_media_config": MagicMock(return_value=cfg),
        "session_manager": MagicMock(get_session_key=MagicMock(return_value="sess")),
        "prepare_companion_run": AsyncMock(return_value=True),
        "_current_delivery_generation": MagicMock(return_value=None),
        "resolve_target": MagicMock(return_value=object()),
        "_filter_current_and_steered": MagicMock(return_value=[]),
        "resolve_custom_dir": MagicMock(return_value=None),
        "build_passive_system_prompt": MagicMock(return_value="sys"),
        "build_group_profile_block": MagicMock(return_value=""),
        "build_media_address_index": MagicMock(return_value={}),
        "_collect_inline_image_urls": MagicMock(return_value=[]),
        "inline_image_frames": AsyncMock(return_value=[]),
        "inline_gif_frames": AsyncMock(return_value=[]),
        "_absent_media_note": MagicMock(return_value=""),
        "_chat_with_overflow_recovery": AsyncMock(return_value=result),
        "_fire_followup_turn": MagicMock(side_effect=lambda **kw: calls["followup"].append(kw)),
        "_settle": MagicMock(side_effect=lambda res, outcome, reason: calls["settle"].append((outcome, reason))),
        "_delivery_identity": MagicMock(return_value=(MagicMock(), None)),
        "_schedule_reconcile": MagicMock(),
        "_finish_history": MagicMock(),
        "send_text_with_media": AsyncMock(
            side_effect=lambda *a, **k: calls["send"].append(
                a[2] if len(a) > 2 else (k.get("text") or "text")
            )
        ),
    }


class PassiveTurnTextProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, result, *, on_chat=None):
        calls = {"note": [], "followup": [], "settle": [], "send": [], "continuation": []}
        patches = _stub_everything(calls, result)

        async def _chat(*_a, **_k):
            if on_chat is not None:
                on_chat()
            return result

        patches["_chat_with_overflow_recovery"] = _chat
        patches["_fire_continuation"] = AsyncMock(
            side_effect=lambda **kw: calls["continuation"].append(kw)
        )
        note_mock = MagicMock(side_effect=lambda key, name, text, **kw: calls["note"].append(name))
        patches["task_notes"] = MagicMock(push_plugin_note=note_mock)

        prompts = patch.object(prompt_builder, "render_current_rows", MagicMock(return_value="hi"))
        prompts.start()
        self.addCleanup(prompts.stop)
        with patch.multiple(mt, **patches):
            await mt._run_passive_turn(
                bot=MagicMock(self_id=1), target=object(), adapter_name="onebotv11",
                user_id="u1", group_id=None, text="hi", image_urls=[],
                is_private=True, now_ms=1,
            )
        return calls

    async def test_protocol_failure_is_fail_closed(self):
        result = ChatResult(raw_text="超长正文", parse_failed=True)
        result.parse_failed_reason = "text_body_overflow"
        calls = await self._run(result)
        self.assertEqual(calls["send"], [])                      # 零发送
        self.assertEqual(calls["note"], [])                      # 无自愈/打回便签
        self.assertEqual(calls["followup"], [])                  # 不重跑一轮
        self.assertEqual(calls["settle"], [("failed", "text_body_overflow")])

    async def test_plain_text_units_sent_in_order(self):
        result = ChatResult(raw_text="你好\n§\n再见", structured={"body": "你好", "should_reply": True})
        result.terminal_decisions = [
            {"body": "你好", "should_reply": True},
            {"body": "再见", "should_reply": True},
        ]
        calls = await self._run(result)
        self.assertEqual(calls["send"], ["你好", "再见"])
        self.assertEqual(calls["note"], [])
        self.assertEqual(calls["followup"], [])

    async def test_legacy_json_line_is_sent_verbatim(self):
        legacy = '{"body":"旧格式","reply_to":"m:1"}'
        result = ChatResult(raw_text=legacy)
        result.terminal_decisions = [{"body": legacy, "should_reply": True}]
        calls = await self._run(result)
        self.assertEqual(calls["send"], [legacy])                # 逐字一条，不识别控制
        self.assertEqual(calls["note"], [])

    async def test_request_continuation_is_consumed_once_after_send(self):
        from nonebot_plugin_hermes_adapter.core import continuation
        from nonebot_plugin_hermes_adapter.core.inflight import make_inflight_key

        key = make_inflight_key("onebotv11", True, "u1", None)
        self.addCleanup(continuation.discard_continuation, key)
        result = ChatResult(raw_text="收到", structured={"body": "收到", "should_reply": True})
        result.terminal_decisions = [{"body": "收到", "should_reply": True}]
        calls = await self._run(
            result, on_chat=lambda: continuation.request_continuation(key),
        )
        self.assertEqual(calls["send"], ["收到"])
        self.assertEqual(len(calls["continuation"]), 1)      # 一轮只续跑一次
        self.assertFalse(continuation.pending_continuation(key))

    async def test_stale_continuation_from_previous_turn_is_dropped(self):
        from nonebot_plugin_hermes_adapter.core import continuation
        from nonebot_plugin_hermes_adapter.core.inflight import make_inflight_key

        key = make_inflight_key("onebotv11", True, "u1", None)
        self.addCleanup(continuation.discard_continuation, key)
        continuation.request_continuation(key)                # 上一轮残留
        result = ChatResult(raw_text="收到", structured={"body": "收到", "should_reply": True})
        result.terminal_decisions = [{"body": "收到", "should_reply": True}]
        calls = await self._run(result)
        self.assertEqual(calls["continuation"], [])          # 不凭陈旧标记续跑
        self.assertFalse(continuation.pending_continuation(key))

    async def test_empty_output_stays_silent(self):
        result = ChatResult(raw_text="")
        result.terminal_decisions = []
        calls = await self._run(result)
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["note"], [])


if __name__ == "__main__":
    unittest.main()
