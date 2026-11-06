import asyncio
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
# 先让真 mcp 包进 sys.modules，避免被本地 work/mcp 同名目录遮住（后者是插件源码目录）
sys.path[:] = [p for p in sys.path if Path(p or os.getcwd()).resolve() != ROOT]
import mcp.types  # noqa: E402,F401
sys.path.insert(0, str(ROOT))




def _load():
    """把工作树挂成包名导入，确保测的是本地改动而不是 site-packages 里的旧副本。"""
    import importlib.util

    name = "nonebot_plugin_hermes_adapter"
    current = sys.modules.get(name)
    if current is None or ROOT not in Path(getattr(current, "__file__", "")).parents:
        spec = importlib.util.spec_from_file_location(
            name, ROOT / "__init__.py", submodule_search_locations=[str(ROOT)]
        )
        for cached in [k for k in sys.modules if k == name or k.startswith(name + ".")]:
            del sys.modules[cached]
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
    from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

    return mt, ChatResult


def _stub_everything(calls, result):
    """把 passive turn 的依赖全换成可用的 stub，只为走到信封判定分支。"""
    cfg = MagicMock()
    cfg.group_profile_enabled = False
    cfg.sticker_vision_enabled = False
    cfg.history_message_limit = 5
    patches = {
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
    return patches


class PassiveTurnEnvelopeTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, result):
        mt, _ = _load()
        calls = {"note": [], "followup": [], "settle": [], "send": []}
        patches = _stub_everything(calls, result)
        note_mock = MagicMock(side_effect=lambda key, name, text, **kw: calls["note"].append(name))
        patches["task_notes"] = MagicMock(push_plugin_note=note_mock)
        from nonebot_plugin_hermes_adapter.core import prompt_builder

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

    async def test_reject_reason_resubmits_at_turn_entry(self):
        _, ChatResult = _load()
        result = ChatResult(raw_text="{}", parse_failed=True)
        result.parse_failed_reason = "envelope_fields_missing"
        calls = await self._run(result)
        self.assertIn("envelope_rejected", calls["note"], msg=str(calls))
        self.assertEqual(len(calls["followup"]), 1)
        self.assertEqual(calls["followup"][0]["tag"], "bounce:envelope-envelope_fields_missing")
        self.assertEqual(calls["settle"], [("suppressed", "envelope_rejected")])
        self.assertEqual(calls["send"], [])

    async def test_a_class_hint_no_resubmit(self):
        mt, ChatResult = _load()
        from nonebot_plugin_hermes_adapter.core.npha_terminal import ENVELOPE_MISSING_HINT
        result = ChatResult(raw_text="hi", structured={
            "body": "hi", "should_reply": True, "_self_heal_hint": ENVELOPE_MISSING_HINT
        })
        calls = await self._run(result)
        self.assertEqual(calls["note"], ["envelope_missing"])
        self.assertEqual(calls["followup"], [])

    async def _run_reactive(self, result):
        mt, _ = _load()
        calls = {"note": [], "followup": [], "settle": [], "send": []}
        patches = _stub_everything(calls, result)
        patches.update({
            "get_bot_nickname": AsyncMock(return_value="nano"),
            "build_reactive_system_prompt": MagicMock(return_value="sys"),
            "split_unpushed_messages": MagicMock(return_value=([], None)),
            "session_manager": MagicMock(
                get_push_watermark=MagicMock(return_value=None),
                get_session_key=MagicMock(return_value="sess"),
            ),
            "ActiveSession": MagicMock(return_value=SimpleNamespace(
                generation=1, last_bot_reply_at=None, session_id="sess",
                triggered_by="u1", participants=set(),
                should_exit_active=False, interim_echo_enabled=False,
            )),
            "_mcp": MagicMock(
                active_sessions=MagicMock(get_if_trigger_alive=MagicMock(return_value=None)),
                message_buffer=MagicMock(get_recent=MagicMock(return_value=[])), run_tracker=None,
            ),
            "task_notes": MagicMock(push_plugin_note=MagicMock(
                side_effect=lambda key, name, text, **kw: calls["note"].append(name))),
        })
        from nonebot_plugin_hermes_adapter.core import prompt_builder

        prompts = patch.object(prompt_builder, "render_current_rows", MagicMock(return_value="hi"))
        prompts.start()
        self.addCleanup(prompts.stop)
        with patch.multiple(mt, **patches):
            await mt._run_reactive_turn(
                bot=MagicMock(self_id=1), target=object(), adapter_name="onebotv11",
                user_id="u1", group_id="g1", text="hi", image_urls=[],
                is_direct=True, addressed_to_bot=True, now_ms=1, recent_override=[], ephemeral=True,
            )
        return calls

    async def test_broken_fullwidth_envelope_sends_clean_body(self):
        mt, ChatResult = _load()
        from nonebot_plugin_hermes_adapter.core.npha_terminal import (
            decode_terminal, structured_from_terminal,
        )
        ctrl = ('｛"reply_to"："m:1"，"topic_hint"："t"，"release_anchor"：""，"answer_anchor"：""，'
                '"anchor_merge"：""，"background_task"：""，"not_final"：false，'
                '"should_exit_active"：false，"pin_anchor"：false｝')
        text = ctrl + "\n" + '｛"body"："正文A"｝'
        result = ChatResult(raw_text=text, structured=structured_from_terminal(decode_terminal(text)))
        calls = await self._run(result)
        self.assertIn("正文A", " ".join(str(c) for c in calls["send"]))
        self.assertIn("envelope_broken", calls["note"])
        self.assertEqual(calls["followup"], [])

    async def test_reactive_reject_resubmits(self):
        mt, ChatResult = _load()
        result = ChatResult(raw_text="{}", parse_failed=True)
        result.parse_failed_reason = "multiline_envelope"
        calls = await self._run_reactive(result)
        self.assertIn("envelope_rejected", calls["note"], msg=str(calls))
        self.assertEqual(len(calls["followup"]), 1)
        self.assertEqual(calls["followup"][0]["tag"], "bounce:envelope-multiline_envelope")
        self.assertEqual(calls["settle"], [("suppressed", "envelope_rejected")])
        self.assertEqual(calls["send"], [])

    def test_fire_followup_accepts_bounce_namespace(self):
        """真实 _fire_followup_turn：tag 走 bounce 命名空间能到 spawn，不抛 KeyError。"""
        from unittest.mock import patch as _patch
        from nonebot_plugin_hermes_adapter.handlers import message_relay as relay

        spawned = []
        with _patch.object(relay, "spawn_bg_task", side_effect=lambda coro: (spawned.append(coro), coro.close())):
            relay._fire_followup_turn(
                key=("g1", "u1"), tag="bounce:envelope-multiline_envelope",
                prompt="重写", bot=MagicMock(self_id=1), target=object(),
                adapter_name="onebotv11", user_id="u1", group_id="g1", is_private=False,
            )
        self.assertEqual(len(spawned), 1)

    async def test_quoted_body_kept_whole(self):
        mt, ChatResult = _load()
        from nonebot_plugin_hermes_adapter.core.npha_terminal import (
            decode_terminal, structured_from_terminal,
        )
        ctrl = '｛"reply_to"："m:1"，"topic_hint"："t"，"release_anchor"：""，"answer_anchor"：""，' \
               '"anchor_merge"：""，"background_task"：""，"not_final"：false，' \
               '"should_exit_active"：false，"pin_anchor"：false｝'
        text = ctrl + "\n" + '｛"body"："他说：\\"你好\\"，然后走了"｝'
        result = ChatResult(raw_text=text, structured=structured_from_terminal(decode_terminal(text)))
        calls = await self._run(result)
        joined = " ".join(str(c) for c in calls["send"])
        self.assertIn("然后走了", joined)

    async def test_control_value_invalid_rejected_and_resubmitted(self):
        mt, ChatResult = _load()
        result = ChatResult(raw_text="{}", parse_failed=True)
        result.parse_failed_reason = "control_value_invalid"
        calls = await self._run(result)
        self.assertIn("envelope_rejected", calls["note"])
        self.assertEqual(len(calls["followup"]), 1)
        self.assertEqual(calls["followup"][0]["tag"], "bounce:envelope-control_value_invalid")
        self.assertEqual(calls["settle"], [("suppressed", "envelope_rejected")])
        self.assertEqual(calls["send"], [])

    async def test_envelope_broken_rejected_and_resubmitted(self):
        mt, ChatResult = _load()
        result = ChatResult(raw_text="{}", parse_failed=True)
        result.parse_failed_reason = "envelope_broken"
        calls = await self._run(result)
        self.assertIn("envelope_rejected", calls["note"])
        self.assertEqual(calls["followup"][0]["tag"], "bounce:envelope-envelope_broken")
        self.assertEqual(calls["send"], [])
