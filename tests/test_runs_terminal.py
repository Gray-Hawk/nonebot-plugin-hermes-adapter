"""E2：Runs 终态双单行 JSON（多份）解码、整批计划与按份交付。"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.npha_terminal import (  # noqa: E402
    MAX_BODY_CHARS,
    TerminalFormatError,
    decode_terminal,
    decode_terminal_batch,
    structured_batch_from_terminal,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    TerminalPlanError,
    build_batch_plan,
    prepare_terminal_batch,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402


def unit(control: dict | None = None, body: str = "正文") -> str:
    return (
        json.dumps(control or {}, ensure_ascii=False)
        + "\n"
        + json.dumps({"body": body}, ensure_ascii=False)
    )


def batch(*pairs) -> str:
    return "\n".join(unit(control, body) for control, body in pairs)


class BatchDecodeTests(unittest.TestCase):
    def test_single_unit(self) -> None:
        decisions = decode_terminal_batch(unit({"reply_to": "m:1", "not_final": True}, "你好"))
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].body, "你好")
        self.assertEqual(decisions[0].control["reply_to"], "m:1")
        self.assertIs(decisions[0].control["not_final"], True)
        self.assertEqual(decisions[0].reminder, "")

    def test_multiple_units_in_order(self) -> None:
        text = batch(({"reply_to": "m:1"}, "第一条"), ({}, "第二条"), ({"topic_hint": "t"}, "第三条"))
        decisions = decode_terminal_batch(text)
        self.assertEqual([d.body for d in decisions], ["第一条", "第二条", "第三条"])
        self.assertEqual(decisions[0].control["reply_to"], "m:1")
        self.assertEqual(decisions[2].control["topic_hint"], "t")

    def test_blank_line_between_units_allowed(self) -> None:
        text = unit({}, "A") + "\n\n" + unit({}, "B") + "\n"
        self.assertEqual([d.body for d in decode_terminal_batch(text)], ["A", "B"])

    def test_empty_control_object(self) -> None:
        decisions = decode_terminal_batch(batch(({}, "A"), ({}, "B")))
        self.assertEqual([d.body for d in decisions], ["A", "B"])
        self.assertIs(decisions[0].control["not_final"], False)
        self.assertEqual(decisions[0].control["reply_to"], "")

    def test_missing_control_body_only(self) -> None:
        decisions = decode_terminal_batch('{"body":"只有正文"}')
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].body, "只有正文")
        self.assertTrue(decisions[0].reminder)

    def test_missing_control_mixed_with_units(self) -> None:
        text = '{"body":"A"}\n' + unit({"reply_to": "m:2"}, "B")
        self.assertEqual([d.body for d in decode_terminal_batch(text)], ["A", "B"])

    def test_bad_control_still_delivers_body(self) -> None:
        text = '{"reply_to":"m:9",,}\n{"body":"正文照发"}'
        decisions = decode_terminal_batch(text)
        self.assertEqual(decisions[0].body, "正文照发")
        self.assertEqual(decisions[0].control["reply_to"], "m:9")
        self.assertTrue(decisions[0].reminder)

    def test_empty_body_is_silent(self) -> None:
        decisions = decode_terminal_batch(batch(({}, "第一条"), ({}, "")))
        self.assertTrue(decisions[1].silent)
        self.assertFalse(decisions[0].silent)

    def test_same_body_twice_is_two_decisions(self) -> None:
        decisions = decode_terminal_batch(batch(({}, "同一句"), ({}, "同一句")))
        self.assertEqual(len(decisions), 2)
        self.assertEqual([d.body for d in decisions], ["同一句", "同一句"])

    def test_quotes_backslash_emoji_preserved(self) -> None:
        body = '他说："你好\\n第二行"，🙂 emoji'
        decisions = decode_terminal_batch(unit({}, body))
        self.assertEqual(decisions[0].body, body)

    def test_long_body_delivers_intact(self) -> None:
        # E1 定版：合法 JSON 正文走标准解码，不做 4000 截断（与 completions 同口径）
        body = "长" * (MAX_BODY_CHARS + 1)
        decisions = decode_terminal_batch(unit({}, body))
        self.assertEqual(decisions[0].body, body)

    def test_naked_body_over_limit_rejected(self) -> None:
        with self.assertRaises(TerminalFormatError) as ctx:
            decode_terminal_batch("x" * (MAX_BODY_CHARS + 1))
        self.assertEqual(ctx.exception.reason, "naked_overflow")

    def test_naked_body_is_single_decision(self) -> None:
        text = "普通一句话\n第二行 {not_final:true}"
        decisions = decode_terminal_batch(text)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].body, text)
        self.assertTrue(decisions[0].reminder)

    def test_illegal_trailing_plain_line_rejected(self) -> None:
        text = unit({}, "A") + "\n尾随文字"
        with self.assertRaises(TerminalFormatError) as ctx:
            decode_terminal_batch(text)
        self.assertEqual(ctx.exception.reason, "residue")

    def test_complete_unit_plus_residue_rejected(self) -> None:
        text = unit({}, "A") + "\n" + json.dumps({"reply_to": "m:2"})
        with self.assertRaises(TerminalFormatError):
            decode_terminal_batch(text)

    def test_complete_units_plus_garbage_rejected(self) -> None:
        text = batch(({}, "A"), ({}, "B")) + "\nxxx"
        with self.assertRaises(TerminalFormatError):
            decode_terminal_batch(text)

    def test_control_without_body_rejected(self) -> None:
        text = json.dumps({"reply_to": "m:1"}) + "\n" + json.dumps({"reply_to": "m:2"})
        with self.assertRaises(TerminalFormatError) as ctx:
            decode_terminal_batch(text)
        self.assertEqual(ctx.exception.reason, "control_without_body")

    def test_empty_text_rejected(self) -> None:
        for text in ("", "   ", "\n\n"):
            with self.assertRaises(TerminalFormatError):
                decode_terminal_batch(text)

    def test_fullwidth_envelope_single_and_batch(self) -> None:
        text = '｛"reply_to"："m:7"，"not_final"：true｝\n｛"body"："正文"｝'
        one = decode_terminal_batch(text)
        self.assertEqual(one[0].body, "正文")
        self.assertEqual(one[0].control["reply_to"], "m:7")
        two = decode_terminal_batch(text + "\n" + text)
        self.assertEqual([d.body for d in two], ["正文", "正文"])

    def test_completions_decoder_still_rejects_multi(self) -> None:
        # E1 定版：completions 只接受恰好一份，多份必须失败
        with self.assertRaises(TerminalFormatError):
            decode_terminal(batch(({}, "A"), ({}, "B")))

    def test_structured_batch_shape(self) -> None:
        payload = structured_batch_from_terminal(decode_terminal_batch(batch(({}, "A"), ({}, ""))))
        self.assertEqual([item["body"] for item in payload], ["A", ""])
        self.assertEqual([item["should_reply"] for item in payload], [True, False])
        self.assertTrue(all("reply_text" not in item for item in payload))


class TerminalPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tracker = RunTracker(Path(self._tmp.name) / "tracker.db")
        self.run_id = "run-e2-1"
        self._seed_delivery()

    def tearDown(self) -> None:
        self.tracker.close()
        self._tmp.cleanup()

    def _seed_delivery(self) -> None:
        self.tracker.record_submission(
            idempotency_key="idem-1", request_body={"input": "hi"},
            target_fingerprint="onebotv11|http://x", scope_key="qq:g:1",
            session_key="sess", mode="reactive", expect_structured=True, bot_self_id="999",
        )
        self.tracker.bind_submission_and_register(
            idempotency_key="idem-1", run_id=self.run_id, scope_key="qq:g:1",
            adapter="onebotv11", is_private=False, user_id="u1", group_id="g1",
            bot_self_id="999", session_key="sess", mode="reactive", expect_structured=True,
        )
        self.tracker.record_expected_delivery(
            run_id=self.run_id, output="o", expect_structured=True
        )

    def _units(self) -> list[dict]:
        return [
            {"body": "第一条", "media_urls": [], "file_paths": [], "reply_to": "m:1"},
            {"body": "第二条", "media_urls": [], "file_paths": [], "reply_to": None},
        ]

    def test_plan_persists_all_components(self) -> None:
        contexts = prepare_terminal_batch(
            tracker=self.tracker, run_id=self.run_id, units=self._units()
        )
        self.assertEqual([c.component_id for c in contexts], ["primary:0", "primary:1"])
        comps = {c["component_id"]: c for c in self.tracker.list_components(self.run_id)}
        self.assertEqual(set(comps), {"primary:0", "history:0", "primary:1", "history:1"})
        plan = json.loads(self.tracker.get_delivery(self.run_id)["plan_json"])
        self.assertTrue(plan["batch"])
        self.assertEqual([u["cleaned_text"] for u in plan["units"]], ["第一条", "第二条"])
        self.assertEqual(plan["units"][0]["reply_to"], "m:1")

    def test_repeated_prepare_keeps_plan_and_sent_state(self) -> None:
        prepare_terminal_batch(tracker=self.tracker, run_id=self.run_id, units=self._units())
        before = self.tracker.get_delivery(self.run_id)["plan_json"]
        self.tracker.mark_component_sent(
            run_id=self.run_id, component_id="primary:0", msg_ids=["m:1"]
        )
        # 恢复路径重复调用：同一 output → 逐字相同计划，不拒绝、不重置已 sent
        prepare_terminal_batch(tracker=self.tracker, run_id=self.run_id, units=self._units())
        self.assertEqual(self.tracker.get_delivery(self.run_id)["plan_json"], before)
        comp = self.tracker.get_component(run_id=self.run_id, component_id="primary:0")
        self.assertEqual(comp["state"], "sent")
        self.assertEqual(comp["msg_ids"], ["m:1"])

    def test_changed_plan_is_rejected(self) -> None:
        prepare_terminal_batch(tracker=self.tracker, run_id=self.run_id, units=self._units())
        changed = self._units()
        changed[1]["body"] = "改过的第二条"
        with self.assertRaises(TerminalPlanError):
            prepare_terminal_batch(tracker=self.tracker, run_id=self.run_id, units=changed)

    def test_missing_ledger_row_fails_explicitly(self) -> None:
        with self.assertRaises(TerminalPlanError):
            prepare_terminal_batch(tracker=self.tracker, run_id="run-missing", units=self._units())
        with self.assertRaises(TerminalPlanError):
            prepare_terminal_batch(tracker=None, run_id=None, units=self._units())

    def test_file_components_are_per_unit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.txt"
            path.write_text("hello", encoding="utf-8")
            units = self._units()
            units[1]["file_paths"] = [str(path)]
            prepare_terminal_batch(tracker=self.tracker, run_id=self.run_id, units=units)
            comps = {c["component_id"] for c in self.tracker.list_components(self.run_id)}
            self.assertIn("file:1:0", comps)

    def test_build_batch_plan_hash_is_stable(self) -> None:
        plan_a, comps_a = build_batch_plan(self._units())
        plan_b, comps_b = build_batch_plan(self._units())
        self.assertEqual(json.dumps(plan_a, sort_keys=True), json.dumps(plan_b, sort_keys=True))
        self.assertEqual(comps_a, comps_b)


class _ReactiveHarness(unittest.IsolatedAsyncioTestCase):
    """复用 turn 里的真实 _run_reactive_turn，只换掉外部 I/O。"""

    def _tracker(self, output: str = "o", *, record_delivery: bool = True):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(tracker.close)
        tracker.record_submission(
            idempotency_key="idem-1", request_body={"input": "hi"},
            target_fingerprint="onebotv11|http://x", scope_key="qq:g:1",
            session_key="sess", mode="reactive", expect_structured=True, bot_self_id="999",
        )
        tracker.bind_submission_and_register(
            idempotency_key="idem-1", run_id="run-e2-h", scope_key="qq:g:1",
            adapter="onebotv11", is_private=False, user_id="u1", group_id="g1",
            bot_self_id="999", session_key="sess", mode="reactive", expect_structured=True,
        )
        if record_delivery:
            tracker.record_expected_delivery(
                run_id="run-e2-h", output=output, expect_structured=True
            )
        return tracker

    async def _run(self, result, *, tracker, bounce=None, real_bounce: bool = False, chat=None,
                   interim_echo: bool = False):
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt
        from nonebot_plugin_hermes_adapter.handlers import message_feedback as fb
        from nonebot_plugin_hermes_adapter.core import prompt_builder

        calls = {"send": [], "settle": [], "history": [], "files": [], "control": {}}
        cfg = MagicMock()
        cfg.group_profile_enabled = False
        cfg.sticker_vision_enabled = False
        cfg.history_message_limit = 5
        cfg.interim_echo_enabled = interim_echo
        bot = MagicMock(self_id=1)
        bot.call_api = AsyncMock(return_value={"file_id": "f"})
        target = SimpleNamespace(private=False, id="g1")

        async def fake_send(*, bot=None, target=None, text="", media_urls=(), delivery=None, **kw):
            calls["send"].append({
                "text": text,
                "component": getattr(delivery, "component_id", None),
                "media_urls": list(media_urls or ()),
            })
            return SimpleNamespace(ok=True, msg_ids=["m:send"])

        async def fake_files(**kw):
            calls["files"].append({
                "prefix": kw.get("component_prefix"),
                "component": getattr(kw.get("delivery"), "component_id", None),
            })
            return 0

        patches = {
            "load_misc_config": MagicMock(return_value=cfg),
            "load_chat_config": MagicMock(return_value=cfg),
            "load_media_config": MagicMock(return_value=cfg),
            "session_manager": MagicMock(
                get_push_watermark=MagicMock(return_value=None),
                get_session_key=MagicMock(return_value="sess"),
            ),
            "_current_delivery_generation": MagicMock(return_value=None),
            "resolve_target": MagicMock(return_value=object()),
            "_filter_current_and_steered": MagicMock(return_value=[]),
            "resolve_custom_dir": MagicMock(return_value=None),
            "build_reactive_system_prompt": MagicMock(return_value="sys"),
            "build_group_profile_block": MagicMock(return_value=""),
            "get_bot_nickname": AsyncMock(return_value="nano"),
            "split_unpushed_messages": MagicMock(return_value=([], None)),
            "ActiveSession": MagicMock(return_value=SimpleNamespace(
                generation=1, last_bot_reply_at=0, session_id="sess",
                last_bot_reply_text="", last_bot_reply_media=0,
                triggered_by="u1", topic_hint="", participants=set(),
                should_exit_active=False, interim_echo_enabled=False,
            )),
            "_chat_with_overflow_recovery": (
                chat if chat is not None else AsyncMock(return_value=result)
            ),
            "_settle": MagicMock(
                side_effect=lambda res, outcome, reason: calls["settle"].append((outcome, reason))
            ),
            "_record_background_note": MagicMock(),
            "_delivery_identity": MagicMock(
                side_effect=lambda res: (tracker, getattr(res, "run_id", None))
            ),
            "_schedule_reconcile": MagicMock(),
            "_finish_history": MagicMock(
                side_effect=lambda *a, **kw: calls["history"].append(kw.get("component_id"))
            ),
            "send_text_with_media": fake_send,
            "send_local_files": fake_files,
            "task_notes": MagicMock(push_plugin_note=MagicMock()),
            "_mcp": MagicMock(
                active_sessions=MagicMock(get_if_trigger_alive=MagicMock(return_value=None)),
                message_buffer=MagicMock(get_recent=MagicMock(return_value=[])),
                run_tracker=tracker,
            ),
            "_maybe_intercept_bounce": (
                fb._maybe_intercept_bounce if real_bounce else
                AsyncMock(side_effect=list(bounce)) if bounce is not None
                else AsyncMock(return_value=False)
            ),
            "_post_send_feedback": AsyncMock(),
            "_collect_inline_image_urls": MagicMock(return_value=[]),
            "inline_image_frames": AsyncMock(return_value=[]),
            "inline_gif_frames": AsyncMock(return_value=[]),
            "_absent_media_note": MagicMock(return_value=""),
            "build_media_address_index": MagicMock(return_value={}),
            "deliver_companion_run_output": AsyncMock(return_value=False),
            "resolve_reply": MagicMock(return_value=None),
        }
        prompts = patch.object(prompt_builder, "render_current_rows", MagicMock(return_value="hi"))
        prompts.start()
        self.addCleanup(prompts.stop)
        with patch.multiple(mt, **patches):
            await mt._run_reactive_turn(
                bot=bot, target=target, adapter_name="onebotv11",
                user_id="u1", group_id="g1", text="hi", image_urls=[],
                is_direct=True, addressed_to_bot=True, now_ms=1,
                recent_override=[], ephemeral=True,
            )
        sessions = patches["_mcp"].active_sessions
        calls["control"] = {
            "end_window": sessions.end_if_current_generation.call_count,
            "update_topic": sessions.update_topic.call_count,
            "background_note": patches["_record_background_note"].call_count,
        }
        return calls


class ReactiveBatchDeliveryTests(_ReactiveHarness):
    async def test_multi_decisions_send_in_order(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        decisions = structured_batch_from_terminal(
            decode_terminal_batch(batch(({"reply_to": "m:1"}, "第一条"), ({}, "第二条")))
        )
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertFalse(result.parse_failed)
        self.assertEqual([c["text"] for c in calls["send"]], ["第一条", "第二条"])
        self.assertEqual(
            [c["component"] for c in calls["send"]], ["primary:0", "primary:1"]
        )
        self.assertEqual(calls["history"], ["history:0", "history:1"])
        self.assertEqual(calls["settle"], [])

    async def test_empty_body_unit_not_sent(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        decisions = structured_batch_from_terminal(
            decode_terminal_batch(batch(({}, "只有这条"), ({}, "")))
        )
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = [d for d in decisions if d["should_reply"]]
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["只有这条"])

    async def test_all_silent_settles_without_send(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        decisions = structured_batch_from_terminal(decode_terminal_batch(unit({}, "")))
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("silent", "should_reply_false")])

    async def test_plan_failure_means_zero_sends(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        # tracker 里没有该 run 的 delivery 行 → 真实 upsert 失败 → 必须零发送
        decisions = structured_batch_from_terminal(
            decode_terminal_batch(batch(({}, "第一条"), ({}, "第二条")))
        )
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-without-ledger"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("failed", "terminal_plan_failed")])


class BatchRepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_already_sent_component_not_resent(self) -> None:
        from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor

        with tempfile.TemporaryDirectory() as tmp:
            tracker = RunTracker(Path(tmp) / "t.db")
            try:
                tracker.record_submission(
                    idempotency_key="k", request_body={"input": "hi"},
                    target_fingerprint="f", scope_key="qq:g:1", session_key="s",
                    mode="reactive", expect_structured=True, bot_self_id="999",
                )
                tracker.bind_submission_and_register(
                    idempotency_key="k", run_id="run-r", scope_key="qq:g:1",
                    adapter="onebotv11", is_private=False, user_id="u1", group_id="g1",
                    bot_self_id="999", session_key="s", mode="reactive", expect_structured=True,
                )
                tracker.record_expected_delivery(run_id="run-r", output="o", expect_structured=True)
                prepare_terminal_batch(tracker=tracker, run_id="run-r", units=[
                    {"body": "A", "media_urls": [], "file_paths": [], "reply_to": None},
                    {"body": "B", "media_urls": [], "file_paths": [], "reply_to": None},
                ])
                tracker.mark_component_sent(run_id="run-r", component_id="primary:0", msg_ids=["m1"])
                delivery = tracker.get_delivery("run-r")
                plan = json.loads(delivery["plan_json"])
                sent: list[str] = []

                async def fake_send_text(**kwargs):
                    sent.append(kwargs["delivery"].component_id)
                    return SimpleNamespace(ok=True, msg_ids=["m2"])

                cfg = SimpleNamespace(unknown_max_attempts=3, unknown_backoff_sec=0)
                with patch.object(monitor, "_send_text", fake_send_text), patch.object(
                    monitor, "_resolve_entry",
                    lambda d: (SimpleNamespace(target=object(), bot_self_id="999"), object()),
                ):
                    await monitor._repair_batch_components(
                        tracker, delivery, cfg, {}, plan
                    )
                self.assertEqual(sent, ["primary:1"])
                self.assertEqual(
                    tracker.get_component(run_id="run-r", component_id="primary:0")["msg_ids"],
                    ["m1"],
                )
            finally:
                tracker.close()


class _PassiveHarness(unittest.IsolatedAsyncioTestCase):
    """passive turn 的真实 handler harness（子类只放行为断言）。"""

    def _tracker(self, *, record_delivery: bool = True):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(tracker.close)
        tracker.record_submission(
            idempotency_key="idem-p", request_body={"input": "hi"},
            target_fingerprint="onebotv11|http://x", scope_key="qq:p:u1",
            session_key="sess", mode="reactive", expect_structured=True, bot_self_id="999",
        )
        tracker.bind_submission_and_register(
            idempotency_key="idem-p", run_id="run-e2-p", scope_key="qq:p:u1",
            adapter="onebotv11", is_private=True, user_id="u1", group_id=None,
            bot_self_id="999", session_key="sess", mode="reactive", expect_structured=True,
        )
        if record_delivery:
            tracker.record_expected_delivery(run_id="run-e2-p", output="o", expect_structured=True)
        return tracker

    async def _run(self, result, *, tracker, is_private: bool = True, group_id: str | None = None,
                   real_files: bool = False, bounce=None, chat=None):
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        calls = {"send": [], "settle": [], "history": [], "files": [],
                 "background": [], "heal": []}
        cfg = MagicMock()
        cfg.group_profile_enabled = False
        cfg.sticker_vision_enabled = False
        cfg.history_message_limit = 5
        cfg.sticker_enabled = False
        bot = MagicMock(self_id=1)
        bot.call_api = AsyncMock(return_value={"file_id": "f1"})
        target = SimpleNamespace(
            private=is_private, id="u1" if is_private else (group_id or "g1")
        )

        async def fake_send(*, text="", delivery=None, **kw):
            calls["send"].append({
                "text": text, "component": getattr(delivery, "component_id", None),
                "media_urls": list(kw.get("media_urls") or ()),
            })
            return SimpleNamespace(ok=True, msg_ids=["m1"])

        async def fake_files(**kw):
            calls["files"].append({
                "prefix": kw.get("component_prefix"),
                "component": getattr(kw.get("delivery"), "component_id", None),
                "paths": list(kw.get("file_paths") or []),
            })
            return 0

        if real_files:
            from nonebot_plugin_hermes_adapter.core import outbound as ob

            files_mock = ob.send_local_files
        else:
            files_mock = fake_files

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
            "_chat_with_overflow_recovery": (
                chat if chat is not None else AsyncMock(return_value=result)
            ),
            "_settle": MagicMock(
                side_effect=lambda res, outcome, reason: calls["settle"].append((outcome, reason))
            ),
            "_record_background_note": MagicMock(
                side_effect=lambda key, decision, **kw: calls["background"].append(
                    decision.get("background_task")
                )
            ),
            "_push_self_heal_note": MagicMock(
                side_effect=lambda key, decision: calls["heal"].append(
                    decision.get("_self_heal_hint")
                )
            ),
            "_delivery_identity": MagicMock(
                side_effect=lambda res: (tracker, getattr(res, "run_id", None))
            ),
            "_schedule_reconcile": MagicMock(),
            "_finish_history": MagicMock(
                side_effect=lambda *a, **kw: calls["history"].append(kw.get("component_id"))
            ),
            "send_text_with_media": fake_send,
            "send_local_files": files_mock,
            "resolve_reply": MagicMock(return_value=None),
            "_maybe_intercept_bounce": (
                AsyncMock(side_effect=list(bounce)) if bounce is not None
                else AsyncMock(return_value=False)
            ),
            "_post_send_feedback": AsyncMock(),
            "get_bot_nickname": AsyncMock(return_value="nano"),
            "task_notes": MagicMock(push_plugin_note=MagicMock()),
            "_mcp": MagicMock(
                message_buffer=MagicMock(get_recent=MagicMock(return_value=[])),
                active_sessions=MagicMock(),
                run_tracker=tracker,
            ),
        }
        with patch.multiple(mt, **patches):
            from nonebot_plugin_hermes_adapter.core import prompt_builder

            rows_patch = patch.object(
                prompt_builder, "render_current_rows", MagicMock(return_value="hi")
            )
            rows_patch.start()
            self.addCleanup(rows_patch.stop)
            await mt._run_passive_turn(
                bot=bot, target=target, adapter_name="onebotv11",
                user_id="u1", group_id=group_id, text="hi", image_urls=[],
                is_private=is_private, now_ms=1,
            )
        return calls

class PassiveBatchDeliveryTests(_PassiveHarness):
    """私聊 passive 路径同样按份发送（runs 终态）。"""

    async def test_passive_multi_decisions_send_in_order(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        decisions = structured_batch_from_terminal(
            decode_terminal_batch(batch(({"reply_to": "m:1"}, "私聊一"), ({}, "私聊二")))
        )
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["私聊一", "私聊二"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:0", "primary:1"])


class PassiveMultiControlTests(_PassiveHarness):
    """R5：私聊 passive 连续多份终态必须按份顺序消费既有控制，空 body 只免发送。"""

    async def test_second_unit_background_task_reaches_control(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = batch(({}, "先回应"), ({"background_task": "第二份异步任务"}, "任务已登记"))
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["先回应", "任务已登记"])
        self.assertEqual(calls["background"], ["", "第二份异步任务"])
        self.assertEqual(calls["heal"], [None, None])

    async def test_silent_second_unit_keeps_background_task(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = batch(({}, "先回应"), ({"background_task": "静默份任务"}, ""))
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["先回应"])
        self.assertEqual(calls["background"], ["", "静默份任务"])
        self.assertEqual(calls["settle"], [])

    async def test_all_silent_batch_keeps_background_task(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = unit({"background_task": "全静默任务"}, "")
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["background"], ["全静默任务"])
        self.assertEqual(calls["settle"], [("silent", "should_reply_false")])

    async def test_every_unit_self_heal_hint_consumed_in_order(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        first = {"body": "一", "should_reply": True, "reply_to": "",
                 "background_task": "", "_self_heal_hint": "第一份提示"}
        second = {"body": "二", "should_reply": True, "reply_to": "",
                  "background_task": "", "_self_heal_hint": "第二份提示"}
        result = ChatResult(raw_text="x", structured=first)
        result.terminal_decisions = [first, second]
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["一", "二"])
        self.assertEqual(calls["heal"], ["第一份提示", "第二份提示"])

    async def test_single_completions_control_unchanged(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        result = ChatResult(
            raw_text='{"background_task":"单份任务"}\n{"body":"单份正文"}',
            structured={"body": "单份正文", "should_reply": True,
                        "background_task": "单份任务"},
        )
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["单份正文"])
        self.assertEqual(calls["background"], ["单份任务"])


class NakedMediaRunsTests(_ReactiveHarness):
    """R6：A 类裸正文的媒体内联不能被 base64 长度误判；普通超长裸文仍拒绝。"""

    async def test_runs_naked_image_survives_chat_and_handler(self) -> None:
        from PIL import Image

        from nonebot_plugin_hermes_adapter.config import plugin_config
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter.core import media_whitelist as mw

        with tempfile.TemporaryDirectory() as tmp:
            path = (Path(tmp) / "noise.png").resolve()
            Image.effect_noise((128, 128), 40).convert("RGB").save(path)
            with patch.object(mw, "allowed_roots", return_value=[Path(tmp).resolve()]):
                output = mw.inline_local_media_images("生成图片 MEDIA:" + str(path))
        self.assertGreater(len(output), MAX_BODY_CHARS)
        self.assertIn("data:image/png;base64,", output)
        # E1 单份解码保持原样；Runs 批量解码必须复用同一份媒体计量与回填。
        self.assertEqual(decode_terminal(output).body, output)
        decisions = decode_terminal_batch(output)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].body, output)
        # 同样覆盖带信封的单份：内联图片在信封路径也不得撑爆行/长度计量。
        enveloped = decode_terminal_batch(unit({}, output))
        self.assertEqual(len(enveloped), 1)
        self.assertEqual(enveloped[0].body, output)

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        try:
            with patch.object(
                hc.hermes_client, "_chat_via_runs",
                AsyncMock(return_value=(output, None, None, "", "run-e2-img")),
            ):
                result = await hc.hermes_client.chat(
                    text="画张图", session_key="s", user_id="u1", group_id="g1",
                    adapter_name="onebotv11", is_private=False, mode="reactive",
                    expect_structured=True, structured_tool_name="submit_decision",
                    system_prompt="SYS",
                    target=SimpleNamespace(
                        base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t",
                    ),
                )
            self.assertFalse(result.parse_failed)
            self.assertEqual(result.parse_failed_reason, "")
            self.assertEqual(len(result.terminal_decisions), 1)
            self.assertIn("data:image/png;base64,", result.terminal_decisions[0]["body"])
            # 真实 handler：同一 ChatResult 经真实 _run_reactive_turn，图片进入计划媒体。
            result.run_id = "run-e2-h"
            tracker = self._tracker(output=output)
            calls = await self._run(result, tracker=tracker)
            self.assertEqual(len(calls["send"]), 1)
            plan = json.loads(tracker.get_delivery("run-e2-h")["plan_json"])
            self.assertEqual(len(plan["units"]), 1)
            self.assertEqual(len(plan["units"][0]["media_urls"]), 1)
            self.assertIn("data:image/png;base64,", plan["units"][0]["media_urls"][0])
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)

    def test_plain_overlong_naked_text_still_rejected(self) -> None:
        with self.assertRaises(TerminalFormatError) as ctx:
            decode_terminal_batch("正" * (MAX_BODY_CHARS + 1))
        self.assertEqual(ctx.exception.reason, "naked_overflow")
        with self.assertRaises(TerminalFormatError) as ctx:
            decode_terminal_batch(("正" * MAX_BODY_CHARS) + "\n" + "正")
        self.assertEqual(ctx.exception.reason, "naked_overflow")


class CompletionsSinglePathRegressionTests(_ReactiveHarness):
    async def test_single_completions_decision_unchanged(self) -> None:
        """completions（无 terminal_decisions）仍走旧单份计划与 single send。"""
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        result = ChatResult(
            raw_text="{}\n{\"body\":\"正文\"}",
            structured={"body": "正文", "should_reply": True},
        )
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["正文"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary"])


def _resume_build_units():
    """载入 tasks.run_resume 的真实 _build_units 源码（拼装树 scheduler 导入不可用）。"""
    import ast

    source = (ROOT / "tasks/run_resume.py").read_text(encoding="utf-8")
    node = next(
        n for n in ast.parse(source).body
        if isinstance(n, ast.FunctionDef) and n.name == "_build_units"
    )
    ns = {"__package__": "nonebot_plugin_hermes_adapter.tasks"}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<run_resume._build_units>", "exec"), ns)
    return ns["_build_units"]


class SilentControlSideEffectTests(_ReactiveHarness):
    """R1：过滤空 body 只能跳过发送，不得丢掉原有 background／topic／收窗控制。"""

    def _silent_decision(self):
        return {
            "body": "",
            "should_reply": False,
            "should_exit_active": True,
            "topic_hint": "收尾",
            "background_task": "任务登记",
        }

    async def test_completions_silent_keeps_controls(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        result = ChatResult(
            raw_text='{"should_exit_active":true,"topic_hint":"收尾"}\n{"body":""}',
            structured=self._silent_decision(),
        )
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(
            calls["control"],
            {"end_window": 1, "update_topic": 1, "background_note": 1},
        )
        self.assertEqual(calls["settle"], [("silent", "should_reply_false")])

    async def test_runs_silent_unit_keeps_controls(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        decision = self._silent_decision()
        result = ChatResult(raw_text="x", structured=decision)
        result.terminal_decisions = [decision]
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(
            calls["control"],
            {"end_window": 1, "update_topic": 1, "background_note": 1},
        )
        self.assertEqual(calls["settle"], [("silent", "should_reply_false")])

    async def test_mixed_batch_runs_controls_and_skips_only_send(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        silent = self._silent_decision()
        spoken = {"body": "第二份", "should_reply": True, "reply_to": ""}
        result = ChatResult(raw_text="x", structured=silent)
        result.terminal_decisions = [silent, spoken]
        result.run_id = "run-e2-h"
        tracker = self._tracker()
        calls = await self._run(result, tracker=tracker)
        self.assertEqual([c["text"] for c in calls["send"]], ["第二份"])
        # 空份不占发送序号：唯一发出的份仍是 primary:0
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:0"])
        self.assertEqual(calls["settle"], [])
        self.assertEqual(
            calls["control"],
            {"end_window": 1, "update_topic": 1, "background_note": 2},
        )
        plan = json.loads(tracker.get_delivery("run-e2-h")["plan_json"])
        self.assertEqual([u["cleaned_text"] for u in plan["units"]], ["第二份"])


class ResumePlanConsistencyTests(_ReactiveHarness):
    """R2：正常入口产物 + 真实恢复适配器必须得到同一计划，已 sent 不重置。"""

    async def test_normal_plan_matches_resume_adapter(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = batch(({}, "第一份"), ({"reply_to": "m:7"}, "第二份"))
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        tracker = self._tracker(output=output)
        calls = await self._run(result, tracker=tracker)
        self.assertEqual([c["text"] for c in calls["send"]], ["第一份", "第二份"])
        delivery = tracker.get_delivery("run-e2-h")
        normal_plan = delivery["plan_json"]
        tracker.mark_component_sent(
            run_id="run-e2-h", component_id="primary:0", msg_ids=["first-was-sent"]
        )
        resume_units = _resume_build_units()(delivery["output"], delivery["expect_structured"])
        expected = json.dumps(
            build_batch_plan(resume_units)[0], sort_keys=True, ensure_ascii=False
        )
        self.assertEqual(normal_plan, expected)
        # 真实 ledger：同一 output 重复落计划必须被接受，且 sent 回执不被重置
        prepare_terminal_batch(tracker=tracker, run_id="run-e2-h", units=resume_units)
        self.assertEqual(tracker.get_delivery("run-e2-h")["plan_json"], normal_plan)
        first = tracker.get_component(run_id="run-e2-h", component_id="primary:0")
        self.assertEqual((first["state"], first["msg_ids"]), ("sent", ["first-was-sent"]))
        second = tracker.get_component(run_id="run-e2-h", component_id="primary:1")
        self.assertEqual(second["state"], "pending")

    def test_empty_reply_to_normalized_in_plan(self) -> None:
        base = [{"body": "A", "media_urls": [], "file_paths": [], "reply_to": ""}]
        as_none = [{"body": "A", "media_urls": [], "file_paths": [], "reply_to": None}]
        self.assertEqual(build_batch_plan(base), build_batch_plan(as_none))


class BatchBounceAccountingTests(_ReactiveHarness):
    """R3：被 bounce 拦截的份记免发终态，repair 对账不得补发。"""

    async def _repair_sends(self, tracker, run_id: str) -> list[str]:
        from types import SimpleNamespace as NS

        from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor

        delivery = tracker.get_delivery(run_id)
        plan = json.loads(delivery["plan_json"])
        sent: list[str] = []

        async def fake_send_text(**kwargs):
            sent.append(kwargs["text"])
            return NS(ok=True, msg_ids=["repair-id"])

        cfg = NS(unknown_max_attempts=3, unknown_backoff_sec=0)
        with patch.object(monitor, "_send_text", fake_send_text), patch.object(
            monitor, "_resolve_entry",
            lambda d: (NS(target=object(), bot_self_id="999"), object()),
        ):
            await monitor._repair_batch_components(tracker, delivery, cfg, {}, plan)
        return sent

    async def test_reactive_bounced_unit_not_repaired(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = batch(({}, "被拦截的正文"), ({}, "正常正文"))
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        tracker = self._tracker()
        calls = await self._run(result, tracker=tracker, bounce=[True, False])
        self.assertEqual([c["text"] for c in calls["send"]], ["正常正文"])
        bounced = tracker.get_component(run_id="run-e2-h", component_id="primary:0")
        self.assertIn(bounced["state"], ("cancelled", "not_required"))
        self.assertEqual(
            tracker.get_component(run_id="run-e2-h", component_id="history:0")["state"],
            "not_required",
        )
        repaired = await self._repair_sends(tracker, "run-e2-h")
        self.assertNotIn("被拦截的正文", repaired)

    async def test_reactive_all_bounced_settles_suppressed(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = unit({}, "唯一一份")
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        tracker = self._tracker()
        calls = await self._run(result, tracker=tracker, bounce=[True])
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("suppressed", "bounce")])
        self.assertEqual(await self._repair_sends(tracker, "run-e2-h"), [])


    async def test_real_bounce_rule_marks_unit_not_required(self) -> None:
        from nonebot_plugin_hermes_adapter.core.feedback_rules import FeedbackRule
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
        from nonebot_plugin_hermes_adapter.handlers import message_feedback as fb

        rule = FeedbackRule(
            name="review-bounce", regex=None, where="reply", action="bounce",
            keywords=("bounce",),
        )
        output = unit({}, "bounce 这条应被真实规则打回")
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        tracker = self._tracker()
        with patch.object(fb, "load_rules", return_value=[rule]), patch.object(
            fb, "_fire_followup_turn", MagicMock()
        ), patch.object(fb, "_chain_allow", return_value=True), patch.object(
            fb, "_chain_bump", MagicMock()
        ):
            calls = await self._run(result, tracker=tracker, real_bounce=True)
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("suppressed", "bounce")])
        self.assertEqual(
            tracker.get_component(run_id="run-e2-h", component_id="primary:0")["state"],
            "not_required",
        )


class PassiveBounceAccountingTests(_PassiveHarness):
    async def test_passive_bounced_unit_not_repaired(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = unit({}, "私聊被拦截")
        decisions = structured_batch_from_terminal(decode_terminal_batch(output))
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        tracker = self._tracker()
        calls = await self._run(result, tracker=tracker, bounce=[True])
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("suppressed", "bounce")])
        self.assertIn(
            tracker.get_component(run_id="run-e2-p", component_id="primary:0")["state"],
            ("cancelled", "not_required"),
        )


class LegacyComponentNamingTests(_ReactiveHarness):
    """R4：带索引的组件名只给 E2 批量份；非批量保持 legacy 名。"""

    async def test_completions_file_and_history_use_legacy_names(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.txt"
            path.write_text("hello", encoding="utf-8")
            result = ChatResult(
                raw_text="文件已生成",
                structured={
                    "body": f"文件已生成 MEDIA:{path}",
                    "should_reply": True,
                    "reply_to": "",
                },
            )
            result.run_id = "run-e2-h"
            calls = await self._run(result, tracker=self._tracker())
            self.assertEqual(len(calls["send"]), 1)
            self.assertTrue(calls["send"][0]["text"].startswith("文件已生成"))
            self.assertEqual(calls["history"], ["history"])
            self.assertEqual([f["prefix"] for f in calls["files"]], [""])


class GroupPassiveLegacyFileTests(_PassiveHarness):
    async def test_group_passive_file_claim_uses_legacy_component(self) -> None:
        from nonebot_plugin_hermes_adapter.core import media_whitelist, outbound as ob
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.txt"
            path.write_text("hello", encoding="utf-8")
            result = ChatResult(raw_text=f"文件已生成 MEDIA:{path}")
            result.run_id = "run-e2-p"
            tracker = self._tracker()
            with patch.object(media_whitelist, "validate_local_file", return_value=path), patch.object(
                ob, "load_media_config", return_value=SimpleNamespace(local_file_max_mb=20)
            ):
                calls = await self._run(
                    result, tracker=tracker, is_private=False, group_id="12345",
                    real_files=True,
                )
            self.assertEqual(calls["history"], ["history"])
            comps = {c["component_id"]: c for c in tracker.list_components("run-e2-p")}
            self.assertEqual(set(comps), {"primary", "file:0", "history"})
            self.assertEqual(comps["file:0"]["state"], "sent")


if __name__ == "__main__":
    unittest.main()
