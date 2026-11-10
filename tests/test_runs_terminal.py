"""Runs 纯文本终态（多份）切分、整批计划与按份交付。"""

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

from nonebot_plugin_hermes_adapter.core.runs_text import (  # noqa: E402
    MAX_BODY_CHARS,
    TextUnitsError,
    decisions_from_text,
    split_text_units,
    units_from_text,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    TerminalPlanError,
    build_batch_plan,
    prepare_terminal_batch,
    units_from_output,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402


def unit(control: dict | None = None, body: str = "正文") -> str:
    """纯文本协议：一份正文就是这段文本；control 形参只为保留调用形状。"""
    return body


def batch(*pairs) -> str:
    """多份正文按协议连发：两份之间一行 ``§``。"""
    return "\n§\n".join(unit(control, body) for control, body in pairs)


def decisions(*bodies) -> list[dict]:
    """正文 → 决策序列（空正文不产生决策，也不占发送序）。"""
    return [{"body": body, "should_reply": True} for body in bodies if str(body).strip()]


class TextUnitTests(unittest.TestCase):
    """正文切分：正文即消息，独立整行 § 只作连发分隔。"""

    def test_single_sentence_is_one_unit(self) -> None:
        self.assertEqual(split_text_units("你好"), ["你好"])

    def test_multiline_without_separator_stays_one(self) -> None:
        text = "第一行\n\n第二段\n第三行"
        self.assertEqual(split_text_units(text), [text])

    def test_three_units_in_order(self) -> None:
        self.assertEqual(split_text_units("甲\n§\n乙\n§\n丙"), ["甲", "乙", "丙"])

    def test_inline_separator_is_literal(self) -> None:
        self.assertEqual(split_text_units("第 3 节 §3"), ["第 3 节 §3"])
        self.assertEqual(split_text_units("§ 内容"), ["§ 内容"])

    def test_separator_with_spaces_is_literal(self) -> None:
        # 严格整行匹配：分节符前后只要多一个空格就是普通正文。
        for line in (" §", "§ ", " § "):
            self.assertEqual(split_text_units(f"甲\n{line}\n乙"), [f"甲\n{line}\n乙"])
            self.assertEqual(split_text_units(line), [line.strip()])

    def test_fenced_separator_still_splits(self) -> None:
        # 唯一规则：整行严格等于 § 就分隔；代码围栏不是例外，也不设转义／豁免。
        self.assertEqual(split_text_units("```\n§\n```"), ["```", "```"])
        self.assertEqual(split_text_units("~~~\n§\n~~~"), ["~~~", "~~~"])
        self.assertEqual(
            split_text_units("说明\n```\n甲\n§\n乙\n```"),
            ["说明\n```\n甲", "乙\n```"],
        )

    def test_backslash_separator_line_is_plain_text(self) -> None:
        # 不设转义协议：`\§` 就是普通正文原样发送。
        self.assertEqual(split_text_units("\\§"), ["\\§"])

    def test_leading_trailing_and_repeated_separators_drop_empties(self) -> None:
        self.assertEqual(split_text_units("§\n甲\n§\n§\n乙\n§"), ["甲", "乙"])

    def test_blank_and_whitespace_only_text_is_silent(self) -> None:
        for text in ("", "   ", "\n\n", "§"):
            self.assertEqual(split_text_units(text), [])

    def test_over_limit_fails_closed(self) -> None:
        self.assertEqual(len(split_text_units("长" * MAX_BODY_CHARS)[0]), MAX_BODY_CHARS)
        with self.assertRaises(TextUnitsError) as ctx:
            split_text_units("长" * (MAX_BODY_CHARS + 1))
        self.assertEqual(ctx.exception.reason, "body_overflow")

    def test_units_from_output_uses_same_splitter(self) -> None:
        units = units_from_output("甲\n§\n乙")
        self.assertEqual([u["body"] for u in units], ["甲", "乙"])
        self.assertEqual([u["reply_to"] for u in units], ["", ""])
        self.assertIsNone(units_from_output("   "))

    def test_units_from_output_keeps_legacy_json_verbatim(self) -> None:
        # 旧 JSON 不再识别：按纯文本原样发送，不拆壳、不消毒。
        legacy = '{"reply_to":"m:1"}\n{"body":"正文"}'
        units = units_from_output(legacy)
        self.assertEqual([u["body"] for u in units], [legacy])

    def test_units_from_text_extracts_media(self) -> None:
        units = units_from_text("看这个 MEDIA:/tmp/a.png")
        self.assertTrue(units[0]["body"].startswith("看这个"))
        self.assertEqual(units[0]["file_paths"], ["/tmp/a.png"])

    def test_decisions_from_text_keeps_raw_body(self) -> None:
        self.assertEqual(
            decisions_from_text("甲\n§\n乙"),
            [{"body": "甲", "should_reply": True}, {"body": "乙", "should_reply": True}],
        )


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
                   sessions=None, current_buffered=None,
                   replied_msg_id=None, now_ms: int = 1, resolve_reply=None,
                   trigger_msg_id=None, task_anchor_enabled: bool = True):
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt
        from nonebot_plugin_hermes_adapter.handlers import message_feedback as fb
        from nonebot_plugin_hermes_adapter.core import prompt_builder

        calls = {"send": [], "settle": [], "history": [], "files": [], "control": {}}
        cfg = MagicMock()
        cfg.group_profile_enabled = False
        cfg.sticker_vision_enabled = False
        cfg.history_message_limit = 5
        # 锚控制路径要拿真实数值算超龄；MagicMock 参与 `>` 比较会直接 TypeError。
        cfg.anchor_max_age_sec = 3600
        # 任务锚总开关：默认开启（保持既有用例语义），关闭用例显式传 False。
        cfg.task_anchor_enabled = task_anchor_enabled
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
                triggered_by="u1", participants=set(),
                should_exit_active=False,
            )),
            "_chat_with_overflow_recovery": (
                chat if chat is not None else AsyncMock(return_value=result)
            ),
            "_settle": MagicMock(
                side_effect=lambda res, outcome, reason: calls["settle"].append((outcome, reason))
            ),
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
                active_sessions=(
                    sessions if sessions is not None
                    else MagicMock(get_if_trigger_alive=MagicMock(return_value=None))
                ),
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
            "resolve_reply": (
                resolve_reply if resolve_reply is not None
                else MagicMock(return_value=None)
            ),
        }
        prompts = patch.object(prompt_builder, "render_current_rows", MagicMock(return_value="hi"))
        prompts.start()
        self.addCleanup(prompts.stop)
        with patch.multiple(mt, **patches):
            await mt._run_reactive_turn(
                bot=bot, target=target, adapter_name="onebotv11",
                user_id="u1", group_id="g1", text="hi", image_urls=[],
                is_direct=True, addressed_to_bot=True, now_ms=now_ms,
                recent_override=[], ephemeral=True,
                current_buffered=current_buffered,
                replied_msg_id=replied_msg_id,
                trigger_msg_id=trigger_msg_id,
            )
        sessions = patches["_mcp"].active_sessions
        calls["control"] = {
            "end_window": getattr(sessions.end_if_current_generation, "call_count", 0),
        }
        # 锚控制只能按调用计数观察（真实 ActiveSessionManager 也可传进来）。
        calls["anchor"] = {
            "pin": getattr(sessions.pin_anchor, "call_count", 0),
            "answer": getattr(sessions.mark_anchor_answered, "call_count", 0),
            "release": getattr(sessions.release_anchor, "call_count", 0),
            "merge": getattr(sessions.merge_into_anchor, "call_count", 0),
        }
        return calls


class ReactiveBatchDeliveryTests(_ReactiveHarness):
    async def test_multi_decisions_send_in_order(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        decisions = decisions_from_text(batch(({}, "第一条"), ({}, "第二条")))
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

        # 尾部空的 § 单元不产生消息，也不占发送序。
        decisions = decisions_from_text(batch(({}, "只有这条"), ({}, "")))
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["只有这条"])

    async def test_all_silent_settles_without_send(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        result = ChatResult(raw_text="", structured={"should_reply": False})
        result.terminal_decisions = []
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("silent", "empty_output")])

    async def test_plan_failure_means_zero_sends(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        # tracker 里没有该 run 的 delivery 行 → 真实 upsert 失败 → 必须零发送
        decisions = decisions_from_text(batch(({}, "第一条"), ({}, "第二条")))
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
                   real_files: bool = False, bounce=None, chat=None, resolve_reply=None):
        from nonebot_plugin_hermes_adapter.handlers import message_turns as mt

        calls = {"send": [], "settle": [], "history": [], "files": [], "heal": []}
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
            "_schedule_reconcile": MagicMock(),
            "_delivery_identity": MagicMock(
                side_effect=lambda res: (tracker, getattr(res, "run_id", None))
            ),
            "_finish_history": MagicMock(
                side_effect=lambda *a, **kw: calls["history"].append(kw.get("component_id"))
            ),
            "send_text_with_media": fake_send,
            "send_local_files": files_mock,
            "resolve_reply": (
                resolve_reply if resolve_reply is not None
                else MagicMock(return_value=None)
            ),
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

        decisions = decisions_from_text(batch(({}, "私聊一"), ({}, "私聊二")))
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual([c["text"] for c in calls["send"]], ["私聊一", "私聊二"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:0", "primary:1"])


class PassiveMultiControlTests(_PassiveHarness):
    """R5：私聊 passive 连续多份终态必须按份顺序消费仍存在的控制（submit_control FIFO）。"""

    def _register(self, *controls) -> None:
        from nonebot_plugin_hermes_adapter.core import control_intent

        key = ("onebotv11", "private:u1")
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, key)
        for control in controls:
            control_intent.CONTROL_QUEUE.register(key, control)

    async def test_second_unit_control_kept(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
        from nonebot_plugin_hermes_adapter.core import control_intent

        self._register({"reply_to": "m:1"}, {"answer_anchor": "all"})
        output = batch(({}, "先回应"), ({}, "任务已登记"))
        decisions = decisions_from_text(output)
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(control_intent.resolve_control(decisions[0])["reply_to"], "m:1")
        self.assertEqual(
            control_intent.resolve_control(decisions[1])["answer_anchor"], "all"
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["先回应", "任务已登记"])

    async def test_unpaired_control_dropped_at_turn_end(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
        from nonebot_plugin_hermes_adapter.core import control_intent

        # 静默轮不消费控制：登记了 but 没有正文 → 轮末丢弃，也不发空消息。
        self._register({"answer_anchor": "all"})
        result = ChatResult(raw_text="", structured={"should_reply": False})
        result.terminal_decisions = []
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["settle"], [("silent", "empty_output")])
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(("onebotv11", "private:u1")), 1)

    async def test_single_completions_text_unit_gets_control(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
        from nonebot_plugin_hermes_adapter.core import control_intent

        self._register({"reply_to": "m:3"})
        output = "单份正文"
        decisions = decisions_from_text(output)
        result = ChatResult(raw_text=output, structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-p"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(control_intent.resolve_control(decisions[0])["reply_to"], "m:3")
        self.assertEqual([c["text"] for c in calls["send"]], ["单份正文"])


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
        # 纯文本切分复用计划器的媒体清洗：内联图片不能撑爆长度计量。
        self.assertEqual(units_from_output(output)[0]["body"], "生成图片")
        self.assertEqual(len(units_from_output(output)[0]["media_urls"]), 1)

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
        with self.assertRaises(TextUnitsError) as ctx:
            split_text_units("正" * (MAX_BODY_CHARS + 1))
        self.assertEqual(ctx.exception.reason, "body_overflow")
        with self.assertRaises(TextUnitsError) as ctx:
            split_text_units(("正" * MAX_BODY_CHARS) + "\n" + "正")
        self.assertEqual(ctx.exception.reason, "body_overflow")


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
    """纯文本协议：没有正文就是静默——不合成空决策、不消费控制、不收窗。"""

    async def test_completions_silent_leaves_controls_alone(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        result = ChatResult(raw_text="", structured={"should_reply": False})
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["control"], {"end_window": 0})
        self.assertEqual(calls["settle"], [("silent", "empty_output")])

    async def test_runs_silent_unit_leaves_controls_alone(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        result = ChatResult(raw_text="", structured={"should_reply": False})
        result.terminal_decisions = []
        result.run_id = "run-e2-h"
        calls = await self._run(result, tracker=self._tracker())
        self.assertEqual(calls["send"], [])
        self.assertEqual(calls["control"], {"end_window": 0})
        self.assertEqual(calls["settle"], [("silent", "empty_output")])

    async def test_batch_exit_control_closes_window_after_group(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult
        from nonebot_plugin_hermes_adapter.core import control_intent

        key = ("onebotv11", "group:g1")
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, key)
        control_intent.CONTROL_QUEUE.register(key, {"should_exit_active": True})

        decisions = decisions_from_text(batch(({}, "第一份"), ({}, "第二份")))
        result = ChatResult(raw_text="x", structured=decisions[0])
        result.terminal_decisions = decisions
        result.run_id = "run-e2-h"
        tracker = self._tracker()
        calls = await self._run(result, tracker=tracker)
        self.assertEqual([c["text"] for c in calls["send"]], ["第一份", "第二份"])
        self.assertEqual([c["component"] for c in calls["send"]], ["primary:0", "primary:1"])
        self.assertEqual(calls["settle"], [])
        # 整组末条之后才收窗一次（退窗是轮级控制，不逐份执行）。
        self.assertEqual(calls["control"], {"end_window": 1})
        plan = json.loads(tracker.get_delivery("run-e2-h")["plan_json"])
        self.assertEqual([u["cleaned_text"] for u in plan["units"]], ["第一份", "第二份"])


class ResumePlanConsistencyTests(_ReactiveHarness):
    """R2：正常入口产物 + 真实恢复适配器必须得到同一计划，已 sent 不重置。"""

    async def test_normal_plan_matches_resume_adapter(self) -> None:
        from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult

        output = batch(({}, "第一份"), ({"reply_to": "m:7"}, "第二份"))
        decisions = decisions_from_text(output)
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
        decisions = decisions_from_text(output)
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
        decisions = decisions_from_text(output)
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
        decisions = decisions_from_text(output)
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
        decisions = decisions_from_text(output)
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
