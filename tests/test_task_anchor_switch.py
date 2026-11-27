"""条目 84：任务锚总开关（实际行为 + 提示词拼装联动）。

覆盖：
- 正式配置键 task_anchor_enabled 缺省 True；模板/样例真实写出默认值；
- 缺省开启 与 显式开启 逐字节同形；显式关闭时 runs／completions／旧接口、
  群聊／私聊、系统提示与 user content 都不再出现锚说明与锚操作示例；
- 关闭时存量锚只隐藏不清理，重新开启后按原规则恢复；
- submit_control 的四个锚字段关闭时无实际副作用，non-anchor 控制不受影响；
- handler 真实 _run_reactive_turn：关闭时锚控制零调用，退窗／reply_to 照常。
"""
from __future__ import annotations

import re
import unittest
from unittest.mock import MagicMock, patch

from tests import _bootstrap  # noqa: F401

from tests import test_runs_terminal as e2
from tests.test_control_regressions import KEY, _result, _row
from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import chat_config as cc
from nonebot_plugin_hermes_adapter.core import config_templates as ct
from nonebot_plugin_hermes_adapter.core import control_intent
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb
from nonebot_plugin_hermes_adapter.core import task_notes
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt
from nonebot_plugin_hermes_adapter.core.active_session import (
    ActiveSessionManager,
    AnchorRecord,
    Participant,
)
from nonebot_plugin_hermes_adapter.core.decision_contract import ANCHOR_FIELD_NAMES
from nonebot_plugin_hermes_adapter.core.hermes_client import _decision_hint
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin
from nonebot_plugin_hermes_adapter.core.prompt_protocol import anchor_prompt_residue
from nonebot_plugin_hermes_adapter.core.routing import CallerScope
from nonebot_plugin_hermes_adapter.mcp.tools.control import (
    SubmitControlInput,
    submit_control_impl,
)

NOW = 1_800_000_000_000
_TIME_RE = re.compile(r"current_time: .*|（当前时间：.*）")


def _commit(enabled: bool) -> None:
    """走真实重载入口：提交一份新的 chat_config 快照。"""
    cc._store.commit(cc.parse_chat_config({"task_anchor_enabled": enabled}))


def _norm(text: str) -> str:
    """抹掉按分钟变化的当前时间行，便于逐字节比较。"""
    return _TIME_RE.sub("T", text)


def _row_obj() -> BufferedMessage:
    return BufferedMessage(
        ts=NOW - 1000, adapter="onebotv11", group_id="g1", user_id="10001",
        nickname="主人", content="帮我盯一下这件事", origin=MessageOrigin.human(),
        id=201,
    )


def _anchor() -> AnchorRecord:
    return AnchorRecord(
        participants=[Participant(user_id="10001", nickname="主人", joined_ts=NOW - 60_000)],
        text="帮我盯一下这件事", ts=NOW - 60_000, m_id=201,
    )


def _render(mode: str, enabled: bool) -> dict[str, str]:
    _commit(enabled)
    with patch.object(plugin_config, "hermes_api_interface", mode), \
            patch.object(pb, "sticker_feature_enabled", return_value=False):
        group = pb.build_reactive_user_content(
            addressed_to_bot=True,
            recent_messages=[], rows=[_row_obj()], current_image_urls=[],
            pinned_anchors=[_anchor()], now_ms=NOW, self_id="999",
            adapter="onebotv11", plugin_note="进度更新",
        )
        private = pb.build_passive_user_content(
            recent_messages=[], rows=[_row_obj()], current_image_urls=[],
            is_private=True, self_id="999", adapter="onebotv11",
            plugin_note="进度更新",
        )
        out = {
            "group_system": pb.build_reactive_system_prompt(
                adapter="onebotv11", group_id="g1", self_id="999",
                self_nickname="Hermes",
            ),
            "private_system": pb.build_passive_system_prompt(
                adapter="onebotv11", is_private=True, user_id="u1", group_id=None,
                private_decision=True,
            ),
            # 模型一轮能看到的全部动态文本：正文 + 请求级上下文 + 批尾 reminder。
            "group_user": group.content + "\n\n" + group.request_context + group.closing_reminder,
            "private_user": private.content + "\n\n" + private.request_context + private.closing_reminder,
        }
    return out


class TaskAnchorConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.addCleanup(cc._store.commit, cc.ChatConfig())

    def test_default_is_on(self) -> None:
        self.assertIs(cc.ChatConfig().task_anchor_enabled, True)
        self.assertIs(cc.load_chat_config().task_anchor_enabled, True)

    def test_parse_reads_false(self) -> None:
        self.assertIs(cc.parse_chat_config({"task_anchor_enabled": False}).task_anchor_enabled, False)
        self.assertIs(cc.parse_chat_config({}).task_anchor_enabled, True)

    def test_template_and_example_state_real_default(self) -> None:
        spec = ct.TARGET_KEYS["task_anchor_enabled"]
        self.assertEqual(spec, {"file": "chat_config.yaml", "default": True, "type": "bool"})
        self.assertIn("task_anchor_enabled: true", ct.CHAT_CONFIG_TEMPLATE)
        from pathlib import Path

        example = (Path(__file__).resolve().parents[1] / "chat_config.example.yaml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(example, ct.CHAT_CONFIG_TEMPLATE)


class TaskAnchorPromptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.addCleanup(cc._store.commit, cc.ChatConfig())

    def test_default_on_matches_explicit_on(self) -> None:
        for mode in ("runs", "completions", "legacy"):
            with self.subTest(mode=mode):
                cc._store.commit(cc.ChatConfig())
                default = _render(mode, True)
                _commit(True)
                explicit = _render(mode, True)
                for name in ("group_system", "private_system", "group_user", "private_user"):
                    self.assertEqual(_norm(default[name]), _norm(explicit[name]), f"{mode}/{name}")

    def test_on_keeps_anchor_guidance(self) -> None:
        for mode in ("runs", "completions", "legacy"):
            with self.subTest(mode=mode):
                rendered = _render(mode, True)
                self.assertTrue(anchor_prompt_residue(rendered["group_system"]))
                self.assertIn("<pinned_tasks>", rendered["group_user"])
                # 旧接口私聊块本来就没有锚说明，只在两种纯文本接口下断言。
                if mode != "legacy":
                    self.assertIn("任务锚", rendered["private_system"])

    def test_off_removes_guidance_and_pinned_block(self) -> None:
        for mode in ("runs", "completions", "legacy"):
            with self.subTest(mode=mode):
                rendered = _render(mode, False)
                for name, text in rendered.items():
                    self.assertEqual(anchor_prompt_residue(text), [], f"{mode}/{name}")
                    self.assertNotIn("<pinned_tasks>", text)
                self.assertNotIn("pin_anchor", rendered["group_system"])
                self.assertNotIn(ANCHOR_FIELD_NAMES[0], rendered["private_system"])

    def test_off_keeps_non_anchor_controls(self) -> None:
        rendered = _render("runs", False)
        group = rendered["group_system"]
        self.assertIn("should_exit_active=true 表示这轮谈完了要退窗。", group)
        self.assertIn("reply_to 填 `m:<id>`", group)
        self.assertIn("需要引用消息或提前退窗时", group)
        self.assertIn("退窗只在群活跃态生效。", rendered["private_system"])
        # 段落结构与正文协议其余部分不变：分节符信封、控制工具只登记等仍在。
        self.assertIn("只有被两个 § 完整包住的段才会发出去", group)
        self.assertIn("控制工具只登记，不直接发消息，也不会自己退窗。", group)

    def test_off_renders_expected_block_constants(self) -> None:
        """开关选择的是现成常量：开启/关闭分别锚到既有块与去锚块。"""
        _commit(True)
        with patch.object(plugin_config, "hermes_api_interface", "runs"), \
                patch.object(pb, "sticker_feature_enabled", return_value=False):
            on = pb.build_reactive_system_prompt(
                adapter="onebotv11", group_id="g1", self_id="999")
            _commit(False)
            off = pb.build_reactive_system_prompt(
                adapter="onebotv11", group_id="g1", self_id="999")
        self.assertTrue(on.startswith(pb._DECISION_PROTOCOL_BLOCK_RUNS))
        self.assertTrue(off.startswith(pb._DECISION_PROTOCOL_BLOCK_RUNS_NO_ANCHOR))
        self.assertNotEqual(
            pb._DECISION_PROTOCOL_BLOCK_RUNS, pb._DECISION_PROTOCOL_BLOCK_RUNS_NO_ANCHOR
        )
        # 去锚块不得反向污染开启块。
        self.assertIn("pin_anchor", pb._DECISION_PROTOCOL_BLOCK_RUNS)

    def test_user_reminder_follows_switch(self) -> None:
        on = _render("runs", True)["group_user"]
        off = _render("runs", False)["group_user"]
        self.assertIn("需要引用／退窗／锚控制时先调 submit_control 登记", on)
        self.assertIn("需要引用／退窗控制时先调 submit_control 登记", off)
        self.assertNotIn("锚控制", off)

        on = _render("completions", True)["group_user"]
        off = _render("completions", False)["group_user"]
        self.assertIn("需要引用／退窗／锚时调用 submit_control。", on)
        self.assertIn("需要引用／退窗时调用 submit_control。", off)

        off_legacy = _render("legacy", False)["group_user"]
        self.assertIn("should_reply / reply_text / reply_to / should_exit_active / not_final", off_legacy)

    def test_legacy_structured_hint_follows_switch(self) -> None:
        _commit(True)
        on = _decision_hint()
        _commit(False)
        off = _decision_hint()
        for name in ANCHOR_FIELD_NAMES:
            self.assertIn(name, on)
            self.assertNotIn(name, off)
        self.assertIn("should_exit_active (boolean, optional)", off)
        self.assertIn("reply_to (string, optional —", off)
        # 除四个锚字段行外逐行一致（其余字段行与固定尾注不变）。
        removed_lines = [
            line for line in on.splitlines()
            if not any(line.startswith(f"  {name} ") for name in ANCHOR_FIELD_NAMES)
        ]
        self.assertEqual(removed_lines, off.splitlines())


class TaskAnchorRestoreTests(unittest.TestCase):
    """关闭只隐藏，不清理；重开按原规则恢复。"""

    def setUp(self) -> None:
        self.addCleanup(cc._store.commit, cc.ChatConfig())
        self.manager = ActiveSessionManager()
        self.session = self.manager.trigger("onebotv11", "g1", "u1", now_ms=NOW)
        self.record = AnchorRecord(
            participants=[Participant(user_id="u1", nickname="u1", joined_ts=NOW)],
            text="原始委托", ts=NOW, m_id=42,
        )
        self.assertTrue(self.manager.pin_anchor(
            self.session, self.record, master_ids=set(), max_age_ms=60_000, now_ms=NOW,
        ))

    def test_off_hides_but_keeps_and_reopen_restores(self) -> None:
        _commit(False)
        # 关闭时 handler 不读锚 → 提示词里没有 pinned 块。
        off_prompt = pb.build_reactive_user_content(
            addressed_to_bot=True, recent_messages=[], rows=[_row_obj()], current_image_urls=[],
            pinned_anchors=[self.record], now_ms=NOW, self_id="999", adapter="onebotv11",
        ).content
        self.assertNotIn("<pinned_tasks>", off_prompt)
        # 状态仍在（读取路径本身不删数据）。
        self.assertEqual(self.manager.pinned_commissioners("onebotv11", "g1"), {"u1"})
        # 重新开启：同一份锚按原规则读回并渲染。
        _commit(True)
        anchors = self.manager.get_anchors("onebotv11", "g1", NOW, max_age_ms=60_000)
        self.assertEqual([a.text for a in anchors], ["原始委托"])
        self.assertEqual(anchors[0].m_id, 42)
        on_prompt = pb.build_reactive_user_content(
            addressed_to_bot=True, recent_messages=[], rows=[_row_obj()], current_image_urls=[],
            pinned_anchors=anchors, now_ms=NOW, self_id="999", adapter="onebotv11",
        ).content
        self.assertIn("<pinned_tasks>", on_prompt)
        self.assertIn("原始委托", on_prompt)


class TaskAnchorToolGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.addCleanup(cc._store.commit, cc.ChatConfig())
        control_intent.CONTROL_QUEUE.discard(KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, KEY)

    def _call(self, **kwargs):
        inp = SubmitControlInput(adapter="onebotv11", group_id="g1", **kwargs)
        return submit_control_impl(
            inp, scope=CallerScope.dev(), interface="runs",
            queue=control_intent.CONTROL_QUEUE,
        )

    def test_anchor_only_call_is_an_honest_failure(self) -> None:
        _commit(False)
        result = self._call(pin_anchor=True)
        self.assertFalse(result.ok)
        self.assertFalse(result.registered)
        self.assertEqual(result.ignored_fields, ["pin_anchor"])
        self.assertIn("task_anchor_enabled=false", result.error or "")
        self.assertEqual(control_intent.CONTROL_QUEUE.pending(KEY), 0)

    def test_mixed_call_keeps_non_anchor_fields(self) -> None:
        _commit(False)
        result = self._call(
            reply_to="m:7", should_exit_active=True, pin_anchor=True,
            release_anchor="u2", answer_anchor="all", anchor_merge="u2",
        )
        self.assertTrue(result.ok)
        self.assertTrue(result.registered)
        self.assertEqual(result.fields, ["reply_to", "should_exit_active"])
        self.assertEqual(
            result.ignored_fields,
            ["pin_anchor", "release_anchor", "answer_anchor", "anchor_merge"],
        )
        self.assertEqual(
            control_intent.CONTROL_QUEUE.pop(KEY),
            {"reply_to": "m:7", "should_exit_active": True},
        )

    def test_non_anchor_control_unchanged_when_off(self) -> None:
        _commit(False)
        result = self._call(reply_to="m:9")
        self.assertTrue(result.ok)
        self.assertEqual(result.ignored_fields, [])
        self.assertEqual(control_intent.CONTROL_QUEUE.pop(KEY), {"reply_to": "m:9"})

    def test_enabled_switch_keeps_full_anchor_registration(self) -> None:
        _commit(True)
        result = self._call(pin_anchor=True, answer_anchor="all")
        self.assertTrue(result.ok)
        self.assertEqual(result.ignored_fields, [])
        self.assertEqual(
            control_intent.CONTROL_QUEUE.pop(KEY), {"pin_anchor": True, "answer_anchor": "all"},
        )


class TaskAnchorHandlerTests(e2._ReactiveHarness):
    """真实 _run_reactive_turn：关闭时锚控制零调用，非锚控制照常。"""

    def setUp(self) -> None:
        self.addCleanup(cc._store.commit, cc.ChatConfig())
        control_intent.CONTROL_QUEUE.discard(KEY)
        self.addCleanup(control_intent.CONTROL_QUEUE.discard, KEY)
        self.now = int(__import__("time").time() * 1000)
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    def _plain_tracker(self):
        return self._tracker()

    def _live_manager(self):
        manager = ActiveSessionManager()
        manager.trigger("onebotv11", "g1", "u1", now_ms=self.now)
        observed = {"pins": 0, "releases": 0, "answers": 0}

        real_pin = manager.pin_anchor

        def pin(*args, **kwargs):
            observed["pins"] += 1
            return real_pin(*args, **kwargs)

        real_release = manager.release_anchor

        def release(*args, **kwargs):
            observed["releases"] += 1
            return real_release(*args, **kwargs)

        real_answer = manager.mark_anchor_answered

        def answer(*args, **kwargs):
            observed["answers"] += 1
            return real_answer(*args, **kwargs)

        manager.pin_anchor = MagicMock(side_effect=pin)
        manager.release_anchor = MagicMock(side_effect=release)
        manager.mark_anchor_answered = MagicMock(side_effect=answer)
        return manager, observed

    async def test_anchor_controls_do_nothing_when_off(self) -> None:
        manager, observed = self._live_manager()
        buffered = _row("帮我跟进这件事", now=self.now)
        buffered.id = 5
        control_intent.CONTROL_QUEUE.register(
            KEY,
            {"pin_anchor": True, "answer_anchor": "u1", "release_anchor": "all",
             "anchor_merge": "u1"},
        )
        calls = await self._run(
            _result('{"body":"甲"}'), tracker=self._plain_tracker(),
            sessions=manager, current_buffered=buffered, now_ms=self.now,
            task_anchor_enabled=False,
        )
        self.assertEqual([c["text"] for c in calls["send"]], ["甲"])
        self.assertEqual(observed, {"pins": 0, "releases": 0, "answers": 0})
        self.assertEqual(manager.pinned_commissioners("onebotv11", "g1"), set())
        # 窗口未被锚控制开合：仍在。
        self.assertIsNotNone(manager.get("onebotv11", "g1"))

    async def test_exit_and_reply_to_still_work_when_off(self) -> None:
        manager, observed = self._live_manager()
        resolved: list = []
        control_intent.CONTROL_QUEUE.register(
            KEY, {"reply_to": "m:7", "should_exit_active": True, "pin_anchor": True},
        )
        await self._run(
            _result('{"body":"甲"}'), tracker=self._plain_tracker(),
            sessions=manager, now_ms=self.now, task_anchor_enabled=False,
            resolve_reply=MagicMock(side_effect=lambda *a, **kw: resolved.append(kw["reply_to"])),
        )
        self.assertEqual(resolved, ["m:7"])
        self.assertIsNone(manager.get("onebotv11", "g1"))
        self.assertEqual(observed["pins"], 0)

    def _manager_with_anchor(self):
        """在钉锚 u2 的任务：非法 anchor_merge / answer_anchor 才会触发纠错便签。"""
        manager = ActiveSessionManager()
        session = manager.trigger("onebotv11", "g1", "u1", now_ms=self.now)
        anchor = AnchorRecord(
            participants=[Participant(user_id="u2", nickname="B", joined_ts=self.now - 1000)],
            text="存量委托", ts=self.now - 1000, m_id=12,
        )
        self.assertTrue(manager.pin_anchor(
            session, anchor, master_ids=set(), max_age_ms=3_600_000, now_ms=self.now,
        ))
        return manager

    async def _run_capturing_user_content(self, manager, *, enabled, control=None):
        """真实 _run_reactive_turn + 真实 task_notes 队列，捕获本轮 user content。"""
        _commit(enabled)
        buffered = _row("跟进这件事", now=self.now)
        buffered.id = 5
        result = _result('{"body":"答复"}')
        if control:
            result.structured["_control"] = control
        captured: list[str] = []
        real_build = mt.build_reactive_user_content
        original = mt._run_reactive_turn

        def capture(**kwargs):
            rendered = real_build(**kwargs)
            # 条目 145：runs 文本轮的通知区拆进了 context_notices（模型仍能看到），
            # 这里按「模型实际可见」拼回去断言，不改变本用例的语义。
            captured.append(rendered.content + (
                f"\n\n{rendered.context_notices}" if rendered.context_notices else ""
            ))
            return rendered

        async def actual(**kwargs):
            kwargs["ephemeral"] = False
            with patch.object(mt, "task_notes", task_notes), \
                    patch.object(mt, "build_reactive_user_content", capture):
                return await original(**kwargs)

        with patch.object(mt, "_run_reactive_turn", actual):
            await self._run(
                result, tracker=self._plain_tracker(), sessions=manager,
                current_buffered=buffered, now_ms=self.now,
                task_anchor_enabled=enabled,
            )
        return captured

    async def test_anchor_feedback_not_injected_after_switch_off(self) -> None:
        """开→非法锚反馈→关：关闭后下一轮不再新组装锚纠错，普通便签照常。

        复用既有便签队列/组装链；开启轮的两条纠错反馈逐字保留，关闭轮只丢弃
        anchor-merge-invalid / answer-anchor-invalid 两类，其余便签原样贴回。
        """
        manager = self._manager_with_anchor()
        task_notes.pop_plugin_notes(KEY)
        self.addCleanup(task_notes.pop_plugin_notes, KEY)
        await self._run_capturing_user_content(
            manager, enabled=True,
            control={"anchor_merge": "nonexistent", "answer_anchor": "nonexistent"},
        )
        queued = task_notes._plugin_notes.get(KEY, [])
        self.assertEqual(len(queued), 2)
        # 开启时组装不变：下一轮（仍开启）两条纠错如实贴进 user content。
        on_prompts = await self._run_capturing_user_content(manager, enabled=True)
        on = "\n".join(on_prompts)
        self.assertIn("anchor_merge=nonexistent", on)
        self.assertIn("要归并请填在钉任务的发起者 id", on)
        self.assertIn("answer_anchor=nonexistent", on)
        self.assertIn("要标应答请填在钉委托人的 id 或 all", on)
        self.assertEqual(task_notes._plugin_notes.get(KEY, []), [])
        # 再做一次开启轮产生纠错，混入一条普通（非锚）便签后切关闭。
        await self._run_capturing_user_content(
            manager, enabled=True,
            control={"anchor_merge": "nonexistent", "answer_anchor": "nonexistent"},
        )
        self.assertEqual(len(task_notes._plugin_notes.get(KEY, [])), 2)
        task_notes.push_plugin_note(KEY, "media-inline-missing", "普通便签：图片没取到")
        off_prompts = await self._run_capturing_user_content(manager, enabled=False)
        off = "\n".join(off_prompts)
        self.assertNotIn("anchor_merge=nonexistent", off)
        self.assertNotIn("answer_anchor=nonexistent", off)
        self.assertNotIn("要归并请填在钉任务的发起者 id", off)
        self.assertNotIn("要标应答请填在钉委托人的 id 或 all", off)
        self.assertIn("普通便签：图片没取到", off)
        # 两类锚纠错按见即焚丢弃，不残留到再开启轮。
        self.assertEqual(task_notes._plugin_notes.get(KEY, []), [])


if __name__ == "__main__":
    unittest.main()
