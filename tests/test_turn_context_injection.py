"""条目 145：runs 文本轮的 hermes_context 拆分与注入（NB 侧，先 RED）。

规格：``docs/nonebot-plugin-hermes-docs/alive/145_计划_Hermes通知专用上下文注入_2026-11-25施工_.md``

覆盖任务书第 9 节 NB builder／client 九项：
1. reactive 纯文本有通知：可见 content 无顶层通知，context_notices 完整，
   正文 reminder 与 closing_reminder 一致；
2. passive 纯文本有通知：可见 content 无通知，context_notices 完整；
3. 纯通知唤醒：拆后 content 非空，通知进独立字段；
4. 无通知：不发 hermes_context，content 与旧版逐字一致；
5. completions 与多模态：保留旧 content，不发新字段；
6. 通知文本里出现聊天区标记、聊天正文里出现通知区字面量时不误拆；
7. runs 真实组包：body 含契约字段，session／instructions／input／幂等键不变；
8. 202 缺少接受头：一条脱敏 WARNING，基本 runs 流程继续；``registered`` 不警告；
9. request dump 与 run submission 保存同一实际 body。
"""

from __future__ import annotations

import itertools
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import turn_context as tc  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import (  # noqa: E402
    MessageOrigin,
    MessageSource,
)
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402

ADAPTER = "onebotv11"
SELF = "2968545011"
GROUP = "253384307"
SESSION_KEY = f"hermes-qqtpc+group+{GROUP}-g2"
FIXED_TIME = "2026-11-25 周三 12:00"

NOTICE_OPEN = "<系统通知区域起始>"
NOTICE_CLOSE = "<系统通知区域结束>"
CHAT_OPEN = "<聊天消息区域起始>"
CHAT_CLOSE = "<聊天消息区域结束>"

_RUN_SEQ = itertools.count(1)


def human(mid: int = 71429, content: str = "看这条", *, group: str | None = GROUP) -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id="10001", nickname="小光",
        content=content, origin=MessageOrigin.human(), id=mid,
    )


def notice_row(content: str = "任务 t_1 已完成\n结果正文", *, label: str = "Kanban通知") -> BufferedMessage:
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=GROUP, user_id="0", nickname="",
        content=content, origin=MessageOrigin.synthetic(MessageSource.WAKE_MANUAL, label=label),
        id=None,
    )


def build_reactive(rows, *, images=(), stickers=(), recent=()):
    with patch.object(pb, "_current_time_str", return_value=FIXED_TIME):
        return pb.build_reactive_user_content(
            addressed_to_bot=False,
            recent_messages=list(recent),
            rows=list(rows),
            current_image_urls=list(images),
            current_sticker_urls=list(stickers),
            self_id=SELF, adapter=ADAPTER,
        )


def build_passive(rows, *, images=(), stickers=(), recent=()):
    with patch.object(pb, "_current_time_str", return_value=FIXED_TIME):
        return pb.build_passive_user_content(
            recent_messages=list(recent),
            rows=list(rows),
            current_image_urls=list(images),
            current_sticker_urls=list(stickers),
            is_private=True,
            self_id=SELF, adapter=ADAPTER,
        )


def expected_notice_region(rows) -> str:
    """通知区期望值：同一渲染器的产物（证明拆出来的就是完整通知区，不是碎片）。"""
    notices = [pb._notice_from_row(row) for row in rows if row.id is None]
    return pb._render_notice_region(notices)


def _runs_reminder() -> str:
    """runs 文本轮的近因 reminder 逐字副本（正文尾部与 closing_reminder 同一份）。"""
    anchor_hint = (
        "需要引用／退窗／锚控制时先调 submit_control 登记，普通回复不要调控制工具。"
        if pb.task_anchor_enabled()
        else "需要引用／退窗控制时先调 submit_control 登记，普通回复不要调控制工具。"
    )
    return (
        "\n\n请直接输出正文，不要写 JSON 信封；每份正文用独立一行 § 开头、独立一行 § 结尾，"
        "多条时每份都要自己写开头和结尾（相邻两份是两个 § 行），静默写两行 §；"
        + anchor_hint
        + pb._RUNS_VISIBLE_REPLY_REMINDER
    )


class _Interface:
    """在用例内切换 hermes_api_interface（builder 的 modes 由它决定）。"""

    def __init__(self, testcase, interface: str) -> None:
        self.testcase = testcase
        self.interface = interface

    def __enter__(self):
        self.previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", self.interface)
        return self

    def __exit__(self, *exc):
        object.__setattr__(plugin_config, "hermes_api_interface", self.previous)
        return False


class BuilderRunsSplitTests(unittest.TestCase):
    """第 1-6 项：builder 行为矩阵。"""

    def test_reactive_text_turn_moves_notice_out_of_visible_content(self) -> None:
        notice = notice_row()
        with _Interface(self, "runs"):
            out = build_reactive([human(), notice])

        self.assertIsInstance(out.content, str)
        # 1. 可见 content 只有真实正文，不含通知／时间／区域／消息头
        self.assertEqual(out.content, "看这条")
        self.assertNotIn(NOTICE_OPEN, out.content)
        self.assertNotIn(CHAT_OPEN, out.content)
        # 2. 通知完整进了独立字段
        self.assertEqual(out.context_notices, expected_notice_region([notice]))
        self.assertIn("t_1 已完成", out.context_notices)
        # 3. 近因 reminder 只走 closing_reminder，不再写回正文
        self.assertTrue(out.closing_reminder.startswith("\n\n请直接输出正文"))
        self.assertNotIn("请直接输出正文", out.content)
        # 4. 动态事实走 request_context
        self.assertTrue(out.request_context.startswith(f"（当前时间：{FIXED_TIME}）"))
        self.assertIn("[m:71429]", out.request_context)

    def test_passive_text_turn_moves_notice_out_without_reminder(self) -> None:
        notice = notice_row()
        with _Interface(self, "runs"):
            out = build_passive([human(), notice])

        self.assertIsInstance(out.content, str)
        self.assertEqual(out.content, "看这条")
        self.assertNotIn(NOTICE_OPEN, out.content)
        self.assertEqual(out.context_notices, expected_notice_region([notice]))
        self.assertEqual(out.closing_reminder, "", "passive 不带 closing_reminder")
        self.assertTrue(out.request_context.startswith(f"（当前时间：{FIXED_TIME}）"))

    def test_pure_notice_wake_keeps_visible_input_non_empty(self) -> None:
        notice = notice_row()
        with _Interface(self, "runs"):
            reactive = build_reactive([notice])
            passive = build_passive([notice])

        # 没有可呈递正文时用单个空格占位；通知与时间都不在正文里
        self.assertEqual(reactive.content, " ")
        self.assertEqual(passive.content, " ")
        self.assertNotIn(NOTICE_OPEN, reactive.content)
        self.assertNotIn("当前时间", reactive.content)
        self.assertEqual(reactive.context_notices, expected_notice_region([notice]))
        self.assertEqual(passive.context_notices, expected_notice_region([notice]))
        self.assertTrue(reactive.request_context.startswith(f"（当前时间：{FIXED_TIME}）"))

    def test_no_notice_turn_has_clean_body_and_request_context(self) -> None:
        """无通知回合：可见 content 只有正文；动态事实仍在 request_context。"""
        rows = [human()]
        with _Interface(self, "runs"):
            out = build_passive(rows)
        self.assertEqual(out.content, "看这条")
        self.assertEqual(out.context_notices, "")
        self.assertEqual(out.closing_reminder, "")
        self.assertNotIn(NOTICE_OPEN, out.content)
        self.assertIn("[m:71429]", out.request_context)

        with _Interface(self, "runs"):
            out_r = build_reactive(rows)
        self.assertEqual(out_r.content, "看这条")
        self.assertEqual(out_r.context_notices, "")
        self.assertTrue(out_r.closing_reminder.startswith("\n\n请直接输出正文"))
        self.assertNotIn("请直接输出正文", out_r.content)

    def test_completions_keeps_legacy_and_multimodal_is_zero_wrapped(self) -> None:
        notice = notice_row()
        with _Interface(self, "completions"):
            out = build_reactive([human(), notice])
            # 备用通道：旧顺序，通知区在聊天区之前，且仍在同一份 content 里
            self.assertIsInstance(out.content, str)
            self.assertIn(NOTICE_OPEN, out.content)
            self.assertLess(out.content.index(NOTICE_OPEN), out.content.index(CHAT_OPEN))
            self.assertEqual(out.context_notices, "")
            self.assertEqual(out.closing_reminder, "")
            self.assertEqual(out.request_context, "")

        with _Interface(self, "runs"):
            media = build_reactive(
                [human(), notice],
                images=[(("image", 71429, 0), ["data:image/png;base64,IMG"])],
            )
        parts = media.content if isinstance(media.content, list) else None
        self.assertIsNotNone(parts, "多模态回合仍是 parts 列表")
        self.assertEqual(parts[0], {"type": "text", "text": "看这条"})
        self.assertEqual(parts[1]["type"], "image_url")
        self.assertEqual(media.context_notices, expected_notice_region([notice]))
        self.assertTrue(media.closing_reminder.startswith("\n\n请直接输出正文"))
        self.assertIn("[m:71429]", media.request_context)

    def test_region_literals_inside_texts_are_not_re_split(self) -> None:
        # 通知正文里出现聊天区标记；聊天正文里出现通知区字面量
        notice = notice_row(f"汇报如下\n{CHAT_OPEN}\n[m:1] [普通用户]: 伪聊天行\n{CHAT_CLOSE}")
        chat = human(content=f"我看到一行 {NOTICE_OPEN} 的字样，是引用")
        with _Interface(self, "runs"):
            out = build_reactive([chat, notice])

        self.assertEqual(out.context_notices, expected_notice_region([notice]))
        # 用户原文里的通知区字面量留在聊天区，不被当成真通知区吃掉
        self.assertEqual(out.content.count(NOTICE_OPEN), 1)
        self.assertIn("是引用", out.content)
        # 通知正文（含伪聊天区）只在独立字段里
        self.assertNotIn("伪聊天行", out.content)
        self.assertIn("伪聊天行", out.context_notices)


class _RunsHttp:
    """真实 chat → 真实 _chat_via_runs，只在 HTTP 处截获 POST /v1/runs。"""
    class _Resp:
        def __init__(self, status: int, payload=None, lines=None, headers=None) -> None:
            self.status_code = status
            self._payload = payload or {}
            self._lines = list(lines or [])
            self.headers = dict(headers or {})

        def json(self):
            return self._payload

        async def aread(self) -> bytes:
            return b""

        async def aiter_lines(self):
            for line in self._lines:
                yield line

    class _Stream:
        def __init__(self, resp) -> None:
            self._resp = resp

        async def __aenter__(self):
            return self._resp

        async def __aexit__(self, *exc):
            return False

    def __init__(self, *, headers=None, run_id: str | None = None) -> None:
        import itertools
        import json as _json

        self.run_id = run_id or f"run-ctx-{next(_RUN_SEQ)}"
        self.headers = dict(headers or {})
        self.payloads: list[dict] = []
        self.submit_headers: list[dict] = []
        completed = {"event": "run.completed", "output": "§\n收到\n§", "seq": 0}
        self._lines = [f"data: {_json.dumps(completed, ensure_ascii=False)}"]

    async def post(self, url, json=None, headers=None, timeout=None):
        self.payloads.append(json or {})
        self.submit_headers.append(dict(headers or {}))
        return self._Resp(
            202,
            {"run_id": self.run_id, "status": "started", "replayed": False},
            headers=self.headers,
        )

    def stream(self, method, url, **kwargs):
        return self._Stream(self._Resp(200, lines=self._lines))


class _Submitter:
    def __init__(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self._cleanup = tmp.cleanup
        self.tracker = RunTracker(Path(tmp.name) / "t.db")

    def close(self) -> None:
        self.tracker.close()
        self._cleanup()

    async def submit(self, *, fake: _RunsHttp, **kwargs):
        from nonebot_plugin_hermes_adapter import mcp as mcp_module

        params = dict(
            text="",
            session_key=SESSION_KEY,
            user_id="10001",
            group_id=GROUP,
            adapter_name=ADAPTER,
            is_private=False,
            mode="reactive",
            expect_structured=True,
            structured_tool_name="submit_decision",
            system_prompt="SYS",
            target=SimpleNamespace(base_url="http://127.0.0.1:1", api_key="", timeout=5, label="t"),
            bot_self_id=SELF,
        )
        params.update(kwargs)
        with patch.object(hc, "_get_http_client", return_value=fake), \
                patch.object(mcp_module, "run_tracker", self.tracker):
            return await hc.hermes_client.chat(**params)


class ClientRunsBodyTests(unittest.IsolatedAsyncioTestCase):
    """第 7-9 项：真实组包、接受头降级与留痕。"""

    def setUp(self) -> None:
        self.submitter = _Submitter()
        self.addCleanup(self.submitter.close)
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

    def _content(self):
        with _Interface(self, "runs"):
            return build_reactive([human(), notice_row()])

    async def test_runs_body_carries_contract_field_and_keeps_rest(self) -> None:
        built = self._content()
        fake = _RunsHttp(headers={tc.TURN_CONTEXT_HEADER: tc.TURN_CONTEXT_REGISTERED})
        await self.submitter.submit(
            fake=fake,
            user_content_override=built.content,
            context_notices=built.context_notices,
            closing_reminder=built.closing_reminder,
        )
        body = fake.payloads[-1]
        self.assertEqual(
            body[tc.TURN_CONTEXT_FIELD],
            {
                "schema": "hermes.turn.context/1",
                "notices": built.context_notices,
                "closing_reminder": built.closing_reminder,
            },
        )
        # 原有字段与幂等键不变
        self.assertEqual(body["session_id"], SESSION_KEY)
        self.assertEqual(body["instructions"], "SYS")
        self.assertEqual(body["input"][0]["content"], built.content)
        self.assertEqual(body["model"], "hermes-agent")
        self.assertTrue(fake.submit_headers[-1]["Idempotency-Key"].startswith("turn-"))

        # 无通知回合：同一个 body，只是没有新字段
        bare = _RunsHttp()
        await self.submitter.submit(fake=bare, user_content_override=built.content)
        plain = bare.payloads[-1]
        self.assertNotIn(tc.TURN_CONTEXT_FIELD, plain)
        self.assertEqual(
            {k: v for k, v in body.items() if k != tc.TURN_CONTEXT_FIELD}, plain
        )

    async def test_runs_body_carries_request_context_for_text_and_parts(self) -> None:
        request_context = "（当前时间：2026-11-27 周五 12:00）\n第1条 → 普通用户 [id=10001 user=小光] [m:7]"
        for content in (
            "看这条",
            [{"type": "text", "text": "看这条"},
             {"type": "image_url", "image_url": {"url": "data:image/png;base64,IMG"}}],
        ):
            fake = _RunsHttp(headers={tc.TURN_CONTEXT_HEADER: tc.TURN_CONTEXT_REGISTERED})
            await self.submitter.submit(
                fake=fake,
                user_content_override=content,
                request_context=request_context,
            )
            body = fake.payloads[-1]
            self.assertEqual(body["input"][0]["content"], content)
            self.assertEqual(
                body[tc.TURN_CONTEXT_FIELD],
                {"schema": "hermes.turn.context/1", "request_context": request_context},
                type(content),
            )

    async def test_missing_accept_header_warns_and_flow_continues(self) -> None:
        built = self._content()
        fake = _RunsHttp(headers={})
        with patch.object(hc.logger, "warning") as warn:
            result = await self.submitter.submit(
                fake=fake,
                user_content_override=built.content,
                context_notices=built.context_notices,
                closing_reminder=built.closing_reminder,
            )
        self.assertEqual(len(fake.payloads), 1, "不重发")
        self.assertTrue(result.raw_text, "基本 runs 流程继续")
        warn.assert_called_once()
        message = warn.call_args[0][0]
        self.assertIn(SESSION_KEY, message)
        self.assertIn("run-ctx", message)
        for forbidden in ("任务 t_1 已完成", "结果正文", "§"):
            self.assertNotIn(forbidden, message)

    async def test_rejected_header_also_warns(self) -> None:
        built = self._content()
        fake = _RunsHttp(headers={tc.TURN_CONTEXT_HEADER: tc.TURN_CONTEXT_REJECTED})
        with patch.object(hc.logger, "warning") as warn:
            await self.submitter.submit(
                fake=fake,
                user_content_override=built.content,
                context_notices=built.context_notices,
                closing_reminder=built.closing_reminder,
            )
        warn.assert_called_once()

    async def test_registered_header_is_silent(self) -> None:
        built = self._content()
        fake = _RunsHttp(headers={tc.TURN_CONTEXT_HEADER: tc.TURN_CONTEXT_REGISTERED})
        with patch.object(hc.logger, "warning") as warn:
            await self.submitter.submit(
                fake=fake,
                user_content_override=built.content,
                context_notices=built.context_notices,
                closing_reminder=built.closing_reminder,
            )
        warn.assert_not_called()

    async def test_no_context_sent_never_warns_about_header(self) -> None:
        built = self._content()
        fake = _RunsHttp(headers={})
        with patch.object(hc.logger, "warning") as warn:
            await self.submitter.submit(fake=fake, user_content_override=built.content)
        warn.assert_not_called()

    async def test_dump_and_submission_are_the_same_body(self) -> None:
        built = self._content()
        fake = _RunsHttp(headers={tc.TURN_CONTEXT_HEADER: tc.TURN_CONTEXT_REGISTERED})
        with patch.object(hc, "maybe_dump_request") as dump:
            await self.submitter.submit(
                fake=fake,
                user_content_override=built.content,
                context_notices=built.context_notices,
                closing_reminder=built.closing_reminder,
            )
        dump.assert_called_once()
        kwargs = dump.call_args.kwargs
        self.assertEqual(kwargs["interface"], "runs")
        self.assertEqual(kwargs["request"], fake.payloads[-1])
        self.assertEqual(
            kwargs["request"][tc.TURN_CONTEXT_FIELD]["notices"], built.context_notices
        )


class ContractConstantsTests(unittest.TestCase):
    def test_wire_contract_literals(self) -> None:
        self.assertEqual(tc.TURN_CONTEXT_FIELD, "hermes_context")
        self.assertEqual(tc.TURN_CONTEXT_SCHEMA, "hermes.turn.context/1")
        self.assertEqual(tc.TURN_CONTEXT_HEADER, "X-Hermes-Turn-Context")
        self.assertEqual(tc.TURN_CONTEXT_REGISTERED, "registered")
        self.assertEqual(tc.TURN_CONTEXT_REJECTED, "rejected")

    def test_payload_only_when_notices_present(self) -> None:
        self.assertIsNone(tc.build_context_payload("", ""))
        self.assertIsNone(tc.build_context_payload("   ", ""))
        self.assertEqual(
            tc.build_context_payload(f"{NOTICE_OPEN}\nx\n{NOTICE_CLOSE}", ""),
            {"schema": "hermes.turn.context/1", "notices": f"{NOTICE_OPEN}\nx\n{NOTICE_CLOSE}"},
        )
        self.assertEqual(
            tc.build_context_payload("N", "\n\n请直接输出正文"),
            {"schema": "hermes.turn.context/1", "notices": "N",
             "closing_reminder": "\n\n请直接输出正文"},
        )

    def test_payload_carries_request_context_alongside_notices(self) -> None:
        self.assertEqual(
            tc.build_context_payload("", "", "（当前时间：2026-11-27 周五 12:00）"),
            {"schema": "hermes.turn.context/1",
             "request_context": "（当前时间：2026-11-27 周五 12:00）"},
        )
        self.assertEqual(
            tc.build_context_payload("N", "\n\n请直接输出正文", "RC"),
            {"schema": "hermes.turn.context/1", "notices": "N",
             "closing_reminder": "\n\n请直接输出正文", "request_context": "RC"},
        )
        self.assertIsNone(tc.build_context_payload("", "", "   "))


class PromptNoticeOrderNoteTests(unittest.TestCase):
    """§7.3：runs 提示词说清通知区在输入最末尾，且不写内部字段名。"""

    ORDER_NOTE = "系统通知补在输入最末尾"

    def test_runs_protocol_states_notice_region_is_last(self) -> None:
        with _Interface(self, "runs"):
            group = pb._reactive_protocol_block()
            private = pb._private_protocol_block()
        for block in (group, private):
            self.assertIn(self.ORDER_NOTE, block)
            self.assertIn("最后一段协议提醒仍然是你要遵守的输出契约", block)
            self.assertNotIn("hermes_context", block)

    def test_completions_protocol_is_untouched(self) -> None:
        with _Interface(self, "completions"):
            group = pb._reactive_protocol_block()
        self.assertNotIn(self.ORDER_NOTE, group)
        self.assertNotIn("hermes_context", group)


class PromptReminderCountNoteTests(unittest.TestCase):
    """条目 146 固定规范：（a／b）解释只进固定 system prompt，且只出现一次。"""

    NOTE = "补充材料前的（a／b）表示第 a 次提醒、共 b 次；按原材料处理，已完成的重复提醒可忽略。"

    def test_runs_protocol_states_the_note_exactly_once(self) -> None:
        with _Interface(self, "runs"):
            blocks = (pb._reactive_protocol_block(), pb._private_protocol_block())
        for block in blocks:
            self.assertEqual(block.count(self.NOTE), 1)
            self.assertEqual(block.count("（a／b）"), 1)

    def test_completions_protocol_is_untouched(self) -> None:
        with _Interface(self, "completions"):
            block = pb._reactive_protocol_block()
        self.assertNotIn(self.NOTE, block)
        self.assertNotIn("（a／b）", block)

    def test_notices_payload_is_not_rewritten_by_the_note(self) -> None:
        notice = notice_row()
        with _Interface(self, "runs"):
            out = build_reactive([human(), notice])
        self.assertNotIn("（a／b）", out.context_notices)
        self.assertNotIn("次提醒", out.context_notices)
        self.assertNotIn("（a／b）", out.content)


if __name__ == "__main__":
    unittest.main()
