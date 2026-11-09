"""段内重复吞掉 → 群聊触发消息表情观察（t_b611baca）。

主人批准的最小增量：Runs 同一 assistant 段的 interim／delta 完整单元全文匹配
被吞（reason=segment_duplicate）时，给「本轮应该回复的用户触发消息」贴一次表情，
作为真实使用中的快速可见验证。覆盖：

- 多个段内重复只贴 1 次（首次命中即置位，失败不重试）；
- unsealed_delta_tail／跨段同文／终态重复都不贴；
- 只接 onebotv11 群聊 reactive：私聊（passive）路径不接线、不调用；
- NapCat 外层 retcode=0 但内部 data.result=65011 / errMsg 非空判失败；
- 429「丢了就贴」原路径行为不变（仍用 rate_limit_emoji_id）。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests import test_runs_terminal as _e2  # noqa: E402

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_feedback as mf  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers.message_feedback import (  # noqa: E402
    _emoji_like_quiet,
)

TRIGGER = "m:77"
EMOJI_ID = 32


async def _settle() -> None:
    """让 fire-and-forget 的后台任务跑完（只在本测试的事件循环里）。"""
    for _ in range(5):
        await asyncio.sleep(0)


def _bot(data=None, *, exc: Exception | None = None) -> MagicMock:
    bot = MagicMock(self_id=999)
    if exc is not None:
        bot.call_api = AsyncMock(side_effect=exc)
    else:
        bot.call_api = AsyncMock(return_value=data)
    return bot


class EmojiLikeQuietTests(unittest.IsolatedAsyncioTestCase):
    """`_emoji_like_quiet` 的返回契约：外层成功不等于贴上。"""

    async def test_group_result_zero_is_success(self) -> None:
        bot = _bot({"result": 0, "errMsg": ""})
        self.assertTrue(await _emoji_like_quiet(bot, TRIGGER, EMOJI_ID))
        bot.call_api.assert_awaited_once_with(
            "set_msg_emoji_like", message_id=TRIGGER, emoji_id=EMOJI_ID
        )

    async def test_legacy_adapter_without_result_is_success(self) -> None:
        # 旧适配器只回 data 本体（没有 result 键）：保持原 429 调用兼容。
        bot = _bot({"file_id": "f"})
        self.assertTrue(await _emoji_like_quiet(bot, TRIGGER, EMOJI_ID))

    async def test_private_inner_result_65011_is_failure(self) -> None:
        # 真实 NapCat 私聊：外层 status=ok/retcode=0，内部 result=65011。
        bot = _bot({"result": 65011, "errMsg": "操作失败，请稍后再试哦"})
        with patch.object(mf, "logger") as log:
            self.assertFalse(await _emoji_like_quiet(bot, TRIGGER, EMOJI_ID))
        log.warning.assert_called_once()
        self.assertIn("65011", log.warning.call_args.args[0])

    async def test_inner_errmsg_with_zero_result_is_failure(self) -> None:
        bot = _bot({"result": 0, "errMsg": "被风控"})
        self.assertFalse(await _emoji_like_quiet(bot, TRIGGER, EMOJI_ID))

    async def test_call_exception_is_failure_and_never_raises(self) -> None:
        bot = _bot(exc=RuntimeError("boom"))
        self.assertFalse(await _emoji_like_quiet(bot, TRIGGER, EMOJI_ID))


class SegmentDuplicateHookTests(unittest.IsolatedAsyncioTestCase):
    """观察口本身：只认 segment_duplicate、每轮一次、失败不影响正文。"""

    def _hook(self, bot, *, adapter="onebotv11", interface="runs", msg_id: str | None = TRIGGER):
        return mt._segment_duplicate_emoji_hook(
            bot=bot,
            trigger_msg_id=msg_id,
            group_id="g1",
            adapter_name=adapter,
            interface=interface,
        )

    async def test_multiple_duplicates_like_once(self) -> None:
        bot = _bot({"result": 0, "errMsg": ""})
        hook = self._hook(bot)
        for _ in range(3):
            hook("segment_duplicate", "好", "segment")
        await _settle()
        self.assertEqual(bot.call_api.await_count, 1)
        bot.call_api.assert_awaited_once_with(
            "set_msg_emoji_like", message_id=TRIGGER, emoji_id=EMOJI_ID
        )

    async def test_other_reasons_do_not_like(self) -> None:
        bot = _bot({"result": 0, "errMsg": ""})
        hook = self._hook(bot)
        for reason in ("unsealed_delta_tail", "terminal_duplicate", "cross_segment"):
            hook(reason, "正文", "terminal")
        await _settle()
        bot.call_api.assert_not_awaited()

    async def test_platform_rejection_is_not_retried(self) -> None:
        # 私聊能力边界：内部 65011 判失败，第二轮重复也不再试（每轮最多一次）。
        bot = _bot({"result": 65011, "errMsg": "操作失败，请稍后再试哦"})
        hook = self._hook(bot)
        hook("segment_duplicate", "好", "segment")
        hook("segment_duplicate", "好", "segment")
        await _settle()
        self.assertEqual(bot.call_api.await_count, 1)

    async def test_transport_failure_never_propagates(self) -> None:
        bot = _bot(exc=RuntimeError("network down"))
        hook = self._hook(bot)
        hook("segment_duplicate", "好", "segment")  # 不得抛出
        await _settle()
        self.assertEqual(bot.call_api.await_count, 1)

    def test_not_armed_outside_group_runs_path(self) -> None:
        bot = _bot({"result": 0, "errMsg": ""})
        self.assertIsNone(self._hook(bot, interface="completions"))
        self.assertIsNone(self._hook(bot, interface="astrbot"))
        self.assertIsNone(self._hook(bot, adapter="astrbot"))
        self.assertIsNone(self._hook(bot, msg_id=None))


def sse(events: list[dict]) -> bytes:
    return "\n".join(
        f"data: {json.dumps(event, ensure_ascii=False)}" for event in events
    ).encode() + b"\n"


class RunsClientToHookTests(unittest.IsolatedAsyncioTestCase):
    """真实 chat() → 真实 _chat_via_runs → 真实 parser：观察口按帧语义触发。"""

    def _client(self, events: list[dict]) -> tuple[httpx.MockTransport, list[str]]:
        seen_paths: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_paths.append(request.url.path)
            if request.url.path == "/v1/runs" and request.method == "POST":
                return httpx.Response(200, json={"run_id": "run-emoji-1"})
            if request.url.path == "/v1/runs/run-emoji-1/events":
                return httpx.Response(
                    200,
                    content=sse(events),
                    headers={"content-type": "text/event-stream"},
                )
            raise AssertionError(f"unexpected request {request.method} {request.url}")

        return httpx.MockTransport(handler), seen_paths

    async def _chat(self, events: list[dict], hook) -> tuple[ChatResult, list[str]]:
        transport, seen = self._client(events)
        real_client = httpx.AsyncClient
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        try:
            async with real_client(transport=transport) as client:
                with patch(
                    "nonebot_plugin_hermes_adapter.core.hermes_client._get_http_client",
                    lambda: client,
                ):
                    result = await mt.hermes_client.chat(
                        text="在吗",
                        session_key="sess",
                        user_id="u1",
                        group_id="g1",
                        adapter_name="onebotv11",
                        is_private=False,
                        mode="reactive",
                        system_prompt="SYS",
                        target=SimpleNamespace(
                            base_url="http://offline.invalid",
                            api_key="",
                            timeout=5,
                            label="t",
                        ),
                        on_suppressed=hook,
                    )
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        return result, seen

    async def test_repeated_interims_like_once(self) -> None:
        bot = _bot({"result": 0, "errMsg": ""})
        hook = mt._segment_duplicate_emoji_hook(
            bot=bot, trigger_msg_id=TRIGGER, group_id="g1",
            adapter_name="onebotv11", interface="runs",
        )
        result, seen = await self._chat([
            {"event": "message.interim", "text": "好", "seq": 0},
            {"event": "message.interim", "text": "好", "seq": 1},
            {"event": "message.interim", "text": "好", "seq": 2},
            {"event": "run.completed", "output": "", "seq": 3},
        ], hook)
        await _settle()
        self.assertFalse(result.parse_failed)
        self.assertEqual(seen, ["/v1/runs", "/v1/runs/run-emoji-1/events"])
        self.assertEqual(bot.call_api.await_count, 1)
        bot.call_api.assert_awaited_once_with(
            "set_msg_emoji_like", message_id=TRIGGER, emoji_id=EMOJI_ID
        )

    async def test_unsealed_delta_tail_does_not_like(self) -> None:
        bot = _bot({"result": 0, "errMsg": ""})
        hook = mt._segment_duplicate_emoji_hook(
            bot=bot, trigger_msg_id=TRIGGER, group_id="g1",
            adapter_name="onebotv11", interface="runs",
        )
        await self._chat([
            {"event": "message.delta", "delta": "未封口的尾巴", "seq": 0},
            {"event": "run.completed", "output": "真正的终态正文", "seq": 1},
        ], hook)
        await _settle()
        bot.call_api.assert_not_awaited()

    async def test_cross_segment_same_text_does_not_like(self) -> None:
        bot = _bot({"result": 0, "errMsg": ""})
        hook = mt._segment_duplicate_emoji_hook(
            bot=bot, trigger_msg_id=TRIGGER, group_id="g1",
            adapter_name="onebotv11", interface="runs",
        )
        await self._chat([
            # 第一段：delta 后工具封段 → 发出。
            {"event": "message.delta", "delta": "同一句", "seq": 0},
            {"event": "tool.started", "tool": "todo_list", "seq": 1},
            # 第二段同文：终端 output 照发，不是段内重复。
            {"event": "run.completed", "output": "同一句", "seq": 2},
        ], hook)
        await _settle()
        bot.call_api.assert_not_awaited()

    async def test_no_hook_keeps_old_behavior(self) -> None:
        result, _ = await self._chat([
            {"event": "message.interim", "text": "好", "seq": 0},
            {"event": "message.interim", "text": "好", "seq": 1},
            {"event": "run.completed", "output": "", "seq": 2},
        ], None)
        self.assertFalse(result.parse_failed)


class GroupReactiveWiringTests(_e2._ReactiveHarness):
    """真实 _run_reactive_turn：群聊接线、429 原路径与私聊不接线。"""

    def _silent(self) -> ChatResult:
        result = ChatResult(raw_text="", structured={"should_reply": False})
        result.run_id = "run-e2-h"
        result.terminal_decisions = []
        return result

    async def test_group_turn_passes_hook(self) -> None:
        captured: dict = {}

        async def fake_chat(label, **kwargs):
            captured.update(kwargs)
            return self._silent()

        emoji = AsyncMock(return_value=True)
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        try:
            with patch.object(mt, "_emoji_like_quiet", emoji):
                await self._run(
                    self._silent(), tracker=self._tracker(), chat=fake_chat,
                    trigger_msg_id=TRIGGER,
                )
                hook = captured.get("on_suppressed")
                assert callable(hook)
                # 终态尾丢弃传进来也不贴：只有 segment_duplicate 才动平台。
                hook("unsealed_delta_tail", "尾巴", "terminal")
                await _settle()
                emoji.assert_not_awaited()
                # 同一轮多个段内重复只贴 1 次。
                hook("segment_duplicate", "好", "segment")
                hook("segment_duplicate", "好", "segment")
                await _settle()
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        self.assertEqual(emoji.await_count, 1)
        call = emoji.await_args
        assert call is not None
        self.assertEqual(call.args[1], TRIGGER)
        self.assertEqual(call.args[2], EMOJI_ID)

    async def test_group_turn_without_trigger_msg_id_not_armed(self) -> None:
        captured: dict = {}

        async def fake_chat(label, **kwargs):
            captured.update(kwargs)
            return self._silent()

        await self._run(self._silent(), tracker=self._tracker(), chat=fake_chat)
        self.assertIsNone(captured.get("on_suppressed"))

    async def test_rate_limit_429_keeps_emoji_path(self) -> None:
        result = ChatResult(
            raw_text="", parse_failed=True,
            is_transport_error=True, is_rate_limited=True,
        )
        result.run_id = "run-e2-h"
        emoji = AsyncMock(return_value=True)
        with patch.object(mt, "_emoji_like_quiet", emoji):
            calls = await self._run(
                result, tracker=self._tracker(), trigger_msg_id=TRIGGER,
            )
        emoji.assert_awaited_once()
        self.assertEqual(emoji.await_args.args[1], TRIGGER)
        # 429 满额丢单不发任何文本兜底。
        self.assertEqual(calls["send"], [])

    def test_only_group_reactive_path_is_wired(self) -> None:
        reactive = inspect.getsource(mt._run_reactive_turn)
        passive = inspect.getsource(mt._run_passive_turn)
        self.assertIn("on_suppressed=_suppressed_hook", reactive)
        self.assertIn("_segment_duplicate_emoji_hook(", reactive)
        # 私聊（passive）路径绝不接线：NapCat 私聊贴表情是能力边界。
        self.assertNotIn("on_suppressed", passive)
        self.assertNotIn("_segment_duplicate_emoji_hook", passive)


if __name__ == "__main__":
    unittest.main()
