"""R1 回归：queued-lane 前答 + 后续 delta 的真 client/handler 平台出口计数。

不是解析器单测，也不是桩：本用例跑

* 真 ``_run_reactive_turn``（群/私 turn 的完整外壳，只有外部 I/O 换成既有 harness 同形替身）；
* 真 ``HermesClient.chat`` → 真 ``_chat_via_runs`` → 真 ``RunsEventParser`` → 真
  ``_RunsReplyQueue``（``_runs_reply_sleep`` 换成即时返回，只为不真等 3 秒节奏）；
* 真 ``RunTracker`` 台账、真 ``runs_terminal`` 计划与组件链；
* 唯一被替换的平台出口：``send_text_with_media``（计数 + 记录正文与 component_id）。

即"只替 HTTP 与平台发送出口"，其余全是真的。

被复现的帧序（reviewer_gpt 在 t_71aa7781 的复现同形）：

    delta(A) → queued-lane interim(A) → 下一轮工具前的 delta(P) → tool.started → completed(C)

期望：平台出口被调用 3 次，正文 ``[A, P, C]``，组件 ``primary:0/1/2``。
同一帧序喂给**已部署的**（stock）解析器，应得到 reviewer 实测的 ``[A, A+P, C]``——这条
对照证明缺口在解析器的段边界，而不是在插件发布的那一帧。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from tests.test_runs_stream import real_chat  # noqa: E402
from tests.test_runs_terminal import _ReactiveHarness  # noqa: E402


def _install_stock_parser_alias() -> type:
    """The deployed site-packages parser under its own package name (stock, pre-fix)."""
    import os
    import types

    name = "qlt_stock_nonebot_adapter"
    site = "/home/neonsky/NeonBot/.venv/lib/python3.13/site-packages/nonebot_plugin_hermes_adapter"
    if name not in sys.modules:
        pkg = types.ModuleType(name)
        pkg.__path__ = [site]
        sys.modules[name] = pkg
    from qlt_stock_nonebot_adapter.core.hermes_client import RunsEventParser

    return RunsEventParser


A, P, C = "答复A", "查B前的说明", "答复C"


def _frame(body: str) -> str:
    """分节符信封：一份正文 = 独立一行 § + 正文 + 独立一行 §（+ 收尾换行）。"""
    return f"§\n{body}\n§\n"


# 新协议帧序：queued-lane 注入的前序答复与 delta 是同文，必须只发一次。
FRAMES = [
    {"event": "message.delta", "delta": _frame(A), "seq": 0},
    {"event": "message.interim", "text": _frame(A), "seq": 1},
    {"event": "message.delta", "delta": _frame(P), "seq": 2},
    {"event": "tool.started", "tool": "lookup", "seq": 3},
    {"event": "run.completed", "output": f"§\n{C}\n§", "seq": 4},
]

# 对照用：同一时序的**旧协议**裸正文帧（部署中的 stock 解析器只认这个形状）。
LEGACY_FRAMES = [
    {"event": "message.delta", "delta": A, "seq": 0},
    {"event": "message.interim", "text": A, "seq": 1},
    {"event": "message.delta", "delta": P, "seq": 2},
    {"event": "tool.started", "tool": "lookup", "seq": 3},
    {"event": "run.completed", "output": C, "seq": 4},
]


class QueuedLaneInterimClientTest(_ReactiveHarness):
    """真客户端 + 真 processor + 真发送计划，只替平台出口。"""

    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config
        from nonebot_plugin_hermes_adapter.core import hermes_client as hc

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

        async def _instant(_delay):
            return None

        pacing = patch.object(hc, "_runs_reply_sleep", _instant)
        pacing.start()
        self.addCleanup(pacing.stop)

    async def _drive(self, frames, *, parser=None):
        """One real reactive turn; ``chat`` is the real client with only HTTP replaced."""
        import tempfile

        from nonebot_plugin_hermes_adapter.core import hermes_client as hc
        from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker

        # A bare ledger: the run row is created by the real client's own submit/register
        # (pre-registering the same run id would collide on run_submissions.run_id, which is
        # run_tracker's unique key — a harness artefact, not production behaviour).
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(tracker.close)
        chat = real_chat(tracker, frames, "run-e2-h")
        if parser is None:
            return await self._run(None, tracker=tracker, chat=chat)
        with patch.object(hc, "RunsEventParser", parser):
            return await self._run(None, tracker=tracker, chat=chat)

    def test_queued_interim_then_next_delta_sends_abc_each_once(self):
        import asyncio

        calls = asyncio.run(self._drive(FRAMES))
        self.assertEqual([c["text"] for c in calls["send"]], [A, P, C])
        self.assertEqual(len(calls["send"]), 3)                  # 平台出口真实调用次数
        self.assertEqual([c["component"] for c in calls["send"]],
                         ["primary:0", "primary:1", "primary:2"])
        self.assertEqual(calls["settle"], [])

    def test_deployed_parser_still_seals_at_interim(self):
        """部署中的 stock 解析器已带 R1 段边界修正：旧协议帧序仍不粘连成 A+P。

        注意这里只比前两份：stock 解析器吃的是旧协议裸正文，而终态 output 现在由
        新信封解析，所以 ``C`` 在**混装**状态下不会被当正文（这正是解析器与提示词
        必须同批部署的原因之一）。
        """
        import asyncio

        stock = _install_stock_parser_alias()
        calls = asyncio.run(self._drive(LEGACY_FRAMES, parser=stock))
        sent = [c["text"] for c in calls["send"]]
        self.assertEqual(sent[:2], [A, P])
        self.assertNotIn(A + P, sent)

    def test_plain_turn_without_interim_is_unchanged(self):
        import asyncio

        frames = [
            {"event": "message.delta", "delta": _frame(A), "seq": 0},
            {"event": "tool.started", "tool": "lookup", "seq": 1},
            {"event": "run.completed", "output": f"§\n{C}\n§", "seq": 2},
        ]
        calls = asyncio.run(self._drive(frames))
        self.assertEqual([c["text"] for c in calls["send"]], [A, C])


if __name__ == "__main__":
    unittest.main(verbosity=2)
