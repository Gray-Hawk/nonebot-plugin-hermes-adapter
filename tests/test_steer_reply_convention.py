"""F2 回归：steer 注入提示词与 Runs 轮内 push 闸门一致（真实入口）。

走真实注入入口（`_maybe_steer_direct` / `_try_steer_followup`），只在平台/HTTP
边界截获最终注入文本，因此断言的是「模型实际看到的那段话」。矩阵覆盖
接口(runs/completions) × 旁白开关(开/关) × 群/私。

契约：
  - runs 且旁白关（`steer_reply_push_active()` 成立）：追加的约定行指向当前
    两行 JSON 决策的 body 行——因为此刻 push_message 正被闸门硬拒；
  - 其余组合（runs+旁白开、completions 全部）：不追加约定行，原文不动。
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

from nonebot_plugin_hermes_adapter import config as plugin_config_module  # noqa: E402
from nonebot_plugin_hermes_adapter.core import misc_config as misc_module  # noqa: E402
from nonebot_plugin_hermes_adapter.core import steer_reconcile  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import (  # noqa: E402
    RunTracker,
    make_scope_key,
)
from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402

RAWS = {
    "group": "[id=u1 user=小光]: 你怎么看这个",
    "private": "[id=u1 user=小光]: 你怎么看这个",
}
GROUP_ID = "g1"
USER_ID = "u1"


class SteerReplyConventionTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        for is_private in (False, True):
            scope = make_scope_key(
                "onebotv11", is_private, USER_ID, None if is_private else GROUP_ID
            )
            self.tracker.upsert(
                run_id=f"run-{'p' if is_private else 'g'}", scope_key=scope,
                adapter="onebotv11", is_private=is_private, user_id=USER_ID,
                group_id=None if is_private else GROUP_ID, bot_self_id="999",
                session_key="s", mode="reactive", expect_structured=True,
            )

    def _inject_direct(self, *, interface: str, interim_echo: bool, is_private: bool) -> str:
        """真实 `_maybe_steer_direct` 入口，返回实际注入模型的文本。"""
        captured: list[str] = []

        async def fake_steer_run(*, tgt, run_id, text):
            captured.append(text)
            return True

        runtime = SimpleNamespace(
            run_tracker=self.tracker,
            active_sessions=MagicMock(pinned_participant_ids=MagicMock(return_value=set())),
            inflight=MagicMock(),
        )
        with patch.object(
            plugin_config_module.plugin_config, "hermes_api_interface", interface
        ), patch.object(
            misc_module, "load_misc_config",
            lambda: SimpleNamespace(interim_echo_enabled=interim_echo),
        ), patch.object(relay, "_mcp", runtime), patch.object(
            relay, "hermes_client", SimpleNamespace(steer_run=fake_steer_run)
        ), patch.object(relay, "_peek_event_db_id", lambda *a, **k: None), patch.object(
            relay, "load_chat_config", lambda: SimpleNamespace(anchor_max_age_sec=600)
        ), patch.object(relay, "resolve_target", lambda *a, **k: object()):
            import asyncio

            ok = asyncio.run(
                relay._maybe_steer_direct(
                    target=SimpleNamespace(private=is_private, id=GROUP_ID),
                    adapter_name="onebotv11",
                    user_id=USER_ID,
                    nickname="小光",
                    group_id=None if is_private else GROUP_ID,
                    msg_text="你怎么看这个",
                    is_direct=True,
                    now_ms=1_700_000_000_000,
                    origin=MessageOrigin.human(),
                )
            )
        self.assertTrue(ok, "steer 注入入口应当在资格内成功")
        self.assertEqual(len(captured), 1)
        return captured[0]

    def _inject_followup(self, *, interface: str, interim_echo: bool) -> str:
        """真实 `_try_steer_followup` 入口（群聊跟帖路径），返回注入文本。"""
        import asyncio

        from nonebot_plugin_hermes_adapter.core.inflight import make_inflight_key
        from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage

        captured: list[str] = []

        async def fake_steer_run(*, tgt, run_id, text):
            captured.append(text)
            return True

        # 真实 BufferedMessage:P-19 的媒体门按 iter_message_media 逐行投影,
        # 四类媒体字段必须真实存在,不能用 SimpleNamespace 冒充消息。
        entry = SimpleNamespace(
            msg=BufferedMessage(
                ts=1, adapter="onebotv11", group_id=GROUP_ID, user_id=USER_ID,
                nickname="小光", content="你怎么看这个", origin=MessageOrigin.human(),
            ),
            fragments=[],
        )
        runtime = SimpleNamespace(
            run_tracker=self.tracker,
            active_sessions=MagicMock(),
            inflight=MagicMock(
                peek_followup=MagicMock(return_value=entry), settle_followup=MagicMock()
            ),
        )
        key = make_inflight_key("onebotv11", False, USER_ID, GROUP_ID)
        with patch.object(
            plugin_config_module.plugin_config, "hermes_api_interface", interface
        ), patch.object(
            misc_module, "load_misc_config",
            lambda: SimpleNamespace(interim_echo_enabled=interim_echo),
        ), patch.object(relay, "_mcp", runtime), patch.object(
            relay, "hermes_client", SimpleNamespace(steer_run=fake_steer_run)
        ), patch.object(relay, "resolve_target", lambda *a, **k: object()):
            ok = asyncio.run(relay._try_steer_followup(key, "onebotv11", USER_ID, GROUP_ID))
        self.assertTrue(ok, "跟帖 steer 入口应当在资格内成功")
        self.assertEqual(len(captured), 1)
        return captured[0]

    # --- runs：约定行指向决策 body，不指向 push_message ---------------------

    def test_runs_without_interim_echo_guides_decision_body(self) -> None:
        for is_private in (False, True):
            with self.subTest(scope="private" if is_private else "group"):
                text = self._inject_direct(
                    interface="runs", interim_echo=False, is_private=is_private
                )
                self.assertTrue(text.startswith(RAWS["private"]))
                self.assertTrue(text.endswith(relay._STEER_REPLY_CONVENTION_LINE))
                self.assertNotIn("push_message", text)
                self.assertIn("两行 JSON", text)
                self.assertIn("body", text)

    def test_runs_without_interim_echo_followup_guides_decision_body(self) -> None:
        text = self._inject_followup(interface="runs", interim_echo=False)
        self.assertTrue(text.endswith(relay._STEER_REPLY_CONVENTION_LINE))
        self.assertNotIn("push_message", text)
        self.assertIn("两行 JSON", text)

    def test_runs_convention_line_is_stripped_on_replay(self) -> None:
        """补投是正常消息：台账里存的注入文本要能剥掉新约定行。"""
        text = self._inject_direct(interface="runs", interim_echo=False, is_private=False)
        self.assertEqual(steer_reconcile.strip_steer_presentation(text), "你怎么看这个")

    def test_runs_with_interim_echo_keeps_steer_text_verbatim(self) -> None:
        for is_private in (False, True):
            with self.subTest(scope="private" if is_private else "group"):
                text = self._inject_direct(
                    interface="runs", interim_echo=True, is_private=is_private
                )
                self.assertEqual(text, RAWS["private"])
                self.assertNotIn("push_message", text)

    # --- completions：原语义不变，不追加任何约定行 -------------------------

    def test_completions_keeps_steer_text_verbatim(self) -> None:
        for interim_echo in (False, True):
            for is_private in (False, True):
                with self.subTest(
                    echo=interim_echo, scope="private" if is_private else "group"
                ):
                    text = self._inject_direct(
                        interface="completions",
                        interim_echo=interim_echo,
                        is_private=is_private,
                    )
                    self.assertEqual(text, RAWS["private"])
                    self.assertNotIn("push_message", text)

    def test_completions_followup_keeps_steer_text_verbatim(self) -> None:
        for interim_echo in (False, True):
            with self.subTest(echo=interim_echo):
                text = self._inject_followup(
                    interface="completions", interim_echo=interim_echo
                )
                self.assertEqual(text, RAWS["private"])
                self.assertNotIn("push_message", text)


if __name__ == "__main__":
    unittest.main()
