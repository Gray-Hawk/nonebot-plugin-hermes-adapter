"""poke_user：戳一戳只能独立使用，由 MCP 工具直接调 OneBot ``send_poke``。

定案（2026-11-18）：poke 必须且只能独立使用——模型正文里不再出现任何 poke 标记，
也没有混合正文动作路径。本文件锁定：

- 群聊 ``send_poke(group_id, user_id)``、私聊 ``send_poke(user_id)`` 的平台参数；
- 目标判定与群/私参数约定复用 ``validate_push_context``（群要有活跃窗口与路由，
  私聊目标要在主动下行白名单里、调用方身份可识别）；
- 自戳拒绝、非数字目标拒绝；
- 结果如实反映平台回执：平台拒绝/网络不明分别记 ``platform_rejected`` /
  ``unconfirmed``，拒绝时保留适配器 ``ActionFailed.info`` 里的真实 ``retcode``；
  成功时 OneBot 适配器只回 ``data``（``send_poke`` 为 ``null``），因此成功不带
  ``retcode``，也不伪造 ``0``；
- 全程不发送任何聊天消息。
"""

from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests import _bootstrap  # noqa: F401

import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
from nonebot.adapters.onebot.v11 import Bot as OneBotV11Bot
from nonebot.adapters.onebot.v11.exception import ActionFailed, NetworkError

from nonebot_plugin_hermes_adapter.core.routing import CallerScope
from nonebot_plugin_hermes_adapter.mcp.tools import poke_user as pu

ADAPTER = "onebotv11"
GROUP = "54321"
SELF = "999"


class _Bot:
    def __init__(self, *, self_id: str = SELF, error: Exception | None = None,
                 response=None) -> None:
        self.self_id = self_id
        self.calls: list[tuple[str, dict]] = []
        self.error = error
        # 真实适配器的 ``call_api`` 成功时返回回执里的 data（send_poke 为 None）。
        self.response = response

    async def call_api(self, api: str, **kwargs):
        self.calls.append((api, kwargs))
        if self.error is not None:
            raise self.error
        return self.response


class _Sessions:
    def __init__(self, alive: bool = True) -> None:
        self.alive = alive

    def is_alive_for_trigger(self, adapter: str, group_id: str, now_ms: int) -> bool:
        return self.alive


class _Registry:
    def __init__(self, bot: _Bot, target=None) -> None:
        self.bot = bot
        self.target = target or SimpleNamespace(private=False, id=GROUP)

    def get(self, adapter: str, scope: str, scope_id: str):
        if (adapter, scope, scope_id) != (ADAPTER, "group", GROUP):
            return None
        return SimpleNamespace(bot_self_id=self.bot.self_id, target=self.target)


def _run(inp: pu.PokeUserInput, *, bot, alive: bool = True,
         scope: CallerScope | None | str = "default", private_route=None):
    """跑一次 impl；只替换平台边界（bot / 私聊路由），台账与校验都是真的。"""
    caller = CallerScope.dev() if scope == "default" else scope
    patches = [patch.object(pu, "get_bot", new=lambda _sid: bot)]
    if private_route is not None:
        from nonebot_plugin_hermes_adapter.handlers import message_relay

        patches.append(
            patch.object(message_relay, "_resolve_wake_route", new=lambda *a, **k: private_route)
        )
    for item in patches:
        item.start()
    try:
        return asyncio.run(
            pu.poke_user_impl(
                inp,
                active_sessions=_Sessions(alive=alive),
                bot_registry=_Registry(bot),
                scope=caller,
            )
        )
    finally:
        for item in reversed(patches):
            item.stop()


class GroupPokeTests(unittest.TestCase):
    def test_group_poke_uses_public_api_with_group_and_user(self) -> None:
        bot = _Bot()
        result = _run(pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP), bot=bot)
        self.assertTrue(result.ok, result)
        self.assertEqual(bot.calls, [("send_poke", {"group_id": 54321, "user_id": 10001})])
        # 适配器成功只回 data，看不到成功码就不编一个。
        self.assertIsNone(result.retcode)
        self.assertIn("no chat message", result.note)

    def test_group_without_active_session_is_rejected(self) -> None:
        bot = _Bot()
        result = _run(
            pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP),
            bot=bot, alive=False,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertEqual(bot.calls, [])

    def test_unknown_group_route_is_rejected(self) -> None:
        bot = _Bot()
        result = _run(
            pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id="77777"), bot=bot
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertEqual(bot.calls, [])

    def test_unresolved_caller_is_rejected(self) -> None:
        bot = _Bot()
        result = _run(
            pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP),
            bot=bot, scope=None,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertEqual(bot.calls, [])


class SelfAndArgumentGuardTests(unittest.TestCase):
    def test_self_poke_is_refused(self) -> None:
        for target in (SELF, "000999"):
            with self.subTest(target=target):
                bot = _Bot()
                result = _run(
                    pu.PokeUserInput(adapter=ADAPTER, user_id=target, group_id=GROUP), bot=bot
                )
                self.assertFalse(result.ok)
                self.assertEqual(result.error_kind, "local_rejected")
                self.assertIn("itself", result.error or "")
                self.assertEqual(bot.calls, [])

    def test_non_digit_target_is_refused(self) -> None:
        for target in ("abc", "10001x", "@10001", "١٠٠٠١"):
            with self.subTest(target=target):
                with self.assertRaises(ValueError):
                    pu.PokeUserInput(adapter=ADAPTER, user_id=target, group_id=GROUP)


class PrivatePokeTests(unittest.TestCase):
    def _route(self, bot: _Bot):
        return bot, SimpleNamespace(private=True, id="10001")

    def test_private_poke_uses_user_id_only(self) -> None:
        bot = _Bot()
        with patch("nonebot_plugin_hermes_adapter.core.user_lists.private_allowed",
                   return_value=True):
            result = _run(
                pu.PokeUserInput(adapter=ADAPTER, user_id="10001"), bot=bot,
                private_route=self._route(bot),
            )
        self.assertTrue(result.ok, result)
        self.assertEqual(bot.calls, [("send_poke", {"user_id": 10001})])

    def test_private_target_outside_allowlist_is_rejected(self) -> None:
        bot = _Bot()
        with patch("nonebot_plugin_hermes_adapter.core.user_lists.private_allowed",
                   return_value=False):
            result = _run(
                pu.PokeUserInput(adapter=ADAPTER, user_id="10001"), bot=bot,
                private_route=self._route(bot),
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertEqual(bot.calls, [])

    def test_private_route_failure_is_reported(self) -> None:
        bot = _Bot()
        with patch("nonebot_plugin_hermes_adapter.core.user_lists.private_allowed",
                   return_value=True):
            result = _run(
                pu.PokeUserInput(adapter=ADAPTER, user_id="10001"), bot=bot,
                private_route="bot registry has no entry for this private chat",
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "local_rejected")
        self.assertEqual(bot.calls, [])


class ReceiptTests(unittest.TestCase):
    def test_platform_refusal_is_reported_as_platform_rejected(self) -> None:
        bot = _Bot(error=ActionFailed())
        result = _run(pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP), bot=bot)
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "platform_rejected")

    def test_network_uncertainty_is_not_auto_retried(self) -> None:
        bot = _Bot(error=NetworkError("boom"))
        result = _run(pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP), bot=bot)
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "unconfirmed")
        self.assertEqual(len(bot.calls), 1)


class _LineSocket:
    """真实 Adapter 的 WebSocket I/O 替身：记录发出的帧，把线形回执塞回 ResultStore。"""

    def __init__(self, store, reply: dict) -> None:
        self._store = store
        self.reply = reply
        self.frames: list[dict] = []

    async def send(self, payload: str) -> None:
        frame = json.loads(payload)
        self.frames.append(frame)
        asyncio.get_running_loop().call_soon(
            self._store.add_result, {**self.reply, "echo": frame["echo"]}
        )


class _StubDriver:
    """只满足 Adapter 的 driver 契约（全局配置 + shutdown 钩子），不带网络能力。"""

    def __init__(self) -> None:
        self.config = nonebot.get_driver().config
        self.shutdown = None

    def on_shutdown(self, func) -> None:
        self.shutdown = func


def _real_bot(reply: dict) -> tuple[OneBotV11Bot, _LineSocket]:
    adapter = OneBotV11Adapter(_StubDriver())
    socket = _LineSocket(adapter._result_store, reply)
    adapter.connections[SELF] = socket
    return OneBotV11Bot(adapter, SELF), socket


class RealAdapterReceiptTests(unittest.TestCase):
    """真实 ``Bot.call_api`` → ``Adapter._call_api`` 线形回执（只替换 WS I/O）。"""

    def test_success_line_does_not_invent_a_retcode(self) -> None:
        bot, socket = _real_bot({"status": "ok", "retcode": 0, "data": None})
        result = _run(
            pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP), bot=bot
        )
        self.assertTrue(result.ok, result)
        # 适配器成功时只回 data；不把看不到的成功码伪造成 0。
        self.assertIsNone(result.retcode)
        self.assertEqual(len(socket.frames), 1)
        self.assertEqual(socket.frames[0]["action"], "send_poke")
        self.assertEqual(socket.frames[0]["params"], {"group_id": 54321, "user_id": 10001})

    def test_rejection_line_keeps_the_platform_retcode(self) -> None:
        bot, socket = _real_bot(
            {"status": "failed", "retcode": 1400, "data": None, "wording": "forbidden"}
        )
        result = _run(
            pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP), bot=bot
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_kind, "platform_rejected")
        self.assertEqual(result.retcode, 1400)
        self.assertNotIn("ActionFailed", result.error or "")
        self.assertEqual(len(socket.frames), 1)  # 拒绝不重试


if __name__ == "__main__":
    unittest.main()
