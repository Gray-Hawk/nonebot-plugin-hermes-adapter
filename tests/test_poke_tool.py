"""poke_user：戳一戳只能独立使用，由 MCP 工具直接调 OneBot ``send_poke``。

定案（2026-11-18）：poke 必须且只能独立使用——模型正文里不再出现任何 poke 标记，
也没有混合正文动作路径。本文件锁定：

- 群聊 ``send_poke(group_id, user_id)``、私聊 ``send_poke(user_id)`` 的平台参数；
- 目标判定与群/私参数约定复用 ``validate_push_context``（群要有活跃窗口与路由，
  私聊目标要在主动下行白名单里、调用方身份可识别）；
- 自戳拒绝、非数字目标拒绝；
- 结果如实反映平台回执：平台拒绝/网络不明分别记 ``platform_rejected`` /
  ``unconfirmed``，成功时带回 ``retcode``；
- 全程不发送任何聊天消息。
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests import _bootstrap  # noqa: F401

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
        self.response = {"retcode": 0} if response is None else response

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


def _run(inp: pu.PokeUserInput, *, bot: _Bot, alive: bool = True,
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
        self.assertEqual(result.retcode, 0)
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

    def test_platform_retcode_is_passed_through(self) -> None:
        bot = _Bot(response={"retcode": 7, "message": "nope"})
        result = _run(pu.PokeUserInput(adapter=ADAPTER, user_id="10001", group_id=GROUP), bot=bot)
        self.assertTrue(result.ok)
        self.assertEqual(result.retcode, 7)


if __name__ == "__main__":
    unittest.main()
