"""通知事件处理器

priority=1 dispatch: 适配器特定 notice 事件 → 合成伪消息 → 复用 message.py 路由。

**CLAUDE.md 规则例外**: 本文件是仅有的允许 import 适配器特定类型的位置。
- 仅在 dispatch 函数内部 try-import (惰性)
- 失败 (ImportError) 即 no-op,不影响其他平台

当前支持两种 QQ 协议：

* OneBot v11：``PokeNotifyEvent``（戳一戳）与 ``GroupIncreaseNoticeEvent``（入群）
* Milky：``FriendNudgeEvent`` / ``GroupNudgeEvent``（戳一戳）——同样合成既有的
  ``[poke] … 戳了你一下`` 语义，复用「谁戳谁」的既有处理，不另造回复流程。
"""

from __future__ import annotations

import time

import nonebot_plugin_alconna as alconna
from nonebot import logger, on_notice
from nonebot.adapters import Bot, Event

from ..core import qqtpc_api
from ..core.chat_config import load_chat_config
from ..core.message_origin import MessageSource
from ..utils import check_isolation, get_adapter_name
from .message import route_synthesized_input

notice_handler = on_notice(priority=1, block=False)


def _now_ms() -> int:
    return int(time.time() * 1000)


@notice_handler.handle()
async def dispatch(bot: Bot, event: Event):
    """notice 事件分发入口。

    早期 return:
      - adapter 既不是 Milky 也不是 OneBot V11
      - ImportError (对应 adapter 未装)

    戳一戳固定开启(语义等同 @ 的隐式激活),入群迎新仍受 greet_on_join 门控。
    """
    adapter_name = get_adapter_name(bot)
    if adapter_name == qqtpc_api.MILKY:
        await _dispatch_milky(bot, event)
        return
    if adapter_name != qqtpc_api.ONEBOT:
        return

    greet_on_join = load_chat_config().greet_on_join

    try:
        from nonebot.adapters.onebot.v11 import (
            GroupIncreaseNoticeEvent,
            PokeNotifyEvent,
        )
    except ImportError:
        logger.debug("[HERMES notice] OneBot v11 adapter not installed; skip")
        return

    if isinstance(event, PokeNotifyEvent):
        # 只处理「别人戳 bot」：target 不是 bot 本体的静默丢弃。
        if str(event.target_id) != str(bot.self_id):
            return
        await _handle_poke(
            bot,
            event,
            adapter_name=qqtpc_api.ONEBOT,
            user_id=str(event.user_id),
            group_id=str(event.group_id) if event.group_id else None,
        )
    elif isinstance(event, GroupIncreaseNoticeEvent) and greet_on_join:
        await _handle_member_join(bot, event)


async def _dispatch_milky(bot: Bot, event: Event) -> None:
    """Milky notice 分发：只接戳一戳（入群事件语义与 OneBot 不同,本轮不扩）。

    Milky 的 nudge 语义（``nonebot/adapters/milky/event.py``）：

    * ``FriendNudgeEvent``：``is_self_receive`` = 好友戳了 bot；``is_self_send`` =
      bot 自己戳好友（自己动作的回显），后者不得当成「有人戳 bot」。
    * ``GroupNudgeEvent``：``sender_id`` 戳 ``receiver_id``；只有
      ``receiver_id == bot.self_id`` 且 ``sender_id != bot.self_id`` 才算戳 bot。
    """
    try:
        from nonebot.adapters.milky.event import FriendNudgeEvent, GroupNudgeEvent
    except ImportError:
        logger.debug("[HERMES notice] Milky adapter not installed; skip")
        return

    if isinstance(event, FriendNudgeEvent):
        if not event.data.is_self_receive or event.data.is_self_send:
            return
        await _handle_poke(
            bot,
            event,
            adapter_name=qqtpc_api.MILKY,
            user_id=str(event.data.user_id),
            group_id=None,
        )
    elif isinstance(event, GroupNudgeEvent):
        if str(event.data.receiver_id) != str(bot.self_id):
            return
        await _handle_poke(
            bot,
            event,
            adapter_name=qqtpc_api.MILKY,
            user_id=str(event.data.sender_id),
            group_id=str(event.data.group_id),
        )


async def _handle_poke(
    bot: Bot, event: Event, *, adapter_name: str, user_id: str, group_id: str | None
) -> None:
    """戳一戳: 仅戳 bot 自己时触发,合成 `[poke] 戳了你一下`。"""
    # 两种协议的 id 都是 int,统一字符串后比较
    if str(user_id) == str(bot.self_id):
        return  # bot 戳自己(罕见),跳过

    target = _build_target(adapter_name=adapter_name, user_id=user_id, group_id=group_id)
    if not check_isolation(event, target):
        return

    nickname = await _resolve_nickname(
        bot, adapter_name=adapter_name, user_id=user_id, group_id=group_id
    )

    await route_synthesized_input(
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        group_id=group_id,
        nickname=nickname,
        text=f"[poke] {nickname}({user_id}) 戳了你一下",
        source=MessageSource.NOTICE_POKE,
        addressed_to_bot=True,  # 只在戳 bot 本体时走到这,平台层面就是指向 bot 的事件
        allow_passive=True,
        now_ms=_now_ms(),
    )


async def _handle_member_join(bot: Bot, event) -> None:
    """入群: 合成 `[event=member_join] {nickname} 加入了群`。
    active_session 关时由 route_synthesized_input 自检并跳过。"""
    if str(event.user_id) == str(bot.self_id):
        return  # bot 自己被拉进新群,buffer 空,跳过

    user_id = str(event.user_id)
    group_id = str(event.group_id)

    target = _build_target(adapter_name=qqtpc_api.ONEBOT, user_id=user_id, group_id=group_id)
    if not check_isolation(event, target):
        return

    nickname = await _resolve_nickname(
        bot, adapter_name=qqtpc_api.ONEBOT, user_id=user_id, group_id=group_id
    )

    await route_synthesized_input(
        bot=bot,
        target=target,
        adapter_name=qqtpc_api.ONEBOT,
        user_id=user_id,
        group_id=group_id,
        nickname=nickname,
        text=f"[event=member_join] {nickname}({user_id}) 加入了群",
        source=MessageSource.NOTICE_JOIN,
        addressed_to_bot=False,  # 群级事件,不是「冲 bot 说的」;要不要迎新由模型自判
        allow_passive=False,
        now_ms=_now_ms(),
    )


# 昵称解析短 TTL 缓存(本部署补丁):戳一戳/入群事件此前每次都实时打 API
# (no_cache=True),刷屏/入群潮时调用放大。TTL 内同一 (adapter, 群, 用户) 复用结果;
# 失败回退不落缓存,下次事件自然重试。
_NICKNAME_TTL_S = 300.0
_NICKNAME_CACHE_MAX = 512
_nickname_cache: dict[tuple[str, str, str], tuple[str, float]] = {}


async def _resolve_nickname(
    bot: Bot, *, adapter_name: str, user_id: str, group_id: str | None
):
    """按协议取群名片/昵称(见 ``core/qqtpc_api.resolve_display_name``),失败回退 user_id。"""
    key = (adapter_name, str(group_id) if group_id else "", user_id)
    now = time.monotonic()
    cached = _nickname_cache.get(key)
    if cached is not None and cached[1] > now:
        return cached[0]
    try:
        nickname = await qqtpc_api.resolve_display_name(
            bot, adapter_name=adapter_name, user_id=user_id, group_id=group_id
        )
    except Exception as e:
        logger.debug(f"[HERMES notice] nickname resolve failed for {user_id}: {e}")
        return user_id
    if not nickname:
        return user_id
    if len(_nickname_cache) >= _NICKNAME_CACHE_MAX:
        for k in [k for k, (_n, exp) in _nickname_cache.items() if exp <= now]:
            _nickname_cache.pop(k, None)
        if len(_nickname_cache) >= _NICKNAME_CACHE_MAX:
            for k in list(_nickname_cache)[: _NICKNAME_CACHE_MAX // 4]:
                _nickname_cache.pop(k, None)
    _nickname_cache[key] = (nickname, now + _NICKNAME_TTL_S)
    return nickname


def _build_target(*, adapter_name: str, user_id: str, group_id: str | None):
    """构造 alconna.Target——notice handler 不在 alconna 消息上下文里,要自己造。"""
    if group_id:
        return alconna.Target(id=group_id, private=False, adapter=adapter_name)
    return alconna.Target(id=user_id, private=True, adapter=adapter_name)
