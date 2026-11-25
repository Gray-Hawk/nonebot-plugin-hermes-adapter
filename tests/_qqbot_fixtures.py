"""QQBotAPI 结构级测试夹具：真 OneBot v11 / Milky 适配器类，只替换线形 I/O。

约定（与 ``tests/test_poke_tool.py`` 同一口径）：
- ``Bot`` / ``Adapter`` / 事件模型全部用**真实**类型，字段与 API 签名由适配器自己校验；
- 只在「平台线形收发」这一层做替身：OneBot 用只记录帧的 WebSocket，Milky 用只返回
  预设 JSON 回执的 ``Adapter.request``；
- fake 不接管业务判断，也不放宽任何字段，字段写错会由 pydantic / 适配器直接报错。
"""

from __future__ import annotations

import asyncio
import json

from tests import _bootstrap  # noqa: F401

import nonebot
from nonebot.adapters.milky import Adapter as MilkyAdapter
from nonebot.adapters.milky import Bot as MilkyBot
from nonebot.adapters.milky.config import ClientInfo
from nonebot.adapters.milky.event import (
    FriendMessageEvent,
    FriendNudgeEvent,
    GroupMessageEvent,
    GroupNudgeEvent,
)
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
from nonebot.adapters.onebot.v11 import Bot as OneBotV11Bot
from nonebot.adapters.onebot.v11 import PokeNotifyEvent
from nonebot.compat import type_validate_python

MILKY_SELF = "3889000001"
ONEBOT_SELF = "999"
GROUP = "462540549"


class _StubDriver:
    """只满足 Adapter 的 driver 契约（全局配置 + shutdown 钩子），不带网络能力。"""

    def __init__(self) -> None:
        self.config = nonebot.get_driver().config
        self.shutdown = None

    def on_shutdown(self, func) -> None:
        self.shutdown = func


# ---------------------------------------------------------------- Milky 平台


class _HttpResponse:
    def __init__(self, payload: dict) -> None:
        self.status_code = 200
        self.content = json.dumps(payload).encode()
        self.headers: dict[str, str] = {}


class MilkyHarness:
    """真 Milky ``Adapter`` / ``Bot``；``request`` 只回放预设的协议端 JSON。

    ``replies`` 是 action → 协议端 ``data`` 段；``failures`` 是 action → 完整回执
    （``status=failed`` / 非零 ``retcode``），交给适配器自己的 ``handle_api_result``
    抛 ``ActionFailed``，从而保住「拒绝/网络不明」的真实异常类型。
    """

    def __init__(self, *, self_id: str = MILKY_SELF) -> None:
        self.adapter = MilkyAdapter(_StubDriver())
        self.bot = MilkyBot(self.adapter, self_id, ClientInfo())
        self.self_id = self_id
        self.frames: list[dict] = []
        self.replies: dict[str, dict] = {}
        self.failures: dict[str, dict] = {}
        self.network_errors: dict[str, Exception] = {}
        self.adapter.request = self._request  # type: ignore[method-assign]

    async def _request(self, req):
        action = str(req.url).rsplit("/", 1)[-1]
        params = json.loads(req.content or b"{}")
        self.frames.append({"action": action, "params": params})
        if action in self.network_errors:
            raise self.network_errors[action]
        if action in self.failures:
            return _HttpResponse(self.failures[action])
        return _HttpResponse(
            {"status": "ok", "retcode": 0, "data": self.replies.get(action, {})}
        )

    def actions(self) -> list[str]:
        return [f["action"] for f in self.frames]

    def params_for(self, action: str) -> list[dict]:
        return [f["params"] for f in self.frames if f["action"] == action]


def milky_group_message(
    *,
    segments: list[dict],
    self_id: str = MILKY_SELF,
    sender_id: int = 10001,
    group_id: str = GROUP,
    message_seq: int = 23097,
    card: str = "名片A",
    nickname: str = "昵称A",
) -> GroupMessageEvent:
    payload = {
        "time": 1795538642,
        "self_id": int(self_id),
        "data": {
            "message_scene": "group",
            "peer_id": int(group_id),
            "message_seq": message_seq,
            "sender_id": sender_id,
            "time": 1795538642,
            "segments": segments,
            "group": {
                "group_id": int(group_id),
                "group_name": "测试群",
                "member_count": 3,
                "max_member_count": 200,
            },
            "group_member": {
                "user_id": sender_id,
                "nickname": nickname,
                "card": card,
                "sex": "unknown",
                "group_id": int(group_id),
                "title": "",
                "level": 1,
                "role": "member",
                "join_time": 1,
                "last_sent_time": 2,
            },
        },
    }
    return type_validate_python(GroupMessageEvent, payload)


def milky_friend_message(
    *,
    segments: list[dict],
    self_id: str = MILKY_SELF,
    sender_id: int = 10001,
    nickname: str = "昵称B",
    message_seq: int = 30001,
) -> FriendMessageEvent:
    payload = {
        "time": 1795538642,
        "self_id": int(self_id),
        "data": {
            "message_scene": "friend",
            "peer_id": sender_id,
            "message_seq": message_seq,
            "sender_id": sender_id,
            "time": 1795538642,
            "segments": segments,
            "friend": {
                "user_id": sender_id,
                "nickname": nickname,
                "sex": "unknown",
                "qid": "",
                "remark": "",
                "category": {"category_id": 0, "category_name": "默认"},
            },
        },
    }
    return type_validate_python(FriendMessageEvent, payload)


def mention(user_id: int) -> dict:
    return {"type": "mention", "data": {"user_id": user_id}}


def text(value: str) -> dict:
    return {"type": "text", "data": {"text": value}}


def milky_group_nudge(
    *,
    group_id: str = GROUP,
    sender_id: int = 10001,
    receiver_id: int = int(MILKY_SELF),
    self_id: str = MILKY_SELF,
) -> GroupNudgeEvent:
    return type_validate_python(
        GroupNudgeEvent,
        {
            "time": 1795538642,
            "self_id": int(self_id),
            "data": {
                "group_id": int(group_id),
                "sender_id": sender_id,
                "receiver_id": receiver_id,
                "display_action": "戳了戳",
                "display_suffix": "",
                "display_action_img_url": "",
            },
        },
    )


def milky_friend_nudge(
    *,
    user_id: int = 10001,
    is_self_send: bool = False,
    is_self_receive: bool = True,
    self_id: str = MILKY_SELF,
) -> FriendNudgeEvent:
    return type_validate_python(
        FriendNudgeEvent,
        {
            "time": 1795538642,
            "self_id": int(self_id),
            "data": {
                "user_id": user_id,
                "is_self_send": is_self_send,
                "is_self_receive": is_self_receive,
                "display_action": "戳了戳",
                "display_suffix": "",
                "display_action_img_url": "",
            },
        },
    )


# ---------------------------------------------------------------- OneBot 平台


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


def onebot_bot(
    reply: dict | None = None, *, self_id: str = ONEBOT_SELF
) -> tuple[OneBotV11Bot, _LineSocket]:
    adapter = OneBotV11Adapter(_StubDriver())
    socket = _LineSocket(
        adapter._result_store, reply or {"status": "ok", "retcode": 0, "data": None}
    )
    adapter.connections[self_id] = socket
    return OneBotV11Bot(adapter, self_id), socket


def onebot_poke_notice(
    *,
    user_id: int = 10001,
    target_id: int = int(ONEBOT_SELF),
    group_id: int | None = int(GROUP),
    self_id: str = ONEBOT_SELF,
) -> PokeNotifyEvent:
    return PokeNotifyEvent(
        time=1795538642,
        self_id=int(self_id),
        post_type="notice",
        notice_type="notify",
        sub_type="poke",
        user_id=user_id,
        target_id=target_id,
        group_id=group_id,
    )
