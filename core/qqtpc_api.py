"""QQ Third-Party Client（QQTPC）薄兼容层：OneBot v11 与 Milky 的差异只在这里收敛。

本次只收敛五项业务真正需要的能力（不重造机器人框架）：

1. **发送回执消息 ID**——OneBot 回 ``message_id``，Milky 回
   ``MessageResponse(message_seq, time)``，QQ 官方模型回 ``id``；对外一律是
   **干净字符串**。OneBot／Milky 的十进制 ID 在协议边界转 ``int``，QQ 官方的
   非数值 ID 原样保留，消息库仍以字符串保存外部 ID。
2. **入站 @**——OneBot 用 ``type=at`` / ``data.qq``，Milky 用 ``type=mention`` /
   ``data.user_id``；两种形状都要认，才能兜住「适配器把首/尾 @bot 从正文剥走」。
3. **发送者资料**——OneBot 在 ``event.sender``，Milky 在 ``event.data.sender``；
   群聊优先有效群名片、其次昵称，空值由调用方回退 QQ 号。
4. **戳一戳**——OneBot 走 ``send_poke``，Milky 走
   ``send_friend_nudge(user_id, is_self=False)`` / ``send_group_nudge(group_id, user_id)``。
5. **撤回**——OneBot 走 ``delete_msg(message_id)``，Milky 走
   ``recall_private_message(user_id, message_seq)`` /
   ``recall_group_message(group_id, message_seq)``（参数以现场安装的适配器源码为准）。

职责边界（刻意不越界）：

* ``BotRegistry`` 仍只负责路由，``Alconna`` 仍负责结构化 @/reply 段转换，
  消息数据库仍以字符串保存外部 ID——本层不碰路由表、不碰段转换、不碰存储。
* 只实现 OneBot v11 与 Milky 两种协议；其它 adapter 标签沿用原有 OneBot 形态调用
  （与迁移前逐字一致），不为 icqq 等预建空实现。
"""

from __future__ import annotations

import re

from ..adapter_norm import normalize_adapter_name

ONEBOT = "onebotv11"
MILKY = "milky"


class UnsupportedOperation(RuntimeError):
    """当前协议或平台对象不提供该能力。

    调用方据此按「本地拒绝」返回，绝不能把没发生的事说成已执行。
    """


def protocol(adapter_name: str | None) -> str:
    """把 adapter 名归一成协议标签；只认本次实现的两种协议，其余返回 ""。"""

    tag = normalize_adapter_name(adapter_name or "")
    return tag if tag in (ONEBOT, MILKY) else ""


# --------------------------------------------------------------- 平台异常面
# 业务模块只从这里取异常类型，不再各自 import 适配器：Milky 与 OneBot 的
# ActionFailed 都携带线形回执 ``info``（数值 ``retcode`` 在同一个键上）。


def _collect_errors(module: str) -> tuple[type[BaseException], ...]:
    try:
        imported = __import__(f"{module}.exception", fromlist=["ActionFailed", "NetworkError"])
    except Exception:  # noqa: BLE001 - 适配器未安装时整面缺席，不等于实现错误
        return ()
    names = ("ActionFailed", "NetworkError")
    found = tuple(getattr(imported, name) for name in names if hasattr(imported, name))
    return found


_PLATFORM_ERRORS = _collect_errors("nonebot.adapters.onebot.v11") + _collect_errors(
    "nonebot.adapters.milky"
)

REJECTION_ERRORS: tuple[type[BaseException], ...] = tuple(
    exc for exc in _PLATFORM_ERRORS if exc.__name__ == "ActionFailed"
)
NETWORK_ERRORS: tuple[type[BaseException], ...] = tuple(
    exc for exc in _PLATFORM_ERRORS if exc.__name__ == "NetworkError"
) + (TimeoutError,)


def rejected_retcode(exc: BaseException) -> int | None:
    """适配器拒绝时把线形回执里的数值 ``retcode`` 原样带回（没有就 None）。"""

    info = getattr(exc, "info", None)
    if isinstance(info, dict):
        raw = info.get("retcode")
        if isinstance(raw, int) and not isinstance(raw, bool):
            return raw
    return None


# --------------------------------------------------------------- P0-1 回执 ID

# 平台回执里装消息 ID 的常见键。``message_seq`` 是 Milky 的
# ``MessageResponse``（以及它的 dict 形态）唯一的 ID 字段。
_RECEIPT_ID_KEYS = ("message_id", "msg_id", "messageId", "id", "message_seq")


def extract_message_id(item: object) -> str | None:
    """从回执条目里取出干净的外部消息 ID 字符串。

    OneBot v11 的 ``Receipt.msg_ids`` 装的是 ``call_api`` 的**原始返回 dict**
    （``{"message_id": 12345, ...}``）；Milky 装的是 ``MessageResponse`` 对象，它只有
    ``message_seq``——此前退回 ``str(obj)`` 会得到 ``"message_seq=… time=…"`` 脏串，
    引用 bot 自己消息与撤回映射全部失效。这里按平台键取 ID，再退到裸标量形态。
    """

    if item is None:
        return None
    if isinstance(item, dict):
        for key in _RECEIPT_ID_KEYS:
            value = item.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None
    for attr in _RECEIPT_ID_KEYS:
        value = getattr(item, attr, None)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    if isinstance(item, (str, int, bytes)):
        text = str(item).strip()
        return text or None
    # 认不出的对象形态：宁可没有 ID（明确事实），也不写一个假的脏串。
    return None


def receipt_msg_ids(receipt: object) -> list[str]:
    """把 alconna ``Receipt.msg_ids`` 归一化成 list[str]（真 ID、剔除空值）。"""

    raw = getattr(receipt, "msg_ids", None)
    if not isinstance(raw, (list, tuple)):
        return []
    ids: list[str] = []
    for item in raw:
        msg_id = extract_message_id(item)
        if msg_id:
            ids.append(msg_id)
    return ids


# --------------------------------------------------------------- P0-2 入站 @

# 各协议的 @ 段形状：段类型 → 承载目标 QQ 号的 data 键（按优先级）。
_AT_SEGMENT_KEYS: dict[str, tuple[str, ...]] = {
    "at": ("qq", "user_id"),        # OneBot v11
    "mention": ("user_id", "qq"),   # Milky
}


def at_target_of(segment: object) -> str | None:
    """@ 段 → 目标 QQ 号字符串；非 @ 段或取不到目标返回 None。"""

    seg_type = getattr(segment, "type", None)
    keys = _AT_SEGMENT_KEYS.get(seg_type) if isinstance(seg_type, str) else None
    if keys is None:
        return None
    data = getattr(segment, "data", None)
    if not isinstance(data, dict):
        return None
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def has_at_bot_in_original(event: object, bot_self_id: str) -> bool:
    """``event.original_message`` 里是否有 @bot 段（两种协议的段形状都认）。

    OneBot v11 与 Milky 适配器都会在分发前把消息**首/尾**的 @bot 段从
    ``event.message`` 摘走并置 ``to_me``；下游从 ``event.message`` 解析出的
    UniMessage 已看不到那个 @bot。``original_message`` 是摘段之前的深拷贝：

    * 纯 @bot（无正文）会变成空串——不认这段兜底就会被空消息闸门吞掉；
    * 「@bot @他人 怎么看」会被误判成「只 @ 了别人」，is_mentioned 为假。

    非 OneBot/Milky 事件不一定保留 ``original_message``；getattr + try/except
    让其它 adapter 安全返回 False（不退化原有行为）。
    """

    original = getattr(event, "original_message", None)
    if not original:
        return False
    target = str(bot_self_id)
    try:
        for seg in original:
            if at_target_of(seg) == target:
                return True
    except Exception:  # noqa: BLE001 - 异形 original_message 一律按「没有 @bot」处理
        return False
    return False


# --------------------------------------------------------------- P0-3 发送者


def sender_display_name(event: object) -> str | None:
    """发送者展示名：群名片优先、其次昵称；取不到（或两者都空白）返回 None。

    位置差异：OneBot v11 在 ``event.sender``，Milky 在 ``event.data.sender``
    （``IncomingMessage.sender`` = ``Friend`` 或 ``Member``）。空值回退 QQ 号是
    调用方的事（``nickname = ... or user_id``），本层不猜、不硬编码任何账号。
    """

    sender = getattr(event, "sender", None)
    if sender is None:
        data = getattr(event, "data", None)
        sender = getattr(data, "sender", None) if data is not None else None
    if sender is None:
        return None
    for attr in ("card", "nickname"):
        value = getattr(sender, attr, None)
        if value is None:
            continue
        text = str(value)
        if text.strip():
            return text
    return None


def _profile_text(info: object) -> str | None:
    """成员/好友资料（dict 或适配器模型）→ 群名片优先、其次昵称。"""

    for attr in ("card", "nickname"):
        if isinstance(info, dict):
            value = info.get(attr)
        else:
            value = getattr(info, attr, None)
        if value is None:
            continue
        text = str(value)
        if text.strip():
            return text
    return None


async def resolve_display_name(
    bot, *, adapter_name: str | None, user_id: str, group_id: str | None = None
) -> str | None:
    """按协议选对成员资料 API，取群名片/昵称（拿不到返回 None，由调用方回退）。"""

    if protocol(adapter_name) == MILKY:
        if group_id is not None:
            info = await bot.get_group_member_info(
                group_id=int(group_id), user_id=int(user_id)
            )
        else:
            info = await bot.get_friend_info(user_id=int(user_id))
        return _profile_text(info)
    if group_id is not None:
        info = await bot.call_api(
            "get_group_member_info", group_id=int(group_id), user_id=int(user_id)
        )
    else:
        info = await bot.call_api("get_stranger_info", user_id=int(user_id))
    return _profile_text(info)


# --------------------------------------------------------------- P0-4 戳一戳


async def send_poke(
    bot, *, adapter_name: str | None, group_id: str | None, user_id: str
) -> None:
    """发一次戳一戳。Milky 走 nudge，其它沿用 OneBot ``send_poke`` 原路径。

    参数一律转 int 才进协议端（Milky 的签名是 ``user_id: int`` /
    ``group_id: int``）。返回 ``None``：两个协议的成功回执都不带语义载荷，
    平台拒绝/网络不明以适配器异常向上抛，由调用方如实分类。
    """

    if protocol(adapter_name) == MILKY:
        if group_id is not None:
            await bot.send_group_nudge(group_id=int(group_id), user_id=int(user_id))
        else:
            await bot.send_friend_nudge(user_id=int(user_id), is_self=False)
        return
    if group_id is not None:
        await bot.call_api("send_poke", group_id=int(group_id), user_id=int(user_id))
    else:
        await bot.call_api("send_poke", user_id=int(user_id))


# --------------------------------------------------------------- P0-5 撤回

_DECIMAL_RE = re.compile(r"[0-9]+")


def _platform_seq(message_id: str | int) -> int:
    text = str(message_id).strip()
    if _DECIMAL_RE.fullmatch(text) is None:
        raise UnsupportedOperation("platform message id must be a decimal integer")
    return int(text)


async def recall_message(
    bot,
    *,
    adapter_name: str | None,
    group_id: str | None,
    user_id: str | None,
    message_id: str | int,
) -> None:
    """撤回一条平台消息（只由调用方保证「撤回的是 bot 自己发的」）。

    * OneBot：``delete_msg(message_id=…)``（原路径不变）。
    * Milky：群 ``recall_group_message(group_id, message_seq)``、
      私聊 ``recall_private_message(user_id, message_seq)``；``message_seq`` 就是
      P0-1 落库的那个十进制外部 ID，本层转 int。
    """

    seq = _platform_seq(message_id)
    if protocol(adapter_name) == MILKY:
        if group_id is not None:
            if not hasattr(bot, "recall_group_message"):
                raise UnsupportedOperation("current bot does not support Milky group recall")
            await bot.recall_group_message(group_id=int(group_id), message_seq=seq)
            return
        if user_id is None:
            raise UnsupportedOperation("private recall needs the peer user id")
        if not hasattr(bot, "recall_private_message"):
            raise UnsupportedOperation("current bot does not support Milky private recall")
        await bot.recall_private_message(user_id=int(user_id), message_seq=seq)
        return
    if not hasattr(bot, "delete_msg"):
        raise UnsupportedOperation("current bot does not support OneBot message recall")
    await bot.delete_msg(message_id=seq)
