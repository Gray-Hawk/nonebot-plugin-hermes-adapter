"""平台差异层:OneBot v11 与 QQ 官方 Bot(nonebot-adapter-qq)差异的唯一集中点。

业务代码只调用本模块的语义函数,不写 `if adapter == "qq"` 分支。QQ 官方的差异
集中在四件事上:

1. **取号**:官方事件的主消息 id 是 ``event.id``(没有 ``message_id``);引用是
   ``QQReplyMessage`` 模型(没有 ``.message``,正文在 ``.content``、引用号在
   ``.msg_idx``)。统一走 ``platform_message_id`` / ``reply_content`` /
   ``reply_message_id``,入库、触发 fragment、steer 与引用对账因此同源。
2. **重复投递**:同一官方消息可能被投递两次(全量事件与 @ 事件共用同一 id),
   按 (scope, adapter, 平台消息 id) 做有界 TTL 去重。perception 与主 handler
   各自一个 scope —— 同一事件的这两条路径都要能走。
3. **身份语义**:官方用户/群是一串 OpenID,与 Bot 自身 appid 没有数值关系;
   全量群消息里 @bot 由平台 ``mentions.is_you`` 给出(适配器会把该 mention 的
   user_id 改写成 ``bot.self_info.id``)。所以判断「是不是我」只看 At 段与
   ``is_tome()`` 的组合,绝不做 ``member_openid == self_id`` 之类的数值比较。
4. **能力边界**:官方接口没有 poke / 撤回 / 群文件库 / 表情回执 / 合并转发 /
   本地文件直传 / 审批私聊通知 / 本地表情库;正文 ``@数字号`` 也没有对应
   openid。没能力的动作必须**明确拒绝或降级**,不得伪造成功。

未知 adapter 一律按 OneBot 旧行为(能力全开、无去重、无取号回落),这样接入
别的平台时行为与今天逐字一致。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from ..adapter_norm import normalize_adapter_name

OFFICIAL_QQ_ADAPTER = "qq"
"""归一化后的官方 Bot adapter 名(adapter-qq 的 ``get_name()`` 是 ``"QQ"``)。"""

MILKY_ADAPTER = "milky"
"""QQTPC 的 Milky 驱动:原生 API 覆盖 nudge/recall/群文件/表情回执/文件上传/转发。"""

_OFFICIAL_QQ_EVENT_MODULE_PREFIX = "nonebot.adapters.qq"


@dataclass(frozen=True)
class Capabilities:
    """一个平台族在本插件里具备的能力。默认值 = OneBot v11 既有行为。"""

    poke: bool = True
    recall: bool = True
    group_files: bool = True
    emoji_reaction: bool = True
    file_upload: bool = True
    forward_expand: bool = True
    approval_notify: bool = True
    sticker_library: bool = True
    integer_message_id: bool = True
    at_markup: bool = True
    """正文受控标记 ``@QQ号`` 是否能转成平台 @(官方号是 openid,数字号无意义)。"""


LEGACY = Capabilities()
"""OneBot v11 及一切未登记平台:保持今天的行为。"""

OFFICIAL_QQ = Capabilities(
    poke=False,
    recall=False,
    group_files=False,
    emoji_reaction=False,
    file_upload=False,
    forward_expand=False,
    approval_notify=False,
    sticker_library=False,
    integer_message_id=False,
    at_markup=False,
)

MILKY = Capabilities(
    # 现场安装的 nonebot-adapter-milky 1.3.0 具备:send_*_nudge、recall_*_message、
    # get_group_files/get_group_file_download_url、send_group_message_reaction、
    # upload_*_file、Forward 导出与私聊发送。唯一缺口是本地表情库 id(OneBot
    # sub_type=1 语义),没有对应映射 → 诚实关闭。
    sticker_library=False,
)
"""Milky:除本地表情库外均为原生实现,不再沿用 OneBot 的「全开」默认。"""


def is_official_qq(adapter_name: str | None) -> bool:
    """是否 QQ 官方 Bot(接受原始 adapter 名 ``"QQ"`` 或已归一化的 ``"qq"``)。"""
    return normalize_adapter_name(adapter_name or "") == OFFICIAL_QQ_ADAPTER


def capabilities(adapter_name: str | None) -> Capabilities:
    tag = normalize_adapter_name(adapter_name or "")
    if tag == OFFICIAL_QQ_ADAPTER:
        return OFFICIAL_QQ
    if tag == MILKY_ADAPTER:
        return MILKY
    return LEGACY


def adapter_name_of(source) -> str:
    """Bot / Target → 归一化 adapter 名(与 ``utils.get_adapter_name`` 同口径)。"""
    adapter = getattr(source, "adapter", "") or ""
    if hasattr(adapter, "get_name"):
        adapter = adapter.get_name()
    return normalize_adapter_name(adapter)


def _is_official_qq_event(event) -> bool:
    return type(event).__module__.startswith(_OFFICIAL_QQ_EVENT_MODULE_PREFIX)


def platform_message_id(event, *, adapter_name: str | None = None):
    """平台侧主消息 id(**原始类型**,取不到即 None)。

    OneBot v11 / v12 走 ``event.message_id``(通常是 int);QQ 官方事件没有该字段,
    主 id 是 ``event.id``(str)。刻意不统一成 str:OneBot 侧这个值会一路进
    `set_msg_emoji_like(message_id=…)` 等平台调用,改类型就是改既有行为。
    需要字符串键(去重、日志、DB 落库)的调用方自己 ``str()``。

    回落到 ``.id`` **只**对官方 QQ 事件或显式传入官方 adapter 名生效——其它平台
    没有 ``message_id`` 时过去就是 None,不能因为多了个 ``.id`` 就悄悄换口径。
    """
    if event is None:
        return None
    value = getattr(event, "message_id", None)
    if value not in (None, ""):
        if isinstance(value, str) and not value.strip():
            value = None
        if value is not None:
            return value
    if is_official_qq(adapter_name) or _is_official_qq_event(event):
        value = getattr(event, "id", None)
        if value not in (None, ""):
            if isinstance(value, str):
                text = value.strip()
                return text or None
            return value
    return None


def reply_message_id(reply):
    """被引消息的引用号。

    OneBot 的 ``Reply.message_id`` 过去原样透传(可能是 int,调用方按原类型比较),
    这里保持逐字不变;平台模型型引用(QQ 官方)取 ``msg_idx``,没有则 ``id``。
    """
    if reply is None:
        return None
    if hasattr(reply, "message_id"):
        return getattr(reply, "message_id", None)
    for attr in ("msg_idx", "id"):
        value = getattr(reply, attr, None)
        if value in (None, ""):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def reply_content(reply) -> tuple[str, list[str]] | None:
    """平台**模型型**引用的 ``(正文, 图片 URL 列表)``;非模型型引用返回 None。

    OneBot 的 ``event.reply`` 是 ``Message``(带 ``.message``),旧路径照旧;
    QQ 官方的引用是 ``QQReplyMessage``:正文在 ``.content``,媒体在
    ``.attachments``(带 ``content_type``/``url``)。这里只挑 image/*,其余附件
    类型在样板里不支持(降级为「引用里只看到文字」)。
    """
    if reply is None or hasattr(reply, "message") or not hasattr(reply, "content"):
        return None
    text = str(getattr(reply, "content", "") or "").strip()
    images: list[str] = []
    for attachment in getattr(reply, "attachments", None) or []:
        url = getattr(attachment, "url", None)
        content_type = str(getattr(attachment, "content_type", "") or "").lower()
        if url and content_type.startswith("image/"):
            images.append(str(url))
    return text, images


def is_bot_authored(event, *, adapter_name: str | None = None) -> bool:
    """这条消息是否由机器人账号发出(防 AI 互聊燃料)。

    只看官方平台:官方 group author 带 ``bot: true``,而 OneBot 的 sender 没有
    这个语义。其它平台一律返回 False,旧行为不变。
    """
    if not is_official_qq(adapter_name):
        return False
    author = getattr(event, "author", None)
    return bool(getattr(author, "bot", False)) if author is not None else False


def describe_send_error(exc: BaseException) -> str:
    """一次平台发送失败的摘要(adapter/HTTP code + message),绝不带凭据。"""
    parts = [type(exc).__name__]
    code = getattr(exc, "code", None)
    if code is not None:
        parts.append(f"code={code}")
    message = getattr(exc, "message", None)
    if message:
        parts.append(f"message={message}")
    status = getattr(exc, "status_code", None)
    if status:
        parts.append(f"http={status}")
    return " ".join(parts)


# ---------------------------------------------------------------- 入站去重
#
# 官方平台会把同一条消息投递多次(全量事件与 @ 事件同 id),而插件的两条入口
# (perception 入库、主 handler 触发)是同一次投递的两站。所以去重键带 scope:
# 同一 scope 内第二次起拒绝,跨 scope 互不影响。

_DEDUPE_TTL_S = 300.0
_DEDUPE_MAX = 4096
_seen: dict[tuple[str, str, str], float] = {}


def _seen_inbound() -> dict:
    """测试与观测用只读视图(不要原地改)。"""
    return _seen


def reset_inbound_dedupe() -> None:
    """清空去重表(测试/重启语义)。"""
    _seen.clear()


def claim_inbound(scope: str, adapter_name: str | None, message_id: str | None) -> bool:
    """认领一次入站事件:True=首次见到,False=重复投递。

    仅官方 QQ 启用(其它平台恒 True,行为与今天一致);没有平台消息 id 时也
    恒 True —— 「取不到号」不等于「重复」。
    """
    if not is_official_qq(adapter_name) or not message_id:
        return True
    key = (str(scope), normalize_adapter_name(adapter_name or ""), str(message_id))
    now = time.monotonic()
    expiry = _seen.get(key)
    if expiry is not None and expiry > now:
        return False
    _seen[key] = now + _DEDUPE_TTL_S
    if len(_seen) > _DEDUPE_MAX:
        for stale_key, stale_expiry in [kv for kv in _seen.items() if kv[1] <= now]:
            _seen.pop(stale_key, None)
        while len(_seen) > _DEDUPE_MAX:
            _seen.pop(next(iter(_seen)))
    return True
