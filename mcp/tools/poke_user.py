"""poke_user: 独立的戳一戳动作（正文里不写任何 poke 标记）。

戳一戳**只能独立使用**：它不是消息段，也不混在正文里。模型显式调用本工具，
插件直接调 OneBot v11 的 ``send_poke`` 公开 API：

* 群聊 ``send_poke(group_id, user_id)``（NapCat 群聊拒绝原生 poke 消息段）；
* 私聊 ``send_poke(user_id)``。

目标判定、范围校验与群/私参数约定与 ``push_message`` 共用同一套
（``validate_push_context`` + ``BotRegistry`` / ``_resolve_wake_route``）：
群聊必须有该 (adapter, group_id) 的活跃 reactive 会话与路由，私聊目标必须在
私聊主动下行白名单里；调用方身份认不出就拒。自戳一律拒绝。

结果如实反映平台回执：平台异常/拒绝记 ``platform_rejected``，网络结果不明记
``unconfirmed``（不自动重试）。OneBot 适配器拒绝时把线形回执原样放进
``ActionFailed.info``，其中的数值 ``retcode`` 照实带回；成功时适配器只返回
``data``（``send_poke`` 为 ``null``），所以成功结果不带 ``retcode``，也不伪造 ``0``。
"""

from __future__ import annotations

import time
from typing import Literal

from nonebot import get_bot, logger
from nonebot.adapters.onebot.v11.exception import ActionFailed, NetworkError
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...core.routing import CallerScope
from ...core import platform_compat
from ..auth import PushContextError, validate_push_context

ErrorKind = Literal["local_rejected", "platform_rejected", "unconfirmed"]


class PokeUserInput(BaseModel):
    """戳一戳输入：被戳的 QQ 号必填，群号可选（缺省＝私聊）。"""

    model_config = ConfigDict(extra="forbid")

    adapter: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1, description="QQ id to poke")
    group_id: str | None = Field(default=None, description="Group chat; omit for private")

    @model_validator(mode="after")
    def _ids_must_be_digits(self) -> "PokeUserInput":
        if not (self.user_id.strip().isascii() and self.user_id.strip().isdigit()):
            raise ValueError("user_id must be an ASCII QQ number")
        if self.group_id is not None and not (
            self.group_id.strip().isascii() and self.group_id.strip().isdigit()
        ):
            raise ValueError("group_id must be an ASCII group number")
        return self


class PokeUserResult(BaseModel):
    """模型可见结果：不包含原始平台异常，只带结论与平台回执号。"""

    ok: bool
    error: str | None = None
    error_kind: ErrorKind | None = None
    retcode: int | None = None
    note: str | None = None


def _failure(kind: ErrorKind, error: str, *, retcode: int | None = None) -> PokeUserResult:
    return PokeUserResult(ok=False, error=error, error_kind=kind, retcode=retcode)


def _rejected_retcode(exc: ActionFailed) -> int | None:
    """适配器以 ``ActionFailed(**回执)`` 保留线形拒绝回执，数值码就在 ``info`` 里。"""
    info = getattr(exc, "info", None)
    if isinstance(info, dict):
        raw = info.get("retcode")
        if isinstance(raw, int) and not isinstance(raw, bool):
            return raw
    return None


def _resolve_target(inp: PokeUserInput, *, bot_registry):
    """群/私两条目标解析（与 push_message / recall_message 同一套路由）。"""
    if inp.group_id is not None:
        entry = bot_registry.get(inp.adapter, "group", inp.group_id.strip())
        if entry is None:  # validate 之后 defensive：路由可能在窗口期内消失
            return "unknown target or no active bot route"
        try:
            bot = get_bot(entry.bot_self_id)
        except (KeyError, ValueError):
            return "current bot is offline"
        return bot, entry.target
    # 延迟导入：handlers → mcp → handlers 在模块级会成环（push_message 同款）。
    from ...handlers import message_relay

    route = message_relay._resolve_wake_route(inp.adapter, "private", inp.user_id.strip())
    if isinstance(route, str):
        logger.warning(
            f"[MCP poke_user] 私聊路由失败 adapter={inp.adapter} user={inp.user_id} "
            f"reason={route}"
        )
        return "no active bot route for this private chat"
    bot, target = route
    return bot, target


async def poke_user_impl(
    inp: PokeUserInput,
    *,
    active_sessions,
    bot_registry,
    scope: CallerScope | None,
) -> PokeUserResult:
    """校验范围与目标后执行一次 ``send_poke``；不发送任何消息。"""
    target_qq = inp.user_id.strip()
    if not target_qq.isdigit():
        return _failure("local_rejected", "user_id must be an ASCII QQ number")
    if not platform_compat.capabilities(inp.adapter).poke:
        # 官方 QQ 没有 poke 能力:明确拒绝,不调平台、不伪造成功。
        return _failure(
            "local_rejected",
            f"poke is unsupported on adapter {inp.adapter!r}; "
            "only OneBot v11 exposes send_poke",
        )
    try:
        validate_push_context(
            adapter=inp.adapter,
            group_id=inp.group_id.strip() if inp.group_id is not None else None,
            user_id=None if inp.group_id is not None else target_qq,
            active_sessions=active_sessions,
            bot_registry=bot_registry,
            now_ms=int(time.time() * 1000),
            scope=scope,
        )
    except PushContextError as exc:
        logger.warning(f"[MCP poke_user] context invalid: {exc}")
        return _failure("local_rejected", str(exc))

    resolved = _resolve_target(inp, bot_registry=bot_registry)
    if isinstance(resolved, str):
        return _failure("local_rejected", resolved)
    bot, _target = resolved

    self_id = str(getattr(bot, "self_id", "") or "").strip()
    if self_id and self_id.isdigit() and int(target_qq) == int(self_id):
        # 自戳是非法目标：不伪造已戳，也不吞掉调用方的错误。
        return _failure("local_rejected", "refusing to poke the bot itself")

    try:
        if inp.group_id is not None:
            await bot.call_api(
                "send_poke", group_id=int(inp.group_id.strip()), user_id=int(target_qq)
            )
        else:
            await bot.call_api("send_poke", user_id=int(target_qq))
    except ActionFailed as exc:
        retcode = _rejected_retcode(exc)
        logger.warning(
            f"[MCP poke_user] 平台拒绝 adapter={inp.adapter} target={target_qq} "
            f"error={type(exc).__name__} retcode={retcode}"
        )
        return _failure(
            "platform_rejected",
            "the platform refused this poke; permission or target state may apply",
            retcode=retcode,
        )
    except (NetworkError, TimeoutError) as exc:
        logger.warning(
            f"[MCP poke_user] 结果不明 adapter={inp.adapter} target={target_qq} "
            f"error={type(exc).__name__}"
        )
        return _failure(
            "unconfirmed",
            "poke result is unconfirmed because the platform response was not "
            "received; do not retry blindly",
        )
    except Exception as exc:  # noqa: BLE001 - 其余平台/连接错误同样失败即闭
        logger.warning(
            f"[MCP poke_user] 调用失败 adapter={inp.adapter} target={target_qq} "
            f"error={type(exc).__name__}"
        )
        return _failure("unconfirmed", "poke call failed before a confirmed receipt")

    # 适配器成功只返回 data（send_poke 为 null）：成功码不可见，就不写 retcode。
    logger.info(
        f"[MCP poke_user] sent adapter={inp.adapter} group={inp.group_id} "
        f"target={target_qq}"
    )
    return PokeUserResult(
        ok=True,
        note="platform accepted one poke request; no chat message was sent",
    )
