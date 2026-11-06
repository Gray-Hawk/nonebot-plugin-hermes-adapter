"""wake_turn: 唤起一轮主动 turn(私聊/群聊),区别于 push_message 的直发。

唤起 = 烧一轮 LLM turn:内容交给模型按人设组织语言后发出,发言天然进会话上下文。
push_message 是「内容在手即写即出」,本工具是「没轮就没嘴」场景的点火器
(定时提醒、到点汇报)。防护在唤起侧收口:
  - 私聊目标过私聊主动下行白名单硬闸(user_lists.yaml 的 private_allow,唤起与直发
    共用,未配置回落管理员集合)——防群友社工诱导 bot 给 arbitrary 私聊;
  - per-target 令牌桶限频(wake_limits.yaml,缺省 3 格/300s 回 1)——超限直接拒,
    不排队;报错文案原样回给调用方,调用方不应重试撞桶;
  - 路由:BotRegistry 优先,重启后无 entry 时合成兜底(适配器过滤选 bot +
    合成 Target),「目标不说话就送不出去」降级为「等 bot 在线即可送达」。
调用面:MCP 工具(轮内的我)+ 插件内部 API(handlers/message_relay.py 的
wake_private_turn / wake_group_turn,apscheduler 定时任务直调,为未来提示
系统演进预留的干净函数签名)。
"""

from __future__ import annotations

from typing import Literal

from nonebot import logger
from pydantic import BaseModel, Field, model_validator

from ...core.routing import CallerScope
from ..auth import scope_refusal


class WakeTurnInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    user_id: str | None = Field(default=None, description="Private-chat target user id (mutually exclusive with group_id)")
    group_id: str | None = Field(default=None, description="Group target id (mutually exclusive with user_id)")
    text: str = Field(..., description="Content/prompt handed to the woken turn")
    source: str = Field(default="agent-request", description="Caller label; recorded as the synthetic message nickname")

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "WakeTurnInput":
        if bool(self.user_id) == bool(self.group_id):
            raise ValueError("exactly one of user_id / group_id must be given")
        self.source = self.source.strip() or "agent-request"
        return self


class WakeTurnResult(BaseModel):
    ok: bool
    error: str | None = None
    scope: Literal["private", "group"]
    target_id: str
    """被唤起的 (scope, id);error 时同样回填,便于调用方对账。"""


async def wake_turn_impl(inp: WakeTurnInput, *, scope: CallerScope | None) -> WakeTurnResult:
    """scope 是调用方的可操作范围,**没有默认值**:漏传会是 TypeError 而不是静默放行。

    范围校验:群侧沿用 CallerScope 群标签模型;私侧的白名单本身就是硬闸,
    这里只要求调用方身份可识别(防匿名端点)。
    """
    is_private = inp.user_id is not None
    scope_kind: Literal["private", "group"] = "private" if is_private else "group"
    target_id: str = inp.user_id if is_private else inp.group_id  # type: ignore[assignment]

    if not inp.text.strip():
        return WakeTurnResult(ok=False, error="text empty", scope=scope_kind, target_id=target_id)
    if is_private:
        if scope is None:
            logger.warning("[MCP wake_turn] 拒绝:调用方身份不可识别(私聊唤起)")
            return WakeTurnResult(
                ok=False,
                error="unresolved caller — the bridge could not identify this endpoint",
                scope=scope_kind,
                target_id=target_id,
            )
    else:
        refusal = scope_refusal(inp.adapter, target_id, scope)
        if refusal is not None:
            return WakeTurnResult(ok=False, error=refusal, scope=scope_kind, target_id=target_id)

    # 延迟导入:handlers 依赖 mcp 运行时单例,模块级互 import 会成环(background_report 同款)
    from ...handlers.message_relay import wake_group_turn, wake_private_turn

    if is_private:
        err = await wake_private_turn(
            adapter_name=inp.adapter, user_id=target_id, text=inp.text, source=inp.source
        )
    else:
        err = await wake_group_turn(
            adapter_name=inp.adapter, group_id=target_id, text=inp.text, source=inp.source
        )
    if err is not None:
        logger.warning(f"[MCP wake_turn] 拒绝: {err}")
        return WakeTurnResult(ok=False, error=err, scope=scope_kind, target_id=target_id)
    return WakeTurnResult(ok=True, scope=scope_kind, target_id=target_id)
