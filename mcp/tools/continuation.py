"""request_continuation：completions 的显式续跑工具（只登记，不执行）。

旧协议把续跑写成控制行里的 ``not_final=true``；正文信封退役后改成一个最小
工具：调用即请求「结束当前轮并追加继续执行轮」，执行链仍是既有 relay 续跑
（``handlers.message_feedback``），只换触发入口。

硬边界：
- 只在 completions 接口生效：Runs 本来就在同一个 run 内续跑，登记了没人消费，
  直接拒绝而不是静默排队；
- 只登记：不发消息、不改正文、不做静默退窗；正文仍按纯文本协议发送；
- 失败一律返回 ok=False 而不是抛异常：续跑是增强，不允许卡住正文。
"""

from __future__ import annotations

from typing import Literal

from nonebot import logger
from pydantic import BaseModel, Field, model_validator

from ...core import continuation
from ...core.inflight import make_inflight_key
from ...core.routing import CallerScope
from ..auth import scope_refusal


class RequestContinuationInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    user_id: str | None = Field(default=None, description="Private-chat target user id (mutually exclusive with group_id)")
    group_id: str | None = Field(default=None, description="Group id (mutually exclusive with user_id)")

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "RequestContinuationInput":
        if bool(self.user_id) == bool(self.group_id):
            raise ValueError("exactly one of user_id / group_id must be given")
        return self


class RequestContinuationResult(BaseModel):
    ok: bool
    error: str | None = None
    scope: Literal["private", "group"]
    target_id: str
    registered: bool = False


def request_continuation_impl(
    inp: RequestContinuationInput,
    *,
    scope: CallerScope | None,
    interface: str,
    queue: continuation.ContinuationQueue = continuation.CONTINUATION_QUEUE,
) -> RequestContinuationResult:
    """登记一次续跑请求。scope 无默认值：漏传即 TypeError，不静默放行。"""
    is_private = inp.user_id is not None
    scope_kind: Literal["private", "group"] = "private" if is_private else "group"
    target_id: str = inp.user_id if is_private else inp.group_id  # type: ignore[assignment]

    def _fail(error: str) -> RequestContinuationResult:
        return RequestContinuationResult(
            ok=False, error=error, scope=scope_kind, target_id=target_id, registered=False,
        )

    if interface != "completions":
        return _fail(
            f"request_continuation is completions-only (current interface: {interface!r}); "
            "a Runs turn already continues inside the same run"
        )
    if is_private:
        if scope is None:
            logger.warning("[MCP request_continuation] 拒绝:调用方身份不可识别(私聊)")
            return _fail("unresolved caller — the bridge could not identify this endpoint")
    else:
        refusal = scope_refusal(inp.adapter, target_id, scope)
        if refusal is not None:
            return _fail(refusal)

    key = make_inflight_key(inp.adapter, is_private, inp.user_id or "", inp.group_id)
    queue.request(key)
    logger.info(
        f"[MCP request_continuation] registered scope={scope_kind} adapter={inp.adapter} "
        f"target={target_id}"
    )
    return RequestContinuationResult(
        ok=True, scope=scope_kind, target_id=target_id, registered=True,
    )
