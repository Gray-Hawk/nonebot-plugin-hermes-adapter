"""submit_control: Runs 控制输入的可选 MCP 工具（只登记，不直接执行）。

控制字段仍是既有真实字段：reply_to / should_exit_active / pin_anchor /
release_anchor / answer_anchor / anchor_merge。一次调用登记一份控制，按登记
先后顺序配给接下来的一份明确正文决策（空 body 也算一份）；没有登记就是缺省。

硬边界：
- 只登记：不发消息、不提前退窗、不提前钉锚——正文仍由决策信封发出，锚/退窗仍走
  发送成功后的原有控制应用时机；
- 只在 runs 接口生效：completions 与 astrbot 的控制输入仍是信封控制行（旧协议
  原样），本工具在那些接口下直接拒（控制丢失，正文照发）；
- 失败一律返回 ok=False 而不是抛异常：控制是增强，绝不允许卡住正文。
"""

from __future__ import annotations

from typing import Literal

from nonebot import logger
from pydantic import BaseModel, Field, model_validator

from ...core import control_intent
from ...core.inflight import make_inflight_key
from ...core.routing import CallerScope
from ..auth import scope_refusal


class SubmitControlInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    user_id: str | None = Field(default=None, description="Private-chat target user id (mutually exclusive with group_id)")
    group_id: str | None = Field(default=None, description="Group id (mutually exclusive with user_id)")
    reply_to: str | None = Field(
        default=None,
        description='Quote one message of this conversation by its plugin m:id, e.g. "m:123" (nothing else is accepted)',
    )
    should_exit_active: bool | None = Field(
        default=None, description="true = leave the active window after this reply is sent"
    )
    pin_anchor: bool | None = Field(
        default=None, description="commission judgement turns only: true pins the current request into <pinned_tasks>"
    )
    release_anchor: str | None = Field(
        default=None, description='commissioner user_id or "all" to unpin finished task anchors'
    )
    answer_anchor: str | None = Field(
        default=None, description='commissioner user_id or "all": this reply answers that pinned task'
    )
    anchor_merge: str | None = Field(
        default=None, description="starter user_id of a pinned task this message belongs to (merge instead of pinning)"
    )

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "SubmitControlInput":
        if bool(self.user_id) == bool(self.group_id):
            raise ValueError("exactly one of user_id / group_id must be given")
        return self


class SubmitControlResult(BaseModel):
    ok: bool
    error: str | None = None
    scope: Literal["private", "group"]
    target_id: str
    registered: bool = False
    """本控制是否真的进了队列（false = 控制丢失，正文将按缺省控制发送）。"""
    fields: list[str] = Field(default_factory=list)
    pending: int = 0
    """登记后该会话队列深度（本份 + 之前未消费的份数）。"""


def submit_control_impl(
    inp: SubmitControlInput,
    *,
    scope: CallerScope | None,
    interface: str,
    queue: control_intent.ControlQueue = control_intent.CONTROL_QUEUE,
) -> SubmitControlResult:
    """把一份控制登记进会话队列。scope 无默认值：漏传即 TypeError，不静默放行。"""
    is_private = inp.user_id is not None
    scope_kind: Literal["private", "group"] = "private" if is_private else "group"
    target_id: str = inp.user_id if is_private else inp.group_id  # type: ignore[assignment]

    def _fail(error: str) -> SubmitControlResult:
        return SubmitControlResult(
            ok=False, error=error, scope=scope_kind, target_id=target_id,
            registered=False, pending=queue.pending(_key()),
        )

    def _key() -> tuple[str, str]:
        # make_inflight_key 的群分支只看 group_id、私聊分支只看 user_id。
        return make_inflight_key(inp.adapter, is_private, inp.user_id or "", inp.group_id)

    if interface != "runs":
        # completions / astrbot 的控制输入仍是信封控制行；这里拒掉而不是静默排队，
        # 免得控制永远没人消费。正文不受影响。
        return _fail(
            f"control tools are Runs-only (current interface: {interface!r}); "
            "put the fields in the decision envelope control line instead"
        )
    if is_private:
        if scope is None:
            logger.warning("[MCP submit_control] 拒绝:调用方身份不可识别(私聊)")
            return _fail("unresolved caller — the bridge could not identify this endpoint")
    else:
        refusal = scope_refusal(inp.adapter, target_id, scope)
        if refusal is not None:
            return _fail(refusal)

    try:
        control = control_intent.build_control(
            reply_to=inp.reply_to,
            should_exit_active=inp.should_exit_active,
            pin_anchor=inp.pin_anchor,
            release_anchor=inp.release_anchor,
            answer_anchor=inp.answer_anchor,
            anchor_merge=inp.anchor_merge,
        )
    except control_intent.ControlIntentError as exc:
        logger.info(f"[MCP submit_control] 拒绝控制: {exc} (adapter={inp.adapter})")
        return _fail(str(exc))

    if not control:
        # 一次空调用等于「什么都不登记」：正文照缺省控制发送，不算失败。
        return SubmitControlResult(
            ok=True, scope=scope_kind, target_id=target_id,
            registered=False, fields=[], pending=queue.pending(_key()),
        )

    key = _key()
    pending = queue.register(key, control)
    logger.info(
        f"[MCP submit_control] registered scope={scope_kind} adapter={inp.adapter} "
        f"target={target_id} fields={sorted(control)} pending={pending}"
    )
    return SubmitControlResult(
        ok=True, scope=scope_kind, target_id=target_id,
        registered=True, fields=sorted(control), pending=pending,
    )
