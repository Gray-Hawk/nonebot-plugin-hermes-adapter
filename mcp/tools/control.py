"""submit_control: 正文决策的控制输入工具（只登记，不直接执行）。

控制字段仍是既有真实字段：reply_to / should_exit_active / pin_anchor /
release_anchor / answer_anchor / anchor_merge。一次调用登记一份控制，按登记
先后顺序配给接下来的一份明确正文（Runs 与 completions 共用）；没有登记就是缺省。

硬边界：
- 只登记：不发消息、不提前退窗、不提前钉锚——正文仍按纯文本协议发送，锚/退窗仍走
  发送成功后的原有控制应用时机；
- 只在有正文决策通道的接口生效（Runs / completions）：astrbot 等接口直接拒
  （控制丢失，正文照发）；
- 任务锚总开关（chat_config.yaml task_anchor_enabled，缺省 true）关闭时，四个锚
  字段一律不进队列：只带锚字段的调用返回 ok=False，混合调用把锚字段列进
  ignored_fields；reply_to / should_exit_active 不受影响；
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
    ignored_fields: list[str] = Field(default_factory=list)
    """因任务锚总开关关闭而丢弃的锚字段（pin/release/answer/merge）；不为空时绝不假装已登记。"""
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

    if interface not in ("runs", "completions"):
        # astrbot 与其它接口没有正文决策通道，登记了也没人消费；直接拒，
        # 正文不受影响。
        return _fail(
            f"control tools need a text-body decision channel (current interface: {interface!r})"
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

    # 任务锚总开关关闭：锚字段一律不进队列（不产生实际锚副作用），非锚控制照常。
    # 只有锚字段时返回 ok=False + ignored_fields，明确告诉模型「没登记」，不假装成功。
    ignored_fields: list[str] = []
    if control and not control_intent.task_anchor_enabled():
        control, ignored_fields = control_intent.drop_anchor_fields(control)
        if ignored_fields:
            logger.info(
                f"[MCP submit_control] 任务锚开关关闭，丢弃锚字段 scope={scope_kind} "
                f"adapter={inp.adapter} target={target_id} ignored={ignored_fields}"
            )

    if not control:
        if ignored_fields:
            return SubmitControlResult(
                ok=False,
                error=(
                    "task anchors are disabled (chat_config.yaml task_anchor_enabled=false); "
                    f"anchor control {ignored_fields} was not registered"
                ),
                scope=scope_kind, target_id=target_id, registered=False,
                fields=[], ignored_fields=ignored_fields, pending=queue.pending(_key()),
            )
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
        + (f" ignored={ignored_fields}" if ignored_fields else "")
    )
    return SubmitControlResult(
        ok=True, scope=scope_kind, target_id=target_id,
        registered=True, fields=sorted(control), ignored_fields=ignored_fields,
        pending=pending,
    )
