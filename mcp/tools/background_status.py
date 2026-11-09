"""get_background_tasks: 后台任务状态的只读查询（真实登记，不由模型自报）。

`background_task` 便签已退役：旧登记消费者随 delivery_poll 一起下线，模型自报的
「我刚派了后台任务」不再有任何落点。后台状态改为插件从真实登记里查——本工具读
`background_report_outbox`（子代理 / Kanban 完成事件的持久信箱）里属于当前会话
且仍在待汇报状态的事件：task_state 是上游任务状态，report_state 是插件侧的汇报
进度。没有登记就是没有后台任务，不要凭记忆断言。

只读：不认领、不销账、不唤醒、不改任何状态。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from ...core.routing import CallerScope
from ..auth import scope_refusal


class GetBackgroundTasksInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    user_id: str | None = Field(default=None, description="Private-chat user id (mutually exclusive with group_id)")
    group_id: str | None = Field(default=None, description="Group id (mutually exclusive with user_id)")

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "GetBackgroundTasksInput":
        if bool(self.user_id) == bool(self.group_id):
            raise ValueError("exactly one of user_id / group_id must be given")
        return self


class BackgroundTaskItem(BaseModel):
    event_id: str
    source: str
    task_state: str
    """上游任务状态（如 running / completed / failed）。"""
    report_state: str
    """插件汇报进度：pending / reporting / retry_wait（已 acked 的不再列出）。"""
    dispatched_at: float | None = None
    completed_at: float | None = None
    summary: str = ""
    """任务结果摘要（单行截断，仅供你判断该不该汇报，不要整段复述）。"""


class GetBackgroundTasksResult(BaseModel):
    ok: bool
    error: str | None = None
    scope: Literal["private", "group"]
    target_id: str
    tasks: list[BackgroundTaskItem] = Field(default_factory=list)
    count: int = 0


def _session_prefix(adapter: str, is_private: bool, target_id: str) -> str:
    """会话在 outbox 里的 origin_session_id 前缀（含 `-g<代际>` 变体）。"""
    kind = "private" if is_private else "group"
    return f"hermes-{adapter}+{kind}+{target_id}"


def _belongs(origin_session_id: str, prefix: str) -> bool:
    return origin_session_id == prefix or origin_session_id.startswith(prefix + "-g")


def _summary(event: dict) -> str:
    """结果摘要：只取第一行，截断到 120 字，不搬整段结果。"""
    text = str(event.get("result") or "").strip()
    if not text:
        return ""
    first = text.splitlines()[0].strip()
    return first if len(first) <= 120 else first[:120] + "…"


def get_background_tasks_impl(
    inp: GetBackgroundTasksInput,
    *,
    outbox,
    scope: CallerScope | None,
    limit: int = 20,
) -> GetBackgroundTasksResult:
    """读真实登记。outbox 为 None（未装配）时明确回错误，不假装「没有后台任务」。"""
    is_private = inp.user_id is not None
    scope_kind: Literal["private", "group"] = "private" if is_private else "group"
    target_id: str = inp.user_id if is_private else inp.group_id  # type: ignore[assignment]

    def _fail(error: str) -> GetBackgroundTasksResult:
        return GetBackgroundTasksResult(
            ok=False, error=error, scope=scope_kind, target_id=target_id,
        )

    if not is_private:
        refusal = scope_refusal(inp.adapter, target_id, scope)
        if refusal is not None:
            return _fail(refusal)
    elif scope is None:
        return _fail("unresolved caller — the bridge could not identify this endpoint")

    if outbox is None:
        return _fail("background registry unavailable (outbox not wired)")

    prefix = _session_prefix(inp.adapter, is_private, target_id)
    items: list[BackgroundTaskItem] = []
    for event in outbox.active_events():
        origin = str(event.get("origin_session_id") or "")
        if not _belongs(origin, prefix):
            continue
        items.append(
            BackgroundTaskItem(
                event_id=str(event.get("event_id") or ""),
                source=str(event.get("source") or ""),
                task_state=str(event.get("task_state") or ""),
                report_state=str(event.get("report_state") or ""),
                dispatched_at=event.get("dispatched_at"),
                completed_at=event.get("completed_at"),
                summary=_summary(event),
            )
        )
        if len(items) >= limit:
            break
    return GetBackgroundTasksResult(
        ok=True, scope=scope_kind, target_id=target_id, tasks=items, count=len(items),
    )
