"""get_background_tasks: 后台状态的只读查询（真实登记，不由模型自报）。

`background_task` 便签已退役后，这个工具要回答两件**不同**的事，字段分开、不许混读：

- ``running``：「现在真的在跑」的后台任务。只读复用两处已有真实登记，不新建登记
  系统——看板 ``tasks.status='running'`` 的执行中卡片，以及 background-completion
  登记库里 ``state='registered'``（已派发、等 durable 终态）的后台子代理；
- ``reports``：``background_report_outbox`` 里属于本会话、仍未汇报完成的
  **已完成**事件（子代理 / Kanban 终态）。``task_state`` 是上游任务状态，
  ``report_state`` 是插件侧的汇报进度。

``active_events`` 的 active 只是投递状态，不是执行状态：非终态任务根本不会进信箱。
所以「信箱为空」不能证明「没有后台任务在跑」。没有可读运行源时
``running_unknown=True``，模型必须按「运行态未知」表述，不能断言没有任务。

只读：不认领、不销账、不唤醒、不改任何状态。
"""

from __future__ import annotations

from typing import Literal, Sequence

from pydantic import BaseModel, Field, model_validator

from ...core.routing import CallerScope
from ...core.running_tasks import collect_running
from ..auth import scope_refusal

# 运行态未知时的统一口径；模型不得把空 running 列表当作「没有任务」。
UNKNOWN_RUNNING_NOTE = (
    "没有可读的真实运行源：running 为空不能证明没有后台任务在跑。"
    "只有 reports 是真实登记；需要确认在跑任务时请明确说明运行态未知。"
)


class GetBackgroundTasksInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    user_id: str | None = Field(default=None, description="Private-chat user id (mutually exclusive with group_id)")
    group_id: str | None = Field(default=None, description="Group id (mutually exclusive with user_id)")

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "GetBackgroundTasksInput":
        if bool(self.user_id) == bool(self.group_id):
            raise ValueError("exactly one of user_id / group_id must be given")
        return self


class BackgroundRunningItem(BaseModel):
    task_id: str
    source: str
    """真实运行源：kanban（看板执行中卡片）/ delegation（后台子代理登记）。"""
    state: Literal["running"] = "running"
    title: str = ""
    assignee: str = ""
    started_at: float | None = None
    run_id: str = ""
    last_heartbeat_at: float | None = None


class BackgroundReportItem(BaseModel):
    event_id: str
    source: str
    task_state: str
    """上游任务状态（终态：completed / blocked / failed 等）。"""
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
    running: list[BackgroundRunningItem] = Field(default_factory=list)
    running_count: int = 0
    running_sources: list[str] = Field(default_factory=list)
    """本次真正读到数据的运行源；空 = 运行态未知。"""
    running_source_errors: list[str] = Field(default_factory=list)
    running_unknown: bool = False
    reports: list[BackgroundReportItem] = Field(default_factory=list)
    report_count: int = 0
    report_source_error: str | None = None
    count: int = 0
    """running_count + report_count（日志/旧调用方用的合计数，勿当单一语义）。"""
    note: str = ""


def _session_prefixes(adapter: str, is_private: bool, target_id: str) -> list[str]:
    """会话在真实登记里的 origin_session_id 前缀（含 `-g<代际>` 变体）。

    session key 按真实后端命名：前缀取自 ``session_manager.session_key_prefixes()``
    （当前后端在前）。Hermes 后端只有 `hermes-`；换成别的后端前缀时还要认升级前
    登记的 `hermes-*`，否则老事件会被当成别人的。

    QQTPC 协议另外要认两种会话形态：归一后的业务会话域（``hermes-qqtpc+…``，新派生
    key）与迁移前登记的真实 transport（``hermes-onebotv11+…``）。旧登记不能当成
    别人的，因此两种都列；未归一的 adapter（QQ 官方等）只有一种。
    """
    from ...core.session import session_domain, session_manager

    kind = "private" if is_private else "group"
    domains = [session_domain(adapter)]
    if domains[0] != adapter:
        domains.append(adapter)
    prefixes: list[str] = []
    for prefix in session_manager.session_key_prefixes():
        for domain in domains:
            item = f"{prefix}-{domain}+{kind}+{target_id}"
            if item not in prefixes:
                prefixes.append(item)
    return prefixes


def _belongs(origin_session_id: str, prefixes: str | Sequence[str]) -> bool:
    if isinstance(prefixes, str):
        prefixes = (prefixes,)
    return any(
        origin_session_id == prefix or origin_session_id.startswith(prefix + "-g")
        for prefix in prefixes
    )


def _summary(event: dict) -> str:
    """结果摘要：只取第一行，截断到 120 字，不搬整段结果。"""
    text = str(event.get("result") or "").strip()
    if not text:
        return ""
    first = text.splitlines()[0].strip()
    return first if len(first) <= 120 else first[:120] + "…"


def _collect_running(running_sources, *, prefixes: list[str], limit: int):
    """按会话前缀聚合运行源（换过后端前缀时可能有两个真实前缀）。

    单前缀时行为与 ``collect_running`` 完全一致；多前缀时逐前缀查、按
    ``(source, task_id)`` 去重，读失败来源名也去重——不让一次换名前缀把旧登记算成别人的。
    """
    if len(prefixes) <= 1:
        return collect_running(
            running_sources, session_prefix=prefixes[0] if prefixes else "", limit=limit,
        )
    items: list[dict] = []
    ok_sources: list[str] = []
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for prefix in prefixes:
        found, sources_ok, source_errors = collect_running(
            running_sources, session_prefix=prefix, limit=limit,
        )
        for name in sources_ok:
            if name not in ok_sources:
                ok_sources.append(name)
        for error in source_errors:
            if error not in errors:
                errors.append(error)
        for item in found:
            key = (str(item.get("source") or ""), str(item.get("task_id") or ""))
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
    return items[:limit], ok_sources, errors


def get_background_tasks_impl(
    inp: GetBackgroundTasksInput,
    *,
    outbox,
    scope: CallerScope | None,
    running_sources=None,
    limit: int = 20,
) -> GetBackgroundTasksResult:
    """读真实登记：running 只来自真实运行源，reports 只来自完成汇报信箱。"""
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

    prefixes = _session_prefixes(inp.adapter, is_private, target_id)

    # 运行中：真实运行源只读聚合；读不到就是「未知」，不是「没有」。
    # 换过后端前缀时还要认升级前的 hermes- 登记，按前缀各查一次再合并。
    running_items, running_sources_ok, running_errors = _collect_running(
        running_sources, prefixes=prefixes, limit=limit,
    )
    running = [
        BackgroundRunningItem(
            task_id=str(item.get("task_id") or ""),
            source=str(item.get("source") or ""),
            title=str(item.get("title") or ""),
            assignee=str(item.get("assignee") or ""),
            started_at=item.get("started_at"),
            run_id=str(item.get("run_id") or ""),
            last_heartbeat_at=item.get("last_heartbeat_at"),
        )
        for item in running_items
    ]

    # 已完成待汇报：仍读持久信箱（这是它唯一的真实语义）。
    reports: list[BackgroundReportItem] = []
    report_error: str | None = None
    if outbox is None:
        report_error = "background report registry unavailable (outbox not wired)"
    else:
        try:
            events = outbox.active_events()
        except Exception as exc:
            report_error = f"background report registry read failed: {type(exc).__name__}"
        else:
            for event in events:
                origin = str(event.get("origin_session_id") or "")
                if not _belongs(origin, prefixes):
                    continue
                reports.append(
                    BackgroundReportItem(
                        event_id=str(event.get("event_id") or ""),
                        source=str(event.get("source") or ""),
                        task_state=str(event.get("task_state") or ""),
                        report_state=str(event.get("report_state") or ""),
                        dispatched_at=event.get("dispatched_at"),
                        completed_at=event.get("completed_at"),
                        summary=_summary(event),
                    )
                )
                if len(reports) >= limit:
                    break

    running_unknown = not running_sources_ok
    if running_unknown and report_error is not None:
        return _fail(
            "background registry unavailable: "
            f"no readable running source ({'; '.join(running_errors) or 'not wired'}); "
            f"{report_error}"
        )
    return GetBackgroundTasksResult(
        ok=True,
        scope=scope_kind,
        target_id=target_id,
        running=running,
        running_count=len(running),
        running_sources=running_sources_ok,
        running_source_errors=running_errors,
        running_unknown=running_unknown,
        reports=reports,
        report_count=len(reports),
        report_source_error=report_error,
        count=len(running) + len(reports),
        note=UNKNOWN_RUNNING_NOTE if running_unknown else "",
    )
