"""后台完成汇报的运行时适配：路由、网关 HTTP 与消费器装配。"""

from __future__ import annotations

import re
from typing import Any, Iterable

import httpx

from .background_report_consumer import BackgroundReportConsumer
from .background_report_outbox import SOURCE_KANBAN
from .session import is_session_domain, session_domain

# 唤醒来源标签（内部身份，不伪装真人）：进 MessageOrigin.synthetic 的 label。
_SOURCE_LABELS = {
    SOURCE_KANBAN: "Kanban通知",
}
_DEFAULT_SOURCE_LABEL = "后台任务完成"



def parse_session_key(
    origin_session_id: str, *, prefixes: Iterable[str] = ("hermes",)
) -> dict[str, Any] | None:
    """解析 ``{prefix}-{adapter|domain}+{scope}+{target}[-gN]`` 会话键语法。

    只回答「这是不是一条可路由形态的会话键」，不解析真实 adapter：
    QQTPC 的 ``qqtpc`` 是业务会话域，真实 adapter 要在运行时另行解析。
    后台汇报的登记闸与唤醒路由共用这一份语法，避免两处正则漂移。
    """
    joined = "|".join(re.escape(prefix) for prefix in prefixes if prefix)
    if not joined or not origin_session_id:
        return None
    match = re.fullmatch(
        rf"(?:{joined})-(?P<adapter>[^+]+)\+(?P<scope>private|group)\+"
        r"(?P<target>[^+]+?)(?:-g\d+)?",
        origin_session_id,
    )
    if match is None:
        return None
    is_private = match.group("scope") == "private"
    target = match.group("target")
    if not target:
        return None
    return {
        "adapter": match.group("adapter"),
        "is_private": is_private,
        "user_id": target if is_private else None,
        "group_id": None if is_private else target,
    }


def resolve_background_route(
    origin_session_id: str,
    known_sessions: Iterable[tuple],
    *,
    prefixes: Iterable[str] = ("hermes",),
    adapter_resolver=None,
) -> dict[str, Any] | None:
    """现会话用登记路由；T1 后旧 session key 从稳定事件身份还原目标。

    session key 按真实后端命名：``prefixes`` 是**本会话可能出现的全部** key 前缀
    （当前在后、升级前登记的旧前缀在后）。默认只有 ``hermes-``，即旧行为。

    QQTPC 的 session_key 里是业务会话域 ``qqtpc``（不是 adapter）。生产调用方
    传 ``adapter_resolver(domain, is_private, user_id, group_id)``，由当前 Bot
    连接／事件／BotRegistry 路由解析真实 adapter；解析不出来返回 None（等待
    语义）。未传解析器时：业务域名本身不猜路由直接放弃，旧 key 自带的真实
    transport 维持旧行为。
    """
    for row in known_sessions:
        if len(row) == 6 and row[5] == origin_session_id:
            _, adapter, is_private, user_id, group_id, _ = row
            return {
                "adapter_name": adapter, "is_private": bool(is_private),
                "user_id": user_id if is_private else None, "group_id": group_id,
            }
    parsed = parse_session_key(origin_session_id, prefixes=prefixes)
    if parsed is None:
        return None
    adapter = parsed["adapter"]
    is_private = parsed["is_private"]
    user_id = parsed["user_id"]
    group_id = parsed["group_id"]
    domain = session_domain(adapter)
    if is_session_domain(adapter):
        # 新派生 key 里是业务会话域，不是 adapter：必须靠当前连接/路由解析。
        if adapter_resolver is None:
            return None
        resolved = adapter_resolver(domain, is_private, user_id, group_id)
        if not resolved:
            return None
        adapter = resolved
    elif domain != adapter and adapter_resolver is not None:
        # 归一前登记的 key 自带真实 transport；有运行时解析器时按当前事实校正
        # （协议可能已经切换），解析不出来就等待。
        resolved = adapter_resolver(domain, is_private, user_id, group_id)
        if not resolved:
            return None
        adapter = resolved
    return {
        "adapter_name": adapter, "is_private": is_private,
        "user_id": user_id, "group_id": group_id,
    }


class BackgroundCompletionGateway:
    """background-completion 薄插件的 HTTP 适配；事件在拉取前已由网关认领。"""

    def __init__(
        self, *, base_url: str, api_key: str = "", timeout: float = 10,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=timeout,
            headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
        )

    @staticmethod
    def _json(response: httpx.Response) -> dict:
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("background-completion response is not an object")
        return payload

    async def list_events(self) -> list[dict]:
        response = await self._client.get("/api/plugins/background-completion/events")
        events = self._json(response).get("events")
        if not isinstance(events, list):
            raise RuntimeError("background-completion events is not a list")
        return [item for item in events if isinstance(item, dict)]

    async def claim(self, event_id: str) -> bool:
        # GET /events 只返回网关已 claimed 或原生 dropped 的可接管事件；
        # 上游竞争在薄插件内完成，QQ 侧不再发起第二次认领。
        return True

    async def complete(self, event_id: str, run_id: str) -> None:
        response = await self._client.post(
            f"/api/plugins/background-completion/events/{event_id}/ack", json={}
        )
        self._json(response)

    async def drop(self, event_id: str) -> None:
        response = await self._client.post(
            f"/api/plugins/background-completion/events/{event_id}/drop",
            json={"reason": "report_failed"},
        )
        self._json(response)

    async def drop_session(self, origin_session_id: str) -> None:
        response = await self._client.post(
            "/api/plugins/background-completion/sessions/drop",
            json={"origin_session_id": origin_session_id, "reason": "session_cleared"},
        )
        self._json(response)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


async def _resolve_route(origin_session_id: str) -> dict | None:
    from .session import session_manager

    return resolve_background_route(
        origin_session_id,
        session_manager.known_sessions(),
        prefixes=session_manager.session_key_prefixes(),
        adapter_resolver=session_manager.resolve_adapter_for_domain,
    )


async def _wake(route: dict, text: str, source_event_id: str, source_kind: str = "subagent") -> str | None:
    """按来源用内部身份（sys/内部通知）点燃正常会话 turn，产出真 run 与 QQ 实发。"""
    from ..handlers.message_relay import wake_group_turn, wake_private_turn

    label = _SOURCE_LABELS.get(source_kind, _DEFAULT_SOURCE_LABEL)
    # 可信内部生产者显式给出通知类型;label 同时作为来源审计留在通知里。
    notice_type = label
    if route["is_private"]:
        return await wake_private_turn(
            adapter_name=route["adapter_name"], user_id=route["user_id"],
            text=text, source=label, source_event_id=source_event_id,
            notice_type=notice_type,
        )
    return await wake_group_turn(
        adapter_name=route["adapter_name"], group_id=route["group_id"],
        text=text, source=label, source_event_id=source_event_id,
        notice_type=notice_type,
    )


async def _reconcile(run_id: str):
    from .run_delivery_monitor import reconcile_run

    return await reconcile_run(run_id, reason="background_report_recovery")


def build_background_report_consumer(*, outbox, tracker, gateway) -> BackgroundReportConsumer:
    return BackgroundReportConsumer(
        outbox=outbox, tracker=tracker, gateway=gateway,
        resolve_route=_resolve_route, wake=_wake, reconcile_run=_reconcile,
    )
