"""后台完成汇报的运行时适配：路由、网关 HTTP 与消费器装配。"""

from __future__ import annotations

import re
from typing import Any, Iterable

import httpx

from .background_report_consumer import BackgroundReportConsumer


def resolve_background_route(
    origin_session_id: str, known_sessions: Iterable[tuple]
) -> dict[str, Any] | None:
    """现会话用登记路由；T1 后旧 session key 从稳定事件身份还原目标。"""
    for row in known_sessions:
        if len(row) == 6 and row[5] == origin_session_id:
            _, adapter, is_private, user_id, group_id, _ = row
            return {
                "adapter_name": adapter, "is_private": bool(is_private),
                "user_id": user_id, "group_id": group_id,
            }
    match = re.fullmatch(
        r"hermes-(?P<adapter>[^+]+)\+(?P<scope>private|group)\+"
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
        "adapter_name": match.group("adapter"), "is_private": is_private,
        "user_id": target if is_private else None,
        "group_id": None if is_private else target,
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

    return resolve_background_route(origin_session_id, session_manager.known_sessions())


async def _wake(route: dict, text: str, source_event_id: str) -> str | None:
    from ..handlers.message_relay import wake_group_turn, wake_private_turn

    if route["is_private"]:
        return await wake_private_turn(
            adapter_name=route["adapter_name"], user_id=route["user_id"],
            text=text, source="后台任务完成", source_event_id=source_event_id,
        )
    return await wake_group_turn(
        adapter_name=route["adapter_name"], group_id=route["group_id"],
        text=text, source="后台任务完成", source_event_id=source_event_id,
    )


async def _reconcile(run_id: str):
    from .run_delivery_monitor import reconcile_run

    return await reconcile_run(run_id, reason="background_report_recovery")


def build_background_report_consumer(*, outbox, tracker, gateway) -> BackgroundReportConsumer:
    return BackgroundReportConsumer(
        outbox=outbox, tracker=tracker, gateway=gateway,
        resolve_route=_resolve_route, wake=_wake, reconcile_run=_reconcile,
    )
