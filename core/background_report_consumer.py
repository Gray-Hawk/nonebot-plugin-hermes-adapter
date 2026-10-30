"""后台完成事件消费器：上游认领 → 汇报轮 → QQ 实发回执 → 上游确认。"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from .background_report_outbox import BackgroundReportOutbox
from .run_tracker import RunTracker

logger = logging.getLogger(__name__)


class BackgroundReportConsumer:
    """按来源会话串行消费持久事件；只有真实出站回执才算送达。"""

    def __init__(
        self,
        *,
        outbox: BackgroundReportOutbox,
        tracker: RunTracker,
        gateway: Any,
        resolve_route: Callable[[str], Awaitable[dict | None]],
        wake: Callable[[dict, str, str], Awaitable[str | None]],
        reconcile_run: Callable[[str], Awaitable[Any]],
        lease_seconds: float = 120,
        retry_delay_seconds: float = 15,
    ) -> None:
        self.outbox = outbox
        self.tracker = tracker
        self.gateway = gateway
        self.resolve_route = resolve_route
        self.wake = wake
        self.reconcile_run = reconcile_run
        self.lease_seconds = lease_seconds
        self.retry_delay_seconds = retry_delay_seconds
        self._lock = asyncio.Lock()

    async def tick(self, now: float) -> None:
        """执行一轮发现、恢复与放行；单实例重入串行，跨实例由存储租约保护。"""
        async with self._lock:
            for origin in self.outbox.pending_session_cancels():
                try:
                    await self.gateway.drop_session(origin)
                except Exception as exc:
                    logger.warning(
                        "[background-report] 旧会话终止待重试 error=%s", type(exc).__name__
                    )
                    continue
                self.outbox.finish_session_cancel(origin)
            try:
                events = await self.gateway.list_events()
            except Exception as exc:
                logger.warning("[background-report] 发现事件失败: %s", type(exc).__name__)
                events = []
            for event in events:
                try:
                    self.outbox.upsert_event(event)
                except Exception as exc:
                    logger.warning(
                        "[background-report] 事件落盘失败 event=%s error=%s",
                        event.get("event_id"), type(exc).__name__,
                    )

            for item in self.outbox.active_events():
                if item["report_state"] != "reporting":
                    continue
                try:
                    await self._reconcile_reporting(item["event_id"], now)
                except Exception as exc:
                    logger.warning(
                        "[background-report] 恢复核对失败 event=%s error=%s",
                        item["event_id"], type(exc).__name__,
                    )

            for due in self.outbox.due_events(now=now):
                item = self.outbox.get_event(due["event_id"])
                if item is None or item["report_state"] not in {"pending", "retry_wait"}:
                    continue
                if self.outbox.has_reporting_for_session(
                    item["origin_session_id"], exclude_event_id=item["event_id"]
                ):
                    continue
                try:
                    await self._start_report(item, now)
                except Exception as exc:
                    logger.warning(
                        "[background-report] 唤起失败 event=%s error=%s",
                        item["event_id"], type(exc).__name__,
                    )
                    self.outbox.release_claim(
                        item["event_id"], now=now,
                        delay_seconds=self.retry_delay_seconds, error=type(exc).__name__,
                    )

    async def _start_report(self, item: dict, now: float) -> None:
        event_id = item["event_id"]
        attempt = int(item["attempt_count"]) + 1
        idempotency_key = f"background-report:{event_id}:attempt:{attempt}"
        if not self.outbox.claim_event(
            event_id, idempotency_key=idempotency_key, now=now,
            lease_seconds=self.lease_seconds,
        ):
            return

        route = await self.resolve_route(item["origin_session_id"])
        if route is None:
            self.outbox.release_claim(
                event_id, now=now, delay_seconds=self.retry_delay_seconds,
                error="route_unavailable",
            )
            return
        try:
            claimed = await self.gateway.claim(event_id)
        except Exception as exc:
            self.outbox.release_claim(
                event_id, now=now, delay_seconds=self.retry_delay_seconds,
                error=type(exc).__name__,
            )
            return
        if not claimed:
            self.outbox.release_claim(
                event_id, now=now, delay_seconds=self.retry_delay_seconds,
                error="upstream_claim_busy",
            )
            return

        rejected = await self.wake(route, self._prompt(item), event_id)
        if rejected is not None:
            self.outbox.release_claim(
                event_id, now=now, delay_seconds=self.retry_delay_seconds,
                error=f"wake_rejected:{rejected[:80]}",
            )

    async def _reconcile_reporting(self, event_id: str, now: float) -> None:
        item = self.outbox.get_event(event_id)
        if item is None or item["report_state"] != "reporting":
            return
        delivery = self.tracker.get_latest_delivery_by_source_event(event_id)
        submission = self.tracker.get_latest_submission_by_source_event(event_id)
        run_id = (delivery or submission or {}).get("run_id")
        if run_id:
            self.outbox.attach_run(event_id, run_id)

        if delivery is not None:
            if self._is_actually_sent(delivery["run_id"]):
                await self.gateway.complete(event_id, delivery["run_id"])
                self.outbox.mark_acked(event_id, now=now)
                return
            if delivery["expected_state"] == "silent":
                await self._retry_or_fail(item, now, "silent_report")
                return
            if self._lease_expired(item, now):
                await self.reconcile_run(delivery["run_id"])
                refreshed = self.tracker.get_delivery(delivery["run_id"])
                if refreshed is not None and self._is_actually_sent(delivery["run_id"]):
                    await self.gateway.complete(event_id, delivery["run_id"])
                    self.outbox.mark_acked(event_id, now=now)
            return

        if submission is not None:
            if run_id and self._lease_expired(item, now):
                await self.reconcile_run(run_id)
            return
        if self._lease_expired(item, now):
            # 唤起曾被接受但没有形成 run_submissions：多为排队丢弃或进程中断。
            # 不允许它吞掉一次模型生成预算，也不换任务身份。
            self.outbox.release_claim(event_id, now=now, error="no_submission")

    async def _retry_or_fail(self, item: dict, now: float, reason: str) -> None:
        event_id = item["event_id"]
        if int(item["attempt_count"]) < 2:
            self.outbox.schedule_retry(
                event_id, error=reason, now=now,
                delay_seconds=self.retry_delay_seconds,
            )
            return
        try:
            await self.gateway.drop(event_id)
        except Exception as exc:
            logger.warning(
                "[background-report] 终止上游投递失败 event=%s error=%s",
                event_id, type(exc).__name__,
            )
            return
        self.outbox.mark_report_failed(event_id, error=reason, now=now)
        logger.error("[background-report] 汇报生成失败终态 event=%s reason=%s", event_id, reason)

    def _is_actually_sent(self, run_id: str) -> bool:
        delivery = self.tracker.get_delivery(run_id)
        if delivery is None or delivery["expected_state"] != "ready":
            return False
        components = self.tracker.list_components(run_id)
        primary = next((c for c in components if c["component_id"] == "primary"), None)
        return (
            primary is not None and primary["state"] == "sent"
            and all(c["state"] in {"sent", "not_required"} for c in components)
        )

    @staticmethod
    def _lease_expired(item: dict, now: float) -> bool:
        expires = item.get("lease_expires_at")
        return expires is None or float(expires) <= now

    @staticmethod
    def _prompt(item: dict) -> str:
        result = str(item.get("result") or "（任务未提供结果正文）")
        if len(result) > 12000:
            result = result[:12000] + "\n……结果过长，已截断。"
        return (
            "[后台任务完成汇报]\n"
            "一个后台子代理任务已经结束。请根据下面的任务结果，用当前会话的自然口吻"
            "直接向用户汇报有用结论；失败则说明失败原因和可行下一步。\n"
            "本轮必须给出最终答复，不要沉默、不要只回表情、不要继续派后台任务，"
            "不要提及事件号、数据库、网关、内部路径或投递机制，不要泄露凭据。\n"
            f"任务状态：{item.get('task_state') or 'unknown'}\n"
            f"任务结果：\n{result}"
        )
