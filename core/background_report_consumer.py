"""后台完成事件消费器：事件登记 → 持久去重 → 唤起 → 实发回执 → 销账。

两类事件来源共用同一张持久信箱、同一套去重与唤醒闸：

- ``subagent``：上游 background-completion 薄插件的事件，需要上游认领／ack/drop；
- ``kanban``：看板终态事件，由 ``core.kanban_report_source`` 以内部身份登记，
  上游没有对应认领对象，销账只落本地台账。

「已送达」只认实发事实（primary 组件 sent ＋ 其余组件已结算），不认交付台账
的期望状态字段——发完的交付会由 `mark_delivery_terminal` 前进到 ``terminal``，
若继续只认 ``ready``，一次成功的汇报会永远停在 ``reporting``，并把这个会话
之后的全部事件挡在门外。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from .background_report_outbox import (
    SOURCE_KANBAN,
    SOURCE_SUBAGENT,
    BackgroundReportOutbox,
)
from .run_tracker import RunTracker

logger = logging.getLogger(__name__)

# 组件层面「不需要再发」的终态；primary 必须真的 sent 才算送达。
_SETTLED_COMPONENTS = frozenset({"sent", "not_required"})
# 交付台账上表示「这一份永远不会再发」的业务终态（P-16 同集合）。
_UNSENDABLE_STATES = frozenset({"silent", "suppressed", "cancelled", "expired", "terminal"})
# 计划尚未定稿：流式即时决策在 run 结束前先落首份（``streaming``），后续决定还会
# 追加；此时已知组件全 sent 也不等于整次汇报完成，不能提前销账／放行同会话下一条。
_PLAN_OPEN_STATES = frozenset({"waiting", "streaming"})
# 走上游认领／ack 的来源；其余来源（kanban）只在本地台账销账。
_UPSTREAM_SOURCES = frozenset({SOURCE_SUBAGENT})
# 同会话串行闸的宽限：唤醒 turn 在 Hermes 侧最长 600s（WAKE_TURN_TIMEOUT_SECONDS），
# 汇报租约只有 120s，长 turn 进行中租约就会过期。若一过期就放行同会话的下一条，
# 会并发跑两轮 turn（api_server 无 per-session 锁，last-writer-wins）。宽限期覆盖
# 最长 turn，超过它就是真·残留，交给恢复核对接管。
SESSION_SERIAL_GRACE_SECONDS = 600.0


class WatermarkUnavailable(RuntimeError):
    """本次尝试的提交代际水位读不到：代际未知，必须 fail closed。

    未知代际不能认领：``attempt_baseline_seq`` 的 NULL 在过滤时是「升级前历史
    行」的兼容标记，新尝试一旦以 NULL 认领会完全绕过代际过滤，把上一代已
    suppressed 的交付当成本次尝试的结果（提前 report_failed／drop 上游）。
    """


def _is_primary(component_id: str) -> bool:
    return component_id == "primary" or component_id.endswith(":primary") or component_id.startswith("primary:")


class BackgroundReportConsumer:
    """按来源会话串行消费持久事件；只有真实出站回执才算送达。"""

    def __init__(
        self,
        *,
        outbox: BackgroundReportOutbox,
        tracker: RunTracker,
        gateway: Any,
        resolve_route: Callable[[str], Awaitable[dict | None]],
        wake: Callable[[dict, str, str, str], Awaitable[str | None]],
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
                    item["origin_session_id"], exclude_event_id=item["event_id"],
                    now=now - SESSION_SERIAL_GRACE_SECONDS,
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

    # -- 唤起 --

    async def _start_report(self, item: dict, now: float) -> None:
        event_id = item["event_id"]
        source = self._source(item)
        attempt = int(item["attempt_count"]) + 1
        idempotency_key = f"background-report:{event_id}:attempt:{attempt}"
        # 本次尝试的提交代际水位：认领前该事件已有的最大 submission_seq。wake 可能
        # 只是被接受并排队（同会话真人 turn 占槽），排队期间不会产生新 submission；
        # 只有水位之后的提交才是本次尝试的产物，用于把上一代交付挡在销账判定之外。
        # 水位读不到时 fail closed：未知代际不认领、不唤醒、不耗预算、不 drop，
        # 事件保持可重试，下一拍重新读取（读恢复后再按当前水位认领）。
        try:
            baseline_seq = self._submission_watermark(event_id)
        except WatermarkUnavailable as exc:
            logger.warning(
                "[background-report] 提交水位不可读,本轮不认领不唤醒 event=%s error=%s",
                event_id, type(exc.__cause__ or exc).__name__,
            )
            return
        if not self.outbox.claim_event(
            event_id, idempotency_key=idempotency_key, now=now,
            lease_seconds=self.lease_seconds, baseline_seq=baseline_seq,
        ):
            return

        route = await self.resolve_route(item["origin_session_id"])
        if route is None:
            self.outbox.release_claim(
                event_id, now=now, delay_seconds=self.retry_delay_seconds,
                error="route_unavailable",
            )
            return
        if source in _UPSTREAM_SOURCES:
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

        rejected = await self.wake(route, self._prompt(item), event_id, source)
        if rejected is not None:
            self.outbox.release_claim(
                event_id, now=now, delay_seconds=self.retry_delay_seconds,
                error=f"wake_rejected:{rejected[:80]}",
            )

    # -- 恢复核对 / 销账 --

    async def _reconcile_reporting(self, event_id: str, now: float) -> None:
        item = self.outbox.get_event(event_id)
        if item is None or item["report_state"] != "reporting":
            return
        submission = self.tracker.get_latest_submission_by_source_event(event_id)
        delivery = self.tracker.get_latest_delivery_by_source_event(event_id)
        # 上一代尝试的提交／交付不能拿来判新一代（R2）：重试会再次 wake，但同会话
        # 被真人 turn 占槽时 wake 只是排队、暂不产生新 submission，此时 latest 仍是
        # 第一次的旧 suppressed run。若照样判定，这一拍就会耗掉本次预算并 drop 上游，
        # 之后真正重试的 run 发成功也无法正常销账。按认领时记下的水位分代。
        submission, delivery = self._filter_current_generation(item, submission, delivery)
        current = self._current_attempt(delivery, submission)
        run_id = None
        if submission is not None and submission.get("run_id"):
            run_id = submission["run_id"]
        elif current is not None:
            run_id = current["run_id"]
        if run_id:
            self.outbox.attach_run(event_id, run_id)

        if current is not None:
            verdict = self._delivery_verdict(current["run_id"])
            if verdict == "sent":
                await self._ack(item, current["run_id"], now)
                return
            if verdict == "terminal_unsent":
                await self._retry_or_fail(
                    item, now, self._unsendable_reason(current),
                )
                return
            if self._lease_expired(item, now):
                await self.reconcile_run(current["run_id"])
                refreshed = self.tracker.get_delivery(current["run_id"])
                if refreshed is None:
                    return
                verdict = self._delivery_verdict(current["run_id"])
                if verdict == "sent":
                    await self._ack(item, refreshed["run_id"], now)
                elif verdict == "terminal_unsent":
                    await self._retry_or_fail(item, now, self._unsendable_reason(refreshed))
                elif self._stream_abandoned(refreshed, current["run_id"]):
                    # run 已不在活跃表里、计划却停在 streaming：产出方没有走到
                    # 终态落账（进程中断／网关丢 run）。不销账（前缀不是完成），
                    # 也不让它永久 reporting 头阻塞——按不可发走有界重试。
                    await self._retry_or_fail(item, now, "unsendable:stream_abandoned:partial")
            return

        if submission is not None:
            if submission.get("run_id"):
                if self._lease_expired(item, now):
                    await self.reconcile_run(submission["run_id"])
                return
            if self._attempt_abandoned(item, now):
                # 提交意图已落盘却始终没绑上 run（HTTP 失败／进程中断）：这一代
                # 没有可对账对象。超过唤醒 turn 上限才认作残留，释放租约让下一拍
                # 重来，不把它当模型失败消耗预算，也不永久占住会话闸。
                self.outbox.release_claim(
                    event_id, now=now, delay_seconds=self.retry_delay_seconds,
                    error="submission_unbound",
                )
            return
        if self._lease_expired(item, now):
            # 唤起曾被接受但没有形成 run_submissions：多为排队丢弃或进程中断。
            # 不允许它吞掉一次模型生成预算，也不换任务身份。
            self.outbox.release_claim(event_id, now=now, error="no_submission")

    def _submission_watermark(self, event_id: str) -> int:
        """认领时的提交水位：该事件已有的最大 submission_seq，没有提交行为 0。

        读不到台账时抛 ``WatermarkUnavailable``（代际未知），由调用方 fail closed。
        绝不把读失败降级成 None 继续认领：NULL 在 ``_filter_current_generation``
        里是「升级前已 claim 的历史行」的兼容标记，新尝试以 NULL 认领会跳过代际
        过滤，用上一代已 suppressed 的交付判死本次尝试。
        """
        try:
            latest = self.tracker.get_latest_submission_by_source_event(event_id)
        except Exception as exc:
            raise WatermarkUnavailable(f"watermark read failed: {event_id}") from exc
        if latest is None:
            return 0
        seq = latest.get("submission_seq")
        if seq is None:
            # 有提交行却读不出序号＝代际同样未知；不当作 0（0 会让旧正序号通过）。
            raise WatermarkUnavailable(f"submission_seq missing: {event_id}")
        return int(seq)

    @staticmethod
    def _filter_current_generation(
        item: dict, submission: dict | None, delivery: dict | None,
    ) -> tuple[dict | None, dict | None]:
        """只留「本次尝试」的提交与交付；水位之前的记录属于上一代。

        列上的 NULL 只可能来自升级前已 claim 的历史行或人工夹具：那时还没有
        水位列。消费器自身**绝不**以 NULL 认领——水位读不到时 fail closed
        （见 ``WatermarkUnavailable``），所以升级后的新尝试一定带水位，不会
        借这条兼容分支绕过代际过滤。水位存在时，seq 不大于水位的提交／交付
        一律丢弃——这正是「wake 已接受但还在 inflight 队列里、尚无新
        submission」的窗口。
        """
        baseline = item.get("attempt_baseline_seq")
        if baseline is None:
            return submission, delivery
        water = int(baseline)
        if submission is not None and int(submission.get("submission_seq") or 0) <= water:
            submission = None
        if delivery is not None and int(delivery.get("submission_seq") or 0) <= water:
            delivery = None
        return submission, delivery

    @staticmethod
    def _current_attempt(delivery: dict | None, submission: dict | None) -> dict | None:
        """最新交付是否属于「当前这次尝试」，不属于则返回 None。

        身份以最新 submission 为准（同一事件的重试会新增更大的 submission_seq）：
        同 run 或提交序不早于最新提交才算当前；无最新 submission 时退回只看交付
        （旧库／人工补账没有提交行）。
        """
        if delivery is None:
            return None
        if submission is None:
            return delivery
        run_id = submission.get("run_id")
        if run_id is not None and delivery.get("run_id") == run_id:
            return delivery
        delivery_seq = delivery.get("submission_seq")
        submission_seq = submission.get("submission_seq")
        if delivery_seq is None or submission_seq is None:
            return None
        return delivery if int(delivery_seq) >= int(submission_seq) else None

    async def _ack(self, item: dict, run_id: str, now: float) -> None:
        event_id = item["event_id"]
        if self._source(item) in _UPSTREAM_SOURCES:
            await self.gateway.complete(event_id, run_id)
        self.outbox.mark_acked(event_id, now=now)

    async def _retry_or_fail(self, item: dict, now: float, reason: str) -> None:
        event_id = item["event_id"]
        if int(item["attempt_count"]) < 2:
            self.outbox.schedule_retry(
                event_id, error=reason, now=now,
                delay_seconds=self.retry_delay_seconds,
            )
            logger.warning(
                "[background-report] 汇报未送达,按重试排程 event=%s reason=%s", event_id, reason,
            )
            return
        if self._source(item) in _UPSTREAM_SOURCES:
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

    # -- 判定 --

    def _delivery_verdict(self, run_id: str) -> str:
        """交付台账的实发结论：sent / pending / terminal_unsent。

        只看组件事实（primary 是否真的 sent、其余组件是否已结算），期望状态
        只用来判断「还会不会再发」：``ready`` 仍可对账补发，业务终态与
        ``terminal`` 不会。这样已发完且已终态的交付才能正常销账。

        计划未定稿（``streaming``／``waiting``）时不能判 sent：流式即时决策
        已落的只是前缀，工具或后续决定还会追加份，提前销账会放行同会话下一
        条事件，和仍在产出正文的 run 并发写同一会话。
        """
        delivery = self.tracker.get_delivery(run_id)
        if delivery is None:
            return "pending"
        expected_state = str(delivery["expected_state"])
        components = self.tracker.list_components(run_id)
        primary = next((c for c in components if _is_primary(c["component_id"])), None)
        sent = (
            expected_state not in _PLAN_OPEN_STATES
            and primary is not None and primary["state"] == "sent"
            and all(c["state"] in _SETTLED_COMPONENTS for c in components)
        )
        if sent:
            return "sent"
        if expected_state in _UNSENDABLE_STATES:
            return "terminal_unsent"
        return "pending"

    @staticmethod
    def _unsendable_reason(delivery: dict) -> str:
        state = delivery.get("expected_state") or "unknown"
        reason = delivery.get("cancel_reason") or ""
        return f"unsendable:{state}" + (f":{reason}" if reason else "")

    @staticmethod
    def _lease_expired(item: dict, now: float) -> bool:
        expires = item.get("lease_expires_at")
        return expires is None or float(expires) <= now

    def _stream_abandoned(self, delivery: dict, run_id: str) -> bool:
        """计划还在流式打开，但产出它的 run 已不在活跃表（不会再追加份）。"""
        if str(delivery.get("expected_state")) not in _PLAN_OPEN_STATES:
            return False
        try:
            return self.tracker.get_by_run_id(run_id) is None
        except Exception:
            return False

    @staticmethod
    def _attempt_abandoned(item: dict, now: float) -> bool:
        """提交意图从未绑上 run，且已超过一次唤醒 turn 的时限。

        只有这种「没有 run 可对账」的情况才释放租约；有 run 的尝试按它自己的
        交付／提交状态对账，不看这个时限。
        """
        expires = item.get("lease_expires_at")
        if expires is None:
            return False
        return float(expires) + SESSION_SERIAL_GRACE_SECONDS <= now

    @staticmethod
    def _source(item: dict) -> str:
        return str(item.get("source") or SOURCE_SUBAGENT)

    @classmethod
    def _prompt(cls, item: dict) -> str:
        result = str(item.get("result") or "（任务未提供结果正文）")
        if len(result) > 12000:
            result = result[:12000] + "\n……结果过长，已截断。"
        if cls._source(item) == SOURCE_KANBAN:
            return (
                "[系统内部通知·Kanban]\n"
                "看板上一张卡片的终态事件需要你处理。请用当前会话的自然口吻向用户简报：\n"
                "卡片做了什么、结果如何、接下来该谁做什么；若是需人工决策的受阻，说清选项。\n"
                "直接给最终答复，不要沉默、不要只回表情、不要继续派后台任务；\n"
                "不要提及事件号、数据库、网关、内部路径或投递机制，不要泄露凭据。\n"
                f"事件类型：{item.get('task_state') or 'unknown'}\n"
                f"事件内容：\n{result}"
            )
        return (
            "[后台任务完成汇报]\n"
            "一个后台子代理任务已经结束。请根据下面的任务结果，用当前会话的自然口吻"
            "直接向用户汇报有用结论；失败则说明失败原因和可行下一步。\n"
            "本轮必须给出最终答复，不要沉默、不要只回表情、不要继续派后台任务，"
            "不要提及事件号、数据库、网关、内部路径或投递机制，不要泄露凭据。\n"
            f"任务状态：{item.get('task_state') or 'unknown'}\n"
            f"任务结果：\n{result}"
        )
