"""Kanban 终态事件的持久来源：把看板事件登记进后台完成汇报信箱（本部署补丁）。

为什么需要这一层
----------------
Kanban 通知器的订阅目标若是 ``api_server``（无状态、由 NB 客户端驱动会话），
它只能把唤醒文本自投内部 ``/v1/chat/completions``：那一轮 turn 确实跑了
（转录里能看到 ``internal_notification`` 的 user 行与助手回复），但调用方是
通知器自己，回应无人接收，NB 侧也没有 ``run_submissions``／``run_deliveries``，
于是 QQ 上什么都收不到。要让提醒落到用户，事件必须由 NB 自己在正常会话里跑
成 turn。

本模块只做「读事件 → 登记」这一步，复用现有后台汇报信箱的持久登记、去重、
唤醒与实发回执，不新盖通知系统，也不改 Hermes 上游核心：

- 数据面：只读打开看板 SQLite（``task_events`` ＋ ``tasks``），沿用上游
  ``hermes kanban watch`` 用的同一份事件表与字段，不做任何写入；
- 身份：事件来源固定为 ``kanban``，唤醒走 NB 的内部合成身份（不伪装真人）；
- 水位：消费位置存在信箱自己的 ``background_report_source_cursors`` 表里，
  NB 停机期间事件留在板上，恢复后从水位继续，重复登记由 ``event_id`` 主键挡住。

只消费「需要助手开口」的终态种类；``status``／``archived``／``unblocked`` 之类
记账事件不唤醒（与通知器的 wake 集合保持一致，避免噪声）。
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

SOURCE = "kanban"

# 需要助手在会话里开口的终态种类（对齐 gateway 通知器的 _WAKE_KINDS）。
WAKE_KINDS: tuple[str, ...] = (
    "completed",
    "blocked",
    "gave_up",
    "crashed",
    "timed_out",
    "review_requested",
    "changes_requested",
    "block_loop_detected",
)

_KIND_LABELS = {
    "completed": "已完成",
    "blocked": "受阻，等人工决策",
    "gave_up": "连续失败后被自动挂起",
    "crashed": "执行进程崩溃",
    "timed_out": "超时终止",
    "review_requested": "已交付，等待评审",
    "changes_requested": "评审要求返修",
    "block_loop_detected": "同一原因反复受阻，已转 triage",
}

# 正文里出现的本机绝对路径不外发（沿用通知器对外的同一取向，宁严勿松）。
_LOCAL_PATH_RE = re.compile(
    r"(?<![\w:/])(?:/(?:Users|home|private|tmp|var|etc|workspace)/[^\s,;，；]+|[A-Za-z]:\\[^\s,;，；]+)"
)
_MAX_TEXT = 1600

# 已经提示过「同一目标还挂着 api_server 自投订阅」的卡片：每卡一次，不刷屏。
_DUPLICATE_WARNED: set[tuple[str, str]] = set()


def _clean(value: Any, limit: int = _MAX_TEXT) -> str:
    text = "" if value is None else str(value)
    text = _LOCAL_PATH_RE.sub("[本地路径]", text)
    text = " ".join(text.split())
    return text[:limit]


def _payload(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _event_body(*, kind: str, task: dict, payload: dict) -> str:
    """事件正文：只放简报所需字段，不放看板内部路径与库标识。"""
    title = _clean(task.get("title"), 200) or task.get("id", "")
    assignee = _clean(task.get("assignee"), 64)
    lines = [f"卡片：{title}（{task.get('id', '')}）", f"状态：{_KIND_LABELS.get(kind, kind)}"]
    if assignee:
        lines.append(f"执行者：@{assignee}")
    summary = payload.get("summary") or payload.get("reason") or payload.get("error") or ""
    if not summary and kind == "completed":
        summary = task.get("result") or ""
    if summary:
        lines.append(f"摘要：{_clean(summary)}")
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, list) and artifacts:
        names = [_clean(item, 120) for item in artifacts if item]
        if names:
            lines.append("产物：" + "、".join(names))
    return "\n".join(lines)


def _routable(session_id: str) -> bool:
    """会话键能否还原成 NB 路由（与信箱 route 解析同一份语法）。"""
    from .background_report_runtime import resolve_background_route

    return bool(session_id) and resolve_background_route(session_id, ()) is not None


class KanbanReportSource:
    """只读的看板事件源；每次 collect 自开连接，不持有跨 tick 状态。"""

    def __init__(
        self,
        db_path: str | Path,
        *,
        kinds: Iterable[str] = WAKE_KINDS,
        limit: int = 200,
    ) -> None:
        self.db_path = str(db_path)
        self.kinds = tuple(kinds)
        self.limit = int(limit)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def collect(self, cursor: str | None) -> tuple[list[dict], str | None]:
        """返回 (待登记事件, 新水位)。

        水位按「已扫描到的最大事件号」推进，与是否产出事件无关：不唤醒的
        事件种类不能占住扫描窗口。首次接入（cursor=None）从**板尾**播种，
        不回放历史终态；播种值本身也作为水位返回，调用方落库后就不再重播。
        任何读取失败都退回 (空, 原水位) 并记一条告警——一根看板读不通不能
        拖垮子代理汇报链。
        """
        if not Path(self.db_path).exists():
            return [], cursor
        try:
            start = int(cursor) if cursor is not None else self._seed()
        except (TypeError, ValueError):
            try:
                start = self._seed()
            except Exception:
                return [], cursor
        try:
            events, next_cursor = self._scan(start)
        except Exception as exc:  # 看板不可读 / 结构缺失：本拍不推进水位
            logger.warning("[background-report] 看板事件读取失败: %s", type(exc).__name__)
            return [], cursor
        return events, next_cursor if next_cursor is not None else str(start)

    def _seed(self) -> int:
        """首次接入：从当前板尾开始，不回放历史终态（避免开机刷一屏旧卡）。"""
        conn = self._connect()
        try:
            row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM task_events").fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def _scan(self, start: int) -> tuple[list[dict], str | None]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT id, task_id, kind, payload, created_at FROM task_events"
                " WHERE id>? ORDER BY id ASC LIMIT ?",
                (start, self.limit),
            ).fetchall()
            if not rows:
                return [], None
            next_cursor = str(int(rows[-1]["id"]))
            wanted = [row for row in rows if str(row["kind"]) in self.kinds]
            if not wanted:
                return [], next_cursor
            tasks = self._tasks(conn, {str(row["task_id"]) for row in wanted})
            events: list[dict] = []
            for row in wanted:
                task = tasks.get(str(row["task_id"])) or self._task(conn, str(row["task_id"]))
                if not task:
                    continue
                session_id = self._session_for(conn, task)
                if not session_id or not _routable(session_id):
                    continue
                self._warn_duplicate_path_once(conn, str(row["task_id"]), session_id)
                payload = _payload(row["payload"])
                events.append({
                    "event_id": f"kanban:{row['task_id']}:{row['id']}",
                    "origin_session_id": session_id,
                    "state": str(row["kind"]),
                    "result": _event_body(kind=str(row["kind"]), task=task, payload=payload),
                    "created_at": row["created_at"],
                    "source": SOURCE,
                })
            return events, next_cursor
        finally:
            conn.close()

    @staticmethod
    def _task(conn: sqlite3.Connection, task_id: str) -> dict:
        row = conn.execute(
            "SELECT id, title, status, assignee, result, session_id FROM tasks WHERE id=?",
            (task_id,),
        ).fetchone()
        return dict(row) if row is not None else {}

    @staticmethod
    def _warn_duplicate_path_once(conn: sqlite3.Connection, task_id: str, session_id: str) -> None:
        """同一目标仍挂着 api_server 唤醒订阅时提示一次：那一路只会自投无回执。

        不改上游订阅（不改生产库），只给运维一个可检索的信号；交接用
        ``scripts/kanban_notify_handover.py`` 按公开 CLI 显式注销。
        """
        if (task_id, session_id) in _DUPLICATE_WARNED:
            return
        try:
            rows = conn.execute(
                "SELECT platform, chat_id FROM kanban_notify_subs WHERE task_id=?",
                (task_id,),
            ).fetchall()
        except sqlite3.Error:
            return
        for row in rows:
            if str(row["platform"] or "").lower() == "api_server" and str(row["chat_id"]) == session_id:
                _DUPLICATE_WARNED.add((task_id, session_id))
                logger.warning(
                    "[background-report] %s 仍挂 api_server 通知订阅,同一事件会有两路:"
                    "通知器自投(无 NB run/delivery)与本信箱唤醒;交接后应注销前者", task_id,
                )
                return

    @staticmethod
    def _tasks(conn: sqlite3.Connection, task_ids: set[str]) -> dict[str, dict]:
        if not task_ids:
            return {}
        placeholders = ",".join("?" for _ in task_ids)
        rows = conn.execute(
            "SELECT id, title, status, assignee, result, session_id FROM tasks"
            f" WHERE id IN ({placeholders})",
            tuple(task_ids),
        ).fetchall()
        return {str(row["id"]): dict(row) for row in rows}

    @staticmethod
    def _session_for(conn: sqlite3.Connection, task: dict) -> str:
        """提醒目标：卡片创建者会话；老卡片没有该列时退回通知订阅里的可路由会话。"""
        session_id = str(task.get("session_id") or "")
        if session_id:
            return session_id
        try:
            rows = conn.execute(
                "SELECT chat_id FROM kanban_notify_subs WHERE task_id=?",
                (task.get("id"),),
            ).fetchall()
        except sqlite3.Error:
            return ""
        for row in rows:
            candidate = str(row["chat_id"] or "")
            if _routable(candidate):
                return candidate
        return ""


def ingest_into(outbox, source: KanbanReportSource | None) -> int:
    """把看板新终态事件登记进持久信箱，返回本次新登记条数。

    水位只在事件写库之后推进；写库失败保持原水位，下一拍重扫——重复登记由
    ``event_id`` 主键挡住，不会重复唤醒。
    """
    if outbox is None or source is None:
        return 0
    cursor = outbox.get_source_cursor(SOURCE)
    events, next_cursor = source.collect(cursor)
    stored = 0
    for event in events:
        if outbox.upsert_event(event):
            stored += 1
    if next_cursor is not None:
        outbox.put_source_cursor(SOURCE, next_cursor)
    return stored


def select_handover_subs(subs: Iterable[dict]) -> list[dict]:
    """从 ``kanban notify-list --json`` 的行里挑出该交接给本信箱的订阅。

    只挑「目标是无状态 api_server、且 chat_id 就是本插件会话键」的行：那一路
    唤醒只能自投 ``/v1/chat/completions``，没有 NB run／delivery。可推送平台的
    订阅（Telegram/Discord/Telegram 等）仍归通知器，不动。
    """
    picked: list[dict] = []
    for sub in subs:
        if not isinstance(sub, dict):
            continue
        if str(sub.get("platform") or "").lower() != "api_server":
            continue
        chat_id = str(sub.get("chat_id") or "")
        if not _routable(chat_id):
            continue
        picked.append(sub)
    return picked
