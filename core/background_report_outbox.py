"""后台任务完成事件的持久汇报信箱（本部署补丁，alive/92）。

任务结果仍由上游 durable 记录保管；这里只保存本次外部汇报所需的
事件快照、消费状态和跨层身份，不用转录水位推断完成事实。
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


_SCHEMA = """
CREATE TABLE IF NOT EXISTS background_report_events (
    event_id          TEXT PRIMARY KEY,
    origin_session_id TEXT NOT NULL,
    task_state        TEXT NOT NULL,
    result            TEXT NOT NULL,
    dispatched_at     REAL,
    completed_at      REAL,
    delivery_state    TEXT,
    claim_id          TEXT,
    source            TEXT NOT NULL DEFAULT 'subagent',
    report_state      TEXT NOT NULL DEFAULT 'pending',
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   REAL NOT NULL DEFAULT 0,
    run_id            TEXT,
    idempotency_key   TEXT,
    lease_expires_at  REAL,
    attempt_baseline_seq INTEGER,
    last_error        TEXT,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_background_report_due
    ON background_report_events(report_state, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_background_report_session
    ON background_report_events(origin_session_id, created_at);
CREATE TABLE IF NOT EXISTS background_report_cancelled_sessions (
    origin_session_id TEXT PRIMARY KEY,
    drop_confirmed INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS background_report_source_cursors (
    source     TEXT PRIMARY KEY,
    cursor     TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""

# 事件来源：subagent 走上游 background-completion 认领／ack；kanban 是插件侧
# 自己从看板事件登记的内部通知，上游没有对应认领对象（销账只在本地台账）。
SOURCE_SUBAGENT = "subagent"
SOURCE_KANBAN = "kanban"


class BackgroundReportOutbox:
    """按任务事件号幂等持久化，连接可跨工作线程使用。"""

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.executescript(_SCHEMA)
        self._add_missing_columns()
        self._conn.commit()

    def _add_missing_columns(self) -> None:
        """旧库自动补列(幂等),与 P-04 同一做法:升级库与新建库同形。"""
        have = {row[1] for row in self._conn.execute("PRAGMA table_info(background_report_events)")}
        if "source" not in have:
            self._conn.execute(
                "ALTER TABLE background_report_events"
                " ADD COLUMN source TEXT NOT NULL DEFAULT 'subagent'"
            )
        if "attempt_baseline_seq" not in have:
            # R2：本次尝试的提交代际水位（认领那一刻已有的最大 submission_seq）。
            # 旧库补 NULL＝代际未知，消费器沿用旧语义，不误伤历史在飞行。
            self._conn.execute(
                "ALTER TABLE background_report_events"
                " ADD COLUMN attempt_baseline_seq INTEGER"
            )


    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def upsert_event(self, event: dict[str, Any]) -> bool:
        """首次发现返回 True；重复发现不得重置汇报进度。"""
        now = time.time()
        with self._lock:
            cancelled = self._conn.execute(
                "SELECT 1 FROM background_report_cancelled_sessions WHERE origin_session_id=?",
                (event["origin_session_id"],),
            ).fetchone()
            if cancelled is not None:
                return False
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO background_report_events "
                "(event_id, origin_session_id, task_state, result, dispatched_at,"
                " completed_at, delivery_state, claim_id, source, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event["event_id"],
                    event["origin_session_id"],
                    event["state"],
                    str(event.get("result") or ""),
                    event.get("dispatched_at"),
                    event.get("completed_at"),
                    event.get("delivery_state"),
                    event.get("claim_id"),
                    event.get("source") or SOURCE_SUBAGENT,
                    now,
                    now,
                ),
            )
            self._conn.commit()
            return cur.rowcount == 1


    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM background_report_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def due_events(self, *, now: float, limit: int = 50) -> list[dict[str, Any]]:
        """到期事件按落盘先后排列；有效汇报租约不参与重跑。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM background_report_events"
                " WHERE ((report_state IN ('pending', 'retry_wait') AND next_attempt_at<=?)"
                " OR (report_state='reporting' AND lease_expires_at<=?))"
                " ORDER BY created_at, event_id LIMIT ?",
                (now, now, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def claim_event(
        self, event_id: str, *, idempotency_key: str, now: float, lease_seconds: float,
        baseline_seq: int | None = None,
    ) -> dict[str, Any] | None:
        """原子认领事件；崩溃接管保留原提交键和已消耗尝试次数。

        ``baseline_seq`` 是本次尝试的提交代际水位：认领这一刻该事件已有的最大
        ``run_submissions.submission_seq``（没有提交行时为 0）。重试会再次 wake，
        但同一会话被真人 turn 占槽时 wake 只是排队、暂不产生新 submission；只有
        超过这个水位的提交才算**本次**尝试的产物，否则会把上一代已 suppressed 的
        交付当成本次结果，提前耗尽预算并 drop 上游。传 None 表示代际未知（旧库
        ／人工夹具），消费器沿用旧语义。
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT * FROM background_report_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is None:
                self._conn.rollback()
                return None
            fresh = row["report_state"] in ("pending", "retry_wait") and row["next_attempt_at"] <= now
            reclaim = row["report_state"] == "reporting" and row["lease_expires_at"] <= now
            if not (fresh or reclaim):
                self._conn.commit()
                return None
            if fresh:
                self._conn.execute(
                    "UPDATE background_report_events SET report_state='reporting',"
                    " attempt_count=attempt_count+1, idempotency_key=?, lease_expires_at=?,"
                    " attempt_baseline_seq=?, run_id=NULL, last_error=NULL, updated_at=?"
                    " WHERE event_id=?",
                    (idempotency_key, now + lease_seconds, baseline_seq, now, event_id),
                )
            else:
                self._conn.execute(
                    "UPDATE background_report_events SET lease_expires_at=?, updated_at=?"
                    " WHERE event_id=?",
                    (now + lease_seconds, now, event_id),
                )
            claimed = self._conn.execute(
                "SELECT * FROM background_report_events WHERE event_id=?", (event_id,)
            ).fetchone()
            self._conn.commit()
            return dict(claimed)

    def schedule_retry(
        self, event_id: str, *, error: str, now: float, delay_seconds: float
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE background_report_events SET report_state='retry_wait',"
                " next_attempt_at=?, lease_expires_at=NULL, last_error=?, updated_at=?"
                " WHERE event_id=? AND report_state='reporting'",
                (now + delay_seconds, error, now, event_id),
            )
            self._conn.commit()

    def mark_report_failed(self, event_id: str, *, error: str, now: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE background_report_events SET report_state='report_failed',"
                " lease_expires_at=NULL, last_error=?, updated_at=?"
                " WHERE event_id=? AND report_state='reporting'",
                (error, now, event_id),
            )
            self._conn.commit()

    def attach_run(self, event_id: str, run_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE background_report_events SET run_id=?, updated_at=?"
                " WHERE event_id=? AND report_state='reporting'",
                (run_id, time.time(), event_id),
            )
            self._conn.commit()

    def mark_acked(self, event_id: str, *, now: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE background_report_events SET report_state='acked',"
                " lease_expires_at=NULL, last_error=NULL, updated_at=?"
                " WHERE event_id=? AND report_state='reporting'",
                (now, event_id),
            )
            self._conn.commit()

    def active_events(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id FROM background_report_events"
                " WHERE report_state IN ('pending','reporting','retry_wait')"
                " ORDER BY created_at,event_id"
            ).fetchall()
            return [item for row in rows if (item := self.get_event(row[0])) is not None]

    def has_reporting_for_session(self, origin_session_id: str, *, exclude_event_id: str | None = None,
                                  now: float | None = None) -> bool:
        """同会话是否已有**有效租约内**的汇报在跑。

        过期的 reporting 行不能继续充当门:它的租约已经失效,下一拍会被
        恢复核对接管(可能重试、也可能落终态)。若把过期行也算进这里,一次
        崩溃留下的 reporting 会永久堵住该会话之后所有来源的事件(子代理与
        Kanban 都收不到),这正是提醒链的实际故障形态。
        """
        at = time.time() if now is None else float(now)
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM background_report_events"
                " WHERE origin_session_id=? AND report_state='reporting' AND event_id<>?"
                " AND lease_expires_at IS NOT NULL AND lease_expires_at>?"
                " LIMIT 1",
                (origin_session_id, exclude_event_id or "", at),
            ).fetchone()
            return row is not None

    def get_source_cursor(self, source: str) -> str | None:
        """事件来源的持久消费水位（Kanban 板事件号）；重启装回，离线不丢。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT cursor FROM background_report_source_cursors WHERE source=?",
                (source,),
            ).fetchone()
            return None if row is None else str(row[0])

    def put_source_cursor(self, source: str, cursor: str | int) -> None:
        """只在事件已持久登记之后推进水位：重复登记是幂等的，不会重放唤醒。"""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO background_report_source_cursors(source,cursor,updated_at)"
                " VALUES (?,?,?) ON CONFLICT(source) DO UPDATE SET"
                " cursor=excluded.cursor, updated_at=excluded.updated_at",
                (source, str(cursor), time.time()),
            )


    def release_claim(self, event_id: str, *, now: float, delay_seconds: float = 0, error: str | None = None) -> None:
        """未形成模型提交时释放租约；基础设施等待不消耗生成预算。"""
        state = "retry_wait" if delay_seconds > 0 else "pending"
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE background_report_events SET report_state=?,attempt_count=MAX(0,attempt_count-1),"
                " lease_expires_at=NULL,next_attempt_at=?,last_error=?,updated_at=?"
                " WHERE event_id=? AND report_state='reporting'",
                (state, now + delay_seconds, error, now, event_id),
            )

    def cancel_session(self, origin_session_id: str, *, now: float) -> list[str]:
        """显式清场取消旧会话未确认汇报，并返回需终止上游认领的事件号。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            rows = self._conn.execute(
                "SELECT event_id FROM background_report_events"
                " WHERE origin_session_id=?"
                " AND report_state IN ('pending', 'retry_wait', 'reporting')"
                " ORDER BY created_at, event_id",
                (origin_session_id,),
            ).fetchall()
            event_ids = [row["event_id"] for row in rows]
            if event_ids:
                placeholders = ",".join("?" for _ in event_ids)
                self._conn.execute(
                    "UPDATE background_report_events SET report_state='cancelled',"
                    " lease_expires_at=NULL, last_error='session_cleared', updated_at=?"
                    f" WHERE event_id IN ({placeholders})",
                    (now, *event_ids),
                )
            self._conn.execute(
                "INSERT OR IGNORE INTO background_report_cancelled_sessions(origin_session_id,created_at)"
                " VALUES (?,?)",
                (origin_session_id, now),
            )
            self._conn.commit()
            return event_ids

    def pending_session_cancels(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT origin_session_id FROM background_report_cancelled_sessions"
                " WHERE drop_confirmed=0 ORDER BY created_at"
            ).fetchall()
            return [row[0] for row in rows]

    def finish_session_cancel(self, origin_session_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE background_report_cancelled_sessions SET drop_confirmed=1"
                " WHERE origin_session_id=?",
                (origin_session_id,),
            )
