"""Runs Companion 消费端：有序事件、分组件交付事实与 ack 恢复。"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .hermes_client import _try_parse_first_json_block, extract_response_media


@dataclass(frozen=True, slots=True)
class CompanionEvent:
    stream_id: str
    event_seq: int
    event_id: str
    session_id: str
    turn_id: str
    assistant_response: str
    response_sha256: str
    captured_at: int
    generation: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CompanionEvent":
        item = cls(**{key: payload[key] for key in cls.__dataclass_fields__})
        if (
            not item.stream_id
            or not item.event_id
            or not item.session_id
            or not item.turn_id
            or type(item.event_seq) is not int
            or item.event_seq < 1
            or type(item.generation) is not int
            or item.generation < 1
            or not isinstance(item.assistant_response, str)
        ):
            raise ValueError("invalid companion event")
        return item


@dataclass(frozen=True, slots=True)
class ComponentResult:
    state: str
    receipt: dict[str, Any] | None = None

    def __post_init__(self):
        if self.state not in {"sent", "failed", "unknown"}:
            raise ValueError("invalid component result state")


@dataclass(frozen=True, slots=True)
class DeliveryPlan:
    event: CompanionEvent
    decision: dict[str, Any]
    cleaned_text: str
    media_urls: tuple[str, ...] = ()
    file_paths: tuple[str, ...] = ()


class CompanionLedger:
    """消费侧独立 SQLite；主循环单线程使用，提交后才允许写历史或 ack。"""

    def __init__(self, db_path: Path):
        self._conn = sqlite3.connect(str(Path(db_path)))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS streams (
                stream_id TEXT PRIMARY KEY,
                generation INTEGER NOT NULL,
                next_seq INTEGER NOT NULL,
                ack_seq INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                stream_id TEXT NOT NULL,
                event_seq INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                response_sha256 TEXT NOT NULL,
                generation INTEGER NOT NULL,
                raw_response TEXT NOT NULL,
                decision_json TEXT NOT NULL,
                cleaned_text TEXT NOT NULL,
                media_json TEXT NOT NULL,
                file_json TEXT NOT NULL,
                text_state TEXT NOT NULL,
                media_state TEXT NOT NULL,
                file_state TEXT NOT NULL,
                history_state TEXT NOT NULL,
                feedback_state TEXT NOT NULL,
                ack_state TEXT NOT NULL,
                receipts_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL,
                UNIQUE(stream_id, event_seq),
                FOREIGN KEY(stream_id) REFERENCES streams(stream_id)
            );
            """
        )
        columns = {
            row["name"] for row in self._conn.execute("PRAGMA table_info(events)")
        }
        if "feedback_state" not in columns:
            # 老事件已经走完旧投递链，不补触发历史反馈。
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN feedback_state TEXT NOT NULL DEFAULT 'not_required'"
            )
        self._conn.commit()

    def open_stream(self, stream_id: str, generation: int) -> None:
        row = self._conn.execute(
            "SELECT generation FROM streams WHERE stream_id=?", (stream_id,)
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO streams(stream_id,generation,next_seq,ack_seq) VALUES(?,?,1,0)",
                (stream_id, generation),
            )
        elif row["generation"] != generation:
            raise ValueError("companion generation mismatch")
        self._conn.commit()

    def ack_seq(self, stream_id: str) -> int:
        row = self._conn.execute(
            "SELECT ack_seq FROM streams WHERE stream_id=?", (stream_id,)
        ).fetchone()
        return 0 if row is None else int(row["ack_seq"])

    def begin(self, plan: DeliveryPlan) -> None:
        item = plan.event
        decision = plan.decision
        visible = bool(decision.get("should_reply"))
        cleaned, media, files = plan.cleaned_text, plan.media_urls, plan.file_paths
        states = {
            "text": "silent" if not visible else ("pending" if cleaned else "not_required"),
            "media": "silent" if not visible else ("pending" if media else "not_required"),
            "file": "silent" if not visible else ("pending" if files else "not_required"),
            "history": "pending" if visible else "not_required",
            "feedback": "pending" if visible and cleaned else "not_required",
        }
        with self._conn:
            expected = self._conn.execute(
                "SELECT next_seq FROM streams WHERE stream_id=?", (item.stream_id,)
            ).fetchone()["next_seq"]
            existing_seq = self._conn.execute(
                "SELECT event_id FROM events WHERE stream_id=? AND event_seq=?",
                (item.stream_id, item.event_seq),
            ).fetchone()
            if existing_seq is not None and existing_seq["event_id"] != item.event_id:
                raise ValueError("companion sequence identity conflict")
            if item.event_seq != expected and existing_seq is None:
                raise ValueError("companion event is outside the contiguous prefix")
            existing = self._conn.execute(
                "SELECT * FROM events WHERE event_id=?", (item.event_id,)
            ).fetchone()
            if existing is not None:
                identity = (
                    existing["stream_id"], existing["event_seq"], existing["session_id"],
                    existing["turn_id"], existing["response_sha256"], existing["generation"],
                    existing["raw_response"],
                )
                incoming = (
                    item.stream_id, item.event_seq, item.session_id, item.turn_id,
                    item.response_sha256, item.generation, item.assistant_response,
                )
                if identity != incoming:
                    raise ValueError("companion event identity conflict")
                return
            self._conn.execute(
                """
                INSERT INTO events(
                    event_id,stream_id,event_seq,session_id,turn_id,response_sha256,
                    generation,raw_response,decision_json,cleaned_text,media_json,file_json,
                    text_state,media_state,file_state,history_state,feedback_state,ack_state,
                    receipts_json,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending','{}',?)
                """,
                (
                    item.event_id, item.stream_id, item.event_seq, item.session_id,
                    item.turn_id, item.response_sha256, item.generation,
                    item.assistant_response, json.dumps(decision, ensure_ascii=False),
                    cleaned, json.dumps(list(media)), json.dumps(list(files)),
                    states["text"], states["media"], states["file"], states["history"],
                    states["feedback"], time.time(),
                ),
            )

    def row(self, event_id: str) -> sqlite3.Row:
        row = self._conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
        if row is None:
            raise KeyError("unknown companion event")
        return row

    def component_state(self, event_id: str, component: str) -> str:
        if component not in {"text", "media", "file", "history", "feedback", "ack"}:
            raise KeyError(component)
        return str(self.row(event_id)[f"{component}_state"])

    def mark_component(
        self, event_id: str, component: str, state: str,
        receipt: dict[str, Any] | None = None, error: str = "",
    ) -> None:
        if component not in {"text", "media", "file", "history", "feedback"}:
            raise KeyError(component)
        with self._conn:
            row = self.row(event_id)
            receipts = json.loads(row["receipts_json"])
            if receipt is not None:
                receipts[component] = receipt
            self._conn.execute(
                f"UPDATE events SET {component}_state=?,receipts_json=?,attempts=attempts+1,"
                "last_error=?,updated_at=? WHERE event_id=?",
                (state, json.dumps(receipts), error, time.time(), event_id),
            )

    def visible_complete(self, event_id: str) -> bool:
        row = self.row(event_id)
        return all(
            row[f"{component}_state"] in {"sent", "silent", "not_required"}
            for component in ("text", "media", "file")
        )

    def mark_acked(self, item: CompanionEvent) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE events SET ack_state='sent',updated_at=? WHERE event_id=?",
                (time.time(), item.event_id),
            )
            self._conn.execute(
                "UPDATE streams SET next_seq=?,ack_seq=? WHERE stream_id=?",
                (item.event_seq + 1, item.event_seq, item.stream_id),
            )

    def has_blocking_event(self, stream_id: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM events
            WHERE stream_id=? AND ack_state='pending' AND (
                text_state IN ('failed','unknown')
                OR media_state IN ('failed','unknown')
                OR file_state IN ('failed','unknown')
                OR history_state='failed'
                OR feedback_state='failed'
            ) LIMIT 1
            """,
            (stream_id,),
        ).fetchone()
        return row is not None

    def close(self) -> None:
        self._conn.close()


class CompanionConsumer:
    def __init__(self, ledger: CompanionLedger, *, stream_id: str, generation: int,
                 sender, history, ack, feedback=None):
        self.ledger = ledger
        self.stream_id = stream_id
        self.generation = generation
        self.sender = sender
        self.history = history
        self.ack = ack
        self.feedback = feedback
        self.ledger.open_stream(stream_id, generation)

    def _plan(self, item: CompanionEvent) -> DeliveryPlan:
        decision = _try_parse_first_json_block(item.assistant_response)
        if not isinstance(decision, dict) or not isinstance(decision.get("should_reply"), bool):
            raise ValueError("invalid submit_decision response")
        reply_text = decision.get("reply_text", "")
        if decision["should_reply"] and (not isinstance(reply_text, str) or not reply_text.strip()):
            raise ValueError("visible decision has no reply_text")
        cleaned, media, files = extract_response_media(reply_text if isinstance(reply_text, str) else "")
        return DeliveryPlan(item, decision, cleaned, tuple(media), tuple(files))

    async def process(self, payload: dict[str, Any]) -> bool:
        try:
            item = CompanionEvent.from_dict(payload)
            if item.stream_id != self.stream_id or item.generation != self.generation:
                return False
            plan = self._plan(item)
            self.ledger.begin(plan)
        except (KeyError, TypeError, ValueError):
            return False
        row = self.ledger.row(item.event_id)
        if row["ack_state"] == "sent":
            return True
        if any(row[f"{c}_state"] == "unknown" for c in ("text", "media", "file")):
            return False
        for component in ("text", "media", "file"):
            state = self.ledger.component_state(item.event_id, component)
            if state in {"sent", "silent", "not_required"}:
                continue
            try:
                result = await self.sender(component, plan)
            except Exception as exc:
                result = ComponentResult("failed")
                result_error = type(exc).__name__
            else:
                result_error = "" if result.state == "sent" else result.state
            self.ledger.mark_component(
                item.event_id, component, result.state, result.receipt, result_error
            )
            if result.state != "sent":
                return False
        if not self.ledger.visible_complete(item.event_id):
            return False
        history_state = self.ledger.component_state(item.event_id, "history")
        if history_state not in {"sent", "not_required"}:
            try:
                history_ok = await self.history(plan)
            except Exception as exc:
                history_ok = False
                history_error = type(exc).__name__
            else:
                history_error = "" if history_ok else "failed"
            self.ledger.mark_component(
                item.event_id, "history", "sent" if history_ok else "failed",
                error=history_error,
            )
            if not history_ok:
                return False
        feedback_state = self.ledger.component_state(item.event_id, "feedback")
        if feedback_state not in {"sent", "not_required"}:
            if self.feedback is None:
                feedback_ok = True
                feedback_error = ""
            else:
                try:
                    feedback_ok = await self.feedback(plan)
                except Exception as exc:
                    feedback_ok = False
                    feedback_error = type(exc).__name__
                else:
                    feedback_error = "" if feedback_ok else "failed"
            self.ledger.mark_component(
                item.event_id, "feedback", "sent" if feedback_ok else "failed",
                error=feedback_error,
            )
            if not feedback_ok:
                return False
        try:
            acked = await self.ack(item)
        except Exception:
            acked = False
        if not acked:
            return False
        self.ledger.mark_acked(item)
        return True


class CompanionHttpClient:
    """受限 Runs Companion API 客户端；secret 只存在于 Authorization 头。"""

    def __init__(self, base_url: str, *, token: str, timeout: float = 10.0, transport=None):
        import httpx

        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
            transport=transport,
        )

    async def pull(self, stream_id: str, *, after_seq: int, limit: int = 50) -> dict[str, Any]:
        response = await self._client.get(
            "/v1/runs-companion/events/pull",
            params={"stream_id": stream_id, "after_seq": after_seq, "limit": limit},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("invalid companion pull response")
        return payload

    async def ack(self, item: CompanionEvent) -> bool:
        response = await self._client.post(
            "/v1/runs-companion/events/ack",
            json={"stream_id": item.stream_id, "event_seq": item.event_seq},
        )
        response.raise_for_status()
        payload = response.json()
        return isinstance(payload, dict) and payload.get("ack_seq") == item.event_seq

    __call__ = ack

    async def close(self) -> None:
        await self._client.aclose()


class CompanionPoller:
    def __init__(self, client: CompanionHttpClient, consumer: CompanionConsumer):
        self.client = client
        self.consumer = consumer

    async def poll_once(self) -> int:
        """拉到本页尾部或首个未确认事件；绝不越过失败继续发送。"""
        processed = 0
        for _ in range(100):
            after_seq = self.consumer.ledger.ack_seq(self.consumer.stream_id)
            payload = await self.client.pull(self.consumer.stream_id, after_seq=after_seq)
            if (
                payload.get("stream_id") != self.consumer.stream_id
                or payload.get("generation") != self.consumer.generation
                or not isinstance(payload.get("events"), list)
            ):
                return processed
            events = payload["events"]
            for raw in events:
                if not isinstance(raw, dict):
                    return processed
                item = dict(raw)
                item["generation"] = payload["generation"]
                if not await self.consumer.process(item):
                    return processed
                processed += 1
            if not payload.get("has_more"):
                return processed
            if not events:
                return processed
        return processed


def run_output_is_authoritative(mode: str) -> bool:
    """legacy 由 run output 交付；companion 只由事件流交付。"""
    if mode not in {"legacy", "companion"}:
        raise ValueError("unknown delivery mode")
    return mode == "legacy"
