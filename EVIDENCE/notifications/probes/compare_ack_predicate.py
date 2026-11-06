"""对照取证：现行「已送达」判定 vs 修复后判定，跑在现网真实台账上。

现行判定（``_is_actually_sent``，部署版与基线同形）：
    expected_state == 'ready' 且存在 component_id 字面等于 'primary' 的组件
    → 对 2026-10-30 之后的多份回复命名（reply:<seq>:primary）以及已终态
      （expected_state='terminal'）的报告 run 恒为 False。

修复后判定：primary（含 reply:<seq>:primary）组件 state='sent' 且其余组件已结算，
且交付计划已定稿（``streaming``／``waiting`` 不算完成），不要求 expected_state
仍是 'ready'。

安全边界（R3 返修）：本脚本**不构造 RunTracker**。那个构造器会开可写连接、
执行 DDL/PRAGMA，并提交 ``steer_ledger sending/reconciling→pending``；在现网
台账上用它取证等于写生产库。这里每条连接都是 ``mode=ro`` ＋ ``PRAGMA
query_only=ON``，只把只读视图（``ReadOnlyLedger``）交给真实消费器判定。

用法：python3 EVIDENCE/notifications/probes/compare_ack_predicate.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.background_report_consumer import (  # noqa: E402
    BackgroundReportConsumer,
)

DATA = Path(os.environ.get("LOCALSTORE_DATA_DIR")
            or Path.home() / ".local/share/nonebot2/nonebot_plugin_hermes_adapter")


class ReadOnlyLedger:
    """交付台账只读视图：mode=ro ＋ query_only，不建表、不迁移、不写任何行。"""

    _DELIVERY_COLUMNS = (
        "run_id, submission_seq, idempotency_key, scope_key, session_key, output,"
        " expect_structured, expected_state, plan_json, created_at, completed_at,"
        " updated_at, cancel_reason, adapter, is_private, user_id, group_id,"
        " source_event_id"
    )

    def __init__(self, path: str | Path) -> None:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"台账不存在: {path}")
        self._conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA query_only=ON")
        if int(self._conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise RuntimeError("只读约束未生效，拒绝取证")

    def get_delivery(self, run_id: str) -> dict | None:
        row = self._conn.execute(
            f"SELECT {self._DELIVERY_COLUMNS} FROM run_deliveries WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row["run_id"],
            "submission_seq": row["submission_seq"],
            "idempotency_key": row["idempotency_key"],
            "scope_key": row["scope_key"],
            "session_key": row["session_key"],
            "output": row["output"],
            "expect_structured": bool(row["expect_structured"]),
            "expected_state": row["expected_state"],
            "plan_json": row["plan_json"],
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "updated_at": row["updated_at"],
            "cancel_reason": row["cancel_reason"],
            "adapter": row["adapter"],
            "is_private": bool(row["is_private"]) if row["is_private"] is not None else None,
            "user_id": row["user_id"],
            "group_id": row["group_id"],
            "source_event_id": row["source_event_id"],
        }

    def list_components(self, run_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT component_id, kind, payload_sha256, state, claim_owner, claim_until,"
            " attempt_count, msg_ids_json, last_error, created_at, updated_at, claim_token"
            " FROM delivery_components WHERE run_id=?"
            " ORDER BY CASE component_id WHEN 'primary' THEN 0 ELSE 1 END, component_id",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def report_run_ids(self) -> list[str]:
        rows = self._conn.execute(
            "SELECT run_id FROM run_deliveries WHERE source_event_id IS NOT NULL"
            " ORDER BY created_at"
        ).fetchall()
        return [row[0] for row in rows]

    def close(self) -> None:
        self._conn.close()


def legacy_is_sent(ledger: ReadOnlyLedger, run_id: str) -> bool:
    """现行判定（逐字复刻部署版语义，仅用于对照）。"""
    delivery = ledger.get_delivery(run_id)
    if delivery is None or delivery["expected_state"] != "ready":
        return False
    components = ledger.list_components(run_id)
    primary = next((c for c in components if c["component_id"] == "primary"), None)
    return (
        primary is not None and primary["state"] == "sent"
        and all(c["state"] in {"sent", "not_required"} for c in components)
    )


def main() -> int:
    ledger = ReadOnlyLedger(DATA / "active_runs.db")
    try:
        consumer = BackgroundReportConsumer(
            outbox=None, tracker=ledger, gateway=None,
            resolve_route=None, wake=None, reconcile_run=None,
        )
        run_ids = ledger.report_run_ids()
        olds = [legacy_is_sent(ledger, rid) for rid in run_ids]
        news = [consumer._delivery_verdict(rid) for rid in run_ids]
        print(f"连接: file:{DATA / 'active_runs.db'}?mode=ro（PRAGMA query_only=ON）")
        print(f"现网报告 run：{len(run_ids)} 条（source_event_id 非空）")
        print(f"  现行判定判『已送达』: {sum(olds)}/{len(run_ids)}")
        print(f"  修复后判定判『已送达』: {news.count('sent')}/{len(run_ids)}"
              f"（terminal_unsent={news.count('terminal_unsent')}, pending={news.count('pending')}）")
        mismatched = [rid for rid, old, new in zip(run_ids, olds, news) if not old and new == "sent"]
        print(f"  被现行判定漏掉的已实发 run: {len(mismatched)} 条，例如 {mismatched[:3]}")
    finally:
        ledger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
