"""runs 路在跑 turn 的持久化跟踪(nb 重启 reattach 用)。

本部署补丁(runs 迁移阶段2a):旧路 nb 重启 = 在跑 turn 全灭
(SSE 断连 interrupt,最终回复必丢);runs 的 turn 活在 gateway,插件只需记住
「哪个 scope 有哪个 run 在跑、事件看到第几帧」,重启后 reattach 续传,
把最终回复补送给用户(tasks/run_resume.py)。

last_seq 只在旁白帧与终态帧更新(粗粒度):delta 高频帧不写库。resume 续传时
seq<=last_seq 的旁白视为已转发跳过;终态 output 送达后删行,天然幂等。
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS active_runs (
    run_id      TEXT PRIMARY KEY,
    scope_key   TEXT NOT NULL,
    adapter     TEXT NOT NULL,
    is_private  INTEGER NOT NULL,
    user_id     TEXT NOT NULL,
    group_id    TEXT,
    bot_self_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    mode        TEXT NOT NULL,
    expect_structured INTEGER NOT NULL DEFAULT 0,
    last_seq    INTEGER NOT NULL DEFAULT -1,
    started_at  REAL NOT NULL
)
"""

# 本部署补丁(弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75 根因、alive/84 方案):
# steer 对账台账。每条被上游 steer 接口接受(HTTP 200 且 accepted=True)的注入记
# 一行 pending;run 终态后按 m_id 拉转录精确搜, 搜不到即被静默丢弃, 批 2 补投成新
# run。无 m:id 的 steer 不入账(对账无锚点)。CREATE TABLE IF NOT EXISTS 幂等。
_STEER_LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS steer_ledger (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL,
    m_id        TEXT NOT NULL,          -- 原始消息的 m:id(纯数字串);无 m:id 不入账
    scope_key   TEXT NOT NULL,
    session_key TEXT NOT NULL,
    sender_id   TEXT NOT NULL,
    steer_text  TEXT NOT NULL,          -- 实际注入的完整文本(含发言人标签与约定行)
    accepted_at REAL NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'  -- pending/reconciled/recovered/replayed
)
"""


@dataclass
class TrackedRun:
    run_id: str
    scope_key: str
    adapter: str
    is_private: bool
    user_id: str
    group_id: str | None
    bot_self_id: str
    session_key: str
    mode: str
    expect_structured: bool
    last_seq: int
    started_at: float


def make_scope_key(adapter: str, is_private: bool, user_id: str, group_id: str | None) -> str:
    """与 inflight registry 键同形,resume 时并发检查/送达定位直接可用。"""
    scope = "private" if is_private else "group"
    scope_id = user_id if is_private else (group_id or "")
    return f"{adapter}:{scope}:{scope_id}"


class RunTracker:
    """active_runs 表的薄封装。sqlite3 连接线程不安全,本类只在插件主循环用。"""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        # WAL 标配 synchronous=NORMAL。影子镜像库丢最后一批
        # 提交只发生在 OS 掉电级事故(应用崩溃不丢),FULL 的每提交 fsync 不值得。
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(_SCHEMA)
        self._conn.execute(_STEER_LEDGER_SCHEMA)
        self._conn.commit()

    def upsert(
        self,
        *,
        run_id: str,
        scope_key: str,
        adapter: str,
        is_private: bool,
        user_id: str,
        group_id: str | None,
        bot_self_id: str,
        session_key: str,
        mode: str,
        expect_structured: bool,
    ) -> None:
        # 冲突改 ON CONFLICT DO UPDATE 只刷元数据——旧写法
        # INSERT OR REPLACE 全行重置,同一 run_id 二次登记会把 last_seq/started_at
        # 清零(事件流水位丢失,reattach 后旁白从头发一遍)。契约仍是「一 run 一
        # upsert」,此处加固到误复用也不丢水位。
        self._conn.execute(
            "INSERT INTO active_runs "
            "(run_id, scope_key, adapter, is_private, user_id, group_id, bot_self_id,"
            " session_key, mode, expect_structured, last_seq, started_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(run_id) DO UPDATE SET"
            " scope_key=excluded.scope_key, adapter=excluded.adapter,"
            " is_private=excluded.is_private, user_id=excluded.user_id,"
            " group_id=excluded.group_id, bot_self_id=excluded.bot_self_id,"
            " session_key=excluded.session_key, mode=excluded.mode,"
            " expect_structured=excluded.expect_structured",
            (
                run_id, scope_key, adapter, int(is_private), user_id, group_id,
                bot_self_id, session_key, mode, int(expect_structured), -1, time.time(),
            ),
        )
        self._conn.commit()

    def update_seq(self, run_id: str, seq: int) -> None:
        self._conn.execute(
            "UPDATE active_runs SET last_seq=? WHERE run_id=?", (seq, run_id)
        )
        self._conn.commit()

    def finish(self, run_id: str) -> None:
        self._conn.execute("DELETE FROM active_runs WHERE run_id=?", (run_id,))
        self._conn.commit()

    def list_running(self) -> list[TrackedRun]:
        rows = self._conn.execute(
            "SELECT run_id, scope_key, adapter, is_private, user_id, group_id,"
            " bot_self_id, session_key, mode, expect_structured, last_seq, started_at"
            " FROM active_runs ORDER BY started_at"
        ).fetchall()
        return [
            TrackedRun(
                run_id=r[0], scope_key=r[1], adapter=r[2], is_private=bool(r[3]),
                user_id=r[4], group_id=r[5], bot_self_id=r[6], session_key=r[7],
                mode=r[8], expect_structured=bool(r[9]), last_seq=r[10], started_at=r[11],
            )
            for r in rows
        ]

    def get_by_scope(self, scope_key: str) -> TrackedRun | None:
        """按 scope_key 取 live run(steer 注入找目标用);同 scope 多行取最新。"""
        rows = self._conn.execute(
            "SELECT run_id, scope_key, adapter, is_private, user_id, group_id,"
            " bot_self_id, session_key, mode, expect_structured, last_seq, started_at"
            " FROM active_runs WHERE scope_key=? ORDER BY started_at DESC LIMIT 1",
            (scope_key,),
        ).fetchall()
        if not rows:
            return None
        r = rows[0]
        return TrackedRun(
            run_id=r[0], scope_key=r[1], adapter=r[2], is_private=bool(r[3]),
            user_id=r[4], group_id=r[5], bot_self_id=r[6], session_key=r[7],
            mode=r[8], expect_structured=bool(r[9]), last_seq=r[10], started_at=r[11],
        )

    def get_by_run_id(self, run_id: str) -> TrackedRun | None:
        """按主键取一行(run 终态钩子 finish 删行前取回完整行用)。"""
        rows = self._conn.execute(
            "SELECT run_id, scope_key, adapter, is_private, user_id, group_id,"
            " bot_self_id, session_key, mode, expect_structured, last_seq, started_at"
            " FROM active_runs WHERE run_id=?",
            (run_id,),
        ).fetchall()
        if not rows:
            return None
        r = rows[0]
        return TrackedRun(
            run_id=r[0], scope_key=r[1], adapter=r[2], is_private=bool(r[3]),
            user_id=r[4], group_id=r[5], bot_self_id=r[6], session_key=r[7],
            mode=r[8], expect_structured=bool(r[9]), last_seq=r[10], started_at=r[11],
        )

    def prune_zombies(self, max_age_sec: float, *, now: float | None = None) -> list[str]:
        """删 started_at 超过 max_age_sec 的僵尸行,返回被删 run_id 列表。

        僵尸来源:送达窗口超时/reattach 网络失败保留的行,其 run 在 gateway 侧早已
        终态并被状态库遗忘(上游终态只留 1h)——行留着只会每次启动重演失败。
        上限取 24h:单 turn 迭代数有硬顶,真在跑的 run 到不了这个龄期,误伤不到。
        """
        now = time.time() if now is None else now
        cutoff = now - max_age_sec
        rows = self._conn.execute(
            "SELECT run_id FROM active_runs WHERE started_at < ?", (cutoff,)
        ).fetchall()
        ids = [r[0] for r in rows]
        if ids:
            self._conn.execute("DELETE FROM active_runs WHERE started_at < ?", (cutoff,))
            self._conn.commit()
        return ids

    # --- steer 对账台账 CRUD(本部署补丁: 弥补 v0.22.1 引入的 steer 丢弃缺陷, ---
    # alive/75 根因、alive/84 方案)。sqlite 主循环专用, 风格对齐本类其余方法。 ---

    def record_steer(
        self,
        *,
        run_id: str,
        m_id: str,
        scope_key: str,
        session_key: str,
        sender_id: str,
        steer_text: str,
        accepted_at: float,
    ) -> None:
        """INSERT 一行 pending 台账(steer 被上游接受后调用)。

        无 m:id 的 steer 不入账(对账无锚点), 由调用方在开关与 m_id 判定后把关。
        """
        self._conn.execute(
            "INSERT INTO steer_ledger "
            "(run_id, m_id, scope_key, session_key, sender_id, steer_text, accepted_at, status)"
            " VALUES (?,?,?,?,?,?,?,'pending')",
            (run_id, m_id, scope_key, session_key, sender_id, steer_text, accepted_at),
        )
        self._conn.commit()

    def pending_steers(self, run_id: str) -> list[dict]:
        """取该 run 全部 pending 台账条目(终态对账用), dict 含全部列。"""
        rows = self._conn.execute(
            "SELECT id, run_id, m_id, scope_key, session_key, sender_id, steer_text,"
            " accepted_at, status FROM steer_ledger WHERE run_id=? AND status='pending'"
            " ORDER BY id",
            (run_id,),
        ).fetchall()
        return [
            {
                "id": r[0],
                "run_id": r[1],
                "m_id": r[2],
                "scope_key": r[3],
                "session_key": r[4],
                "sender_id": r[5],
                "steer_text": r[6],
                "accepted_at": r[7],
                "status": r[8],
            }
            for r in rows
        ]

    def mark_steers(self, run_id: str, m_ids: list[str], status: str) -> None:
        """按 run_id+m_id 批量置状态(reconciled/recovered/replayed 核销用)。"""
        if not m_ids:
            return
        placeholders = ",".join("?" for _ in m_ids)
        self._conn.execute(
            f"UPDATE steer_ledger SET status=? WHERE run_id=? AND m_id IN ({placeholders})",
            (status, run_id, *m_ids),
        )
        self._conn.commit()

    def recover_all_pending(self, run_id: str) -> int:
        """把该 run 全部 pending 台账条目置 recovered, 返回影响行数。

        核销依据:native 路终态 pending_steer 上浮 = 上游已还回这批 steer, 对账不得
        再补(防双重投递)。按 run_id 全量核销是因回收点拿不到 m:id 列表
        (pending_steer 是拼接文本)。
        """
        cur = self._conn.execute(
            "UPDATE steer_ledger SET status='recovered' WHERE run_id=? AND status='pending'",
            (run_id,),
        )
        self._conn.commit()
        return cur.rowcount

    def recover_by_text(self, recovered_text: str) -> int:
        """按文本子串核销 pending 台账, 返回影响行数。

        回收点(message_turns 两处 pending_steer 回收)拿不到 run_id(ChatResult 不带、
        tracker 行已 finish), 但拿得到 result.pending_steer 拼接文本; 上游 agent.steer()
        的拼接是 existing + "\\n" + cleaned, 台账 steer_text 是单次注入文本, 故
        pending_steer 必然把各条 steer_text 含为子串, 据此反向核销防双重投递。
        """
        cur = self._conn.execute(
            "UPDATE steer_ledger SET status='recovered'"
            " WHERE status='pending' AND ? LIKE '%'||steer_text||'%'",
            (recovered_text,),
        )
        self._conn.commit()
        return cur.rowcount

    def prune_steer_ledger(self, max_age_sec: float, *, now: float | None = None) -> int:
        """删 accepted_at 超龄台账行, 返回删除数。

        挂接点 = tasks/run_resume.py 启动清理处(prune_zombies 旁)。批 1 只提供方法,
        启动清理接线待批 2(对账/补投落地时一并接)。
        """
        now = time.time() if now is None else now
        cutoff = now - max_age_sec
        cur = self._conn.execute(
            "DELETE FROM steer_ledger WHERE accepted_at < ?", (cutoff,)
        )
        self._conn.commit()
        return cur.rowcount

    def close(self) -> None:
        self._conn.close()
