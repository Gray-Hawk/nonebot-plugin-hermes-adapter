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

    def close(self) -> None:
        self._conn.close()
