"""runs 路在跑 turn 的持久化跟踪(nb 重启 reattach 用)。

本部署补丁(runs 迁移阶段2a):旧路 nb 重启 = 在跑 turn 全灭
(SSE 断连 interrupt,最终回复必丢);runs 的 turn 活在 gateway,插件只需记住
「哪个 scope 有哪个 run 在跑、事件看到第几帧」,重启后 reattach 续传,
把最终回复补送给用户(tasks/run_resume.py)。

last_seq 只在旁白帧与终态帧更新(粗粒度):delta 高频帧不写库。resume 续传时
seq<=last_seq 的旁白视为已转发跳过;终态 output 送达后删行,天然幂等。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import zlib
from dataclasses import dataclass
from pathlib import Path


class StaleGenerationError(RuntimeError):
    """消息入场代际早于 clear 栅栏；原子提交门拒绝旧代开新 run。"""


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
    reply_protocol_version INTEGER NOT NULL DEFAULT 0,
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
    steer_text  TEXT NOT NULL,          -- 实际发送的逐行投影(不含批次约定行)
    accepted_at REAL NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'  -- pending/reconciled/recovered/replayed
)
"""


_RECOVERED_REPLY_LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS recovered_reply_ledger (
    run_id            TEXT NOT NULL,
    transcript_row_id INTEGER NOT NULL,
    delivered_at      REAL NOT NULL,
    PRIMARY KEY (run_id, transcript_row_id)
)
"""

# 本部署补丁(alive/108 半接管提交 A):提交意图与交付事实台账。
# run_submissions 在第一次 POST 前落库,保存原始请求体(zlib 压缩)与同键重放
# 所需的全部身份;崩溃后用原 key＋原请求体 reclaim 原 run,不换 key 新跑。
# run_deliveries 以 run_id 为主身份记录终态 output;active_runs 行会被 finish
# 删除,交付事实独立存这里,正常路与恢复路共用。
# delivery_components 本批只建表(主消息/文件/历史组件),认领与回执在后续批接。
_DELIVERY_LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_submissions (
    submission_seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_gzip    BLOB NOT NULL,
    request_sha256  TEXT NOT NULL,
    target_fingerprint TEXT NOT NULL,
    scope_key       TEXT NOT NULL,
    session_key     TEXT NOT NULL,
    mode            TEXT NOT NULL,
    expect_structured INTEGER NOT NULL DEFAULT 0,
    bot_self_id     TEXT NOT NULL,
    delivery_generation INTEGER,
    source_event_id TEXT,
    run_id          TEXT UNIQUE,
    state           TEXT NOT NULL DEFAULT 'submitted',
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS run_deliveries (
    run_id          TEXT PRIMARY KEY,
    submission_seq  INTEGER UNIQUE,
    idempotency_key TEXT UNIQUE,
    scope_key       TEXT NOT NULL,
    session_key     TEXT NOT NULL,
    output          TEXT NOT NULL,
    expect_structured INTEGER NOT NULL DEFAULT 0,
    expected_state  TEXT NOT NULL DEFAULT 'waiting',
    plan_json       TEXT,
    recovery_json   TEXT,
    reply_protocol_version INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL,
    completed_at    REAL,
    updated_at      REAL NOT NULL,
    cancel_reason   TEXT,
    adapter         TEXT,
    is_private      INTEGER,
    user_id         TEXT,
    group_id        TEXT,
    source_event_id TEXT
);
CREATE TABLE IF NOT EXISTS delivery_components (
    run_id          TEXT NOT NULL,
    component_id    TEXT NOT NULL,
    kind            TEXT NOT NULL,
    payload_sha256  TEXT,
    state           TEXT NOT NULL DEFAULT 'pending',
    claim_owner     TEXT,
    claim_token     TEXT,
    claim_until     REAL,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    msg_ids_json    TEXT,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    PRIMARY KEY (run_id, component_id)
);
CREATE TABLE IF NOT EXISTS delivery_generation_fences (
    scope_key       TEXT PRIMARY KEY,
    fence_seq       INTEGER NOT NULL,
    fence_generation INTEGER,
    updated_at      REAL NOT NULL
)
"""

_ACTIVE_RUN_UPSERT_SQL = (
    "INSERT INTO active_runs "
    "(run_id, scope_key, adapter, is_private, user_id, group_id, bot_self_id,"
    " session_key, mode, expect_structured, reply_protocol_version, last_seq, started_at)"
    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    " ON CONFLICT(run_id) DO UPDATE SET"
    " scope_key=excluded.scope_key, adapter=excluded.adapter,"
    " is_private=excluded.is_private, user_id=excluded.user_id,"
    " group_id=excluded.group_id, bot_self_id=excluded.bot_self_id,"
    " session_key=excluded.session_key, mode=excluded.mode,"
    " expect_structured=excluded.expect_structured,"
    " reply_protocol_version=excluded.reply_protocol_version"
)


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
    reply_protocol_version: int = 0


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
        self._conn.execute(_RECOVERED_REPLY_LEDGER_SCHEMA)
        self._conn.executescript(_DELIVERY_LEDGER_SCHEMA)
        # 提交 C 补投递目标列(恢复发送需要 adapter/私群/收发双方,active_runs
        # 行 finish 后仍要能解析 target);旧库逐列补齐,幂等。
        existing = {
            r[1] for r in self._conn.execute("PRAGMA table_info(run_deliveries)").fetchall()
        }
        for col, ddl in (
            ("adapter", "TEXT"), ("is_private", "INTEGER"),
            ("user_id", "TEXT"), ("group_id", "TEXT"),
            ("recovery_json", "TEXT"),
            ("reply_protocol_version", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if col not in existing:
                self._conn.execute(f"ALTER TABLE run_deliveries ADD COLUMN {col} {ddl}")
        active_cols = {
            r[1] for r in self._conn.execute("PRAGMA table_info(active_runs)")
        }
        if "reply_protocol_version" not in active_cols:
            self._conn.execute(
                "ALTER TABLE active_runs ADD COLUMN"
                " reply_protocol_version INTEGER NOT NULL DEFAULT 0"
            )
        # 每次平台尝试持有独立令牌；旧行 NULL 仅兼容既有直接台账调用。
        component_cols = {
            r[1] for r in self._conn.execute("PRAGMA table_info(delivery_components)")
        }
        if "claim_token" not in component_cols:
            self._conn.execute("ALTER TABLE delivery_components ADD COLUMN claim_token TEXT")
        # 方案2：消息入场代际随 submission 持久化；clear 栅栏同时记录新代。
        # 旧行保持 NULL，继续走 submission_seq 兼容判定。
        for table, col in (
            ("run_submissions", "delivery_generation"),
            ("delivery_generation_fences", "fence_generation"),
        ):
            existing = {r[1] for r in self._conn.execute(f"PRAGMA table_info({table})")}
            if col not in existing:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} INTEGER")
        # 后台汇报事件号只作显式跨层身份；普通消息保持 NULL。
        for table in ("run_submissions", "run_deliveries"):
            existing = {r[1] for r in self._conn.execute(f"PRAGMA table_info({table})")}
            if "source_event_id" not in existing:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN source_event_id TEXT")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_submissions_source_event"
            " ON run_submissions(source_event_id, submission_seq)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_deliveries_source_event"
            " ON run_deliveries(source_event_id)"
        )
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
        reply_protocol_version: int = 0,
    ) -> None:
        # 冲突改 ON CONFLICT DO UPDATE 只刷元数据——旧写法
        # INSERT OR REPLACE 全行重置,同一 run_id 二次登记会把 last_seq/started_at
        # 清零(事件流水位丢失,reattach 后旁白从头发一遍)。契约仍是「一 run 一
        # upsert」,此处加固到误复用也不丢水位。
        self._conn.execute(
            _ACTIVE_RUN_UPSERT_SQL,
            (
                run_id, scope_key, adapter, int(is_private), user_id, group_id,
                bot_self_id, session_key, mode, int(expect_structured),
                int(reply_protocol_version), -1, time.time(),
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
            " bot_self_id, session_key, mode, expect_structured, reply_protocol_version,"
            " last_seq, started_at"
            " FROM active_runs ORDER BY started_at"
        ).fetchall()
        return [
            TrackedRun(
                run_id=r[0], scope_key=r[1], adapter=r[2], is_private=bool(r[3]),
                user_id=r[4], group_id=r[5], bot_self_id=r[6], session_key=r[7],
                mode=r[8], expect_structured=bool(r[9]), reply_protocol_version=int(r[10] or 0),
                last_seq=r[11], started_at=r[12],
            )
            for r in rows
        ]

    def get_by_scope(self, scope_key: str) -> TrackedRun | None:
        """按 scope_key 取 live run(steer 注入找目标用);同 scope 多行取最新。"""
        rows = self._conn.execute(
            "SELECT run_id, scope_key, adapter, is_private, user_id, group_id,"
            " bot_self_id, session_key, mode, expect_structured, reply_protocol_version,"
            " last_seq, started_at"
            " FROM active_runs WHERE scope_key=? ORDER BY started_at DESC LIMIT 1",
            (scope_key,),
        ).fetchall()
        if not rows:
            return None
        r = rows[0]
        return TrackedRun(
            run_id=r[0], scope_key=r[1], adapter=r[2], is_private=bool(r[3]),
            user_id=r[4], group_id=r[5], bot_self_id=r[6], session_key=r[7],
            mode=r[8], expect_structured=bool(r[9]), reply_protocol_version=int(r[10] or 0),
            last_seq=r[11], started_at=r[12],
        )

    def get_by_run_id(self, run_id: str) -> TrackedRun | None:
        """按主键取一行(run 终态钩子 finish 删行前取回完整行用)。"""
        rows = self._conn.execute(
            "SELECT run_id, scope_key, adapter, is_private, user_id, group_id,"
            " bot_self_id, session_key, mode, expect_structured, reply_protocol_version,"
            " last_seq, started_at"
            " FROM active_runs WHERE run_id=?",
            (run_id,),
        ).fetchall()
        if not rows:
            return None
        r = rows[0]
        return TrackedRun(
            run_id=r[0], scope_key=r[1], adapter=r[2], is_private=bool(r[3]),
            user_id=r[4], group_id=r[5], bot_self_id=r[6], session_key=r[7],
            mode=r[8], expect_structured=bool(r[9]), reply_protocol_version=int(r[10] or 0),
            last_seq=r[11], started_at=r[12],
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
        scope_key: str,
        session_key: str,
        accepted_at: float,
        items: list[dict],
    ) -> None:
        """INSERT 逐行 pending 台账(alive/103 §17.9.8:整批一次调用,逐行落行)。

        items=[{m_id, sender_id, steer_text}, ...]——发送/台账/去重同一份
        SteerPresentation 派生,不再有第二条平行 ID 轨。无 m:id 的行不入账
        (对账无锚点),由调用方在投影时过滤。
        """
        for it in items:
            self._conn.execute(
                "INSERT INTO steer_ledger "
                "(run_id, m_id, scope_key, session_key, sender_id, steer_text, accepted_at, status)"
                " VALUES (?,?,?,?,?,?,?,'pending')",
                (run_id, str(it["m_id"]), scope_key, session_key,
                 str(it["sender_id"]), it["steer_text"], accepted_at),
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

    def pending_steers_by_scope(self, scope_key: str) -> list[dict]:
        """按 scope_key 取全部 pending 台账(native 回收归因用,rework-1 F3)。

        native 回收点(run 终态 pending_steer 上浮)拿不到 run_id——ChatResult
        不携带、tracker 行已被 finish 删除——但 ledger 行带 scope_key;按 scope
        拉候选,再由调用方判定 steer_text 是否为本次 pending_steer 拼接文本的
        子串,精确找回这批被回收行的 sender 与逐行正文。返回按台账 id 升序。
        """
        rows = self._conn.execute(
            "SELECT id, run_id, m_id, scope_key, session_key, sender_id, steer_text,"
            " accepted_at, status FROM steer_ledger WHERE scope_key=? AND status='pending'"
            " ORDER BY id",
            (scope_key,),
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

    def claim_recovered_reply(self, run_id: str, transcript_row_id: int) -> bool:
        """原子认领一条前序答复；已送达或正在投递的同一行不得重发。"""
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO recovered_reply_ledger "
            "(run_id, transcript_row_id, delivered_at) VALUES (?,?,?)",
            (run_id, transcript_row_id, time.time()),
        )
        self._conn.commit()
        return cur.rowcount == 1

    def release_recovered_reply(self, run_id: str, transcript_row_id: int) -> None:
        """发送未成功时释放认领，允许断流恢复或下次重试。"""
        self._conn.execute(
            "DELETE FROM recovered_reply_ledger WHERE run_id=? AND transcript_row_id=?",
            (run_id, transcript_row_id),
        )
        self._conn.commit()

    def prune_recovered_reply_ledger(
        self, max_age_sec: float, *, now: float | None = None
    ) -> int:
        """清理只需覆盖 run 恢复窗口的短期去重记录。"""
        now = time.time() if now is None else now
        cur = self._conn.execute(
            "DELETE FROM recovered_reply_ledger WHERE delivered_at < ?",
            (now - max_age_sec,),
        )
        self._conn.commit()
        return cur.rowcount

    # --- 半接管提交与交付台账(本部署补丁, alive/108 提交 A)。 ---

    def record_submission(
        self,
        *,
        idempotency_key: str,
        request_body: dict,
        target_fingerprint: str,
        scope_key: str,
        session_key: str,
        mode: str,
        expect_structured: bool,
        bot_self_id: str,
        delivery_generation: int | None = None,
        source_event_id: str | None = None,
    ) -> int:
        """第一次 POST 前原子核验入场代际并持久化提交意图。

        原始请求体规范化 JSON 后压缩落库——同键重放要求请求指纹一致,重启后
        不能用新 prompt 重建字段冒充同一提交。同键同体重记入幂等(429 重试与
        提交段网络重试会复用同 key);同键不同体说明调用方身份串了,失败关闭。
        新 submission 与 clear 栅栏检查共用 BEGIN IMMEDIATE：消息在 clear 前
        入场、clear 后才提交时，旧代在 HTTP 前失败关闭，不产生新 run。
        request_gzip 可能含图片 data URL,任何日志都不得 dump 该列。
        """
        canonical = json.dumps(
            request_body, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        digest = hashlib.sha256(canonical).hexdigest()
        now = time.time()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT submission_seq, request_sha256 FROM run_submissions WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if row is not None:
                if row[1] != digest:
                    raise ValueError("idempotency_key reused with a different request body")
                self._conn.commit()
                return int(row[0])
            if delivery_generation is not None:
                fence = self._conn.execute(
                    "SELECT fence_generation FROM delivery_generation_fences WHERE scope_key=?",
                    (scope_key,),
                ).fetchone()
                if fence is not None and fence[0] is not None and int(delivery_generation) < int(fence[0]):
                    raise StaleGenerationError(
                        f"stale delivery generation: scope={scope_key} "
                        f"generation={delivery_generation} fence={fence[0]}"
                    )
            cur = self._conn.execute(
                "INSERT INTO run_submissions "
                "(idempotency_key, request_gzip, request_sha256, target_fingerprint,"
                " scope_key, session_key, mode, expect_structured, bot_self_id,"
                " delivery_generation, source_event_id, state, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,'submitted',?,?)",
                (
                    idempotency_key, zlib.compress(canonical), digest, target_fingerprint,
                    scope_key, session_key, mode, int(expect_structured), bot_self_id,
                    delivery_generation, source_event_id, now, now,
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid or 0)
        except Exception:
            self._conn.rollback()
            raise

    def get_submission(self, idempotency_key: str) -> dict | None:
        """按 key 取提交记录(含解压还原的原始请求体),不存在返回 None。"""
        r = self._conn.execute(
            "SELECT submission_seq, idempotency_key, request_gzip, request_sha256,"
            " target_fingerprint, scope_key, session_key, mode, expect_structured,"
            " bot_self_id, run_id, state, created_at, updated_at, delivery_generation,"
            " source_event_id"
            " FROM run_submissions WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if r is None:
            return None
        return {
            "submission_seq": r[0],
            "idempotency_key": r[1],
            "request_body": json.loads(zlib.decompress(r[2]).decode("utf-8")),
            "request_sha256": r[3],
            "target_fingerprint": r[4],
            "scope_key": r[5],
            "session_key": r[6],
            "mode": r[7],
            "expect_structured": bool(r[8]),
            "bot_self_id": r[9],
            "run_id": r[10],
            "state": r[11],
            "created_at": r[12],
            "updated_at": r[13],
            "delivery_generation": r[14],
            "source_event_id": r[15],
        }

    def list_unbound_submissions(self) -> list[dict]:
        """启动 reclaim 候选:已持久化、尚未取得 run_id 且未被 clear 取消。"""
        rows = self._conn.execute(
            "SELECT idempotency_key FROM run_submissions"
            " WHERE run_id IS NULL AND state='submitted' ORDER BY submission_seq"
        ).fetchall()
        return [self.get_submission(r[0]) for r in rows]

    def mark_submission_state(self, idempotency_key: str, state: str) -> None:
        """记录 reclaim 结果(needs_manual/cancelled 等),不改原 key 与原请求体。"""
        self._conn.execute(
            "UPDATE run_submissions SET state=?, updated_at=? WHERE idempotency_key=?",
            (state, time.time(), idempotency_key),
        )
        self._conn.commit()

    def bind_submission_and_register(
        self,
        *,
        idempotency_key: str,
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
        reply_protocol_version: int = 0,
    ) -> None:
        """取得 run_id 后,同一事务完成 submission 绑定＋active_runs 登记。

        两步必须原子:只绑定不登记,重启后 active_runs 恢复链看不到该 run;
        只登记不绑定,崩溃 reclaim 拿不到 run 身份。无 submission 行时失败关闭,
        不允许凭空登记一个无法 reclaim 的 run。
        """
        with self._conn:
            cur = self._conn.execute(
                "UPDATE run_submissions SET run_id=?, state='bound', updated_at=?"
                " WHERE idempotency_key=?",
                (run_id, time.time(), idempotency_key),
            )
            if cur.rowcount != 1:
                raise KeyError(f"no submission for idempotency_key={idempotency_key!r}")
            self._conn.execute(
                _ACTIVE_RUN_UPSERT_SQL,
                (
                    run_id, scope_key, adapter, int(is_private), user_id, group_id,
                    bot_self_id, session_key, mode, int(expect_structured),
                    int(reply_protocol_version), -1, time.time(),
                ),
            )

    def record_expected_delivery(
        self, *, run_id: str, output: str, expect_structured: bool
    ) -> None:
        """终态 output 落交付台账;必须先于 finish 删 active_runs 调用。

        无绑定 submission 的 run 不在半接管保证范围(未入账即无法 reclaim),
        失败关闭由调用方转 detached 保留恢复依据,不伪造交付身份。
        """
        sub = self._conn.execute(
            "SELECT submission_seq, idempotency_key, scope_key, session_key, source_event_id"
            " FROM run_submissions WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if sub is None:
            raise KeyError(f"no bound submission for run_id={run_id!r}")
        now = time.time()
        # 投递目标从 active_runs 复制(finish 删行后恢复发送仍可解析 target)。
        target = self._conn.execute(
            "SELECT adapter, is_private, user_id, group_id, reply_protocol_version"
            " FROM active_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        adapter, is_private, user_id, group_id, protocol_version = (
            target if target else (None, None, None, None, 0)
        )
        self._conn.execute(
            "INSERT INTO run_deliveries "
            "(run_id, submission_seq, idempotency_key, scope_key, session_key,"
            " output, expect_structured, reply_protocol_version, expected_state, created_at, completed_at, updated_at,"
            " adapter, is_private, user_id, group_id, source_event_id)"
            " VALUES (?,?,?,?,?,?,?,?,'ready',?,?,?,?,?,?,?,?)"
            " ON CONFLICT(run_id) DO UPDATE SET"
            " output=excluded.output, expect_structured=excluded.expect_structured,"
            " reply_protocol_version=excluded.reply_protocol_version,"
            " updated_at=excluded.updated_at,"
            " adapter=COALESCE(run_deliveries.adapter, excluded.adapter),"
            " is_private=COALESCE(run_deliveries.is_private, excluded.is_private),"
            " user_id=COALESCE(run_deliveries.user_id, excluded.user_id),"
            " group_id=COALESCE(run_deliveries.group_id, excluded.group_id),"
            " source_event_id=COALESCE(run_deliveries.source_event_id, excluded.source_event_id)",
            (run_id, sub[0], sub[1], sub[2], sub[3], output, int(expect_structured),
             int(protocol_version or 0), now, now, now,
             adapter, is_private, user_id, group_id, sub[4]),
        )
        self._conn.commit()

    def record_recovery_evidence(
        self, *, run_id: str, recoveries: list[dict], protocol_version: int
    ) -> None:
        """保存纯取证结果；计划封存后只允许相同证据重入。"""
        seqs = [int(item["reply_seq"]) for item in recoveries]
        if len(set(seqs)) != len(seqs) or any(seq <= 0 for seq in seqs):
            raise ValueError("invalid recovery reply_seq")
        if recoveries and protocol_version < 1:
            raise ValueError("legacy run must not recover replies")
        row = self._conn.execute(
            "SELECT plan_json FROM run_deliveries WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"no delivery for run_id={run_id!r}")
        if row[0]:
            sealed = json.loads(row[0]).get("recoveries", [])
            if sealed != recoveries:
                raise ValueError(f"recovery evidence changed for run_id={run_id!r}")
        self._conn.execute(
            "UPDATE run_deliveries SET recovery_json=?, reply_protocol_version=?,"
            " updated_at=? WHERE run_id=?",
            (json.dumps(recoveries, sort_keys=True, ensure_ascii=False),
             int(protocol_version), time.time(), run_id),
        )
        self._conn.commit()

    def fence_scope_generation(
        self, scope_key: str, *, reason: str = "generation_cleared",
        fence_generation: int | None = None,
    ) -> dict:
        """clear/T1 换代：记录提交序与 SessionManager 新代际双水位。

        已存在 run 仍按 submission_seq 立即取消；消息先入场、clear 后才提交的
        竞态由 fence_generation 在原子提交门与出站 guard 失败关闭。旧 NULL 行
        继续按序号兼容，上游压缩不换 SessionManager generation，不会误伤。
        已 sent 组件与历史事实保留,只取消未发送部分。
        """
        now = time.time()
        with self._conn:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(submission_seq), 0) FROM run_submissions WHERE scope_key=?",
                (scope_key,),
            ).fetchone()
            previous = self._conn.execute(
                "SELECT fence_seq, fence_generation FROM delivery_generation_fences WHERE scope_key=?",
                (scope_key,),
            ).fetchone()
            fence_seq = max(int(row[0] or 0), int(previous[0]) if previous else 0)
            if fence_generation is not None and previous is not None and previous[1] is not None:
                fence_generation = max(int(fence_generation), int(previous[1]))
            elif fence_generation is None and previous is not None:
                fence_generation = previous[1]
            self._conn.execute(
                "INSERT INTO delivery_generation_fences(scope_key, fence_seq, fence_generation, updated_at)"
                " VALUES(?,?,?,?) ON CONFLICT(scope_key) DO UPDATE SET"
                " fence_seq=excluded.fence_seq, fence_generation=excluded.fence_generation,"
                " updated_at=excluded.updated_at",
                (scope_key, fence_seq, fence_generation, now),
            )
            old_deliveries = (
                "SELECT run_id FROM run_deliveries WHERE scope_key=?"
                " AND submission_seq<=? AND expected_state='ready'"
            )
            component_cur = self._conn.execute(
                "UPDATE delivery_components SET state='cancelled', claim_owner=NULL,"
                " claim_until=NULL, last_error=?, updated_at=?"
                " WHERE state IN ('pending','claimed','failed') AND run_id IN (" + old_deliveries + ")",
                (reason, now, scope_key, fence_seq),
            )
            delivery_cur = self._conn.execute(
                "UPDATE run_deliveries SET expected_state='cancelled', cancel_reason=?, updated_at=?"
                " WHERE scope_key=? AND submission_seq<=? AND expected_state='ready'",
                (reason, now, scope_key, fence_seq),
            )
            submission_cur = self._conn.execute(
                "UPDATE run_submissions SET state='cancelled', updated_at=?"
                " WHERE scope_key=? AND submission_seq<=? AND run_id IS NULL AND state='submitted'",
                (now, scope_key, fence_seq),
            )
        return {
            "fence_seq": fence_seq,
            "deliveries": int(delivery_cur.rowcount or 0),
            "components": int(component_cur.rowcount or 0),
            "submissions": int(submission_cur.rowcount or 0),
        }

    def is_delivery_current(self, run_id: str) -> bool:
        """出站前再核代际：新 token 比代际，旧 NULL 行退回提交序水位。"""
        row = self._conn.execute(
            "SELECT d.submission_seq, s.delivery_generation, f.fence_seq, f.fence_generation"
            " FROM run_deliveries d LEFT JOIN run_submissions s ON s.run_id=d.run_id"
            " LEFT JOIN delivery_generation_fences f ON f.scope_key=d.scope_key"
            " WHERE d.run_id=?",
            (run_id,),
        ).fetchone()
        if row is None or row[0] is None:
            return False
        if row[1] is not None and row[3] is not None:
            return int(row[1]) >= int(row[3])
        return row[2] is None or int(row[0]) > int(row[2])

    def get_delivery(self, run_id: str) -> dict | None:
        """按 run_id 取交付台账行,不存在返回 None。"""
        r = self._conn.execute(
            "SELECT run_id, submission_seq, idempotency_key, scope_key, session_key,"
            " output, expect_structured, expected_state, plan_json, created_at,"
            " completed_at, updated_at, cancel_reason, adapter, is_private, user_id, group_id,"
            " source_event_id, recovery_json, reply_protocol_version"
            " FROM run_deliveries WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if r is None:
            return None
        return {
            "run_id": r[0],
            "submission_seq": r[1],
            "idempotency_key": r[2],
            "scope_key": r[3],
            "session_key": r[4],
            "output": r[5],
            "expect_structured": bool(r[6]),
            "expected_state": r[7],
            "plan_json": r[8],
            "created_at": r[9],
            "completed_at": r[10],
            "updated_at": r[11],
            "cancel_reason": r[12],
            "adapter": r[13],
            "is_private": bool(r[14]) if r[14] is not None else None,
            "user_id": r[15],
            "group_id": r[16],
            "source_event_id": r[17],
            "recovery_json": r[18],
            "reply_protocol_version": r[19],
        }

    def get_latest_submission_by_source_event(self, source_event_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT idempotency_key FROM run_submissions WHERE source_event_id=?"
            " ORDER BY submission_seq DESC LIMIT 1",
            (source_event_id,),
        ).fetchone()
        return self.get_submission(row[0]) if row is not None else None

    def get_latest_delivery_by_source_event(self, source_event_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT run_id FROM run_deliveries WHERE source_event_id=?"
            " ORDER BY submission_seq DESC LIMIT 1",
            (source_event_id,),
        ).fetchone()
        return self.get_delivery(row[0]) if row is not None else None

    # --- 出站组件计划、认领与回执(本部署补丁, alive/108 提交 B)。 ---

    def upsert_delivery_plan(
        self, *, run_id: str, plan: dict, components: list[dict]
    ) -> None:
        """首次形成的投递计划与组件清单落库。

        计划一旦形成不得因后续配置变化悄悄变形:同 run 重复 upsert 只接受
        逐字相同的计划;组件用 INSERT OR IGNORE,已 sent 的组件不被重置。
        """
        canonical = json.dumps(plan, sort_keys=True, ensure_ascii=False)
        row = self._conn.execute(
            "SELECT plan_json FROM run_deliveries WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"no delivery for run_id={run_id!r}")
        if row[0] is not None and row[0] != canonical:
            raise ValueError(f"delivery plan changed for run_id={run_id!r}")
        now = time.time()
        with self._conn:
            self._conn.execute(
                "UPDATE run_deliveries SET plan_json=?, updated_at=? WHERE run_id=?",
                (canonical, now, run_id),
            )
            for comp in components:
                self._conn.execute(
                    "INSERT OR IGNORE INTO delivery_components "
                    "(run_id, component_id, kind, payload_sha256, state, created_at, updated_at)"
                    " VALUES (?,?,?,?,'pending',?,?)",
                    (
                        run_id, comp["component_id"], comp["kind"],
                        comp.get("payload_sha256"), now, now,
                    ),
                )

    def claim_component(
        self, *, run_id: str, component_id: str, owner: str, lease_sec: float = 30.0,
        unknown_backoff_sec: float | None = None, unknown_max_attempts: int = 0,
        attempt_token: str | None = None,
    ) -> str:
        """原子认领一个出站组件,返回 send／sent／busy。

        send=取得认领可以调平台;sent=已完成(含 cancelled/not_required 的
        业务免发),调用方直接读既有事实,绝不再调平台;busy=他人有效租约或
        unknown(平台结果不明,不盲补)。failed 是平台调用前的本地失败,允许
        修复后重试。BEGIN IMMEDIATE 保证跨进程只有一个认领者。
        unknown_backoff_sec 与 unknown_max_attempts 只由 reconcile 传入:
        结果不明超过退避且认领次数未耗尽时才允许一次有限重试。
        """
        now = time.time()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT state, claim_owner, claim_token, claim_until, attempt_count, updated_at"
                " FROM delivery_components WHERE run_id=? AND component_id=?",
                (run_id, component_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"no component {component_id!r} for run_id={run_id!r}")
            state, claim_owner, claim_token, claim_until, attempt_count, updated_at = row
            outcome = "send"
            reentrant = False
            if state in ("sent", "cancelled", "not_required"):
                outcome = "sent"
            elif state == "unknown":
                retryable = (
                    unknown_backoff_sec is not None
                    and attempt_count < unknown_max_attempts
                    and now - (updated_at or 0.0) >= unknown_backoff_sec
                )
                outcome = "send" if retryable else "busy"
            elif (
                state == "claimed" and claim_owner == owner
                and claim_until is not None and claim_until > now
                and claim_token == attempt_token
            ):
                # 只有同一次尝试能重入；同进程另一个 reconcile 即使复用
                # owner 字符串，不同令牌也只能看到 busy。
                reentrant = True
            elif (
                state == "claimed" and claim_until is not None and claim_until > now
            ):
                outcome = "busy"
            if outcome == "send" and not reentrant:
                self._conn.execute(
                    "UPDATE delivery_components SET state='claimed', claim_owner=?,"
                    " claim_token=?, claim_until=?, attempt_count=attempt_count+1, updated_at=?"
                    " WHERE run_id=? AND component_id=?",
                    (owner, attempt_token, now + max(0.0, lease_sec), now, run_id, component_id),
                )
            self._conn.commit()
            return outcome
        except Exception:
            self._conn.rollback()
            raise

    def get_component(self, *, run_id: str, component_id: str) -> dict | None:
        """按主键取组件事实(含 msg_ids 解析),不存在返回 None。"""
        r = self._conn.execute(
            "SELECT kind, payload_sha256, state, claim_owner, claim_token, claim_until,"
            " attempt_count, msg_ids_json, last_error, created_at, updated_at"
            " FROM delivery_components WHERE run_id=? AND component_id=?",
            (run_id, component_id),
        ).fetchone()
        if r is None:
            return None
        return {
            "run_id": run_id,
            "component_id": component_id,
            "kind": r[0],
            "payload_sha256": r[1],
            "state": r[2],
            "claim_owner": r[3],
            "claim_token": r[4],
            "claim_until": r[5],
            "attempt_count": r[6],
            "msg_ids": json.loads(r[7]) if r[7] else [],
            "last_error": r[8],
            "created_at": r[9],
            "updated_at": r[10],
        }

    def _mark_component(
        self, run_id: str, component_id: str, state: str, *, msg_ids=None, error=None,
        attempt_token: str | None = None, allow_pending: bool = False,
    ) -> bool:
        sql = (
            "UPDATE delivery_components SET state=?, msg_ids_json=COALESCE(?, msg_ids_json),"
            " last_error=?, claim_owner=NULL, claim_until=NULL, updated_at=?"
            " WHERE run_id=? AND component_id=?"
        )
        params = [
            state,
            json.dumps(list(msg_ids)) if msg_ids is not None else None,
            (str(error)[:300] if error is not None else None),
            time.time(), run_id, component_id,
        ]
        if attempt_token is not None:
            if allow_pending:
                sql += " AND (claim_token=? OR state='pending')"
            else:
                sql += " AND claim_token=?"
            params.append(attempt_token)
        cur = self._conn.execute(sql, params)
        self._conn.commit()
        return cur.rowcount == 1

    def mark_component_sent(
        self, *, run_id: str, component_id: str, msg_ids: list,
        attempt_token: str | None = None,
    ) -> bool:
        """平台成功后立即写回执；带令牌时过期尝试不能覆盖新事实。"""
        return self._mark_component(
            run_id, component_id, "sent", msg_ids=msg_ids, attempt_token=attempt_token
        )

    def mark_component_unknown(
        self, *, run_id: str, component_id: str, error,
        attempt_token: str | None = None,
    ) -> bool:
        """平台调用已发起但结果不明(异常/超时/崩溃);不得立即盲补。"""
        return self._mark_component(
            run_id, component_id, "unknown", error=error, attempt_token=attempt_token
        )

    def mark_component_failed(
        self, *, run_id: str, component_id: str, error,
        attempt_token: str | None = None,
    ) -> bool:
        """平台调用前的本地失败(白名单/校验),允许修复后重试。"""
        return self._mark_component(
            run_id, component_id, "failed", error=error, attempt_token=attempt_token,
            allow_pending=True,
        )

    def set_expected_state(
        self, run_id: str, expected_state: str, *, cancel_reason: str | None = None
    ) -> bool:
        """业务销账:静默／抑制／取消／超期。无台账行返回 False(不伪造)。"""
        cur = self._conn.execute(
            "UPDATE run_deliveries SET expected_state=?, cancel_reason=?, updated_at=?"
            " WHERE run_id=?",
            (expected_state, cancel_reason, time.time(), run_id),
        )
        self._conn.commit()
        return cur.rowcount == 1

    # --- 统一 reconcile 查询与终态(本部署补丁, alive/108 提交 C)。 ---

    def list_components(self, run_id: str) -> list[dict]:
        """某 run 的全部组件,primary 在前,其余按 component_id 排序(文件有序)。"""
        rows = self._conn.execute(
            "SELECT component_id FROM delivery_components WHERE run_id=?"
            " ORDER BY CASE component_id WHEN 'primary' THEN 0 ELSE 1 END, component_id",
            (run_id,),
        ).fetchall()
        return [self.get_component(run_id=run_id, component_id=r[0]) for r in rows]

    def list_ready_deliveries(self, scope_key: str | None = None) -> list[dict]:
        """expected 已落账但尚未终态的 deliveries(待投递/待对账),按提交序。"""
        sql = (
            "SELECT run_id FROM run_deliveries WHERE expected_state='ready'"
        )
        params: tuple = ()
        if scope_key is not None:
            sql += " AND scope_key=?"
            params = (scope_key,)
        sql += " ORDER BY submission_seq"
        return [self.get_delivery(r[0]) for r in self._conn.execute(sql, params).fetchall()]

    def prune_delivery_ledger(
        self, *, retention_sec: float, now: float | None = None
    ) -> dict[str, int]:
        """清理超期且已完整结算的 runs 交付台账，返回三表删除数。"""
        cutoff = (time.time() if now is None else now) - retention_sec
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self._conn.execute(
                "SELECT d.run_id FROM run_deliveries d"
                " JOIN run_submissions s ON s.run_id=d.run_id"
                " WHERE s.state='bound'"
                " AND NOT EXISTS (SELECT 1 FROM delivery_components c"
                "   WHERE c.run_id=d.run_id AND c.state IN ('unknown','claimed'))"
                " AND ((d.expected_state='ready'"
                "   AND EXISTS (SELECT 1 FROM delivery_components c WHERE c.run_id=d.run_id)"
                "   AND NOT EXISTS (SELECT 1 FROM delivery_components c"
                "     WHERE c.run_id=d.run_id AND c.state NOT IN ('sent','cancelled','not_required')))"
                "  OR d.expected_state<>'ready')"
                " AND d.updated_at<? AND s.updated_at<?"
                " AND NOT EXISTS (SELECT 1 FROM active_runs a WHERE a.run_id=d.run_id)"
                " AND NOT EXISTS (SELECT 1 FROM delivery_components c"
                "   WHERE c.run_id=d.run_id AND c.updated_at>=?)",
                (cutoff, cutoff, cutoff),
            ).fetchall()
            run_ids = [r[0] for r in rows]
            counts = {"submissions": 0, "deliveries": 0, "components": 0}
            for run_id in run_ids:
                counts["components"] += self._conn.execute(
                    "DELETE FROM delivery_components WHERE run_id=?", (run_id,)
                ).rowcount
                counts["deliveries"] += self._conn.execute(
                    "DELETE FROM run_deliveries WHERE run_id=?", (run_id,)
                ).rowcount
                counts["submissions"] += self._conn.execute(
                    "DELETE FROM run_submissions WHERE run_id=?", (run_id,)
                ).rowcount
            self._conn.commit()
            return counts
        except Exception:
            self._conn.rollback()
            raise

    def list_predecessors(self, *, scope_key: str, submission_seq: int) -> list[dict]:
        """同 scope、提交序更早且仍未终态的 deliveries(顺序门前序)。"""
        rows = self._conn.execute(
            "SELECT run_id FROM run_deliveries"
            " WHERE scope_key=? AND submission_seq<? AND expected_state='ready'"
            " ORDER BY submission_seq",
            (scope_key, submission_seq),
        ).fetchall()
        return [self.get_delivery(r[0]) for r in rows]

    def mark_delivery_terminal(self, run_id: str) -> None:
        """所有必需组件终态后置 terminal;只前进,不回写其他状态。"""
        self._conn.execute(
            "UPDATE run_deliveries SET expected_state='terminal', updated_at=?"
            " WHERE run_id=? AND expected_state='ready'",
            (time.time(), run_id),
        )
        self._conn.commit()

    def mark_component_not_required(self, *, run_id: str, component_id: str) -> None:
        """组件业务上不再需要(如历史缓冲不可用),按免发终态处理。"""
        self._mark_component(run_id, component_id, "not_required")

    def close(self) -> None:
        self._conn.close()
