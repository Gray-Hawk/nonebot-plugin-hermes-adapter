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
    last_seq    INTEGER NOT NULL DEFAULT -1,
    started_at  REAL NOT NULL
)
"""

# 本部署补丁(弥补 v0.22.1 引入的 steer 丢弃缺陷, alive/75 根因、alive/84 方案):
# steer 对账台账。每条被上游 steer 接口接受(HTTP 200 且 accepted=True)的注入记
# 一行 pending;run 终态后按 m_id 拉转录精确搜, 搜不到即被静默丢弃, 批 2 补投成新
# run。无 m:id 的 steer 不入账(对账无锚点)。CREATE TABLE IF NOT EXISTS 幂等。
#
# P-07 起本表同时是「同一原始行只入账一次」的唯一身份来源:发送前先按
# (run_id, m_id) 认领(status='sending'),HTTP 接受后确认成 pending,失败则撤行;
# 重叠入口看到在途认领即不重复发送。sending 只是进程内在途标记, 进程重启后没有
# 在途请求, __init__ 会把滞留的 sending 归位 pending(见下方 UPDATE)。
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
    status      TEXT NOT NULL DEFAULT 'pending'  -- sending/pending/reconciling/reconciled/recovered/replayed
)
"""

# 终态对账**认领**的候选状态只能是被上游接受的账项。sending 是发送前的去重认领,
# 上游还没确认:把它当 accepted-but-swallowed 会抢在 HTTP 回应之前补投, 409/失败
# 后原 followup 又照常留槽 → 同一原始行呈递两次(第 2 轮评审 R3)。
# 晚到的 200 由发送方确认后交给同一条对账路径(见 message_relay), 不靠预判。
_STEER_ACCEPTED_STATUSES = "'pending'"
# native 归还(pending_steer 原文上浮)是**正向证据**:文本确实在上游手里, 在途
# 认领的身份也可以据此还原;核销同理。与终态对账的「缺件推断」不是一回事。
# reconciling 是对账消费者自己的在途认领(见 claim_steers_for_reconcile):终态钩子
# 与晚到确认可以各触发一次对账, 只有原子认领到行的那一次负责补投。
_STEER_LIVE_STATUSES = "'pending','sending','reconciling'"


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
    " session_key, mode, expect_structured, last_seq, started_at)"
    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
    " ON CONFLICT(run_id) DO UPDATE SET"
    " scope_key=excluded.scope_key, adapter=excluded.adapter,"
    " is_private=excluded.is_private, user_id=excluded.user_id,"
    " group_id=excluded.group_id, bot_self_id=excluded.bot_self_id,"
    " session_key=excluded.session_key, mode=excluded.mode,"
    " expect_structured=excluded.expect_structured"
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


@dataclass(frozen=True)
class SteerBatchClaim:
    """一批 steer 行在本 run 台账上的认领结果(P-07 幂等去重的唯一身份来源)。

    claimed:   本批新认领的行(status='sending',尚未被上游接受;发送失败要撤);
    accepted:  本 run 早前已入账的行(重复触发/重复前缀:本批不再发送);
    in_flight: 本 run 内已被别的重叠入口认领、仍在发送中的行(本批既不重发,
               也不得替那个入口结算掉——它失败时这些行还要走接力)。
    """

    claimed: tuple[dict, ...] = ()
    accepted: tuple[dict, ...] = ()
    in_flight: tuple[dict, ...] = ()


def make_scope_key(adapter: str, is_private: bool, user_id: str, group_id: str | None) -> str:
    """与 inflight registry 键同形,resume 时并发检查/送达定位直接可用。"""
    scope = "private" if is_private else "group"
    scope_id = user_id if is_private else (group_id or "")
    return f"{adapter}:{scope}:{scope_id}"


def _is_plan_prefix_growth(old_json: str, new_json: str) -> bool:
    """新计划是否只是旧计划的尾部追加（旧 units 逐字不变，其余字段不变）。

    E3 流式即时决策每到达一份就追加一份；30 号「计划一旦形成不得变形」的语义
    保留为「旧份不得改写/重排」。任何前缀不等、字段变化都判否。
    """
    try:
        old = json.loads(old_json)
        new = json.loads(new_json)
    except ValueError:
        return False
    if not isinstance(old, dict) or not isinstance(new, dict):
        return False
    if {k: v for k, v in old.items() if k != "units"} != {
        k: v for k, v in new.items() if k != "units"
    }:
        return False
    old_units = old.get("units")
    new_units = new.get("units")
    if not isinstance(old_units, list) or not isinstance(new_units, list):
        return False
    return len(new_units) >= len(old_units) and new_units[: len(old_units)] == old_units


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
        # 在途认领是进程内瞬态(见 _STEER_LEDGER_SCHEMA 注记):进程重启后没有任何
        # steer HTTP／对账请求在途,滞留的 sending／reconciling 必须归位 pending,
        # 否则这批行既不算已入账也不会被终态对账/回收看见——正是台账要防的静默丢弃。
        # 幂等 UPDATE,零行时零开销。
        self._conn.execute(
            "UPDATE steer_ledger SET status='pending'"
            " WHERE status IN ('sending','reconciling')"
        )
        # 提交 C 补投递目标列(恢复发送需要 adapter/私群/收发双方,active_runs
        # 行 finish 后仍要能解析 target);旧库逐列补齐,幂等。
        existing = {
            r[1] for r in self._conn.execute("PRAGMA table_info(run_deliveries)").fetchall()
        }
        for col, ddl in (
            ("adapter", "TEXT"), ("is_private", "INTEGER"),
            ("user_id", "TEXT"), ("group_id", "TEXT"),
        ):
            if col not in existing:
                self._conn.execute(f"ALTER TABLE run_deliveries ADD COLUMN {col} {ddl}")
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
    ) -> None:
        # 冲突改 ON CONFLICT DO UPDATE 只刷元数据——旧写法
        # INSERT OR REPLACE 全行重置,同一 run_id 二次登记会把 last_seq/started_at
        # 清零(事件流水位丢失,reattach 后旁白从头发一遍)。契约仍是「一 run 一
        # upsert」,此处加固到误复用也不丢水位。
        self._conn.execute(
            _ACTIVE_RUN_UPSERT_SQL,
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

    def has_active_run_for_target(
        self, *, adapter: str, is_private: bool, group_id: str | None = None,
        user_id: str | None = None,
    ) -> bool:
        """目标 scope 是否有在跑 run（E3：Runs 轮内 push 硬拒的权限闸门）。

        群侧按 adapter+group_id、私侧按 adapter+user_id 判；只看 active_runs
        （run 终态即删行），不引入新的身份映射或窗口。
        """
        if is_private:
            row = self._conn.execute(
                "SELECT 1 FROM active_runs WHERE adapter=? AND is_private=1"
                " AND user_id=? LIMIT 1",
                (adapter, str(user_id or "")),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT 1 FROM active_runs WHERE adapter=? AND is_private=0"
                " AND group_id=? LIMIT 1",
                (adapter, str(group_id or "")),
            ).fetchone()
        return row is not None

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
        scope_key: str,
        session_key: str,
        accepted_at: float,
        items: list[dict],
    ) -> None:
        """逐行入账(等价「已认领且已被上游接受」),同一 run 内 m_id 幂等。

        items=[{m_id, sender_id, steer_text}, ...]。发送/台账/去重同一份
        SteerPresentation 派生,不再有第二条平行 ID 轨。无 m:id 的行由调用方在
        投影时过滤(对账无锚点),这里只落给到的行。

        生产路径走 claim_steer/confirm_steer(发送前认领,重叠入口才看得到在途);
        本方法保留给「已知接受、无在途发窗口」的调用场景,重复 m_id 只落一次。
        """
        for it in items:
            self._conn.execute(
                "INSERT INTO steer_ledger "
                "(run_id, m_id, scope_key, session_key, sender_id, steer_text, accepted_at, status)"
                " SELECT ?,?,?,?,?,?,?,'pending'"
                " WHERE NOT EXISTS (SELECT 1 FROM steer_ledger WHERE run_id=? AND m_id=?)",
                (
                    run_id, str(it["m_id"]), scope_key, session_key,
                    str(it["sender_id"]), it["steer_text"], accepted_at,
                    run_id, str(it["m_id"]),
                ),
            )
        self._conn.commit()

    def claim_steer(
        self,
        *,
        run_id: str,
        scope_key: str,
        session_key: str,
        accepted_at: float,
        items: list[dict],
    ) -> SteerBatchClaim | None:
        """发送前按 (run_id, m_id) 认领本批原始行,返回真正要发的部分。

        「同一原始行只入账一次」由这里保证:同一 run 内已在账(无论已接受还是
        正在别的入口发送)的 m_id 绝不第二次插入,本批也不再发送它。这样重叠入口
        (A 的 HTTP 未回,B 再入同一 followup)不会各发一遍同一原始行,也不会在台账
        里留下重复账项让 native 回收凭空多还原一行。

        身份矛盾(同 m_id 不同 sender)返回 None:台账与本次呈递各说一套发送人,
        调用方整批失败关闭——绝不发一份无法可靠对账的文本。同 m_id 同 sender 但
        文本不同(同一行经不同投影路径重渲染)视为已呈现:同一行的正文会随投影
        变化(如跟帖路径剥表情包占位),它不构成新的呈现。

        认领行以 status='sending' 落库:发送失败由 release_steer 撤销,成功后
        confirm_steer 转 pending。中间没有任何 await,单事件循环内即原子。
        """
        rows = self._conn.execute(
            "SELECT m_id, sender_id, status FROM steer_ledger WHERE run_id=?",
            (run_id,),
        ).fetchall()
        existing = {r[0]: (r[1], r[2]) for r in rows}
        claimed: list[dict] = []
        accepted: list[dict] = []
        in_flight: list[dict] = []
        for it in items:
            mid = str(it["m_id"])
            prev = existing.get(mid)
            if prev is not None:
                if prev[0] != str(it["sender_id"]):
                    return None
                (in_flight if prev[1] == "sending" else accepted).append(it)
                continue
            self._conn.execute(
                "INSERT INTO steer_ledger "
                "(run_id, m_id, scope_key, session_key, sender_id, steer_text, accepted_at, status)"
                " VALUES (?,?,?,?,?,?,?,'sending')",
                (
                    run_id, mid, scope_key, session_key,
                    str(it["sender_id"]), it["steer_text"], accepted_at,
                ),
            )
            existing[mid] = (str(it["sender_id"]), "sending")
            claimed.append(it)
        self._conn.commit()
        return SteerBatchClaim(
            claimed=tuple(claimed),
            accepted=tuple(accepted),
            in_flight=tuple(in_flight),
        )

    def confirm_steer(self, run_id: str, m_ids: list[str]) -> tuple[str, ...]:
        """上游已接受本批认领:在途认领转 pending(终态对账/回收的正式账项)。

        返回真正从 sending 转成 pending 的行号。空元组 = 这批认领已经被别的
        路径结算(released/recovered/replayed),调用方不得把它当成「刚被接受」
        再走一遍补投;非空且 run 已收尾 = 晚到的 200, 内容需要交给对账路径。
        """
        if not m_ids:
            return ()
        placeholders = ", ".join("?" for _ in m_ids)
        rows = self._conn.execute(
            f"SELECT m_id FROM steer_ledger"
            f" WHERE run_id=? AND status='sending' AND m_id IN ({placeholders})",
            (run_id, *[str(m) for m in m_ids]),
        ).fetchall()
        confirmed = tuple(str(r[0]) for r in rows)
        if not confirmed:
            return ()
        self._conn.execute(
            f"UPDATE steer_ledger SET status='pending'"
            f" WHERE run_id=? AND status='sending' AND m_id IN ({placeholders})",
            (run_id, *[str(m) for m in m_ids]),
        )
        self._conn.commit()
        return confirmed

    def release_steer(self, run_id: str, m_ids: list[str]) -> None:
        """上游没接受本批(HTTP 失败/409/异常):撤掉在途认领,行留给接力路重投。"""
        if not m_ids:
            return
        placeholders = ", ".join("?" for _ in m_ids)
        self._conn.execute(
            f"DELETE FROM steer_ledger"
            f" WHERE run_id=? AND status='sending' AND m_id IN ({placeholders})",
            (run_id, *[str(m) for m in m_ids]),
        )
        self._conn.commit()

    def pending_steers(self, run_id: str) -> list[dict]:
        """取该 run 全部 live 台账行(终态预检/巡检用), dict 含全部列。

        live = pending(已接受)/sending(发送在途)/reconciling(对账在途)。终态钩子
        只拿它做「要不要起对账」的零开销预检;真正的候选由 claim_steers_for_reconcile
        原子认领, 只认已接受的 pending——在途认领不是「已接受但被吞」, 不得据此
        补投(第 2 轮评审 R3)。
        """
        rows = self._conn.execute(
            "SELECT id, run_id, m_id, scope_key, session_key, sender_id, steer_text,"
            f" accepted_at, status FROM steer_ledger WHERE run_id=?"
            f" AND status IN ({_STEER_LIVE_STATUSES})"
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

    def claim_steers_for_reconcile(self, run_id: str) -> list[dict]:
        """原子认领本 run 的已接受账项给终态对账(单消费者), 返回认领到的行。

        pending → reconciling 一条语句完成认领:终态钩子与「晚到确认」可能各触发
        一次对账, 只有先认领到行的那次负责转录核验/补投, 另一次看到空集直接返回
        ——两个触发点不会把同一行补投两次。认领是进程内瞬态, __init__ 会把重启后
        滞留的 reconciling 归位 pending。

        SELECT 与 UPDATE 之间没有 await, 单事件循环内即原子。
        """
        rows = self._conn.execute(
            "SELECT id, run_id, m_id, scope_key, session_key, sender_id, steer_text,"
            " accepted_at, status FROM steer_ledger WHERE run_id=?"
            f" AND status IN ({_STEER_ACCEPTED_STATUSES})"
            " ORDER BY id",
            (run_id,),
        ).fetchall()
        if not rows:
            return []
        ids = [int(r[0]) for r in rows]
        placeholders = ",".join("?" for _ in ids)
        self._conn.execute(
            f"UPDATE steer_ledger SET status='reconciling' WHERE id IN ({placeholders})"
            " AND status='pending'",
            ids,
        )
        self._conn.commit()
        claimed = [
            {
                "id": r[0],
                "run_id": r[1],
                "m_id": r[2],
                "scope_key": r[3],
                "session_key": r[4],
                "sender_id": r[5],
                "steer_text": r[6],
                "accepted_at": r[7],
                "status": "reconciling",
            }
            for r in rows
        ]
        return claimed

    def pending_steers_by_scope(self, scope_key: str) -> list[dict]:
        """按 scope_key 取全部 live 台账(native 回收逐行归因用)。

        live = pending(已接受)/sending(在途认领)/reconciling(对账在途)。这里与
        终态对账不同:sending 出现在 pending_steer 原文里是上游确实收下这条的
        **正向证据**, 据此还原身份不会补投; 缺件推断只属于终态对账。

        native 回收点(run 终态 pending_steer 上浮)拿不到 run_id——ChatResult
        不携带、tracker 行已被 finish 删除——但台账行带 scope_key:按 scope 拉
        候选,再由调用方按 pending 原文的实际位置/次数匹配(台账只提供身份,
        不提供顺序),精确找回这批被回收行的 sender 与逐行正文。返回按台账 id 升序。
        """
        rows = self._conn.execute(
            "SELECT id, run_id, m_id, scope_key, session_key, sender_id, steer_text,"
            f" accepted_at, status FROM steer_ledger WHERE scope_key=?"
            f" AND status IN ({_STEER_LIVE_STATUSES})"
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

    def steer_statuses(self, run_id: str, m_ids: list[str]) -> dict[str, str]:
        """读指定行当前台账状态(含非 live 终态),供「晚到确认是否仍欠呈递」判定。

        `_try_steer_followup` 用它把「本批已接受并已呈现」与「已被 native 归还」
        分开:后者台账为 recovered——上游 pending_steer 上浮证明模型没看到,晚到的
        HTTP200 不能把这行从接力槽里摘掉,否则真正被吞的内容会静默丢失。
        不存在的行不出现在结果里。
        """
        if not m_ids:
            return {}
        placeholders = ",".join("?" for _ in m_ids)
        rows = self._conn.execute(
            f"SELECT m_id, status FROM steer_ledger"
            f" WHERE run_id=? AND m_id IN ({placeholders})",
            (run_id, *[str(m) for m in m_ids]),
        ).fetchall()
        return {str(r[0]): str(r[1]) for r in rows}

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
            f"UPDATE steer_ledger SET status='recovered' WHERE run_id=?"
            f" AND status IN ({_STEER_LIVE_STATUSES})",
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
            f" WHERE status IN ({_STEER_LIVE_STATUSES}) AND ? LIKE '%'||steer_text||'%'",
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
                    bot_self_id, session_key, mode, int(expect_structured), -1, time.time(),
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
            "SELECT adapter, is_private, user_id, group_id FROM active_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        adapter, is_private, user_id, group_id = target if target else (None, None, None, None)
        self._conn.execute(
            "INSERT INTO run_deliveries "
            "(run_id, submission_seq, idempotency_key, scope_key, session_key,"
            " output, expect_structured, expected_state, created_at, completed_at, updated_at,"
            " adapter, is_private, user_id, group_id, source_event_id)"
            " VALUES (?,?,?,?,?,?,?,'ready',?,?,?,?,?,?,?,?)"
            " ON CONFLICT(run_id) DO UPDATE SET"
            " output=excluded.output, expect_structured=excluded.expect_structured,"
            " expected_state='ready', updated_at=excluded.updated_at,"
            " adapter=COALESCE(run_deliveries.adapter, excluded.adapter),"
            " is_private=COALESCE(run_deliveries.is_private, excluded.is_private),"
            " user_id=COALESCE(run_deliveries.user_id, excluded.user_id),"
            " group_id=COALESCE(run_deliveries.group_id, excluded.group_id),"
            " source_event_id=COALESCE(run_deliveries.source_event_id, excluded.source_event_id)",
            (run_id, sub[0], sub[1], sub[2], sub[3], output, int(expect_structured), now, now, now,
             adapter, is_private, user_id, group_id, sub[4]),
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
            " source_event_id FROM run_deliveries WHERE run_id=?",
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
        self, *, run_id: str, plan: dict, components: list[dict],
        allow_prefix_growth: bool = False,
    ) -> None:
        """首次形成的投递计划与组件清单落库。

        计划一旦形成不得因后续配置变化悄悄变形:同 run 重复 upsert 只接受
        逐字相同的计划;组件用 INSERT OR IGNORE,已 sent 的组件不被重置。

        allow_prefix_growth 只给 E3 流式即时决策用：允许新计划在**尾部追加**份，
        旧份必须逐字不变（旧 decisions 不被重排、改写），已 sent 组件不重置。
        任何非追加变化仍按原约束拒绝。
        """
        canonical = json.dumps(plan, sort_keys=True, ensure_ascii=False)
        row = self._conn.execute(
            "SELECT plan_json FROM run_deliveries WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"no delivery for run_id={run_id!r}")
        if row[0] is not None and row[0] != canonical:
            if not allow_prefix_growth or not _is_plan_prefix_growth(row[0], canonical):
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

    def open_stream_delivery(self, run_id: str) -> None:
        """E3 流式即时决策：首份发送前建台账行，状态 streaming。

        `streaming` 不在 `list_ready_deliveries` 的扫描集合里——run 还在跑时
        对账器不得把半批计划当漏件补发。run.completed 的 record_expected_delivery
        会把它提升为 ready；失败/取消/中断由既有 settle 路径落终态。
        """
        sub = self._conn.execute(
            "SELECT submission_seq, idempotency_key, scope_key, session_key, source_event_id"
            " FROM run_submissions WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if sub is None:
            raise KeyError(f"no bound submission for run_id={run_id!r}")
        target = self._conn.execute(
            "SELECT adapter, is_private, user_id, group_id FROM active_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        adapter, is_private, user_id, group_id = target if target else (None, None, None, None)
        now = time.time()
        self._conn.execute(
            "INSERT INTO run_deliveries "
            "(run_id, submission_seq, idempotency_key, scope_key, session_key,"
            " output, expect_structured, expected_state, created_at, completed_at, updated_at,"
            " adapter, is_private, user_id, group_id, source_event_id)"
            " VALUES (?,?,?,?,?,'',1,'streaming',?,NULL,?,?,?,?,?,?)"
            " ON CONFLICT(run_id) DO NOTHING",
            (run_id, sub[0], sub[1], sub[2], sub[3], now, now,
             adapter, is_private, user_id, group_id, sub[4]),
        )
        self._conn.commit()

    def claim_component(
        self, *, run_id: str, component_id: str, owner: str, lease_sec: float = 30.0,
        unknown_backoff_sec: float | None = None, unknown_max_attempts: int = 0,
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
                "SELECT state, claim_owner, claim_until, attempt_count, updated_at"
                " FROM delivery_components WHERE run_id=? AND component_id=?",
                (run_id, component_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"no component {component_id!r} for run_id={run_id!r}")
            state, claim_owner, claim_until, attempt_count, updated_at = row
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
            ):
                # reconcile 已带 unknown 策略认领后，统一出站会持同一 owner 重入；
                # 有效租约仍归原尝试，不得把一次平台发送重复计成两次。
                reentrant = True
            elif (
                state == "claimed" and claim_owner != owner
                and claim_until is not None and claim_until > now
            ):
                outcome = "busy"
            if outcome == "send" and not reentrant:
                self._conn.execute(
                    "UPDATE delivery_components SET state='claimed', claim_owner=?,"
                    " claim_until=?, attempt_count=attempt_count+1, updated_at=?"
                    " WHERE run_id=? AND component_id=?",
                    (owner, now + max(0.0, lease_sec), now, run_id, component_id),
                )
            self._conn.commit()
            return outcome
        except Exception:
            self._conn.rollback()
            raise

    def get_component(self, *, run_id: str, component_id: str) -> dict | None:
        """按主键取组件事实(含 msg_ids 解析),不存在返回 None。"""
        r = self._conn.execute(
            "SELECT kind, payload_sha256, state, claim_owner, claim_until,"
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
            "claim_until": r[4],
            "attempt_count": r[5],
            "msg_ids": json.loads(r[6]) if r[6] else [],
            "last_error": r[7],
            "created_at": r[8],
            "updated_at": r[9],
        }

    def _mark_component(
        self, run_id: str, component_id: str, state: str, *, msg_ids=None, error=None
    ) -> None:
        self._conn.execute(
            "UPDATE delivery_components SET state=?, msg_ids_json=COALESCE(?, msg_ids_json),"
            " last_error=?, claim_owner=NULL, claim_until=NULL, updated_at=?"
            " WHERE run_id=? AND component_id=?",
            (
                state,
                json.dumps(list(msg_ids)) if msg_ids is not None else None,
                (str(error)[:300] if error is not None else None),
                time.time(), run_id, component_id,
            ),
        )
        self._conn.commit()

    def mark_component_sent(self, *, run_id: str, component_id: str, msg_ids: list) -> None:
        """平台成功后立即写回执;发送成功但无 msg_id 同样记 sent(空列表)。"""
        self._mark_component(run_id, component_id, "sent", msg_ids=msg_ids)

    def mark_component_unknown(self, *, run_id: str, component_id: str, error) -> None:
        """平台调用已发起但结果不明(异常/超时/崩溃);不得立即盲补。"""
        self._mark_component(run_id, component_id, "unknown", error=error)

    def mark_component_failed(self, *, run_id: str, component_id: str, error) -> None:
        """平台调用前的本地失败(白名单/校验),允许修复后重试。"""
        self._mark_component(run_id, component_id, "failed", error=error)

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
