"""SQLite 后端的 session key 映射。

存 internal_id → (当前 session key, generation)。两件事都必须跨重启活着:

- **轮换后的 key**:上游 compression 会把旧 session 置为 closed 并回传新 id
  (响应头 ``X-Hermes-Session-Id``)。重启后若退回派生 key,就又钉回那个已关闭的
  父会话 —— 写全部失败,且每次压缩再分叉一个兄弟,直到血缘歧义无法自愈。
- **generation**:`/clear` 靠它产出 `-gN` 新 key。只存内存的话,重启等于把被清掉的
  会话原地复活。

本模块**不存真实 transport**。internal_id 可能已被归一到业务会话域(``qqtpc+…``),
但把它持久化成会话事实,以后在 OneBot／Milky 之间来回切就要改库——那正是要避免的
人工门槛。真实 adapter 是运行时信息,由 SessionManager 从当前连接／既有路由解析;
本模块只负责「哪个会话、用哪个 key、第几代」。
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from nonebot import logger

# updated_at 只写不读;闲置状态列由 _ensure_idle_state_columns 对存量库可重放补齐。
_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS session_keys (
        internal_id  TEXT    PRIMARY KEY,
        session_key  TEXT    NOT NULL,
        generation   INTEGER NOT NULL DEFAULT 0,
        updated_at   REAL    NOT NULL,
        last_activity REAL   NOT NULL DEFAULT 0,
        idle_armed   INTEGER NOT NULL DEFAULT 0,
        keepalive_last REAL  NOT NULL DEFAULT 0
    )
    """,
    # /delay 暂停状态持久化——原内存态重启即失效,
    # 被暂停会话重启后悄悄恢复 T1 计时。
    """
    CREATE TABLE IF NOT EXISTS idle_paused (
        internal_id  TEXT    PRIMARY KEY
    )
    """,
]

# 统一闲置动作的最后活动时刻、动作资格与上次保活:与 session key 同库同表。
# 存量库用 PRAGMA table_info + ALTER TABLE 补列,可重放;旧行取默认值
# ``last_activity=0 / idle_armed=0 / keepalive_last=0``——部署后不会给失活会话突然动作。
_IDLE_STATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("last_activity", "REAL NOT NULL DEFAULT 0"),
    ("idle_armed", "INTEGER NOT NULL DEFAULT 0"),
    ("keepalive_last", "REAL NOT NULL DEFAULT 0"),
)


# session key 映射的持久化/装载失败从 WARN 升级为 ERROR 并
# 写清后果——原 WARN 混在日常噪音里,防护静默失效(重启退回派生 key、钉回已关闭的
# 父会话,正是本模块要防的故障形态)。300s 节流防 disk-full 类持续失败刷屏。
# 契约不变:失败降级为不持久化,绝不抛。
_FAIL_LOGGED_AT: dict[str, float] = {}
_FAIL_LOG_THROTTLE_SEC = 300.0


def _log_store_failure(op: str, exc: Exception, detail: str = "") -> None:
    now = time.monotonic()
    if now - _FAIL_LOGGED_AT.get(op, -1e9) < _FAIL_LOG_THROTTLE_SEC:
        return
    _FAIL_LOGGED_AT[op] = now
    logger.error(
        f"[SESSION] {op}失败{detail}: {exc} —— 持久化防护失效,"
        f"重启后将退回派生 key,可能钉回已关闭的父会话(本模块要防的故障形态)"
    )


class SessionDomainMigrationConflict(RuntimeError):
    """会话域归一冲突:同一业务会话已有两套 internal_id 映射并存。

    失败关闭(零写入):不合并、不覆盖、不猜哪个才是真的——交给运维清理。
    旧行原样留在库里；未归一/有冲突的库启动时也失败关闭，先在库外做完一次性
    归一（必要时 --exclude/--override）再启动。
    """

    def __init__(self, conflicts: list[tuple[str, str]]) -> None:
        self.conflicts = tuple(conflicts)
        detail = ", ".join(f"{old} -> {new}" for old, new in conflicts)
        super().__init__(f"session domain migration conflicts: {detail}")


@dataclass(frozen=True)
class SessionDomainMigrationReport:
    """一次性归一结果(计数 + 冲突 + 完整性自检)。"""

    session_keys_migrated: int = 0
    idle_paused_migrated: int = 0
    integrity_ok: bool = True
    conflicts: tuple[tuple[str, str], ...] = ()
    excluded: tuple[str, ...] = ()
    overrides_applied: int = 0
    overrides_ignored: tuple[str, ...] = ()
    dry_run: bool = False


def plan_domain_migration(
    internal_ids: list[str],
    transport_to_domain: Mapping[str, str],
    *,
    exclude: tuple[str, ...] = (),
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """算出 ``(旧 id → 新 id)`` 计划与冲突清单(纯函数,便于测试)。

    只处理 ``{transport}+…`` 形态,其余平台(QQ 官方、tg 等)原样不动。同一目标被
    两条源行指向(两种协议各有映射)或目标已存在,都算冲突——归一不能靠「谁先到」
    决定血缘。``exclude`` 是运维显式点名的旧行(如主实例里那条只有 6 条记录的
    「milky测试」小会话):不收录、不删除、不参与冲突判定,原样留在库里。
    """
    excluded = set(exclude)
    existing = set(internal_ids)
    plan: list[tuple[str, str]] = []
    claimed: dict[str, list[str]] = {}
    for internal_id in internal_ids:
        if internal_id in excluded:
            continue
        head, sep, tail = internal_id.partition("+")
        if not sep or not tail:
            continue
        domain = transport_to_domain.get(head)
        if not domain:
            continue
        new_id = f"{domain}+{tail}"
        if new_id == internal_id:
            continue
        plan.append((internal_id, new_id))
        claimed.setdefault(new_id, []).append(internal_id)
    conflicts = sorted(
        (old_id, new_id)
        for old_id, new_id in plan
        if new_id in existing or len(claimed.get(new_id, ())) > 1
    )
    return plan, conflicts


class SessionKeyStore:
    """internal_id → session key 的持久化映射。失败降级为不持久化,绝不抛。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False 的理由同 MessageStore:NoneBot driver 可能在多
        # thread 上访问 ASGI 应用,而写入方只有 event loop 一个。
        self._conn = sqlite3.connect(self._db_path, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        for stmt in _SCHEMA_STATEMENTS:
            self._conn.execute(stmt)
        self._ensure_idle_state_columns()
        self._closed = False

    def _ensure_idle_state_columns(self) -> None:
        """给存量库补 ``last_activity`` / ``idle_armed``(可重放、可重入)。

        旧行取列默认值(0 / 0),等价「没记录过活动、资格关闭」:部署后不会
        把历史会话当成刚活动过,也不会替它们开始保活或压缩。
        """
        try:
            existing = {
                row[1] for row in self._conn.execute("PRAGMA table_info(session_keys)").fetchall()
            }
            for name, decl in _IDLE_STATE_COLUMNS:
                if name in existing:
                    continue
                self._conn.execute(f"ALTER TABLE session_keys ADD COLUMN {name} {decl}")
        except Exception as exc:
            _log_store_failure("补闲置状态列", exc, ",本次退化为纯内存闲置状态")

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._conn.close()
        except Exception:
            pass
        self._closed = True

    def load_all(self) -> dict[str, tuple[str, int]]:
        """返回 internal_id → (session_key, generation)。读失败返回空表。"""
        try:
            rows = self._conn.execute("SELECT internal_id, session_key, generation FROM session_keys").fetchall()
        except Exception as exc:
            _log_store_failure("读取 session key 映射", exc, ",本次退化为纯内存")
            return {}
        return {r["internal_id"]: (r["session_key"], r["generation"]) for r in rows}

    def put(self, internal_id: str, session_key: str, generation: int, *, now: float) -> None:
        try:
            # updated_at 只写不读:随写入落库即可,无读点。
            self._conn.execute(
                "INSERT INTO session_keys (internal_id, session_key, generation, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(internal_id) DO UPDATE SET "
                "session_key=excluded.session_key, generation=excluded.generation, updated_at=excluded.updated_at",
                (internal_id, session_key, generation, now),
            )
        except Exception as exc:
            _log_store_failure("持久化 session key", exc, f" ({internal_id})")

    # --- /delay 暂停状态持久化 ---

    def load_idle_paused(self) -> set[str]:
        """返回被 /delay 暂停 T1 的 internal_id 集合。读失败返回空集。"""
        try:
            rows = self._conn.execute("SELECT internal_id FROM idle_paused").fetchall()
        except Exception as exc:
            logger.warning(f"[SESSION] 读取 delay 暂停表失败,本次退化为纯内存: {exc}")
            return set()
        return {r[0] for r in rows}

    def set_idle_paused(self, internal_id: str, paused: bool) -> None:
        try:
            if paused:
                self._conn.execute(
                    "INSERT OR IGNORE INTO idle_paused (internal_id) VALUES (?)", (internal_id,)
                )
            else:
                self._conn.execute("DELETE FROM idle_paused WHERE internal_id=?", (internal_id,))
        except Exception as exc:
            logger.warning(f"[SESSION] 持久化 delay 暂停失败 ({internal_id}): {exc}")

    # --- 统一闲置动作状态持久化(最后活动 + 资格 + 上次保活)---

    def load_idle_states(self) -> dict[str, tuple[float, bool, float]]:
        """返回 internal_id → (最后活动时刻, 是否具备资格, 上次保活时刻)。

        读失败返回空表:退化后所有会话按「没有活动记录」处理,不会误保活／误清。
        """
        try:
            rows = self._conn.execute(
                "SELECT internal_id, last_activity, idle_armed, keepalive_last FROM session_keys"
            ).fetchall()
        except Exception as exc:
            _log_store_failure("读取闲置状态", exc, ",本次按无活动记录处理")
            return {}
        return {
            r["internal_id"]: (
                float(r["last_activity"] or 0.0),
                bool(r["idle_armed"]),
                float(r["keepalive_last"] or 0.0),
            )
            for r in rows
        }

    def set_idle_state(
        self,
        internal_id: str,
        last_activity: float,
        armed: bool,
        *,
        keepalive_last: float = 0.0,
    ) -> None:
        """写一条会话的完整闲置状态(状态一变就落库,重启原样读回)。"""
        self.set_idle_states_batch(
            [(internal_id, last_activity, armed, keepalive_last)]
        )

    def set_idle_states_batch(self, rows) -> None:
        """在一个事务里写多条 ``(internal_id, last_activity, armed, keepalive_last)``。

        正常进展由 SessionManager 全局限频合批；同批同 id 只传最新值。只 UPDATE
        已存在的 key 行,写失败回滚并降级为内存态,绝不抛。
        """
        values = [
            (float(last_activity), 1 if armed else 0, float(keepalive_last), internal_id)
            for internal_id, last_activity, armed, keepalive_last in rows
        ]
        if not values:
            return
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.executemany(
                "UPDATE session_keys SET last_activity=?, idle_armed=?, keepalive_last=? "
                "WHERE internal_id=?",
                values,
            )
            self._conn.execute("COMMIT")
        except Exception as exc:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            _log_store_failure("批量持久化闲置状态", exc, f" ({len(values)} 行)")

    def set_keepalive_last(self, internal_id: str, value: float) -> None:
        """只推进上次保活时刻,不改真实活动时间或资格。"""
        try:
            self._conn.execute(
                "UPDATE session_keys SET keepalive_last=? WHERE internal_id=?",
                (float(value), internal_id),
            )
        except Exception as exc:
            _log_store_failure("持久化保活时间", exc, f" ({internal_id})")

    # --- 会话域一次性归一(此后互切不再改库)---

    def migrate_session_domain(
        self,
        transport_to_domain: Mapping[str, str],
        *,
        dry_run: bool = False,
        exclude: tuple[str, ...] = (),
        overrides: Mapping[str, tuple[str, int]] | None = None,
    ) -> SessionDomainMigrationReport:
        """把 ``{transport}+…`` 形态的 internal_id 归一到业务会话域(如 ``qqtpc+…``)。

        语义(一次性、可重放、可先 dry_run):

        * **只改 internal_id**,``session_key`` / ``generation`` / ``updated_at`` /
          ``last_activity`` / ``idle_armed`` / ``keepalive_last`` 原样保留——那是上游
          转录的实名与本地闲置状态,改名等于断历史或让刚被清场/压缩的会话突然复活;
        * ``idle_paused`` 跟着迁,别的平台行不动;
        * 归一完成后 OneBot↔Milky 互切**不再动库**:真实 adapter 由运行时连接解析;
        * ``exclude`` 显式排除的旧行(主实例的「milky测试」小会话)不收录、不删除,
          也不会与主行争同一目标;
        * ``overrides`` 把某条旧行归一到**运维指定的** ``(session_key, generation)``:
          先在上游完成转录合并,再把 canonical internal_id 指向合并目标;本方法
          不自行挑 gN、不复制/重写 state.db;
        * **冲突失败关闭**:目标 id 已存在,或两种协议指向同一目标时,一行都不写,
          抛 ``SessionDomainMigrationConflict``。

        迁移在单个 ``BEGIN IMMEDIATE`` 事务里完成:中途失败整批回滚,不会留下半迁移
        状态。
        """
        excluded = tuple(dict.fromkeys(str(item) for item in exclude if item))
        override_map = {
            str(old): (str(key), int(generation))
            for old, (key, generation) in (overrides or {}).items()
            if old
        }
        for old, (key, _generation) in override_map.items():
            if not key.strip():
                raise ValueError(f"override session_key for {old} is empty")
        try:
            key_rows = [
                (
                    r["internal_id"],
                    r["session_key"],
                    int(r["generation"]),
                    float(r["updated_at"]),
                    float(r["last_activity"] or 0.0),
                    bool(r["idle_armed"]),
                    float(r["keepalive_last"] or 0.0),
                )
                for r in self._conn.execute(
                    "SELECT internal_id, session_key, generation, updated_at,"
                    " last_activity, idle_armed, keepalive_last FROM session_keys"
                ).fetchall()
            ]
            paused_ids = [
                r["internal_id"]
                for r in self._conn.execute("SELECT internal_id FROM idle_paused").fetchall()
            ]
        except Exception as exc:
            _log_store_failure("读取会话域归一源", exc, ",跳过本次归一")
            return SessionDomainMigrationReport(integrity_ok=False, excluded=excluded)

        key_ids = [row[0] for row in key_rows]
        key_plan, key_conflicts = plan_domain_migration(
            key_ids, transport_to_domain, exclude=excluded
        )
        paused_plan, paused_conflicts = plan_domain_migration(
            paused_ids, transport_to_domain, exclude=excluded
        )
        conflicts = sorted(set(key_conflicts) | set(paused_conflicts))
        applied_overrides = [old for old, _new in key_plan if old in override_map]
        ignored_overrides = tuple(old for old in override_map if old not in set(applied_overrides))
        if conflicts:
            if dry_run:
                return SessionDomainMigrationReport(
                    conflicts=tuple(conflicts),
                    excluded=excluded,
                    overrides_applied=len(applied_overrides),
                    overrides_ignored=ignored_overrides,
                    dry_run=True,
                )
            raise SessionDomainMigrationConflict(conflicts)
        if dry_run:
            return SessionDomainMigrationReport(
                session_keys_migrated=len(key_plan),
                idle_paused_migrated=len(paused_plan),
                excluded=excluded,
                overrides_applied=len(applied_overrides),
                overrides_ignored=ignored_overrides,
                dry_run=True,
            )

        source = {
            old: (key, generation, updated_at, last_activity, armed, keepalive_last)
            for old, key, generation, updated_at, last_activity, armed, keepalive_last in key_rows
        }
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            for old_id, new_id in key_plan:
                key, generation, updated_at, last_activity, armed, keepalive_last = source[old_id]
                if old_id in override_map:
                    key, generation = override_map[old_id]
                self._conn.execute(
                    "INSERT INTO session_keys (internal_id, session_key, generation, updated_at,"
                    " last_activity, idle_armed, keepalive_last) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        new_id, key, generation, updated_at, last_activity,
                        1 if armed else 0, keepalive_last,
                    ),
                )
                self._conn.execute("DELETE FROM session_keys WHERE internal_id=?", (old_id,))
            for old_id, new_id in paused_plan:
                self._conn.execute("INSERT INTO idle_paused (internal_id) VALUES (?)", (new_id,))
                self._conn.execute("DELETE FROM idle_paused WHERE internal_id=?", (old_id,))
            self._conn.execute("COMMIT")
        except sqlite3.IntegrityError as exc:
            self._conn.execute("ROLLBACK")
            # 计划外的竞争(别的进程刚写了目标行):整批回滚,按冲突如实上报。
            raise SessionDomainMigrationConflict(
                [(old_id, new_id) for old_id, new_id in key_plan + paused_plan]
            ) from exc
        except Exception as exc:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            _log_store_failure("会话域归一", exc, ",整批回滚(零写入)")
            raise

        if key_plan or paused_plan:
            logger.info(
                f"[SESSION] 会话域归一: {len(key_plan)} 条 session key / "
                f"{len(paused_plan)} 条 delay 暂停 → 业务会话域"
                + (f";定制目标 {len(applied_overrides)} 条" if applied_overrides else "")
            )
        return SessionDomainMigrationReport(
            session_keys_migrated=len(key_plan),
            idle_paused_migrated=len(paused_plan),
            excluded=excluded,
            overrides_applied=len(applied_overrides),
            overrides_ignored=ignored_overrides,
            integrity_ok=self.integrity_ok(),
        )

    def integrity_ok(self) -> bool:
        """``PRAGMA integrity_check`` 自检(归一后调用;失败只报告不抛)。"""
        try:
            return self._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        except Exception as exc:
            logger.warning(f"[SESSION] integrity_check 失败: {exc}")
            return False
