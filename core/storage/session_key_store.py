"""SQLite 后端的 session key 映射。

存 internal_id → (当前 session key, generation)。两件事都必须跨重启活着:

- **轮换后的 key**:上游 compression 会把旧 session 置为 closed 并回传新 id
  (响应头 ``X-Hermes-Session-Id``)。重启后若退回派生 key,就又钉回那个已关闭的
  父会话 —— 写全部失败,且每次压缩再分叉一个兄弟,直到血缘歧义无法自愈。
- **generation**:`/clear` 靠它产出 `-gN` 新 key。只存内存的话,重启等于把被清掉的
  会话原地复活。
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from nonebot import logger

# updated_at 列:只写不读(保留至 M2);不改 DB 结构,存量库保持原样。
_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS session_keys (
        internal_id  TEXT    PRIMARY KEY,
        session_key  TEXT    NOT NULL,
        generation   INTEGER NOT NULL DEFAULT 0,
        updated_at   REAL    NOT NULL
    )
    """,
    # 投递轮询水位持久化。原内存态重启即清+首轮播种跳过
    # 存量——nb 重启期间完成的子代理投递行被盖在水位下永不播报(维护者实测「重启后
    # 投递轮不到」)。落本库:水位本就按 session_key 记,与映射表同生命周期。
    """
    CREATE TABLE IF NOT EXISTS delivery_watermarks (
        session_key  TEXT    PRIMARY KEY,
        last_row_id  INTEGER NOT NULL,
        updated_at   REAL    NOT NULL
    )
    """,
    # 2026-10-17:/delay 暂停状态持久化(维护者确认要修)——原内存态重启即失效,
    # 被暂停会话重启后悄悄恢复 T1 计时。
    """
    CREATE TABLE IF NOT EXISTS idle_paused (
        internal_id  TEXT    PRIMARY KEY
    )
    """,
]


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
        self._closed = False

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
            # updated_at 只写不读(保留至 M2):随写入落库即可,无读点。
            self._conn.execute(
                "INSERT INTO session_keys (internal_id, session_key, generation, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(internal_id) DO UPDATE SET "
                "session_key=excluded.session_key, generation=excluded.generation, updated_at=excluded.updated_at",
                (internal_id, session_key, generation, now),
            )
        except Exception as exc:
            _log_store_failure("持久化 session key", exc, f" ({internal_id})")

    # --- 投递轮询水位持久化 ---

    def load_watermarks(self) -> dict[str, int]:
        """返回 session_key → 已扫描最大行 id。读失败返回空表(退化为旧播种语义)。"""
        try:
            rows = self._conn.execute(
                "SELECT session_key, last_row_id FROM delivery_watermarks"
            ).fetchall()
        except Exception as exc:
            logger.warning(f"[SESSION] 读取投递水位失败,本次退化为重启播种语义: {exc}")
            return {}
        return {r["session_key"]: r["last_row_id"] for r in rows}

    def put_watermark(self, session_key: str, last_row_id: int, *, now: float) -> None:
        try:
            self._conn.execute(
                "INSERT INTO delivery_watermarks (session_key, last_row_id, updated_at) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(session_key) DO UPDATE SET "
                "last_row_id=excluded.last_row_id, updated_at=excluded.updated_at",
                (session_key, last_row_id, now),
            )
        except Exception as exc:
            logger.warning(f"[SESSION] 持久化投递水位失败 ({session_key}): {exc}")

    def delete_watermarks(self, session_keys: list[str]) -> None:
        """清理已换代/消失会话的水位行(与内存侧清扫同步)。"""
        if not session_keys:
            return
        try:
            self._conn.executemany(
                "DELETE FROM delivery_watermarks WHERE session_key=?",
                [(k,) for k in session_keys],
            )
        except Exception as exc:
            logger.warning(f"[SESSION] 清理投递水位失败: {exc}")

    # --- 2026-10-17:/delay 暂停状态持久化 ---

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
