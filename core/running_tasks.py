"""后台「正在运行」任务的只读聚合（本部署补丁）。

`background_report_outbox` 只承载**已完成待汇报**的事件（看板唤醒种类、上游终态
事件），不是运行态查询：非终态任务不会进信箱。本模块补的是「运行中」这一侧，
只读复用两处**已有的真实登记**，不新建任务登记系统、不写任何库：

- ``kanban``：看板 SQLite 的 ``tasks`` 表里 ``status='running'`` 的卡片（创建者
  会话与本会话一致），是派发器真实在跑的 worker；
- ``delegation``：background-completion 插件登记库 ``bg_completions`` 里
  ``state='registered'`` 的行——``registered`` 的定义就是「已派发、等 durable
  终态」，终态才会迁走；这是子代理后台任务的真实登记。

两个来源都可能不可用（没装看板库 / 没装 background-completion 插件 / 读失败）。
调用方必须把「没有可读运行源」和「确实没有运行中任务」分开：前者不能据空列表
否定后台任务。

只读：自开 ``mode=ro`` 连接、用完即关，不认领、不销账、不改任何状态。
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# 看板里代表「真的在跑」的任务状态；ready/todo 是排队，不是运行。
KANBAN_RUNNING_STATUS = "running"
# background-completion 登记库里「已派发、等终态」的状态。
DELEGATION_RUNNING_STATE = "registered"


class RunningSourceError(RuntimeError):
    """真实运行源读不到：调用方按「运行态未知」处理，不当作没有任务。"""


def belongs(origin_session_id: str, prefix: str) -> bool:
    """会话键是否属于本会话（含 `-g<代际>` 变体，与汇报信箱同一份判定）。"""
    return bool(origin_session_id) and (
        origin_session_id == prefix or origin_session_id.startswith(prefix + "-g")
    )


def _belongs_clause(column: str) -> str:
    """SQL 版 ``belongs()``：``x = 前缀 OR x 以 '<前缀>-g' 开头``。

    会话归属必须在 SQL 侧先过滤、再限额：两个源都在别的会话里堆着活跃记录时，
    先取全局 ``LIMIT`` 再把不属于本会话的行丢掉，会把本会话真在跑的任务挤出窗口，
    返回「ok、没有在跑任务」的假空。``substr`` 而不是 ``LIKE``：前缀里可能有
    ``%``/``_``，LIKE 需要转义，前缀匹配用长度截取语义更贴 startswith。
    """
    return f"({column} = ? OR substr({column}, 1, ?) = ?)"


def _belongs_params(prefix: str) -> tuple[str, int, str]:
    """配合 :func:`_belongs_clause` 的三个占位参数。"""
    return prefix, len(prefix) + 2, prefix + "-g"


def _connect_ro(db_path: str | Path) -> sqlite3.Connection:
    path = str(db_path)
    if not Path(path).is_file():
        raise RunningSourceError(f"database not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


class RunningSource(Protocol):
    name: str

    def running(self, *, session_prefix: str, limit: int) -> list[dict[str, Any]]: ...


class KanbanRunningSource:
    """看板 ``tasks.status='running'`` 的真实运行任务。"""

    name = "kanban"

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

    def running(self, *, session_prefix: str, limit: int = 20) -> list[dict[str, Any]]:
        if not session_prefix:  # 与 belongs() 同口径:空前缀不归属任何会话
            return []
        conn = _connect_ro(self.db_path)
        try:
            # 只按真实运行状态筛；会话归属（含 `-g` 变体）下推到 SQL 再限额。
            # 排序用 rowid（tasks 必有）而不是 started_at：老库/夹具可能没有该列。
            rows = conn.execute(
                f"SELECT * FROM tasks WHERE status=? AND {_belongs_clause('session_id')}"
                " ORDER BY rowid DESC LIMIT ?",
                (KANBAN_RUNNING_STATUS, *_belongs_params(session_prefix), max(limit, 1)),
            ).fetchall()
        except sqlite3.Error as exc:
            raise RunningSourceError(f"kanban read failed: {exc}") from exc
        finally:
            conn.close()
        items: list[dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            session_key = str(data.get("session_id") or "")
            if not belongs(session_key, session_prefix):
                # 理论上不可达（SQL/Python 判定同构）；真出现就留痕，且绝不静默丢行。
                logger.warning(
                    f"[HERMES running] kanban session mismatch: {session_key!r} "
                    f"prefix={session_prefix!r}"
                )
                continue
            items.append({
                "task_id": str(data.get("id") or ""),
                "title": str(data.get("title") or ""),
                "source": self.name,
                "assignee": str(data.get("assignee") or ""),
                "started_at": data.get("started_at"),
                "run_id": str(data.get("current_run_id") or ""),
                "last_heartbeat_at": data.get("last_heartbeat_at"),
            })
            if len(items) >= limit:
                break
        return items


class DelegationRunningSource:
    """background-completion 登记库里「已派发未终态」的后台子代理任务。"""

    name = "delegation"

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

    def running(self, *, session_prefix: str, limit: int = 20) -> list[dict[str, Any]]:
        if not session_prefix:  # 与 belongs() 同口径:空前缀不归属任何会话
            return []
        conn = _connect_ro(self.db_path)
        try:
            # 会话归属（含 `-g` 变体）与已派发未终态一起下推到 SQL 再限额。
            rows = conn.execute(
                "SELECT delegation_id, origin_session_id, dispatched_at"
                " FROM bg_completions WHERE state=? AND acked=0"
                f" AND {_belongs_clause('origin_session_id')}"
                " ORDER BY created_at DESC LIMIT ?",
                (DELEGATION_RUNNING_STATE, *_belongs_params(session_prefix), max(limit, 1)),
            ).fetchall()
        except sqlite3.Error as exc:
            raise RunningSourceError(f"delegation registry read failed: {exc}") from exc
        finally:
            conn.close()
        items: list[dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            session_key = str(data.get("origin_session_id") or "")
            if not belongs(session_key, session_prefix):
                # 理论上不可达（SQL/Python 判定同构）；真出现就留痕，且绝不静默丢行。
                logger.warning(
                    f"[HERMES running] delegation session mismatch: {session_key!r} "
                    f"prefix={session_prefix!r}"
                )
                continue
            items.append({
                "task_id": str(data.get("delegation_id") or ""),
                "title": "",
                "source": self.name,
                "assignee": "",
                "started_at": data.get("dispatched_at"),
                "run_id": "",
                "last_heartbeat_at": None,
            })
            if len(items) >= limit:
                break
        return items


def collect_running(
    sources: list[RunningSource] | tuple[RunningSource, ...] | None,
    *,
    session_prefix: str,
    limit: int = 20,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """聚合多个真实运行源：返回 (运行中任务, 读到的来源名, 读失败的来源描述)。

    单个来源失败只记该来源不可用，不影响其他来源；同一 task_id 以先到者为准。
    """
    items: list[dict[str, Any]] = []
    ok_sources: list[str] = []
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for source in sources or ():
        name = str(getattr(source, "name", type(source).__name__))
        try:
            found = source.running(session_prefix=session_prefix, limit=limit)
        except RunningSourceError as exc:
            errors.append(f"{name}: {exc}")
            continue
        except Exception as exc:  # 只读查询不允许把查询工具本身拖垮
            errors.append(f"{name} read failed: {type(exc).__name__}")
            continue
        ok_sources.append(name)
        for item in found:
            key = (str(item.get("source") or name), str(item.get("task_id") or ""))
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
    return items[:limit], ok_sources, errors
