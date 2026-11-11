"""Kanban：新卡不再产生 api_server 自投订阅（窄补丁，不动通知器/看板/worker）。

为什么是这一层：``api_server`` 订阅是无状态目标，通知器只能把终态自投内部
``/v1/chat/completions``——那一轮跑在网关里，回应无人接收，NB 侧没有 run/回执，
QQ 上永远收不到。本部署的提醒改由 NB ``core.kanban_report_source`` 从
``tasks.session_id`` 路由（后台汇报信箱＋真 run），所以 api_server 自投行只剩噪声。

自动产生这些行的入口（本补丁逐个堵住，只堵自动的）：

1. ``tools.kanban_tools._maybe_auto_subscribe``——``kanban_create`` 建卡后按调用会话的
   ``HERMES_SESSION_PLATFORM`` 自动订阅；NB 驱动的回合平台就是 ``api_server``。
2. ``hermes_cli.kanban_db._inherit_notify_subs``——子卡继承父卡的订阅行（含历史
   api_server 行），新卡因此继续长出 api_server 行。
3. ``gateway.slash_commands.GatewaySlashCommandsMixin._kanban_auto_subscribe``——
   ``/kanban create`` 从 api_server 事件进来时同样会写一行。

明确不动：通知器（``gateway/kanban_watchers*``）、dispatcher、worker、看板首页订阅
（``plugins/kanban/dashboard``，那是用户点出来的真实 home 平台）、CLI 显式
``notify-subscribe``，以及其他平台的自动订阅（telegram/discord/… 行为原样）。
不写轮询清理器：存量 api_server 行的注销是部署步骤，由
``scripts/kanban_notify_handover.py --apply`` 一次性完成（本卡只 dry-run）。
"""
from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

PATCH_ID = "kanban-api-server-notify-guard/1"
_MARK = "__kanban_api_server_notify_guard__"
_API_SERVER = "api_server"


@dataclass(frozen=True)
class InstallState:
    auto_subscribe: str = "unchanged"
    inherit: str = "unchanged"
    slash: str = "unchanged"

    def describe(self) -> str:
        return (f"auto_subscribe={self.auto_subscribe} inherit={self.inherit} "
                f"slash={self.slash}")


def _platform_value(platform) -> str:
    return str(getattr(platform, "value", platform) or "").strip().lower()


def _guard_maybe_auto_subscribe(kanban_tools) -> str:
    current = kanban_tools._maybe_auto_subscribe
    mark = getattr(current, _MARK, None)
    if mark == PATCH_ID:
        return "already-installed"
    if mark is not None:
        raise RuntimeError(f"_maybe_auto_subscribe already carries {mark!r}")
    original = current
    resolve = kanban_tools._resolve_notify_target

    def _maybe_auto_subscribe(conn, task_id):
        try:
            target = resolve() or {}
        except Exception:  # an unresolvable session keeps the stock behaviour
            return original(conn, task_id)
        if _platform_value(target.get("platform")) == _API_SERVER:
            # The NB report source delivers this card's terminal events from task.session_id;
            # an api_server subscription would only self-post into a turn nobody reads.
            logger.info("kanban auto-subscribe skipped for %s: api_server self-notification is "
                        "not a delivery path (NB report source keeps it)", task_id)
            return False
        return original(conn, task_id)

    setattr(_maybe_auto_subscribe, _MARK, PATCH_ID)
    kanban_tools._maybe_auto_subscribe = _maybe_auto_subscribe
    return "installed"


def _sub_keys(conn, task_id):
    rows = conn.execute(
        "SELECT platform, chat_id, COALESCE(thread_id, '') FROM kanban_notify_subs WHERE task_id=?",
        (task_id,),
    ).fetchall()
    return {(str(row[0] or ""), str(row[1] or ""), str(row[2] or "")) for row in rows}


def _guard_inherit(kanban_db) -> str:
    current = kanban_db._inherit_notify_subs
    mark = getattr(current, _MARK, None)
    if mark == PATCH_ID:
        return "already-installed"
    if mark is not None:
        raise RuntimeError(f"_inherit_notify_subs already carries {mark!r}")
    original = current

    def _inherit_notify_subs(conn, child_id, parents, *, created_at=None):
        before = _sub_keys(conn, child_id)
        original(conn, child_id, parents, created_at=created_at)
        for platform, chat_id, thread_id in sorted(_sub_keys(conn, child_id) - before):
            if platform.lower() != _API_SERVER:
                continue
            # Inherited api_server rows are dead weight for the child too (same stateless
            # chat_id); drop only the row this inheritance just copied.
            conn.execute(
                "DELETE FROM kanban_notify_subs WHERE task_id=? AND platform=? AND chat_id=? "
                "AND COALESCE(thread_id, '')=?",
                (child_id, platform, chat_id, thread_id),
            )
            logger.info("kanban inherit skipped api_server subscription for %s (chat %s)",
                        child_id, chat_id)

    setattr(_inherit_notify_subs, _MARK, PATCH_ID)
    kanban_db._inherit_notify_subs = _inherit_notify_subs
    return "installed"


def _guard_slash(slash_commands) -> str:
    target = slash_commands.GatewaySlashCommandsMixin
    current = target._kanban_auto_subscribe
    mark = getattr(current, _MARK, None)
    if mark == PATCH_ID:
        return "already-installed"
    if mark is not None:
        raise RuntimeError(f"_kanban_auto_subscribe already carries {mark!r}")
    if not inspect.iscoroutinefunction(current):
        raise RuntimeError("GatewaySlashCommandsMixin._kanban_auto_subscribe is not a coroutine")
    original = current

    async def _kanban_auto_subscribe(self, event, task_id, requested_board):
        source = getattr(event, "source", None)
        if _platform_value(getattr(source, "platform", None)) == _API_SERVER:
            logger.info("kanban create auto-subscribe skipped for %s: api_server source", task_id)
            return False
        return await original(self, event, task_id, requested_board)

    setattr(_kanban_auto_subscribe, _MARK, PATCH_ID)
    target._kanban_auto_subscribe = _kanban_auto_subscribe
    return "installed"


def install(kanban_tools=None, kanban_db=None, slash_commands=None) -> InstallState:
    """Guard the three automatic api_server subscription entries once."""
    if kanban_tools is None:
        from tools import kanban_tools as kanban_tools
    if kanban_db is None:
        from hermes_cli import kanban_db as kanban_db
    if slash_commands is None:
        from gateway import slash_commands as slash_commands
    return InstallState(
        auto_subscribe=_guard_maybe_auto_subscribe(kanban_tools),
        inherit=_guard_inherit(kanban_db),
        slash=_guard_slash(slash_commands),
    )
