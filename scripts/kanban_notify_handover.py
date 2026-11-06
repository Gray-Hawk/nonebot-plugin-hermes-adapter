#!/usr/bin/env python3
"""Kanban→NB 提醒交接：把「api_server 自投订阅」从通知器交给后台汇报信箱。

为什么需要交接
--------------
Kanban 通知器对 ``api_server`` 订阅只能自投内部 ``/v1/chat/completions``：
那一轮 turn 会跑，但调用方是通知器自己，回应无人接收，NB 侧没有
``run_submissions``／``run_deliveries``，QQ 上收不到。交接之后该目标的终态事件
由 ``core.kanban_report_source`` 登记进后台汇报信箱，由 NB 在正常会话里跑成
turn（真 run ＋ 真 QQ 实发回执）。

只用上游公开 CLI（``hermes kanban notify-list --json`` / ``notify-unsubscribe``），
不直接写看板库。默认 dry-run，加 ``--apply`` 才真的注销。

用法：
    python3 scripts/kanban_notify_handover.py                     # 只列候选
    python3 scripts/kanban_notify_handover.py --apply             # 执行注销
    python3 scripts/kanban_notify_handover.py --board default --apply
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests import _bootstrap  # noqa: E402,F401  按真实包名导入本工作树

from nonebot_plugin_hermes_adapter.core.kanban_report_source import (  # noqa: E402
    select_handover_subs,
)


def _hermes() -> str:
    exe = shutil.which("hermes")
    if not exe:
        raise SystemExit("找不到 hermes CLI：交接必须走上游公开命令")
    return exe


def list_subs(board: str | None) -> list[dict]:
    cmd = [_hermes(), "kanban"]
    if board:
        cmd += ["--board", board]
    cmd += ["notify-list", "--json"]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise SystemExit(f"notify-list 失败({out.returncode}): {out.stderr.strip()[:300]}")
    payload = out.stdout.strip()
    if not payload:
        return []
    try:
        data = json.loads(payload)
    except ValueError:
        raise SystemExit("notify-list --json 输出不是 JSON")
    if isinstance(data, dict):
        data = data.get("subscriptions") or data.get("data") or []
    return [row for row in data if isinstance(row, dict)]


def unsubscribe(board: str | None, sub: dict) -> bool:
    cmd = [_hermes(), "kanban"]
    if board:
        cmd += ["--board", board]
    cmd += [
        "notify-unsubscribe",
        str(sub.get("task_id")),
        "--platform", str(sub.get("platform")),
        "--chat-id", str(sub.get("chat_id")),
    ]
    if sub.get("thread_id"):
        cmd += ["--thread-id", str(sub["thread_id"])]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        print(f"  [FAIL] {sub.get('task_id')}: {out.stderr.strip()[:200]}")
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="把 api_server 通知订阅交接给 NB 后台汇报信箱")
    parser.add_argument("--board", default=None, help="看板 slug（默认当前板）")
    parser.add_argument("--apply", action="store_true", help="真的执行注销（默认 dry-run）")
    args = parser.parse_args()

    subs = list_subs(args.board)
    candidates = select_handover_subs(subs)
    print(f"通知订阅共 {len(subs)} 条，其中需交接 {len(candidates)} 条")
    for sub in candidates:
        print(
            f"- {sub.get('task_id')} platform={sub.get('platform')} "
            f"chat={sub.get('chat_id')} mode={sub.get('delivery_mode', '?')}"
        )
    if not candidates:
        return 0
    if not args.apply:
        print("\n(dry-run) 确认无误后加 --apply 执行注销")
        return 0

    failed = 0
    for sub in candidates:
        if unsubscribe(args.board, sub):
            print(f"  [OK] {sub.get('task_id')} 已交接")
        else:
            failed += 1
    print(f"完成：{len(candidates) - failed} 成功，{failed} 失败")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
