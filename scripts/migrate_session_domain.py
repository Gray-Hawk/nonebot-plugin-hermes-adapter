#!/usr/bin/env python3
"""一次性把 session_keys.db 的旧协议 internal_id 归一到 QQTPC 业务会话域。

只改 ``session_keys.internal_id`` 与 ``idle_paused.internal_id``：
``session_key`` / ``generation`` / ``updated_at`` 原样保留，**不碰** state.db、
消息库或任何转录。真实 adapter 不落库，运行时由当前 Bot 连接／事件／路由解析。

典型用法（主实例：OneBot 主会话 + Milky 测试小会话共存）：
    # 1. 先 dry-run，明确看到唯一冲突来自「milky测试」小会话
    python3 scripts/migrate_session_domain.py --db <session_keys.db> --dry-run \
        --exclude 'milky+private+<USER_ID>'
    # 2. 显式排除那条小会话后执行；小会话原样保留（不收录、不删除）
    python3 scripts/migrate_session_domain.py --db <session_keys.db> \
        --exclude 'milky+private+<USER_ID>'

需要「先合并转录、再指向合并目标」的群（如主实例 253384307）用 --override：
    python3 scripts/migrate_session_domain.py --db <session_keys.db> \
        --exclude 'milky+group+253384307' \
        --override 'onebotv11+group+253384307=<合并后的 session_key>'

冲突（同一目标已有行／两种协议指向同一目标）时整批零写入并退出码 2；先 dry-run
确认，再用 ``--exclude``/``--override`` 显式指定运维认可的目标后重跑。脚本可重放：
已归一的库第二次运行计数为 0。
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401  按真实包名导入本工作树

from nonebot_plugin_hermes_adapter.core.session import SESSION_DOMAIN_TRANSPORTS  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionDomainMigrationConflict,
    SessionKeyStore,
)


def _excludes(items: list[str]) -> tuple[str, ...]:
    out: list[str] = []
    for item in items:
        for part in str(item or "").split(","):
            part = part.strip()
            if part and part not in out:
                out.append(part)
    return tuple(out)


def _generation_of(session_key: str) -> int:
    """从本地派生 key 的 ``-gN`` 后缀取 generation；上游 uuid 视为 0。"""
    match = re.search(r"-g(\d+)$", session_key)
    return int(match.group(1)) if match else 0


def _overrides(items: list[str]) -> dict[str, tuple[str, int]]:
    out: dict[str, tuple[str, int]] = {}
    for item in items:
        old, sep, key = str(item or "").partition("=")
        old, key = old.strip(), key.strip()
        if not sep or not old or not key:
            raise SystemExit(f"--override expects OLD_INTERNAL_ID=SESSION_KEY, got {item!r}")
        out[old] = (key, _generation_of(key))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="session_keys.db 路径")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只算计划与冲突，不写库",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="INTERNAL_ID",
        help="显式排除的旧 internal_id（可多次/逗号分隔）；不收录、不删除",
    )
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        metavar="INTERNAL_ID=SESSION_KEY",
        help=(
            "定制归一目标（可多次）：先从上游合并转录，再把该旧行归一到指定 "
            "session_key；generation 从 key 的 -gN 后缀推断"
        ),
    )
    args = parser.parse_args(argv)

    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"session_keys.db not found: {db_path}", file=sys.stderr)
        return 1

    store = SessionKeyStore(db_path)
    try:
        try:
            report = store.migrate_session_domain(
                SESSION_DOMAIN_TRANSPORTS,
                dry_run=args.dry_run,
                exclude=_excludes(args.exclude),
                overrides=_overrides(args.override),
            )
        except SessionDomainMigrationConflict as exc:
            print(f"CONFLICT (zero writes): {exc}", file=sys.stderr)
            return 2
    finally:
        store.close()

    mode = "dry-run" if report.dry_run else "applied"
    print(
        f"{mode}: session_keys={report.session_keys_migrated} "
        f"idle_paused={report.idle_paused_migrated} "
        f"excluded={list(report.excluded)} "
        f"overrides={report.overrides_applied} "
        f"ignored_overrides={list(report.overrides_ignored)} "
        f"integrity_ok={report.integrity_ok}"
    )
    return 0 if report.integrity_ok else 3


if __name__ == "__main__":
    raise SystemExit(main())
