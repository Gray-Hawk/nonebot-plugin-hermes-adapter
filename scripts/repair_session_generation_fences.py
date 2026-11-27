#!/usr/bin/env python3
"""Repair session generations below legacy delivery fences.

Dry-run by default. ``--apply`` advances only affected sessions above the maximum
legacy/canonical fence and disarms idle actions until the next real chat.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.run_tracker import _domain_scope_key  # noqa: E402


@dataclass(frozen=True)
class RepairRow:
    internal_id: str
    old_session_key: str
    new_session_key: str
    old_generation: int
    new_generation: int
    fence_generation: int


def _scope_for_internal_id(internal_id: str) -> str:
    parts = internal_id.split('+', 2)
    if len(parts) != 3:
        return ''
    return f'{parts[0]}:{parts[1]}:{parts[2]}'


def scan(session_keys_path: str | Path, active_runs_path: str | Path) -> list[RepairRow]:
    runs = sqlite3.connect(f'file:{Path(active_runs_path)}?mode=ro', uri=True)
    try:
        fences: dict[str, int] = {}
        for scope, generation in runs.execute(
            'SELECT scope_key,fence_generation FROM delivery_generation_fences'
            ' WHERE fence_generation IS NOT NULL'
        ):
            canonical = _domain_scope_key(scope)
            fences[canonical] = max(fences.get(canonical, 0), int(generation))
    finally:
        runs.close()

    keys = sqlite3.connect(f'file:{Path(session_keys_path)}?mode=ro', uri=True)
    try:
        rows = keys.execute(
            'SELECT internal_id,session_key,generation FROM session_keys ORDER BY internal_id'
        ).fetchall()
    finally:
        keys.close()

    out: list[RepairRow] = []
    for internal_id, session_key, generation in rows:
        fence = fences.get(_scope_for_internal_id(internal_id))
        if fence is None or int(generation) >= fence:
            continue
        new_generation = fence + 1
        new_key, count = re.subn(r'-g\d+$', f'-g{new_generation}', session_key)
        if count != 1:
            raise RuntimeError(
                f'cannot derive replacement session key for {internal_id}: {session_key!r}'
            )
        out.append(RepairRow(
            internal_id=internal_id,
            old_session_key=session_key,
            new_session_key=new_key,
            old_generation=int(generation),
            new_generation=new_generation,
            fence_generation=fence,
        ))
    return out


def apply(
    session_keys_path: str | Path,
    active_runs_path: str | Path,
    *,
    now: float | None = None,
) -> list[RepairRow]:
    rows = scan(session_keys_path, active_runs_path)
    if not rows:
        return []
    stamp = time.time() if now is None else float(now)
    keys = sqlite3.connect(session_keys_path)
    try:
        keys.execute('BEGIN IMMEDIATE')
        for row in rows:
            cur = keys.execute(
                'UPDATE session_keys SET session_key=?,generation=?,updated_at=?,'
                ' last_activity=?,idle_armed=0 WHERE internal_id=? AND generation=?',
                (
                    row.new_session_key,
                    row.new_generation,
                    stamp,
                    stamp,
                    row.internal_id,
                    row.old_generation,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f'concurrent generation change: {row.internal_id}')
        check = keys.execute('PRAGMA quick_check').fetchone()[0]
        if check != 'ok':
            raise RuntimeError(f'session_keys quick_check={check}')
        keys.commit()
    except Exception:
        keys.rollback()
        raise
    finally:
        keys.close()
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('session_keys_db', type=Path)
    parser.add_argument('active_runs_db', type=Path)
    parser.add_argument('--apply', action='store_true', help='write repairs; default is dry-run')
    args = parser.parse_args()
    rows = apply(args.session_keys_db, args.active_runs_db) if args.apply else scan(
        args.session_keys_db, args.active_runs_db
    )
    print(json.dumps({
        'mode': 'apply' if args.apply else 'dry-run',
        'count': len(rows),
        'rows': [row.__dict__ for row in rows],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
