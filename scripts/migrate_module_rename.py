#!/usr/bin/env python3
"""模块改名冷迁移工具：同设备整目录改名，默认 dry-run，``--apply`` 才动盘。

把插件包目录、localstore 数据目录与缓存目录从旧模块名迁到新模块名：

    python scripts/migrate_module_rename.py \\
        --old-package <site-packages>/nonebot_plugin_hermes \\
        --new-package <site-packages>/nonebot_plugin_hermes_adapter \\
        --old-data ~/.local/share/nonebot2/nonebot_plugin_hermes \\
        --new-data ~/.local/share/nonebot2/nonebot_plugin_hermes_adapter \\
        --old-cache ~/.cache/nonebot2/nonebot_plugin_hermes \\
        --new-cache ~/.cache/nonebot2/nonebot_plugin_hermes_adapter
    # 加 --apply 才真正改名

纪律：
  - 六条路径全部由命令行显式给出，脚本不读 .env / 不 import nonebot / 不猜任何
    缺省目录；缺一半的成对参数直接拒绝。
  - 只做 ``os.rename``（同文件系统整目录搬动），不复制、不删除、不改文件内容，
    不打印任何文件内容，因此不会回显 secret。
  - 前置拒绝（零变更）：目标已存在、源/目标重叠、跨设备、源缺失或为符号链接、
    目标父目录不存在、数据目录存在 WAL/SHM/journal 热文件、任一 ``*.db`` 的
    ``PRAGMA integrity_check`` 非 ``ok``。
  - 迁移前后按“相对路径 / 大小 / sha256 清单”对账；任一项不符或中途异常，
    按逆序把已迁移项搬回旧名并复账，宁可整体回滚也不留半迁移状态。
  - 不碰 ``*.dist-info``/``*.egg-info``、venv ``bin``（console scripts）、不重启
    也不操作任何服务；带 WAL 的数据目录必须在停服窗口内执行本工具。

退出码：0 成功（或 dry-run 计划完成）；2 前置拒绝/参数不合法（零变更）；
3 迁移失败但已完整回滚；4 回滚不完整（需人工处理）。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

EXIT_OK = 0
EXIT_REFUSED = 2
EXIT_FAILED_ROLLED_BACK = 3
EXIT_ROLLBACK_INCOMPLETE = 4

_CHUNK = 1 << 20
_META_SUFFIXES = (".dist-info", ".egg-info")
_RESERVED_DIRS = frozenset({"bin", "Scripts"})


@dataclass(frozen=True)
class Item:
    """一对“旧目录 -> 新目录”迁移项。kind 仅用于报告与数据目录专项检查。"""

    kind: str
    old: Path
    new: Path


# --------------------------------------------------------------------------- #
# 清单与对账
# --------------------------------------------------------------------------- #


def manifest(root: Path) -> dict[str, tuple[int, str]]:
    """相对路径 -> (字节数, sha256)。符号链接按目标串登记，不跟随。"""

    out: dict[str, tuple[int, str]] = {}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode):
            target = os.readlink(path).encode("utf-8", "surrogateescape")
            out[rel] = (0, "link:" + hashlib.sha256(target).hexdigest())
        elif stat.S_ISREG(st.st_mode):
            digest = hashlib.sha256()
            with path.open("rb") as fh:
                for chunk in iter(lambda: fh.read(_CHUNK), b""):
                    digest.update(chunk)
            out[rel] = (st.st_size, digest.hexdigest())
    return out


def manifest_digest(m: dict[str, tuple[int, str]]) -> str:
    digest = hashlib.sha256()
    for rel in sorted(m):
        size, sha = m[rel]
        digest.update(f"{rel}\0{size}\0{sha}\n".encode("utf-8", "surrogateescape"))
    return digest.hexdigest()


def summarize(m: dict[str, tuple[int, str]]) -> tuple[int, int]:
    return len(m), sum(size for size, _ in m.values())


# --------------------------------------------------------------------------- #
# 前置检查
# --------------------------------------------------------------------------- #


def _st_dev(path: Path) -> int:
    return os.stat(path).st_dev


def db_integrity(root: Path) -> list[tuple[str, str]]:
    """数据目录里全部 ``*.db`` 的 (相对路径, integrity 结果)，只读打开。"""

    results: list[tuple[str, str]] = []
    for db in sorted(p for p in root.rglob("*.db") if p.is_file()):
        rel = db.relative_to(root).as_posix()
        try:
            # immutable=1：不加锁、不建 -shm/-journal，只读校验绝不扰动源目录
            uri = db.resolve().as_uri() + "?mode=ro&immutable=1"
            con = sqlite3.connect(uri, uri=True)
            try:
                row = con.execute("PRAGMA integrity_check").fetchone()
            finally:
                con.close()
            results.append((rel, str(row[0]) if row else "no-result"))
        except sqlite3.Error as exc:
            results.append((rel, f"error: {type(exc).__name__}: {exc}"))
    return results


def _hot_files(root: Path) -> tuple[list[str], list[str]]:
    wal: list[str] = []
    journal: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        name = path.name
        if name.endswith("-wal") or name.endswith("-shm"):
            wal.append(path.relative_to(root).as_posix())
        elif name.endswith("-journal"):
            journal.append(path.relative_to(root).as_posix())
    return wal, journal


def _check_path_shape(item: Item) -> list[str]:
    msgs: list[str] = []
    for side, path in (("源", item.old), ("目标", item.new)):
        if any(part.endswith(_META_SUFFIXES) for part in path.parts):
            msgs.append(f"[{item.kind}] {side}路径命中 dist-info/egg-info，元数据目录不随本工具迁移: {path}")
        if any(part in _RESERVED_DIRS for part in path.parts):
            msgs.append(f"[{item.kind}] {side}路径落在 bin/Scripts（console scripts）内，不由本工具处理: {path}")
    return msgs


def validate(items: list[Item]) -> list[str]:
    """返回全部拒绝原因；空列表表示可以动盘。"""

    msgs: list[str] = []
    kinds: set[str] = set()
    for item in items:
        item_msgs = _check_path_shape(item)
        if item.kind in kinds:
            item_msgs.append(f"[{item.kind}] 迁移项重复")
        kinds.add(item.kind)

        old, new = item.old, item.new
        if old.is_symlink():
            item_msgs.append(f"[{item.kind}] 源是符号链接，拒绝按目录改名: {old}")
        elif not old.exists():
            item_msgs.append(f"[{item.kind}] 源目录不存在: {old}")
        elif not old.is_dir():
            item_msgs.append(f"[{item.kind}] 源不是目录: {old}")

        if os.path.lexists(new):
            item_msgs.append(f"[{item.kind}] 目标已存在，拒绝覆盖: {new}")
        elif not new.parent.is_dir():
            item_msgs.append(f"[{item.kind}] 目标父目录不存在（本工具不建目录）: {new.parent}")

        try:
            real_old, real_new = old.resolve(), new.resolve()
        except OSError as exc:
            item_msgs.append(f"[{item.kind}] 路径解析失败: {exc}")
        else:
            if real_old == real_new or real_old in real_new.parents or real_new in real_old.parents:
                item_msgs.append(f"[{item.kind}] 源/目标重叠，拒绝迁移: {old} <-> {new}")

        # 同设备才谈得上 rename：源与目标父目录都在才判，逐项独立，不被别的项带偏
        if old.is_dir() and new.parent.is_dir():
            dev_old, dev_new = _st_dev(old), _st_dev(new.parent)
            if dev_old != dev_new:
                item_msgs.append(
                    f"[{item.kind}] 跨设备 rename 不可用（源 dev={dev_old}，目标父目录 dev={dev_new}）: {old} -> {new}"
                )
        msgs.extend(item_msgs)

    for item in items:
        if item.kind != "data" or not item.old.is_dir():
            continue
        wal, journal = _hot_files(item.old)
        if wal or journal:
            if wal:
                msgs.append(
                    f"[data] 数据目录存在 WAL/SHM 热文件，必须在停服并正常关闭连接后再迁移: {', '.join(wal)}"
                )
            if journal:
                msgs.append(
                    f"[data] 数据目录存在回滚日志 journal，必须在停服并正常关闭连接后再迁移: {', '.join(journal)}"
                )
            continue  # 热文件在场已决定拒绝，不再打开数据库（只读校验也可能扰动现场）
        for rel, status in db_integrity(item.old):
            if status != "ok":
                msgs.append(f"[data] {rel} 完整性检查不是 ok: {status}")
    return msgs


# --------------------------------------------------------------------------- #
# 迁移与回滚
# --------------------------------------------------------------------------- #


def apply_plan(items: list[Item], manifest_fn=manifest) -> tuple[int, list[str]]:
    """按给定顺序改名，逐项对账；异常则逆序回滚并复账。返回 (退出码, 报告行)。"""

    lines: list[str] = []
    pre = {item.kind: manifest_fn(item.old) for item in items}
    moved: list[Item] = []
    try:
        for item in items:
            os.rename(item.old, item.new)
            moved.append(item)
            lines.append(f"MOVED {item.kind} {item.old} -> {item.new}")
            post = manifest_fn(item.new)
            if post != pre[item.kind]:
                before_sha = manifest_digest(pre[item.kind])
                after_sha = manifest_digest(post)
                lines.append(
                    f"VERIFY mismatch {item.kind} before={before_sha} after={after_sha} "
                    f"files {len(pre[item.kind])}->{len(post)}"
                )
                raise RuntimeError(f"{item.kind} 迁移后文件清单/大小/sha256 对账不符")
            count, total = summarize(post)
            lines.append(
                f"VERIFY ok {item.kind} files={count} bytes={total} sha256={manifest_digest(post)}"
            )
    except Exception as exc:  # noqa: BLE001 - 任何异常都必须走回滚
        lines.append(f"APPLY FAILED: {exc}")
        failed: list[str] = []
        for item in reversed(moved):
            try:
                if item.new.exists() and not os.path.lexists(item.old):
                    os.rename(item.new, item.old)
                restored = manifest_fn(item.old)
                if restored != pre[item.kind]:
                    raise RuntimeError(
                        f"回滚后复账不符（files {len(pre[item.kind])}->{len(restored)}）"
                    )
                lines.append(f"ROLLBACK {item.kind} restored {item.old}")
            except Exception as rexc:  # noqa: BLE001
                failed.append(item.kind)
                lines.append(f"ROLLBACK-FAILED {item.kind} {item.new} -> {item.old}: {rexc}")
        if failed:
            lines.append(
                f"APPLY FAILED: rolled back {len(moved) - len(failed)}/{len(moved)} item(s); "
                f"未恢复项需人工处理: {', '.join(failed)}"
            )
            return EXIT_ROLLBACK_INCOMPLETE, lines
        lines.append(f"APPLY FAILED: rolled back {len(moved)} item(s); 目录已恢复迁移前状态")
        return EXIT_FAILED_ROLLED_BACK, lines

    lines.append(f"APPLIED: {len(items)} item(s) migrated, verified")
    return EXIT_OK, lines


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

_PAIRS = (("package", "--old-package", "--new-package"), ("data", "--old-data", "--new-data"), ("cache", "--old-cache", "--new-cache"))


def _build_items(args: argparse.Namespace) -> tuple[list[Item], list[str]]:
    items: list[Item] = []
    errors: list[str] = []
    for kind, old_flag, new_flag in _PAIRS:
        old_raw = getattr(args, old_flag.replace("--", "").replace("-", "_"))
        new_raw = getattr(args, new_flag.replace("--", "").replace("-", "_"))
        if bool(old_raw) != bool(new_raw):
            errors.append(f"[{kind}] {old_flag} 与 {new_flag} 必须成对给出")
            continue
        if not old_raw:
            continue
        items.append(Item(kind=kind, old=Path(old_raw).expanduser(), new=Path(new_raw).expanduser()))
    if not items and not errors:
        errors.append("至少要给出一对迁移目录（package/data/cache 任一）；本工具不猜任何缺省路径")
    return items, errors


def _refuse(msgs: list[str]) -> int:
    for msg in msgs:
        print(f"REFUSE {msg}")
    print("REFUSED: no changes made")
    return EXIT_REFUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="按显式路径把旧模块名的包/数据/缓存目录整体改名，默认 dry-run。",
    )
    for _kind, old_flag, new_flag in _PAIRS:
        parser.add_argument(old_flag, dest=old_flag.replace("--", "").replace("-", "_"), metavar="DIR")
        parser.add_argument(new_flag, dest=new_flag.replace("--", "").replace("-", "_"), metavar="DIR")
    parser.add_argument("--apply", action="store_true", help="真正改盘；缺省只打印计划")
    args = parser.parse_args(argv)

    items, errors = _build_items(args)
    if errors:
        return _refuse(errors)

    msgs = validate(items)
    if msgs:
        return _refuse(msgs)

    for item in items:
        current = manifest(item.old)
        count, total = summarize(current)
        print(
            f"PLAN {item.kind} {item.old} -> {item.new} files={count} bytes={total} "
            f"sha256={manifest_digest(current)}"
        )
        if item.kind == "data":
            for rel, status in db_integrity(item.old):
                print(f"db-integrity {'ok' if status == 'ok' else 'FAIL'} {rel}")

    if not args.apply:
        print("DRY-RUN: no changes made")
        print('NEXT: NoneBot 插件注册改为 nonebot-plugin-hermes-adapter = ["nonebot_plugin_hermes_adapter"]')
        return EXIT_OK

    code, lines = apply_plan(items)
    for line in lines:
        print(line)
    if code == EXIT_OK:
        print('NEXT: NoneBot 插件注册改为 nonebot-plugin-hermes-adapter = ["nonebot_plugin_hermes_adapter"]')
    return code


if __name__ == "__main__":
    sys.exit(main())
