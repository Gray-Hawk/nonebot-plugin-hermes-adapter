#!/usr/bin/env python3
"""公开 wheel 发行面审计：只读解包，不安装、不改文件。

发行边界（主人 2026-11-14 决定）：公开 wheel 只保留插件运行需要的安装文件。
tests/、scripts/、gateway_plugins/、turn_tests/、verify.py 等开发文件留在源码仓库，
不得出现在 wheel 里——它们带个人主机路径与内部取证群号，是上传前的硬门槛。

本脚本是「反向断言 + 结构/完整性核对」：正向断言运行包该有的文件都在（防止排除过头
产出空壳 wheel），反向断言开发目录/历史证据/缓存/数据库一个都不在。RECORD 逐条核
sha256 与大小，元数据核分发名、版本、top_level。

用法：
    python scripts/audit_public_wheel.py <wheel> [--expect-version 0.2.0]

退出码：0 全过；1 任一断言失败（逐条打印）；2 参数/读取错误。
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import sys
import zipfile
from pathlib import PurePosixPath

PKG = "nonebot_plugin_agent_adapter"
DIST = "nonebot-plugin-agent-adapter"

# 必须存在的运行时载荷（少一个就是排除过头或打包失败）。
REQUIRED = (
    f"{PKG}/__init__.py",
    f"{PKG}/config.py",
    f"{PKG}/utils.py",
    f"{PKG}/adapter_norm.py",
    f"{PKG}/core/__init__.py",
    f"{PKG}/handlers/__init__.py",
    f"{PKG}/mcp/__init__.py",
    f"{PKG}/tasks/__init__.py",
    f"{PKG}/backends/__init__.py",
    f"{PKG}/skill/SKILL.md",
)

# 反向断言：开发文件与历史证据前缀（相对 wheel 根或包内）。
DEV_ONLY = (
    f"{PKG}/tests/",
    f"{PKG}/scripts/",
    f"{PKG}/gateway_plugins/",
    f"{PKG}/turn_tests/",
    f"{PKG}/verify.py",
)
HISTORY_PREFIXES = ("EVIDENCE", "evidence_", "DELIVERY", "review_t_", "SAMPLES")
FORBIDDEN_SUFFIXES = (".pyc", ".pyo", ".db", ".db-wal", ".db-shm", ".sqlite", ".sqlite3",
                      ".log", ".swp", ".tmp", ".bak")
FORBIDDEN_PARTS = ("__pycache__", ".git", ".venv")


class Report:
    def __init__(self) -> None:
        self.failed: list[str] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
        if not ok:
            self.failed.append(name)


def record_digest(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="公开 wheel 发行面审计（只读）")
    parser.add_argument("wheel")
    parser.add_argument("--expect-version", default=None)
    args = parser.parse_args(argv[1:])

    report = Report()
    try:
        archive = zipfile.ZipFile(args.wheel)
    except (OSError, zipfile.BadZipFile) as exc:
        print(f"ERROR: 无法打开 wheel: {exc}", file=sys.stderr)
        return 2

    with archive:
        names = archive.namelist()
        infos = {info.filename: info for info in archive.infolist()}
        duplicates = sorted({name for name in names if names.count(name) > 1})
        unsafe = [
            name for name in names
            if name.startswith("/") or ".." in PurePosixPath(name).parts or "\\" in name
        ]
        report.check("无重复成员/路径穿越/绝对路径", not duplicates and not unsafe,
                     f"重复={duplicates[:3]} 不安全={unsafe[:3]}")

        dist_infos = sorted({PurePosixPath(name).parts[0] for name in names
                             if PurePosixPath(name).parts[0].endswith(".dist-info")})
        tops = sorted({PurePosixPath(name).parts[0] for name in names})
        unexpected_tops = [t for t in tops if t != PKG and not t.endswith(".dist-info")]
        report.check("顶层只有运行包与 dist-info", not unexpected_tops,
                     f"多出={unexpected_tops[:5]} 顶层={tops}")

        missing = [path for path in REQUIRED if path not in infos]
        report.check("运行时载荷齐全（反向断言的正向对照）", not missing, f"缺={missing}")

        dev_hits = [name for name in names if name.startswith(DEV_ONLY)]
        history_hits = [
            name for name in names
            if PurePosixPath(name).parts[0].startswith(HISTORY_PREFIXES)
            or any(part.startswith(HISTORY_PREFIXES) for part in PurePosixPath(name).parts)
        ]
        cache_hits = [name for name in names if any(part in FORBIDDEN_PARTS
                                                    for part in PurePosixPath(name).parts)]
        suffix_hits = [name for name in names if name.endswith(FORBIDDEN_SUFFIXES)]
        report.check("不含开发文件（tests/scripts/gateway_plugins/turn_tests/verify.py）",
                     not dev_hits, f"命中={dev_hits[:5]}")
        report.check("不含历史证据/交付物/样例目录", not history_hits, f"命中={history_hits[:5]}")
        report.check("不含缓存/虚拟环境/git 目录", not cache_hits, f"命中={cache_hits[:5]}")
        report.check("不含字节码/数据库/日志/临时文件", not suffix_hits, f"命中={suffix_hits[:5]}")

        meta_name = next((d for d in dist_infos if d.startswith(f"{PKG}-")), None)
        report.check("恰好一个本分发的 dist-info", len(dist_infos) == 1 and meta_name is not None,
                     f"dist-info={dist_infos}")
        if meta_name:
            metadata = archive.read(f"{meta_name}/METADATA").decode("utf-8", "replace")
            fields = {}
            for line in metadata.splitlines():
                if line[:1].isalpha() and ":" in line:
                    key, _, value = line.partition(":")
                    fields.setdefault(key.strip(), value.strip())
            version = fields.get("Version", "")
            report.check("METADATA 分发名/版本正确",
                         fields.get("Name") == DIST and bool(version)
                         and (args.expect_version is None or version == args.expect_version),
                         f"Name={fields.get('Name')} Version={version}")
            top_level_path = f"{meta_name}/top_level.txt"
            top_level = (archive.read(top_level_path).decode("utf-8").split()
                         if top_level_path in infos else [])
            report.check("top_level.txt 只声明运行包", top_level == [PKG], f"top_level={top_level}")

            record_path = f"{meta_name}/RECORD"
            if record_path not in infos:
                report.check("RECORD 存在并覆盖全部文件", False, "缺 RECORD")
            else:
                rows = {}
                for line in archive.read(record_path).decode("utf-8").splitlines():
                    if not line.strip():
                        continue
                    parts = line.split(",")
                    rows[parts[0]] = parts[1:]
                record_problems: list[str] = []
                for name, info in infos.items():
                    if name == record_path:
                        continue
                    row = rows.get(name)
                    if row is None:
                        record_problems.append(f"未记录 {name}")
                        continue
                    want_digest = row[0] if row else ""
                    want_size = row[1] if len(row) > 1 else ""
                    data = archive.read(name)
                    if want_digest != record_digest(data):
                        record_problems.append(f"sha256 不符 {name}")
                    if want_size != str(len(data)):
                        record_problems.append(f"大小不符 {name}")
                extra = sorted(set(rows) - set(infos))
                if extra:
                    record_problems.append(f"记录多出 {extra[:3]}")
                report.check("RECORD 覆盖全部文件且 sha256/大小一致", not record_problems,
                             "; ".join(record_problems[:5]))

        py_total = sum(1 for name in names if name.endswith(".py"))
        print(f"    条目={len(names)} .py={py_total} 文件大小={os.path.getsize(args.wheel)}")
        print(f"    分发={meta_name}")

    if report.failed:
        print(f"\nFAILED: {len(report.failed)} 项")
        return 1
    print("\nALL PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
