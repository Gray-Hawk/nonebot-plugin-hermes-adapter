"""把仓库内网关用户插件（gateway_plugins/<name>/）安装到 Hermes 的 $HERMES_HOME/plugins/。

默认安装 session-compress 到 ~/.hermes/plugins/session-compress；肥鱼等独立 profile 用
--hermes-home 或 --dest 指定，不要手抄 .acceptance 里的副本。

在源码根目录使用：
    python scripts/install_gateway_plugin.py            # 安装/升级 session-compress
    python scripts/install_gateway_plugin.py --check    # 只比对，不写盘（退出码 0=一致）
    python scripts/install_gateway_plugin.py --force    # 覆盖已装的不同版本

安装完必须重启该实例的 gateway 进程才会加载插件（systemctl restart hermes-gateway 或
等价的受控重启）；本脚本不重启任何服务。
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
from pathlib import Path

# 只复制插件运行期需要的文件；tests/ 与 __pycache__ 不进目标机。
_PAYLOAD_SUFFIXES = {".py", ".yaml", ".yml", ".md"}
_SKIP_DIRS = {"__pycache__", "tests", ".pytest_cache"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _default_hermes_home() -> Path:
    import os

    env = os.environ.get("HERMES_HOME", "").strip()
    return Path(env) if env else Path.home() / ".hermes"


def _payload_files(src: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(src.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(src)
        if any(part in _SKIP_DIRS for part in rel.parts):
            continue
        if rel.suffix.lower() in _PAYLOAD_SUFFIXES:
            files.append(rel)
    return files


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _compare(src: Path, dest: Path, rels: list[Path]) -> list[str]:
    """返回不一致的文件（缺失或哈希不同）。"""
    diffs: list[str] = []
    for rel in rels:
        target = dest / rel
        if not target.is_file():
            diffs.append(f"missing: {rel}")
        elif _sha256(target) != _sha256(src / rel):
            diffs.append(f"differs: {rel}")
    if dest.is_dir():
        for path in dest.rglob("*.py"):
            rel = path.relative_to(dest)
            if rel not in rels:
                diffs.append(f"extra: {rel}")
    return diffs


def main() -> int:
    parser = argparse.ArgumentParser(description="Install a gateway user plugin from gateway_plugins/<name>/")
    parser.add_argument("name", nargs="?", default="session-compress", help="插件目录名（默认 session-compress）")
    parser.add_argument("--hermes-home", default="", help="Hermes 根目录（默认 $HERMES_HOME 或 ~/.hermes）")
    parser.add_argument("--dest", default="", help="直接指定目标目录（优先于 --hermes-home）")
    parser.add_argument("--force", action="store_true", help="目标已存在且内容不同时覆盖")
    parser.add_argument("--check", action="store_true", help="只比对不写盘；一致退出 0，不一致退出 1")
    args = parser.parse_args()

    src = _repo_root() / "gateway_plugins" / args.name
    if not (src / "__init__.py").is_file():
        print(f"[install_gateway_plugin] source not found: {src}/__init__.py", file=sys.stderr)
        return 2

    dest = Path(args.dest) if args.dest else Path(args.hermes_home or _default_hermes_home()) / "plugins" / args.name
    rels = _payload_files(src)
    if not rels:
        print(f"[install_gateway_plugin] no payload files under {src}", file=sys.stderr)
        return 2

    diffs = _compare(src, dest, rels)
    if args.check:
        if diffs:
            print(f"[install_gateway_plugin] NOT in sync: {dest}")
            for line in diffs:
                print(f"  {line}")
            return 1
        print(f"[install_gateway_plugin] in sync: {dest} ({len(rels)} files)")
        return 0

    if dest.exists() and diffs and not args.force:
        print(f"[install_gateway_plugin] target exists and differs: {dest}", file=sys.stderr)
        for line in diffs:
            print(f"  {line}", file=sys.stderr)
        print("[install_gateway_plugin] re-run with --force to overwrite", file=sys.stderr)
        return 1

    dest.mkdir(parents=True, exist_ok=True)
    for rel in rels:
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src / rel, target)

    remaining = _compare(src, dest, rels)
    if remaining:
        print("[install_gateway_plugin] copy verification FAILED:", file=sys.stderr)
        for line in remaining:
            print(f"  {line}", file=sys.stderr)
        return 1

    print(f"[install_gateway_plugin] installed {args.name} -> {dest} ({len(rels)} files, sha256 verified)")
    for rel in rels:
        print(f"  {rel}  {_sha256(dest / rel)[:12]}")
    print("[install_gateway_plugin] restart that instance's gateway to load the plugin")
    print('[install_gateway_plugin] route: POST /api/plugins/<name>/sessions/{session_id}/compress (Bearer API_SERVER_KEY)')
    return 0


if __name__ == "__main__":
    sys.exit(main())
