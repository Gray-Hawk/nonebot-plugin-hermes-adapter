#!/usr/bin/env python3
"""上传硬门：白名单、署名、交付物与提交说明。

同一份门同时服务三个入口，避免出现第二套发布规则：

* ``verify.py``：验收时 ``--tree HEAD --commits HEAD``；
* 上传入口 ``scripts/dev_sync.sh``：提交前 ``--index``，推送前 ``--push-ready``；
* 主代理验货后 fast-forward 之前：``--push-ready --remote-ref <sha>``。

退出码：0 = 通过；2 = 违规；3 = 用法错误。
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ALLOWED_AUTHORS = {
    "NormiAgent <344182818+NormiAgent@users.noreply.github.com>",
    "Gray Hawk <34613788+Gray-Hawk@users.noreply.github.com>",
}

ALLOWED_TOP_DIRS = {
    "backends",
    "core",
    "gateway_plugins",
    "handlers",
    "mcp",
    "SAMPLES",
    "scripts",
    "skill",
    "tasks",
    "tests",
    "turn_tests",
}
ALLOWED_ROOT_FILES = {
    ".env.example",
    "LICENSE",
    "NoneBotPlugin.svg",
    "README.md",
    "__init__.py",
    "adapter_norm.py",
    "config.py",
    "pyproject.toml",
    "setup.py",
    "utils.py",
    "verify.py",
}
DENY_DIRS = {"__pycache__", ".pytest_cache", ".mypy_cache", "EVIDENCE", "ACCEPTANCE", "REVIEW"}
DENY_SUFFIXES = (".pyc", ".pyo", ".log", ".orig", ".rej", ".bak", ".swp")
DENY_PREFIXES = ("DELIVERY_",)

CONVENTIONAL = re.compile(
    r"^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([a-z0-9._/-]+\))?!?: \S.*$"
)
CJK = re.compile(r"[\u4e00-\u9fff]")

# 内部标识在源码里按片段拼接，避免这道门自己把内部主机名／实例名写进公开仓库。
HOME_PATH = "/home/" + r"(neonsky|feiyu)"
HOST_NAME = "VM-" + "101"
INSTANCE_NAMES = "|".join(["肥" + "鱼", "neon" + "bot", "nb_" + "neon" + "bot", "nb_" + "feiyu"])

FORBIDDEN_MESSAGE = (
    (re.compile(r"t_[0-9a-f]{6,}"), "内部卡号"),
    (re.compile(r"候选"), "过程性噪声"),
    (re.compile(r"返修|复审|返工"), "过程性噪声"),
    (re.compile(r"第\s*\d+\s*轮"), "过程性噪声"),
    (re.compile(r"\b[RN]\d+([-–~][RN]?\d+)*\b"), "轮次编号"),
    (re.compile(r"\b(RED|GREEN)\b"), "测试流水账"),
    (re.compile(r"\bverify\b\s*\d+/\d+"), "测试流水账"),
    (re.compile(r"\b\d+\s*OK\b"), "测试流水账"),
    (re.compile(r"\b\d+/\d+\b"), "测试流水账"),
    (re.compile(r"全量\s*\d+"), "测试流水账"),
    (re.compile(INSTANCE_NAMES), "内部实例名"),
    (re.compile(r"\b\d{9,10}\b"), "真实 QQ 号"),
    (re.compile(r"(?i)deepseek|claude|codex|gpt-?4|gemini"), "模型名"),
    (re.compile(HOME_PATH), "内部路径"),
    (re.compile(HOST_NAME), "内部主机名"),
    (re.compile(r"(?i)sk-[A-Za-z0-9]|token\s*="), "疑似凭据"),
)


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def git(*args: str, root: Path | None = None) -> subprocess.CompletedProcess:
    base = root or repo_root()
    return subprocess.run(
        ["git", "-c", f"safe.directory={base}", *args],
        cwd=str(base), capture_output=True, text=True,
    )


def path_violations(path: str) -> list[str]:
    parts = path.split("/")
    if any(part in DENY_DIRS for part in parts):
        return [f"{path}: 证据目录或编译缓存"]
    name = parts[-1]
    if name.startswith(DENY_PREFIXES) and name.endswith(".md"):
        return [f"{path}: 交付说明"]
    if name.endswith(DENY_SUFFIXES):
        return [f"{path}: 缓存或中间产物"]
    if len(parts) == 1:
        if name in ALLOWED_ROOT_FILES or name.endswith(".example.yaml"):
            return []
        return [f"{path}: 不在根目录白名单"]
    if parts[0] not in ALLOWED_TOP_DIRS:
        return [f"{path}: 不在白名单目录"]
    return []


def message_violations(subject: str, body: str = "") -> list[str]:
    out: list[str] = []
    if not CONVENTIONAL.match(subject):
        out.append(f"提交说明不符合 Conventional Commits：{subject}")
    else:
        matched = CONVENTIONAL.match(subject)
        assert matched is not None
        if not CJK.search(matched.group(0).split(":", 1)[-1]):
            out.append(f"提交说明缺少中文功能描述：{subject}")
    if len(subject) > 100:
        out.append(f"提交说明过长（{len(subject)} 字符）：{subject}")
    text = f"{subject}\n{body}"
    for pattern, label in FORBIDDEN_MESSAGE:
        hit = pattern.search(text)
        if hit:
            out.append(f"提交说明含{label}：{hit.group(0)!r}（{subject}）")
    return out


def author_violations(identity: str) -> list[str]:
    return [] if identity in ALLOWED_AUTHORS else [f"署名不在白名单：{identity}"]


def check_tree(rev: str, root: Path | None = None) -> list[str]:
    proc = git("ls-tree", "-r", "--name-only", "-z", rev, root=root)
    if proc.returncode != 0:
        return [f"读不到 {rev}：{proc.stderr.strip()}"]
    return [v for path in proc.stdout.split("\0") if path for v in path_violations(path)]


def check_commits(rev: str, root: Path | None = None) -> list[str]:
    """审计 rev 及其全部祖先。"""
    out: list[str] = []
    meta = git("log", "--format=%H%x1f%an <%ae>%x1f%cn <%ce>%x1f%s%x1f%b%x1e", rev, root=root)
    if meta.returncode != 0:
        return [f"读不到历史 {rev}：{meta.stderr.strip()}"]
    for record in meta.stdout.split("\x1e"):
        record = record.strip("\n")
        if not record:
            continue
        sha, author, committer, subject, body = record.split("\x1f")
        out += [f"{sha[:7]} {msg}" for msg in author_violations(author)]
        out += [f"{sha[:7]} 提交者{msg[2:]}" for msg in author_violations(committer)]
        out += [f"{sha[:7]} {msg}" for msg in message_violations(subject, body)]
    names = git("log", "--format=%x1e%H", "--name-only", "-z", rev, root=root)
    if names.returncode != 0:
        return out + [f"读不到路径历史 {rev}：{names.stderr.strip()}"]
    for block in names.stdout.split("\x1e"):
        paths = [p for p in block.replace("\0", "\n").split("\n") if p and not re.fullmatch(r"[0-9a-f]{40}", p)]
        sha = next((p for p in block.replace("\0", "\n").split("\n") if re.fullmatch(r"[0-9a-f]{40}", p)), "")
        for path in paths:
            out += [f"{sha[:7]} {msg}" for msg in path_violations(path)]
    return out


def check_index(root: Path | None = None) -> list[str]:
    proc = git("diff", "--cached", "--name-only", "-z", "--diff-filter=ACMRT", root=root)
    if proc.returncode != 0:
        return [f"读不到暂存区：{proc.stderr.strip()}"]
    return [v for path in proc.stdout.split("\0") if path for v in path_violations(path)]


def check_push_ready(remote_ref: str | None, root: Path | None = None) -> list[str]:
    out: list[str] = []
    if remote_ref:
        if not re.fullmatch(r"[0-9a-f]{40}", remote_ref):
            return [f"--remote-ref 需给完整 40 位 SHA：{remote_ref}"]
        if git("merge-base", "--is-ancestor", remote_ref, "HEAD", root=root).returncode != 0:
            out.append(f"HEAD 不是 {remote_ref[:12]} 的快进后继：强制改写必须走单独的批准流程")
    out += check_commits("HEAD", root=root)
    out += check_tree("HEAD", root=root)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="上传硬门：白名单、署名、交付物与提交说明")
    parser.add_argument("--tree", metavar="REV", help="只检查某个提交的树")
    parser.add_argument("--commits", metavar="REV", help="审计该提交及其全部祖先")
    parser.add_argument("--index", action="store_true", help="检查暂存区（提交前）")
    parser.add_argument("--message", metavar="MSG", help="检查即将使用的提交说明")
    parser.add_argument("--push-ready", action="store_true", help="推送前总检（历史＋树＋快进）")
    parser.add_argument("--remote-ref", metavar="SHA", help="远端 dev 当前 SHA，用于快进判定")
    args = parser.parse_args(argv)

    if not (args.tree or args.commits or args.index or args.push_ready or args.message):
        parser.print_usage(sys.stderr)
        return 3

    problems: list[str] = []
    if args.tree:
        problems += check_tree(args.tree)
    if args.commits:
        problems += check_commits(args.commits)
    if args.index:
        problems += check_index()
    if args.message:
        head, _, body = args.message.partition("\n\n")
        problems += message_violations(head.strip(), body)
    if args.push_ready:
        problems += check_push_ready(args.remote_ref)

    if problems:
        print("UPLOAD_GATE FAILED")
        for item in problems:
            print(f"  - {item}")
        return 2
    print("UPLOAD_GATE PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
