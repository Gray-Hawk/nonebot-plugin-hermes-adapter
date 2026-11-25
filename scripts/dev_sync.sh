#!/bin/bash
# dev_sync.sh — 在 dev 工作树内提交当前改动并推送到 dev。
#
# 与 verify.py 共用 scripts/check_upload_gate.py 这一道门：
#   * 提交前：暂存区路径必须落在产品白名单里（交付说明／证据目录／缓存一律拦下）；
#   * 推送前：整段历史的白名单、署名与提交说明总检；
#   * 给定远端 SHA 时还要求 HEAD 是它的快进后继——候选 fast-forward 也走同一道门。
set -euo pipefail

SRC="${NPH_WORK:-$HOME/nph-20-work/devclone}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MESSAGE="${1:-}"

if [ "$#" -ne 1 ] || [[ "$MESSAGE" =~ ^[[:space:]]*$ ]]; then
    echo "用法：SECRET=<token> $0 <提交信息>" >&2
    exit 2
fi
: "${SECRET:?请先通过 SECRET 环境变量提供 GitHub token}"
SECRET_VALUE=$SECRET
unset SECRET

cd "$SRC"
SRC="$(pwd -P)"
if [ "$(git rev-parse --is-inside-work-tree 2>/dev/null)" != "true" ]; then
    echo "目标不是 git 工作树：$SRC" >&2
    exit 2
fi
if [ "$(git branch --show-current)" != "dev" ]; then
    echo "只在 dev 分支运行，当前分支：$(git branch --show-current)" >&2
    exit 2
fi
# TODO(D3): 远端仓库改名未批准，origin 仍指旧仓库路径；改名后连同此行一起切。
EXPECTED_ORIGIN="${NPH_DEV_SYNC_EXPECT_ORIGIN:-https://github.com/Gray-Hawk/nonebot-plugin-hermes-adapter.git}"
ORIGIN="$(git remote get-url origin)"
if [ "$ORIGIN" != "$EXPECTED_ORIGIN" ]; then
    echo "origin 与预期仓库不符：$ORIGIN" >&2
    exit 2
fi

GATE=(python3 -B "$SRC/scripts/check_upload_gate.py")

mapfile -t TRACKED_FILES < <(git diff --name-only --diff-filter=ACMRT HEAD -- . ':(exclude)README.md')
mapfile -t UNTRACKED_FILES < <(git ls-files --others --exclude-standard -- . ':(exclude)README.md' ':(exclude,glob)**/__pycache__/**' ':(exclude,glob)**/*.pyc')
SCAN_FILES=("${TRACKED_FILES[@]}" "${UNTRACKED_FILES[@]}")
if ((${#SCAN_FILES[@]})); then
    NPH_WORK="$SRC" python3 "$SCRIPT_DIR/scan_changed_residual.py" "${SCAN_FILES[@]}"
fi

python3 - "$SRC" <<'PY'
import os
from pathlib import Path
import shutil
import sys

root = Path(sys.argv[1])
caches = []
for current, dirs, files in os.walk(root):
    dirs[:] = [name for name in dirs if name != ".git"]
    current_path = Path(current)
    for name in files:
        if name.endswith(".pyc"):
            (current_path / name).unlink()
    caches.extend(current_path / name for name in dirs if name == "__pycache__")
for path in sorted(caches, key=lambda item: len(item.parts), reverse=True):
    if path.exists():
        shutil.rmtree(path)
PY

git reset -q -- README.md
git add -A -- . ':(exclude)README.md'
if ! git diff --cached --quiet -- README.md; then
    echo "README.md 仍在暂存区，已中止" >&2
    exit 1
fi
if git diff --cached --quiet; then
    echo "NO_CHANGE"
    exit 0
fi

# 门 1：暂存区路径白名单（交付说明／证据目录／缓存不进提交）
"${GATE[@]}" --index || { echo "上传硬门拦下暂存区内容，未提交" >&2; exit 2; }

# 未来提交说明也是门的一部分：先离线校验，别等推上去才发现。
"${GATE[@]}" --message "$MESSAGE" || { echo "上传硬门拦下提交说明，未提交" >&2; exit 2; }

git config user.name NormiAgent
git config user.email 344182818+NormiAgent@users.noreply.github.com
git commit -q -m "$MESSAGE"
SECRET=$SECRET_VALUE
export SECRET
unset SECRET_VALUE
HELPER='!f() { echo username=x-access-token; echo password="***"; }; f'
GIT_AUTH=(-c credential.helper= -c "credential.https://github.com.helper=$HELPER")

# 门 2：推送前历史总检（白名单／署名／提交说明）＋快进判定
set +e
REMOTE_LINE="$("${GIT_AUTH[@]}" git ls-remote origin refs/heads/dev)"
STATUS=$?
set -e
if [ $STATUS -ne 0 ]; then
    echo "远端读取失败，无法核对 dev；本地 HEAD=$(git rev-parse HEAD)" >&2
    exit 1
fi
REMOTE="${REMOTE_LINE%%$'\t'*}"
if [ -n "$REMOTE" ]; then
    "${GATE[@]}" --push-ready --remote-ref "$REMOTE" || {
        echo "上传硬门未通过，未推送；本地 HEAD=$(git rev-parse HEAD) 仍留在本机" >&2
        exit 2
    }
fi

git "${GIT_AUTH[@]}" push origin HEAD:dev

LOCAL="$(git rev-parse HEAD)"
if ! REMOTE_LINE="$("${GIT_AUTH[@]}" git ls-remote origin refs/heads/dev)"; then
    echo "远端读取失败，无法核对 dev；本地 HEAD=$LOCAL" >&2
    exit 1
fi
REMOTE="${REMOTE_LINE%%$'\t'*}"
unset SECRET
if [ "$REMOTE" != "$LOCAL" ]; then
    echo "远端核对失败：HEAD=$LOCAL，远端 dev=${REMOTE:-缺失}" >&2
    exit 1
fi
printf 'SYNCED %s\n' "$LOCAL"
