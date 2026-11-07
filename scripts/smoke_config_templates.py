"""注释 YAML 模板离线 smoke 测试。

跑法（离线，不 import nonebot，仅需 PyYAML）：
    cd <repo root> && python scripts/smoke_config_templates.py

覆盖：
  1. 82 旧键映射表完整且无重复，新 Config 仅 8 键；
  2. 54 目标键对账（8 env + 21 chat + 15 misc + 7 media + 3 user_lists，默认值一致）；
  3. 注释与空行（每个键上一行是注释，键间恰好至少一个空行）；
  4. media 保留既有嵌套结构（mode/inline/fetch/sticker/forward_image_cap/exemptions）；
  5. user_lists 保留 private_allow（注释示例，不合并进 allow_users）并新增三键；
  6. .env 模板 8 键带注释。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

import yaml  # noqa: E402

from core import config_templates as ct  # noqa: E402

FAILS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


# --------------------------------------------------------------------------- #
# 1. 82 旧键映射无遗漏
# --------------------------------------------------------------------------- #

config_src = (_REPO_ROOT / "config.py").read_text(encoding="utf-8")
config_fields = re.findall(r"^    (hermes_[a-zA-Z0-9_]+):", config_src, flags=re.M)
check("82 旧键映射完整且无重复", len(ct.OLD_KEYS) == 82 and len(set(ct.OLD_KEYS)) == 82)
check("新 Config 仅 8 个启动键", len(config_fields) == 8, f"got {len(config_fields)}")

# 处置类别计数（旧防抖三键全部删除，不再直接迁移或宣称固定群聊配置）
from collections import Counter  # noqa: E402

kinds = Counter(kind for kind, _t, _r in ct.OLD_KEYS.values())
check("删除 20 键(含旧防抖三键)", kinds.get("deleted") == 20, str(kinds))
check("固定行为 8 键", kinds.get("fixed") == 8, str(kinds))
check("合并 3+1 键", kinds.get("merge_history") == 3 and kinds.get("merge_text") == 1, str(kinds))
# 直接迁移键数：按旧键映射计数；新统一防抖键使用缺省 6.0，不继承旧分键值。
direct = kinds["env"] + kinds["chat"] + kinds["misc"] + kinds["media"] + kinds["user_lists"]
check("直接迁移 50 键", direct == 50, f"direct={direct}")

# --------------------------------------------------------------------------- #
# 2. 54 目标键对账
# --------------------------------------------------------------------------- #

check("目标键总数 54", len(ct.TARGET_KEYS) == 54, f"got {len(ct.TARGET_KEYS)}")
by_file = Counter(v["file"] for v in ct.TARGET_KEYS.values())
expect_files = {
    ".env": 8,
    "chat_config.yaml": 21,
    "misc_config.yaml": 15,
    "media_config.yaml": 7,
    "user_lists.yaml": 3,
}
check("54 键按文件分布", dict(by_file) == expect_files, str(by_file))
check(".env 规格 8 键", len(ct.ENV_SPEC) == 8, f"got {len(ct.ENV_SPEC)}")

# 每份 YAML 模板可解析，且目标键默认值一致
for fname in ("chat_config.yaml", "misc_config.yaml", "media_config.yaml", "user_lists.yaml"):
    doc = yaml.safe_load(ct.TEMPLATES[fname])
    check(f"{fname} 可解析为映射", isinstance(doc, dict))
    targets = ct.TARGET_BY_FILE[fname]
    missing = [k for k in targets if k not in doc]
    check(f"{fname} 目标键齐全", not missing, f"缺 {missing}")
    wrong = [
        k for k, spec in targets.items()
        if k in doc and doc[k] != spec["default"]
    ]
    check(f"{fname} 目标键默认值一致", not wrong, f"不符 {wrong}")

# --------------------------------------------------------------------------- #
# 3. 注释与空行
# --------------------------------------------------------------------------- #

_KEY_RE = re.compile(r"^[ \t]*[A-Za-z_][A-Za-z0-9_]*:")


def _comment_and_blank_ok(text: str) -> tuple[bool, str]:
    lines = text.splitlines()
    key_idxs = [
        i for i, l in enumerate(lines)
        if l.strip() and not l.lstrip().startswith("#") and _KEY_RE.match(l)
    ]
    for i in key_idxs:
        if i == 0 or not lines[i - 1].lstrip().startswith("#"):
            return False, f"第 {i + 1} 行键上一行不是注释: {lines[i]!r}"
    top_idxs = [i for i in key_idxs if lines[i] and not lines[i][0].isspace()]
    for a, b in zip(top_idxs, top_idxs[1:]):
        if not any(lines[j] == "" for j in range(a + 1, b)):
            return False, f"第 {a + 1} 与 {b + 1} 行键间无空行"
    return True, ""


for fname in ("chat_config.yaml", "misc_config.yaml", "media_config.yaml", "user_lists.yaml"):
    ok, detail = _comment_and_blank_ok(ct.TEMPLATES[fname])
    check(f"{fname} 逐键注释+键间空行", ok, detail)

# --------------------------------------------------------------------------- #
# 4. media 保留既有嵌套结构
# --------------------------------------------------------------------------- #

media = yaml.safe_load(ct.TEMPLATES["media_config.yaml"])
check("media mode 默认 semi", media.get("mode") == "semi")
check("media semi_window 默认 3", media.get("semi_window") == 3)
check(
    "media inline 嵌套结构保留",
    media.get("inline") == {"ladder": [[2048, 80], [1024, 70]], "max_bytes": 500_000, "gif_max_frames": 5},
    str(media.get("inline")),
)
check(
    "media fetch 嵌套结构保留",
    media.get("fetch") == {"gate_bytes": 2_000_000, "ladder": [[4096, 95], [4096, 70]], "gif_max_frames": 10},
    str(media.get("fetch")),
)
check("media sticker 嵌套结构保留", media.get("sticker") == {"gate_bytes": 200_000})
check("media forward_image_cap 保留", media.get("forward_image_cap") == 30)
check("media exemptions 保留(空)", media.get("exemptions") == {})

# --------------------------------------------------------------------------- #
# 5. user_lists 保留 private_allow 并新增三键
# --------------------------------------------------------------------------- #

ul_text = ct.TEMPLATES["user_lists.yaml"]
ul = yaml.safe_load(ul_text)
check("user_lists blacklist 保留", ul.get("blacklist") == {})
check("user_lists known_bots 保留", ul.get("known_bots") == [])
check("user_lists 新增 allow_users", "allow_users" in ul and ul["allow_users"] == [])
check("user_lists 新增 allow_groups", "allow_groups" in ul and ul["allow_groups"] == [])
check("user_lists 新增 admin_users", "admin_users" in ul and ul["admin_users"] == [])
check("private_allow 在模板中保留(注释示例,不写死空表)", "private_allow" in ul_text and "# private_allow:" in ul_text)
check("private_allow 不与 allow_users 合并(模板里两键分离)", "allow_users:" in ul_text and "# private_allow:" in ul_text)

# --------------------------------------------------------------------------- #
# 6. .env 模板 8 键
# --------------------------------------------------------------------------- #

env_keys = re.findall(r"^(AGENT_[A-Z_]+)=", ct.ENV_TEMPLATE, flags=re.M)
check(".env 模板 8 个 AGENT_* 键", len(env_keys) == 8, str(env_keys))
env_lines = ct.ENV_TEMPLATE.splitlines()
env_key_idxs = [i for i, l in enumerate(env_lines) if re.match(r"^AGENT_[A-Z_]+=", l)]
env_ok = all(i > 0 and env_lines[i - 1].lstrip().startswith("#") for i in env_key_idxs)
check(".env 逐键注释", env_ok)

print()
if FAILS:
    print(f"FAILED {len(FAILS)}: {FAILS}")
    sys.exit(1)
print("ALL PASS")
