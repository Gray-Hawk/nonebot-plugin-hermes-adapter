"""S2 v6 配置迁移工具离线 smoke 测试。

跑法（离线，不 import nonebot，仅需 PyYAML + python-dotenv）：
    cd <repo root> && ~/NeonBot/.venv/bin/python scripts/smoke_migrate_v6.py

覆盖：
  1. 82 旧键映射表完整且无重复，新 Config 仅 8 键；
  2. 原值保留（自定义 env 值迁到 AGENT_* env 与 chat/misc YAML）；
  3. secret 不回显（API key 写进文件、但绝不打印到 stdout）；
  4. 拒绝覆盖（目标已存在则跳过，不改内容）；
  5. 冲突诊断（三条历史条数上限冲突报错；perception_text_length 非默认显式 WARN + 取 max）；
  6. 媒体/名单已有自定义值不丢（mode: off、exemptions、blacklist、private_allow 等）。
"""

from __future__ import annotations

import io
import json
import re
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

import yaml  # noqa: E402

from core import config_templates as ct  # noqa: E402
from scripts import migrate_config_v6 as mig  # noqa: E402

FAILS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def write_env(path: Path, d: dict) -> None:
    path.write_text("".join(f"{k}={v}\n" for k, v in d.items()), encoding="utf-8")


def run_capture(args: list[str]) -> tuple[int, str]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = mig.run(args)
    return code, buf.getvalue()


# --------------------------------------------------------------------------- #
# 1. 82 旧键映射无遗漏
# --------------------------------------------------------------------------- #

config_src = (_REPO_ROOT / "config.py").read_text(encoding="utf-8")
config_fields = re.findall(r"^    (hermes_[a-zA-Z0-9_]+):", config_src, flags=re.M)
bare_fields = [f[len("hermes_"):] for f in config_fields]
check("82 旧键覆盖", len(ct.OLD_KEYS) == 82 and len(set(ct.OLD_KEYS)) == 82)
check("新 Config 仅保留 8 个启动键", len(bare_fields) == 8)

# --------------------------------------------------------------------------- #
# 2. 原值保留 + 3. secret 不回显 + 6. 媒体/名单自定义值不丢（端到端一次跑）
# --------------------------------------------------------------------------- #

SECRET = "sk-SECRET-x7k9p2m4"

OLD_ENV = {
    "HERMES_API_URL": "http://10.0.0.5:8642",
    "HERMES_API_KEY": SECRET,
    "HERMES_API_TIMEOUT": "120",
    "HERMES_RUNS_CHANNEL_ENABLED": "true",
    "HERMES_MCP_ENABLED": "true",
    "HERMES_MCP_PORT": "9000",
    "HERMES_CUSTOM_DIR": "/opt/hermes/cfg",
    "HERMES_MAX_LENGTH": "2500",
    "HERMES_KEYWORDS": '["/ai","/hi"]',
    "HERMES_IGNORE_PREFIX": '[".","!"]',
    "HERMES_REPLY_DELAY_SEC": "1.5",
    "HERMES_ACTIVE_SESSION_T5_SEC": "600",
    "HERMES_ADMIN_USERS": '["onebotv11:10001"]',
    "HERMES_ALLOW_USERS": '["onebotv11:20001"]',
    "HERMES_ALLOW_GROUPS": '["onebotv11:30001"]',
    "HERMES_PROACTIVE_GROUPS": '["onebotv11:40001"]',
    "HERMES_LOCAL_FILE_MAX_MB": "80",
    "HERMES_IMAGE_CACHE_QUOTA_MB": "500",
    "HERMES_STICKER_VISION_ENABLED": "true",
    "HERMES_TRANSPORT_ERROR_FALLBACK_TEXT": "出错了",
    "HERMES_REPLY_DEBOUNCE_GROUP_SEC": "2.0",
}

MEDIA_IN = (
    "mode: off\n"
    "semi_window: 5\n"
    "inline:\n"
    "  ladder: [[1024, 60]]\n"
    "  max_bytes: 123456\n"
    "fetch:\n"
    "  gate_bytes: 999\n"
    "sticker:\n"
    "  gate_bytes: 555\n"
    "forward_image_cap: 40\n"
    "exemptions:\n"
    '  "10001": off\n'
    "custom_extra: keep-me\n"
)

USER_IN = (
    "blacklist:\n"
    '  "20001": ["30001"]\n'
    'known_bots: ["30002"]\n'
    'private_allow: ["10001"]\n'
)

d = Path(tempfile.mkdtemp(prefix="mig_smoke_"))
env_file = d / "old.env"
media_in = d / "media_config.yaml"
user_in = d / "user_lists.yaml"
out_dir = d / "out"
env_out = out_dir / ".env"
write_env(env_file, OLD_ENV)
media_in.write_text(MEDIA_IN, encoding="utf-8")
user_in.write_text(USER_IN, encoding="utf-8")

code, report = run_capture([
    "--env-file", str(env_file),
    "--output-dir", str(out_dir),
    "--env-output", str(env_out),
    "--media-yaml", str(media_in),
    "--user-yaml", str(user_in),
])
check("端到端迁移退出码 0", code == 0, report[-400:] if code else "")

# 3. secret 不回显
check("secret 不回显到 stdout", SECRET not in report)
check("secret 写入 .env 文件", SECRET in env_out.read_text(encoding="utf-8"))
check("含凭据 ENV 权限为 0600", (env_out.stat().st_mode & 0o777) == 0o600,
      oct(env_out.stat().st_mode & 0o777))
check("旧 secret 键名不再出现在报告", "HERMES_API_KEY" not in report)

# 2. 原值保留：env
env_lines = env_out.read_text(encoding="utf-8")
def env_val(name: str) -> str | None:
    m = re.search(rf"^{name}=(.*)$", env_lines, flags=re.M)
    return m.group(1) if m else None
check("AGENT_API_URL 保留", env_val("AGENT_API_URL") == "http://10.0.0.5:8642")
check("AGENT_API_TIMEOUT 保留", env_val("AGENT_API_TIMEOUT") == "120")
check("AGENT_API_INTERFACE runs 枚举(旧 true)", env_val("AGENT_API_INTERFACE") == "runs")
check("AGENT_MCP_ENABLED 保留", env_val("AGENT_MCP_ENABLED") == "true")
check("AGENT_MCP_PORT 保留", env_val("AGENT_MCP_PORT") == "9000")
check("AGENT_PLUGIN_CUSTOM_DIR 改名迁移", env_val("AGENT_PLUGIN_CUSTOM_DIR") == "/opt/hermes/cfg")
check("AGENT_API_KEY 保留原值", env_val("AGENT_API_KEY") == SECRET)

# 2. 原值保留：chat / misc YAML
chat = yaml.safe_load((out_dir / "chat_config.yaml").read_text(encoding="utf-8"))
misc = yaml.safe_load((out_dir / "misc_config.yaml").read_text(encoding="utf-8"))
check("keywords 保留", chat["keywords"] == ["/ai", "/hi"])
check("ignore_prefix 保留", chat["ignore_prefix"] == [".", "!"])
check("reply_delay_for_bot_sec 改名迁移(1.5)", chat["reply_delay_for_bot_sec"] == 1.5)
check("active_session_timeout_sec 改名迁移(600)", chat["active_session_timeout_sec"] == 600)
check("proactive_groups 保留", chat["proactive_groups"] == ["onebotv11:40001"])
check("reply_debounce_group_sec 保留(2.0)", chat["reply_debounce_group_sec"] == 2.0)
check("max_length 迁到 misc(2500)", misc["max_length"] == 2500)
check("transport_error_fallback_text 保留", misc["transport_error_fallback_text"] == "出错了")
check("合并键 history_message_limit 默认 10", chat["history_message_limit"] == 10)
check("合并键 history_text_max_chars 默认 800", misc["history_text_max_chars"] == 800)

# 6. 媒体已有自定义值不丢（含 YAML1.1 陷阱 mode: off）
media = yaml.safe_load((out_dir / "media_config.yaml").read_text(encoding="utf-8"))
check("media mode off 保留为字符串", media["mode"] == "off", repr(media["mode"]))
check("media semi_window 保留", media["semi_window"] == 5)
check("media inline.max_bytes 保留", media["inline"]["max_bytes"] == 123456)
check("media fetch.gate_bytes 保留", media["fetch"]["gate_bytes"] == 999)
check("media sticker.gate_bytes 保留", media["sticker"]["gate_bytes"] == 555)
check("media forward_image_cap 保留", media["forward_image_cap"] == 40)
check("media exemptions off 保留", media["exemptions"] == {"10001": "off"}, repr(media["exemptions"]))
check("media 迁入 local_file_max_mb=80", media["local_file_max_mb"] == 80)
check("media 迁入 image_cache_quota_mb=500", media["image_cache_quota_mb"] == 500)
check("media 迁入 sticker_vision_enabled=true", media["sticker_vision_enabled"] is True)
check("media 未建模自定义键 custom_extra 保留", media.get("custom_extra") == "keep-me", repr(media.get("custom_extra")))

# 6. 名单已有自定义值不丢 + 新键迁入
ul = yaml.safe_load((out_dir / "user_lists.yaml").read_text(encoding="utf-8"))
check("user_lists blacklist 保留", ul["blacklist"] == {"20001": ["30001"]})
check("user_lists known_bots 保留", ul["known_bots"] == ["30002"])
check("user_lists private_allow 保留", ul["private_allow"] == ["10001"])
check("user_lists admin_users 迁入", ul["admin_users"] == ["onebotv11:10001"])
check("user_lists allow_users 迁入", ul["allow_users"] == ["onebotv11:20001"])
check("user_lists allow_groups 迁入", ul["allow_groups"] == ["onebotv11:30001"])

# --------------------------------------------------------------------------- #
# 4. 拒绝覆盖
# --------------------------------------------------------------------------- #

sentinel = "# SENTINEL: 用户已有文件，不得覆盖\nkeep: me\n"
preexisting = d / "preexist"
preexisting.mkdir()
(preexisting / "chat_config.yaml").write_text(sentinel, encoding="utf-8")
code2, report2 = run_capture([
    "--env-file", str(env_file),
    "--output-dir", str(preexisting),
    "--env-output", str(preexisting / ".env"),
])
check("拒绝覆盖: 退出码仍 0", code2 == 0)
check("拒绝覆盖: 已存在文件内容不变", (preexisting / "chat_config.yaml").read_text(encoding="utf-8") == sentinel)
check("拒绝覆盖: 报告含跳过提示", "跳过" in report2)
check("拒绝覆盖: 其他文件仍创建", (preexisting / "misc_config.yaml").is_file())
check("漏传既有媒体路径有 WARN", "--media-yaml" in report2)
check("漏传既有名单路径有 WARN", "--user-yaml" in report2)

# --------------------------------------------------------------------------- #
# 5. 冲突诊断
# --------------------------------------------------------------------------- #

# 三条历史条数上限冲突
conflict_env = d / "conflict.env"
write_env(conflict_env, {
    "HERMES_PERCEPTION_BUFFER": "20",
    "HERMES_KEYWORD_SOFT_CONTEXT": "30",
    "HERMES_PROACTIVE_MAX_MESSAGES": "10",
})
code3, report3 = run_capture([
    "--env-file", str(conflict_env),
    "--output-dir", str(d / "conflict_out"),
    "--env-output", str(d / "conflict_out" / ".env"),
])
check("三条历史上限冲突报错(非0)", code3 != 0)
check("冲突诊断点名两个非默认键", "perception_buffer" in report3 and "keyword_soft_context" in report3, report3[-300:])
check("冲突诊断给出目标键名", "history_message_limit" in report3)
check("冲突时不写任何文件", not (d / "conflict_out").exists())

# 三条历史上限一致的非默认值 → 取该值
agree_env = d / "agree.env"
write_env(agree_env, {
    "HERMES_PERCEPTION_BUFFER": "25",
    "HERMES_KEYWORD_SOFT_CONTEXT": "25",
})
m = mig.build_migration(mig.dotenv_values(str(agree_env), interpolate=False))
check("历史上限一致非默认取 25", m.merged.get("history_message_limit") == 25)

# perception_text_length 非默认 → 显式 WARN + 取 max
txt_env = d / "txt.env"
write_env(txt_env, {"HERMES_PERCEPTION_TEXT_LENGTH": "1200"})
m2 = mig.build_migration(mig.dotenv_values(str(txt_env), interpolate=False))
check("文本上限非默认 1200 与 800 取 max=1200", m2.merged.get("history_text_max_chars") == 1200)
check("文本上限合并有 WARN", any("perception_text_length" in w for w in m2.warnings))
# 非默认 < 800 时取 800(不收紧渲染上限)
write_env(txt_env, {"HERMES_PERCEPTION_TEXT_LENGTH": "300"})
m3 = mig.build_migration(mig.dotenv_values(str(txt_env), interpolate=False))
check("文本上限非默认 300 与 800 取 max=800", m3.merged.get("history_text_max_chars") == 800)

# v6 明确把故障兜底默认改为空串；旧默认非空时必须在迁移报告中显式提醒。
m4 = mig.build_migration({})
check("故障兜底旧默认变化有 WARN",
      any("transport_error_fallback_text" in w and "空串" in w for w in m4.warnings))

# 历史条数运行时要求 >=1，迁移不得产出会被静默改回 10 的 0/负值。
for bad_limit in ("0", "-2"):
    bad_hist = mig.build_migration({"HERMES_PERCEPTION_BUFFER": bad_limit})
    check(f"历史条数 {bad_limit} 迁移报错", any("history_message_limit" in e for e in bad_hist.errors))
    check(f"历史条数 {bad_limit} 不产出合并值", "history_message_limit" not in bad_hist.merged)

# --------------------------------------------------------------------------- #
# 7. dry-run 不写盘
# --------------------------------------------------------------------------- #

dry_dir = d / "dry"
code4, report4 = run_capture([
    "--env-file", str(env_file),
    "--output-dir", str(dry_dir),
    "--env-output", str(dry_dir / ".env"),
    "--dry-run",
])
check("dry-run 退出码 0", code4 == 0)
check("dry-run 不写盘", not dry_dir.exists())

print()
if FAILS:
    print(f"FAILED {len(FAILS)}: {FAILS}")
    sys.exit(1)
print("ALL PASS")
