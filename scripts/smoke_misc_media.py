"""misc 15 键 + media 7 键消费端迁移 smoke 测试。

跑法（shim 挂 work 副本，中立目录执行，显式临时 AGENT_PLUGIN_CUSTOM_DIR）：
    ln -s <work 副本路径> <shim>/nonebot_plugin_hermes_adapter
    cd <中立目录> && PYTHONPATH=<shim> python <work>/scripts/smoke_misc_media.py

覆盖（misc/media 消费端的离线验收）：
  1. MiscConfig 15 键 / MediaConfig 7 键形态与默认值；
  2. 历史双截断合并：入库(message_perception)与渲染(prompt_builder)共用
     history_text_max_chars，硬编码 800 与旧 hermes_perception_text_length 零读点；
  3. 显式空 fallback：transport_error_fallback_text 默认空串、显式空串仍静默、
     旧“嗯…我这边遇到点状况”默认文案不再回退；
  4. 媒体七键实际消费（非只 schema）：allowed_roots 吃 media_allow_dirs、
     _fetch_all 吃 image_fetch_timeout_s，其余键静态断言读 load_media_config()；
  5. 手动重载后 load_misc_config()/load_media_config() 立即反映新快照；
  6. 逐点静态断言：旧键零读点、新键读点到位。
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from dataclasses import fields as _fields
from pathlib import Path

import nonebot

# 沙箱化 localstore 数据目录 + 显式独立临时 AGENT_PLUGIN_CUSTOM_DIR，
# 避免冒烟运行往生产目录写模板/读生产 YAML。
_SCRATCH = Path(tempfile.mkdtemp(prefix="nph_misc_media_"))
os.environ["LOCALSTORE_DATA_DIR"] = str(_SCRATCH / "data")
os.environ["AGENT_PLUGIN_CUSTOM_DIR"] = str(_SCRATCH / "cfg")
(_SCRATCH / "cfg").mkdir(parents=True, exist_ok=True)

nonebot.init()

import nonebot_plugin_hermes_adapter  # noqa: E402  (shim 提供的包名)
from nonebot_plugin_hermes_adapter.core import media_whitelist as mw  # noqa: E402
from nonebot_plugin_hermes_adapter.core import image_inline as ii  # noqa: E402
from nonebot_plugin_hermes_adapter.core import misc_config as misc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import media_config as media  # noqa: E402
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core.media_config import (  # noqa: E402
    MediaConfig,
    load_media_config,
    parse_media_config,
)
from nonebot_plugin_hermes_adapter.core.misc_config import (  # noqa: E402
    MiscConfig,
    load_misc_config,
    parse_misc_config,
)

FAILS: list[str] = []
SKIPS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def skip(name: str, detail: str = "") -> None:
    print("SKIP " + name + (f" | {detail}" if detail else ""))
    SKIPS.append(name)


SRC = Path(nonebot_plugin_hermes_adapter.__file__).parent


def src_has_no(relpath: str, token: str) -> bool:
    return token not in (SRC / relpath).read_text(encoding="utf-8")


def src_has(relpath: str, token: str) -> bool:
    return token in (SRC / relpath).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# 1. 键形态与默认值
# --------------------------------------------------------------------------- #

misc_names = {f.name for f in _fields(MiscConfig)}
check("MiscConfig 共 15 键", len(misc_names) == 15, f"实际 {len(misc_names)} 键")
for k in (
    "history_text_max_chars", "group_profile_enabled", "max_length",
    "transport_error_fallback_text", "interim_echo_enabled", "help_menu_file",
    "prompt_dump_enabled", "approval_expiry_sec", "prompt_timezone",
    "delivery_poll_interval_sec", "bot_name", "storage_db_path",
    "storage_message_retention_days", "storage_message_max_rows", "sticker_enabled",
):
    check(f"misc 键 {k} 存在", k in misc_names)

mc = MiscConfig()
check("默认 history_text_max_chars=800", mc.history_text_max_chars == 800)
check("默认 group_profile_enabled=True", mc.group_profile_enabled is True)
check("默认 max_length=4000", mc.max_length == 4000)
check("默认 transport_error_fallback_text 空串", mc.transport_error_fallback_text == "")
check("默认 interim_echo_enabled=False", mc.interim_echo_enabled is False)
check("默认 prompt_dump_enabled=False", mc.prompt_dump_enabled is False)
check("默认 approval_expiry_sec=300", mc.approval_expiry_sec == 300)
check("默认 prompt_timezone 空串", mc.prompt_timezone == "")
check("默认 delivery_poll_interval_sec=30", mc.delivery_poll_interval_sec == 30)
check("默认 storage_db_path 空串", mc.storage_db_path == "")
check("默认 storage_message_retention_days=30", mc.storage_message_retention_days == 30)
check("默认 storage_message_max_rows=100000", mc.storage_message_max_rows == 100_000)
check("默认 sticker_enabled=False", mc.sticker_enabled is False)

media_names = {f.name for f in _fields(MediaConfig)}
v6_media = {
    "media_allow_dirs", "local_file_max_mb", "sticker_vision_enabled",
    "image_cache_dir", "image_cache_quota_mb", "image_fetch_timeout_s",
    "image_fetch_max_attempts",
}
check("MediaConfig 含 v6 七键", v6_media <= media_names, f"缺 {v6_media - media_names}")
pm = parse_media_config({})
check("默认 media_allow_dirs 空串", pm.media_allow_dirs == "")
check("默认 local_file_max_mb=50", pm.local_file_max_mb == 50)
check("默认 sticker_vision_enabled=False", pm.sticker_vision_enabled is False)
check("默认 image_cache_dir 空串", pm.image_cache_dir == "")
check("默认 image_cache_quota_mb=200", pm.image_cache_quota_mb == 200)
check("默认 image_fetch_timeout_s=10", pm.image_fetch_timeout_s == 10)
check("默认 image_fetch_max_attempts=2", pm.image_fetch_max_attempts == 2)

# history_text_max_chars 不提供 0=无限制：0 回退默认 800
p0 = parse_misc_config({"history_text_max_chars": 0})
check("history_text_max_chars=0 回退默认 800", p0.history_text_max_chars == 800)

# --------------------------------------------------------------------------- #
# 2. 历史双截断合并：入库与渲染共用 history_text_max_chars
# --------------------------------------------------------------------------- #

check("入库: message_perception 无 plugin_config.hermes_perception_text_length",
      src_has_no("handlers/message_perception.py", "plugin_config.hermes_perception_text_length"))
check("入库: message_perception 读 load_misc_config().history_text_max_chars",
      src_has("handlers/message_perception.py", "load_misc_config().history_text_max_chars"))
check("渲染: prompt_builder 无硬编码 _MAX_HISTORY_LINE_CHARS",
      src_has_no("core/prompt_builder.py", "_MAX_HISTORY_LINE_CHARS"))
check("渲染: prompt_builder 读 load_misc_config().history_text_max_chars",
      src_has("core/prompt_builder.py", "load_misc_config().history_text_max_chars"))

# 行为：渲染端 _sanitize_history_content 缺省 max_chars 现读同一键，且随重载变化。
orig_misc_snap = load_misc_config()
misc._store.commit(parse_misc_config({"history_text_max_chars": 60}))
try:
    check("重载: history_text_max_chars=60 生效", load_misc_config().history_text_max_chars == 60)
    out = pb._sanitize_history_content("x" * 200)
    mark = pb._HISTORY_TRUNCATION_MARK
    check("渲染: 缺省 max_chars 现读 60 截断", len(out) == 60 + len(mark), f"len={len(out)}")
    check("渲染: 截断内容前缀正确", out == "x" * 60 + mark)
    out_explicit = pb._sanitize_history_content("x" * 200, max_chars=30)
    check("渲染: 显式 max_chars 仍可覆盖", len(out_explicit) == 30 + len(mark))
finally:
    misc._store.commit(orig_misc_snap)
check("重载: 恢复默认回到 800", load_misc_config().history_text_max_chars == 800)

# --------------------------------------------------------------------------- #
# 3. 显式空 fallback
# --------------------------------------------------------------------------- #

check("默认 transport_error_fallback_text 空串", load_misc_config().transport_error_fallback_text == "")
check("显式空 transport_error_fallback_text 仍空串",
      parse_misc_config({"transport_error_fallback_text": ""}).transport_error_fallback_text == "")
check("旧默认文案不再回退(空串即静默)",
      load_misc_config().transport_error_fallback_text != "嗯…我这边遇到点状况,稍后再问一次")
check("覆盖 transport_error_fallback_text 生效",
      parse_misc_config({"transport_error_fallback_text": "出错了"}).transport_error_fallback_text == "出错了")
check("消费: message_turns 无 plugin_config.hermes_transport_error_fallback_text",
      src_has_no("handlers/message_turns.py", "plugin_config.hermes_transport_error_fallback_text"))
check("消费: message_turns 读 misc_cfg.transport_error_fallback_text",
      src_has("handlers/message_turns.py", "misc_cfg.transport_error_fallback_text"))

# --------------------------------------------------------------------------- #
# 4. 媒体七键实际消费（非只 schema）
# --------------------------------------------------------------------------- #

# 4a. media_allow_dirs 被 allowed_roots 实际消费（行为）。
extra_dir = Path(tempfile.mkdtemp(prefix="nph_allow_"))
orig_media_snap = load_media_config()
try:
    media._store.commit(parse_media_config({"media_allow_dirs": str(extra_dir)}))
    roots = mw.allowed_roots()
    check("media_allow_dirs 被 allowed_roots 实际消费", extra_dir.resolve() in roots)
finally:
    media._store.commit(orig_media_snap)

# 4b. image_fetch_timeout_s 被 _fetch_all 实际消费（行为：monkeypatch httpx.AsyncClient）。
captured: dict = {}


class _FakeResp:
    content = b""

    def raise_for_status(self):
        return None


class _FakeClient:
    def __init__(self, timeout=None, follow_redirects=True):
        captured["timeout"] = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url):
        return _FakeResp()


_orig_async_client = ii.httpx.AsyncClient
_orig_fetch_timeout = ii._FETCH_TIMEOUT_S
ii.httpx.AsyncClient = _FakeClient
ii._FETCH_TIMEOUT_S = 42
try:
    asyncio.run(ii._fetch_all(["http://example.com/x.png"]))
    check("image_fetch_timeout_s 被 _fetch_all 实际消费(httpx timeout=42)", captured.get("timeout") == 42)
    media._store.commit(parse_media_config({"image_fetch_timeout_s": 77}))
    check("image_fetch_timeout_s 保持重启生效", ii._FETCH_TIMEOUT_S == 42)
finally:
    ii.httpx.AsyncClient = _orig_async_client
    ii._FETCH_TIMEOUT_S = _orig_fetch_timeout
    media._store.commit(orig_media_snap)

# 4c. 其余五键的消费端静态断言。
check("local_file_max_mb 被 outbound 消费", src_has("core/outbound.py", "load_media_config().local_file_max_mb"))
check("sticker_vision_enabled 被 message_turns 消费", src_has("handlers/message_turns.py", "media_cfg.sticker_vision_enabled"))
check("image_cache_dir 被 mcp 装配消费", src_has("mcp/__init__.py", "media_cfg.image_cache_dir"))
check("image_cache_quota_mb 被 mcp 装配消费", src_has("mcp/__init__.py", "media_cfg.image_cache_quota_mb"))
check("image_fetch_timeout_s 被 mcp 装配消费", src_has("mcp/__init__.py", "media_cfg.image_fetch_timeout_s"))
check("image_fetch_max_attempts 被 mcp 装配消费", src_has("mcp/__init__.py", "media_cfg.image_fetch_max_attempts"))
check("旧键零读点: media_whitelist 无 hermes_media_allow_dirs",
      src_has_no("core/media_whitelist.py", "plugin_config.hermes_media_allow_dirs"))
check("旧键零读点: image_inline 无 hermes_image_fetch_timeout_s",
      src_has_no("core/image_inline.py", "plugin_config.hermes_image_fetch_timeout_s"))
check("旧键零读点: message_turns 无 hermes_sticker_vision_enabled",
      src_has_no("handlers/message_turns.py", "plugin_config.hermes_sticker_vision_enabled"))
check("旧键零读点: outbound 无 hermes_local_file_max_mb",
      src_has_no("core/outbound.py", "plugin_config.hermes_local_file_max_mb"))

# --------------------------------------------------------------------------- #
# 5. misc 其余消费端逐点静态断言
# --------------------------------------------------------------------------- #

check("max_length 被 outbound 消费", src_has("core/outbound.py", "load_misc_config().max_length"))
check("旧键零读点: outbound 无 hermes_max_length", src_has_no("core/outbound.py", "plugin_config.hermes_max_length"))
check("approval_expiry_sec 被 approvals 消费", src_has("core/approvals.py", "load_misc_config().approval_expiry_sec"))
check("旧键零读点: approvals 无 hermes_approval_expiry_sec", src_has_no("core/approvals.py", "plugin_config.hermes_approval_expiry_sec"))
check("prompt_timezone 被 prompt_builder 消费", src_has("core/prompt_builder.py", "load_misc_config().prompt_timezone"))
check("旧键零读点: prompt_builder 无 hermes_prompt_timezone", src_has_no("core/prompt_builder.py", "plugin_config.hermes_prompt_timezone"))
check("interim_echo_enabled 被 message_turns 消费", src_has("handlers/message_turns.py", "misc_cfg.interim_echo_enabled"))
check("interim_echo_enabled 被 run_resume 消费", src_has("tasks/run_resume.py", "load_misc_config().interim_echo_enabled"))
check("旧键零读点: run_resume 无 hermes_interim_echo_enabled", src_has_no("tasks/run_resume.py", "plugin_config.hermes_interim_echo_enabled"))
check("prompt_dump_enabled 被 prompt_dump 消费", src_has("core/prompt_dump.py", "load_misc_config().prompt_dump_enabled"))
check("旧键零读点: prompt_dump 无 hermes_prompt_dump_enabled", src_has_no("core/prompt_dump.py", "hermes_prompt_dump_enabled"))
check("delivery_poll_interval_sec 被 delivery_poll 消费", src_has("tasks/delivery_poll.py", "load_misc_config().delivery_poll_interval_sec"))
check("旧键零读点: delivery_poll 无 hermes_delivery_poll_interval_sec", src_has_no("tasks/delivery_poll.py", "plugin_config.hermes_delivery_poll_interval_sec"))
check("storage_message_retention_days 被 storage_vacuum 消费", src_has("tasks/storage_vacuum.py", "misc_cfg.storage_message_retention_days"))
check("storage_message_max_rows 被 storage_vacuum 消费", src_has("tasks/storage_vacuum.py", "misc_cfg.storage_message_max_rows"))
check("旧键零读点: storage_vacuum 无 hermes_storage_message_*", src_has_no("tasks/storage_vacuum.py", "plugin_config.hermes_storage_message_"))
check("storage_db_path 被 mcp 装配消费", src_has("mcp/__init__.py", "misc_cfg.storage_db_path"))
check("旧键零读点: mcp 无 hermes_storage_db_path", src_has_no("mcp/__init__.py", "plugin_config.hermes_storage_db_path"))
check("group_profile_enabled 被 group_profile 消费", src_has("core/group_profile.py", "load_misc_config().group_profile_enabled"))
check("group_profile_enabled 被 identity 消费", src_has("core/identity.py", "load_misc_config().group_profile_enabled"))
check("旧键零读点: group_profile 无 hermes_group_profile_enabled", src_has_no("core/group_profile.py", "plugin_config.hermes_group_profile_enabled"))
check("旧键零读点: identity 无 hermes_group_profile_enabled", src_has_no("core/identity.py", "plugin_config.hermes_group_profile_enabled"))

print()
if FAILS:
    print(f"FAILED {len(FAILS)}: {FAILS}")
    sys.exit(1)
print("ALL PASS" + (f" (skipped {len(SKIPS)})" if SKIPS else ""))
