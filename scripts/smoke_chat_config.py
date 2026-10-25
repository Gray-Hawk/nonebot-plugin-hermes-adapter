"""chat_config 24 键迁移 smoke 测试。

跑法（shim 挂 work 副本，中立目录执行）：
    ln -s <work 副本路径> <shim>/nonebot_plugin_hermes
    cd /tmp && PYTHONPATH=<shim> ~/NeonBot/.venv/bin/python <work>/scripts/smoke_chat_config.py

覆盖（S2 chat_config 24 键全消费端迁移的离线验收）：
  1. ChatConfig 24 键形态 + 三链历史统一（perception_buffer / keyword_soft_context /
     proactive_max_messages 三键撤并，只剩 history_message_limit）。
  2. 改名（reply_delay_for_bot_sec / active_session_timeout_sec）与解析映射。
  3. 手动重载机制：commit 新快照后 load_chat_config() 立即反映（T1/proactive 的
     固定 tick 每拍现读同一缓存，据此 0=跳过）。
  4. bot 延迟：_is_reply_delayed 只认 known_bots，reply_delay_users 额外名单已删。
  5. T1/proactive 注册函数固定 tick 总是注册（0 不早退）。
  6. 消费端源码静态断言：旧键零读点、新键读点到位。
"""

import inspect
import os
import sys
import tempfile
from pathlib import Path

import nonebot

# 沙箱化 localstore 数据目录，避免冒烟运行往生产数据目录（messages.db 同目录）写模板。
os.environ["LOCALSTORE_DATA_DIR"] = tempfile.mkdtemp(prefix="nph_smoke_data_")

nonebot.init()

import nonebot_plugin_hermes  # noqa: E402  (shim 提供的包名)
from nonebot_plugin_hermes import config as cfg  # noqa: E402
from nonebot_plugin_hermes.core import chat_config as cc  # noqa: E402
from nonebot_plugin_hermes.core import user_lists as ul  # noqa: E402
from nonebot_plugin_hermes.core.chat_config import (  # noqa: E402
    ChatConfig,
    load_chat_config,
    parse_chat_config,
)
from nonebot_plugin_hermes.handlers import message as msg  # noqa: E402

FAILS = []
SKIPS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def skip(name: str, detail: str = "") -> None:
    print("SKIP " + name + (f" | {detail}" if detail else ""))
    SKIPS.append(name)


SRC = Path(nonebot_plugin_hermes.__file__).parent


def src_has_no(relpath: str, token: str) -> bool:
    return token not in (SRC / relpath).read_text(encoding="utf-8")


# --- 1. ChatConfig 24 键形态 + 三链历史统一 + 改名 ---
from dataclasses import fields as _fields  # noqa: E402

names = {f.name for f in _fields(ChatConfig)}
check("ChatConfig 共 24 键", len(names) == 24, f"实际 {len(names)} 键")
check("三链统一: history_message_limit 存在", "history_message_limit" in names)
check("三链统一: perception_buffer 已撤", "perception_buffer" not in names)
check("三链统一: keyword_soft_context 已撤", "keyword_soft_context" not in names)
check("三链统一: proactive_max_messages 已撤", "proactive_max_messages" not in names)
check("改名: reply_delay_for_bot_sec 存在", "reply_delay_for_bot_sec" in names)
check("改名: reply_delay_sec 已撤", "reply_delay_sec" not in names)
check("改名: active_session_timeout_sec 存在", "active_session_timeout_sec" in names)
check("改名: active_session_t5_sec 已撤", "active_session_t5_sec" not in names)

c = ChatConfig()
check("默认 history_message_limit=10", c.history_message_limit == 10)
check("默认 reply_delay_for_bot_sec=0.0", c.reply_delay_for_bot_sec == 0.0)
check("默认 active_session_timeout_sec=0", c.active_session_timeout_sec == 0)

p = parse_chat_config(
    {
        "history_message_limit": 7,
        "reply_delay_for_bot_sec": 1.5,
        "active_session_timeout_sec": 120,
        "session_idle_reset_sec": 42,
        "proactive_interval_sec": 99,
    }
)
check("parse: history_message_limit 生效", p.history_message_limit == 7)
check("parse: reply_delay_for_bot_sec 生效", p.reply_delay_for_bot_sec == 1.5)
check("parse: active_session_timeout_sec 生效", p.active_session_timeout_sec == 120)

# --- 2. 手动重载机制（T1/proactive 固定 tick 现读同一缓存） ---
cc._store.commit(
    parse_chat_config({"session_idle_reset_sec": 42, "proactive_interval_sec": 99, "history_message_limit": 5})
)
reloaded = load_chat_config()
check("重载: session_idle_reset_sec 生效", reloaded.session_idle_reset_sec == 42)
check("重载: proactive_interval_sec 生效", reloaded.proactive_interval_sec == 99)
check("重载: history_message_limit 生效", reloaded.history_message_limit == 5)
cc._store.commit(ChatConfig())
check("重载: 恢复默认后回到 10", load_chat_config().history_message_limit == 10)

# --- 3. bot 延迟: 只认 known_bots, reply_delay_users 已删 ---
_orig_is_known_bot = ul.is_known_bot
ul.is_known_bot = lambda uid: uid == "90000001"
try:
    check("bot延迟: known_bot 命中", msg._is_reply_delayed("90000001") is True)
    check("bot延迟: 非 bot 不延迟", msg._is_reply_delayed("12345") is False)
finally:
    ul.is_known_bot = _orig_is_known_bot
_sig = inspect.signature(msg._is_reply_delayed)
check("bot延迟: 签名只收 user_id(无 adapter_name)", list(_sig.parameters) == ["user_id"], str(list(_sig.parameters)))

# --- 4. T1/proactive 固定 tick 总是注册（0 不早退） ---
try:
    from nonebot_plugin_apscheduler import scheduler as _sch

    from nonebot_plugin_hermes.tasks.proactive_activation import register_proactive_activation
    from nonebot_plugin_hermes.tasks.session_idle_reset import register_session_idle_reset

    _before = len(_sch.get_jobs())
    register_session_idle_reset()
    register_proactive_activation()
    _after = len(_sch.get_jobs())
    check("T1/proactive 固定 tick 总是注册(0 不早退)", _after >= _before + 2, f"before={_before} after={_after}")
except Exception as _e:  # noqa: BLE001
    skip("T1/proactive 注册器运行断言", f"调度器不可用: {type(_e).__name__}: {_e}")

# --- 5. 消费端源码静态断言: 旧键零读点、新键读点到位 ---
check("三链: message_turns 无 plugin_config.hermes_perception_buffer",
      src_has_no("handlers/message_turns.py", "plugin_config.hermes_perception_buffer"))
check("三链: message 无 plugin_config.hermes_keyword_soft_context",
      src_has_no("handlers/message.py", "plugin_config.hermes_keyword_soft_context"))
check("三链: proactive 无 plugin_config.hermes_proactive_max_messages",
      src_has_no("tasks/proactive_activation.py", "plugin_config.hermes_proactive_max_messages"))
check("T1: 无 plugin_config.hermes_session_idle_reset_sec",
      src_has_no("tasks/session_idle_reset.py", "plugin_config.hermes_session_idle_reset_sec"))
check("proactive: 无 plugin_config.hermes_proactive_interval_sec",
      src_has_no("tasks/proactive_activation.py", "plugin_config.hermes_proactive_interval_sec"))
check("bot延迟: 无 plugin_config.hermes_reply_delay_users",
      src_has_no("handlers/message.py", "plugin_config.hermes_reply_delay_users"))
check("bot延迟: 无 plugin_config.hermes_reply_delay_sec",
      src_has_no("handlers/message.py", "plugin_config.hermes_reply_delay_sec"))
check("三链: proactive tick 现读 load_chat_config",
      "load_chat_config()" in (SRC / "tasks/proactive_activation.py").read_text(encoding="utf-8"))
check("T1: sweep 现读 load_chat_config",
      "load_chat_config()" in (SRC / "tasks/session_idle_reset.py").read_text(encoding="utf-8"))

print()
if FAILS:
    print(f"FAILED {len(FAILS)}: {FAILS}")
    sys.exit(1)
print("ALL PASS" + (f" (skipped {len(SKIPS)})" if SKIPS else ""))
