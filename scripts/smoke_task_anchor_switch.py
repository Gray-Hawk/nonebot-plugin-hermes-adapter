#!/usr/bin/env python3
"""任务锚总开关（条目 84）：真实渲染与真实控制入口 smoke。

跑法（venv，任一中立 cwd）：
    AGENT_PLUGIN_CUSTOM_DIR=$(mktemp -d) \
    /home/neonsky/NeonBot/.venv/bin/python -B \
        /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109/scripts/smoke_task_anchor_switch.py

不碰网络、不写插件数据目录：开关只走 chat_config 的真实内存快照 commit，
提示词走 build_* 真实入口，控制走 submit_control_impl 真实实现，
存量锚走真实 ActiveSessionManager。
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True

saved = list(sys.path)
try:
    sys.path = [p for p in sys.path if Path(p or ".").resolve() != ROOT]
    for name, module in list(sys.modules.items()):
        if name != "mcp" and not name.startswith("mcp."):
            continue
        file = getattr(module, "__file__", "") or ""
        if file and Path(file).resolve().is_relative_to(ROOT):
            sys.modules.pop(name, None)
    import mcp.types  # noqa: F401
finally:
    sys.path = saved

import nonebot  # noqa: E402

nonebot.init()
package = types.ModuleType("nonebot_plugin_hermes_adapter")
package.__path__ = [str(ROOT)]
package.__package__ = "nonebot_plugin_hermes_adapter"
sys.modules.setdefault("nonebot_plugin_hermes_adapter", package)

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.core import chat_config as cc  # noqa: E402
from nonebot_plugin_hermes_adapter.core import control_intent  # noqa: E402
from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core.active_session import (  # noqa: E402
    ActiveSessionManager,
    AnchorRecord,
    Participant,
)
from nonebot_plugin_hermes_adapter.core.hermes_client import _decision_hint  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin  # noqa: E402
from nonebot_plugin_hermes_adapter.core.prompt_protocol import (  # noqa: E402
    anchor_prompt_residue,
)
from nonebot_plugin_hermes_adapter.core.inflight import make_inflight_key  # noqa: E402
from nonebot_plugin_hermes_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_hermes_adapter.mcp.tools.control import (  # noqa: E402
    SubmitControlInput,
    submit_control_impl,
)

ADAPTER = "onebotv11"
GROUP = "462540549"
USER = "10001"
NOW = 1_800_000_000_000
FAILS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def commit(enabled: bool) -> None:
    cc._store.commit(cc.parse_chat_config({"task_anchor_enabled": enabled}))


def row() -> BufferedMessage:
    return BufferedMessage(
        ts=NOW - 1000, adapter=ADAPTER, group_id=GROUP, user_id=USER,
        nickname="主人", content="帮我盯一下这件事", origin=MessageOrigin.human(),
        id=201,
    )


def anchor() -> AnchorRecord:
    return AnchorRecord(
        participants=[Participant(user_id=USER, nickname="主人", joined_ts=NOW - 60_000)],
        text="帮我盯一下这件事", ts=NOW - 60_000, m_id=201,
    )


def render(mode: str, enabled: bool) -> dict[str, str]:
    commit(enabled)
    previous = plugin_config.hermes_api_interface
    object.__setattr__(plugin_config, "hermes_api_interface", mode)
    try:
        return {
            "group_system": pb.build_reactive_system_prompt(
                adapter=ADAPTER, group_id=GROUP, self_id="2968545011",
            ),
            "private_system": pb.build_passive_system_prompt(
                adapter=ADAPTER, is_private=True, user_id=USER, group_id=None,
                private_decision=True,
            ),
            "group_user": pb.build_reactive_user_content(
                addressed_to_bot=True, recent_messages=[], rows=[row()], current_image_urls=[],
                pinned_anchors=[anchor()], now_ms=NOW, self_id="2968545011",
                adapter=ADAPTER,
            ),
            "private_user": pb.build_passive_user_content(
                recent_messages=[], rows=[row()], current_image_urls=[],
                is_private=True, self_id="2968545011", adapter=ADAPTER,
            ),
        }
    finally:
        object.__setattr__(plugin_config, "hermes_api_interface", previous)


def anchor_lines(text: str) -> list[str]:
    return [
        line for line in text.splitlines()
        if anchor_prompt_residue(line) or "需要引用／" in line or "退窗只在" in line
    ]


print("=" * 78)
print("一、缺省状态与显式开启：既有行为（开关 true）")
print("=" * 78)
cc._store.commit(cc.ChatConfig())
check("缺省 task_anchor_enabled=True", cc.load_chat_config().task_anchor_enabled is True)

for mode in ("runs", "completions", "legacy"):
    on = render(mode, True)
    residues = {name: anchor_prompt_residue(text) for name, text in on.items()}
    check(f"开启[{mode}] 群系统提示保留锚说明", bool(residues["group_system"]))
    check(f"开启[{mode}] 群 user 注入 <pinned_tasks>", "<pinned_tasks>" in on["group_user"])
    if mode != "legacy":
        check(f"开启[{mode}] 私聊系统提示保留锚说明", bool(residues["private_system"]))
    print(f"\n--- 开启[{mode}] 群系统提示里的锚相关行 ---")
    for line in anchor_lines(on["group_system"]):
        print(line)

print("\n--- 开启[runs] 群 user content 的锚块 ---")
start = on["group_user"].find("<pinned_tasks>")
print(on["group_user"][start:start + 220] if start >= 0 else "（缺 <pinned_tasks>）")

print()
print("=" * 78)
print("二、显式关闭：锚说明/示例与 pinned 块同时消失，非锚控制说明保留")
print("=" * 78)
for mode in ("runs", "completions", "legacy"):
    off = render(mode, False)
    for name, text in off.items():
        check(f"关闭[{mode}] {name} 无锚残留", anchor_prompt_residue(text) == [],
              str(anchor_prompt_residue(text)))
    check(f"关闭[{mode}] 群 user 无 <pinned_tasks>", "<pinned_tasks>" not in off["group_user"])
    check(f"关闭[{mode}] reply_to 仍在提示词里", "reply_to" in off["group_system"])
    if mode == "runs":
        check("关闭[runs] 退窗说明仍是原句",
              "should_exit_active=true 表示这轮谈完了要退窗" in off["group_system"])
    elif mode == "completions":
        check("关闭[completions] 退窗入口仍在",
              "需要引用消息或提前退窗时" in off["group_system"])
    else:
        check("关闭[legacy] 退出门槛与字段仍在",
              "退出门槛（决定 should_exit_active）" in off["group_system"])
    print(f"\n--- 关闭[{mode}] 群系统提示里的控制说明行 ---")
    for line in off["group_system"].splitlines():
        if "submit_control 的字段" in line or "需要引用" in line:
            print(line)

off_runs = render("runs", False)
print("\n--- 关闭[runs] 私聊 scope 行 ---")
for line in off_runs["private_system"].splitlines():
    if "私聊里的控制" in line:
        print(line)
print("\n--- 关闭[runs] user content 末行提醒 ---")
print(off_runs["group_user"].splitlines()[-1])

print()
print("=" * 78)
print("三、旧接口 structured hint：字段行同步去掉四个锚字段")
print("=" * 78)
commit(True)
on_hint = _decision_hint()
commit(False)
off_hint = _decision_hint()
check("开启 hint 含 pin_anchor", "pin_anchor" in on_hint)
check("关闭 hint 无锚字段", all(name not in off_hint for name in
      ("pin_anchor", "release_anchor", "answer_anchor", "anchor_merge")))
check("关闭 hint 仍含 reply_to / should_exit_active",
      "reply_to" in off_hint and "should_exit_active" in off_hint)
print("关闭 hint 字段单：")
for line in off_hint.splitlines():
    if line.strip().startswith(("should_reply", "reply_text", "reply_to",
                                "should_exit_active", "not_final")):
        print(line[:120])

print()
print("=" * 78)
print("四、submit_control：关闭时锚字段无副作用、非锚控制可用")
print("=" * 78)
GROUP_KEY = make_inflight_key(ADAPTER, False, "", GROUP)
control_intent.CONTROL_QUEUE.discard(GROUP_KEY)


def call(**kwargs):
    return submit_control_impl(
        SubmitControlInput(adapter=ADAPTER, group_id=GROUP, **kwargs),
        scope=CallerScope.dev(), interface="runs",
        queue=control_intent.CONTROL_QUEUE,
    )


commit(False)
only_anchor = call(pin_anchor=True)
check("只带锚字段 → ok=False 且未登记", not only_anchor.ok and not only_anchor.registered,
      f"error={only_anchor.error!r}")
check("只带锚字段 → ignored_fields 可诊断", only_anchor.ignored_fields == ["pin_anchor"])
check("只带锚字段 → 队列为空", control_intent.CONTROL_QUEUE.pending(GROUP_KEY) == 0)

mixed = call(reply_to="m:7", should_exit_active=True, pin_anchor=True, answer_anchor="all")
check("混合调用 → 非锚字段照常登记", mixed.ok and mixed.registered and mixed.fields == ["reply_to", "should_exit_active"],
      f"fields={mixed.fields}")
check("混合调用 → 锚字段列进 ignored_fields", mixed.ignored_fields == ["pin_anchor", "answer_anchor"])
print("混合调用出队内容：", control_intent.CONTROL_QUEUE.pop(GROUP_KEY))

commit(True)
enabled = call(pin_anchor=True, answer_anchor="all")
check("开启时锚字段照常登记", enabled.ok and sorted(enabled.fields) == ["answer_anchor", "pin_anchor"],
      f"fields={enabled.fields}")
control_intent.CONTROL_QUEUE.discard(GROUP_KEY)

print()
print("=" * 78)
print("五、存量锚：关闭只隐藏，重新开启按原规则恢复")
print("=" * 78)
commit(False)
manager = ActiveSessionManager()
session = manager.trigger(ADAPTER, GROUP, USER, now_ms=NOW)
record = AnchorRecord(
    participants=[Participant(user_id=USER, nickname="主人", joined_ts=NOW)],
    text="存量委托任务", ts=NOW, m_id=88,
)
check("钉入存量锚", manager.pin_anchor(
    session, record, master_ids=set(), max_age_ms=60_000, now_ms=NOW,
))
hidden = pb.build_reactive_user_content(
    addressed_to_bot=True, recent_messages=[], rows=[row()], current_image_urls=[],
    pinned_anchors=[record], now_ms=NOW, self_id="2968545011", adapter=ADAPTER,
)
check("关闭时提示词不渲染 pinned 块", "<pinned_tasks>" not in hidden)
check("关闭时 manager 仍保留委托人", manager.pinned_commissioners(ADAPTER, GROUP) == {USER})
commit(True)
restored = manager.get_anchors(ADAPTER, GROUP, NOW, max_age_ms=60_000)
check("重开后读回同一锚", [a.text for a in restored] == ["存量委托任务"] and restored[0].m_id == 88)
shown = pb.build_reactive_user_content(
    addressed_to_bot=True, recent_messages=[], rows=[row()], current_image_urls=[],
    pinned_anchors=restored, now_ms=NOW, self_id="2968545011", adapter=ADAPTER,
)
check("重开后提示词重新渲染 pinned 块", "<pinned_tasks>" in shown and "存量委托任务" in shown)
print("恢复后的 pinned 块：")
start = shown.find("<pinned_tasks>")
print(shown[start:start + 200])

cc._store.commit(cc.ChatConfig())
print()
if FAILS:
    print(f"FAILED: {len(FAILS)} 项 -> {FAILS}")
    raise SystemExit(1)
print("ALL PASS")
