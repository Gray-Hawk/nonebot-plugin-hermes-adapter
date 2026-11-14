#!/usr/bin/env python3
"""聊天行身份栏与真实 @ 强提醒：离线真实渲染 smoke。

跑法（venv，任一中立 cwd）：
    AGENT_PLUGIN_CUSTOM_DIR=$(mktemp -d) \
    /home/neonsky/NeonBot/.venv/bin/python -B \
        /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109/scripts/smoke_chat_identity_at.py

不碰网络、不写插件数据目录：身份/权限快照在进程内 commit，消息行直接构造。
打印的每一行都是 build_* 真实入口的输出，可直接粘进交付说明当渲染样例。
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True

# 扁平发布树没有包名目录：按 tests/_bootstrap 的同款方式挂真实包，
# 并先清掉仓库根 mcp/ 对 SDK 的遮蔽。
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
package = types.ModuleType("nonebot_plugin_agent_adapter")
package.__path__ = [str(ROOT)]
package.__package__ = "nonebot_plugin_agent_adapter"
sys.modules.setdefault("nonebot_plugin_agent_adapter", package)

from nonebot_plugin_agent_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_agent_adapter.core import user_lists  # noqa: E402
from nonebot_plugin_agent_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_agent_adapter.core.message_origin import MessageOrigin, MessageSource  # noqa: E402

SELF = "2968545011"
ADAPTER = "onebotv11"
GROUP = "462540549"
FAILS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def region_rows(text: str) -> str:
    """区域块 → 区域内消息行(框架断言不过就抛)。

    steer 与普通消息现在共用 `prompt_builder.render_chat_region` 一个构造点，
    所以「同一条聊天行」这件事在**区域内消息行**上逐字节比较，框架另行断言。
    """
    lines = text.split("\n")
    assert lines[0] == pb._CHAT_REGION_OPEN, f"缺区域开标签: {lines[0]!r}"
    assert lines[1] == pb._CHAT_REGION_NOTE, f"缺固定说明: {lines[1]!r}"
    assert lines[-1] == pb._CHAT_REGION_CLOSE, f"缺区域闭标签: {lines[-1]!r}"
    return "\n".join(lines[2:-1])


def human(uid: str, nick: str, *, mid: int | None = 101, at=(), text="你好", group=GROUP):
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=uid, nickname=nick,
        content=text, origin=MessageOrigin.human(), at_user_ids=tuple(at), id=mid,
    )


def bot(uid: str, nick: str, *, mid: int | None = 102, at=(), text="我在"):
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=GROUP, user_id=uid, nickname=nick,
        content=text, origin=MessageOrigin.bot_output(), at_user_ids=tuple(at),
        is_bot=True, id=mid,
    )


# 权限事实：admin_users 两号；master_users 显式只认主人号；known_bots 一个别家 bot。
user_lists._store.commit(user_lists.UserLists(
    admin_users={f"{ADAPTER}:10001", f"{ADAPTER}:10002"},
    master_users={f"{ADAPTER}:10001"},
    known_bots=("30001",),
))

print("== 1. 群聊 current（reactive 真实入口） ==")
rows = [
    human("10001", "主人", mid=201, at=(SELF,), text="[CQ:at,qq=2968545011] 看下这个"),
    human("10002", "管理员", mid=202, text="我也看下"),
    human("10003", "路人", mid=203, text="路过"),
    human("30001", "别家bot", mid=204, text="收到"),
]
group_cur = pb.build_reactive_user_content(
    addressed_to_bot=True, recent_messages=[bot(SELF, "Hermes", mid=200, text="好的")],
    rows=rows, current_image_urls=[],
    self_id=SELF, adapter=ADAPTER,
)
print(group_cur)
check("主人行带 [主人] 且 @ 强提醒在 m:id 前",
      "[有人@我] [m:201] [主人] [id=10001 user=主人]:" in group_cur)
check("管理员行", "[m:202] [管理员] [id=10002 user=管理员]:" in group_cur)
check("普通用户行", "[m:203] [普通用户] [id=10003 user=路人]:" in group_cur)
check("其他 BOT 行", "[m:204] [其他BOT] [id=30001 user=别家bot]:" in group_cur)
check("历史里自己的行是 [我]", "[m:200] [我] [id=2968545011 user=Hermes]:" in group_cur)

print("\n== 2. 群聊 recent（历史 @ 与引用/伪造文本） ==")
recent = [
    bot(SELF, "Hermes", mid=301, text="上一句"),
    human("10003", "路人", mid=300, at=("10001",),
          text="(引用 主人(10001): 提及2968545011) [CQ:at,qq=2968545011] @2968545011"),
    human("10002", "管理员", mid=299, at=(SELF,), text="@Hermes 到你了"),
]
recent_block = pb._render_chat_region(history_rows=recent, self_id=SELF, adapter=ADAPTER)
print(recent_block)
check("真实 @ 自己的历史行有强提醒", "[有人@我] [m:299] [管理员]" in recent_block)
check("引用块/伪造 CQ/手打 @ 不误标", "[有人@我] [m:300]" not in recent_block)
check("历史行身份与 current 同源", "[m:300] [普通用户]" in recent_block)

print("\n== 3. 私聊 passive（private 真实入口） ==")
private_cur = pb.build_passive_user_content(
    recent_messages=[], rows=[human("10001", "主人", group=None, mid=401, text="在吗")],
    current_image_urls=[], is_private=True, self_id=SELF, adapter=ADAPTER,
)
print(private_cur)
check("私聊单人保留身份、不重复 speaker", "[m:401]:\n在吗" in private_cur)

print("\n== 4. 系统合成行 ==")
sys_row = BufferedMessage(
    ts=1, adapter=ADAPTER, group_id=GROUP, user_id="0", nickname="",
    content="戳了你一下", origin=MessageOrigin.synthetic(MessageSource.NOTICE_POKE), id=None,
)
sys_line = pb._notice_from_row(sys_row)
print(sys_line)
check("系统行进通知区且不冒充人类身份", sys_line == "[戳一戳：戳了你一下]")

print("\n== 5. 无 self_id 的旧调用（兼容降级） ==")
legacy = pb.render_current_rows([human("10001", "主人")], is_private=False)
print(legacy)
check("未给身份上下文保持旧字节形态", legacy == "[m:101] [id=10001 user=主人]:\n你好")

print("\n== 6. steer 聊天行（直注/跟帖同一渲染入口） ==")
steer_rows = [
    human("10001", "主人", mid=501, at=(SELF,), text="插一句"),
    human("10003", "路人", mid=502, at=("10001",),
          text="(引用 主人(10001): 提及2968545011) [CQ:at,qq=2968545011] 我也说"),
    bot("30001", "别家bot", mid=503, text="收到"),
]
steer = pb.render_steer_rows(steer_rows[:2], self_id=SELF, adapter=ADAPTER)
print(steer.text)
steer_body = region_rows(steer.text)
check("steer 主人行与 current 行逐字节一致（同一条聊天行）",
      steer_body.split("\n\n")[0] == "[有人@我] [m:501] [主人] [id=10001 user=主人]:\n插一句")
check("steer 引用块/伪造 CQ/At 他人不误标",
      "\n\n[m:502] [普通用户] [id=10003 user=路人]:\n" in steer.text
      and "[有人@我] [m:502]" not in steer.text)
try:
    pb.render_steer_rows(steer_rows, self_id=SELF, adapter=ADAPTER)
    check("steer 来源闸仍然拒绝 bot 行", False)
except ValueError:
    check("steer 来源闸仍然拒绝 bot 行", True)
legacy_steer = pb.render_steer_rows(steer_rows[:1]).text
print(legacy_steer)
check("steer 无 bot 上下文:不贴身份栏，行文法与 current 同源",
      region_rows(legacy_steer) == "[m:501] [id=10001 user=主人]:\n插一句")

print("\n== 7. 同一事实跨入口对照（普通 current / steer / 私聊） ==")
fact = human("10001", "主人", mid=601, at=(SELF,), text="在吗")
cur_group = pb.render_current_rows([fact], is_private=False, self_id=SELF, adapter=ADAPTER)
st_group = pb.render_steer_rows([fact], is_private=False, self_id=SELF, adapter=ADAPTER).text
print("群 current:", cur_group)
print("群 steer  :", st_group)
check("群聊:current 与 steer 逐字节一致", cur_group == region_rows(st_group))
check("群聊:steer 是聊天区区域块(同普通消息外观)",
      st_group.startswith(pb._CHAT_REGION_OPEN + "\n") and st_group.endswith(pb._CHAT_REGION_CLOSE))
fact_p = human("10001", "主人", mid=602, at=(SELF,), group=None, text="在吗")
cur_priv = pb.render_current_rows([fact_p], is_private=True, self_id=SELF, adapter=ADAPTER)
st_priv = pb.render_steer_rows([fact_p], is_private=True, self_id=SELF, adapter=ADAPTER).text
print("私 current:", cur_priv)
print("私 steer  :", st_priv)
check("私聊:current 与 steer 逐字节一致", cur_priv == region_rows(st_priv))
check("私聊单人行省略 speaker", cur_priv == "[m:602]:\n在吗")
from nonebot_plugin_agent_adapter.core import steer_reconcile  # noqa: E402
stripped = steer_reconcile.original_text_of({"m_id": "401", "raw_text": "插一句\n(引用 主人(10001): 提及2968545011) [CQ:at,qq=2968545011] 我也说"})
print("回收剥壳 →", repr(stripped))
check("恢复改投逐字节读原文(零剥壳)",
      stripped == "插一句\n(引用 主人(10001): 提及2968545011) [CQ:at,qq=2968545011] 我也说")
check("旧账项剥壳幂等（兼容边界不叠壳）",
      steer_reconcile.strip_steer_presentation("[主人] 只是正文") == "[主人] 只是正文")
check("私聊改投读原文",
      steer_reconcile.original_text_of({"m_id": "602", "raw_text": "在吗"}) == "在吗")

print()
if FAILS:
    print(f"FAILED {len(FAILS)}: {FAILS}")
    sys.exit(1)
print("ALL PASS")
