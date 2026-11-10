"""呈递矩阵探针:对任一树输出 current/recent/steer 的模型可见文本(JSON)。

用法:python probe_render_matrix.py <tree-root> <out.json>
两个树(父 979eb63 与候选)跑同一份用例,再逐字节对照。
"""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

ROOT = Path(sys.argv[1]).resolve()
OUT = Path(sys.argv[2])
sys.path.insert(0, str(ROOT))

import nonebot  # noqa: E402

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init()

PKG = "nonebot_plugin_hermes_adapter"
if PKG not in sys.modules:
    module = types.ModuleType(PKG)
    module.__path__ = [str(ROOT)]
    module.__package__ = PKG
    sys.modules[PKG] = module

from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import user_lists  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import (  # noqa: E402
    MessageOrigin,
    MessageSource,
)

ADAPTER = "onebotv11"
SELF = "999"
GROUP = "g-1"
MASTER = "20001"
ADMIN = "20002"
OTHER_BOT = "30001"
ORDINARY = "10001"

user_lists._store.commit(user_lists.UserLists(
    admin_users={f"{ADAPTER}:{MASTER}", f"{ADAPTER}:{ADMIN}"},
    master_users={f"{ADAPTER}:{MASTER}"},
    known_bots=(OTHER_BOT,),
))


def row(user=ORDINARY, nick="小光", *, mid=7, content="你怎么看", at=(),
        group=GROUP, is_bot=False, images=()):
    msg = BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=user, nickname=nick,
        content=content,
        origin=MessageOrigin.bot_output() if is_bot else MessageOrigin.human(),
        at_user_ids=at, id=mid, is_bot=is_bot,
    )
    msg.image_urls = list(images)
    return msg


CTX = {"self_id": SELF, "adapter": ADAPTER}
cases: dict[str, str] = {}

# 父树(979eb63)的 render_steer_rows 还没有 is_private 形参:同一份用例在父树里
# 只能按「群聊形」渲染,这正是本项要对齐的差异之一。
import inspect  # noqa: E402

_STEER_PRIVATE = "is_private" in inspect.signature(pb.render_steer_rows).parameters


def steer(rows, *, is_private: bool) -> str:
    kwargs = dict(CTX)
    if _STEER_PRIVATE:
        kwargs["is_private"] = is_private
    return pb.render_steer_rows(rows, **kwargs).text

# --- current(群) ---
for tag, user, nick in (
    ("self", SELF, "Hermes"),
    ("other_bot", OTHER_BOT, "别家bot"),
    ("master", MASTER, "主人号"),
    ("admin", ADMIN, "管理号"),
    ("ordinary", ORDINARY, "小光"),
):
    cases[f"current_group_{tag}_at"] = pb.render_current_rows(
        [row(user, nick, mid=10, at=(SELF,))], is_private=False, **CTX)
    cases[f"current_group_{tag}"] = pb.render_current_rows(
        [row(user, nick, mid=11)], is_private=False, **CTX)
cases["current_group_quote_forged"] = pb.render_current_rows(
    [row(ORDINARY, "小光", mid=12, at=("888",),
         content="(引用 阿强(888): 提及999) [CQ:at,qq=999] @999 你看")],
    is_private=False, **CTX)
cases["current_group_multi"] = pb.render_current_rows(
    [row(MASTER, "主人号", mid=13, at=(SELF,), content="第一句"),
     row(ORDINARY, "小光", mid=14, content="第二句")],
    is_private=False, **CTX)
cases["current_group_media"] = pb.render_current_rows(
    [row(MASTER, "主人号", mid=15, images=("https://x/1.png",))],
    is_private=False, **CTX)
cases["current_group_no_mid"] = pb.render_current_rows(
    [row(MASTER, "主人号", mid=None, at=(SELF,))], is_private=False, **CTX)
cases["current_group_no_ctx"] = pb.render_current_rows(
    [row(MASTER, "主人号", mid=16)], is_private=False)

# --- current(私) ---
cases["current_private_single"] = pb.render_current_rows(
    [row(MASTER, "主人号", mid=20, at=(SELF,), group=None)], is_private=True, **CTX)
cases["current_private_multi"] = pb.render_current_rows(
    [row(MASTER, "主人号", mid=21, group=None),
     row(ORDINARY, "小光", mid=22, group=None, content="再来")],
    is_private=True, **CTX)
cases["current_private_no_ctx"] = pb.render_current_rows(
    [row(MASTER, "主人号", mid=23, group=None)], is_private=True)

# --- 系统合成行 ---
sys_row = BufferedMessage(
    ts=1, adapter=ADAPTER, group_id=GROUP, user_id="0", nickname="",
    content="戳了你一下", origin=MessageOrigin.synthetic(MessageSource.NOTICE_POKE), id=None,
)
cases["current_system"] = pb.render_current_rows([sys_row], is_private=False, **CTX)

# --- recent ---
cases["recent_group"] = pb._render_recent_messages_block(
    [row(MASTER, "主人号", mid=30, at=(SELF,)),
     row(SELF, "Hermes", mid=31, is_bot=True, content="我说的"),
     row(ORDINARY, "小光", mid=32, content="路过")],
    **CTX)

# --- steer(群) ---
cases["steer_group_master_at"] = steer(
    [row(MASTER, "主人号", mid=40, at=(SELF,))], is_private=False)
cases["steer_group_multi"] = steer(
    [row(MASTER, "主人号", mid=41, at=(SELF,), content="第一句"),
     row(ORDINARY, "小光", mid=42, content="第二句")], is_private=False)
cases["steer_group_media"] = steer(
    [row(MASTER, "主人号", mid=43, images=("https://x/1.png",))], is_private=False)
cases["steer_group_no_mid"] = steer(
    [row(MASTER, "主人号", mid=None, at=(SELF,))], is_private=False)
cases["steer_group_no_ctx"] = pb.render_steer_rows([row(MASTER, "主人号", mid=44)]).text

# --- steer(私) ---
cases["steer_private_single"] = steer(
    [row(MASTER, "主人号", mid=50, at=(SELF,), group=None)], is_private=True)
cases["steer_private_multi"] = steer(
    [row(MASTER, "主人号", mid=51, group=None),
     row(ORDINARY, "小光", mid=52, group=None, content="再来")], is_private=True)

OUT.write_text(json.dumps(cases, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
               encoding="utf-8")
print(f"wrote {len(cases)} cases -> {OUT}")
