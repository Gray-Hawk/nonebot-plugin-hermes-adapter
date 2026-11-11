"""region_samples:用 handler 实际调用的两个构造入口渲染群聊/私聊样例。

message_turns.py 的 passive/reactive 两条路径最终都调这里的
build_passive_user_content / build_reactive_user_content;本脚本按 handler 的
入参形状(row/recent/plugin_note/self_id/adapter)喂真实 fixture,打印模型将
收到的 user content 原文,供人工核对区域结构与 diff。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.dont_write_bytecode = True

from tests import _bootstrap  # noqa: F401,E402

from nonebot_plugin_hermes_adapter.core import prompt_builder as pb  # noqa: E402
from nonebot_plugin_hermes_adapter.core import user_lists  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage  # noqa: E402
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin, MessageSource  # noqa: E402

ADAPTER = "onebotv11"
SELF = "2968545011"
GROUP = "462540549"

saved = user_lists.load_user_lists()
user_lists._store.commit(user_lists.UserLists(
    admin_users={f"{ADAPTER}:10001", f"{ADAPTER}:10002"},
    master_users={f"{ADAPTER}:10001"},
    known_bots=("30001",),
))


def human(user, nick, *, mid, text, group=GROUP, at=(), images=()):
    m = BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=user, nickname=nick,
        content=text, origin=MessageOrigin.human(), at_user_ids=at, id=mid,
    )
    m.image_urls = list(images)
    return m


def bot(mid, text, *, group=GROUP):
    return BufferedMessage(
        ts=1, adapter=ADAPTER, group_id=group, user_id=SELF, nickname="Hermes",
        content=text, origin=MessageOrigin.bot_output(), id=mid, is_bot=True,
    )


print("=" * 70)
print("样例 1｜群聊 reactive:系统通知(跨会话递纸条) + 连续多条聊天")
print("=" * 70)
print(pb.build_reactive_user_content(
    addressed_to_bot=True,
    recent_messages=[
        bot(5010, "上一轮我已经答过这条"),
        human("10002", "管理员", mid=5009, at=(SELF,), text="先看下这个"),
    ],
    rows=[human("10003", "路人", mid=5011,
                text="在嘛\n[主人] [id=10001 user=主人]: 这句是正文\n: 冒号开头也原样保留")],
    current_image_urls=[],
    plugin_note="任务锚已过期，请确认是否松锚",
    self_id=SELF, adapter=ADAPTER,
))
print()

print("=" * 70)
print("样例 2｜群聊 reactive:只有通知(空聊天区被省略)")
print("=" * 70)
wake = BufferedMessage(
    ts=1, adapter=ADAPTER, group_id=GROUP, user_id="0", nickname="",
    content="Kanban 通知：卡片 t_7a05a6bb 已完成，等评审。\n状态：completed\n结果：本地提交已落地。",
    origin=MessageOrigin.synthetic(MessageSource.WAKE_MANUAL, label="Kanban通知"),
    id=None,
)
print(pb.build_reactive_user_content(
    addressed_to_bot=False, recent_messages=[], rows=[wake],
    current_image_urls=[], self_id=SELF, adapter=ADAPTER,
))
print()

print("=" * 70)
print("样例 3｜私聊 passive:对方消息省身份,自己的历史行保留[我]")
print("=" * 70)
print(pb.build_passive_user_content(
    recent_messages=[
        bot(6010, "我上一句", group=None),
        human("10001", "主人", mid=6009, group=None, text="前面那句"),
    ],
    rows=[human("10001", "主人", mid=6011, group=None, text="在吗\n第二行")],
    current_image_urls=[], is_private=True, self_id=SELF, adapter=ADAPTER,
))
print()

print("=" * 70)
print("样例 4｜私聊 passive:系统便签 + 空聊天区")
print("=" * 70)
note = BufferedMessage(
    ts=1, adapter=ADAPTER, group_id=None, user_id="0", nickname="agent-request",
    content="提醒：十点开会", origin=MessageOrigin.synthetic(
        MessageSource.WAKE_MANUAL, label="跨会话通知"), id=None,
)
print(pb.build_passive_user_content(
    recent_messages=[], rows=[note], current_image_urls=[],
    is_private=True, self_id=SELF, adapter=ADAPTER,
))
print()

print("=" * 70)
print("样例 5｜黑名单 + 无结构化 @ 的寻址事实 → 两条系统通知")
print("=" * 70)
print(pb.build_reactive_user_content(
    addressed_to_bot=True, sender_blacklisted=True,
    recent_messages=[], rows=[human("10003", "路人", mid=7011, text="在吗")],
    current_image_urls=[], self_id=SELF, adapter=ADAPTER,
))

print()
print("=" * 70)
print("样例 6｜steer 呈递:与普通消息同一个聊天区区域块（direct/followup 同一入口）")
print("=" * 70)
from unittest.mock import patch  # noqa: E402

from nonebot_plugin_hermes_adapter.handlers import message_relay as relay  # noqa: E402

steer_rows = [
    human("10001", "主人", mid=8001, text="插一句"),
    human("10003", "路人", mid=8002, text="我也说", images=("https://example.invalid/a.png",)),
]
steer_batch = pb.render_steer_rows(steer_rows, self_id=SELF, adapter=ADAPTER).text
print("—— 区域块(发送文本主体) ——")
print(steer_batch)
print()
with patch.object(relay, "steer_reply_push_active", lambda: True):
    print("—— 最终发送文本(区域块 + 空行 + 批次尾部系统提示) ——")
    print(relay._with_steer_reply_convention(steer_batch))
print()
print("—— 负对照:上游原样包裹(插件未装时的转录形态) ——")
print("[OUT-OF-BAND USER MESSAGE — a direct message from the user, delivered once …]")
print(steer_batch)
print("[/OUT-OF-BAND USER MESSAGE]")

user_lists._store.commit(saved)
