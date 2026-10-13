"""In-flight 调用追踪 + 排队/合并重燃支持。

修同一 (adapter, group_id|user_id) 上事件 task 并发调 chat() 的 bug:
in-flight 时新消息不占并发,只排队;当前一发完成后按顺序接力。

线程安全:**否**。预设单线程 asyncio 事件循环,与 ActiveSessionManager 一致。

2026-10-13 队列化(维护者拍板):pending 从「单槽 latest-wins」改成
「委托队列 + 闲聊跟进槽」。委托=私聊消息或群聊 addressed_to_bot 的 @;
不同委托人的 @ 各自排队(总上限 3),同一委托人重复 @ 只留一份并拼接内容;
非委托的旁观消息仍 latest-wins,只留一个跟进触发,绝不顶掉委托。
队列满只丢新委托并 WARN,不用闲聊填满委托槽。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Literal

from .message_buffer import BufferedMessage

# Refire 链最大深度。超过则清空队列并释放 slot,等下一个新触发。
# 一次 burst 最多产出 1(主回) + MAX_REFIRE_DEPTH(链尾)= 4 发回复。
MAX_REFIRE_DEPTH = 3

# 委托排队上限(2026-10-13 维护者拍板):同一 target 最多 3 条委托;
# 同一委托人同时在队只留 1 条(后来的拼进这条)。超出上限的新委托直接丢,
# 防止「队列积压 = 一条消息补跑一次 LLM turn」。
MAX_PENDING_COMMISSIONS = 3
MAX_PENDING_PER_USER = 1


@dataclass
class PendingEntry:
    """In-flight 期间排队的消息及触发元数据。

    pending 不裸存 BufferedMessage——单独抽 PendingEntry 是为了跨 turn 边界
    保留几个 plumbing 决策所需要的事实:
      - 这条排队消息原本是不是 explicit-trigger
      - 它是否被平台确认为「冲 bot 说的」(寻址与触发是两个事实,不互相派生:
        synth 事件可以 explicit 而不寻址,trigger=all 下每条都 explicit 也不寻址)
      - 它的原始 adapter 侧 message_id(用于失败时贴 emoji notice)
    """

    msg: BufferedMessage
    is_explicit_trigger: bool
    original_msg_id: str | int | None = None
    # 默认 False 是安全方向:漏传只会让模型自己判归属,不会错误断言「这条冲你说的」。
    addressed_to_bot: bool = False


@dataclass
class InflightSlot:
    started_at: int
    # 非委托旁观触发:只留最新一条,turn 结束后补一次跟进即可。
    followup: PendingEntry | None = None
    # 委托触发:按到达顺序 FIFO;同一委托人重复触发合并进已排队条目。
    commissions: deque[PendingEntry] = field(default_factory=deque)


def _merge_pending_message(old: BufferedMessage, new: BufferedMessage) -> None:
    """同一委托人的在队消息并入旧条目(语义同防抖拼接)。

    文本换行拼接;图片/表情包 URL 追加去重;ts/nickname 取最新(接力时序以最后一条
    为准);original_msg_id 由调用方随 PendingEntry 换成最新。reply_to_ts 目前全空,
    仍取最新值,避免未来启用后旧 None 盖掉新值。
    """

    new_text = (new.content or "").strip()
    if new_text:
        old_text = (old.content or "").strip()
        old.content = f"{old_text}\n{new_text}" if old_text else new_text
    for field_name in ("image_urls", "sticker_urls"):
        incoming = list(getattr(new, field_name) or [])
        if incoming:
            merged = list(getattr(old, field_name) or [])
            merged.extend(u for u in incoming if u not in merged)
            setattr(old, field_name, merged)
    old.ts = new.ts
    old.nickname = new.nickname
    old.reply_to_ts = new.reply_to_ts


class InflightRegistry:
    """per-target 非阻塞 busy 标记 + 排队单元。

    Key 约定:
      - 群: ("adapter", "group:" + group_id)
      - 私聊: ("adapter", "private:" + user_id)

    入队规则:
      - 空 slot → 占位,返回 'entered'
      - commission=True(委托):同一 user_id 已在队 → 拼接内容,返回 'queued';
        新委托人且队列未满 → 追加,返回 'queued';队列满 → 不丢旧委托,丢本条,
        返回 'queue_full'
      - commission=False(旁观/闲聊):只刷新 followup 槽(latest wins),返回 'queued'

    不持有任何 asyncio.Task 引用 —— 重燃由 caller 用 create_task 自己接手,
    registry 只负责「现在有没有人在跑」+「跑完后下一发是谁」两个状态。
    """

    def __init__(self) -> None:
        self._slots: dict[tuple[str, str], InflightSlot] = {}

    def try_enter(
        self,
        key: tuple[str, str],
        current_msg: BufferedMessage,
        *,
        is_explicit_trigger: bool,
        original_msg_id: str | int | None,
        now_ms: int,
        addressed_to_bot: bool = False,
        commission: bool = False,
    ) -> Literal["entered", "queued", "queue_full"]:
        """无 slot → 占位 started_at=now_ms,返回 'entered';否则按规则入队。"""
        slot = self._slots.get(key)
        if slot is None:
            self._slots[key] = InflightSlot(started_at=now_ms)
            return "entered"

        entry = PendingEntry(
            msg=current_msg,
            is_explicit_trigger=is_explicit_trigger,
            original_msg_id=original_msg_id,
            addressed_to_bot=addressed_to_bot,
        )
        if not commission:
            # 旁观闲聊也不 latest-wins 了(2026-10-13 维护者指出「正常任务会被打趣驱逐」):
            # 已在队则把内容拼进同一跟进单,触发归属保留首条,文本/媒体不丢;
            # 它只承诺补一次跟进判断,真正的取舍由那次 submit_decision 做。
            if slot.followup is not None:
                _merge_pending_message(slot.followup.msg, current_msg)
                slot.followup.is_explicit_trigger = slot.followup.is_explicit_trigger or is_explicit_trigger
                slot.followup.addressed_to_bot = slot.followup.addressed_to_bot or addressed_to_bot
                slot.followup.original_msg_id = original_msg_id
            else:
                slot.followup = entry
            return "queued"

        for queued in slot.commissions:
            if queued.msg.user_id == current_msg.user_id:
                _merge_pending_message(queued.msg, current_msg)
                queued.is_explicit_trigger = queued.is_explicit_trigger or is_explicit_trigger
                queued.addressed_to_bot = queued.addressed_to_bot or addressed_to_bot
                queued.original_msg_id = original_msg_id
                return "queued"
        if len(slot.commissions) >= MAX_PENDING_COMMISSIONS:
            return "queue_full"
        slot.commissions.append(entry)
        return "queued"

    def pop_next_pending(self, key: tuple[str, str], *, after_ts: int) -> PendingEntry | None:
        """取下一发接力。委托 FIFO 优先;没有委托才轮到闲聊跟进槽。

        after_ts 之前的条目视为「当前 turn 已覆盖」直接丢弃,防止旧时间戳重复点火。
        返回 None 不代表必须 exit——调用方确认没有下一发后才 exit。
        """
        slot = self._slots.get(key)
        if slot is None:
            return None
        while slot.commissions and slot.commissions[0].msg.ts <= after_ts:
            slot.commissions.popleft()
        if slot.commissions:
            return slot.commissions.popleft()
        if slot.followup is not None:
            entry = slot.followup
            slot.followup = None
            if entry.msg.ts > after_ts:
                return entry
        return None

    def pending_stats(self, key: tuple[str, str]) -> tuple[int, bool]:
        """(委托排队数, 是否有闲聊跟进)。日志观测用。"""
        slot = self._slots.get(key)
        if slot is None:
            return 0, False
        return len(slot.commissions), slot.followup is not None

    def exit(self, key: tuple[str, str]) -> None:
        """释放 slot。仍在队的委托/跟进随 slot 一并丢弃(调用方应先 pop 干净)。
        slot 不存在则 no-op。
        """
        self._slots.pop(key, None)

    def is_inflight(self, key: tuple[str, str]) -> bool:
        """该 key 上是否有正在跑的 turn。T1 空闲重置的「执行中绝不清」判据。"""
        return key in self._slots
