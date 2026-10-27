"""In-flight 调用追踪 + 排队/合并重燃支持。

修同一 (adapter, group_id|user_id) 上事件 task 并发调 chat() 的 bug:
in-flight 时新消息不占并发,只排队;当前一发完成后按顺序接力。

线程安全:**否**。预设单线程 asyncio 事件循环,与 ActiveSessionManager 一致。

队列化:pending 从「单槽 latest-wins」改成
「委托队列 + 闲聊跟进槽」。委托=私聊消息或群聊 addressed_to_bot 的 @;
不同委托人的 @ 各自排队(总上限 2),同一委托人重复 @ 只留一份并拼接内容;
非委托的旁观消息仍 latest-wins,只留一个跟进触发,绝不顶掉委托。
队列满只丢新委托并 WARN,不用闲聊填满委托槽。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace
from typing import Literal

from nonebot import logger

from .log_throttle import throttled_info
from .message_buffer import BufferedMessage

# Refire 链最大深度。超过则清空队列并释放 slot,等下一个新触发。
# 一次 burst 最多产出 1(主回) + MAX_REFIRE_DEPTH(链尾)= 4 发回复。
MAX_REFIRE_DEPTH = 3

# 委托排队上限(同日由 3 调 2):同一 target 最多 2 条委托;
# 同一委托人同时在队只留 1 条(后来的拼进这条)。超出上限的新委托直接丢,
# 防止「队列积压 = 一条消息补跑一次 LLM turn」。
# 容量数学(一波爆发 = 1 主 turn + MAX_REFIRE_DEPTH 3 层接力 = 4 发):
# 2 委托 + 1 闲聊 followup 正好消化完,深度门不浪费也不冲掉委托。
# 本值与 MAX_REFIRE_DEPTH 可调,缺省取低只为保证换任何模型都不出问题。
MAX_PENDING_COMMISSIONS = 2
MAX_PENDING_PER_USER = 1


def make_inflight_key(
    adapter: str, is_private: bool, user_id: str, group_id: str | None
) -> tuple[str, str]:
    """inflight/scope 键的单一构造函数(tuple 形)。

    键形(同源化收口批,审计确认6;此前 9+ 处逐字手拼):
      - 群: (adapter, "group:<group_id>")
      - 私聊: (adapter, "private:<user_id>")
    group_id 缺省(None)回落空串;私聊恒用 user_id(group_id 不参与)。
    消费方:InflightRegistry / 防抖键 / 反馈便签 scope / T1 空闲重置 /
    delivery_poll / run_resume / proactive 点火前复查。字符串孪生形见
    core/run_tracker.make_scope_key(冒号串,另行保留)。
    """
    scope = "private" if is_private else "group"
    scope_id = user_id if is_private else (group_id or "")
    return (adapter, f"{scope}:{scope_id}")


@dataclass
class PendingEntry:
    """In-flight 期间排队的消息及触发元数据。

    pending 不裸存 BufferedMessage——单独抽 PendingEntry 是为了跨 turn 边界
    保留几个 plumbing 决策所需要的事实:
      - 这条排队消息原本是不是直说(direct)
      - 它是否被平台确认为「冲 bot 说的」(寻址与直说是两个事实,不互相派生:
        synth 事件可以 direct 而不寻址,每条都 direct 也不寻址)
      - 它的原始 adapter 侧 message_id(用于失败时贴 emoji notice)
      - 它自己的 ephemeral 无窗判定(接力链按条目读,不再透传发包 turn 的值)
    """

    msg: BufferedMessage
    is_direct: bool
    original_msg_id: str | int | None = None
    # 默认 False 是安全方向:漏传只会让模型自己判归属,不会错误断言「这条冲你说的」。
    addressed_to_bot: bool = False
    # 条目自带无窗判定——冷窗关键词 turn
    # 运行中排队的 addressed 委托此前会被以 ephemeral=True 接力跑掉(不读钉锚 +
    # 二次 trigger 窗口)。默认 False 同样是安全方向:漏传按「有窗」跑,
    # 至多被 _run_reactive_turn 入口的 get_if_trigger_alive 挡下,不会错误开新窗。
    ephemeral: bool = False
    # 迟到 steer 回收标记:runs 轮收尾时随 completed 上浮的
    # pending_steer 排回 pending 的条目。豁免理由=系统已收过它一次(steer_run 接受),
    # 不是「bot 刚说完别人接话」类过触发,refire 的 post-reply cooldown 闸门放行。
    # 不借 is_direct(该字段已钉死为寻址事实,借它会继承破信封 raw 转发、
    # transport 兜底文本等一串负资产)。合并处与 direct 同规取 or,防并入后丢失。
    steer_recovered: bool = False
    # 拆批 fragments 序列(alive/103 §14.2):本条目覆盖的有序真实碎片
    # (各自插件 m:id/发送者/媒体),接力时呈递拆回真实消息序列。
    # None=无拆批信息(旧路径单行);合并时任一侧缺序列整体回退 None,
    # 不拿半份序列冒充完整。序列是呈递副本,不随入队剥皮(队列条目 msg
    # 剥皮是 steer 门保护,碎片原文与媒体清单留给拆批渲染按号召回)。
    fragments: list[BufferedMessage] | None = None


def _merge_fragments(
    old_frags: list[BufferedMessage] | None,
    new_frags: list[BufferedMessage] | None,
) -> list[BufferedMessage] | None:
    """两侧都有完整序列才按序追加;任一侧 None=信息不完整,回退 None。"""
    if old_frags is None or new_frags is None:
        return None
    return list(old_frags) + list(new_frags)


@dataclass
class InflightSlot:
    started_at: int
    # 非委托旁观触发:只留最新一条,turn 结束后补一次跟进即可。
    followup: PendingEntry | None = None
    # 委托触发:按到达顺序 FIFO;同一委托人重复触发合并进已排队条目。
    commissions: deque[PendingEntry] = field(default_factory=deque)


def _strip_stickers_for_queue(msg: BufferedMessage) -> BufferedMessage:
    """入队前剥掉表情包:sticker_urls 置空,文本补一个 [表情包] 占位。

    队列不承载表情包:表情包的生命周期止步于 perception
    层——到达时已入库留字节、buffer 留占位,模型可经 get_message_images 召回;
    排队回放只需要知道「有表情包」,不需要字节。此前在 _refire 接力时才降级,
    时机太晚:条目在槽期间 sticker_urls 会毒化 steer 的媒体检查
    (_try_steer_followup),让后续纯文本跟帖连坐失去实时注入。前移到入队时点
    后,槽内条目永无 sticker,steer 门不再被堵。
    """
    if not msg.sticker_urls:
        return msg
    return replace(msg, sticker_urls=[], content=(msg.content + " [表情包]").strip())


# 合成后续轮（离场确认/接力/重交）在私聊沿用真实 user_id 时的身份昵称。
# 本常量只承担显示（speaker 标签/补投昵称），不再承担身份识别——合成 fragment
# 的身份分界由 BufferedMessage.is_synthetic 单一事实承担，手抄字面量会漂移、
# 且撞真人同名昵称会误判（alive/86 普查收口项）。
_SYNTHETIC_NICKNAME = "系统核对"


def _merge_pending_message(old: BufferedMessage, new: BufferedMessage) -> None:
    """排队消息并入旧条目(语义同防抖拼接)。

    文本换行拼接;图片 URL 追加去重;ts 取最新(接力时序以最后一条为准);
    original_msg_id 由调用方保留首个非空(与 msg.id 首条同源,alive/103)。
    reply_to_ts 目前全空,仍取最新值,避免未来启用后旧 None 盖掉新值。

    表情包不并入(队列不承载表情包):new 带的 sticker
    只折算成一个 [表情包] 文本占位拼进内容,URL 不进场——槽内条目永无
    sticker_urls,steer 媒体检查不会被毒化。

    归属不再被后到者覆盖——旧实现无条件
    old.nickname = new.nickname,跨发送者合并(followup 槽)会渲染出「昵称是B
    id是A」的错配发言者。现在 nickname 只在同发送者合并时刷新;跨发送者拼接的
    文本带「昵称: 」前缀,归属信息不丢也不错配。
    """
    same_user = new.user_id == old.user_id
    new_text = (new.content or "").strip()
    if new.sticker_urls:
        new_text = (new_text + " [表情包]").strip()
    if new_text:
        if not same_user:
            new_text = f"{new.nickname or new.user_id}: {new_text}"
        elif old.is_synthetic and not new.is_synthetic:
            # 合成后续轮
            # （私聊顶真实 user_id、昵称「系统核对」）与真人消息按同委托人规则合并时，
            # 真人部分必须带身份分界——合成 prompt 自带「(系统核对:」前缀，真人部分
            # 不标会被读成系统指令的延续。反向（真人旧条目+合成新 fragment）不用标：
            # 合成 prompt 文本自带头。
            new_text = f"(用户留言: {new_text})"
        old_text = (old.content or "").strip()
        old.content = f"{old_text}\n{new_text}" if old_text else new_text
    # 只合并普通图片:委托带图接力后仍要看图是真实需求,sticker 见上方说明。
    for field_name in ("image_urls",):
        incoming = list(getattr(new, field_name) or [])
        if incoming:
            merged = list(getattr(old, field_name) or [])
            merged.extend(u for u in incoming if u not in merged)
            setattr(old, field_name, merged)
    old.ts = new.ts
    if same_user:
        old.nickname = new.nickname
    old.reply_to_ts = new.reply_to_ts
    # 合成身份 OR 传播：任一片段是合成，合并条目就保持合成——后续真人片段并入时
    # 仍要带身份分界，不因先并入一条真人文本就把合成底丢成「真人条目」。
    old.is_synthetic = old.is_synthetic or new.is_synthetic


class InflightRegistry:
    """per-target 非阻塞 busy 标记 + 排队单元。

    Key 约定(构造统一走 make_inflight_key,勿手拼):
      - 群: (adapter, "group:<group_id>")
      - 私聊: (adapter, "private:<user_id>")

    入队规则:
      - 空 slot → 占位,返回 'entered'
      - commission=True(委托):同一 user_id 已在队 → 拼接内容,返回 'queued';
        新委托人且队列未满 → 追加,返回 'queued';队列满 → 不丢旧委托,丢本条,
        返回 'queue_full'
      - commission=False(旁观/闲聊):只刷新 followup 槽(latest wins),返回 'queued'
      - 表情包不入队:sticker_urls 入队即剥,
        折算成 [表情包] 文本占位;字节在 perception 层已入库,召回不受影响

    不持有任何 asyncio.Task 引用 —— 重燃由 caller 用 create_task 自己接手,
    registry 只负责「现在有没有人在跑」+「跑完后下一发是谁」两个状态。
    """

    # --- 阶段2b:followup 槽操作(runs steer 注入与 pending_steer 回收用) ---
    def __init__(self) -> None:
        self._slots: dict[tuple[str, str], InflightSlot] = {}

    def peek_followup(self, key: tuple[str, str]) -> PendingEntry | None:
        """非破坏性读 followup 槽条目(无则 None)。

        steer 注入改「先看后确认」——旧 take/restore
        组合在 await steer_run(最长 15s)期间条目已离槽,turn 若此时收尾,槽被
        exit 删除、restore 静默吞条目(消息既未注入也不 refire)。peek 模式下条目
        全程在槽:期间 turn 收尾会由 pop_next_pending 正常捡走接力,零丢失路径。
        """
        slot = self._slots.get(key)
        return slot.followup if slot is not None else None

    def settle_followup(self, key: tuple[str, str], entry: PendingEntry) -> None:
        """steer 注入成功后结算:仅当槽里还是同一个条目(identity)才摘除。

        调用方(_try_steer_followup)还会先核对内容快照:await 期间并入过新文本
        的条目不 settle,留槽走 pending 接力——宁可注入前缀重复一次,不丢并入内容。
        若条目已被 pop_next_pending 取走接力,则 no-op。绝不误删后来居上的新条目。
        """
        slot = self._slots.get(key)
        if slot is not None and slot.followup is entry:
            slot.followup = None

    def add_followup(self, key: tuple[str, str], entry: PendingEntry) -> None:
        """写入 followup 槽;已有条目时把新文本并入(latest-wins 语义下不整槽替换,
        防 pending_steer 回收把 turn 期间真实跟帖顶掉)。入槽即剥表情包,
        与 try_enter 排队路径同规。"""
        if entry.msg.sticker_urls:
            entry = replace(entry, msg=_strip_stickers_for_queue(entry.msg))
        slot = self._slots.get(key)
        if slot is None:
            self._slots[key] = InflightSlot(started_at=entry.msg.ts, followup=entry)
            return
        if slot.followup is None:
            slot.followup = entry
            return
        _merge_pending_message(slot.followup.msg, entry.msg)
        slot.followup.fragments = _merge_fragments(slot.followup.fragments, entry.fragments)
        # steer_recovered 与 explicit 同规取 or:迟到 steer
        # 回收文本并入已有条目时豁免标记不丢,否则主修复场景之一仍被 cooldown 吞。
        slot.followup.steer_recovered = slot.followup.steer_recovered or entry.steer_recovered


    def try_enter(
        self,
        key: tuple[str, str],
        current_msg: BufferedMessage,
        *,
        is_direct: bool,
        original_msg_id: str | int | None,
        now_ms: int,
        addressed_to_bot: bool = False,
        ephemeral: bool = False,
        commission: bool = False,
        fragments: list[BufferedMessage] | None = None,
    ) -> Literal["entered", "queued", "queue_full"]:
        """无 slot → 占位 started_at=now_ms,返回 'entered';否则按规则入队。"""
        slot = self._slots.get(key)
        if slot is None:
            self._slots[key] = InflightSlot(started_at=now_ms)
            return "entered"

        # 队列不承载表情包:入队即剥,占位文本保留语义。
        # 注意只影响排队路径——entered 分支不落 current_msg,当轮 turn 的
        # sticker vision 走 dispatch 直传的函数参数,不受本剥离影响。
        current_msg = _strip_stickers_for_queue(current_msg)
        entry = PendingEntry(
            msg=current_msg,
            is_direct=is_direct,
            original_msg_id=original_msg_id,
            addressed_to_bot=addressed_to_bot,
            ephemeral=ephemeral,
            fragments=fragments,
        )
        if not commission:
            # 旁观闲聊也不 latest-wins 了(「正常任务会被打趣驱逐」):
            # 已在队则把内容拼进同一跟进单,触发归属保留首条,文本/媒体不丢;
            # 它只承诺补一次跟进判断,真正的取舍由那次 submit_decision 做。
            if slot.followup is not None:
                _merge_pending_message(slot.followup.msg, current_msg)
                slot.followup.is_direct = slot.followup.is_direct or is_direct
                slot.followup.addressed_to_bot = slot.followup.addressed_to_bot or addressed_to_bot
                # ephemeral 与防抖合并且同规(两者皆无窗才无窗):任一片段来自
                # 有窗语境,合并条目就按有窗跑——错开新窗比少跑一发更贵。
                slot.followup.ephemeral = slot.followup.ephemeral and ephemeral
                # 来源保留首条（alive/103）：与 BufferedMessage.id 首条同源，
                # 钉锚不再出现「插件 ID 取首条、平台 ID 取末条」的错位。
                if slot.followup.original_msg_id is None:
                    slot.followup.original_msg_id = original_msg_id
                slot.followup.fragments = _merge_fragments(slot.followup.fragments, fragments)
            else:
                slot.followup = entry
            return "queued"

        for queued in slot.commissions:
            if queued.msg.user_id == current_msg.user_id:
                _merge_pending_message(queued.msg, current_msg)
                queued.is_direct = queued.is_direct or is_direct
                queued.addressed_to_bot = queued.addressed_to_bot or addressed_to_bot
                # 同上:ephemeral 取与,防「一条有窗片段被无窗语义跑掉」。
                queued.ephemeral = queued.ephemeral and ephemeral
                # 同 followup 槽：来源保留首条，与 msg.id 首条同源（alive/103）。
                if queued.original_msg_id is None:
                    queued.original_msg_id = original_msg_id
                queued.fragments = _merge_fragments(queued.fragments, fragments)
                return "queued"
        if len(slot.commissions) >= MAX_PENDING_COMMISSIONS:
            # queue_full 静默丢弃是设计定案(勿「修」成贴表情/报错):
            # 反馈表情用户看不明白、还可能被刺激反复 @ 刷屏;委托被丢用户自己会察觉
            # 再找 bot。代价不对称,静默是对的。
            # 补同因节流观测:丢弃方此前无任何对账/观测通道,排障时无法定位。
            throttled_info(
                f"gate.queue_full:{key[0]}:{key[1]}",
                f"[HERMES inflight] 委托队列已满丢弃新委托 key={key}(已在队 {len(slot.commissions)} 条)",
            )
            return "queue_full"
        slot.commissions.append(entry)
        return "queued"

    def peek_next_pending(self, key: tuple[str, str], *, after_ts: int) -> PendingEntry | None:
        """非破坏读「下一发是谁」——与 pop_next_pending 同判据,但不摘除。

        用:turn 收尾要先知道槽里还有没有
        下一发,才能决定要不要等聚合窗;而 pop 会把条目取走,判据只能是非破坏读。
        stale 前缀(ts <= after_ts)按 pop 同规跳过、不返回。返回非 None 只表示
        「有货」;真正的取队仍必须走 pop_next_pending。
        """
        slot = self._slots.get(key)
        if slot is None:
            return None
        for entry in slot.commissions:
            if entry.msg.ts > after_ts:
                return entry
        if slot.followup is not None and slot.followup.msg.ts > after_ts:
            return slot.followup
        return None

    def pop_next_pending(self, key: tuple[str, str], *, after_ts: int) -> PendingEntry | None:
        """取下一发接力。委托 FIFO 优先;没有委托才轮到闲聊跟进槽。

        after_ts 之前的条目视为「当前 turn 已覆盖」直接丢弃,防止旧时间戳重复点火。
        返回 None 不代表必须 exit——调用方确认没有下一发后才 exit。
        """
        slot = self._slots.get(key)
        if slot is None:
            return None
        # 陈旧条目丢弃补日志——丢弃本身是设计行为
        # (防旧时间戳重复点火),但静默=观测盲区,无法区分「没有跟帖」与「跟帖被覆盖」。
        dropped_comm = 0
        while slot.commissions and slot.commissions[0].msg.ts <= after_ts:
            slot.commissions.popleft()
            dropped_comm += 1
        if dropped_comm:
            logger.info(f"[HERMES inflight] 丢弃 {dropped_comm} 条陈旧委托条目 key={key}(已被当前 turn 覆盖)")
        if slot.commissions:
            return slot.commissions.popleft()
        if slot.followup is not None:
            entry = slot.followup
            slot.followup = None
            if entry.msg.ts > after_ts:
                return entry
            logger.info(f"[HERMES inflight] 丢弃 1 条陈旧跟帖 key={key}(已被当前 turn 覆盖)")
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
