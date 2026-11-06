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
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Literal

from nonebot import logger

from .log_throttle import throttled_info
from .message_buffer import BufferedMessage
from .message_origin import MessageOrigin, MessageSource

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
    # 拆批 fragments 序列(alive/103 §14.2;alive/104 起恒为列表):本条目覆盖的
    # 有序真实碎片(各自插件 m:id/发送者/媒体),接力时呈递拆回真实消息序列。
    # 恒为 list 且长度 ≥1:缺序列=只有这一条消息,构造时以 msg 的独立呈递副本
    # 播种,不再有 None 语义,也没有「一侧缺序列整批回退首条身份」的分支。
    # 序列是呈递副本,不随入队剥皮;队列条目 msg 剥皮是 steer 门保护,
    # 碎片原文与媒体清单留给拆批渲染按号召回。任何碎片都不得与聚合 msg
    # 共用可变对象,否则合并时会把 MIXED 身份与拼接正文回写进首条碎片。
    fragments: list[BufferedMessage] = field(default_factory=list)

    def __post_init__(self) -> None:
        # 播种:调用方没给序列(含沿用旧 None 形参)时本条目就是单条消息。
        # 复制一层列表,并把任何与聚合 msg 同对象的行改成独立副本:后续
        # _merge_pending_message 会原地更新 msg 的批级来源/正文,绝不能污染成分。
        fragments = [replace(row) if row is self.msg else row for row in (self.fragments or ())]
        self.fragments = fragments or [replace(self.msg)]


def _merge_fragments(
    old_frags: list[BufferedMessage],
    new_frags: list[BufferedMessage],
) -> list[BufferedMessage]:
    """按序追加两侧碎片序列。

    两侧序列恒非空(构造时播种),因此没有「缺序列回退 None」分支;
    也不做去重或同人相邻折叠——逐段时序与来路原样保留。
    """
    return [*old_frags, *new_frags]


def _merge_recovered_batch(
    slot_fragments: Sequence[BufferedMessage],
    entry_fragments: Sequence[BufferedMessage],
    known_m_ids: Sequence[str | None],
) -> tuple[list[BufferedMessage], list[str], list[BufferedMessage]]:
    """把 native 同批归还行按 pending 原文顺序并进槽内 fragments。

    同批行只有一部分还留在槽里时(前面的行已被 ACK 结算摘走、尾行仍在途),
    「槽内先、缺行后补」的追加会把 payload=A/B 排成 B/A(第 4 轮评审 R5)。
    pending 原文就是这批行真实的相对顺序,所以这里以 payload 顺序为准:槽里已有
    的行沿用**槽内原 fragment 对象**(保住真实 m:id/媒体/身份),缺的行用回收副本
    补在批内相邻位置;不属于这批的排队行不吞、不变身份,也尽量不动位置——
    槽内已有的批内行本来就按原文顺序排列时,只做「插入缺行」,其余行原地不动;
    只有当槽内批内行彼此已经乱序时,才把这批行收拢到第一行原位置重建顺序。
    只按 m_id 认行,不看正文——不同原始行的相同正文各自保留。

    返回 `(ordered, absorbed_ids, kept)`:
      ordered      —— 合并后的槽内 fragments;
      absorbed_ids —— 槽里已有、因此没有追加回收副本的行号(payload 序);
      kept         —— 回收 entry 里真正新增的 fragment(批级正文载体只用这些)。
    完全没有可认原行(无台账身份)时返回槽原序列 + 空 absorbed,调用方走追加。
    """
    batch_ids = [None if m_id is None else str(m_id) for m_id in known_m_ids]
    wanted = {m_id for m_id in batch_ids if m_id is not None}
    if not wanted:
        return list(slot_fragments), [], list(entry_fragments)
    slot_at: dict[str, int] = {}
    for index, row in enumerate(slot_fragments):
        if row.id is None:
            continue
        m_id = str(row.id)
        if m_id in wanted and m_id not in slot_at:
            slot_at[m_id] = index
    if not slot_at:
        return list(slot_fragments), [], list(entry_fragments)

    # 按 payload 顺序过一遍:命中槽内原行 → 记槽位;缺行 → 记回收副本。
    # 同一原始行在原文里出现两次时仍只呈递槽里那一份。
    absorbed_ids: list[str] = []
    kept: list[BufferedMessage] = []
    slot_pos_row: list[int] = []          # >=0 = 该行在槽内的位置;-1 = 缺行
    frag_row: list[BufferedMessage | None] = []
    seen: set[str] = set()
    for m_id, fragment in zip(batch_ids, entry_fragments):
        if m_id is not None and m_id in slot_at:
            absorbed_ids.append(m_id)
            if m_id in seen:
                continue
            seen.add(m_id)
            slot_pos_row.append(slot_at[m_id])
            frag_row.append(None)
        else:
            kept.append(fragment)
            slot_pos_row.append(-1)
            frag_row.append(fragment)
    if not absorbed_ids:
        return list(slot_fragments), [], list(entry_fragments)

    present_positions = [pos for pos in slot_pos_row if pos >= 0]
    if all(
        left <= right for left, right in zip(present_positions, present_positions[1:])
    ):
        # 槽内批内行本就有序:只把缺行插到批内相邻位置,其余行原地不动。
        ordered = list(slot_fragments)
        total = len(slot_pos_row)
        inserts: list[tuple[int, list[BufferedMessage]]] = []
        cursor = 0
        while cursor < total:
            if slot_pos_row[cursor] >= 0:
                cursor += 1
                continue
            run: list[BufferedMessage] = []
            end = cursor
            while end < total and slot_pos_row[end] < 0:
                missing = frag_row[end]
                if missing is not None:
                    run.append(missing)
                end += 1
            next_present = next(
                (slot_pos_row[k] for k in range(end, total) if slot_pos_row[k] >= 0),
                None,
            )
            prev_present = next(
                (slot_pos_row[k] for k in range(cursor - 1, -1, -1) if slot_pos_row[k] >= 0),
                None,
            )
            if next_present is not None:
                at = next_present
            elif prev_present is not None:
                at = prev_present + 1
            else:  # pragma: no cover - absorbed_ids 非空时必有相邻原行
                at = len(ordered)
            inserts.append((at, run))
            cursor = end
        for at, run in sorted(inserts, key=lambda item: item[0], reverse=True):
            ordered[at:at] = run
        return ordered, absorbed_ids, kept

    # 槽内批内行彼此乱序(理论上只在原文与槽序冲突时出现):以第一行原位置为锚,
    # 整批按原文顺序重建,批外行仍按原相对顺序留在锚前/锚后。
    anchor = min(present_positions)
    block: list[BufferedMessage] = []
    for pos, fragment in zip(slot_pos_row, frag_row):
        block.append(slot_fragments[pos] if pos >= 0 else fragment)  # type: ignore[arg-type]
    ordered = [
        *slot_fragments[:anchor],
        *block,
        *(
            row
            for index, row in enumerate(slot_fragments)
            if index > anchor and index not in set(present_positions)
        ),
    ]
    return ordered, absorbed_ids, kept


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


def _different_delivery_generation(a: BufferedMessage, b: BufferedMessage) -> bool:
    """两侧都有入场章且不同才分代；None 旧调用维持原合并兼容。"""
    return (
        a.delivery_generation is not None
        and b.delivery_generation is not None
        and a.delivery_generation != b.delivery_generation
    )


def _merge_pending_message(old: BufferedMessage, new: BufferedMessage) -> None:
    """排队消息并入旧条目:只更新批级事实,不再表达身份。

    正文是非呈递用途的文本载体(steer 注入与日志):同一来源的碎片仍按序拼接,
    保证跨条文本不丢;一旦这次合并产生混合来源,正文保留首条原文、不再拼接——
    混合批的逐段身份由 fragments 逐行呈递,正文不拼昵称、不加分段标签
    之类手写分界(它们是昵称轨与字段轨的混血,表达不了三段以上混合)。

    图片 URL 追加去重;ts 取最新(接力时序以最后一条为准);original_msg_id 由
    调用方保留首个非空(与 msg.id 首条同源,alive/103)。reply_to_ts 目前全空,
    仍取最新值,避免未来启用后旧 None 盖掉新值。

    表情包不并入(队列不承载表情包):new 带的 sticker 只折算成一个 [表情包]
    文本占位拼进内容,URL 不进场——槽内条目永无 sticker_urls,steer 媒体检查
    不会被毒化。

    归属不被后到者覆盖——旧实现无条件 old.nickname = new.nickname,跨发送者
    合并(followup 槽)会渲染出「昵称是B id是A」的错配发言者。现在 nickname 只在
    同发送者合并时刷新,跨发送者的逐段归属由 fragments 各自承担,批级留首条。
    """
    same_user = new.user_id == old.user_id
    new_text = (new.content or "").strip()
    if new.sticker_urls:
        new_text = (new_text + " [表情包]").strip()
    merged_origin = MessageOrigin.merge(old.origin, new.origin)
    if new_text and merged_origin.source is not MessageSource.MIXED:
        old_text = (old.content or "").strip()
        # msg.content 仍供 steer 直注使用;同源跨人拼接必须保留后到者归属,
        # 否则整段会以首条 speaker_tag 注入。模型当前行则由 fragments 呈递,
        # 不读取这个业务载体,因此不会再出现双 speaker 外壳。
        speaker_prefix = "" if same_user else f"{new.nickname or new.user_id}: "
        incoming_text = f"{speaker_prefix}{new_text}"
        old.content = f"{old_text}\n{incoming_text}" if old_text else incoming_text
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
    # 来源只在值对象中合并;混合/系统聚合体不再借用首条消息的寻址身份(无号律)。
    old.origin = merged_origin
    if old.origin.id_required_none:
        old.id = None
        old.qq_message_id = None


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
        if _different_delivery_generation(slot.followup.msg, entry.msg):
            # clear 两侧不混批；新代整槽替换，迟到旧代不得反向覆盖。
            if (entry.msg.delivery_generation or 0) > (slot.followup.msg.delivery_generation or 0):
                slot.followup = entry
            return
        merged_fragments = _merge_fragments(slot.followup.fragments, entry.fragments)
        _merge_pending_message(slot.followup.msg, entry.msg)
        slot.followup.fragments = merged_fragments

    def add_recovered_followup(
        self,
        key: tuple[str, str],
        entry: PendingEntry,
        *,
        known_m_ids: Sequence[str | None] | None,
    ) -> tuple[str, ...]:
        """native 归还已知原始行 × 仍在槽的原行:同一原行只留一份呈递。

        `pending_steer` 上浮 = 这批 steer 被上游吞掉、没送到模型,内容仍欠模型
        一次呈递。回收行本身是无号 `human_replay` 副本(见
        `_pending_steer_recovery_fragments`):如果原行还在 followup 槽里没被 pop,
        它下一轮本来就会呈递——再追加一份回收副本就是同一原行两次(第 3 轮评审
        R4)。这里按 `m_id` 认原行(不看正文:不同原始行的相同正文各自保留):槽里
        已在的行不再追加回收副本(槽里的原行带真实 m:id/媒体,身份更完整),不在槽
        里的回收行照旧追加,真正被吞的内容不丢。

        `known_m_ids` 与 `entry.fragments` 等长,None=无台账身份(原文 fallback,
        无法认行,按旧行为追加)。返回被槽内原行吸收的行号,供日志说明。

        同批行的**相对顺序**按 `pending` 原文(见 `_merge_recovered_batch`):部分
        行已被 ACK 摘走、尾行仍在槽时,缺行补回原位置而不是追加到槽尾,否则下一轮
        会把 A/B 呈递成 B/A(第 4 轮评审 R5)。

        晚到 HTTP200 不得再把这行从槽里摘走:结算侧(`_try_steer_followup`)按
        台账状态区分「已接受并已呈现」与「已被 native 归还、仍欠一次呈递」。
        """
        absorbed: tuple[str, ...] = ()
        slot = self._slots.get(key)
        if (
            slot is not None
            and slot.followup is not None
            and known_m_ids is not None
            and len(known_m_ids) == len(entry.fragments)
            # 换代不混批:新代整槽替换由 add_followup 处理,这里吸收会连旧代
            # followup 一起丢——只在同代会并的槽上做去重。
            and not _different_delivery_generation(slot.followup.msg, entry.msg)
        ):
            ordered, absorbed_ids, kept = _merge_recovered_batch(
                slot.followup.fragments, entry.fragments, known_m_ids,
            )
            if absorbed_ids:
                absorbed = tuple(absorbed_ids)
                if not kept:
                    # 整批行都还在槽:只按 pending 原文校正相对顺序,不追加副本。
                    if list(ordered) != list(slot.followup.fragments):
                        slot.followup.fragments = ordered
                        logger.info(
                            f"[HERMES steer回收] 原行仍在接力槽,按 pending 原文"
                            f"校正同批顺序 (key={key} absorbed={list(absorbed)})"
                        )
                    else:
                        logger.info(
                            f"[HERMES steer回收] 原行仍在接力槽,不追加回收副本 "
                            f"(key={key} absorbed={list(absorbed)})"
                        )
                    return absorbed
                # 只把真正新增的行并进批级正文载体,避免重复文本进 slot.msg;
                # fragments 用按原文顺序排好的合并序列(缺行补在原位置)。
                text = "\n".join(f.content for f in kept if (f.content or "").strip())
                new_msg = replace(entry.msg, content=text or entry.msg.content)
                if new_msg.sticker_urls:
                    new_msg = _strip_stickers_for_queue(new_msg)
                _merge_pending_message(slot.followup.msg, new_msg)
                slot.followup.fragments = ordered
                return absorbed
        self.add_followup(key, entry)
        return absorbed

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
        """无 slot → 占位 started_at=now_ms,返回 'entered';否则按规则入队。

        fragments 缺省(None)由 PendingEntry 播种成 [本条];调用方仍可沿用
        None 形参,但队列条目恒持有非空列表。
        """
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
            fragments=list(fragments) if fragments else [],
        )
        if not commission:
            # 旁观闲聊也不 latest-wins 了(「正常任务会被打趣驱逐」):
            # 已在队则把内容拼进同一跟进单,触发归属保留首条,文本/媒体不丢;
            # 它只承诺补一次跟进判断,真正的取舍由那次 submit_decision 做。
            if slot.followup is not None:
                if _different_delivery_generation(slot.followup.msg, current_msg):
                    # 新代只替换旧 followup；迟到旧代不覆盖新代，也不混入 fragments。
                    if (current_msg.delivery_generation or 0) > (slot.followup.msg.delivery_generation or 0):
                        slot.followup = entry
                    return "queued"
                merged_fragments = _merge_fragments(slot.followup.fragments, entry.fragments)
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
                slot.followup.fragments = merged_fragments
            else:
                slot.followup = entry
            return "queued"

        for queued in slot.commissions:
            if queued.msg.user_id == current_msg.user_id:
                if _different_delivery_generation(queued.msg, current_msg):
                    # 不同代各占一条，旧条稍后由原子门拒绝；新条不被旧文合并吞掉。
                    # 迟到的更旧委托直接丢弃，不能排到新代后面再次开轮。
                    if (current_msg.delivery_generation or 0) < (queued.msg.delivery_generation or 0):
                        return "queued"
                    continue
                merged_fragments = _merge_fragments(queued.fragments, entry.fragments)
                _merge_pending_message(queued.msg, current_msg)
                queued.is_direct = queued.is_direct or is_direct
                queued.addressed_to_bot = queued.addressed_to_bot or addressed_to_bot
                # 同上:ephemeral 取与,防「一条有窗片段被无窗语义跑掉」。
                queued.ephemeral = queued.ephemeral and ephemeral
                # 同 followup 槽：来源保留首条，与 msg.id 首条同源（alive/103）。
                if queued.original_msg_id is None:
                    queued.original_msg_id = original_msg_id
                queued.fragments = merged_fragments
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
