"""群活跃态(reactive 模式)的内存状态机。

每 (adapter, group_id) 至多一条活跃记录;TTL 滑动续期。
重启即清。
"""

from __future__ import annotations

import builtins
from dataclasses import dataclass

# turn 收尾时给窗口垫的最小剩余量。只需盖住「turn 结束 → pending 接力 refire」这次
# 交接(一次事件循环跳转,期间 cron sweep 可能扫过),不是再送一个 TTL。
_TURN_END_GRACE_MS = 10_000


@dataclass
class AnchorRecord:
    """委托锚(本部署补丁:凌晨批项4):一条被钉住的 addressed
    委托消息,随窗口存活,渲染进 <pinned_tasks> 块供续发轮定位任务来源。"""

    commissioner_id: str
    commissioner_nickname: str | None
    msg_id: str | int | None
    text: str  # 钉入时已截断(渲染端还会再过一道 sanitize)
    ts: int  # ms
    answered: bool = False
    """bot 在锚钉入后是否已发言过(mark_bot_replied 置位)。客人槽顶锚规则的
    「旧锚任务已应答」判据——旧委托已拿到答案,锚剩余价值低,可被顶。"""


@dataclass
class ActiveSession:
    adapter: str
    group_id: str
    triggered_by: str
    started_at: int  # ms
    last_active_at: int  # ms
    expires_at: int  # ms
    topic_hint: str | None = None
    last_bot_reply_at: int = 0
    """bot 最近一次在本群发出 reactive 回复的 ms 时间戳。0 = 本窗口期未回复过。
    用作 handlers 的 post-reply cooldown 判定。再次 trigger() 时清零,避免跨窗口
    残留状态把新一轮对话的第一条非显式消息直接 skip 掉。"""

    last_bot_reply_media: int = 0
    """上一次回复实际投出去的媒体数。同 turn 去重闸门据此区分「重复答案」与
    「文本已答但图还没出去」:push_message 拿主机本地路径当图时只发出文本,
    此时若把带图的 submit_decision 整条抑制,那张图就彻底丢了。"""

    last_bot_reply_text: str = ""
    """上一次回复(含 MCP push)的文本原文。同 turn 去重据此判断 submit_decision 是不是
    在**复述**刚发出去的话 —— 只有近乎逐字相同才抑制。空串 = 不知道原文,一律放行。"""

    last_strong_activation_at: int = 0
    """最后一次实质激活的 ms 时间戳:显式触发(trigger)或 bot 发言(mark_bot_replied)。
    T5 判据:窗口仅靠隐式 touch 续期时,now - 本字段 > T5 即在读路径视为不活跃。
    0 = 本窗口期无实质激活(不可能——trigger 建窗时即赋值,防御用)。"""

    # 本部署补丁(凌晨批项4): 委托锚双槽。维护者槽
    # (commissioner ∈ hermes_admin_users)只被维护者自己的新委托顶;客人槽全客人
    # 共享,顶锚需满足三条件之一(旧锚已应答/超龄/已被 release,见 pin_anchor)。
    # 窗口死(TTL/T5/should_exit_active)= session 实例销毁,锚随窗死;
    # trigger() 换实例时两槽随记录carry-over(answered 标记骑在记录上,不丢)。
    anchor_master: AnchorRecord | None = None
    anchor_guest: AnchorRecord | None = None

    # 本部署补丁: 租约计数从实例级移到 key 级(管理器
    # _leases 字典),实例上不再有 inflight_turns 字段。旧设计里 trigger() 换新实例
    # 会把计数清零,in-flight turn 的保护落不到新窗口上(排查记录见
    # 项目文档库 2026-10-02 节)。

    def is_alive(self, now_ms: int) -> bool:
        """纯 TTL 判活:expires_at 未到期。

        租约不在实例上 —— 要判「窗口还活着」(TTL 或租约任一成立)请用
        ActiveSessionManager.is_alive(adapter, group_id, now_ms)。"""
        return self.expires_at > now_ms


class ActiveSessionManager:
    """滑动 TTL 状态机,跟踪哪些 (adapter, group_id) 处于 reactive 监听窗口。

    线程安全:**否**。预设单线程 asyncio 事件循环,不要从背景线程访问。
    租约(key 级,本部署补丁 2026-10-02):`begin_turn` / `end_turn` 之间该
    (adapter, group_id) 一律算活着。计数挂在管理器的 `_leases` 字典上而非
    session 实例上 —— trigger() 换新实例不丢租约,in-flight turn 的保护跟着群走。
    `is_active` / `get_if_active` / `touch` / `sweep_expired` 都尊重它。
    GC 策略:不在读路径自动剔除过期 session,由 Task 16 的 cron 调 sweep_expired()
    定期清理。读 API 中:`is_active` / `touch` / `get_if_active` 是 TTL 感知的;
    `get()` 是裸访问,**返回过期 session**——仅用于调试 / 日志,handler 应优先用
    is_active / get_if_active。
    """

    def __init__(self, default_ttl_sec: int = 300, t5_sec: int = 0) -> None:
        self._ttl_ms = default_ttl_sec * 1000
        self._t5_ms = t5_sec * 1000
        self._sessions: dict[tuple[str, str], ActiveSession] = {}
        # 本部署补丁: key 级 turn 租约计数。
        # >0 时该 key 下的窗口不判过期、不被 sweep;归零即删条目,不会漏涨。
        self._leases: dict[tuple[str, str], int] = {}
        # 本部署补丁(阶段2a): 窗口持久化镜像(可选绑定)。
        self._store = None

    # --- 阶段2a 持久化镜像 ---
    def bind_store(self, store, now_ms: int) -> int:
        """绑定 WindowStore 并装回未死窗口,返回装回条数。

        装回判据与读路径同口径:TTL 未过期 且 未超 T5;死窗不装回且顺手删行。
        租约不持久化(turn 不跨重启)。内存为主、库为影子——装回只发生在启动,
        之后一切读走内存。"""
        from .storage.window_store import session_from_payload

        self._store = store
        restored = 0
        for adapter, group_id, payload, _ in store.load_all():
            s = session_from_payload(adapter, group_id, payload)
            if not s.is_alive(now_ms) or self._beyond_t5(s, now_ms):
                store.delete(adapter, group_id)
                continue
            self._sessions[(adapter, group_id)] = s
            restored += 1
        return restored

    def _persist(self, s: ActiveSession) -> None:
        if self._store is None:
            return
        from .storage.window_store import session_to_payload

        self._store.upsert(s.adapter, s.group_id, session_to_payload(s), s.expires_at)

    def _drop(self, adapter: str, group_id: str) -> None:
        if self._store is not None:
            self._store.delete(adapter, group_id)

    def _beyond_t5(self, s: ActiveSession, now_ms: int) -> bool:
        """T5 封顶:窗口仅靠隐式续期吊着的时间超过 T5 即视为不活跃。

        判断放读路径(is_active / get_if_active)而非 sweep cron:窗口被隐式续期的
        前提是消息在流动,每条隐式激活都必经这两个读 API,判断点天然在此;没消息时
        窗口按 TTL 自然过期,T5 无需介入。超 T5 后不做物理删除,expires_at 不再被
        touch 推动,窗口随 TTL 自然死、由 sweep 清理。"""
        return (
            self._t5_ms > 0
            and s.last_strong_activation_at > 0
            and now_ms - s.last_strong_activation_at > self._t5_ms
        )

    def trigger(
        self,
        adapter: str,
        group_id: str,
        user_id: str,
        now_ms: int,
        topic_hint: str | None = None,
    ) -> ActiveSession:
        s = ActiveSession(
            adapter=adapter,
            group_id=group_id,
            triggered_by=user_id,
            started_at=now_ms,
            last_active_at=now_ms,
            expires_at=now_ms + self._ttl_ms,
            topic_hint=topic_hint,
            last_strong_activation_at=now_ms,
        )
        # 本部署补丁(凌晨批项4): trigger 换实例时委托锚
        # carry-over——否则客人的每条 @ 都借「换实例」顶掉维护者槽,双槽规则形同虚设。
        # answered 标记骑在 AnchorRecord 上,不依赖旧实例的 last_bot_reply_at
        # (后者按既有设计随换实例清零,供 cooldown 用,不动)。
        old = self._sessions.get((adapter, group_id))
        if old is not None:
            s.anchor_master = old.anchor_master
            s.anchor_guest = old.anchor_guest
        self._sessions[(adapter, group_id)] = s
        self._persist(s)
        return s

    def touch(self, adapter: str, group_id: str, now_ms: int) -> ActiveSession | None:
        s = self._sessions.get((adapter, group_id))
        if s is None or not self.is_alive(adapter, group_id, now_ms):
            return None
        if self._beyond_t5(s, now_ms):
            # T5 封顶:隐式续期到此为止,窗口停在当前 expires_at 自然死
            return None
        s.last_active_at = now_ms
        s.expires_at = now_ms + self._ttl_ms
        self._persist(s)
        return s

    def get(self, adapter: str, group_id: str) -> ActiveSession | None:
        """裸访问,**不检查 TTL**——可能返回已过期 session。

        多数 handler 应改用 `get_if_active` 或先 `is_active` 校验。本方法保留是为了
        调试 / 日志场景需要观测已过期但尚未被 sweep 的 session。
        """
        return self._sessions.get((adapter, group_id))

    def get_if_active(self, adapter: str, group_id: str, now_ms: int) -> ActiveSession | None:
        """TTL 感知的 get:只在 session 存在且未过期时返回。T5 封顶同样视为不活跃。"""
        s = self._sessions.get((adapter, group_id))
        if s is None or not self.is_alive(adapter, group_id, now_ms) or self._beyond_t5(s, now_ms):
            return None
        return s

    def is_active(self, adapter: str, group_id: str, now_ms: int) -> bool:
        s = self._sessions.get((adapter, group_id))
        return (
            s is not None
            and self.is_alive(adapter, group_id, now_ms)
            and not self._beyond_t5(s, now_ms)
        )

    def mark_bot_replied(
        self,
        adapter: str,
        group_id: str,
        now_ms: int,
        media_count: int = 0,
        text: str = "",
    ) -> bool:
        """记录 bot 在本群刚发出回复的时间戳、实际投出的媒体数与文本原文;session 缺失则 no-op。

        不滑动 expires_at(滑动续期由 touch 负责)。handlers 在 reactive 模式 send 成功后
        调用,push_message 也调,供 post-reply cooldown 与同 turn 去重判定。

        text 用于判断后续 submit_decision 是否在复述刚发出去的话;不传(空串)时同 turn
        去重会放行,宁可多发一条也不吞掉答案。

        返回是否真的写进去了。False = session 已经被 should_exit_active 收掉(`end()` 的
        唯一调用点),本群的 post-reply cooldown
        与同 turn 去重这一轮都失去依据 —— 调用方应当把它记出来,否则「冷却全程不生效」
        在日志里只表现为 last_bot_reply_at 永远是 0,查不到原因。
        """
        s = self._sessions.get((adapter, group_id))
        if s is None:
            return False
        s.last_bot_reply_at = now_ms
        s.last_bot_reply_media = media_count
        s.last_bot_reply_text = text
        s.last_strong_activation_at = now_ms  # bot 发言 = 实质激活,T5 重新起算
        # 本部署补丁(凌晨批项4): bot 发言后,所有早于本次
        # 发言的委托锚标记「已应答」(客人槽顶锚规则的条件 a)。应答 ≠ 松锚——
        # 应答后线程阶段的追问仍需要锚,锚继续钉到窗口死/超龄/模型 release。
        for rec in (s.anchor_master, s.anchor_guest):
            if rec is not None and rec.ts <= now_ms:
                rec.answered = True
        self._persist(s)
        return True

    def pin_anchor(
        self,
        session: ActiveSession,
        record: AnchorRecord,
        *,
        master_ids: set[str],
        max_age_ms: int,
        now_ms: int,
    ) -> bool:
        """钉委托锚(本部署补丁:凌晨批项4)。返回是否钉上。

        双槽规则(设计见 项目文档库 锚系统各节):
        - 委托人 ∈ master_ids → 维护者槽,无条件顶自己的旧锚;
        - 否则客人槽:空槽直接钉;旧锚「已应答 / 超龄」才可顶,都不满足则不顶
          (旧锚 open 且新鲜 = 正在给 A 干活,B 的委托轮本身是 addressed 全量轮
          不亏上下文,代价不对称,不为 1-2% 的交错形态硬顶)。
          「不顶就是不顶」——待提升补位为设计可选件,本期未做。
        直接操作传入的 session 实例(trigger() 返回的那个),不按 key 查。
        """
        if record.commissioner_id in master_ids:
            session.anchor_master = record
            self._persist(session)
            return True
        old = session.anchor_guest
        if (
            old is None
            or old.answered
            or (max_age_ms > 0 and now_ms - old.ts > max_age_ms)
        ):
            session.anchor_guest = record
            self._persist(session)
            return True
        return False

    def release_anchor(self, adapter: str, group_id: str, who: str) -> int:
        """松锚(who=委托人 user_id 或 "all"),返回松掉的条数。无窗口/无匹配 = 0。

        模型的 release_anchor 决策字段经 handlers 到这里;who 是模型给的字符串,
        不匹配任何在钉锚的 commissioner 时安全 no-op。
        """
        s = self._sessions.get((adapter, group_id))
        if s is None:
            return 0
        released = 0
        for slot in ("anchor_master", "anchor_guest"):
            rec = getattr(s, slot)
            if rec is not None and (who == "all" or rec.commissioner_id == who):
                setattr(s, slot, None)
                released += 1
        if released:
            self._persist(s)
        return released

    def get_anchors(
        self,
        adapter: str,
        group_id: str,
        now_ms: int,
        *,
        max_age_ms: int,
    ) -> builtins.list[AnchorRecord]:
        """取本群当前在钉的委托锚(维护者槽在前),懒式超龄剔除(硬寿命上限)。

        只读活跃窗口:get_if_active 口径,过期/T5 封顶的窗口视为无锚。
        超龄剔除是原地写(槽位置 None),与「窗口死锚随窗死」互补:
        前者管「窗口还活着但锚太旧」,后者管「窗口没了」。
        """
        s = self.get_if_active(adapter, group_id, now_ms)
        if s is None:
            return []
        out: builtins.list[AnchorRecord] = []
        aged_out = False
        for slot in ("anchor_master", "anchor_guest"):
            rec = getattr(s, slot)
            if rec is None:
                continue
            if max_age_ms > 0 and now_ms - rec.ts > max_age_ms:
                setattr(s, slot, None)
                aged_out = True
                continue
            out.append(rec)
        if aged_out:
            self._persist(s)
        return out

    def lease_count(self, adapter: str, group_id: str) -> int:
        """该群当前在跑的 turn 租约数(本部署补丁 2026-10-02, key 级)。"""
        return self._leases.get((adapter, group_id), 0)

    def is_alive(self, adapter: str, group_id: str, now_ms: int) -> bool:
        """TTL 或租约任一成立即活(不看 T5 —— T5 是「值不值得听」,租约是「还在干活」)。

        状态展示 / 工具过滤 / sweep 用这个口径;实例上的 `ActiveSession.is_alive`
        是纯 TTL,别混用。
        """
        s = self._sessions.get((adapter, group_id))
        return s is not None and (
            s.is_alive(now_ms) or self._leases.get((adapter, group_id), 0) > 0
        )

    def begin_turn(self, adapter: str, group_id: str) -> tuple[str, str]:
        """占用租约,返回令牌(key),`end_turn` 凭令牌归还。

        本部署补丁: 计数挂在 key 上而非实例上 ——
        turn 跑期间 trigger() 换新实例不丢租约,收尾的 touch / mark_bot_replied /
        宽限垫都落在当前实例上,修掉「in-flight 期间换实例导致新窗失去保护被
        sweep、pending 触发被吞」(排查记录见 项目文档库)。

        session 不存在时也照常计数(不新建窗口):ephemeral turn 期间被别人的显式
        触发开出的新窗、以及本 turn 自己「发言即开窗」开出的窗,都因此受保护到
        end_turn。不新建窗口的语义不变:窗口只会被显式触发/发言建立,
        should_exit_active 收掉的窗不会被一发排队的 turn 复活。
        """
        key = (adapter, group_id)
        self._leases[key] = self._leases.get(key, 0) + 1
        return key

    def end_turn(self, lease: tuple[str, str] | None, now_ms: int) -> None:
        """释放 `begin_turn` 拿到的租约;计数归零时,当前窗口剩余不足
        `_TURN_END_GRACE_MS` 则垫到这个下限。

        `lease=None`(调用方没走 reactive 分支)→ no-op。

        宽限垫给**当前**实例(按 key 查):turn 跑期间 trigger() 可能换过实例,
        垫给旧实例等于没垫(本部署补丁 2026-10-02)。

        为什么要垫:pending 里的消息紧接着就要在 refire 入口做 get_if_active,而这
        条路径上"窗口刚好在交接的这一瞬过期"是真实存在的 —— 交接跨一次事件循环跳转,
        期间 cron sweep 也可能扫过。按剩余量垫到一个**小**的下限,既盖住交接,又不会
        把「turn 结束后重新计时」变成「turn 结束后再送一整个 TTL」:后者对
        should_reply=false 的静默 turn 尤其错,那正是滑动窗该自然收口的时刻,却被
        续成了只要群里还有人说话就永不过期。真答了话的 turn 由 touch 给满窗续期。
        """
        if lease is None:
            return
        n = max(0, self._leases.get(lease, 0) - 1)
        if n > 0:
            self._leases[lease] = n
            return
        self._leases.pop(lease, None)
        s = self._sessions.get(lease)
        if s is None:
            return
        floor = now_ms + min(self._ttl_ms, _TURN_END_GRACE_MS)
        s.expires_at = max(s.expires_at, floor)
        self._persist(s)

    def update_topic(self, adapter: str, group_id: str, topic_hint: str | None) -> None:
        """更新或清空 topic_hint。

        传 None 即清空(允许 Hermes 在话题漂移检测后主动收尾 topic)。
        若 (adapter, group_id) 不存在则 no-op。
        """
        s = self._sessions.get((adapter, group_id))
        if s is not None:
            s.topic_hint = topic_hint
            self._persist(s)

    def end(self, adapter: str, group_id: str) -> None:
        """按 key 无条件收窗。turn 内表达"这轮谈完了"请用 `end_if_current`。"""
        self._sessions.pop((adapter, group_id), None)
        self._drop(adapter, group_id)

    def end_if_current(self, session: ActiveSession) -> bool:
        """只在字典里仍是这个实例时收窗;返回是否真的收了。

        turn 跑期间可能有别人的显式触发调过 `trigger()`,字典里已经换成一个**新**窗口
        (还带着它自己排在 pending 里的那条 @)。本 turn 的 should_exit_active 说的是
        "我这轮谈完了",无权把后来那个窗口一起弹掉 —— 按 key 收窗会让那个人的 @ 在
        refire 入口的 get_if_active 处静默蒸发,用户侧只看到一个 busy 表情。
        """
        key = (session.adapter, session.group_id)
        if self._sessions.get(key) is not session:
            return False
        del self._sessions[key]
        self._drop(*key)
        return True

    def list(self, adapter: str | None = None) -> builtins.list[ActiveSession]:
        if adapter is None:
            return list(self._sessions.values())
        return [s for s in self._sessions.values() if s.adapter == adapter]

    def sweep_expired(self, now_ms: int) -> builtins.list[ActiveSession]:
        expired = [
            s
            for key, s in self._sessions.items()
            if not s.is_alive(now_ms) and self._leases.get(key, 0) <= 0
        ]
        for s in expired:
            self._sessions.pop((s.adapter, s.group_id), None)
            self._drop(s.adapter, s.group_id)
        return expired
