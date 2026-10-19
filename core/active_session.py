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
class Participant:
    """任务参与者(任务锚重构):平权、有序,首位恒为发起者。"""

    user_id: str
    nickname: str | None
    joined_ts: int  # ms


@dataclass
class AnchorRecord:
    """任务锚(任务化重构,原「委托锚」):一桩被钉住的委托任务,
    随窗口存活,渲染进 <pinned_tasks> 块供续发轮定位任务来源。

    按任务组织而非按人:同任务的补充/验收 merge 进同一条锚,参与者平权累积;
    发起者恒为 participants[0],任务句柄=发起者 id(release/answer/merge 的
    引用值)。answered=「该任务已应答」:模型信封 answer_anchor 声明置位,
    merge 进新输入时程控重置为 False(防遗漏回复)。客人槽顶锚规则
    照旧认 answered——旧任务已应答,锚剩余价值低,可被顶。
    """

    participants: list[Participant]
    msg_id: str | int | None
    text: str  # 钉入时已截断(渲染端还会再过一道 sanitize)
    ts: int  # ms
    answered: bool = False

    @property
    def commissioner_id(self) -> str:
        """兼容旧读取点:发起者 id(任务句柄)。空锚防御回 ''。"""
        return self.participants[0].user_id if self.participants else ""

    @property
    def commissioner_nickname(self) -> str | None:
        return self.participants[0].nickname if self.participants else None


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
    用作 handlers 的 post-reply cooldown 判定。续期保留,cooldown 基准跨触发存活;
    死窗新建才归零,避免跨窗口残留状态把新一轮对话的第一条非显式消息直接 skip 掉。"""

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

    # 本部署补丁: 委托锚双槽。管理员槽
    # (commissioner ∈ hermes_admin_users)只被管理员自己的新委托顶;客人槽全客人
    # 共享,顶锚需满足三条件之一(旧锚已应答/超龄/已被 release,见 pin_anchor)。
    # 窗口死(TTL/T5/should_exit_active)= session 实例销毁,锚随窗死;
    # 活窗续期复用同一实例,两槽天然保留。
    anchor_master: AnchorRecord | None = None
    anchor_guest: AnchorRecord | None = None

    # 世代号:活窗被显式 trigger 续期时 +1。收窗双重比对(实例同一性+世代号)
    # 用——turn 期间窗被续期即新世代,旧 turn 的 should_exit_active 收不动续期后的窗。
    generation: int = 0

    # 本部署补丁: 租约计数从实例级移到 key 级(管理器
    # _leases 字典),实例上不再有 inflight_turns 字段。旧设计里 trigger() 每次
    # 换实例会把计数清零,in-flight turn 的保护落不到新窗口上;key 级挂点后
    # 续不换/新建都不影响租约(排查记录见 项目文档库)。

    def is_alive_ttl(self, now_ms: int) -> bool:
        """纯 TTL 判活:expires_at 未到期。

        租约不在实例上 —— 要判「窗口还活着」(TTL 或租约任一成立)请用
        ActiveSessionManager.is_alive_with_lease(adapter, group_id, now_ms)。"""
        return self.expires_at > now_ms


class ActiveSessionManager:
    """滑动 TTL 状态机,跟踪哪些 (adapter, group_id) 处于 reactive 监听窗口。

    线程安全:**否**。预设单线程 asyncio 事件循环,不要从背景线程访问。
    租约(key 级,本部署补丁):`begin_turn` / `end_turn` 之间该
    (adapter, group_id) 一律算活着。计数挂在管理器的 `_leases` 字典上而非
    session 实例上 —— 续不换后 trigger 原地续期、死窗新建都不动租约计数,
    in-flight turn 的保护跟着群走。
    `is_alive_for_trigger` / `get_if_trigger_alive` / `touch` / `sweep_expired` 都尊重它。
    GC 策略:不在读路径自动剔除过期 session,由 Task 16 的 cron 调 sweep_expired()
    定期清理。读 API 中:`is_alive_for_trigger` / `touch` / `get_if_trigger_alive` 是 TTL 感知的;
    `get()` 是裸访问,**返回过期 session**——仅用于调试 / 日志,handler 应优先用
    is_alive_for_trigger / get_if_trigger_alive。
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
            if not s.is_alive_ttl(now_ms) or self._beyond_t5(s, now_ms):
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

        判断放读路径(is_alive_for_trigger / get_if_trigger_alive)而非 sweep cron:窗口被隐式续期的
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
        key = (adapter, group_id)
        old = self._sessions.get(key)
        if old is not None and self.is_alive_with_lease(adapter, group_id, now_ms):
            # 活窗续期复用同一实例(TTL 或租约任一即活;不看 T5——显式触发本身
            # 会重算 last_strong_activation_at,T5 随之解开)。锚双槽与
            # last_bot_reply_* 天然保留在同实例上,无需搬运。
            old.triggered_by = user_id
            old.topic_hint = topic_hint  # 直接赋值,None 即清空,与新建语义一致
            old.last_active_at = now_ms
            old.expires_at = now_ms + self._ttl_ms
            old.last_strong_activation_at = now_ms
            old.generation += 1
            self._persist(old)
            return old
        # 无窗或死窗(TTL 与租约都过期):新建实例。锚随旧窗死,不继承。
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
        self._sessions[key] = s
        self._persist(s)
        return s

    def touch(self, adapter: str, group_id: str, now_ms: int) -> ActiveSession | None:
        s = self._sessions.get((adapter, group_id))
        if s is None or not self.is_alive_with_lease(adapter, group_id, now_ms):
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

        多数 handler 应改用 `get_if_trigger_alive` 或先 `is_alive_for_trigger` 校验。本方法保留是为了
        调试 / 日志场景需要观测已过期但尚未被 sweep 的 session。
        """
        return self._sessions.get((adapter, group_id))

    def get_if_trigger_alive(self, adapter: str, group_id: str, now_ms: int) -> ActiveSession | None:
        """TTL 感知的 get:只在 session 存在且未过期时返回。T5 封顶同样视为不活跃。"""
        s = self._sessions.get((adapter, group_id))
        if s is None or not self.is_alive_with_lease(adapter, group_id, now_ms) or self._beyond_t5(s, now_ms):
            return None
        return s

    def is_alive_for_trigger(self, adapter: str, group_id: str, now_ms: int) -> bool:
        s = self._sessions.get((adapter, group_id))
        return (
            s is not None
            and self.is_alive_with_lease(adapter, group_id, now_ms)
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

        bot 发言同步滑动 TTL(expires_at=max(原值,now+TTL))——
        与 last_strong_activation_at 的 T5 重算同口径;touch() 仍是日常续期入口。
        handlers 在 reactive 模式 send 成功后
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
        # bot 发言同步滑动 TTL——既已重算 T5,不续 TTL 自相矛盾;
        # 旧口径下长 turn(超 TTL)收尾后窗口即死,紧随的跟帖接力被无窗 drop。
        s.expires_at = max(s.expires_at, now_ms + self._ttl_ms)
        # answered 不再由本方法自动全覆盖标记——
        # 「答没答、答的是谁」模型自己最清楚,改由信封 answer_anchor 字段声明,
        # 走 mark_anchor_answered;插件端不再替模型做归属判断(旧逻辑:bot 答任何人
        # 都把所有锚标 answered,客人槽保护期形同虚设)。
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
        """钉委托锚(本部署补丁)。返回是否钉上。

        双槽规则(设计见 项目文档库 锚系统各节):
        - 委托人 ∈ master_ids → 管理员槽,无条件顶自己的旧锚;
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

    def pinned_commissioners(self, adapter: str, group_id: str) -> set[str]:
        """当前在钉锚的委托人裸 id 集合(answer_anchor 的合法值校验用)。无窗口/无锚 = 空集。"""
        s = self._sessions.get((adapter, group_id))
        if s is None:
            return set()
        return {
            rec.commissioner_id
            for rec in (s.anchor_master, s.anchor_guest)
            if rec is not None
        }

    def mark_anchor_answered(self, adapter: str, group_id: str, who: str) -> int:
        """把指定委托人的在钉锚标「已应答」,返回新标记的条数(方案D:模型自答自标)。

        who=委托人 user_id 或 "all"。合法性校验(值是否在 pinned_commissioners 里)
        在调用方(handler)做,本方法只管标记;无窗口/无匹配 = 0 安全 no-op。
        已 answered 的锚不重复计数。应答 ≠ 松锚——追问期锚继续钉到窗口死/超龄/release。
        """
        s = self._sessions.get((adapter, group_id))
        if s is None:
            return 0
        marked = 0
        for slot in ("anchor_master", "anchor_guest"):
            rec = getattr(s, slot)
            if rec is not None and (who == "all" or rec.commissioner_id == who):
                if not rec.answered:
                    rec.answered = True
                    marked += 1
        if marked:
            self._persist(s)
        return marked

    def pinned_participant_ids(self, adapter: str, group_id: str, now_ms: int, max_age_ms: int) -> set[str]:
        """全部在钉锚的全体参与者 id 集合(steer 资格判定用,任务化重构 )。

        走 get_anchors(懒式超龄剔除)口径:过期/超龄锚的参与者不再有资格。
        """
        out: set[str] = set()
        for rec in self.get_anchors(adapter, group_id, now_ms, max_age_ms=max_age_ms):
            out.update(p.user_id for p in rec.participants)
        return out

    def find_anchor_starter_by_msg_id(self, adapter: str, group_id: str, msg_id) -> str | None:
        """按首条委托消息 id 找在钉锚,命中返回发起者 id(quote 自动归并用)。"""
        if msg_id is None:
            return None
        s = self._sessions.get((adapter, group_id))
        if s is None:
            return None
        for rec in (s.anchor_master, s.anchor_guest):
            if rec is not None and rec.msg_id is not None and str(rec.msg_id) == str(msg_id):
                return rec.commissioner_id
        return None

    def merge_into_anchor(
        self,
        session: "ActiveSession",
        *,
        starter_id: str,
        user_id: str,
        nickname: str | None,
        now_ms: int,
    ) -> bool:
        """把发送者并入指定任务的锚(任务化重构 )。返回是否并入。

        participants 追加去重(同人刷新昵称);**answered 程控重置为 False**
        (merge 进新输入=任务重新打开,防遗漏回复;答完由模型
        answer_anchor 手动标回)。直接操作传入的 session 实例(与 pin_anchor 同约定)。
        """
        for slot in ("anchor_master", "anchor_guest"):
            rec = getattr(session, slot)
            if rec is not None and rec.commissioner_id == starter_id:
                known = {p.user_id for p in rec.participants}
                if user_id not in known:
                    rec.participants.append(Participant(user_id=user_id, nickname=nickname, joined_ts=now_ms))
                else:
                    for p in rec.participants:
                        if p.user_id == user_id:
                            p.nickname = nickname or p.nickname
                rec.answered = False
                self._persist(session)
                return True
        return False

    def get_anchors(
        self,
        adapter: str,
        group_id: str,
        now_ms: int,
        *,
        max_age_ms: int,
    ) -> builtins.list[AnchorRecord]:
        """取本群当前在钉的委托锚(管理员槽在前),懒式超龄剔除(硬寿命上限)。

        只读活跃窗口:get_if_trigger_alive 口径,过期/T5 封顶的窗口视为无锚。
        超龄剔除是原地写(槽位置 None),与「窗口死锚随窗死」互补:
        前者管「窗口还活着但锚太旧」,后者管「窗口没了」。
        """
        s = self.get_if_trigger_alive(adapter, group_id, now_ms)
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
        """该群当前在跑的 turn 租约数(本部署补丁, key 级)。"""
        return self._leases.get((adapter, group_id), 0)

    def is_alive_with_lease(self, adapter: str, group_id: str, now_ms: int) -> bool:
        """TTL 或租约任一成立即活(不看 T5 —— T5 是「值不值得听」,租约是「还在干活」)。

        状态展示 / 工具过滤 / sweep 用这个口径;实例上的 `ActiveSession.is_alive_ttl`
        是纯 TTL,别混用。
        """
        s = self._sessions.get((adapter, group_id))
        return s is not None and (
            s.is_alive_ttl(now_ms) or self._leases.get((adapter, group_id), 0) > 0
        )

    def begin_turn(self, adapter: str, group_id: str) -> tuple[str, str]:
        """占用租约,返回令牌(key),`end_turn` 凭令牌归还。

        本部署补丁: 计数挂在 key 上而非实例上 ——
        turn 跑期间 trigger() 对活窗原地续期复用实例,租约不丢;收尾的 touch /
        mark_bot_replied / 宽限垫都落在当前实例上(排查记录见 项目文档库)。

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

        宽限垫给**当前**实例(按 key 查)。续不换落地后,turn 持有的 session
        引用与字典当前实例恒为同一对象,本注释保留的机制描述仍成立(本部署补丁)。

        为什么要垫:pending 里的消息紧接着就要在 refire 入口做 get_if_trigger_alive,而这
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
        """按 key 无条件收窗。turn 内表达"这轮谈完了"请用 `end_if_current_generation`。"""
        self._sessions.pop((adapter, group_id), None)
        self._drop(adapter, group_id)

    def end_if_current_generation(self, session: ActiveSession, generation: int) -> bool:
        """只在字典里仍是这个实例、且世代号未变时收窗;返回是否真的收了。

        世代号保护:turn 跑期间若有别人的显式触发调过 `trigger()`,活窗被原地
        续期,世代号 +1。本 turn 持有的 (session, generation) 是旧世代对,
        与当前 (实例, 世代号) 不再匹配,收窗请求自然失效——否则旧 turn 的
        should_exit_active 会把续期后的新窗一起弹掉,别人那条排在 pending 里
        等 refire 的 @ 会在 get_if_trigger_alive 处静默蒸发,用户侧只看到
        一个 busy 表情。
        """
        key = (session.adapter, session.group_id)
        if self._sessions.get(key) is not session or session.generation != generation:
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
            if not s.is_alive_ttl(now_ms) and self._leases.get(key, 0) <= 0
        ]
        for s in expired:
            self._sessions.pop((s.adapter, s.group_id), None)
            self._drop(s.adapter, s.group_id)
        return expired
