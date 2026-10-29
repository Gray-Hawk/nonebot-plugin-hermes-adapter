"""
会话管理

只维护 Hermes session key 映射;消息历史已移交 MessageBuffer。
"""

from __future__ import annotations

import time

from nonebot import logger

from .storage.session_key_store import SessionKeyStore


class SessionManager:
    """管理 Hermes session key 的生成、采纳与过期"""

    def __init__(self) -> None:
        self._cache: dict[str, str] = {}
        self._generation: dict[str, int] = {}
        self._store: SessionKeyStore | None = None
        # T1 空闲重置的三份状态(内存态,重启即清——重启后没有活动记录,
        # idle_expired 只挑有记录的会话,不会重启完立刻清人):
        self._components: dict[str, tuple[str, bool, str, str | None]] = {}
        """internal_id -> (adapter_name, is_private, user_id, group_id),get_session_key 时登记"""
        self._last_activity: dict[str, float] = {}
        """internal_id -> 最后一次 chat() 完成 / push_message 成功的 epoch 秒"""
        self._idle_paused: set[str] = set()
        """被 /delay 暂停 T1 计时的 internal_id"""
        # 增量推送水位:internal_id -> (session_key, 已推最大消息 id)。
        # 内存态即可——nb 重启丢水位的代价只是每群首 turn 全量重推一次,无副作用。
        self._push_watermark: dict[str, tuple[str, int]] = {}
        # alive/108:clear/T1 换代时给 runs 交付台账落提交序水位。只 duck-type
        # 调用公开方法,不引入 session → run_tracker 的模块依赖。
        self._delivery_tracker = None

    def bind_run_tracker(self, tracker) -> None:
        """绑定交付代际栅栏;mcp 启动装配时调用,未绑定保持旧 clear 行为。"""
        self._delivery_tracker = tracker

    def _fence_delivery_generation(
        self, adapter_name: str, is_private: bool, user_id: str, group_id: str | None
    ) -> None:
        tracker = self._delivery_tracker
        if tracker is None:
            return
        scope = "private" if is_private else "group"
        scope_id = user_id if is_private else (group_id or "")
        scope_key = f"{adapter_name}:{scope}:{scope_id}"
        try:
            result = tracker.fence_scope_generation(scope_key)
            if result["deliveries"] or result["submissions"]:
                logger.info(f"[SESSION] 旧代交付已取消: scope={scope_key} {result}")
        except Exception:
            logger.warning(f"[SESSION] 交付代际栅栏落账失败: scope={scope_key}", exc_info=True)

    def bind_store(self, store: SessionKeyStore) -> None:
        """绑定持久化存储并载入已有映射(启动时调一次)。

        未绑定时整个管理器退化成纯内存,行为不变——但那样重启会退回派生 key,
        既复活被 /clear 掉的会话,也会把已被上游压缩关闭的父会话重新钉上。

        载入后做 T1 全量播种(定案):所有已知会话以开机时刻为活动
        基准,重启后沉默满 T1 即被自动清掉。不设 generation 门槛——T1 上线后
        generation 本来就快速上涨,gen=0 条件会迅速失效成摆设;反复清理的代价
        仅为本地计数器 +1(不上游、无消息、无 LLM 调用),可接受。
        """
        self._store = store
        for internal_id, (session_key, generation) in store.load_all().items():
            self._cache[internal_id] = session_key
            self._generation[internal_id] = generation
        # /delay 暂停集合装回(原内存态重启即失效)。
        self._idle_paused.update(store.load_idle_paused())
        now = time.time()
        seeded = 0
        for internal_id in self._cache:
            comp = self._parse_internal_id(internal_id)
            if comp is None:
                continue
            self._components[internal_id] = comp
            self._last_activity[internal_id] = now
            seeded += 1
        if self._cache:
            logger.debug(f"[SESSION] 载入 {len(self._cache)} 条持久化 session key 映射")
        if seeded:
            logger.info(f"[SESSION] T1 全量播种: {seeded} 个已知会话以开机时刻为活动基准")

    @staticmethod
    def _parse_internal_id(internal_id: str) -> tuple[str, bool, str, str | None] | None:
        """从 internal_id 反解组件,供 T1 播种/清理反查用。

        格式由 _get_internal_id 定义:
          私聊: adapter+private+user_id
          群:   adapter+group+group_id(群 session 固定共享)
        依赖「adapter/user_id/group_id 不含 +」的既有约定。解析失败返回 None,
        该会话不参与 T1(比解错强)。
        """
        parts = internal_id.split("+")
        if len(parts) < 3:
            return None
        adapter, kind = parts[0], parts[1]
        if kind == "private":
            return (adapter, True, parts[2], None)
        if kind == "group":
            group_id = parts[2]
            user_id = parts[3] if len(parts) > 3 else ""
            return (adapter, False, user_id, group_id)
        return None

    def _persist(self, internal_id: str) -> None:
        if self._store is None:
            return
        self._store.put(
            internal_id,
            self._cache[internal_id],
            self._generation.get(internal_id, 0),
            now=time.time(),
        )

    def _get_internal_id(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
    ) -> str:
        """生成统一的内部会话 ID(群 session 固定共享,不按配置切换)。

        注:adapter_name / user_id / group_id 假定不含 '+';真实 adapter 名经
        get_adapter_name() 规整后均为 [a-z0-9],平台 user_id 多为数字串。
        """
        if is_private:
            return f"{adapter_name}+private+{user_id}"
        # 群 session 固定共享(旧 per-user 分支已删):不再按人细分。
        return f"{adapter_name}+group+{group_id or 'unknown'}"

    def get_session_key(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
    ) -> str:
        """获取或创建 Hermes session key,通过 X-Hermes-Session-Id 头送给上游。"""
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        # 登记组件,供 T1 空闲重置反查(internal_id → clear / inflight key 都需要)
        self._components[internal_id] = (adapter_name, is_private, user_id, group_id)
        cached = self._cache.get(internal_id)
        if cached is not None:
            return cached

        gen = self._generation.get(internal_id, 0)
        session_key = f"hermes-{internal_id}"
        if gen > 0:
            session_key = f"{session_key}-g{gen}"

        self._cache[internal_id] = session_key
        self._persist(internal_id)
        logger.debug(f"[SESSION] 新建会话: {internal_id} -> {session_key}")
        return session_key

    def adopt_session_key(self, previous_key: str, new_key: str) -> bool:
        """采纳上游轮换后的 session key,返回是否命中一条映射。

        上游自动压缩上下文时会关闭旧 session 并新建 continuation,新 id 走响应头
        X-Hermes-Session-Id 回传。不采纳就等于每轮都往一个 end_reason='compression'
        的会话里写,上游一律拒收,而且每压缩一次就再分叉一个兄弟会话。

        previous_key 认不出来时(典型是采纳与 /clear 撞车,映射已被换掉)不做任何事:
        凭 key 反推 internal_id 会把刚清空的会话又接回旧血缘。
        """
        if not new_key or new_key == previous_key:
            return False
        for internal_id, current in self._cache.items():
            if current != previous_key:
                continue
            self._cache[internal_id] = new_key
            # 新转录是空的,增量推送水位随轮换失效(get 侧也有 key 校验兜底)
            self._push_watermark.pop(internal_id, None)
            self._persist(internal_id)
            logger.info(f"[SESSION] 采纳上游轮换: {internal_id} -> {new_key}")
            return True
        logger.debug(f"[SESSION] 上游轮换的 {previous_key} 已不在映射中,忽略")
        return False

    def clear_session(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
        cutoff_msg_id: int | None = None,
    ) -> None:
        """重置会话并递增 generation；群聊人工命令可留下瞬态截断水位。"""
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        self._cache.pop(internal_id, None)
        self._push_watermark.pop(internal_id, None)
        gen = self._generation.get(internal_id, 0) + 1
        self._generation[internal_id] = gen
        # 立刻把新 key 落库:generation 只活在内存的话,重启后 /clear 会失效,
        # 被清掉的会话原地复活。
        new_key = self.get_session_key(adapter_name, is_private, user_id, group_id)
        if cutoff_msg_id is not None:
            self._push_watermark[internal_id] = (new_key, cutoff_msg_id)
        self._fence_delivery_generation(adapter_name, is_private, user_id, group_id)
        logger.info(f"[SESSION] 会话已重置: {internal_id} (generation={gen})")

    # --- 增量推送水位(仅群聊 reactive 使用) ---

    def get_push_watermark(
        self, adapter_name: str, user_id: str, group_id: str
    ) -> int | None:
        """本会话已推过的最大消息 id;None = 首推/已归零(下轮全量窗口)。

        水位与 session key 绑定:存储的 key 与当前映射不一致(上游轮换/
        未留 cutoff 的 /clear 后重建)即视为无效,返回 None 让下轮全量重推。
        群聊人工 /clear 留下 cutoff 时,key 与水位一起换新,仅推 cutoff 后新消息。
        """
        internal_id = self._get_internal_id(adapter_name, False, user_id, group_id)
        entry = self._push_watermark.get(internal_id)
        if entry is None:
            return None
        key, watermark = entry
        if key != self._cache.get(internal_id):
            self._push_watermark.pop(internal_id, None)
            return None
        return watermark

    def advance_push_watermark(
        self,
        adapter_name: str,
        user_id: str,
        group_id: str,
        msg_id: int,
        expected_key: str,
    ) -> None:
        """推进水位(只升不降,并发 refire/ephemeral 乱序安全)。

        expected_key = 本轮 chat() 使用的 session key:本 turn 内若发生上游轮换
        (溢出恢复),缓存 key 已变,推送的内容只进了旧转录,新转录靠 continuation
        摘要——此时拒绝推进,下轮 get 返回 None 全量重推,宁重复不丢失。
        """
        internal_id = self._get_internal_id(adapter_name, False, user_id, group_id)
        key = self._cache.get(internal_id)
        if key is None or key != expected_key:
            return
        entry = self._push_watermark.get(internal_id)
        if entry is None or entry[0] != key:
            self._push_watermark[internal_id] = (key, msg_id)
        else:
            self._push_watermark[internal_id] = (key, max(entry[1], msg_id))

    # --- T1 空闲自动重置(本部署新增) ---

    def known_sessions(self) -> list[tuple[str, str, bool, str, str | None, str]]:
        """全量已知会话快照(本部署补丁:投递轮询用)。

        返回 (internal_id, adapter_name, is_private, user_id, group_id, session_key)。
        只出组件齐全的条目(组件缺失意味着反解失败,轮询无法定位目标群/人)。
        """
        out: list[tuple[str, str, bool, str, str | None, str]] = []
        for internal_id, session_key in self._cache.items():
            comp = self._components.get(internal_id)
            if comp is None:
                continue
            adapter, is_private, user_id, group_id = comp
            out.append((internal_id, adapter, is_private, user_id, group_id, session_key))
        return out

    def mark_activity(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
    ) -> None:
        """T1 活动基准:每次 chat() 完成(无论成败,调用即活动)时由
        _chat_and_adopt_rotation 收口处调用。"""
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        self._last_activity[internal_id] = time.time()

    def mark_activity_group(self, adapter_name: str, group_id: str) -> None:
        """群级活动标记(push_message 用):群 session 固定共享,internal_id 即
        adapter+group+group_id 唯一形态。"""
        prefix = f"{adapter_name}+group+{group_id}"
        now = time.time()
        for internal_id in self._components:
            if internal_id == prefix:
                self._last_activity[internal_id] = now

    def set_idle_paused(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None,
        paused: bool,
    ) -> str:
        """/delay(paused=True)暂停 T1;/resume(paused=False)恢复并重置计时。
        返回 internal_id 供日志用。持久化到 session_keys.db。"""
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        if paused:
            self._idle_paused.add(internal_id)
        else:
            self._idle_paused.discard(internal_id)
            self._last_activity[internal_id] = time.time()
        if self._store is not None:
            self._store.set_idle_paused(internal_id, paused)
        return internal_id

    def idle_expired(
        self, idle_sec: int, now: float | None = None
    ) -> list[tuple[str, tuple[str, bool, str, str | None]]]:
        """返回超过 idle_sec 未活动且未 /delay 暂停的 (internal_id, components) 列表。

        只挑有活动记录的会话:插件重启后 _last_activity 是空的,没有这个限制就会
        把「重启前一直在聊、重启后还没人说话」的会话立刻清掉。"""
        now = now if now is not None else time.time()
        out: list[tuple[str, tuple[str, bool, str, str | None]]] = []
        for internal_id, last in self._last_activity.items():
            if internal_id in self._idle_paused:
                continue
            if now - last <= idle_sec:
                continue
            comp = self._components.get(internal_id)
            if comp is not None:
                out.append((internal_id, comp))
        return out

    def clear_internal(self, internal_id: str) -> None:
        """按 internal_id 直接重置会话(T1 自动清用),不发任何用户可见提示。

        与 clear_session 同逻辑,但 key 渲染直接作用在传入的 internal_id 上,
        不经组件反解重算,直接作用在传入的 internal_id 上——播种来的会话组件是反解
        出来的,重算可能落到另一个 internal_id,导致原 id 的 generation 递增不持久化。
        重置后活动基准刷新为现在,给新会话一个完整 T1 周期。"""
        self._cache.pop(internal_id, None)
        self._push_watermark.pop(internal_id, None)  # 转录清空,增量推送水位归零
        gen = self._generation.get(internal_id, 0) + 1
        self._generation[internal_id] = gen
        self._cache[internal_id] = f"hermes-{internal_id}-g{gen}"
        self._persist(internal_id)
        comp = self._parse_internal_id(internal_id)
        if comp is not None:
            self._fence_delivery_generation(*comp)
        self._last_activity[internal_id] = time.time()
        logger.info(f"[SESSION] T1 空闲自动重置: {internal_id} (generation={gen})")


# 全局会话管理器
session_manager = SessionManager()
