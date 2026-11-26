"""
会话管理

只维护 Hermes session key 映射;消息历史已移交 MessageBuffer。
"""

from __future__ import annotations

import time

from nonebot import logger

from .storage.session_key_store import (
    SessionDomainMigrationConflict,
    SessionKeyStore,
)

# 业务会话域：QQTPC＝QQ Third-Party Client。OneBot v11/v12、Milky、icqq 都是
# QQ 用户侧同一整体在不同驱动下的形态，用户在协议间切换不应产生新会话。
# 会话身份归一到 ``qqtpc``；真实 adapter 只是运行时路由事实（当前 Bot 连接／
# 事件／BotRegistry），**不落库**，切换协议因此零人工迁移。
SESSION_DOMAIN_QQTPC = "qqtpc"
QQTPC_TRANSPORTS: tuple[str, ...] = ("onebotv11", "onebotv12", "milky", "icqq")
# transport -> 业务会话域。未登记 adapter（QQ 官方 ``qq``、tg 等）不归一，
# internal_id 原样带自己的 adapter 名，行为与改名前逐字一致。
SESSION_DOMAIN_TRANSPORTS: dict[str, str] = {t: SESSION_DOMAIN_QQTPC for t in QQTPC_TRANSPORTS}
_SESSION_DOMAINS: frozenset[str] = frozenset(SESSION_DOMAIN_TRANSPORTS.values())


def session_domain(adapter_name: str | None) -> str:
    """adapter 名 → 业务会话域；未登记的 adapter 就是它自己。"""
    from ..adapter_norm import normalize_adapter_name

    tag = normalize_adapter_name(adapter_name or "")
    return SESSION_DOMAIN_TRANSPORTS.get(tag, tag)


def is_session_domain(name: str | None) -> bool:
    """``name`` 是否业务会话域（而非可路由的真实 adapter）。"""
    return normalize_domain_tag(name) in _SESSION_DOMAINS


def normalize_domain_tag(name: str | None) -> str:
    from ..adapter_norm import normalize_adapter_name

    return normalize_adapter_name(name or "")


# session key 前缀按**真实后端**命名(不再把 Hermes 名字硬绑在 key 上):
# - 上游 session id 不可本地强改,已缓存的 key 必须原样续用,否则断上游历史;
# - 换后端前缀时只改**本地派生**形态 ``{prefix}-{internal_id}[-gN]`` 的名字。
KEY_PREFIX_HERMES = "hermes"
# 升级前所有后端都用 hermes- 前缀,换前缀时由它识别"旧派生 key"。
LEGACY_KEY_PREFIX = KEY_PREFIX_HERMES
# 高频 run 进展的全局写入合并窗口:开始/结束/异常立即冲刷,中间最多每 10 秒一批。
ACTIVITY_FLUSH_INTERVAL_SEC = 10.0
# 接口名 -> session key 前缀。主线现有后端(completions / runs / astrbot)都是
# Hermes 后端本体,沿用 hermes-;接入独立后端(自带上游会话命名)时在这里登记,
# 未登记一律 hermes-(即旧行为)。
_KEY_PREFIX_BY_INTERFACE: dict[str, str] = {}


def session_key_prefix_for_interface(interface: str) -> str:
    """接口名对应的 session key 前缀(未登记的后端用 hermes-)。"""
    return _KEY_PREFIX_BY_INTERFACE.get(str(interface or ""), KEY_PREFIX_HERMES)


def resolve_session_key_prefix() -> str:
    """当前后端使用的 session key 前缀(只读启动配置,不新增配置项)。

    配置未初始化(离线工具/单测)时按 Hermes 形态,旧行为不变。
    """
    try:
        from ..config import plugin_config

        interface = str(getattr(plugin_config, "hermes_api_interface", "") or "")
    except Exception:  # pragma: no cover - 配置导入失败时保持旧行为
        interface = ""
    return session_key_prefix_for_interface(interface)


def _runtime_bot_registry():
    """取当前 BotRegistry(未装配/离线工具返回 None),避免 core → mcp 硬依赖。"""
    try:
        from .. import mcp as _mcp

        return getattr(_mcp, "bot_registry", None)
    except Exception:  # pragma: no cover - 插件未装配时的离线工具/单测
        return None


def _live_transports(domain: str) -> list[str]:
    """当前在线 bot 里属于该业务会话域的真实 adapter(去重)。

    只用于「该域唯一驱动」时的兜底解析;多个同时在线返回多个,调用方按歧义
    处理。nonebot 未初始化时返回空表。
    """
    try:
        import nonebot

        bots = nonebot.get_bots()
    except Exception:  # pragma: no cover - 未初始化 driver 的离线工具/单测
        return []
    found: list[str] = []
    for bot in bots.values():
        get_name = getattr(getattr(bot, "adapter", None), "get_name", None)
        raw = str(get_name() or "") if callable(get_name) else ""
        name = normalize_domain_tag(raw)
        if not name or is_session_domain(name):
            continue
        if session_domain(name) == domain and name not in found:
            found.append(name)
    return found


def _adapter_is_live(adapter_name: str) -> bool:
    """某个真实 adapter 现在是否还有在线 Bot 连接(当前事实,非历史协议名)。"""
    name = normalize_domain_tag(adapter_name)
    if not name or is_session_domain(name):
        return False
    return name in _live_transports(session_domain(name))


class SessionManager:
    """管理 session key 的生成、采纳与过期"""

    def __init__(self) -> None:
        self._cache: dict[str, str] = {}
        self._generation: dict[str, int] = {}
        self._store: SessionKeyStore | None = None
        # 当前 session key 前缀(按后端;bind_store / 取 key 时校准并给旧派生 key 改名)。
        self._key_prefix = KEY_PREFIX_HERMES
        # T1 空闲重置的三份状态(内存态,重启即清——重启后没有活动记录,
        # idle_expired 只挑有记录的会话,不会重启完立刻清人):
        self._components: dict[str, tuple[str, bool, str, str | None]] = {}
        """internal_id -> (adapter_name, is_private, user_id, group_id),get_session_key 时登记"""
        self._last_activity: dict[str, float] = {}
        """internal_id -> 最后一次真实活动(chat 完成 / 允许计时的 push)的 epoch 秒。

        跨重启从 ``session_keys.db`` 原样读回;没有记录(旧库默认值)就不在表里,
        不播种、不猜开机时刻。"""
        self._idle_armed: dict[str, bool] = {}
        """internal_id -> 是否具备闲置动作资格(clear/compress/keepalive)。

        只有真实活动才置 True;clear／compress 成功与上游压缩轮换立即置 False,
        重启后按库里的值恢复——失活会话不会被重新武装。"""
        self._keepalive_last: dict[str, float] = {}
        """internal_id -> 上次成功保活的 epoch 秒,随其他闲置状态一起持久化。"""
        self._activity_dirty: set[str] = set()
        """有活动进展、等待下一批全局合并写入的 internal_id。"""
        self._activity_last_flush = 0.0
        self._idle_paused: set[str] = set()
        """被 /delay 暂停 clear 的 internal_id；compress/keepalive 不读此集合。"""
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
            result = tracker.fence_scope_generation(
                scope_key,
                fence_generation=self.get_delivery_generation(
                    adapter_name, is_private, user_id, group_id
                ),
            )
            if result["deliveries"] or result["submissions"]:
                logger.info(f"[SESSION] 旧代交付已取消: scope={scope_key} {result}")
        except Exception:
            logger.warning(f"[SESSION] 交付代际栅栏落账失败: scope={scope_key}", exc_info=True)

    def bind_store(self, store: SessionKeyStore) -> None:
        """绑定持久化存储并载入已有映射(启动时调一次)。

        未绑定时整个管理器退化成纯内存,行为不变——但那样重启会退回派生 key,
        既复活被 /clear 掉的会话,也会把已被上游压缩关闭的父会话重新钉上。

        每会话的「最后真实活动时刻 + 闲置动作资格」与 key 同库持久化,这里原样
        读回:armed 会话按真实闲置时长继续计时,disarmed 与旧库默认行保持关闭。
        **不做全量播种**——把已知会话的活动时间重填成开机时刻,等于重启后给所有
        沉默会话重新计时(该清的没清),反方向还会让刚压缩过的会话重新保活。

        会话库只做一次 ``{transport}+… → qqtpc+…`` 的 canonical 归一（由
        ``scripts/migrate_session_domain.py`` 在启动前显式执行）；真实 adapter
        不落库——重启后由 ``resolve_adapter_for_domain`` 从当前 Bot 连接／事件／
        BotRegistry 路由恢复。恢复不了的真实适配器动作按现有等待／跳过语义处理，
        不猜旧协议，也不把业务域当 adapter 去调平台 API。
        """
        self._store = store
        self._key_prefix = resolve_session_key_prefix()
        # 会话库只做一次 ``{transport}+… → qqtpc+…`` 的 canonical 归一，且必须在
        # 启动前由运维显式执行(scripts/migrate_session_domain.py)。这里不自动改库:
        # 主实例的「milky测试」小会话会被显式排除，自动重跑只会反复撞冲突。
        # 未归一的旧库 fail closed：宁可拒绝启动，也不在 qqtpc+… 下新建 key 把
        # 主会话历史接断。返回失败关闭时异常里的 (旧 id → 目标 id) 就是迁移清单。
        rows = store.load_all()
        canonical_ids = set(rows)
        pending: list[tuple[str, str]] = []
        for internal_id in rows:
            target = self._legacy_domain_target(internal_id)
            if target is not None and target not in canonical_ids:
                pending.append((internal_id, target))
        paused_ids = store.load_idle_paused()
        for internal_id in paused_ids:
            target = self._legacy_domain_target(internal_id)
            if target is not None and target not in canonical_ids and target not in paused_ids:
                pending.append((internal_id, target))
        if pending:
            logger.error(
                "[SESSION] 会话库仍有未归一的旧协议 internal_id,已零写入并停止启动: "
                f"{pending} —— 先 dry-run 再执行 "
                "`python3 scripts/migrate_session_domain.py --db <session_keys.db> "
                "[--exclude <测试小会话旧 id>]`,然后重启。"
            )
            raise SessionDomainMigrationConflict(pending)
        for internal_id, (session_key, generation) in rows.items():
            # 已归一目标已存在时，旧 transport 行是显式排除后留下的历史小会话；
            # 不参与本次运行（不删除、不改写），也绝不拿它覆盖 canonical 映射。
            if self._legacy_domain_target(internal_id) is not None:
                continue
            self._cache[internal_id] = session_key
            self._generation[internal_id] = generation
        # 一次性改本地 key:后端前缀与旧 hermes- 不同时,把缓存的派生 key
        # ``hermes-{internal_id}[-gN]`` 换成 ``{新前缀}-...``。旧上游会话映射保持
        # 无引用历史(不迁移、不别名),下一轮按真实行为新建上游会话。
        # Hermes 后端前缀不变,这里绝不动任何 key。
        self._rename_derived_keys(LEGACY_KEY_PREFIX, self._key_prefix)
        # /delay 暂停集合装回(原内存态重启即失效);被排除的旧行同样不参与。
        self._idle_paused.update(
            internal_id
            for internal_id in paused_ids
            if self._legacy_domain_target(internal_id) is None
        )
        unresolved = 0
        for internal_id in self._cache:
            comp = self._parse_internal_id(internal_id)
            if comp is None:
                continue
            # 元件登记仍然对所有可解析的已知会话做:重启后业务域会话的真实 adapter
            # 尚未就绪,只登记组件不猜测——等 current 连接/路由恢复后由
            # _components_for 在闲置扫描 / known_sessions 调用时解析。
            if is_session_domain(comp[0]):
                unresolved += 1
                continue
            self._components[internal_id] = comp
        # 闲置状态按库里的真实事实恢复(不播种):armed 会话沿用原始活动时刻继续计时,
        # disarmed／旧库默认行保持关闭,等下一次真实活动再武装。
        restored = 0
        for internal_id, (last_activity, armed, keepalive_last) in store.load_idle_states().items():
            if self._legacy_domain_target(internal_id) is not None:
                continue
            if last_activity <= 0:
                continue
            self._last_activity[internal_id] = last_activity
            self._idle_armed[internal_id] = armed
            self._keepalive_last[internal_id] = keepalive_last
            restored += 1
        if self._cache:
            logger.debug(f"[SESSION] 载入 {len(self._cache)} 条持久化 session key 映射")
        if restored:
            armed_count = sum(1 for value in self._idle_armed.values() if value)
            logger.info(
                f"[SESSION] 恢复 {restored} 条闲置状态(其中 {armed_count} 条具备动作资格)"
                + (f";{unresolved} 个业务域会话等待当前连接恢复真实 adapter" if unresolved else "")
            )

    @staticmethod
    def _parse_internal_id(internal_id: str) -> tuple[str, bool, str, str | None] | None:
        """从 internal_id 反解组件,供闲置动作/清理反查用。

        格式由 _get_internal_id 定义:
          私聊: adapter+private+user_id
          群:   adapter+group+group_id(群 session 固定共享)
        依赖「adapter/user_id/group_id 不含 +」的既有约定。解析失败返回 None,
        该会话不参与 T1(比解错强)。

        第一段对 QQTPC 协议是**业务会话域**(``qqtpc``),不是可路由 adapter:
        真实 adapter 由 ``_components_for`` → ``resolve_adapter_for_domain``
        在调用时从 current 连接/事件/路由解析,解析不出来就不参与。
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

    @staticmethod
    def _legacy_domain_target(internal_id: str) -> str | None:
        """旧 ``{transport}+…`` internal_id → 归一目标;非旧协议行返回 None。"""
        head, sep, tail = internal_id.partition("+")
        if not sep or not tail:
            return None
        head = normalize_domain_tag(head)
        if is_session_domain(head):
            return None
        domain = SESSION_DOMAIN_TRANSPORTS.get(head)
        if not domain:
            return None
        return f"{domain}+{tail}"

    def _component_is_current(
        self, comp: tuple[str, bool, str, str | None]
    ) -> bool:
        """运行时记忆的组件是否仍是**当前**事实。

        ``_components`` 只证明「当时」走过哪个驱动。协议切换后旧驱动可能已经
        下线：只有它现在仍有在线 Bot 连接时，记忆才可以优先。否则返回 False，
        交给 ``resolve_adapter_for_domain`` 按当前连接／路由重新解析（解析不出来
        就是 None，按等待／跳过处理），绝不回落到旧协议名。

        **存活前提是在线连接，不是路由**：``BotRegistry`` 没有断线清理钩子，
        旧驱动的真实路由在断线后会残留；拿残留路由当「当前事实」会让后台唤醒
        走已死驱动、T1 栅栏落错 scope（与「旧驱动记忆压过当前事实」同一类故障，
        只是换成路由形态）。路由只在第 2 步用于**在线**驱动之间消歧。
        """
        adapter, is_private, user_id, group_id = comp
        if is_session_domain(adapter):
            return False
        scope = "private" if is_private else "group"
        scope_id = str(user_id or "") if is_private else str(group_id or "")
        if not scope_id:
            return False
        return _adapter_is_live(adapter)

    def resolve_adapter_for_domain(
        self,
        domain: str,
        is_private: bool,
        user_id: str | None,
        group_id: str | None = None,
    ) -> str | None:
        """业务会话域 → 当前真实 adapter;没有可信运行时事实时返回 None。

        解析顺序(全部是「当前」事实,不读任何持久协议名):

        1. 本进程事件登记过的组件(``_components``):最近一次真实入站/出站
           已经证明该会话走哪个 adapter;协议切换时由 ``get_session_key`` 直接
           覆盖,不需要迁移。**但记忆只证明「当时」——该 adapter 现在已下线时,
           记忆不是当前事实,继续走第 2/3 步。**
        2. ``BotRegistry`` 中该 (scope, scope_id) 的现存路由:真实感知 entry
           与 bot 连接恢复的 synthetic entry 都算,**但只在该 adapter 现在仍有
           在线连接时才算**(断线后真实路由会残留);同一 scope 同时有两个不同
           adapter 路由时视为歧义,返回 None 交给等待/跳过。
        3. 当前在线 bot:该会话域只有一个 adapter 在线时,它就是唯一真实驱动;
           多个同时在线则歧义,返回 None。

        业务域名本身永远不被当成 adapter 返回——拿 ``qqtpc`` 去调平台 API 是
        本方法要防的故障。
        """
        tag = normalize_domain_tag(domain)
        if not tag:
            return None
        scope = "private" if is_private else "group"
        scope_id = str(user_id or "") if is_private else str(group_id or "")
        if not scope_id:
            return None
        # 1) 本进程真实事件登记(同一 internal_id 只会有一个当前 adapter)。
        # 已下线的旧驱动不算当前事实:否则切换后后台唤醒走死驱动、T1 栅栏落错 scope。
        for comp in self._components.values():
            adapter, c_private, c_user, c_group = comp
            if bool(c_private) != bool(is_private):
                continue
            if (str(c_user or "") if is_private else str(c_group or "")) != scope_id:
                continue
            if is_session_domain(adapter) or session_domain(adapter) != tag:
                continue
            if not self._component_is_current(comp):
                continue
            return adapter
        # 2) BotRegistry 现存路由(真实感知优先于 synthetic,但同域多驱动算歧义)
        registry = _runtime_bot_registry()
        if registry is not None:
            real_matches: list[str] = []
            synthetic_matches: list[str] = []
            for adapter, reg_scope, reg_id in registry.known():
                if reg_scope != scope or str(reg_id) != scope_id:
                    continue
                if is_session_domain(adapter) or session_domain(adapter) != tag:
                    continue
                if not _adapter_is_live(adapter):
                    # BotRegistry 断线后不清路由：只有该驱动现在还有在线连接，
                    # 残留的真实/synthetic 路由才是当前事实。
                    continue
                entry = registry.get(adapter, reg_scope, reg_id)
                target = synthetic_matches if getattr(entry, "synthetic", False) else real_matches
                if adapter not in target:
                    target.append(adapter)
            picked = real_matches or synthetic_matches
            if len(picked) == 1:
                return picked[0]
            if len(picked) > 1:
                logger.debug(
                    f"[SESSION] 会话域 {tag} 在 {scope}:{scope_id} 有多个在线路由 "
                    f"{picked},不猜当前协议,按等待处理"
                )
        # 3) 当前在线 bot(仅该域唯一驱动时)
        live = _live_transports(tag)
        if len(live) == 1:
            return live[0]
        return None

    def _components_for(self, internal_id: str) -> tuple[str, bool, str, str | None] | None:
        """T1 / known_sessions / 交付代际用的组件(adapter 位置必为真实 adapter)。

        运行时登记的组件优先——但只在该驱动仍是当前事实时(在线连接或现存路由);
        已下线的旧驱动记忆不算,走反解 + 当前连接/路由解析。业务域名再按当前连接
        解析真实 adapter。解析不出来返回 None——宁可不参与 T1/投递轮询,也不
        拿 ``qqtpc`` 去查路由或调平台 API。
        """
        comp = self._components.get(internal_id)
        parsed = self._parse_internal_id(internal_id)
        if comp is not None:
            # 非业务域的 internal_id 自带真实 adapter 名,记忆就是事实。
            if parsed is None or not is_session_domain(parsed[0]):
                return comp
            # 业务域:记忆只在旧驱动仍是当前事实时才可优先。
            if self._component_is_current(comp):
                return comp
        if parsed is None:
            return None
        domain_or_adapter = parsed[0]
        if is_session_domain(domain_or_adapter):
            resolved = self.resolve_adapter_for_domain(
                domain_or_adapter, parsed[1], parsed[2], parsed[3]
            )
            if not resolved:
                return None
            return (resolved, parsed[1], parsed[2], parsed[3])
        return parsed

    def _persist(self, internal_id: str) -> None:
        if self._store is None:
            return
        self._store.put(
            internal_id,
            self._cache[internal_id],
            self._generation.get(internal_id, 0),
            now=time.time(),
        )

    # --- session key 前缀(按真实后端命名)---

    @property
    def key_prefix(self) -> str:
        """当前后端的 session key 前缀(未登记的后端用 hermes-)。"""
        return self._key_prefix

    def session_key_prefixes(self) -> tuple[str, ...]:
        """本会话可能在真实登记里出现的全部前缀。

        当前后端前缀在前;后端前缀不是 hermes- 时额外保留 hermes-——升级前登记的
        后台事件(看板卡片 session_id / 子代理 origin_session_id)仍是旧 key,
        不能当别人的。
        """
        self._ensure_key_prefix()
        if self._key_prefix == LEGACY_KEY_PREFIX:
            return (self._key_prefix,)
        return (self._key_prefix, LEGACY_KEY_PREFIX)

    @staticmethod
    def _is_derived_key(session_key: str, internal_id: str, prefix: str) -> bool:
        """是否本地派生形态 ``{prefix}-{internal_id}[-gN]``。

        上游轮换回传的 id(任意 uuid)不是派生形态,任何时候都不改名——那是上游
        session id,本地改名等于断上游历史。
        """
        base = f"{prefix}-{internal_id}"
        if session_key == base:
            return True
        return session_key.startswith(base + "-g") and session_key[len(base) + 2 :].isdigit()

    def _rename_derived_keys(self, old_prefix: str, new_prefix: str) -> int:
        """把缓存的派生 key 从 old 前缀换到 new 前缀(只改本地 key 名字)。

        只动 ``{old}-{internal_id}[-gN]``;上游轮换回传的 id 不是派生形态,永不改名。
        换名后旧上游会话映射成为无引用历史(本地 key 已不再指向它),不迁移、不别名:
        新 key 下一轮按真实行为新建上游会话,绝不会错误续读旧映射。
        """
        if old_prefix == new_prefix:
            return 0
        renamed = 0
        for internal_id, session_key in list(self._cache.items()):
            if not self._is_derived_key(session_key, internal_id, old_prefix):
                continue
            self._cache[internal_id] = new_prefix + session_key[len(old_prefix) :]
            # key 变了,增量推送水位作废(旧水位绑在旧 key 上,get 侧也会校验兜底)。
            self._push_watermark.pop(internal_id, None)
            if self._store is not None:
                self._persist(internal_id)
            renamed += 1
        if renamed:
            logger.info(
                f"[SESSION] session key 前缀迁移: {old_prefix}-* -> {new_prefix}-* "
                f"({renamed} 条)"
            )
        self._key_prefix = new_prefix
        return renamed

    def _ensure_key_prefix(self) -> str:
        """取 key 前校准前缀:后端配置与当前前缀不一致时只做一次迁移。"""
        prefix = resolve_session_key_prefix()
        if prefix != self._key_prefix:
            self._rename_derived_keys(self._key_prefix, prefix)
        return self._key_prefix

    def _render_key(self, internal_id: str, generation: int) -> str:
        key = f"{self._key_prefix}-{internal_id}"
        return f"{key}-g{generation}" if generation > 0 else key

    def _get_internal_id(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
    ) -> str:
        """生成统一的内部会话 ID(群 session 固定共享,不按配置切换)。

        QQTPC 驱动的协议(OneBot v11 / Milky / icqq)统一归到业务会话域
        ``qqtpc``:同一 QQ 群/人跨协议就是同一会话,session_key/generation 自然
        续用。真实 adapter 不进 internal_id——它在运行时解析,不落库。
        QQ 官方与其它平台不归一,internal_id 仍带自己的 adapter 名。

        注:adapter_name / user_id / group_id 假定不含 '+';真实 adapter 名经
        get_adapter_name() 规整后均为 [a-z0-9],平台 user_id 多为数字串。
        """
        domain = session_domain(adapter_name)
        if is_private:
            return f"{domain}+private+{user_id}"
        # 群 session 固定共享(旧 per-user 分支已删):不再按人细分。
        return f"{domain}+group+{group_id or 'unknown'}"

    def get_delivery_generation(
        self, adapter_name: str, is_private: bool, user_id: str, group_id: str | None = None
    ) -> int:
        """消息入场时取交付代际；上游压缩轮换 session key 不改变此值。"""
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        return int(self._generation.get(internal_id, 0))

    def get_session_key(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
    ) -> str:
        """获取或创建当前后端的 session key,通过 X-Hermes-Session-Id 头送给上游。"""
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        # 登记组件,供 T1 空闲重置反查(internal_id → clear / inflight key 都需要)
        self._components[internal_id] = (adapter_name, is_private, user_id, group_id)
        self._ensure_key_prefix()
        cached = self._cache.get(internal_id)
        if cached is not None:
            return cached

        gen = self._generation.get(internal_id, 0)
        session_key = self._render_key(internal_id, gen)

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
            # 轮换后的新会话没有自己的活动记录:先关闭闲置动作资格。真实 chat 里
            # 发生轮换时,调用方收口的 mark_activity 会随即重新打开;保活/定时
            # 路径上的轮换则就此失效,旧循环不会挂到新 key 上继续跑。
            self._persist(internal_id)
            self._set_idle_state(
                internal_id, self._last_activity.get(internal_id, 0.0), armed=False
            )
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
        # 手动 /clear 也是 clear 成功:计时归零 + 关闭闲置动作资格,新会话要等
        # 下一次真实活动才重新参与 clear／compress／keepalive。
        self._set_idle_state(internal_id, time.time(), armed=False)
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

    # --- 闲置动作状态(最后活动 + 资格 + 上次保活,持久化)---

    def _set_idle_state(self, internal_id: str, last_activity: float, armed: bool) -> None:
        """内存与库一起更新(状态一变就落库,重启原样读回)。

        没有 key 映射的会话不会出现在 ``idle_snapshot`` 里,库里也不建行;
        写失败只降级为内存态(存储层自己记日志),不影响本轮行为。
        资格关闭时同时作废旧保活时间,新 key 不继承上一代的循环节奏。
        """
        self._activity_dirty.discard(internal_id)
        if not armed:
            self._keepalive_last[internal_id] = 0.0
        keepalive_last = self._keepalive_last.get(internal_id, 0.0)
        self._last_activity[internal_id] = last_activity
        self._idle_armed[internal_id] = armed
        if self._store is not None:
            self._store.set_idle_state(
                internal_id, last_activity, armed, keepalive_last=keepalive_last
            )

    def is_idle_armed(self, internal_id: str) -> bool:
        """该会话是否具备闲置动作资格(clear/compress/keepalive)。"""
        return bool(self._idle_armed.get(internal_id, False))

    def is_idle_paused(self, internal_id: str) -> bool:
        """该会话是否被 /delay 暂停 clear；compress/keepalive 不读此标记。"""
        return internal_id in self._idle_paused

    def keepalive_last(self, internal_id: str) -> float:
        """上次成功保活时刻;无记录返回 0。"""
        return float(self._keepalive_last.get(internal_id, 0.0))

    def mark_keepalive(self, internal_id: str, session_key: str, *, now: float) -> bool:
        """按当前 key 推进保活时间,拒绝把迟到结果写进换代后的会话。"""
        if not self.is_idle_armed(internal_id) or self._cache.get(internal_id) != session_key:
            return False
        self._keepalive_last[internal_id] = now
        if self._store is not None:
            self._store.set_keepalive_last(internal_id, now)
        return True

    def _flush_activity_states(self) -> None:
        """把本批活动进展用一个事务写盘;没有待写项时零开销。"""
        if not self._activity_dirty:
            return
        rows = [
            (
                internal_id,
                self._last_activity[internal_id],
                self.is_idle_armed(internal_id),
                self.keepalive_last(internal_id),
            )
            for internal_id in sorted(self._activity_dirty)
            if internal_id in self._last_activity
        ]
        if self._store is not None:
            self._store.set_idle_states_batch(rows)
        self._activity_dirty.clear()
        self._activity_last_flush = time.monotonic()

    def _record_activity(self, internal_id: str, now: float, *, force: bool) -> None:
        """内存即时更新,持久化按全局 10 秒窗口合批;关键边界强制冲刷。"""
        self._last_activity[internal_id] = now
        self._idle_armed[internal_id] = True
        self._activity_dirty.add(internal_id)
        if force or time.monotonic() - self._activity_last_flush >= ACTIVITY_FLUSH_INTERVAL_SEC:
            self._flush_activity_states()

    def mark_activity(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
        *,
        now: float | None = None,
        force: bool = True,
    ) -> None:
        """登记一次真实活动并**打开**闲置动作资格。

        调用点:正常 chat 开始、有效进展与收尾。``force=True`` 立即冲刷本批,
        高频进展传 False,由全局 10 秒窗口合批;``now`` 供离线/测试注入。
        """
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        self._record_activity(
            internal_id, now if now is not None else time.time(), force=force
        )

    def mark_activity_group(self, adapter_name: str, group_id: str) -> None:
        """群级活动标记(push_message 用):群 session 固定共享,internal_id 即
        会话域+group+group_id 唯一形态(QQTPC 协议体现为 qqtpc+group+…)。

        真实 push 成功算一次允许计时的活动:刷新时间并打开资格。私聊 push 在
        调用方就提前返回(不进这里),不建立活动基准。
        """
        internal_id = self._get_internal_id(adapter_name, False, "", group_id)
        self._record_activity(internal_id, time.time(), force=True)

    def disarm_idle(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None = None,
        *,
        now: float | None = None,
    ) -> None:
        """重置空闲计时并**关闭**闲置动作资格(clear／compress 成功收口用)。

        关闭后 clear / compress / keepalive 三者都不再对这个会话动作,直到下一次
        真实活动(mark_activity)重新打开——重启后同样按库里的关闭状态恢复。
        """
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        self._set_idle_state(
            internal_id, now if now is not None else time.time(), armed=False
        )

    def set_idle_paused(
        self,
        adapter_name: str,
        is_private: bool,
        user_id: str,
        group_id: str | None,
        paused: bool,
        *,
        now: float | None = None,
    ) -> str:
        """/delay(paused=True)只暂停 clear;/resume(paused=False)恢复并重置计时。

        compress/keepalive 在暂停期间继续执行。``/resume`` 只把计时起点挪到现在,
        **不**改变资格:被 clear／compress 关掉资格的会话不能靠 /resume 偷偷变成 armed。
        返回 internal_id 供日志用。持久化到 session_keys.db。
        """
        internal_id = self._get_internal_id(adapter_name, is_private, user_id, group_id)
        if paused:
            self._idle_paused.add(internal_id)
        else:
            self._idle_paused.discard(internal_id)
            if internal_id in self._last_activity:
                self._set_idle_state(
                    internal_id,
                    now if now is not None else time.time(),
                    armed=self.is_idle_armed(internal_id),
                )
        if self._store is not None:
            self._store.set_idle_paused(internal_id, paused)
        return internal_id

    def idle_snapshot(
        self,
    ) -> list[tuple[str, tuple[str, bool, str, str | None], str, float]]:
        """全部**具备资格**且可参与闲置动作的会话快照(每会话一条)。

        返回 ``(internal_id, 真实组件, 当前 session key, 最后活动时刻)``:

        - 只挑 ``idle_armed`` 为真的会话:clear／compress 成功后资格立即关闭,
          重启后也按库里的关闭状态恢复,因此不会每拍重复清场、也不会替失活
          会话保活或压缩;
        - 没有活动记录的会话(旧库默认行)不出现,不做任何播种;
        - ``/delay`` 暂停中的会话仍保留在快照里，由扫描器只跳过 clear；
          compress/keepalive 继续按自己的时间基准执行；
        - 组件必须是**真实 adapter**;业务域会话的真实 adapter 尚未随连接/路由
          恢复时不出条目,下一拍再试(等待语义,不猜旧协议)。

        计时判定交给调用方(统一闲置扫描器):它要给 clear / compress / keepalive
        各自的时间基准,不能在这里按单一阈值过滤。
        """
        out: list[tuple[str, tuple[str, bool, str, str | None], str, float]] = []
        for internal_id, last in self._last_activity.items():
            if not self.is_idle_armed(internal_id):
                continue
            comp = self._components_for(internal_id)
            if comp is None:
                continue
            key = self._cache.get(internal_id)
            if not key:
                continue
            out.append((internal_id, comp, key, last))
        return out

    def idle_expired(
        self, idle_sec: int, now: float | None = None
    ) -> list[tuple[str, tuple[str, bool, str, str | None]]]:
        """返回超过 idle_sec 未活动的 ``(internal_id, components)`` 列表(旧 T1 读法)。

        只看「有活动记录」的会话,不看资格:保留给既有调用方做组件/域解析回归;
        统一闲置扫描器一律走 ``idle_snapshot``,按资格与各自阈值判定。
        """
        now = now if now is not None else time.time()
        out: list[tuple[str, tuple[str, bool, str, str | None]]] = []
        for internal_id, last in self._last_activity.items():
            if internal_id in self._idle_paused or now - last <= idle_sec:
                continue
            comp = self._components_for(internal_id)
            if comp is not None:
                out.append((internal_id, comp))
        return out

    def current_session_key(self, internal_id: str) -> str | None:
        """按 internal_id 取当前 session key(定时动作换代后重新登记状态用)。"""
        return self._cache.get(internal_id)

    def clear_internal(self, internal_id: str, *, now: float | None = None) -> None:
        """按 internal_id 直接重置会话(T1 自动清用),不发任何用户可见提示。

        与 clear_session 同逻辑,但 key 渲染直接作用在传入的 internal_id 上,
        不经组件反解重算——组件反解出来的会话重算可能落到另一个 internal_id,导致
        原 id 的 generation 递增不持久化。
        换代后空闲计时归零并关闭资格:新会话要等下一次真实活动才重新参与闲置动作。"""
        self._cache.pop(internal_id, None)
        self._push_watermark.pop(internal_id, None)  # 转录清空,增量推送水位归零
        self._ensure_key_prefix()
        gen = self._generation.get(internal_id, 0) + 1
        self._generation[internal_id] = gen
        self._cache[internal_id] = self._render_key(internal_id, gen)
        self._persist(internal_id)
        # 组件必须是真实 adapter:业务域会话按当前连接/路由解析,解析不出来就
        # 不落交付栅栏(不能拿 qqtpc 去 fence,台账 scope 永远匹配不上)。
        comp = self._components_for(internal_id)
        if comp is not None:
            self._fence_delivery_generation(*comp)
        self._set_idle_state(
            internal_id, now if now is not None else time.time(), armed=False
        )
        logger.info(f"[SESSION] T1 空闲自动重置: {internal_id} (generation={gen})")

    def known_sessions(self) -> list[tuple[str, str, bool, str, str | None, str]]:
        """全量已知会话快照(本部署补丁:投递轮询用;语义同 iter_sessions)。

        返回 (internal_id, adapter_name, is_private, user_id, group_id, session_key)。
        adapter 位置永远是**真实 adapter**:业务域会话由当前连接/路由解析,解析
        不出来(重启后 bot 还没连上/路由还没恢复)就不出条目,交给调用方按现有
        等待/跳过语义处理——绝不把 ``qqtpc`` 当 adapter 交出去。
        """
        out: list[tuple[str, str, bool, str, str | None, str]] = []
        for internal_id, session_key in self._cache.items():
            comp = self._components_for(internal_id)
            if comp is None:
                continue
            adapter, is_private, user_id, group_id = comp
            out.append((internal_id, adapter, is_private, user_id, group_id, session_key))
        return out


# 全局会话管理器
session_manager = SessionManager()


def resolve_transport_for_row(
    adapter_name: str, *, is_private: bool, user_id, group_id
) -> str:
    """台账/交付行的**发送** adapter（QQTPC 域内跨协议恢复的唯一判定点）。

    真实 adapter 不落库：台账里的 ``adapter`` 只是「当时」的驱动。恢复投递时：

    * 历史驱动仍是当前事实（有在线连接/可信路由）→ 原样用它；
    * 同业务会话域内历史驱动已下线、当前只有另一个驱动在线 → 用当前驱动
      （协议切换后补投的唯一正确出口）；
    * 跨域（官方 QQ 等，``session_domain`` 不同）或多驱动歧义 → 保持历史值，
      宁可不投也不混协议。

    本函数只读运行时事实，不写库、不新增持久字段。
    """
    resolved = session_manager.resolve_adapter_for_domain(
        session_domain(adapter_name), is_private, user_id, group_id
    )
    if (
        resolved
        and resolved != adapter_name
        and session_domain(resolved) == session_domain(adapter_name)
    ):
        return resolved
    return adapter_name
