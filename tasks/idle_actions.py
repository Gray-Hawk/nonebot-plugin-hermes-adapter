"""统一闲置动作扫描器:clear / compress / keepalive(本部署新增)。

一拍 60 秒扫一遍**具备闲置动作资格**的会话;每个会话各自计时,每拍每会话最多
执行一个动作,固定优先级 ``clear > compress > keepalive``。

- ``clear``:旧 T1 的静默重置,保持 ``session_idle_reset_sec`` 与
  ``SessionManager.clear_internal()`` 行为;
- ``compress``:复用 ``HermesClient.compress_session()`` 与网关
  ``session-compress`` 插件,轮换后采纳新 session id;
- ``keepalive``:复用 ``HermesClient.chat()`` 的 runs 通道,正文严格一个普通
  空格、通知只含当前时间、指令要求不调用工具且只输出两行 ``§``;结果验收后
  丢弃,不发 QQ、不写用户消息、不刷新用户活动基准。

「最后活动时刻 + 资格 + 上次保活」由 ``SessionManager`` 持久化在 ``session_keys.db``:状态一变
即落库、重启原样读回。资格关闭(clear／compress 成功、上游压缩轮换)后三个动作全部
跳过,直到下一次真实 chat／允许计时的 push 重新打开——不靠内存时间门槛去猜。
``/delay`` 只暂停 clear；inflight / active lease 保护由三个动作共用。与正常 chat、压缩在
``HermesClient`` 入口按 session key 串行。单个会话失败只影响它自己这一拍。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from nonebot import logger, require

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..core.chat_config import ChatConfig, load_chat_config
from ..core.inflight import make_inflight_key
from ..core.log_throttle import throttled_info
from ..core.prompt_builder import render_system_notice_region
from ..core.session import SessionManager, session_manager

# 检查节奏固定 60s:三个动作都是秒级可配,但「闲置多久做一次」的语义不需要
# 跟着细;最坏晚 60s 动作,对缓存保活与空闲重置都无实质影响。
SWEEP_SEC = 60

ACTION_CLEAR = "clear"
ACTION_COMPRESS = "compress"
ACTION_KEEPALIVE = "keepalive"
# 固定优先级(不提供配置项):清场最彻底、压缩次之、保活最轻。
ACTIONS: tuple[str, ...] = (ACTION_CLEAR, ACTION_COMPRESS, ACTION_KEEPALIVE)

# 失败冷却(同会话同动作):一次失败后固定窗口内不重试,避免每拍反复撞同一
# 个坏会话。只记一个截止时间,不做状态机。
FAIL_COOLDOWN_SEC = 300

# 缓存保活轮的请求形态(已用真实 runs 证明):正文严格一个普通空格,模型只回
# 两行分节符即"静默",结果不进入任何投递链。
KEEPALIVE_TEXT = " "
KEEPALIVE_SYSTEM_PROMPT = (
    "这是插件自动发出的缓存保活轮：没有用户消息，不需要回应任何人。"
    "不要调用任何工具。只输出两行分节符：第一行 §，第二行 §。"
)


def keepalive_supported() -> bool:
    """保活只在 runs 通道成立。

    runs 才有 ``hermes_context`` 通知注入(``context_notices``);completions /
    AstrBot 会把空格当普通用户消息、通知也到不了模型,不在这些通道伪造成功。
    """
    from ..config import plugin_config

    return str(getattr(plugin_config, "hermes_api_interface", "")) == "runs"


def keepalive_notice(now: float) -> str:
    """保活通知:只有当前时间的一次性系统通知,不带任何用户正文。"""
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
    return render_system_notice_region(kind="缓存保活", summary=f"当前时间 {stamp}")


# 结果只验收并丢弃:这些标记说明这一轮根本没跑成,记失败冷却,不当成功推进循环。
# ``is_context_overflow``:上游顶到上下文上限时同样以 200 回一段提示,不是保活成功;
# 若还发生轮换,``adopt_session_key`` 已关闭资格,旧循环不会挂到新 key 上继续跑。
_KEEPALIVE_FAILURE_FLAGS = (
    "is_transport_error",
    "is_local_refusal",
    "is_rate_limited",
    "is_forbidden_error",
    "is_outage_error",
    "is_persistence_error",
    "is_context_overflow",
    "is_interrupted",
    "is_run_detached",
)


def _keepalive_failure_reason(result: Any) -> str:
    for flag in _KEEPALIVE_FAILURE_FLAGS:
        if getattr(result, flag, False):
            return flag
    return ""


@dataclass(frozen=True)
class IdleActionRecord:
    """这一拍对一个会话执行的唯一动作。"""

    internal_id: str
    action: str
    components: tuple[str, bool, str, str | None]
    session_key: str


class IdleActionScanner:
    """统一闲置扫描器。

    外部边界(chat / compress / inflight / active_sessions / target)全部可注入,
    离线测试用真实 ``SessionManager`` + 假外部依赖即可覆盖全部决策与副作用。
    未注入的依赖在调用时从运行时单例解析,插件装配前后都不会提前绑定。
    """

    def __init__(
        self,
        *,
        manager: SessionManager | None = None,
        config_loader: Callable[[], ChatConfig] = load_chat_config,
        now: Callable[[], float] = time.time,
        chat: Callable[..., Any] | None = None,
        compress: Callable[..., Any] | None = None,
        inflight: Any = None,
        active_sessions: Any = None,
        target_resolver: Callable[..., Any] | None = None,
    ) -> None:
        self._manager = manager
        self._config_loader = config_loader
        self._now = now
        self._chat = chat
        self._compress = compress
        self._inflight = inflight
        self._active_sessions = active_sessions
        self._target_resolver = target_resolver
        # 失败冷却是进程级重试策略;活动、资格与保活节奏都由 SessionManager 持久化。
        self._fail_until: dict[tuple[str, str], float] = {}

    # --- 运行时依赖(未注入才解析) ---------------------------------------
    @property
    def _sm(self) -> SessionManager:
        return self._manager if self._manager is not None else session_manager

    def _runtime_inflight(self):
        return self._inflight if self._inflight is not None else _mcp.inflight

    def _runtime_sessions(self):
        return self._active_sessions if self._active_sessions is not None else _mcp.active_sessions

    def _resolve_target(self, comp: tuple[str, bool, str, str | None]):
        adapter, is_private, _user_id, group_id = comp
        if self._target_resolver is not None:
            return self._target_resolver(adapter, is_private, group_id)
        from ..core.routing import resolve_target

        return resolve_target(adapter, is_private, group_id)

    # --- 决策 -------------------------------------------------------------
    def due_action(
        self,
        internal_id: str,
        last_activity: float,
        cfg: ChatConfig,
        now: float,
    ) -> str | None:
        """这一拍该会话唯一的动作(固定优先级),没有到点返回 None。

        资格(``idle_armed``)已经在 ``idle_snapshot`` 里过滤过:clear／compress
        成功或上游轮换关掉资格后,三个动作全部跳过,直到下一次真实活动。
        """
        for action in ACTIONS:
            if now < self._fail_until.get((internal_id, action), 0.0):
                continue  # 冷却期内不重试同一动作
            if action == ACTION_CLEAR:
                # /delay 只暂停 clear；compress/keepalive 在暂停期间继续。
                if self._sm.is_idle_paused(internal_id):
                    continue
                # 旧 T1 语义:严格超过阈值才清。
                if cfg.session_idle_reset_sec > 0 and now - last_activity > cfg.session_idle_reset_sec:
                    return action
            elif action == ACTION_COMPRESS:
                secs = cfg.session_auto_compress_sec
                # 一次性由持久化资格保证:成功后资格关闭,真实活动才重新打开。
                if secs > 0 and now - last_activity >= secs:
                    return action
            elif action == ACTION_KEEPALIVE:
                if not keepalive_supported():
                    continue
                secs = cfg.session_cache_keepalive_sec
                if secs <= 0:
                    continue
                # 自己的上次执行时间与真实活动取较晚者:用户一开口就重新起算。
                base = max(self._sm.keepalive_last(internal_id), last_activity)
                if now - base >= secs:
                    return action
        return None

    # --- 保护 -------------------------------------------------------------
    def _blocked_reason(self, comp: tuple[str, bool, str, str | None]) -> str:
        adapter, is_private, user_id, group_id = comp
        inflight = self._runtime_inflight()
        if inflight is not None and inflight.is_inflight(
            make_inflight_key(adapter, is_private, user_id, group_id)
        ):
            return "turn in-flight"
        sessions = self._runtime_sessions()
        if not is_private and group_id and sessions is not None:
            # 租约改 key 级:turn 跑期间实例可能已被 trigger() 换过。
            if sessions.lease_count(adapter, group_id) > 0:
                return "active session leased"
        return ""

    # --- 扫描 -------------------------------------------------------------
    async def sweep(self, now: float | None = None) -> list[IdleActionRecord]:
        """扫一拍:每会话最多一个动作,错误按会话隔离。

        串行执行(与巡查队列同取舍):同一拍有多个会话到点时不会一起抢网关的
        并发槽;单个慢动作只顺延本拍后续会话,不阻塞事件循环。
        """
        inflight = self._runtime_inflight()
        if inflight is None:  # 运行时未装配(启动早期/离线工具)
            return []
        now = self._now() if now is None else now
        cfg = self._config_loader()
        done: list[IdleActionRecord] = []
        for internal_id, comp, session_key, last_activity in self._sm.idle_snapshot():
            action = self.due_action(internal_id, last_activity, cfg, now)
            if action is None:
                continue
            blocked = self._blocked_reason(comp)
            if blocked:
                logger.debug(f"[HERMES 闲置] skip {internal_id}/{action}: {blocked}")
                continue
            record = IdleActionRecord(internal_id, action, comp, session_key)
            try:
                ok = await self._run(action, record, now)
            except Exception:  # noqa: BLE001 - 单会话失败不得拖垮其他会话
                logger.warning(f"[HERMES 闲置] {action} 失败: {internal_id}", exc_info=True)
                ok = False
            self._note(action, record, now, ok)
            done.append(record)
        return done

    async def _run(self, action: str, record: IdleActionRecord, now: float) -> bool:
        if action == ACTION_CLEAR:
            # 旧 T1 语义:静默清会话(等价 /clear 的 session 效果,但不发提示)。
            # ``clear_internal`` 自己把计时归零并关闭资格:本拍之后不再每拍重复清。
            self._sm.clear_internal(record.internal_id, now=now)
            throttled_info(
                f"gate.t1_clear:{record.internal_id}",
                f"[HERMES T1] 闲置重置已清会话 {record.internal_id}",
            )
            return True
        if action == ACTION_COMPRESS:
            return await self._run_compress(record, now)
        if action == ACTION_KEEPALIVE:
            return await self._run_keepalive(record, now)
        raise AssertionError(f"未实现的动作: {action}")

    async def _run_compress(self, record: IdleActionRecord, now: float) -> bool:
        """调现有压缩客户端;轮换就采纳新 id,成功后关闭闲置动作资格。

        不另写压缩器、不读 SessionDB 历史:网关 ``session-compress`` 跑的就是
        messaging ``/compress`` 的同一实现,这里只负责触发与采纳。
        """
        compress_fn = self._compress
        if compress_fn is None:
            from ..core.hermes_client import hermes_client

            compress_fn = hermes_client.compress_session
        result = await compress_fn(
            tgt=self._resolve_target(record.components),
            session_key=record.session_key,
        )
        if not result.get("ok"):
            # 404/409/网络错误:只记这一条,不改映射、不换会话、不改资格。
            logger.info(
                f"[HERMES 闲置] 自动压缩未完成: {record.internal_id} "
                f"code={result.get('code') or '-'} status={result.get('status') or '-'}"
            )
            return False
        # 200 视为本空闲期已处理(含 nothing_to_do/aborted):先按需采纳新会话 id。
        new_key = str(result.get("session_id") or "")
        if result.get("rotated") and new_key and new_key != record.session_key:
            adopted = self._sm.adopt_session_key(record.session_key, new_key)
            logger.info(
                f"[HERMES 闲置] 自动压缩轮换采纳: {record.session_key} -> {new_key} "
                f"adopted={adopted}"
            )
        # 压缩成功:计时归零并关闭资格——旧空闲时间不会在下一拍误判 clear,新会话也
        # 不会自动继承保活/压缩循环;等下一次真实活动再重新开放。
        self._sm.disarm_idle(*record.components, now=now)
        return True

    async def _run_keepalive(self, record: IdleActionRecord, now: float) -> bool:
        """发一轮 runs 空格保活,只验收结果、不投递。

        正文严格一个普通空格、通知只含当前时间、指令要求不调用工具;模型回的两
        行 § 属于静默,结果直接丢弃——不发 QQ、不写用户消息、不刷新活动基准。
        """
        chat_fn = self._chat
        if chat_fn is None:
            from ..core.hermes_client import hermes_client

            chat_fn = hermes_client.chat
        adapter, is_private, user_id, group_id = record.components
        result = await chat_fn(
            text=KEEPALIVE_TEXT,
            session_key=record.session_key,
            user_id=user_id,
            group_id=group_id,
            adapter_name=adapter,
            is_private=is_private,
            mode="passive",
            system_prompt=KEEPALIVE_SYSTEM_PROMPT,
            context_notices=keepalive_notice(now),
            target=self._resolve_target(record.components),
        )
        # 上游轮换(completions 路)时跟上新 id;runs 路由服务端透明处理。
        # 采纳会关闭资格:换过代的会话不再继承旧循环,等下一次真实活动重新开放。
        new_key = str(getattr(result, "effective_session_key", "") or "")
        if new_key and new_key != record.session_key:
            self._sm.adopt_session_key(record.session_key, new_key)
        reason = _keepalive_failure_reason(result)
        if reason:
            logger.info(f"[HERMES 闲置] 保活未完成: {record.internal_id} reason={reason}")
            return False
        logger.debug(
            f"[HERMES 闲置] 保活完成: {record.internal_id} "
            f"out={len(str(getattr(result, 'raw_text', '') or ''))}"
        )
        return True

    def _note(self, action: str, record: IdleActionRecord, now: float, ok: bool) -> None:
        key = (record.internal_id, action)
        if not ok:
            self._fail_until[key] = now + FAIL_COOLDOWN_SEC
            return
        self._fail_until.pop(key, None)
        if action == ACTION_KEEPALIVE:
            # 只更新自己的最近执行时间,绝不碰用户活动基准;按当前 key 验证,
            # 防止迟到的保活结果写进已经 clear／compress 换代的新会话。
            self._sm.mark_keepalive(record.internal_id, record.session_key, now=now)


# 进程级扫描器:tick 与后续观测共用同一份动作计时。
idle_action_scanner = IdleActionScanner()


def register_idle_actions() -> None:
    """注册统一闲置扫描 tick(固定 60s,每拍现读 chat_config)。"""

    @scheduler.scheduled_job("interval", seconds=SWEEP_SEC, id="hermes_idle_actions")
    async def _sweep() -> None:
        await idle_action_scanner.sweep()

    initial = load_chat_config()
    enabled = [
        f"{name}={secs}s"
        for name, secs in (
            ("clear", initial.session_idle_reset_sec),
            ("compress", initial.session_auto_compress_sec),
            ("keepalive", initial.session_cache_keepalive_sec if keepalive_supported() else 0),
        )
        if secs > 0
    ]
    if initial.session_cache_keepalive_sec > 0 and not keepalive_supported():
        logger.info("[HERMES] 缓存保活需要 runs 通道,当前后端不是 runs:保活不启用")
    logger.info(
        "[HERMES] 统一闲置扫描 tick 已注册"
        f"({('启用: ' + ', '.join(enabled)) if enabled else '当前关闭:0=跳过'}), 每 {SWEEP_SEC}s 检查"
    )
