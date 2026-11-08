"""
命令处理器

/clear, /help, /ping, /hermes-status 等内置命令。
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import nonebot_plugin_alconna as alconna
from nonebot import logger, on_command
from nonebot.adapters import Bot, Event
from nonebot.matcher import Matcher

from .. import mcp as _mcp
from ..config import hermes_control_plane_enabled, plugin_config
from ..core.hermes_client import hermes_client
from ..core.log_throttle import throttled_info
from ..core.approvals import pending_count as approvals_pending_count
from ..core.approvals import peek_oldest as approvals_peek_oldest
from ..core.approvals import acknowledge as approvals_acknowledge
from ..core.approvals import answer_lock as approvals_answer_lock
from ..core.routing import resolve_target
from ..core.run_tracker import make_scope_key
from ..core.sticker_library import default_sticker_library
from ..core.config_reload import reload_runtime_config
from ..core.chat_config import load_chat_config
from ..core.misc_config import load_misc_config
from ..core.user_lists import load_user_lists
from ..core.session import session_manager
from ..utils import check_isolation, get_adapter_name
from .message_perception import _claim_event_db_id

# 本部署补丁: 指令寻址前缀(多 bot 共群指令域区分)。
# 注册期定型(下面全部 on_command 在插件加载期执行),改名需重启 nb 生效。
_raw_bot_name = load_misc_config().bot_name.strip()
_name_parts = _raw_bot_name.split()
CMD_PREFIX = _name_parts[0] if _name_parts else ""
if len(_name_parts) > 1:
    logger.warning(f"[HERMES] hermes_bot_name 含空白,只取首段 {CMD_PREFIX!r} 作指令前缀")


def _cmd(name: str) -> str:
    """拼带寻址前缀的命令名。空前缀 = 原名(旧行为直连)。"""
    return f"{CMD_PREFIX} {name}" if CMD_PREFIX else name


def _display_cmd(name: str) -> str:
    """回执与 /help 菜单里展示的指令形态(带斜杠与寻址前缀)。"""
    return f"/{_cmd(name)}"


def _tail_args(raw: str) -> str:
    """从命令消息纯文本剥余文(命令头一段 + 可能存在的寻址前缀一段)。

    带前缀时 plaintext 形如 "/normi reasoning high",余文从第三段起;
    空前缀照旧 "/reasoning high" 余文从第二段起。无多余文返回空串。
    """
    idx = 2 if CMD_PREFIX else 1
    parts = raw.split(maxsplit=idx)
    return parts[idx].strip() if len(parts) > idx else ""


# --- /clear ---
clear_command = on_command(_cmd("clear"), force_whitespace=True, priority=88, block=True)


@clear_command.handle()
async def handle_clear(bot: Bot, event: Event, matcher: Matcher):
    """重置当前会话"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    adapter_name = get_adapter_name(target)
    # 取不到 user_id 不折叠 "user" 公共桶,命令不执行。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] /clear 取不到 user_id,不执行 (adapter={adapter_name})")
        return
    group_id = None if target.private else target.id

    cutoff_msg_id = None
    if not target.private:
        # perception 已先入库；领取命令自身的真实 db id 作瞬态截断水位。
        event_message_id = getattr(event, "message_id", None)
        if isinstance(event_message_id, int):
            cutoff_msg_id = _claim_event_db_id(adapter_name, event_message_id)
        if cutoff_msg_id is None:
            logger.warning(f"[HERMES] /clear 未取到命令 db id，本轮不设置前文截断 (adapter={adapter_name})")

    old_session_key = session_manager.get_session_key(
        adapter_name, target.private, user_id, group_id
    )
    session_manager.clear_session(
        adapter_name=adapter_name,
        is_private=target.private,
        user_id=user_id,
        group_id=group_id,
        cutoff_msg_id=cutoff_msg_id,
    )
    if _mcp.background_report_outbox is not None:
        # 仅人工 /clear 终止旧会话汇报；T1 自动换代不经过这里，仍会续投。
        _mcp.background_report_outbox.cancel_session(old_session_key, now=time.time())

    reply = alconna.UniMessage("✅ 会话已重置，开始新的对话。")
    if not target.private:
        reply = alconna.UniMessage([alconna.At("user", user_id), "\n"]) + reply

    await reply.send(target=target, bot=bot)


# --- /delay、/resume(T1 空闲重置超控,本部署新增) ---
delay_command = on_command(_cmd("delay"), force_whitespace=True, priority=88, block=True)
resume_command = on_command(_cmd("resume"), force_whitespace=True, priority=88, block=True)


@delay_command.handle()
async def handle_delay(bot: Bot, event: Event, matcher: Matcher):
    """暂停当前会话的 T1 空闲自动重置"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    if load_chat_config().session_idle_reset_sec <= 0:
        await alconna.UniMessage("ℹ️ 空闲自动重置(T1)当前未启用。").send(target=target, bot=bot)
        return

    adapter_name = get_adapter_name(target)
    # 同上,匿名事件不执行。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] /delay 取不到 user_id,不执行 (adapter={adapter_name})")
        return
    group_id = None if target.private else target.id
    internal_id = session_manager.set_idle_paused(adapter_name, target.private, user_id, group_id, True)
    logger.info(f"[HERMES] /delay: T1 计时已暂停 ({internal_id})")
    await alconna.UniMessage(f"⏸️ 已暂停本会话的空闲自动重置,{_display_cmd('resume')} 恢复。").send(target=target, bot=bot)


@resume_command.handle()
async def handle_resume(bot: Bot, event: Event, matcher: Matcher):
    """恢复当前会话的 T1 空闲自动重置,并重置计时"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    adapter_name = get_adapter_name(target)
    # 同上,匿名事件不执行。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] /resume 取不到 user_id,不执行 (adapter={adapter_name})")
        return
    group_id = None if target.private else target.id
    internal_id = session_manager.set_idle_paused(adapter_name, target.private, user_id, group_id, False)
    logger.info(f"[HERMES] /resume: T1 计时已恢复并重置 ({internal_id})")
    await alconna.UniMessage("▶️ 已恢复空闲自动重置,计时重新开始。").send(target=target, bot=bot)


# --- /ping ---
ping_command = on_command(_cmd("ping"), force_whitespace=True, priority=88, block=True)


def build_ping_message(ok: bool) -> str:
    """/ping 的回复。

    这条命令只过 check_isolation,任何被允许的用户都能打,所以只回报连接结果,
    不附带内部接入点标签或路由信息。
    """
    if ok:
        return "🏓 pong! Hermes Agent 连接正常。"
    return "⚠️ 无法连接到 Hermes Agent，请检查 Gateway 是否正在运行。"


async def probe_current_endpoint(adapter_name: str, is_private: bool, group_id: str | None) -> bool:
    """只探当前会话自己的接入点 —— 用户问的是"我这儿能不能用"。"""
    return await hermes_client.health_check(resolve_target(adapter_name, is_private, group_id))


@ping_command.handle()
async def handle_ping(bot: Bot, event: Event, matcher: Matcher):
    """检查 Hermes 连接状态(当前会话自己的接入点)"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    adapter_name = get_adapter_name(target)
    group_id = None if target.private else target.id
    ok = await probe_current_endpoint(adapter_name, target.private, group_id)

    await alconna.UniMessage(build_ping_message(ok)).send(target=target, bot=bot)


# --- /help ---
help_command = on_command(_cmd("help"), aliases={_cmd("帮助")}, force_whitespace=True, priority=88, block=True)

_HELP_MENU_SEP = "---"


def load_help_menu_decor(path: str) -> tuple[str | None, str | None]:
    """读取 /help 自定义头尾(只自定义头尾,独立文件)。

    返回 (header, footer);任一为 None 表示该段回退内置。文件不存在/读取失败/
    内容为空时整体回退并 WARN,/help 不挂。文件现读现发,保存即生效。
    格式:第一行单独 --- 分隔;之前是头,之后是尾;没有 --- 则整个文件只当头。
    注意头尾内容里不能再出现单独一行的 ---(会被当分隔符),
    文件里有多行 --- 时只按第一行切并 WARN 提醒。
    """
    if not path:
        return None, None
    try:
        raw = Path(path).expanduser().read_text(encoding="utf-8")
    except Exception as e:
        logger.warning(f"[HERMES] /help 菜单文件读取失败({path}): {e};回退内置菜单")
        return None, None
    lines = raw.splitlines()
    sep_lines = [i for i, line in enumerate(lines) if line.strip() == _HELP_MENU_SEP]
    if len(sep_lines) > 1:
        logger.warning(
            f"[HERMES] /help 菜单文件含 {len(sep_lines)} 行 {_HELP_MENU_SEP}({path});"
            "只按第一行切分,其余当尾段内容——若不符合预期请整改文件"
        )
    sep_at = sep_lines[0] if sep_lines else None
    if sep_at is None:
        header, footer = raw, ""
    else:
        header = "\n".join(lines[:sep_at])
        footer = "\n".join(lines[sep_at + 1 :])
    header = header.strip("\n")
    footer = footer.strip("\n")
    if not header.strip() and not footer.strip():
        logger.warning(f"[HERMES] /help 菜单文件为空({path});回退内置菜单")
        return None, None
    return (header or None), (footer or None)


def build_help_text(
    is_private: bool,
    is_admin: bool,
    *,
    header: str | None = None,
    footer: str | None = None,
    cmd_prefix: str = "",
) -> str:
    """拼装 /help 正文。与 handler 分开以便离线冒烟。

    默认形态保持旧行为一字不差;自定义只换头尾,中间命令清单仍按功能开关
    动态生成(T1 才列 /delay /resume,管理员才列 /hermes-status)。
    cmd_prefix 非空时每条命令带寻址前缀(如 /normi clear)。
    """

    def _cl(cmd: str) -> str:
        return f"/{cmd_prefix} {cmd}" if cmd_prefix else f"/{cmd}"

    lines = [
        f"{_cl('clear')} - 重置对话",
        f"{_cl('ping')} - 检查连接状态",
        f"{_cl('help')} - 显示本帮助",
    ]
    if load_chat_config().session_idle_reset_sec > 0:
        # T1 启用时才暴露超控命令,免得用户对着不存在的功能打命令
        lines.append(f"{_cl('delay')} - 暂停空闲自动重置")
        lines.append(f"{_cl('resume')} - 恢复空闲自动重置")
    if is_admin:
        # 管理员才看见运行时状态命令,普通用户视角下此命令"不存在"
        lines.append(f"{_cl('hermes-status')} - 查看插件运行时状态(管理员)")
        if hermes_control_plane_enabled():
            lines.append(f"{_cl('run-status')} - 查看当前会话在跑的 run(管理员)")
            lines.append(f"{_cl('run-stop')} - 终止当前会话在跑的 run(管理员)")
            lines.append(f"{_cl('compress')} - 手动压缩当前会话上下文(管理员)")
            lines.append(f"{_cl('reasoning')} - 查看/切换全局推理档位(管理员)")
        lines.append(f"{_cl('reload-config')} - 手动重载配置与表情库(管理员)")

    if header is not None:
        text = header.strip("\n") + "\n\n命令：\n" + "\n".join(lines)
    else:
        if is_private:
            intro = "🤖 Hermes Agent 帮助\n\n直接发送消息即可与 AI 对话。\n\n命令：\n"
        else:
            intro = "🤖 Hermes Agent 帮助\n\n@我 发送消息即可与 AI 对话。\n\n命令：\n"
        text = intro + "\n".join(lines)
    if footer is not None:
        text += "\n\n" + footer.strip("\n")
    return text


@help_command.handle()
async def handle_help(bot: Bot, event: Event, matcher: Matcher):
    """显示帮助信息"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    # 是否管理员决定要不要把 /hermes-status 暴露给当前用户
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    is_admin = _is_admin(adapter_name, user_id)

    # 菜单文件读取挪出事件循环(同步 IO 会堵住其他消息处理)
    header, footer = await asyncio.to_thread(load_help_menu_decor, load_misc_config().help_menu_file)
    help_text = build_help_text(target.private, is_admin, header=header, footer=footer, cmd_prefix=CMD_PREFIX)
    await alconna.UniMessage(help_text).send(target=target, bot=bot)


# --- /hermes-status ---
status_command = on_command(_cmd("hermes-status"), force_whitespace=True, priority=88, block=True)


@status_command.handle()
async def handle_status(bot: Bot, event: Event, matcher: Matcher):
    """打印插件 运行时状态:MCP / 活跃 session / buffer / registry。"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    # /hermes-status 暴露内部运行时(活跃群、buffer 内容、bot 路由),
    # 应限制在管理员白名单内。**隐身策略**:对非管理员完全静默,不发"未授权"
    # 提示——避免把命令存在性暴露给一般用户。空集 = deny by default。
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    admin_key = f"{adapter_name}:{user_id}"
    if not _is_admin(adapter_name, user_id):
        logger.debug(f"[HERMES] /hermes-status silent skip for {admin_key}")
        return  # block=True 已阻断后续 matcher,直接 return 即静默

    lines = build_status_lines(int(time.time() * 1000))
    await alconna.UniMessage("\n".join(lines)).send(target=target, bot=bot)


def build_status_lines(now_ms: int) -> list[str]:
    """拼装 /hermes-status 的正文。

    与 handler 分开是为了能脱离 matcher / adapter 直接测:这段代码读的全是运行时
    单例的内部状态,最容易在重构后悄悄失配(SQLite 化之后 MessageBuffer 就没有
    per-group 内存桶了)。
    """
    # MCP 状态
    mcp_line = (
        f"on @ {plugin_config.hermes_mcp_host}:{plugin_config.hermes_mcp_port}"
        if plugin_config.hermes_mcp_enabled
        else "off"
    )
    active_line = "on"

    # ActiveSessionManager:统计当前未过期 session
    active_count = 0
    active_details: list[str] = []
    if _mcp.active_sessions is not None:
        for s in _mcp.active_sessions.list():
            # 按管理器 is_alive_with_lease(TTL + key 级租约)而非 ttl_left>0:正在跑长 turn 的群
            # 带着租约,窗口虽已过 TTL 但仍在监听 —— 诊断命令把它藏起来,恰好藏掉了
            # 最需要看见的那一个。(本部署补丁: 租约改 key 级)
            if not _mcp.active_sessions.is_alive_with_lease(s.adapter, s.group_id, now_ms):
                continue
            active_count += 1
            ttl_left = max(0, (s.expires_at - now_ms) // 1000)
            held = " in-flight" if _mcp.active_sessions.lease_count(s.adapter, s.group_id) else ""
            active_details.append(f"  - {s.adapter}/{s.group_id} by {s.triggered_by} ttl={ttl_left}s{held}")

    # MessageBuffer:按 scope 统计消息数(数据在 SQLite 里,不是内存桶)
    buf_lines: list[str] = []
    buf_total_msgs = 0
    if _mcp.message_buffer is not None:
        for (adapter, scope_id), count in sorted(_mcp.message_buffer.counts_by_scope().items()):
            buf_total_msgs += count
            buf_lines.append(f"  - {adapter}/{scope_id}: {count}")

    # BotRegistry:统计已知路由
    reg_lines: list[str] = []
    reg_count = 0
    if _mcp.bot_registry is not None:
        for k in _mcp.bot_registry.known():
            reg_count += 1
            reg_lines.append(f"  - {k[0]}/{k[1]}/{k[2]}")

    lines = [
        "🔍 Hermes Plugin M1-mem 状态",
        f"MCP: {mcp_line}",
        f"active_session: {active_line}",
        f"hermes_api: {plugin_config.hermes_api_url}",
        "",
        f"📊 ActiveSessions: {active_count} 个活跃",
    ]
    lines.extend(active_details[:10])  # 最多 10 个
    if len(active_details) > 10:
        lines.append(f"  ... +{len(active_details) - 10} more")

    lines.extend(
        [
            "",
            f"💬 MessageBuffer: {buf_total_msgs} 条 / {len(buf_lines)} 个 bucket",
        ]
    )
    lines.extend(buf_lines[:5])
    if len(buf_lines) > 5:
        lines.append(f"  ... +{len(buf_lines) - 5} more")

    lines.extend(
        [
            "",
            f"🤖 BotRegistry: {reg_count} 个路由",
        ]
    )
    lines.extend(reg_lines[:5])
    if len(reg_lines) > 5:
        lines.append(f"  ... +{len(reg_lines) - 5} more")

    return lines

# --- /run-status、/run-stop(阶段3a/3b,管理员;群内私聊均可用,非管理员隐身) ---
run_status_command = on_command(_cmd("run-status"), force_whitespace=True, priority=88, block=True)
run_stop_command = on_command(_cmd("run-stop"), force_whitespace=True, priority=88, block=True)


def _is_admin(adapter_name: str, user_id: str) -> bool:
    """管理员判定(与 /hermes-status 同口径:白名单 {adapter}:{user_id})。"""
    return f"{adapter_name}:{user_id}" in load_user_lists().admin_users


_HERMES_CONTROL_UNAVAILABLE = "当前 AstrBot 后端未提供此 Hermes 专用命令"


def _scope_live_run(adapter_name: str, private: bool, user_id: str, group_id: str | None):
    """取当前会话登记的 live run(runs 通道开启时才有;tracker 为空=没在跑)。"""
    if _mcp.run_tracker is None:
        return None
    return _mcp.run_tracker.get_by_scope(make_scope_key(adapter_name, private, user_id, group_id))


@run_status_command.handle()
async def handle_run_status(bot: Bot, event: Event, matcher: Matcher):
    """查看当前会话在跑的 run(管理员)"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    if not _is_admin(adapter_name, user_id):
        logger.debug(f"[HERMES] /run-status silent skip for {adapter_name}:{user_id}")
        return  # 隐身策略(同 /hermes-status):不暴露命令存在性
    if not hermes_control_plane_enabled():
        await alconna.UniMessage(_HERMES_CONTROL_UNAVAILABLE).send(target=target, bot=bot)
        return
    group_id = None if target.private else target.id
    live = _scope_live_run(adapter_name, target.private, user_id, group_id)
    if live is None:
        await alconna.UniMessage("当前会话没有在跑的 run(runs 通道关闭或无活动 turn)").send(target=target, bot=bot)
        return
    st = await hermes_client.get_run_status(
        tgt=resolve_target(adapter_name, target.private, group_id), run_id=live.run_id
    )
    if st is None:
        await alconna.UniMessage(
            f"run {live.run_id} 状态拉取失败(gateway 侧可能已终结,等插件收尾清理)"
        ).send(target=target, bot=bot)
        return
    elapsed = int(time.time() - live.started_at)
    lines = [
        f"🏃 run: {live.run_id}",
        f"状态: {st.get('status')} (已 {elapsed}s)",
        f"最近事件: {st.get('last_event') or '-'}",
        f"模式: {live.mode} | session: {live.session_key}",
    ]
    usage = st.get("usage")
    if isinstance(usage, dict) and usage:
        lines.append(
            f"tokens: 入{usage.get('input_tokens')} 出{usage.get('output_tokens')}"
            f"(缓存读 {usage.get('cache_read_tokens', 0)})"
        )
    await alconna.UniMessage("\n".join(lines)).send(target=target, bot=bot)


@run_stop_command.handle()
async def handle_run_stop(bot: Bot, event: Event, matcher: Matcher):
    """终止当前会话在跑的 run(管理员,协作式)"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    if not _is_admin(adapter_name, user_id):
        logger.debug(f"[HERMES] /run-stop silent skip for {adapter_name}:{user_id}")
        return
    if not hermes_control_plane_enabled():
        await alconna.UniMessage(_HERMES_CONTROL_UNAVAILABLE).send(target=target, bot=bot)
        return
    group_id = None if target.private else target.id
    live = _scope_live_run(adapter_name, target.private, user_id, group_id)
    if live is None:
        await alconna.UniMessage("当前会话没有在跑的 run,没有可终止的对象").send(target=target, bot=bot)
        return
    ok = await hermes_client.stop_run(
        tgt=resolve_target(adapter_name, target.private, group_id), run_id=live.run_id
    )
    if ok:
        # 被停 turn 自身走 interrupted 硬静默,本条命令回执即唯一通知
        await alconna.UniMessage(f"🛑 已发送终止: {live.run_id}(协作式,正在收尾)").send(target=target, bot=bot)
    else:
        await alconna.UniMessage(f"终止请求未被受理: {live.run_id}(run 可能已自行结束)").send(target=target, bot=bot)

# --- /compress(管理员;Runs 会话手动压缩控制) ---
# 链路:QQ 命令 → 插件按当前会话 session key 调 gateway `POST /api/sessions/{id}/compress`
# → 网关跑 messaging /compress 的同一实现(临时 agent + 共享 compress_now + 同一轮换持久化)。
# 同会话正在跑 turn 时网关回 409 session_busy,这里只回执不重试;轮换时采纳新 session id。
compress_command = on_command(
    _cmd("compress"), aliases={_cmd("压缩")}, force_whitespace=True, priority=88, block=True
)


def compress_receipt_lines(
    result: dict, *, previous_key: str, adopted: bool | None = None
) -> list[str]:
    """把压缩结果拼成 QQ 回执(纯函数,便于离线测)。

    ``adopted``: 轮换时插件映射是否成功换到新 id(None = 未发生轮换)。
    """
    if result.get("ok"):
        before = int(result.get("before_messages") or 0)
        after = int(result.get("after_messages") or 0)
        status = str(result.get("status") or "compressed")
        if status == "preview":
            head = "🗜️ 压缩预览(未写入)"
        elif status == "nothing_to_do":
            head = "🗜️ 没有可压缩的内容(会话太短或无可摘要中段)"
        elif status == "aborted":
            head = f"🗜️ 未压缩(摘要被拒或中止,消息仍是 {before} 条)"
        else:
            head = f"🗜️ 会话压缩完成:{before} → {after} 条消息"
        lines = [head]
        if status == "not_enough":
            lines = ["🗜️ 会话太短,没有可压缩的内容(至少需要几条消息)"]
        elif status in {"nothing_to_do", "no_change"}:
            lines = ["🗜️ 没有可压缩的内容(无可摘要中段,或摘要被拒/中止;消息数未变)"]
        if result.get("rotated"):
            lines.append(f"↪️ 会话已轮换:{previous_key} → {result.get('session_id')}")
            if adopted:
                lines.append("✅ 插件已采纳新会话 id")
            elif adopted is False:
                lines.append("⚠️ 新会话 id 未能写入插件映射(映射已变),下轮可能仍按旧 id 提交")
        elif result.get("in_place"):
            lines.append(f"🧷 in-place 压缩,session 不变:{previous_key}")
        message = str(result.get("message") or "").strip()
        if message:
            lines.append(message)
        return lines
    code = int(result.get("code") or 0)
    status = str(result.get("status") or "")
    if code == 409 and status in {"session_busy", "session_compressing", "session_turn_active"}:
        return [
            "⏳ 当前会话正忙,压缩未执行:"
            f"{'正在跑 turn' if status != 'session_compressing' else '已有一次压缩在进行'}",
            "等它结束再发一次;不会并发压缩,也没有全局互斥。",
        ]
    if code == 404:
        return ["⚠️ 网关侧没有这个会话(尚未建立或已被清理),无需压缩"]
    detail = str(result.get("error") or "未知错误")[:200]
    return [f"⚠️ 压缩失败(HTTP {code or '-'} {status or ''}):{detail}"]


@compress_command.handle()
async def handle_compress(bot: Bot, event: Event, matcher: Matcher):
    """手动压缩当前会话上下文(管理员;同会话忙碌时明确拒绝)"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    if not _is_admin(adapter_name, user_id):
        logger.debug(f"[HERMES] /compress silent skip for {adapter_name}:{user_id}")
        return  # 隐身策略(同 /run-status):不暴露命令存在性
    if not hermes_control_plane_enabled():
        await alconna.UniMessage(_HERMES_CONTROL_UNAVAILABLE).send(target=target, bot=bot)
        return
    is_private = target.private
    group_id = None if is_private else target.id
    # 手动压缩针对"当前聊天所在会话";Runs 通道下这就是 X-Hermes-Session-Id 携带的值。
    session_key = session_manager.get_session_key(adapter_name, is_private, user_id, group_id)
    raw = ""
    try:
        raw = event.get_plaintext().strip()
    except Exception:
        pass
    focus = _tail_args(raw)  # 可选焦点主题(剥掉命令头/寻址前缀)
    result = await hermes_client.compress_session(
        tgt=resolve_target(adapter_name, is_private, group_id),
        session_key=session_key,
        focus_topic=focus,
    )
    adopted: bool | None = None
    new_key = str(result.get("session_id") or "")
    if result.get("ok") and result.get("rotated") and new_key and new_key != session_key:
        # 轮换后旧会话已 closed;必须采纳新 id,别把后续 turn 写回已关闭的旧会话。
        adopted = session_manager.adopt_session_key(session_key, new_key)
        logger.info(f"[HERMES] /compress 轮换采纳: {session_key} -> {new_key} adopted={adopted}")
    lines = compress_receipt_lines(result, previous_key=session_key, adopted=adopted)
    await alconna.UniMessage("\n".join(lines)).send(target=target, bot=bot)


# --- /reasoning(管理员;全局推理档位快切) ---
# 链路:hermes config set 原子改写 config.yaml → api_server 每请求现读 → 即时全局生效,
# gateway 与 nb 均无需重启;chat/completions 与 runs 通道共用同一回退路径。
reasoning_command = on_command(_cmd("reasoning"), force_whitespace=True, priority=88, block=True)

# 线上模型只认这三档(其余内部档位会被归并),故只开放这三个;换模型时按需再扩
_REASONING_LEVELS = ("low", "high", "max")
_REASONING_CONFIG_KEY = "agent.reasoning_effort"


def _hermes_cli_path() -> str:
    """hermes CLI 位置:PATH 优先,兜底 ~/.local/bin/hermes(systemd 服务 PATH 可能不含它)。"""
    import shutil
    return shutil.which("hermes") or str(Path.home() / ".local/bin/hermes")


async def _hermes_config_cli(*args: str) -> tuple[bool, str]:
    """调官方 hermes config 子命令(原子写 + 注释保留 + fail-closed 护栏全自带,
    不手改 config.yaml)。返回 (成功与否, 输出摘要)。stdout 的 unknown-key notice
    是校验名单过时(只拒 wrong-prefix 形态,普通未知键照写),以退出码判成败。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            _hermes_cli_path(), *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        return proc.returncode == 0, out.decode("utf-8", "replace").strip()
    except Exception as e:
        return False, str(e)


async def reasoning_effort_get() -> str:
    """读当前全局档位(展示用;读不到回 'unknown' 不阻断查询回复)。"""
    ok, out = await _hermes_config_cli("config", "get", _REASONING_CONFIG_KEY, "--raw")
    return out.splitlines()[-1].strip() if ok and out.strip() else "unknown"


async def reasoning_effort_set(level: str) -> tuple[bool, str]:
    return await _hermes_config_cli("config", "set", _REASONING_CONFIG_KEY, level)


@reasoning_command.handle()
async def handle_reasoning(bot: Bot, event: Event, matcher: Matcher):
    """查看/切换全局推理档位(管理员):/reasoning [low|high|max],改完全局即时生效"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    if not _is_admin(adapter_name, user_id):
        logger.debug(f"[HERMES] /reasoning silent skip for {adapter_name}:{user_id}")
        return  # 隐身策略(同 /run-status):不暴露命令存在性
    if not hermes_control_plane_enabled():
        await alconna.UniMessage(_HERMES_CONTROL_UNAVAILABLE).send(target=target, bot=bot)
        return
    # 手剥参数(同 /approve):get_plaintext 含命令本体,剥掉命令头与寻址前缀取余文
    raw = ""
    try:
        raw = event.get_plaintext().strip()
    except Exception:
        pass
    level = _tail_args(raw).lower()
    if not level:
        current = await reasoning_effort_get()
        await alconna.UniMessage(
            f"当前全局推理档位: {current}\n切换: {_display_cmd('reasoning')} <low|high|max>(即时生效,无需重启)"
        ).send(target=target, bot=bot)
        return
    if level not in _REASONING_LEVELS:
        await alconna.UniMessage(
            f"不支持的档位: {level}(可选 low/high/max)"
        ).send(target=target, bot=bot)
        return
    before = await reasoning_effort_get()
    ok, out = await reasoning_effort_set(level)
    if ok:
        logger.info(f"[HERMES] /reasoning: {adapter_name}:{user_id} 切全局档位 {before} -> {level}")
        await alconna.UniMessage(
            f"✅ 全局推理档位已切换: {before} → {level}(所有会话即时生效)"
        ).send(target=target, bot=bot)
    else:
        await alconna.UniMessage(
            f"⚠️ 切换失败,config 未改动: {out[:200]}"
        ).send(target=target, bot=bot)

# --- /reload-config(管理员；YAML 快照手动重载) ---
reload_config_command = on_command(_cmd("reload-config"), force_whitespace=True, priority=88, block=True)


def _config_reload_message(adapter_name: str, user_id: str) -> str | None:
    """先按旧管理员集合鉴权，再统一换配置快照；非管理员不暴露命令。"""
    if not _is_admin(adapter_name, user_id):
        logger.debug(f"[HERMES] /reload-config silent skip for {adapter_name}:{user_id}")
        return None
    result = reload_runtime_config()
    if result.ok:
        sticker_count = default_sticker_library().reload()
        logger.info(
            f"[HERMES] /reload-config: {adapter_name}:{user_id} "
            f"重载 {result.reloaded}，表情 {sticker_count} 条"
        )
        return (
            f"✅ {result.message}；表情 {sticker_count} 条"
            "(标注“重启生效”的键仍需重启)"
        )
    logger.warning(f"[HERMES] /reload-config 失败: {result.message}")
    return f"⚠️ {result.message}"


@reload_config_command.handle()
async def handle_reload_config(bot: Bot, event: Event, matcher: Matcher):
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()
    adapter_name = get_adapter_name(target)
    message = _config_reload_message(adapter_name, event.get_user_id() or "")
    if message is not None:
        await alconna.UniMessage(message).send(target=target, bot=bot)


# --- /approve(阶段3c,管理员应答挂起审批) ---
approve_command = on_command(_cmd("approve"), force_whitespace=True, priority=88, block=True)


@approve_command.handle()
async def handle_approve(bot: Bot, event: Event, matcher: Matcher, args: alconna.UniMessage | None = None):
    """应答挂起审批(管理员):/approve <once|session|always|deny>,答最旧一条"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    if not _is_admin(adapter_name, user_id):
        logger.debug(f"[HERMES] /approve silent skip for {adapter_name}:{user_id}")
        return
    if not hermes_control_plane_enabled():
        await alconna.UniMessage(_HERMES_CONTROL_UNAVAILABLE).send(target=target, bot=bot)
        return
    # nonebot on_command 的参数在 event 余文里;手剥比引 CommandArg 少一层适配器依赖
    raw = ""
    try:
        raw = event.get_plaintext().strip()
    except Exception:
        pass
    # get_plaintext 含命令本体(/approve xxx),剥掉命令头与寻址前缀取余文
    choice = _tail_args(raw).lower()
    if choice not in {"once", "session", "always", "deny"}:
        await alconna.UniMessage(
            f"用法: {_display_cmd('approve')} <once|session|always|deny>(当前挂起 {approvals_pending_count()} 条)"
        ).send(target=target, bot=bot)
        return
    # 看→答→删全程持锁,两把并发 /approve 不会 peek 到同一条目
    async with approvals_answer_lock:
        entry = approvals_peek_oldest()
        if entry is not None:
            ok, msg = await hermes_client.respond_approval(
                tgt=resolve_target(entry.adapter, entry.is_private, entry.group_id),
                run_id=entry.run_id,
                choice=choice,
                request_id=entry.request_id,
            )
            if ok:
                # 先应答后删单——失败留单,管理员可重试
                approvals_acknowledge(entry.approval_id)
    if entry is None:
        await alconna.UniMessage("当前没有挂起的审批(可能已超时自动拒绝)").send(target=target, bot=bot)
        return
    if ok:
        await alconna.UniMessage(f"✅ 审批 #{entry.approval_id} 已应答: {msg}").send(target=target, bot=bot)
    else:
        await alconna.UniMessage(f"审批 #{entry.approval_id} 应答失败: {msg}(可能已超时或已被应答;条目保留,可重试)").send(target=target, bot=bot)

# --- 裸指令吞没(本部署补丁: 寻址前缀启用时) ---
# 带前缀命令由上面各 matcher 接走(命令名不同互不冲突);裸形态(无前缀)到这里
# 一律静默吞掉——不执行也不落聊天路径,防止旧习惯的 /clear 被当普通消息发给 LLM。
# 私聊群聊同口径。hermes_keywords(如 /ai)不在吞没清单,关键词点火不受影响。
async def _swallow_bare_command(bot: Bot, event: Event, matcher: Matcher) -> None:
    # 补同因节流观测:吞没此前零日志零现场,用户打旧命令无从排查;只记命令名
    # 与 scope,不带余文,不新增用户可见提示。
    target = alconna.get_target()
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    scope = f"private:{user_id}" if target.private else f"group:{target.id}"
    raw = ""
    try:
        raw = event.get_plaintext().strip()
    except Exception:
        pass
    cmd = raw.split()[0] if raw else "?"
    throttled_info(
        f"gate.bare_cmd:{cmd}:{adapter_name}:{scope}",
        f"[HERMES cmd] 裸命令已吞没 cmd={cmd}(adapter={adapter_name} scope={scope})",
    )
    return


if CMD_PREFIX:
    for _bare_name in (
        "clear", "delay", "resume", "ping", "help", "帮助",
        "hermes-status", "run-status", "run-stop", "reasoning", "reload-config", "approve",
    ):
        on_command(_bare_name, force_whitespace=True, priority=89, block=True).handle()(_swallow_bare_command)
