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
from ..config import plugin_config
from ..core.hermes_client import hermes_client
from ..core.approvals import pending_count as approvals_pending_count
from ..core.approvals import peek_oldest as approvals_peek_oldest
from ..core.approvals import acknowledge as approvals_acknowledge
from ..core.approvals import answer_lock as approvals_answer_lock
from ..core.routing import all_targets, resolve_target
from ..core.run_tracker import make_scope_key
from ..core.session import session_manager
from ..utils import check_isolation, get_adapter_name

# --- /clear ---
clear_command = on_command("clear", force_whitespace=True, priority=88, block=True)


@clear_command.handle()
async def handle_clear(bot: Bot, event: Event, matcher: Matcher):
    """重置当前会话"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    adapter_name = get_adapter_name(target)
    # H9 收紧(2026-10-17):取不到 user_id 不折叠 "user" 公共桶,命令不执行。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] /clear 取不到 user_id,不执行 (adapter={adapter_name})")
        return
    group_id = None if target.private else target.id

    session_manager.clear_session(
        adapter_name=adapter_name,
        is_private=target.private,
        user_id=user_id,
        group_id=group_id,
    )

    reply = alconna.UniMessage("✅ 会话已重置，开始新的对话。")
    if not target.private:
        reply = alconna.UniMessage([alconna.At("user", user_id), "\n"]) + reply

    await reply.send(target=target, bot=bot)


# --- /delay、/resume(T1 空闲重置超控,本部署新增) ---
delay_command = on_command("delay", force_whitespace=True, priority=88, block=True)
resume_command = on_command("resume", force_whitespace=True, priority=88, block=True)


@delay_command.handle()
async def handle_delay(bot: Bot, event: Event, matcher: Matcher):
    """暂停当前会话的 T1 空闲自动重置"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    if plugin_config.hermes_session_idle_reset_sec <= 0:
        await alconna.UniMessage("ℹ️ 空闲自动重置(T1)当前未启用。").send(target=target, bot=bot)
        return

    adapter_name = get_adapter_name(target)
    # H9 收紧(2026-10-17):同上,匿名事件不执行。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] /delay 取不到 user_id,不执行 (adapter={adapter_name})")
        return
    group_id = None if target.private else target.id
    internal_id = session_manager.set_idle_paused(adapter_name, target.private, user_id, group_id, True)
    logger.info(f"[HERMES] /delay: T1 计时已暂停 ({internal_id})")
    await alconna.UniMessage("⏸️ 已暂停本会话的空闲自动重置,/resume 恢复。").send(target=target, bot=bot)


@resume_command.handle()
async def handle_resume(bot: Bot, event: Event, matcher: Matcher):
    """恢复当前会话的 T1 空闲自动重置,并重置计时"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    adapter_name = get_adapter_name(target)
    # H9 收紧(2026-10-17):同上,匿名事件不执行。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] /resume 取不到 user_id,不执行 (adapter={adapter_name})")
        return
    group_id = None if target.private else target.id
    internal_id = session_manager.set_idle_paused(adapter_name, target.private, user_id, group_id, False)
    logger.info(f"[HERMES] /resume: T1 计时已恢复并重置 ({internal_id})")
    await alconna.UniMessage("▶️ 已恢复空闲自动重置,计时重新开始。").send(target=target, bot=bot)


# --- /ping ---
ping_command = on_command("ping", force_whitespace=True, priority=88, block=True)


def build_ping_message(ok: bool) -> str:
    """/ping 的回复。

    这条命令只过 check_isolation,任何被允许的用户都能打,所以它不能带接入点标签
    或数量 —— 那等于把别的群的路由配置广播给所有人。逐接入点体检在
    build_endpoint_health_lines,只出现在管理员命令里。
    """
    if ok:
        return "🏓 pong! Hermes Agent 连接正常。"
    return "⚠️ 无法连接到 Hermes Agent，请检查 Gateway 是否正在运行。"


async def probe_current_endpoint(adapter_name: str, is_private: bool, group_id: str | None) -> bool:
    """只探当前会话自己的接入点 —— 用户问的是"我这儿能不能用"。"""
    return await hermes_client.health_check(resolve_target(adapter_name, is_private, group_id))


def build_endpoint_health_lines(results: list[tuple[str, bool]]) -> list[str]:
    """逐接入点体检结果。仅供 admin-only 的 /hermes-status 使用(含路由键)。"""
    if not results:
        return []
    return ["", "🌐 接入点体检:"] + [f"  {'✅' if ok else '❌'} {label}" for label, ok in results]


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
help_command = on_command("help", aliases={"帮助"}, force_whitespace=True, priority=88, block=True)

_HELP_MENU_SEP = "---"


def load_help_menu_decor(path: str) -> tuple[str | None, str | None]:
    """读取 /help 自定义头尾(2026-10-13 维护者拍板:只自定义头尾,独立文件)。

    返回 (header, footer);任一为 None 表示该段回退内置。文件不存在/读取失败/
    内容为空时整体回退并 WARN,/help 不挂。文件现读现发,保存即生效。
    格式:第一行单独 --- 分隔;之前是头,之后是尾;没有 --- 则整个文件只当头。
    注意(低危⑬,2026-10-14):头尾内容里不能再出现单独一行的 ---(会被当分隔符),
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
) -> str:
    """拼装 /help 正文。与 handler 分开以便离线冒烟。

    默认形态保持旧行为一字不差;自定义只换头尾,中间命令清单仍按功能开关
    动态生成(T1 才列 /delay /resume,管理员才列 /hermes-status)。
    """
    lines = [
        "/clear - 重置对话",
        "/ping - 检查连接状态",
        "/help - 显示本帮助",
    ]
    if plugin_config.hermes_session_idle_reset_sec > 0:
        # T1 启用时才暴露超控命令,免得用户对着不存在的功能打命令
        lines.append("/delay - 暂停空闲自动重置")
        lines.append("/resume - 恢复空闲自动重置")
    if is_admin:
        # 管理员才看见运行时状态命令,普通用户视角下此命令"不存在"
        lines.append("/hermes-status - 查看插件运行时状态(管理员)")
        lines.append("/run-status - 查看当前会话在跑的 run(管理员)")
        lines.append("/run-stop - 终止当前会话在跑的 run(管理员)")
        lines.append("/reasoning - 查看/切换全局推理档位(管理员)")

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
    is_admin = f"{adapter_name}:{user_id}" in plugin_config.hermes_admin_users

    # 低危⑬(2026-10-14):菜单文件读取挪出事件循环(同步 IO 会堵住其他消息处理)
    header, footer = await asyncio.to_thread(load_help_menu_decor, plugin_config.hermes_help_menu_file)
    help_text = build_help_text(target.private, is_admin, header=header, footer=footer)
    await alconna.UniMessage(help_text).send(target=target, bot=bot)


# --- /hermes-status ---
status_command = on_command("hermes-status", force_whitespace=True, priority=88, block=True)


@status_command.handle()
async def handle_status(bot: Bot, event: Event, matcher: Matcher):
    """打印插件 M1-mem 运行时状态:MCP / 活跃 session / buffer / registry。"""
    target = alconna.get_target()
    if not check_isolation(event, target):
        matcher.skip()

    # /hermes-status 暴露内部运行时(活跃群、buffer 内容、bot 路由),
    # 应限制在管理员白名单内。**隐身策略**:对非管理员完全静默,不发"未授权"
    # 提示——避免把命令存在性暴露给一般用户。空集 = deny by default。
    adapter_name = get_adapter_name(target)
    user_id = event.get_user_id() or ""
    admin_key = f"{adapter_name}:{user_id}"
    if admin_key not in plugin_config.hermes_admin_users:
        logger.debug(f"[HERMES] /hermes-status silent skip for {admin_key}")
        return  # block=True 已阻断后续 matcher,直接 return 即静默

    lines = build_status_lines(int(time.time() * 1000))
    if plugin_config.hermes_group_endpoints:
        # 逐接入点体检只在这条管理员命令里做:它会列出路由键。并发探活,
        # 否则 N 个接入点最坏要串行等 N×5s。
        targets = all_targets()
        oks = await asyncio.gather(*(hermes_client.health_check(t) for t in targets))
        lines.extend(build_endpoint_health_lines([(t.label, ok) for t, ok in zip(targets, oks)]))

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
    active_line = "on" if plugin_config.hermes_active_session_enabled else "off"

    # ActiveSessionManager:统计当前未过期 session
    active_count = 0
    active_details: list[str] = []
    if _mcp.active_sessions is not None:
        for s in _mcp.active_sessions.list():
            # 按管理器 is_alive_with_lease(TTL + key 级租约)而非 ttl_left>0:正在跑长 turn 的群
            # 带着租约,窗口虽已过 TTL 但仍在监听 —— 诊断命令把它藏起来,恰好藏掉了
            # 最需要看见的那一个。(本部署补丁 2026-10-02: 租约改 key 级)
            if not _mcp.active_sessions.is_alive_with_lease(s.adapter, s.group_id, now_ms):
                continue
            active_count += 1
            ttl_left = max(0, (s.expires_at - now_ms) // 1000)
            topic = f" topic={s.topic_hint}" if s.topic_hint else ""
            held = " in-flight" if _mcp.active_sessions.lease_count(s.adapter, s.group_id) else ""
            active_details.append(f"  - {s.adapter}/{s.group_id} by {s.triggered_by} ttl={ttl_left}s{held}{topic}")

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
        f"hermes_api: {plugin_config.hermes_api_url}"
        + (
            f" (+{len(plugin_config.hermes_group_endpoints)} 个按群接入点)"
            if plugin_config.hermes_group_endpoints
            else ""
        ),
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
run_status_command = on_command("run-status", force_whitespace=True, priority=88, block=True)
run_stop_command = on_command("run-stop", force_whitespace=True, priority=88, block=True)


def _is_admin(adapter_name: str, user_id: str) -> bool:
    """管理员判定(与 /hermes-status 同口径:白名单 {adapter}:{user_id})。"""
    return f"{adapter_name}:{user_id}" in plugin_config.hermes_admin_users


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

# --- /reasoning(2026-10-14 维护者定案,管理员;全局推理档位快切) ---
# 链路:hermes config set 原子改写 config.yaml → api_server 每请求现读 → 即时全局生效,
# gateway 与 nb 均无需重启;chat/completions 与 runs 通道共用同一回退路径。
reasoning_command = on_command("reasoning", force_whitespace=True, priority=88, block=True)

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
    # 手剥参数(同 /approve):get_plaintext 含命令本体,取首个空白后的余文
    raw = ""
    try:
        raw = event.get_plaintext().strip()
    except Exception:
        pass
    level = raw.split(maxsplit=1)[1].strip().lower() if " " in raw else ""
    if not level:
        current = await reasoning_effort_get()
        await alconna.UniMessage(
            f"当前全局推理档位: {current}\n切换: /reasoning <low|high|max>(即时生效,无需重启)"
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

# --- /approve(阶段3c,管理员应答挂起审批) ---
approve_command = on_command("approve", force_whitespace=True, priority=88, block=True)


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
    # nonebot on_command 的参数在 event 余文里;手剥比引 CommandArg 少一层适配器依赖
    raw = ""
    try:
        raw = event.get_plaintext().strip()
    except Exception:
        pass
    # get_plaintext 含命令本体(/approve xxx),剥掉首个空白前的部分
    choice = raw.split(maxsplit=1)[1].strip().lower() if " " in raw else ""
    if choice not in {"once", "session", "always", "deny"}:
        await alconna.UniMessage(
            f"用法: /approve <once|session|always|deny>(当前挂起 {approvals_pending_count()} 条)"
        ).send(target=target, bot=bot)
        return
    # 低危⑬(2026-10-14):看→答→删全程持锁,两把并发 /approve 不会 peek 到同一条目
    async with approvals_answer_lock:
        entry = approvals_peek_oldest()
        if entry is not None:
            ok, msg = await hermes_client.respond_approval(
                tgt=resolve_target(entry.adapter, entry.is_private, entry.group_id),
                run_id=entry.run_id,
                choice=choice,
            )
            if ok:
                # 2026-10-13 审核修复(中危⑦): 先应答后删单——失败留单,管理员可重试
                approvals_acknowledge(entry.approval_id)
    if entry is None:
        await alconna.UniMessage("当前没有挂起的审批(可能已超时自动拒绝)").send(target=target, bot=bot)
        return
    if ok:
        await alconna.UniMessage(f"✅ 审批 #{entry.approval_id} 已应答: {msg}").send(target=target, bot=bot)
    else:
        await alconna.UniMessage(f"审批 #{entry.approval_id} 应答失败: {msg}(可能已超时或已被应答;条目保留,可重试)").send(target=target, bot=bot)
