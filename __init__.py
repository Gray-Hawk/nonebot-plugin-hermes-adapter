"""
NoneBot Plugin Hermes

通过 Hermes Agent API Server 实现多平台 AI 聊天机器人。
"""

from nonebot import get_driver, logger, require
from nonebot.plugin import PluginMetadata, inherit_supported_adapters

# 确保依赖的插件已加载
require("nonebot_plugin_alconna")
require("nonebot_plugin_apscheduler")
require("nonebot_plugin_localstore")

from .config import Config, plugin_config

__version__ = "0.1.0"

__plugin_meta__ = PluginMetadata(
    name="Hermes Agent",
    description="通过 Hermes Agent API Server 实现多平台 AI 聊天机器人",
    homepage="https://github.com/Gray-Hawk/nonebot-plugin-hermes-adapter",
    usage=(
        "在群聊中 @机器人 发送消息即可与 Hermes Agent 对话。\n"
        "私聊直接发送消息即可。\n\n"
        "M1: 群聊 @ 后 5 分钟内 bot 会监听话题相关消息并按需主动插话。\n\n"
        "命令:\n"
        "/clear - 重置对话\n"
        "/ping - 检查连接状态\n"
        "/help - 显示帮助"
    ),
    type="application",
    config=Config,
    supported_adapters=inherit_supported_adapters("nonebot_plugin_alconna"),
    extra={"author": "gsskk", "version": __version__},
)

# handlers 必须在 plugin manager 上下文里 import,matchers 才会注册到此插件名下
from . import handlers  # noqa: F401

# 所有运行时副作用挪到 driver.on_startup 钩子,避免 plugin import 阶段
# 触发 nb-cli 的 "not loaded as a plugin" 检测(顶层副作用过重时会撞这个雷)。
_driver = get_driver()


@_driver.on_startup
async def _hermes_m1_startup():
    """启动序列:装配运行时单例 + 起 MCP server + 注册 cron。

    注意:不能在 on_startup 钩子里再追加 on_startup 钩子(已经在 startup phase 里),
    所以直接 await start_mcp_server() 而非走 register_lifecycle 的间接路径。
    on_shutdown 钩子仍可在此追加,因为 shutdown phase 还没到。
    """
    import os

    from .mcp import (
        init_runtime_state,
        start_mcp_server,
        start_storage,
        stop_mcp_server,
        stop_storage,
    )
    from .tasks import (
        register_delivery_poll,
        register_expire_active_sessions,
        register_proactive_activation,
        register_session_idle_reset,
        register_storage_vacuum,
    )

    from .core.config_reload import initialize_runtime_config

    initialize_runtime_config()
    init_runtime_state()
    await start_storage()
    # system_prompt 留痕开关装载(启动装载+缓存;热载指令随配置专项接入)
    from .core.prompt_dump import init_prompt_dump

    init_prompt_dump()
    await start_mcp_server()
    _driver.on_shutdown(stop_mcp_server)
    _driver.on_shutdown(stop_storage)
    # 挂起审批装回后延迟重发管理员通知(本部署补丁:best-effort):
    # 通知随旧进程死了,装回的条目管理员可能从没见过;条目本身已在,/approve 可答。
    from .core.approvals import pending_count as _approvals_pending_count
    from .core.approvals import renotify_pending as _approvals_renotify

    if _approvals_pending_count() > 0:
        from .core.bg_tasks import spawn_bg_task
        from .core.user_lists import load_user_lists

        spawn_bg_task(
            _approvals_renotify(admin_users=set(load_user_lists().admin_users)),
            name="hermes-approval-renotify",
        )
    # 进程级共享 httpx client 随 shutdown 释放(连接池复用)
    from .core.hermes_client import close_shared_http_client

    _driver.on_shutdown(close_shared_http_client)
    register_expire_active_sessions()
    register_storage_vacuum()
    register_session_idle_reset()
    register_proactive_activation()
    register_delivery_poll()
    from .core.misc_config import load_misc_config

    _misc_cfg = load_misc_config()
    # 本部署补丁: 指令寻址前缀启用时启动期明示(前缀在 handlers/commands.py 注册期定型)
    if _misc_cfg.bot_name.strip():
        logger.info(
            f"[HERMES] 指令寻址已启用: 本实例指令形如 "
            f"/{_misc_cfg.bot_name.strip().split()[0]} <命令>, 裸指令静默不执行"
        )
    # nb 重启后续跑在跑 runs turn;completions 路不写 tracker。
    if plugin_config.hermes_api_interface == "runs":
        from .tasks import resume_active_runs

        await resume_active_runs()

    # 投递轮询间隔过小只会白增本地请求与日志噪音,任务完成汇报的时效收益可忽略
    if 0 < _misc_cfg.delivery_poll_interval_sec < 10:
        logger.warning(
            f"[HERMES] delivery_poll_interval_sec({_misc_cfg.delivery_poll_interval_sec}) "
            "小于 10s:汇报时效收益可忽略,只会白增轮询开销;若这是本意可忽略本警告"
        )

    # T5 应大于 TTL,配反了等效于“窗口只能活 T5”——能跑但多半不是本意
    from .core.chat_config import load_chat_config as _load_chat_config

    _chat_cfg = _load_chat_config()
    if 0 < _chat_cfg.active_session_timeout_sec <= _chat_cfg.active_session_ttl_sec:
        logger.warning(
            f"[HERMES] active_session_timeout_sec({_chat_cfg.active_session_timeout_sec}) "
            f"<= active_session_ttl_sec({_chat_cfg.active_session_ttl_sec}):窗口实际只能活 T5 秒,"
            "隐式续期完全失效;若这是本意可忽略本警告"
        )

    # /help 菜单文件配了路径但读不到时,启动期先提醒;运行期仍会回退内置菜单
    if _misc_cfg.help_menu_file and not os.path.exists(
        os.path.expanduser(_misc_cfg.help_menu_file)
    ):
        logger.warning(
            f"[HERMES] help_menu_file 指向的文件不存在: "
            f"{_misc_cfg.help_menu_file};/help 将回退内置菜单"
        )

    # 定制定居目录:配了但目录不存在时启动期提醒;各功能各自回退内置
    if plugin_config.hermes_custom_dir and not os.path.isdir(
        os.path.expanduser(plugin_config.hermes_custom_dir)
    ):
        logger.warning(
            f"[HERMES] AGENT_PLUGIN_CUSTOM_DIR 指向的目录不存在: "
            f"{plugin_config.hermes_custom_dir};定制功能将回退内置默认"
        )

    if plugin_config.hermes_api_interface == "runs":
        logger.info("[HERMES] API 接口: runs")

    logger.info(
        f"Hermes Plugin loaded — API: {plugin_config.hermes_api_url} | "
        f"MCP: {'on ' + plugin_config.hermes_mcp_host + ':' + str(plugin_config.hermes_mcp_port) if plugin_config.hermes_mcp_enabled else 'off'} | "
        "active_session: on"
    )
