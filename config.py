"""插件启动配置。

对外只接受 AGENT_ 前缀环境变量；其余设置分属 chat/media/misc/user_lists YAML。
Python 字段保留 hermes_ 前缀以维持内部 API 形态，不代表旧 HERMES_ 环境变量仍可用。
"""

from pathlib import Path
from typing import Literal

from nonebot import get_plugin_config
from pydantic import BaseModel, ConfigDict, Field

# 发布模块名：仅用于 localstore 调用栈缺失时的按名兜底；实际导入名由包目录决定。
PLUGIN_MODULE_NAME = "nonebot_plugin_hermes_adapter"


class Config(BaseModel):
    """进程启动时读取的 8 个配置项；修改后重启生效。"""

    hermes_api_key: str = Field(default="", alias="agent_api_key")
    """AGENT_API_KEY：默认 Hermes API Server 的 Bearer 凭据。空串仅用于无鉴权开发环境。"""

    hermes_custom_dir: str = Field(default="", alias="agent_plugin_custom_dir")
    """AGENT_PLUGIN_CUSTOM_DIR：定制文件与运行时数据根目录；留空使用 localstore 插件数据目录。"""

    hermes_api_url: str = Field(default="http://127.0.0.1:8642", alias="agent_api_url")
    """AGENT_API_URL：默认 Hermes API Server 地址。"""

    hermes_api_timeout: int = Field(default=300, alias="agent_api_timeout")
    """AGENT_API_TIMEOUT：单次上游请求超时秒数。"""

    hermes_api_interface: Literal["completions", "runs"] = Field(
        default="completions", alias="agent_api_interface"
    )
    """AGENT_API_INTERFACE：turn 使用 completions 或 runs；runs 同时启用 steer 推送与对账能力。"""

    hermes_mcp_enabled: bool = Field(default=False, alias="agent_mcp_enabled")
    """AGENT_MCP_ENABLED：是否启动反向 MCP server。"""

    hermes_mcp_host: str = Field(default="127.0.0.1", alias="agent_mcp_host")
    """AGENT_MCP_HOST：反向 MCP server 监听地址。"""

    hermes_mcp_port: int = Field(default=8643, alias="agent_mcp_port")
    """AGENT_MCP_PORT：反向 MCP server 监听端口。"""

    model_config = ConfigDict(extra="ignore", populate_by_name=False)


plugin_config = get_plugin_config(Config)


def steer_reply_push_active() -> bool:
    """steer 回应实时推送：runs 接口开启，且旁白推送未占用同一输出通道。"""

    from .core.misc_config import load_misc_config

    return plugin_config.hermes_api_interface == "runs" and not load_misc_config().interim_echo_enabled


# resolve_custom_dir 的兜底 WARN 按形态每进程去重：调用方高频读取，不能逐次刷屏。
_resolve_warned: set[str] = set()


def _resolve_warn_once(kind: str, text: str) -> None:
    if kind in _resolve_warned:
        return
    _resolve_warned.add(kind)
    from nonebot import logger

    logger.warning(text)


def resolve_custom_dir() -> "Path | None":
    """解析定制定居目录；显式目录优先，否则使用 localstore 插件数据目录。

    返回 None 表示不可用，调用方回退内置行为。兜底 WARN 每进程每形态只打一次。
    """

    raw = (plugin_config.hermes_custom_dir or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_dir():
            _resolve_warn_once(
                "custom_dir_missing",
                f"[HERMES] AGENT_PLUGIN_CUSTOM_DIR 指向的目录不可用: {path};"
                "定制功能(规则/群档案/媒体配置等)将回退内置默认",
            )
        return path
    try:
        import nonebot_plugin_localstore as store

        try:
            return store.get_plugin_data_dir()
        except Exception:
            # 单测/脚本直 import 时没有插件调用栈，按插件名解析同一默认目录。
            path = store.get_data_dir(PLUGIN_MODULE_NAME)
            _resolve_warn_once(
                "localstore_by_name",
                f"[HERMES] localstore 调用栈识别失败,定制目录按名解析为 {path}"
                "(不吃 localstore_plugin_data_dir 重映射);插件正常加载时不应出现本警告",
            )
            return path
    except Exception:
        _resolve_warn_once(
            "localstore_missing",
            "[HERMES] localstore 不可用且未配置 AGENT_PLUGIN_CUSTOM_DIR:"
            "定制功能全部回退内置默认(不产生任何定制文件)",
        )
        return None
