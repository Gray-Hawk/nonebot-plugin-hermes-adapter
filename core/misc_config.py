"""misc_config.yaml：历史、输出、通知与存储的不可变配置快照。"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from nonebot import logger

from ..config import resolve_custom_dir
from .config_fields import bool_field, int_field, str_field
from .config_store import ConfigSnapshotStore
from .config_templates import MISC_CONFIG_TEMPLATE


@dataclass(frozen=True)
class MiscConfig:
    """misc_config.yaml 的不可变快照字段表。

    新增配置域只依赖本模块已接好的快照加载器（解析、缺档建档、显式重载）即可读取；
    但若新键会被启动期消费方读取（如启动时定型的开关），还须同时核对该消费方的
    生效方式——``/reload-config`` 只换快照，不会重跑启动初始化。
    """

    history_text_max_chars: int = 800
    group_profile_enabled: bool = True
    sticker_enabled: bool = False  # 本地表情库总开关，重启生效
    max_length: int = 4000
    transport_error_fallback_text: str = ""
    help_menu_file: str = ""
    prompt_dump_enabled: bool = False
    approval_expiry_sec: int = 300
    prompt_timezone: str = ""
    delivery_poll_interval_sec: int = 30  # 重启生效；后台完成汇报轮询间隔(键名沿用旧名)
    bot_name: str = ""  # 重启生效，命令 matcher 注册期定型
    storage_db_path: str = ""  # 重启生效
    storage_message_retention_days: int = 30
    storage_message_max_rows: int = 100_000


def parse_misc_config(doc: dict) -> MiscConfig:
    return MiscConfig(
        history_text_max_chars=int_field(doc, "history_text_max_chars", 800, min_v=1),
        group_profile_enabled=bool_field(doc, "group_profile_enabled", True),
        sticker_enabled=bool_field(doc, "sticker_enabled", False),
        max_length=int_field(doc, "max_length", 4000, min_v=1),
        transport_error_fallback_text=str_field(doc, "transport_error_fallback_text", ""),
        help_menu_file=str_field(doc, "help_menu_file", ""),
        prompt_dump_enabled=bool_field(doc, "prompt_dump_enabled", False),
        approval_expiry_sec=int_field(doc, "approval_expiry_sec", 300, min_v=1),
        prompt_timezone=str_field(doc, "prompt_timezone", ""),
        delivery_poll_interval_sec=int_field(doc, "delivery_poll_interval_sec", 30),
        bot_name=str_field(doc, "bot_name", ""),
        storage_db_path=str_field(doc, "storage_db_path", ""),
        storage_message_retention_days=int_field(doc, "storage_message_retention_days", 30, min_v=1),
        storage_message_max_rows=int_field(doc, "storage_message_max_rows", 100_000, min_v=1),
    )


def _path():
    base = resolve_custom_dir()
    return base / "misc_config.yaml" if base else None


_store = ConfigSnapshotStore(
    label="misc_config",
    path_provider=_path,
    parser=parse_misc_config,
    default_factory=MiscConfig,
    default_content=MISC_CONFIG_TEMPLATE,
)


def load_misc_config() -> MiscConfig:
    return _store.get()


def bot_name_prefix(bot_name: str) -> str:
    """``bot_name`` → 指令寻址前缀：首段分词；空／纯空白 → 空串（旧直连形态）。"""
    parts = bot_name.strip().split()
    return parts[0] if parts else ""


@lru_cache(maxsize=1)
def startup_command_prefix() -> str:
    """启动期定型的指令寻址前缀：命令 matcher 与审批通知同源。

    ``bot_name`` 规定重启生效；``/reload-config`` 只提交新快照、不重跑启动初始化。
    因此这里在首次调用（插件加载期 ``handlers.commands`` 注册 matcher）定型并缓存，
    之后快照怎么改都不跟随——否则热重载后重启前，审批通知会给出管理员实际无法
    执行的 ``/新名 approve``，而旧命令才是真正注册着的那条。重启（新进程）自然
    重新定型，改名生效。
    """
    raw = load_misc_config().bot_name
    prefix = bot_name_prefix(raw)
    if len(raw.strip().split()) > 1:
        logger.warning(f"[HERMES] hermes_bot_name 含空白,只取首段 {prefix!r} 作指令前缀")
    return prefix
