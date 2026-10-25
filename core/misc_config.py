"""misc_config.yaml：历史、输出、通知与存储的不可变配置快照。"""
from __future__ import annotations

from dataclasses import dataclass

from ..config import resolve_custom_dir
from .config_fields import bool_field, int_field, str_field
from .config_store import ConfigSnapshotStore
from .config_templates import MISC_CONFIG_TEMPLATE


@dataclass(frozen=True)
class MiscConfig:
    history_text_max_chars: int = 800
    group_profile_enabled: bool = True
    max_length: int = 4000
    transport_error_fallback_text: str = ""
    interim_echo_enabled: bool = False
    help_menu_file: str = ""
    prompt_dump_enabled: bool = False
    approval_expiry_sec: int = 300
    prompt_timezone: str = ""
    delivery_poll_interval_sec: int = 30  # 重启生效；旧通道后续专项移除
    bot_name: str = ""  # 重启生效，命令 matcher 注册期定型
    storage_db_path: str = ""  # 重启生效
    storage_message_retention_days: int = 30
    storage_message_max_rows: int = 100_000


def parse_misc_config(doc: dict) -> MiscConfig:
    return MiscConfig(
        history_text_max_chars=int_field(doc, "history_text_max_chars", 800, min_v=1),
        group_profile_enabled=bool_field(doc, "group_profile_enabled", True),
        max_length=int_field(doc, "max_length", 4000, min_v=1),
        transport_error_fallback_text=str_field(doc, "transport_error_fallback_text", ""),
        interim_echo_enabled=bool_field(doc, "interim_echo_enabled", False),
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
