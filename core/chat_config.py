"""chat_config.yaml：私聊、群聊、历史与主动巡查的不可变配置快照。"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..config import resolve_custom_dir
from .config_fields import (
    bool_field,
    choice_field,
    float_field,
    int_field,
    str_field,
    str_set_field,
)
from .config_store import ConfigSnapshotStore
from .config_templates import CHAT_CONFIG_TEMPLATE


@dataclass(frozen=True)
class ChatConfig:
    private_trigger: str = "all"
    reply_debounce_private_sec: float = 2.5
    group_trigger: str = "keyword"
    keywords: set[str] = field(default_factory=lambda: {"/ai"})
    anchor_max_age_sec: int = 150
    reply_debounce_group_sec: float = 3.5  # 旧配置兼容字段；群聊首轮已固定 6 秒，不读取
    reply_delay_for_bot_sec: float = 0.0
    long_reply_forward: bool = True
    rate_limit_emoji_id: int = 96
    active_session_timeout_sec: int = 0
    active_session_ttl_sec: int = 300  # 重启生效
    history_message_limit: int = 10
    ignore_prefix: set[str] = field(default_factory=lambda: {"."})
    reply_debounce_max_wait_sec: float = 8.0  # 仅私聊；群聊首轮固定 6 秒
    session_idle_reset_sec: int = 0
    proactive_interval_sec: int = 0
    proactive_jitter_sec: int = 0
    proactive_window_sec: int = 300
    proactive_groups: set[str] = field(default_factory=set)
    mcp_recent_limit_max: int = 50
    forward_extract_max_nodes: int = 10
    forward_extract_max_chars: int = 800
    greet_on_join: bool = False


def parse_chat_config(doc: dict) -> ChatConfig:
    return ChatConfig(
        private_trigger=choice_field(doc, "private_trigger", "all", ("all", "allowlist")),
        reply_debounce_private_sec=float_field(doc, "reply_debounce_private_sec", 2.5),
        group_trigger=choice_field(doc, "group_trigger", "keyword", ("keyword", "at")),
        keywords=str_set_field(doc, "keywords", {"/ai"}),
        anchor_max_age_sec=int_field(doc, "anchor_max_age_sec", 150, min_v=1),
        reply_debounce_group_sec=float_field(doc, "reply_debounce_group_sec", 3.5),
        reply_delay_for_bot_sec=float_field(doc, "reply_delay_for_bot_sec", 0.0),
        long_reply_forward=bool_field(doc, "long_reply_forward", True),
        rate_limit_emoji_id=int_field(doc, "rate_limit_emoji_id", 96),
        active_session_timeout_sec=int_field(doc, "active_session_timeout_sec", 0),
        active_session_ttl_sec=int_field(doc, "active_session_ttl_sec", 300, min_v=1),
        history_message_limit=int_field(doc, "history_message_limit", 10, min_v=1),
        ignore_prefix=str_set_field(doc, "ignore_prefix", {"."}),
        reply_debounce_max_wait_sec=float_field(doc, "reply_debounce_max_wait_sec", 8.0),
        session_idle_reset_sec=int_field(doc, "session_idle_reset_sec", 0),
        proactive_interval_sec=int_field(doc, "proactive_interval_sec", 0),
        proactive_jitter_sec=int_field(doc, "proactive_jitter_sec", 0),
        proactive_window_sec=int_field(doc, "proactive_window_sec", 300, min_v=1),
        proactive_groups=str_set_field(doc, "proactive_groups", set()),
        mcp_recent_limit_max=int_field(doc, "mcp_recent_limit_max", 50, min_v=1),
        forward_extract_max_nodes=int_field(doc, "forward_extract_max_nodes", 10, min_v=1),
        forward_extract_max_chars=int_field(doc, "forward_extract_max_chars", 800, min_v=1),
        greet_on_join=bool_field(doc, "greet_on_join", False),
    )


def _path():
    base = resolve_custom_dir()
    return base / "chat_config.yaml" if base else None


_store = ConfigSnapshotStore(
    label="chat_config",
    path_provider=_path,
    parser=parse_chat_config,
    default_factory=ChatConfig,
    default_content=CHAT_CONFIG_TEMPLATE,
)


def load_chat_config() -> ChatConfig:
    """返回启动或最近一次手动重载后的缓存快照。"""
    return _store.get()
