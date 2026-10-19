import nonebot_plugin_alconna as alconna
from nonebot.adapters import Event

from .adapter_norm import normalize_adapter_name
from .config import plugin_config


def get_adapter_name(source) -> str:
    """提取并归一化 adapter 名。

    支持两种输入:
      - `alconna.Target` (`.adapter` 是 str) — 消息处理路径主用
      - `nonebot.adapters.Bot` (`.adapter` 是 Adapter 实例,有 classmethod `get_name()`)
        — notice handler 路径用,因为还没有 alconna 消息上下文构造 Target

    归一化与 core/routing._normalize_adapter 同源(adapter_norm 模块)。
    """
    adapter = getattr(source, "adapter", "") or ""
    if hasattr(adapter, "get_name"):
        adapter = adapter.get_name()
    return normalize_adapter_name(adapter)


def check_isolation(event: Event, target: alconna.Target) -> bool:
    """
    检查当前消息是否在隔离白名单中。
    如果未通过白名单检查，返回 False。

    本部署补丁: allow 系白名单统一为 `{adapter}:{id}` 键
    (与 hermes_admin_users / hermes_group_endpoints 同构)——上游 allow_users/
    allow_groups 裸 id 匹配与 admin 系前缀匹配并存,属于两个时代的配置风格,
    fork 统一为前缀制。
    """
    # 取不到 user_id 用 ""——白名单键永不匹配空 id,
    # 匿名/异形事件直接被隔离检查拒绝(不再折叠进 "user" 公共桶)。
    user_id = event.get_user_id() or ""
    adapter_name = get_adapter_name(target)

    if target.private:
        # 私聊触发检查
        if (
            plugin_config.hermes_private_trigger == "allowlist"
            and f"{adapter_name}:{user_id}" not in plugin_config.hermes_allow_users
        ):
            return False
    else:
        # 群聊触发检查
        group_id = target.id
        if (
            plugin_config.hermes_allow_groups
            and f"{adapter_name}:{group_id}" not in plugin_config.hermes_allow_groups
        ):
            return False

    return True
