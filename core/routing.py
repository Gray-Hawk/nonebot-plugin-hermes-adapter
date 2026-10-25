"""Hermes 接入点解析(单上游)。

Hermes 把工具集、模型、文件工作区都绑在 profile(一份独立的 HERMES_HOME)上。
本部署统一所有会话(私聊与全部群聊)走默认接入点;需要多上游隔离时部署多实例,
不在插件内按群路由 —— 按群接不同上游会把会话、记忆与反向权限边界混在一起。

API key / url / timeout 一律来自默认上游配置(hermes_api_* 系列)。
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass

from ..config import plugin_config

_DEFAULT_LABEL = "default"


@dataclass(frozen=True)
class HermesTarget:
    """一次调用要发往的 Hermes 接入点。

    不可变且随用随解析:配置可在运行期被改(测试、热更),缓存会让路由与配置漂移。
    """

    base_url: str
    api_key: str
    timeout: int
    label: str
    """诊断用标签:单上游下恒为 \"default\"。"""


def default_target() -> HermesTarget:
    return HermesTarget(
        base_url=plugin_config.hermes_api_url.rstrip("/"),
        api_key=plugin_config.hermes_api_key,
        timeout=plugin_config.hermes_api_timeout,
        label=_DEFAULT_LABEL,
    )


def resolve_target(adapter_name: str, is_private: bool, group_id: str | None) -> HermesTarget:
    """解析这一轮该发往哪个接入点。单上游下所有会话一律走默认接入点。

    形参(adapter/is_private/group_id)保留是为了公共 API 形态稳定,消费端零改动。
    """
    return default_target()


def all_targets() -> list[HermesTarget]:
    """全部接入点。单上游下只有默认一个(体检类命令沿用旧形态)。"""
    return [default_target()]


# --- 反向通道:调用方范围 -------------------------------------------------
#
# 单上游后反向通道只有一个调用方(默认接入点的 agent),一把全局 key。范围模型退化为
# 两态:dev(未配置任何 key,维持「未配置即不校验」的开发模式)与 uniform(全局 key,
# 对所有群同权)。不再有 per-group 隔离:需要隔离时部署多实例。

_KIND_DEV = "dev"
_KIND_UNIFORM = "uniform"


@dataclass(frozen=True)
class CallerScope:
    """一个反向通道调用方能操作的群范围。

    单上游下没有 per-group 隔离:持有全局 key 即对所有群同权(uniform);
    未配置任何 key 时维持开发模式(dev)。两者 allows() 均恒真,区别只在自述与
    日志语义。刻意不留 master token:全局 key 就是默认 profile 自己的
    API_SERVER_KEY,单上游时它本来就该能操作全部群。
    """

    kind: str

    @classmethod
    def dev(cls) -> "CallerScope":
        """开发模式(未配置任何 key)。直调 impl 的单测也用它。"""
        return cls(kind=_KIND_DEV)

    @classmethod
    def uniform(cls) -> "CallerScope":
        """全局 key 认证通过:对所有群同权。"""
        return cls(kind=_KIND_UNIFORM)

    def allows(self, adapter: str, group_id: str) -> bool:
        return True

    def describe(self) -> str:
        """给**日志**用的自述。**不含 token 本身。**"""
        if self.kind == _KIND_DEV:
            return "dev-mode(未配置任何 key)"
        return "uniform(全局 key,对所有群同权)"

    def describe_for_caller(self) -> str:
        """回给 MCP 调用方的范围说明。"""
        if self.kind == _KIND_DEV:
            return "all groups (no API key configured on the bridge)"
        return "all groups (authenticated with the bridge API key)"


def _parse_bearer(authorization_header: str | None) -> str | None:
    if not authorization_header:
        return None
    parts = authorization_header.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    # 容忍尾部空白:客户端偶有意外空格。
    return parts[1].strip() or None


def resolve_caller_scope(authorization_header: str | None) -> CallerScope | None:
    """认出反向通道调用方并给出它的范围。

    返回 None = 认不出,调用方必须拒(HTTP 层 401 / 工具层拒绝)。**不得回落成"不限"。**
    单上游:全局 key 匹配即 uniform;未配置 key 即 dev(开发模式)。
    """
    global_key = plugin_config.hermes_api_key
    if not global_key:
        return CallerScope.dev()
    token = _parse_bearer(authorization_header)
    if token is None:
        return None
    if hmac.compare_digest(token, global_key):
        return CallerScope.uniform()
    return None
