"""统一用户设置清单:`<hermes_custom_dir>/user_lists.yaml`,启动缓存 + 显式重载。

名单共住一个文件(缺档自动建档,模板见包内 config_templates):
  - blacklist:          黑名单 {群号: [账号id, ...]}——名单账号的消息不置
    explicit/addressed,仅群聊生效(私聊不受名单影响)。
  - known_bots:         已知 bot 账号表——发言不钉委托锚,仍可被正常响应,全局生效。
  - private_allow:      私聊主动下行白名单——允许被主动下行私聊的账号(裸 id),
    唤起(wake_turn)与直发(push_message)两通道共用。未配置本键(或整文件
    缺失/坏档)时回落到管理员集合(本文件 admin_users,复合键按当前适配器拆
    前缀取裸 id);显式配置(含空表)以显式值为准,显式空表 = 不允许任何人。
  - admin_users:        敏感命令与管理操作的管理员集合({adapter}:{user_id} 复合 id),
    空集 = 默认 deny。手动重载先按旧集合鉴权,成功后再替换新集合。
  - allow_users:        私聊 allowlist 模式的被动聊天白名单({adapter}:{user_id} 复合 id),
    仅当 chat_config.yaml 的 private_trigger = allowlist 时生效;与 private_allow
    语义不同,不得合并。
  - allow_groups:       允许响应的群集合({adapter}:{group_id} 复合 id),空集 = 全部群。

纪律:文件缺失/坏档 → 全默认(启动)或保留旧快照(重载);条目只做字符校验,
坏条目跳过;裸 id 只允许数字/字母/下划线/横线,复合 id 为 adapter:id 两段。
此文件缺档时只写一次模板,手动重载不重排、不覆盖用户已有文件。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..config import resolve_custom_dir
from .approvals import admin_user_ids
from .config_store import ConfigSnapshotStore
from .config_templates import USER_LISTS_TEMPLATE

_CONFIG_FILENAME = "user_lists.yaml"


@dataclass(frozen=True)
class UserLists:
    """用户设置清单快照。frozen:单轮内共享一份,显式重载后才更换。"""

    blacklist: dict[str, tuple[str, ...]] = field(default_factory=dict)
    """黑名单:{群号: (账号id, ...)}。仅群聊生效。"""
    known_bots: tuple[str, ...] = ()
    """已知 bot 账号表(裸 id,全局生效)。"""
    private_allow: tuple[str, ...] | None = None
    """私聊主动下行白名单(裸 id),唤起与直发两通道共用。None = 未配置 → 回落管理员
    集合(见 private_allowed,读本文件 admin_users);显式配置(含空表)以显式值为准。"""
    admin_users: set[str] = field(default_factory=set)
    """管理员集合({adapter}:{user_id} 复合 id)。空集 = 默认 deny。"""
    allow_users: set[str] = field(default_factory=set)
    """私聊 allowlist 被动聊天白名单({adapter}:{user_id} 复合 id)。与 private_allow 不混。"""
    allow_groups: set[str] = field(default_factory=set)
    """允许响应的群集合({adapter}:{group_id} 复合 id)。空集 = 全部群。"""


def _valid_id(value: Any) -> str | None:
    """条目 id 校验:非空、只含数字/字母/下划线/横线。坏值返回 None(跳过)。"""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    s = str(value).strip()
    if not s or not all(c.isalnum() or c in "_-" for c in s):
        return None
    return s


def _valid_composite(value: Any) -> str | None:
    """复合 ID 校验:adapter:id 两段、各段非空且只含数字/字母/下划线/横线。"""
    if isinstance(value, bool) or not isinstance(value, str):
        return None
    s = value.strip()
    if not s or s.count(":") != 1:
        return None
    adapter, uid = s.split(":")
    if not adapter or not uid:
        return None
    if not all(c.isalnum() or c in "_-" for c in adapter):
        return None
    if not all(c.isalnum() or c in "_-" for c in uid):
        return None
    return s


def _parse_blacklist(raw: Any) -> dict[str, tuple[str, ...]]:
    """黑名单:{群号: [账号id, ...]}。群键与成员各自校验,坏条目跳过。"""
    out: dict[str, tuple[str, ...]] = {}
    if not isinstance(raw, dict):
        return out
    for gk, gv in raw.items():
        gid = _valid_id(gk)
        if gid is None or not isinstance(gv, list):
            continue
        ids: list[str] = []
        for item in gv:
            uid = _valid_id(item)
            if uid is not None and uid not in ids:
                ids.append(uid)
        out[gid] = tuple(ids)
    return out


def _parse_id_tuple(raw: Any) -> tuple[str, ...] | None:
    """账号表:[账号id, ...];键存在但形态坏 → None(按未配置处理,遵循字段级回退总则)——
    private_allow 由此回落管理员集合;显式空表 [] 才是「不允许任何人」。"""
    if not isinstance(raw, list):
        return None
    ids: list[str] = []
    for item in raw:
        uid = _valid_id(item)
        if uid is not None and uid not in ids:
            ids.append(uid)
    return tuple(ids)


def _parse_composite_set(raw: Any) -> set[str]:
    """复合 id 集合:[adapter:id, ...];坏条目跳过,去重保序不保证。"""
    out: set[str] = set()
    if not isinstance(raw, list):
        return out
    for item in raw:
        cid = _valid_composite(item)
        if cid is not None:
            out.add(cid)
    return out


def parse_user_lists(doc: dict) -> UserLists:
    """解析 user_lists.yaml 顶层映射为 UserLists 快照。"""
    return UserLists(
        blacklist=_parse_blacklist(doc.get("blacklist")),
        known_bots=_parse_id_tuple(doc.get("known_bots")) or (),
        private_allow=_parse_id_tuple(doc.get("private_allow")),
        admin_users=_parse_composite_set(doc.get("admin_users")),
        allow_users=_parse_composite_set(doc.get("allow_users")),
        allow_groups=_parse_composite_set(doc.get("allow_groups")),
    )


def _path():
    base = resolve_custom_dir()
    return base / _CONFIG_FILENAME if base else None


_store = ConfigSnapshotStore(
    label="user_lists",
    path_provider=_path,
    parser=parse_user_lists,
    default_factory=UserLists,
    default_content=USER_LISTS_TEMPLATE,
)


def load_user_lists() -> UserLists:
    """返回启动加载或最近一次显式重载后的缓存快照；不再每次读盘。"""
    return _store.get()


def is_blacklisted(group_id: str | None, user_id: str) -> bool:
    """黑名单判定。仅群聊生效(名单按群分组);私聊(None 群)恒不命中。"""
    if not group_id:
        return False
    return user_id in load_user_lists().blacklist.get(group_id, ())


def is_known_bot(user_id: str) -> bool:
    """已知 bot 账号判定(裸 id 直比,全局生效)。"""
    return user_id in load_user_lists().known_bots


def private_allowed(adapter_name: str, user_id: str) -> bool:
    """私聊主动下行白名单硬闸(唤起与直发两通道共用)。

    显式配置了 private_allow(含空表)以显式值为准;未配置时回落本文件
    admin_users(复合键按当前适配器拆前缀取裸 id),不再读 plugin_config。
    """
    lists = load_user_lists()
    if lists.private_allow is not None:
        return user_id in lists.private_allow
    return user_id in set(admin_user_ids(set(lists.admin_users), adapter_name))
