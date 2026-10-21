"""统一用户设置清单:`<hermes_custom_dir>/user_lists.yaml`,现读现用(保存即生效)。

三个名单共住一个文件,人改(模板见包内 example 文件):
  - blacklist:          黑名单 {群号: [账号id, ...]}——名单账号的消息不置
    explicit/addressed,仅群聊生效(私聊不受名单影响)。
  - known_bots:         已知 bot 账号表——发言不钉委托锚,仍可被正常响应,全局生效。
  - private_allow:      私聊主动下行白名单——允许被主动下行私聊的账号(裸 id),
    唤起(wake_turn)与直发(push_message)两通道共用。未配置本键(或整文件
    缺失/坏档)时回落到管理员集合(hermes_admin_users,复合键按当前适配器拆
    前缀取裸 id);显式配置(含空表)以显式值为准,显式空表 = 不允许任何人。

三个名单从 .env 迁入本文件;旧 .env 键保留字段定义但不再被读取(迁移口径见
配置项设计约定)。
纪律:文件缺失/坏档 → 全默认(WARN 按内容去重,同 media_config);条目 id 只做
字符校验,坏条目跳过。此文件只读不写,任何字段缺失都有缺省。
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from typing import Any

from nonebot import logger

from ..config import plugin_config, resolve_custom_dir
from .approvals import admin_user_ids

_CONFIG_FILENAME = "user_lists.yaml"


@dataclass(frozen=True)
class UserLists:
    """用户设置清单快照。frozen:单轮内共享一份,改文件下轮生效。"""

    blacklist: dict[str, tuple[str, ...]] = field(default_factory=dict)
    """黑名单:{群号: (账号id, ...)}。仅群聊生效。"""
    known_bots: tuple[str, ...] = ()
    """已知 bot 账号表(裸 id,全局生效)。"""
    private_allow: tuple[str, ...] | None = None
    """私聊主动下行白名单(裸 id),唤起与直发两通道共用。None = 未配置 → 回落管理员
    集合(见 private_allowed);显式配置(含空表)以显式值为准。"""


def _valid_id(value: Any) -> str | None:
    """条目 id 校验:非空、只含数字/字母/下划线/横线。坏值返回 None(跳过)。"""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    s = str(value).strip()
    if not s or not all(c.isalnum() or c in "_-" for c in s):
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


_last_warn: str = ""


def load_user_lists() -> UserLists:
    """加载 user_lists.yaml。文件缺失/坏档 → 全默认(WARN 按内容去重)。

    现读现用:每次调用现读文件,保存即生效,不做缓存。
    """
    global _last_warn
    custom_dir = resolve_custom_dir()
    path = custom_dir / _CONFIG_FILENAME if custom_dir else None
    if path is None or not path.is_file():
        return UserLists()
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            raise ValueError("顶层必须是映射(YAML mapping)")
    except Exception as exc:
        warn = f"[HERMES user-lists] 配置文件不可用,回退全默认: {exc}"
        if warn != _last_warn:
            logger.warning(warn)
            _last_warn = warn
        return UserLists()
    return UserLists(
        blacklist=_parse_blacklist(doc.get("blacklist")),
        known_bots=_parse_id_tuple(doc.get("known_bots")) or (),
        private_allow=_parse_id_tuple(doc.get("private_allow")),
    )


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

    显式配置了 private_allow(含空表)以显式值为准;未配置时回落管理员集合
    (hermes_admin_users 为 {adapter}:{id} 复合键,按当前适配器拆前缀取裸 id)。
    """
    lists = load_user_lists()
    if lists.private_allow is not None:
        return user_id in lists.private_allow
    return user_id in set(admin_user_ids(set(plugin_config.hermes_admin_users), adapter_name))
