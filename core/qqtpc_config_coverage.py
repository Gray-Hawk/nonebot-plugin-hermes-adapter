"""QQTPC 归一后的**配置覆盖检查**：只告警，不改任何权限/交付语义。

归一后同一会话会在 OneBot / Milky 间切换，但下面这些面仍按**真实 adapter** 配：

* ``user_lists.yaml`` 的复合键（``adapter:id``）：``admin_users`` /
  ``master_users`` / ``allow_users`` / ``allow_groups``；
* ``runs_delivery.yaml`` 的 ``repair_scopes``（``adapter:scope:id``）。

某个面只覆盖 QQTPC transport 的**真子集**时，切到缺失协议后会静默降级：
敏感命令按非管理员 skip、私聊唤醒被拒并无限重试、自动补发落回全局模式。
本模块只把这些缺口打成 WARN，判定逻辑一行不动——补配置是运维动作。

官方 QQ（``qq``）等非 QQTPC transport 不参与：它们不是同一会话的驱动形态。
"""

from __future__ import annotations

from nonebot import logger

from .runs_delivery_config import load_runs_delivery_config
from .session import QQTPC_TRANSPORTS
from .user_lists import load_user_lists

_TRANSPORTS: tuple[str, ...] = tuple(QQTPC_TRANSPORTS)


def _adapters_of_entries(entries) -> set[str]:
    """复合键集合 → 覆盖到的 adapter 标签集合（裸 id / 坏条目忽略）。"""
    tags: set[str] = set()
    for entry in entries or ():
        text = str(entry or "")
        if ":" not in text:
            continue
        tag = text.split(":", 1)[0].strip().lower()
        if tag:
            tags.add(tag)
    return tags


def _gap_for(surface: str, adapters: set[str]) -> str | None:
    """单面缺口：有 QQTPC 覆盖但未覆盖全部 transport 时返回 WARN 文案。"""
    covered = [t for t in _TRANSPORTS if t in adapters]
    if not covered:
        # 根本没配这一面（或只配了官方 QQ）= 不是「归一造成的缺口」。
        return None
    missing = [t for t in _TRANSPORTS if t not in adapters]
    if not missing:
        return None
    return (
        f"[HERMES config] {surface} 只覆盖 QQTPC transport {covered},缺 {missing};"
        "切到缺失协议后该面会静默降级(管理命令 skip / 私聊唤醒被拒 / 自动补发落回全局模式)。"
        "本检查只告警,不改判定;请在 user_lists.yaml / runs_delivery.yaml 按需补齐。"
    )


def collect_coverage_gaps(
    *,
    admin_users=None,
    master_users=None,
    allow_users=None,
    allow_groups=None,
    repair_scopes=(),
) -> list[str]:
    """收集全部配置覆盖缺口（纯函数，离线可测；返回空列表 = 覆盖完整）。"""
    gaps: list[str] = []
    for surface, entries in (
        ("user_lists.admin_users", admin_users),
        ("user_lists.master_users", master_users or ()),
        ("user_lists.allow_users", allow_users),
        ("user_lists.allow_groups", allow_groups),
    ):
        gap = _gap_for(surface, _adapters_of_entries(entries))
        if gap:
            gaps.append(gap)
    gap = _gap_for("runs_delivery.repair_scopes", _adapters_of_entries(repair_scopes))
    if gap:
        gaps.append(gap)
    return gaps


def warn_config_coverage() -> list[str]:
    """启动期跑一次覆盖检查并逐条 WARN；返回缺口列表（空 = 无缺口）。"""
    try:
        lists = load_user_lists()
        delivery = load_runs_delivery_config()
    except Exception as exc:  # noqa: BLE001 - 检查失败绝不拖垮启动
        logger.warning(f"[HERMES config] QQTPC 配置覆盖检查跳过(配置读取失败): {exc}")
        return []
    gaps = collect_coverage_gaps(
        admin_users=lists.admin_users,
        master_users=lists.master_users,
        allow_users=lists.allow_users,
        allow_groups=lists.allow_groups,
        repair_scopes=getattr(delivery, "repair_scopes", ()),
    )
    for gap in gaps:
        logger.warning(gap)
    return gaps
