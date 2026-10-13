"""runs 审批的挂起登记与管理员通知(本部署补丁:阶段3c)。

链路:turn 在 gateway 侧命中审批门 → events 流 approval.request 帧 →
RunsEventParser 捕获 → turn 的 approval_callback → 本模块登记 + 私聊通知管理员
→ 管理员 /approve <choice> 应答 → POST /v1/runs/{id}/approval → turn 继续。

设计要点:
- 挂起条目本地自增序号 + FIFO 应答(/approve <choice> 答最旧一条);
  上游 approvals.timeout(默认约 300s)到期后条目作废,应答时懒剔除;
- 通知走 OneBot send_private_msg 直发(私聊 Target 构造在 BotRegistry 外没有
  现成路由,且本平台取向=OneBot 为底);非 onebotv11 适配器记日志跳过(安全回退);
- 群聊侧不发任何审批消息(审批是管理员私事);
- 通知失败不拖垮主 turn(审批是附属通道,与旁白同原则)。
"""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field

from nonebot import logger

# 与上游 approvals.timeout 同量级;到期条目应答时按「已超时」处理
APPROVAL_EXPIRY_SEC = 300


@dataclass
class PendingApproval:
    approval_id: int
    run_id: str
    adapter: str
    scope_desc: str
    command: str
    choices: list[str]
    is_private: bool = False
    group_id: str | None = None  # 应答路由(resolve_target 用)
    ts: float = field(default_factory=time.time)

    @property
    def expired(self) -> bool:
        return time.time() - self.ts > APPROVAL_EXPIRY_SEC


_pending: dict[int, PendingApproval] = {}
_seq = itertools.count(1)


def add_pending(
    *, run_id: str, adapter: str, scope_desc: str, command: str, choices: list[str],
    is_private: bool = False, group_id: str | None = None,
) -> PendingApproval:
    """登记一条挂起审批,返回条目(通知文本引用其序号)。"""
    entry = PendingApproval(
        approval_id=next(_seq), run_id=run_id, adapter=adapter,
        scope_desc=scope_desc, command=command, choices=choices,
        is_private=is_private, group_id=group_id,
    )
    _pending[entry.approval_id] = entry
    return entry


def pop_oldest() -> PendingApproval | None:
    """取最旧的未过期条目(FIFO 应答);过期条目顺手剔除。"""
    for aid in sorted(_pending):
        entry = _pending[aid]
        if entry.expired:
            del _pending[aid]
            continue
        del _pending[aid]
        return entry
    # 全过期则清空报告 None
    return None


def pending_count() -> int:
    return len(_pending)


def drop_expired() -> int:
    """剔除全部过期条目,返回剔除数(sweeper/测试用)。"""
    expired = [aid for aid, e in _pending.items() if e.expired]
    for aid in expired:
        del _pending[aid]
    return len(expired)


def build_admin_notice(entry: PendingApproval) -> str:
    """管理员私聊通知文本(纯函数,离线可测)。"""
    choices = " / ".join(entry.choices) if entry.choices else "once / session / always / deny"
    return (
        f"⚠️ 审批请求 #{entry.approval_id}\n"
        f"run: {entry.run_id}\n"
        f"来源: {entry.scope_desc}\n"
        f"命令: {entry.command or '(无命令详情)'}\n"
        f"选项: {choices}\n"
        f"回复 /approve once 批准一次;/approve deny 拒绝;/approve session 本会话放行;"
        f"/approve always 永久放行。(约 300s 不答自动超时拒绝)"
    )


def admin_user_ids(admin_users: set[str], adapter_name: str) -> list[str]:
    """从 {adapter}:{user_id} 白名单里筛出指定适配器的管理员 id 列表(纯函数)。"""
    prefix = f"{adapter_name}:"
    return sorted(k[len(prefix):] for k in admin_users if k.startswith(prefix))


async def notify_admins(*, bot, adapter_name: str, entry: PendingApproval, admin_users: set[str]) -> int:
    """给所有管理员发审批私聊通知,返回送达数。

    OneBot send_private_msg 直发;非 onebotv11 记日志跳过(安全回退,不报错)。
    单个管理员发送失败不影响其余。
    """
    if adapter_name != "onebotv11":
        logger.info(f"[HERMES approval] 适配器 {adapter_name} 暂不支持审批私聊通知,跳过")
        return 0
    text = build_admin_notice(entry)
    sent = 0
    for uid in admin_user_ids(admin_users, adapter_name):
        try:
            await bot.call_api("send_private_msg", user_id=int(uid), message=text)
            sent += 1
        except Exception as exc:
            logger.warning(f"[HERMES approval] 审批通知发送失败 admin={uid}: {exc}")
    return sent
