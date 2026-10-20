"""runs 审批的挂起登记与管理员通知(本部署补丁:阶段3c)。

链路:turn 在 gateway 侧命中审批门 → events 流 approval.request 帧 →
RunsEventParser 捕获 → turn 的 approval_callback → 本模块登记 + 私聊通知管理员
→ 管理员 /approve <choice> 应答 → POST /v1/runs/{id}/approval → turn 继续。

设计要点:
- 挂起条目本地自增序号 + FIFO 应答(/approve <choice> 答最旧一条);
  上游 approvals.timeout(默认约 300s)到期后条目作废,应答时懒剔除;
  本条超时秒数走配置 hermes_approval_expiry_sec(与上游孪生,需人工保持一致);
- 挂起集合持久化到插件数据目录 pending_approvals.json(增删即写,原子替换),
  重启装回未过期条目并延迟重发管理员通知(best-effort)——纯内存时重启全丢,
  窗口期内到达的审批管理员永远看不到;
- 通知走 OneBot send_private_msg 直发(私聊 Target 构造在 BotRegistry 外没有
  现成路由,且本平台取向=OneBot 为底);非 onebotv11 适配器记日志跳过(安全回退);
- 群聊侧不发任何审批消息(审批是管理员私事);
- 通知失败不拖垮主 turn(审批是附属通道,与旁白同原则)。
"""

from __future__ import annotations

import asyncio
import itertools
import json
import time
from dataclasses import dataclass, field
from pathlib import Path

from nonebot import logger

from ..config import plugin_config


def _approval_expiry_sec() -> int:
    """本条超时秒数(现读配置)。与上游 gateway approvals.timeout 孪生,无同步机制。"""
    return plugin_config.hermes_approval_expiry_sec


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
    # v0.22 authority 形态应答三键之一,来自审批通知帧(request_id);老形态用不到,
    # 老存档/老 gateway 无此值时为空串,应答时从 run 状态 pending_controls 兜底。
    request_id: str = ""
    ts: float = field(default_factory=time.time)

    @property
    def expired(self) -> bool:
        return time.time() - self.ts > _approval_expiry_sec()


_pending: dict[int, PendingApproval] = {}
_seq = itertools.count(1)

# 挂起集合持久化(本部署补丁):纯内存时重启全丢,窗口期内到达的审批管理员
# 永远看不到、只能等上游超时自动拒绝。绑定存储后增删即写盘;启动装回未过期条目。
_store_path: Path | None = None


def bind_storage(path: Path) -> int:
    """绑定挂起审批的持久化文件并装回未过期条目,返回装回条数。

    由运行时装配(init_runtime_state)在启动期调用一次。读档失败/格式不对
    按空档处理并 WARN——审批是附属通道,持久化故障不拖垮启动。
    """
    global _store_path, _seq
    _store_path = path
    if not path.exists():
        return 0
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError("存档顶层不是数组")
    except Exception as exc:
        logger.warning(f"[HERMES approval] 挂起审批存档读取失败,按空档处理: {exc}")
        return 0
    restored = 0
    max_id = 0
    for row in rows:
        try:
            entry = PendingApproval(
                approval_id=int(row["approval_id"]),
                run_id=str(row["run_id"]),
                adapter=str(row["adapter"]),
                scope_desc=str(row["scope_desc"]),
                command=str(row["command"]),
                choices=[str(c) for c in row["choices"]],
                is_private=bool(row.get("is_private", False)),
                group_id=(str(row["group_id"]) if row.get("group_id") is not None else None),
                request_id=str(row.get("request_id") or ""),
                ts=float(row["ts"]),
            )
        except Exception:
            continue
        if entry.expired:
            continue
        _pending[entry.approval_id] = entry
        max_id = max(max_id, entry.approval_id)
        restored += 1
    if max_id:
        _seq = itertools.count(max_id + 1)
    if restored:
        logger.info(f"[HERMES approval] 从存档装回 {restored} 条未过期挂起审批")
    return restored


def _persist() -> None:
    """把当前挂起集合写盘(tmp+replace 原子写)。失败只 WARN 不抛。"""
    if _store_path is None:
        return
    try:
        rows = [
            {
                "approval_id": e.approval_id,
                "run_id": e.run_id,
                "adapter": e.adapter,
                "scope_desc": e.scope_desc,
                "command": e.command,
                "choices": e.choices,
                "is_private": e.is_private,
                "group_id": e.group_id,
                "request_id": e.request_id,
                "ts": e.ts,
            }
            for e in _pending.values()
        ]
        tmp = _store_path.with_name(_store_path.name + ".tmp")
        tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(_store_path)
    except Exception as exc:
        logger.warning(f"[HERMES approval] 挂起审批写盘失败(内存态仍可用,重启即丢): {exc}")

# /approve 应答互斥锁。peek 与 acknowledge 之间夹着 ack RPC
# 的 await,两把并发 /approve 会 peek 到同一条目→重复应答、第二条目被跳过。
# 锁把「看→答→删」串成原子段;回执发送在锁外,不占串行段。
answer_lock = asyncio.Lock()


def add_pending(
    *, run_id: str, adapter: str, scope_desc: str, command: str, choices: list[str],
    is_private: bool = False, group_id: str | None = None, request_id: str = "",
) -> PendingApproval:
    """登记一条挂起审批,返回条目(通知文本引用其序号)。"""
    entry = PendingApproval(
        approval_id=next(_seq), run_id=run_id, adapter=adapter,
        scope_desc=scope_desc, command=command, choices=choices,
        is_private=is_private, group_id=group_id, request_id=request_id,
    )
    _pending[entry.approval_id] = entry
    _persist()
    return entry


def pop_oldest() -> PendingApproval | None:
    """取最旧的未过期条目(FIFO 应答);过期条目顺手剔除。

    ⑪ 审计定案:插件内零调用;保留供回归脚本使用(技能侧 smoke_approvals.py)。
    """
    changed = False
    result: PendingApproval | None = None
    for aid in sorted(_pending):
        entry = _pending[aid]
        del _pending[aid]
        changed = True
        if not entry.expired:
            result = entry
            break
    if changed:
        _persist()
    # 全过期则清空报告 None
    return result


def peek_oldest() -> PendingApproval | None:
    """非破坏性读最旧的未过期条目(过期顺手剔除)。

    /approve 应答改「先看后确认」——旧 pop_oldest
    先删条目再 POST,网络/HTTP 失败条目即丢、409 与断网不可区分无法重试。
    现在由 handle_approve peek → 应答成功才 acknowledge 删除;失败留单可重试。
    """
    changed = False
    result: PendingApproval | None = None
    for aid in sorted(_pending):
        entry = _pending[aid]
        if entry.expired:
            del _pending[aid]
            changed = True
            continue
        result = entry
        break
    if changed:
        _persist()
    return result


def acknowledge(approval_id: int) -> None:
    """应答成功后删单(不存在的 id 安全 no-op)。"""
    if approval_id in _pending:
        del _pending[approval_id]
        _persist()


def pending_count() -> int:
    return len(_pending)


def drop_expired() -> int:
    """剔除全部过期条目,返回剔除数(sweeper/测试用)。

    ⑪ 审计定案:插件内零调用,保留勿删(全树含冒烟目录 grep 复核无直接调用;
    随 pop_oldest 同批保留)。
    """
    expired = [aid for aid, e in _pending.items() if e.expired]
    for aid in expired:
        del _pending[aid]
    if expired:
        _persist()
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
        f"/approve always 永久放行。(约 {_approval_expiry_sec()}s 不答自动超时拒绝)"
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


async def renotify_pending(*, admin_users: set[str], delay_sec: float = 20.0) -> int:
    """启动后延迟重发未过期挂起审批的管理员通知(best-effort,返回送达数)。

    重启装回的条目管理员此前可能根本没看到(通知随旧进程死了)。等 NapCat
    连接建立后再发;找不到 onebotv11 bot 或发送失败只记日志,不拖垮启动。
    条目本身留在 _pending,/approve 随时可答。
    """
    await asyncio.sleep(delay_sec)
    if not _pending:
        return 0
    from nonebot import get_bots

    from ..adapter_norm import normalize_adapter_name

    bots = [
        b for b in get_bots().values()
        if normalize_adapter_name(getattr(b.adapter, "get_name", lambda: "")()) == "onebotv11"
    ]
    if not bots:
        logger.info("[HERMES approval] 重发审批通知时无 onebotv11 bot 在线,跳过(条目仍在,/approve 可答)")
        return 0
    sent = 0
    for aid in sorted(_pending):
        entry = _pending[aid]
        if entry.expired:
            continue
        sent += await notify_admins(bot=bots[0], adapter_name="onebotv11", entry=entry, admin_users=admin_users)
    return sent
