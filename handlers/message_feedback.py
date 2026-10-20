"""反馈/打回/审批域:分级反馈规则检测、离场确认轮、类①接力/类②重交、emoji 回执、审批通知工厂。

从 handlers/message.py 拆入;被 turns/relay 域调用,处于依赖底层。
"""

import time

from nonebot import logger
from nonebot.adapters import Bot

from ..config import plugin_config
from ..core import task_notes
from ..core.approvals import add_pending as approvals_add_pending
from ..core.approvals import notify_admins as approvals_notify_admins
from ..core.feedback_rules import load_rules
from ..core.inflight import make_inflight_key


# --- 错误分级反馈 + 离场确认轮(规则引擎见 core/feedback_rules.py) ---
# 后续轮一族:规则命中 → 追加指定 prompt 的后续轮,prompt 分族——
#   confirm 离场确认轮 | relay 继续执行轮(类①) | bounce 重交轮(类②);不并存两套机制。
# 统一不拦截:命中 action=note 的规则 → 下一轮贴暗示便签;confirm/relay/bounce → 追加后续轮。
# 规则来自 hermes_custom_dir/feedback_rules.yaml(schema v2),缺省回退内置两条。
# 总闸:缺省不拦截——没有 relay/bounce 规则时,系统无任何拦截/接力行为。


_DEPARTURE_CONFIRM_PROMPT = (
    "(系统核对:你上一条回复里出现了“稍后/回头再汇报”类措辞。"
    "如果有后台任务在跑、完成时系统会叫醒你——填 background_task 登记任务便签后 "
    "should_reply=false,或不登记直接静默,都可以;如果刚才是说漏了、并没有任务在跑,"
    "请用 reply_text 向对方补充说明)"
)

# (scope_key, rule_name) → 上次 confirm 点火时刻(按规则名各自冷却,互不影响)
_confirm_fired_at: dict[tuple[tuple[str, str], str], float] = {}


# 内存兜底(本部署补丁):这类冷却/计数标记表只增不减,长跑缓慢泄漏。淘汰时长
# 刻意远超任何实用冷却(confirm 缺省 300s),只作内存边界,不改变冷却语义。
_MARK_KEEP_SEC = 48 * 3600
_MARK_MAX = 512


def _sweep_ts_marks(table: dict, cutoff: float, *, companions: tuple[dict, ...] = ()) -> None:
    """淘汰 ts 早于 cutoff 的条目(同步清 companion 表同键);仍超容量再按 ts 从旧到新削。"""
    if len(table) <= _MARK_MAX:
        return
    dropped = [k for k, ts in table.items() if ts < cutoff]
    if len(table) - len(dropped) > _MARK_MAX:
        rest = sorted((k for k in table if k not in dropped), key=lambda k: table[k])
        dropped += rest[: len(table) - len(dropped) - _MARK_MAX // 2]
    for k in dropped:
        table.pop(k, None)
        for c in companions:
            c.pop(k, None)


# 打回机制增强(章一;规划书 项目文档库):
# 类① 放行+接力 / 类② 拦截+重交。两类的防循环都靠同一套「同 scope 连续硬计数」结构
# (仿 _confirm_fired_at 的 (key, kind) 形态)。私聊合成轮用真实 user_id,不在
# _SYNTHETIC_USER_IDS 豁免内——身份豁免断不了链,只有硬计数能断(规划书 §3.2 第 5 条)。
_RELAY_CHAIN_MAX = 2    # 类① 接力上限:超限照发 + WARN
_BOUNCE_CHAIN_MAX = 1   # 类② 重交上限(规划书 §3.3):第二次检出起放行残片 + 贴便签


# 类① 继续执行轮内建提示词(规则可用 prompt 覆盖)
_CONTINUE_TASK_PROMPT = (
    "(系统提示:你上一条回复是任务回执,不是最终结果。"
    "现在实际执行该任务,完成后按正常流程(同一信封)输出最终回复)"
)


# 类② 重交轮内建提示词(规则可用 prompt 覆盖)
_RESUBMIT_REPLY_PROMPT = (
    "(系统提示:你上一条回复混入了协议 JSON 残片,已被拦截、未投递给用户。"
    "请重新给出最终回复;信封 JSON 只在轮末完整输出一次,正文里不要再出现字段名)"
)


# 类② 重交链达上限时贴的便签文案(规则给了 note 就用规则的)
_BOUNCE_LIMIT_NOTE = (
    "上一轮回复里混入了协议 JSON 残片(已被拦截、可能已投递)。"
    "信封 JSON 只在轮末完整输出一次,正文里不要再出现字段名。"
)


def _feedback_scope_key(adapter_name: str, is_private: bool, user_id: str, group_id: str | None) -> tuple[str, str]:
    """反馈便签/确认轮的 scope key:与 inflight 键同形(同一构造函数,勿手拼)。"""
    return make_inflight_key(adapter_name, is_private, user_id, group_id)


def _record_background_note(key: tuple[str, str], decision: dict, *, log_tag: str) -> None:
    """模型在 submit_decision 里填了 background_task → 登记离场任务便签(静默也登记)。"""
    note = decision.get("background_task")
    if isinstance(note, str) and note.strip():
        task_notes.set_task_note(key, note.strip()[:200])
        logger.info(f"[HERMES {log_tag}] 离场任务便签已登记: {note.strip()[:60]}")


async def _fire_departure_confirm(
    *,
    key: tuple[str, str],
    rule_name: str,
    prompt: str,
    cooldown_sec: float,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
) -> None:
    """追加一发离场确认轮(create_task 排队,经 inflight 接力在本 turn 后跑,不并发)。"""
    now = time.time()
    mark = (key, rule_name)
    if now - _confirm_fired_at.get(mark, 0) < cooldown_sec:
        logger.debug(f"[HERMES feedback] 确认轮冷却中,跳过 {key} rule={rule_name}")
        return
    _confirm_fired_at[mark] = now
    _sweep_ts_marks(_confirm_fired_at, now - _MARK_KEEP_SEC)
    _fire_followup_turn(
        key=key, tag=f"confirm:{rule_name}", prompt=prompt,
        bot=bot, target=target, adapter_name=adapter_name,
        user_id=user_id, group_id=group_id, is_private=is_private,
    )


async def _post_send_feedback(
    *,
    key: tuple[str, str],
    reply_text: str,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
    synthetic: bool = False,
    not_final: bool = False,
) -> None:
    """发送成功后的分级反馈检查(规则引擎驱动)。

    - 逐条评估 feedback_rules 里 where=reply/all 的规则(统一走 rule.matches:
      正则 / keywords+position / match=envelope_json 三种检出);
    - note → 写下轮暗示便签(按规则冷却去重),不当轮打断;
    - confirm → 按规则冷却追加离场确认轮;
    - relay(类①)→ 回复照发后再追加「继续执行轮」;not_final=true 同效(§3.4:
      同轮同填时 not_final 优先续跑,background_task 便签照登记);
    - bounce(类②)→ 不在这里处理:它是发送**前**的拦截(_maybe_intercept_bounce),
      能走到本函数说明该回复已放行。
    synthetic=True(巡查/确认轮等系统轮)不触发——防合成轮自己的措辞再命中递归。
    合成轮身份由调用方按 user_id 约定("system-check"/"timer"/"delivery")判定,
    不在 _handle/_run 签名上加参数。
    """
    if synthetic or not reply_text:
        return
    relay_rule = None
    for rule in load_rules():
        if rule.where not in ("reply", "all") or not rule.matches(reply_text):
            continue
        if rule.action == "note":
            task_notes.push_plugin_note(
                key, rule.name, rule.note, cooldown_sec=rule.cooldown_sec or 24 * 3600
            )
        elif rule.action == "confirm":
            await _fire_departure_confirm(
                key=key, rule_name=rule.name,
                prompt=rule.prompt or _DEPARTURE_CONFIRM_PROMPT,
                cooldown_sec=rule.cooldown_sec or 300.0,
                bot=bot, target=target, adapter_name=adapter_name,
                user_id=user_id, group_id=group_id, is_private=is_private,
            )
        elif rule.action == "relay" and relay_rule is None:
            relay_rule = rule

    # 类① 放行+接力:not_final 声明 或 命中 relay 规则 → 回复已发出,追加继续执行轮。
    if not_final or relay_rule is not None:
        if _chain_allow(key, "relay", _RELAY_CHAIN_MAX):
            _chain_bump(key, "relay")
            _fire_followup_turn(
                key=key,
                tag=f"relay:{relay_rule.name if relay_rule else 'not_final'}",
                prompt=(relay_rule.prompt if relay_rule and relay_rule.prompt else _CONTINUE_TASK_PROMPT),
                bot=bot, target=target, adapter_name=adapter_name,
                user_id=user_id, group_id=group_id, is_private=is_private,
            )
        else:
            # 达上限:回复照发(本函数本就在发送之后),只 WARN 不再接力。
            _chain_reset(key, "relay")
            logger.warning(
                f"[HERMES feedback] 接力链达上限({_RELAY_CHAIN_MAX}),照发不接力 {key} "
                f"tag={relay_rule.name if relay_rule else 'not_final'}"
            )
    else:
        # 普通投递:接力链在此归零(「连续」接力的定义边界)。
        _chain_reset(key, "relay")


def _match_bounce_rule(reply_text: str):
    """类②第一步:返回首条命中的 action=bounce 规则(where=reply/all),无则 None。

    总闸:缺省不拦截——没有 action=bounce 的规则时恒 None,系统无任何拦截行为。
    """
    if not reply_text:
        return None
    for rule in load_rules():
        if rule.where in ("reply", "all") and rule.action == "bounce" and rule.matches(reply_text):
            return rule
    return None


async def _maybe_intercept_bounce(
    *,
    key: tuple[str, str],
    reply_text: str,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
) -> bool:
    """类② 拦截+重交:命中 action=bounce 规则 → 不投递该回复,立刻追加重交轮。

    返回 True = 已拦截(调用方必须直接 return,不许发送);False = 放行(照常发送)。
    链条达上限(第二次检出起)→ 放行残片 + 按规则 note 贴便签下轮提醒 + WARN
    (规划书 §3.3:"宁让残片漏一次,不无限重交")。
    注意:合成轮身份豁免在这里**不适用**——重交轮自己再吐残片正是要放行的那一发,
    身份豁免会把它变成静默死循环;断链靠硬计数。
    """
    rule = _match_bounce_rule(reply_text)
    if rule is None:
        # 干净投递:与类①「普通投递即归零」同规——链条只在「连续」检出时累计,
        # 否则重交成功一次后,进程生命周期内的第二次泄漏会被直接放行(修于 max 终验)。
        _chain_reset(key, "bounce")
        return False
    if not _chain_allow(key, "bounce", _BOUNCE_CHAIN_MAX):
        _chain_reset(key, "bounce")
        logger.warning(
            f"[HERMES feedback] 重交链达上限({_BOUNCE_CHAIN_MAX}),残片放行并贴便签 {key} "
            f"rule={rule.name}"
        )
        task_notes.push_plugin_note(
            key, rule.name, rule.note or _BOUNCE_LIMIT_NOTE,
            cooldown_sec=rule.cooldown_sec or 600.0,
        )
        return False
    _chain_bump(key, "bounce")
    _fire_followup_turn(
        key=key, tag=f"bounce:{rule.name}", prompt=rule.prompt or _RESUBMIT_REPLY_PROMPT,
        bot=bot, target=target, adapter_name=adapter_name,
        user_id=user_id, group_id=group_id, is_private=is_private,
    )
    return True


def _check_interim_feedback(key: tuple[str, str], segment: str) -> None:
    """旁白段的规则检测(where=interim/all;仅 note 动作——旁白是过程
    文本,命中 confirm/relay/bounce 不当真,后续轮只跟正式回复)。"""
    if not segment:
        return
    for rule in load_rules():
        if rule.where in ("interim", "all") and rule.action == "note" and rule.matches(segment):
            task_notes.push_plugin_note(
                key, rule.name, rule.note, cooldown_sec=rule.cooldown_sec or 24 * 3600
            )


def _make_approval_notify(
    bot: Bot, adapter_name: str, scope_desc: str, *, is_private: bool, group_id: str | None
):
    """阶段3c:approval_callback 工厂——登记挂起审批并私聊通知管理员。

    群聊侧不发任何消息(审批是管理员私事);通知失败只记日志,不拖垮主 turn。
    """
    async def _notify(approval: dict) -> None:
        entry = approvals_add_pending(
            run_id=str(approval.get("run_id") or ""),
            adapter=adapter_name,
            scope_desc=scope_desc,
            command=str(approval.get("command") or approval.get("preview") or ""),
            choices=[str(c) for c in (approval.get("choices") or [])],
            is_private=is_private,
            group_id=group_id,
            # v0.22 authority 应答三键之一;通知帧自带,老形态没有则为空串
            request_id=str(approval.get("request_id") or ""),
        )
        sent = await approvals_notify_admins(
            bot=bot, adapter_name=adapter_name, entry=entry,
            admin_users=set(plugin_config.hermes_admin_users),
        )
        logger.info(
            f"[HERMES approval] 审批挂起 #{entry.approval_id} run={entry.run_id} "
            f"scope={scope_desc} 通知管理员 {sent} 人"
        )

    return _notify


async def _emoji_like_quiet(bot: Bot, msg_id, emoji_id: int) -> None:
    """贴 emoji 回执,失败只记 debug(阶段2d:429 丢单「丢了就贴」用)。

    QQ NT 协议下 set_msg_emoji_like 仅群聊可用——调用方自己保证群聊场景。
    """
    try:
        await bot.call_api("set_msg_emoji_like", message_id=msg_id, emoji_id=emoji_id)
    except Exception as e:
        logger.debug(f"[HERMES] emoji 回执失败 (msg_id={msg_id} emoji_id={emoji_id}): {e}")


# relay 域顶部 import 本域(_sweep_ts_marks),本域对 relay 的调用置底,
# 与 turns 域的底部 import 一起打破 feedback→relay→turns 加载环。
from .message_relay import (
    _chain_allow,
    _chain_bump,
    _chain_reset,
    _fire_followup_turn,
)
