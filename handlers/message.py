"""
消息处理器

priority=1 perception:写 MessageBuffer + BotRegistry,非阻塞
priority=98 main:触发判断 → reactive 决策 → 出向
"""

from __future__ import annotations

import asyncio
import re
import time
from contextlib import asynccontextmanager
from collections.abc import Sequence
from dataclasses import dataclass, field

import nonebot_plugin_alconna as alconna
from nonebot import logger, on_message
from nonebot.adapters import Bot, Event
from nonebot.matcher import Matcher
from nonebot.rule import Rule

from .. import mcp as _mcp  # lazy access to runtime singletons
from ..config import plugin_config
from ..core.active_session import ActiveSession, AnchorRecord, Participant
from ..core.approvals import add_pending as approvals_add_pending
from ..core.approvals import admin_user_ids
from ..core.approvals import notify_admins as approvals_notify_admins
from ..core.group_profile import build_group_profile_block, sync_buddy_to_group
from ..core import identity
from ..config import resolve_custom_dir
from ..core.hermes_client import (
    ChatResult,
    extract_response_media,
    hermes_client,
    maybe_extract_decision_reply_text,
    preview_raw,
)
from ..core.image_inline import inline_gif_frames, inline_image_frames
from ..core.media_config import MediaConfig, load_media_config
from ..core.inflight import PendingEntry, make_inflight_key, _SYNTHETIC_NICKNAME
from ..core.message_buffer import (
    BufferedMessage,
    build_media_address_index,
    media_kind_word,
)
from ..core.run_tracker import make_scope_key
from ..core import task_notes
from ..core.bg_tasks import spawn_bg_task
from ..core.feedback_rules import load_rules
from ..core.outbound import get_bot_nickname, send_local_files, send_text_with_media
from ..core.prompt_builder import (
    _format_speaker_tag,
    build_passive_system_prompt,
    build_passive_user_content,
    build_reactive_system_prompt,
    build_reactive_user_content,
    split_unpushed_messages,
)
from ..core.routing import resolve_target
from ..core.session import session_manager
from ..utils import check_isolation, get_adapter_name


async def _ignore_rule(event: Event) -> bool:
    try:
        msg_text = event.get_plaintext().strip()
    except Exception:
        return False
    if not msg_text:
        return True
    for prefix in plugin_config.hermes_ignore_prefix:
        if msg_text.startswith(prefix):
            return False
    return True


receive_message = on_message(rule=Rule(_ignore_rule), priority=98, block=True)
perception_message = on_message(priority=1, block=False)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_bot_at(uni_msg: alconna.UniMessage, bot_self_id: str) -> bool:
    if uni_msg.has(alconna.At):
        for seg in uni_msg[alconna.At]:
            if str(seg.target) == str(bot_self_id):
                return True
    return False


def _has_at_bot_in_original(event: Event, bot_self_id: str) -> bool:
    """检查 event.original_message 里是否有 @bot 段。

    OneBot v11 adapter 在分发事件前调 `_check_at_me`,把消息**开头/结尾**的 @bot 段
    从 event.message 中删除并置 to_me=True。下游 alconna 从 event.message 解析
    UniMessage 时已看不到那个 @bot,导致:
      - `_is_bot_at(uni_msg, ...)` 返回 False
      - 「@bot @other 怎么看」被当成只 @ 他人,is_mentioned 错误地为 False
      - emoji ack / chat 都不触发

    event.original_message 是 v11 在 _check_at_me 跑之前深拷贝的,@bot 还在那里。
    本函数兜底翻原始消息,告诉上游「用户确实 @ 了我」。

    非 OneBot adapter 一般无此剥离行为、不一定保留 original_message;`getattr` +
    try/except 让其他 adapter 安全返回 False(不退化原行为)。
    """
    original = getattr(event, "original_message", None)
    if not original:
        return False
    try:
        for seg in original:
            if getattr(seg, "type", None) != "at":
                continue
            data = getattr(seg, "data", None)
            if not isinstance(data, dict):
                continue
            if str(data.get("qq", "")) == str(bot_self_id):
                return True
    except Exception:
        pass
    return False


def _msg_at_only_other_users(uni_msg: alconna.UniMessage, bot_self_id: str) -> bool:
    """消息含 At 段、且所有 At target 都不是 bot 自身 → True。

    用于 reactive 入口 C 层过滤(模式 1 修复):active 窗口内,若消息明确 @ 了
    其他用户但**未点名 bot**,视作非本路径触发,只让 perception matcher 写 buffer,
    不进 chat() 决策。无 At 段时返回 False(走原有路径)。
    """
    if not uni_msg.has(alconna.At):
        return False
    return not _is_bot_at(uni_msg, bot_self_id)


# 本部署补丁(凌晨批项2): hermes_group_trigger 作废模式的
# 告警去重(每模式只 WARN 一次,重启清空)。
_TRIGGER_MODE_DEPRECATED_WARNED: set[str] = set()


def _is_blacklisted(group_id: str | None, user_id: str) -> bool:
    """黑名单判定(本部署补丁:凌晨批项3)。

    仅群聊生效(名单按群分组);私聊不受名单影响——私聊是显式对话,
    「不主动对话」语义不适用。
    """
    if not group_id:
        return False
    return user_id in plugin_config.hermes_blacklist.get(group_id, ())


# 单条昵称在 prompt 里的最大字符数。
# 中文 12 字 / 英文 24 字符,覆盖正常昵称;超长名片(常被用来塞动作短语/
# 系统消息伪装)会被截断成「前缀…」,降低被 LLM 当系统信号读的风险。
_MAX_NICKNAME_LEN = 24

# 伪造身份字段形态(等号全角化归一后的唯一形态):id＝xxx。
# 正常人不会起这种名片,见即没收(H4 修复,2026-10-16 维护者拍板)。
_FORGED_ID_RE = re.compile(r"[iI]\s*[dD]\s*＝\s*\S+")

# H7(2026-10-17):单例未装配 WARN 的节流状态(模块级,60s 一次)。
_unassembled_warned_at = 0.0


def _warn_unassembled_throttled(what: str) -> None:
    """启动窗口期单例未装配的 WARN,60s 节流:防启动潮时消息逐条刷屏。"""
    global _unassembled_warned_at
    now = time.time()
    if now - _unassembled_warned_at >= 60:
        _unassembled_warned_at = now
        logger.warning(
            f"[HERMES] 运行时单例未装配({what}),消息暂不处理"
            "——启动窗口期正常现象,若持续出现请查装配序列"
        )


def _sanitize_nickname(value) -> str | None:
    """清洗外部输入昵称,失败返 None。

    防御目标(顺手卫生 + 阻挡 [id=…] 定界符/字段伪装):
    1. 控制字符 / 换行 / 零宽 → 全删,防止把多行片段塞进 prompt
    2. `]` 全角化,防止把 [id=…]: 标签提前闭合伪装成系统标签
    3. `=` 全角化,拆掉标签字段分隔符(昵称不再能伪造字段)
    4. 归一化后没收 `id＝xxx` 伪造身份字段(大小写/空格变种一并落网)
    5. 长度 cap = _MAX_NICKNAME_LEN,超长截断 + 加省略号
    """
    if value is None:
        return None
    s = str(value)
    s = "".join(ch for ch in s if ch.isprintable())  # Cc/Cf/Cs 全过滤,ASCII space 保留
    s = s.replace("]", "］")  # 全角 `]`,与半角 `]` 视觉相近不破坏外观,但不闭合 [id=…] 标签
    # H4(2026-10-16 受控群实测实锤):先归一化再校验——等号全角化后,
    # 伪造身份字段收敛为唯一形态,没收并留 WARN 作案证据。
    s = s.replace("=", "＝")
    forged = _FORGED_ID_RE.findall(s)
    if forged:
        logger.warning(f"[HERMES 昵称注入拦截] 没收伪造身份字段 {forged},原昵称={value!r}")
        s = _FORGED_ID_RE.sub("", s)
    s = s.strip()
    if not s:
        return None
    if len(s) > _MAX_NICKNAME_LEN:
        s = s[:_MAX_NICKNAME_LEN] + "…"
    return s


def _extract_sender_nickname(event: Event, adapter_name: str) -> str | None:
    """从 event 抽真实昵称(群名片优先),失败回 None。所有命中字段都过 _sanitize_nickname。

    保持 cross-adapter,不 import adapter-specific 类型,全靠 getattr 链——
    各 adapter event 形状不一致,Python 重命名/缺失字段都吞掉。

    覆盖的形状:
    - OneBot v11/v12: event.sender.card(群名片) → event.sender.nickname
    - QQ Official / Kook 等 .author: event.author.{global_name,nickname,username,name}
    - Discord: event.member.nick(server 名片) → 同上 author 链兜底
    - Telegram: event.from_.first_name + last_name → username
    """
    sender = getattr(event, "sender", None)
    if sender is not None:
        n = _sanitize_nickname(getattr(sender, "card", None)) or _sanitize_nickname(getattr(sender, "nickname", None))
        if n:
            return n

    member = getattr(event, "member", None)
    if member is not None:
        n = _sanitize_nickname(getattr(member, "nick", None)) or _sanitize_nickname(getattr(member, "nickname", None))
        if n:
            return n

    author = getattr(event, "author", None)
    if author is not None:
        for attr in ("global_name", "nickname", "username", "name"):
            n = _sanitize_nickname(getattr(author, attr, None))
            if n:
                return n

    from_user = getattr(event, "from_", None) or getattr(event, "from_user", None)
    if from_user is not None:
        first = _sanitize_nickname(getattr(from_user, "first_name", None))
        last = _sanitize_nickname(getattr(from_user, "last_name", None))
        if first or last:
            # 合并后再过一次 sanitize,确保拼接结果也受长度上限约束
            return _sanitize_nickname(" ".join(p for p in (first, last) if p))
        n = _sanitize_nickname(getattr(from_user, "username", None))
        if n:
            return n

    return None


def _label_safe(value: str) -> str:
    """低危⑬(2026-10-14):引用归属标签的定界符加固。

    `(引用 昵称(id): …)` 形态里半角 ( ) : 是结构字符——群名片或异平台 id 里
    混入这些字符能伪造归属边界、往引用块注入假内容。全角化后语义不变、结构不破。
    只用于归属标签拼装;昵称的通用清洗仍是 _sanitize_nickname。
    注意:映射表必须用 \\u 转义写,直接写全角字面量会被上游管道规范化回半角
    (2026-10-14 踩过:replace("(", "(") 两侧同字节,等于没替换)。
    """
    _FW = {"(": "\uff08", ")": "\uff09", ":": "\uff1a", "[": "\uff3b", "]": "\uff3d"}
    return "".join(_FW.get(ch, ch) for ch in value)


def _extract_replied_sender_label(event: Event) -> str:
    """取被引消息的发送者标签 `昵称(user_id)`,取不到返回 ''。

    本部署补丁:信封的 `(引用: ...)` 原本不带被引消息归属,
    LLM 分不清引用的是 bot 自己的话、当前发言者的话还是第三方的话——「引用 A 的
    旧消息对 B 说『说的就是你』」会因归属缺失被误判成冲 bot 说(2026-10-13 群
    200000003 实测误接话)。OneBot v11 的 Reply 对象自带 sender(user_id/card/
    nickname),无需额外 API;昵称沿用 _extract_sender_nickname 的群名片优先约定
    并过 _sanitize_nickname 防 [user=…] 标签伪装。非 OneBot adapter 或无 sender
    字段时经 getattr 链安全落 '',下游回退旧的无归属格式,行为不退化。
    """
    try:
        reply = getattr(event, "reply", None)
        sender = getattr(reply, "sender", None)
        if sender is None:
            # 低危⑬(2026-10-14):v12 等 adapter 的 reply 不带 sender——记一笔
            # 可观测,不再静默吞;下游照旧回退无归属格式,行为不退化。
            logger.info("[HERMES] 引用消息无 sender 字段(adapter 缺归属信息),回退无归属格式")
            return ""
        user_id = _label_safe(
            "".join(ch for ch in str(getattr(sender, "user_id", "") or "") if ch.isprintable()).strip()
        )
        nickname = _sanitize_nickname(getattr(sender, "card", None)) or _sanitize_nickname(
            getattr(sender, "nickname", None)
        )
        if nickname:
            nickname = _label_safe(nickname)
        if nickname and user_id:
            return f"{nickname}({user_id})"
        return nickname or user_id
    except Exception:
        return ""


def _format_quote_prefix(replied_text: str, sender_label: str) -> str:
    """拼 `(引用 ...)` 前缀。sender_label 为空时回退旧的无归属形态。"""
    if sender_label:
        return f"(引用 {sender_label}: {replied_text})"
    return f"(引用: {replied_text})"


def _rewrite_quoted_ats(placeholders: list[str]) -> str:
    """引用内历史@改写:['@全体', '@<id>'] → '提及全体 提及<id>',空列表返 ''。

    本部署补丁(雷②, 维护者定案用「提及」):半角「@<bot_id>」
    随引用文本进 current_message 会字面命中 decision 协议「`@<你的 id>` 就是在 @ 你」
    的规则(2026-10-13 群 200000003 误接话事故:引用含 @bot 的旧消息 + 当前在对
    别人说话,被判成冲 bot 说)。改写后 id 信息保留、字面 token 断开,「提及」与@
    语义等价。只作用于引用块内——当前消息自身的 @ 占位(at_placeholders)维持
    「@<id>」原格式,协议块靠「块外才是当前@」区分两者。
    """
    return " ".join("提及" + p.removeprefix("@") for p in placeholders)


# 字面手打 @ 的有界匹配:仅 QQ 号段(5-12 位数字,后不接数字),别碰邮箱/@英文名。
_QUOTED_LITERAL_AT_RE = re.compile(r"@(\d{5,12})(?!\d)")
_QUOTED_LITERAL_ATALL_RE = re.compile(r"@全体(?:成员)?")


def _rewrite_quoted_literal_ats(text: str) -> str:
    """引用文本里字面手打的「@QQ号」/「@全体」改写「提及」(2026-10-13 审核修复 中危⑥)。

    _rewrite_quoted_ats 只覆盖 At 段派生的占位;手打进文本的 @数字 不经过它,
    会字面命中决策协议「@<id>=@你」规则——群 200000003 事故的同形态残余面。
    这里补一道有界兜底(只认 QQ 号段,不误伤邮箱/英文@)。只作用引用块内,
    当前消息正文不经过本函数。
    """
    text = _QUOTED_LITERAL_ATALL_RE.sub("提及全体", text)
    return _QUOTED_LITERAL_AT_RE.sub(r"提及\1", text)


# --- 2026-10-13 错误分级反馈 + 离场确认轮(维护者合成方案;规则引擎见 core/feedback_rules.py) ---
# 后续轮一族(2026-10-16 章一合族,Q1):规则命中 → 追加指定 prompt 的后续轮,prompt 分族——
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

# 2026-10-16 打回机制增强(章一;规划书 项目文档库):
# 类① 放行+接力 / 类② 拦截+重交。两类的防循环都靠同一套「同 scope 连续硬计数」结构
# (仿 _confirm_fired_at 的 (key, kind) 形态)。私聊合成轮用真实 user_id,不在
# _SYNTHETIC_USER_IDS 豁免内——身份豁免断不了链,只有硬计数能断(规划书 §3.2 第 5 条)。
_RELAY_CHAIN_MAX = 2    # 类① 接力上限(维护者 2026-10-16 拍板 Q4):超限照发 + WARN
_BOUNCE_CHAIN_MAX = 1   # 类② 重交上限(规划书 §3.3):第二次检出起放行残片 + 贴便签

# (scope_key, kind) → 连续点火计数(kind ∈ {"relay", "bounce"};普通投递即归零)
_chain_counts: dict[tuple[tuple[str, str], str], int] = {}
# (scope_key, kind) → 最近一次点火时刻(内存兜底淘汰用,不参与计数语义;
# 值保持 int 不变,既有回归按整数断言)
_chain_touched: dict[tuple[tuple[str, str], str], float] = {}

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

# 私聊排队汇总成批(条目 23,规划书章二,topic: 项目文档库 §4):
# turn 收尾→接力的边界才是链式连发的真凶——slot 一释放,尾随消息就各自 try_enter
# 成功、各自开一轮;而每个接力 turn 本身又是一整轮 LLM,期间新消息继续补货。
# 修法:收尾发现 pending 非空时不立即 pop+refire,而是**先不释放 slot**、等一个聚合窗,
# 窗内新到消息经 try_enter 自然入队并同委托人合并(现成机制),窗后一次性 pop 开轮。
# 维护者 2026-10-16 拍板 Q5:**全量启用**(不限长 turn);窗长按候选区间 3-5s 定稿 4s
# (对齐应答防抖量级:防抖 2.5s 只覆盖同 turn 内的碎片,治不了跨 turn 边界)。
_QUEUE_AGGREGATE_WINDOW_SEC = 4.0


def _chain_allow(key: tuple[str, str], kind: str, limit: int) -> bool:
    """同 scope 连续计数未达上限 → 允许点火。"""
    return _chain_counts.get((key, kind), 0) < limit


def _chain_bump(key: tuple[str, str], kind: str) -> None:
    _chain_counts[(key, kind)] = _chain_counts.get((key, kind), 0) + 1
    _chain_touched[(key, kind)] = time.time()
    # 链条计数只在「连续点火」期间有意义,对端不再说话的残留按闲置淘汰
    _sweep_ts_marks(_chain_touched, time.time() - 3600, companions=(_chain_counts,))


def _chain_reset(key: tuple[str, str], kind: str) -> None:
    _chain_counts.pop((key, kind), None)
    _chain_touched.pop((key, kind), None)


def _fire_followup_turn(
    *,
    key: tuple[str, str],
    tag: str,
    prompt: str,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    is_private: bool,
) -> None:
    """追加一发合成后续轮(create_task 排队,经 inflight 接力在本 turn 后跑,不并发)。

    全族唯一出口(2026-10-16 Q1 合族):离场确认轮 / 类①继续执行轮 / 类②重交轮都走这里,
    只换 tag 与 prompt。合成身份沿用现有约定:群聊 "system-check"(命中 _SYNTHETIC_USER_IDS
    豁免),私聊真实 user_id + 昵称「系统核对」(不在豁免内,靠硬计数断链)。
    """
    logger.info(f"[HERMES feedback] 追加后续轮({tag}) {key}")
    if is_private:
        spawn_bg_task(
            _handle_passive_path(
                bot=bot, target=target, adapter_name=adapter_name,
                user_id=user_id, nickname=_SYNTHETIC_NICKNAME, group_id=None,
                text=prompt, image_urls=[], is_private=True,
                now_ms=_now_ms(),
            )
        )
    else:
        spawn_bg_task(
            _handle_reactive_path(
                bot=bot, target=target, adapter_name=adapter_name,
                user_id="system-check", nickname=_SYNTHETIC_NICKNAME, group_id=group_id,
                text=prompt, image_urls=[], is_explicit_trigger=False,
                addressed_to_bot=False, now_ms=_now_ms(), ephemeral=True,
            )
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
    """发送成功后的分级反馈检查(2026-10-13 维护者合成方案,规则引擎驱动)。

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
    """旁白段的规则检测(2026-10-13,where=interim/all;仅 note 动作——旁白是过程
    文本,命中 confirm/relay/bounce 不当真,后续轮只跟正式回复)。"""
    if not segment:
        return
    for rule in load_rules():
        if rule.where in ("interim", "all") and rule.action == "note" and rule.matches(segment):
            task_notes.push_plugin_note(
                key, rule.name, rule.note, cooldown_sec=rule.cooldown_sec or 24 * 3600
            )


# 合成轮 user_id 约定:这些来源的措辞不进入分级反馈检测(防确认轮/巡查提示词递归命中)
_SYNTHETIC_USER_IDS = frozenset({"system-check", "timer", "delivery"})


_ACK_CANCEL_UNSUPPORTED_WARNED: set[str] = set()
"""Bot self_id 集合: 已经 WARN 过"撤销 emoji 在此 OneBot 实现端不支持"的 bot。

模块级 dedupe, 避免每个 turn 都刷 WARN。重启清空 (set), 不持久。"""


@asynccontextmanager
async def _ack_scope(
    bot: Bot,
    event: Event,
    *,
    adapter_name: str,
    is_explicit_trigger: bool,
    is_private: bool,
):
    """B-0: OneBot v11 emoji ack 回执 (set 进 / clear 出)。

    适用条件 (全部满足):
      - hermes_ack_feedback_enabled = True
      - is_explicit_trigger = True (仅用户主动 @ bot, bystander/notice 不贴)
      - adapter_name = 'onebotv11'
      - is_private = False (QQ NT 协议:emoji reactions 是群聊 only,
        私聊调 set_msg_emoji_like 实现端会 raise "只支持群聊消息")
      - event.message_id 可取

    撤销路径两套兼容:
      - LLOneBot 风格: 独立 endpoint `unset_msg_emoji_like`
      - NapCat 风格:   同 endpoint `set_msg_emoji_like` 加 set=False
    先试 LLOneBot 路径, 失败 fallback NapCat 路径。

    silently-fail 路径: 任何 API 错误吞掉, 绝不阻塞真实回复。
    set 失败 → 不尝试 clear (避免无意义错误日志); set 成功 → clear 在 finally 内,
    chat() 抛异常 / 取消 / Ctrl-C 都会触发清理。

    **已知限制**: 老版本 LLOneBot (< 大约 2024 中) 两条撤销路径都不支持
    (unset endpoint 不存在, set=False 也不被识别)——emoji 会持久存在,
    我们一次性 WARN 告知用户、建议升级 / 切 NapCat / 接受持久标记。

    notice 触发的 synthesized 路径 (戳一戳/入群) 不进入本 scope——它们没有'原消息'可贴。
    """
    enabled = (
        plugin_config.hermes_ack_feedback_enabled
        and is_explicit_trigger
        and adapter_name == "onebotv11"
        and not is_private
    )
    if not enabled:
        yield
        return

    msg_id = getattr(event, "message_id", None)
    if msg_id is None:
        yield
        return

    emoji_id = plugin_config.hermes_ack_emoji_id
    set_ok = False
    try:
        # 添加表情两边一致: set_msg_emoji_like(message_id, emoji_id) 即可
        # (NapCat 的 set 参数 Optional 默认 true, LLOneBot 无该参数)。
        await bot.call_api("set_msg_emoji_like", message_id=msg_id, emoji_id=emoji_id)
        set_ok = True
    except Exception as e:
        logger.debug(f"[HERMES ack] set failed (msg_id={msg_id} emoji_id={emoji_id}): {e}")

    try:
        yield
    finally:
        if set_ok:
            # 撤销表情两边风格不同:
            #   - LLOneBot: 独立 endpoint `unset_msg_emoji_like`
            #   - NapCat:   同 endpoint `set_msg_emoji_like` 加 set=False
            # 先试 LLOneBot 路径, 失败 fallback 到 NapCat 路径。两边都不识别就静默。
            cleared = False
            unset_unsupported = False
            try:
                await bot.call_api("unset_msg_emoji_like", message_id=msg_id, emoji_id=emoji_id)
                cleared = True
            except Exception as e:
                err_str = str(e).lower()
                # OneBot 标准 retcode 1404 = "不支持的 api"; 不同实现端也可能用 "unsupported"
                # 或直接走 retcode != 0 + message 含 "不支持". 字串兜底, 即使匹配漏了
                # 也只是少一次 WARN, 主功能不受影响。
                unset_unsupported = "1404" in err_str or "unsupported" in err_str or "不支持" in err_str
                logger.debug(f"[HERMES ack] unset failed (LLOneBot path, msg_id={msg_id}): {e}")
            if not cleared:
                try:
                    await bot.call_api(
                        "set_msg_emoji_like",
                        message_id=msg_id,
                        emoji_id=emoji_id,
                        set=False,
                    )
                except Exception as e:
                    logger.debug(f"[HERMES ack] clear-via-set=false failed (NapCat path, msg_id={msg_id}): {e}")
                else:
                    # set=False 没抛错。但如果是老 LLOneBot, 它会接受请求却不真正撤销
                    # (LLOneBot 早期 set 参数不识别)。和 unset 不支持是同一类版本陈旧问题——
                    # 一次性 WARN 告知用户。NapCat / 新 LLOneBot 不会进这个分支
                    # (走 unset 那条已 cleared=True)。
                    if unset_unsupported:
                        bot_id = str(bot.self_id)
                        if bot_id not in _ACK_CANCEL_UNSUPPORTED_WARNED:
                            _ACK_CANCEL_UNSUPPORTED_WARNED.add(bot_id)
                            logger.warning(
                                f"[HERMES ack] bot {bot_id} 所在 OneBot 实现端不支持撤销 emoji "
                                f"(unset_msg_emoji_like 1404; set=False 也可能空转)。emoji 将永久标记。"
                                f"建议: 升级 LLOneBot / 改用 NapCat / 设 HERMES_ACK_FEEDBACK_ENABLED=false。"
                            )


async def _resolve_image_segment_url(img, bot: Bot, adapter_name: str) -> str | None:
    """把单个 alconna.Image 段解析成可 HTTP GET 拿字节的 URL;解析不了返回 None。

    多 adapter 行为不一致:
    - OneBot v11 / QQ Official / Discord 等:alconna 直接在 Image 段上填好 `.url`,
      最廉价路径,优先用
    - Telegram:alconna 只填 `.id`(就是 file_id),URL 必须二次调
      `bot.get_file(file_id)` 拿到 `file_path` 后拼成 `https://api.telegram.org/
      file/bot<TOKEN>/<file_path>`。这条路径 URL 里**带 token**,但 fetcher
      和 DB 都本地,落地可接受;且 file_path 只有 ~1h 有效,异步 fetcher 必须
      及时抓字节进 ImageCache,后面 MCP 工具读 cache 不再依赖 URL
    - 其他 adapter:只看 `.url`,没的话就放弃这张图(不抛、不 fail bot)
    """
    adapter_lc = (adapter_name or "").lower()
    url = getattr(img, "url", None)
    if url and isinstance(url, str) and url.startswith(("http://", "https://")):
        return url
    file_id = getattr(img, "id", None)
    if not file_id:
        return None
    if "telegram" in adapter_lc:
        return await _resolve_telegram_file_url(bot, file_id)
    # 其他 adapter 但 Image 没 .url 的情况:debug 一行,不当错误处理
    logger.debug(
        f"[image] skipped image segment with no resolvable URL (adapter={adapter_lc} id={file_id[:24]}...)"
    )
    return None


async def _extract_image_urls(uni_msg: alconna.UniMessage, bot: Bot, adapter_name: str) -> list[str]:
    """抽出普通图片 URL 列表(可直接 HTTP GET 拿字节的那种)。

    QQ 大表情包(Image.sticker=True)不进本列表,由 _extract_sticker_urls 单独抽;
    是否喂给模型见 config.hermes_sticker_vision_enabled。
    """
    urls: list[str] = []
    if not uni_msg.has(alconna.Image):
        return urls
    for img in uni_msg[alconna.Image]:
        # B-0: QQ 大表情包不进普通 vision URL list,避免和正常图片混同语义
        if getattr(img, "sticker", False):
            continue
        url = await _resolve_image_segment_url(img, bot, adapter_name)
        if url:
            urls.append(url)
    return urls


async def _extract_sticker_urls(uni_msg: alconna.UniMessage, bot: Bot, adapter_name: str) -> list[str]:
    """抽出 QQ 大表情包(Image.sticker=True)URL 列表,与普通图片分开。

    本函数只负责解析;是否内联喂模型由调用方按 hermes_sticker_vision_enabled 门控。
    """
    urls: list[str] = []
    if not uni_msg.has(alconna.Image):
        return urls
    for img in uni_msg[alconna.Image]:
        if not getattr(img, "sticker", False):
            continue
        url = await _resolve_image_segment_url(img, bot, adapter_name)
        if url:
            urls.append(url)
    return urls

# 本部署补丁(图片投送管线三模式): 取代 2026-09-29 修改3 的
# addressed 单点门控。设计与参数定案见 项目文档库。
def _collect_inline_image_urls(
    media_cfg: MediaConfig,
    *,
    window_msgs: Sequence[BufferedMessage],
    current_image_urls: Sequence[str],
    current_user_id: str,
    addressed_to_bot: bool,
    self_id: str,
) -> list[str]:
    """按三模式收集本轮待内联的图片 URL(旧→新序;URL 级去重在编码入口做)。

    - off: 全占位符,返回 [](例外名单不生效,统统不内联;条目 off/semi 也救不回)。
    - full: window_msgs(**增量窗口**,新→旧序)内所有图 + 当前消息图。
    - semi: 以最新消息为锚的前后窗口(window_msgs 是新→旧序,开头即当前消息
      紧邻段)内**存在 @bot**(当前消息 addressed 即满足;其余消息看 content
      里 `@<self_id>` 字面——self_id 运行时传入,不锚定任何平台账号格式)
      才内联窗口内图 + 当前消息图。

    例外名单(审计修复批 2026-10-16 实装,维护者补充拍板:只实装两档):
    - 条目 off:该用户图永不内联(仍可被 get_message_images 拉图);
    - 条目 semi:该用户图按半内联规则收窄——总 full 下不再恒收,只在 semi 窗口
      命中时收,且范围限 2N+1 半窗;总 semi 下与默认一致;
    - 无条目:跟随总模式。总模式 off 仍全灭(优先级最高,前面早退)。
    (full 值不实装:解析层遇 full 按未支持值 WARN 跳过,见 media_config._parse_exemptions。)
    当前消息同理按其发言者的有效模式判定。
    """
    if media_cfg.mode == "off":
        return []

    def _eff_mode(uid: str) -> str:
        """每用户有效模式:条目 semi 覆盖总模式;off 恒 off;无条目跟随总模式。"""
        entry = media_cfg.exemption_for(uid)
        if entry == "off":
            return "off"
        if entry == "semi":
            return "semi"
        return media_cfg.mode

    # semi 窗口命中判定(沿用 addressed/窗口内 @bot 逻辑;总 full 下也现算——
    # semi 条目的用户要按同一判定门收窄)。window_msgs 新→旧序:开头 2N+1 条=当前
    # 消息及其前后窗口(当前消息未入库时为紧邻的前 N 条,多出的后 N 条近似无害)。
    n = media_cfg.semi_window
    window_len = min(len(window_msgs), 2 * n + 1) if n > 0 else 0
    window = list(window_msgs[:window_len])
    semi_hit = addressed_to_bot or any(f"@{self_id}" in (m.content or "") for m in window)

    out: list[str] = []
    # 旧→新序收集:图 parts 按旧→新排,与 recent_messages 渲染序一致。
    # 逐条按该行发言者的有效模式判定:off 跳过;semi 需窗口命中且落在 2N+1 半窗内
    # (semi 语义本身的范围,follow 总模式 semi 时即现状);full(=无条目跟随总 full)
    # 恒收,范围即整个 window_msgs。
    for i in range(len(window_msgs) - 1, -1, -1):
        m = window_msgs[i]
        eff = _eff_mode(m.user_id)
        if eff == "off":
            continue
        if eff == "semi" and (not semi_hit or i >= window_len):
            continue
        out.extend(m.image_urls)
    # 当前消息的图(同样按发言者有效模式):full 恒收;semi 窗口命中时收;
    # off / semi 未命中不收(没让 bot 看,图留占位符——可拉图)。
    eff_cur = _eff_mode(current_user_id)
    if eff_cur == "full" or (eff_cur == "semi" and semi_hit):
        for u in current_image_urls:
            if u not in out:
                out.append(u)
    return out


def _collect_nontext_placeholders(uni_msg: alconna.UniMessage) -> list[str]:
    """扫描非文本/普通图段,返回占位文本列表 (顺序近似按段类型聚合)。

    覆盖:
      - Image.sticker=True (QQ 大表情包) → [表情包]
      - Voice → [语音]
      - Video → [视频]
      - Emoji (QQ face 段) → [表情:<name>] 或 [表情] (name 缺失时)
      - File → [文件:<name>] 或 [文件:未命名] (name 缺失时)

    与现有 [图片] 占位策略一致——仅追加到 msg_text 末尾,不试图与文本段 interleave。
    普通 (非 sticker) Image 不在本函数处理,沿用 _extract_image_urls + [图片] 占位流。
    """
    placeholders: list[str] = []
    if uni_msg.has(alconna.Image):
        for img in uni_msg[alconna.Image]:
            if getattr(img, "sticker", False):
                placeholders.append("[表情包]")
    if uni_msg.has(alconna.Voice):
        for _v in uni_msg[alconna.Voice]:
            placeholders.append("[语音]")
    if uni_msg.has(alconna.Video):
        for _v in uni_msg[alconna.Video]:
            placeholders.append("[视频]")
    if uni_msg.has(alconna.Emoji):
        for face in uni_msg[alconna.Emoji]:
            name = getattr(face, "name", None)
            placeholders.append(f"[表情:{name}]" if name else "[表情]")
    if uni_msg.has(alconna.File):
        # File 与 Emoji 的 fallback 风格故意不一致:文件 metadata 几乎只剩"有/没有名字"这两个信号,
        # 用 [文件:未命名] 保住"曾有文件且名字丢了"的语义,比裸 [文件] 更具信息量。
        for f in uni_msg[alconna.File]:
            name = getattr(f, "name", None) or "未命名"
            placeholders.append(f"[文件:{name}]")
    return placeholders


def _collect_at_placeholders(
    uni_msg: alconna.UniMessage,
    *,
    event: Event | None = None,
    bot_self_id: str | None = None,
) -> list[str]:
    """扫描 At / AtAll 段,返回 ['@全体', '@<user_id>', ...] 占位列表。

    alconna `extract_plain_text()` 默认丢掉 At 段,Hermes 在 prompt 里看不到
    "当前消息 @ 了谁"。在引用 bot 旧消息 + @ 群友 这类场景下,LLM 只能凭
    引用关系判归属,误判"归你"。把 At 信息按 `@<target>` 回填到 plain text
    末尾,让 decision_protocol 的"@ 了别人 → 不归你"规则有可用证据。

    AtAll 放在 At 之前 — `@全体` 是更强的寻址信号,优先呈现。

    target 一律按字符串输出。bot 自己被 @ 时不特判,Hermes 通过对比 recent_messages
    里 [bot] 行的 `id=` 字段即可识别。

    若同时给出 event + bot_self_id 且 uni_msg 里没 @bot 但 event.original_message 里
    有(OneBot v11 _check_at_me 剥走的情况),把 `@<bot_self_id>` 补到列表开头 ——
    否则 Hermes 在多 @ 场景下看到「只 @ 了他人」会按 decision_protocol 抑制回复。
    """
    placeholders: list[str] = []
    if uni_msg.has(alconna.AtAll):
        for _ in uni_msg[alconna.AtAll]:
            placeholders.append("@全体")
    if uni_msg.has(alconna.At):
        for at in uni_msg[alconna.At]:
            placeholders.append(f"@{at.target}")
    if event is not None and bot_self_id is not None:
        bot_at_visible = any(p == f"@{bot_self_id}" for p in placeholders)
        if not bot_at_visible and _has_at_bot_in_original(event, bot_self_id):
            placeholders.insert(0, f"@{bot_self_id}")
    return placeholders


# --- Phase B-1: 合并转发消息提取 ---


def _node_summary(node: dict, *, image_labels=None) -> str | None:
    """将 OneBot get_forward_msg 返回的单条节点转成单行摘要。

    节点结构在不同 OneBot 实现端之间不完全统一,主要存在两种:
      A) flat + content (NapCat / LuckyLilliaBot 现行):
         {"sender": {"nickname": "...", "card": "..."}, "content": [...segments...],
          "time": ..., "message_format": "array", "message_type": "..."}
      B) flat + message (旧 go-cqhttp / 部分 LLOneBot 版本):
         {"sender": {...}, "name": "...", "message": [...segments...]}
    本函数对二者都接;优先取 content,缺则取 message。

    image_labels: 可选的 image 段标签迭代器(与 _node_image_urls 同序),给定时每个
    image 段取一个标签(如 `[转发图#3]`);缺省则该段用 `[图片]`(兼容旧行为)。

    空内容节点返回 None(调用方跳过,避免裸 'Unknown: ' 行)。
    """
    sender = node.get("sender", {})
    nickname = sender.get("nickname") or sender.get("card") or node.get("name") or "Unknown"
    parts: list[str] = []
    segs = node.get("content") or node.get("message") or []
    for seg in segs:
        seg_type = seg.get("type", "")
        data = seg.get("data", {}) if isinstance(seg.get("data"), dict) else {}
        if seg_type == "text":
            text = data.get("text", "")
            if text:
                parts.append(text)
        elif seg_type == "image":
            if image_labels is not None:
                parts.append(next(image_labels, "[图片]"))
            else:
                parts.append("[图片]")
        elif seg_type == "record":
            parts.append("[语音]")
        elif seg_type == "video":
            parts.append("[视频]")
        elif seg_type == "file":
            name = data.get("name") or data.get("file") or "未命名"
            parts.append(f"[文件:{name}]")
        elif seg_type == "face":
            name = data.get("name")
            parts.append(f"[表情:{name}]" if name else "[表情]")
        elif seg_type == "forward":
            content = data.get("content")
            count = len(content) if isinstance(content, list) else "?"
            parts.append(f"[嵌套合并转发 ({count} 条)]")
        # other types → skip
    joined = "".join(parts).strip()
    if not joined:
        return None
    return f"{nickname}: {joined}"


def _node_image_urls(node: dict) -> list[str | None]:
    """节点内 image 段的 url(按段序);无 url 的段给 None。

    与 _node_summary 对同一段列表按同一顺序遍历,产出的标签一一对应(供转发图发号)。
    取 data['url'](NapCat get_forward_msg 的 image 段带 url);拿不到就退 [图片] 占位。
    """
    urls: list[str | None] = []
    segs = node.get("content") or node.get("message") or []
    for seg in segs:
        if not isinstance(seg, dict) or seg.get("type") != "image":
            continue
        data = seg.get("data", {}) if isinstance(seg.get("data"), dict) else {}
        u = data.get("url")
        urls.append(str(u) if u else None)
    return urls


# 折叠转发 get_forward_msg 的短 TTL 结果缓存(2026-10-16 同源化收口批,审计确认4):
# 同一条消息的同一 ref 会被 perception(priority=1)与主 handler(priority=98)各调
# 一次 _extract_forward_full;被引消息本身是折叠转发时那段还要再各调一次——同一
# 事件最多 4 发 get_forward_msg,而两层拿到的是同一次 API 响应应有的同一结果。
# 与 Telegram _resolved_url_cache 同款:模块级 dict + 单调时钟 TTL + 兜底清过期,
# 生命周期靠 TTL 自然滚出(不是常驻存储)。键按 bot 实例 + ref_id,另含
# number_base/cap 这两个决定输出形态的参数(两层口径一致=命中;未来漂移=只 miss)。
_FORWARD_FULL_TTL_S = 60.0
# 键按 bot 身份(self_id)而非 Bot 对象本体(本部署补丁):以对象做键会把死实例
# 锚在 dict 里不释放,重连换新实例后旧条目也永远 miss。无 self_id 的调用方
# (离线冒烟的 FakeBot)退化 id(bot),实例天然隔离的语义不变。
_forward_full_cache: dict[
    tuple[str, str, int | None, int | None], tuple[str, tuple[str, ...], float]
] = {}


async def _extract_forward_full(
    uni_msg: alconna.UniMessage,
    bot: Bot,
    *,
    adapter_name: str,
    number_base: int | None = None,
    cap: int | None = None,
) -> tuple[str | None, list[str]]:
    """提取合并转发消息,返回 (块文本, 转发图 URL 列表)。

    块文本形如 <forwarded_messages count="N">...</forwarded_messages>;仅支持
    onebotv11/onebotv12(其他适配器返回 (None, []));get_forward_msg 失败时返回
    自闭合 fetch_failed 标签,不抛出。

    number_base: 给定时启用转发图发号——把每个节点内 image 段原位渲染成
    `[转发图#x]`(x = number_base + 本条消息转发图的连续编号),并把注册到的转发图
    URL 按节点顺序返回,供入库 kind='forward_image'(规划书 §8)。同一次 get_forward_msg
    响应内提取,不二次调用 API;跨 matcher 的双调用由 _forward_full_cache 短 TTL
    缓存兜住(2026-10-16 同源化收口批,审计确认4),不重复打 API。为 None 时不发号、不注册,节点图保持 `[图片]`
    (用于被引消息里的折叠转发:那些图不属本条消息,不占本条消息的 idx)。
    cap: 本消息转发图注册上限;None 时现读 media_config.forward_image_cap。
    超出上限按内容(URL)去重截断 + WARN。
    """
    # TODO(B-1.3): onebotv12 uses the same `get_forward_msg` API name based on OneBot
    # spec convergence, but this has not been verified against a live v12 deployment.
    # If the v12 API name differs, the call below will raise and we'll return the
    # fetch_failed self-closing tag — degraded but non-crashing. Verify with a real
    # v12 adapter and either confirm or branch the call.
    if adapter_name not in {"onebotv11", "onebotv12"}:
        return None, []

    refs = uni_msg[alconna.Reference]
    ref_id: str | None = None
    for ref in refs:
        if ref.id:
            ref_id = ref.id
            break
    if ref_id is None:
        return None, []

    # 短 TTL 缓存命中即返(见 _forward_full_cache 注释)。命中时返回新列表副本,
    # 不与缓存共享可变对象。
    cache_key = (str(getattr(bot, "self_id", "") or id(bot)), ref_id, number_base, cap)
    now_mono = time.monotonic()
    cached = _forward_full_cache.get(cache_key)
    if cached is not None and cached[2] > now_mono:
        return cached[0], list(cached[1])

    try:
        resp = await bot.call_api("get_forward_msg", id=ref_id)
    except Exception as exc:
        logger.warning(
            f"[HERMES forward] get_forward_msg failed adapter={adapter_name} "
            f"ref_id={ref_id}: {type(exc).__name__}: {exc}"
        )
        return '<forwarded_messages count="?" status="fetch_failed"/>', []

    nodes: list = []
    if isinstance(resp, dict):
        nodes = resp.get("messages") or []
    elif isinstance(resp, list):
        nodes = resp

    total_nodes = len(nodes)

    max_nodes = plugin_config.hermes_forward_extract_max_nodes
    max_chars = plugin_config.hermes_forward_extract_max_chars

    numbering = number_base is not None
    effective_cap = 0
    if numbering:
        effective_cap = cap if cap is not None else load_media_config().forward_image_cap
    seen_img_urls: set[str] = set()
    registered: list[str] = []
    img_truncated = False

    lines: list[str] = []
    total_chars = 0
    omitted = 0  # default: loop finished naturally, nothing hidden

    for i, node in enumerate(nodes):
        if numbering:
            image_labels: list[str] = []
            for u in _node_image_urls(node):
                if u is not None and u not in seen_img_urls and len(registered) >= effective_cap:
                    # 超上限:该图不发号(占位),并记账避免同 url 反复告警。
                    img_truncated = True
                    seen_img_urls.add(u)
                if u is None or u in seen_img_urls:
                    image_labels.append("[图片]")
                else:
                    seen_img_urls.add(u)
                    registered.append(u)
                    image_labels.append(f"[转发图#{number_base + len(registered) - 1}]")
            summary = _node_summary(node, image_labels=iter(image_labels))
        else:
            summary = _node_summary(node)
        if summary is None:
            continue
        next_len = total_chars + len(summary) + (1 if lines else 0)
        if next_len > max_chars and lines:
            # Char-limit truncation requires at least one line already collected — better to
            # overshoot max_chars with a single huge first node than to emit a content-less
            # wrapper. Rare in practice (a 2KB single node under an 800-char cap), but the
            # policy is "always show something."
            lines.append("[...因字符上限截断]")
            omitted = 0  # char path doesn't report a numeric "other N"
            break
        lines.append(summary)
        total_chars = next_len
        if len(lines) >= max_nodes:
            # node truncation: count remaining indices not yet examined
            omitted = total_nodes - (i + 1)
            if omitted > 0:
                lines.append(f"[...另有 {omitted} 条已省略]")
            break

    if img_truncated:
        logger.warning(
            f"[HERMES forward] 转发图注册达上限 cap={effective_cap},超出节点图按 [图片] 占位"
            "(只注册不内联,按需可拉)"
        )

    if not lines:
        return '<forwarded_messages count="?" status="fetch_failed"/>', registered

    content = "\n".join(lines)
    block = f'<forwarded_messages count="{total_nodes}">\n{content}\n</forwarded_messages>'
    # 成功结果入缓存(失败/空块路径不落缓存,下一层照常重试)。
    _forward_full_cache[cache_key] = (block, tuple(registered), now_mono + _FORWARD_FULL_TTL_S)
    if len(_forward_full_cache) > 256:
        expired = [k for k, (_b, _u, exp) in _forward_full_cache.items() if exp <= now_mono]
        for k in expired:
            _forward_full_cache.pop(k, None)
    return block, registered


# 自闭合 forwarded_messages 标签(fetch_failed / preview 两种变体共用此形态)。
# _summarize_forward 的透传判断与 _forward_full_for_store 的落库过滤共用。
_FORWARD_SELF_CLOSING_RE = re.compile(r"<forwarded_messages [^>]+/>")


def _summarize_forward(full_block: str, *, max_chars: int = 120) -> str:
    """将 _extract_forward_full 返回的多行块压缩为单行自闭合预览标签。

    用于 MessageBuffer / perception buffer 存储,避免 <recent_messages> 过度膨胀。
    自闭合输入(含 fetch_failed 变体)原样返回。
    """
    # Already self-closing → unchanged
    if _FORWARD_SELF_CLOSING_RE.match(full_block.strip()):
        return full_block.strip()

    # Extract count attribute
    count_match = re.search(r'count="([^"]*)"', full_block)
    count_val = count_match.group(1) if count_match else "?"

    # Extract inner lines (between opening and closing tag)
    inner_match = re.search(r"<forwarded_messages [^>]*>\n(.*?)\n</forwarded_messages>", full_block, re.DOTALL)
    if not inner_match:
        return f'<forwarded_messages count="{count_val}" preview=""/>'

    inner = inner_match.group(1)
    raw_lines = [ln.strip() for ln in inner.splitlines() if ln.strip()]

    # Overhead: '<forwarded_messages count="N" preview=""/>'
    overhead = len(f'<forwarded_messages count="{count_val}" preview=""/>')
    budget = max_chars - overhead

    preview_parts: list[str] = []
    used = 0
    for line in raw_lines:
        compressed = line[:30] + "…" if len(line) > 30 else line
        # separator cost
        sep_cost = len(" / ") if preview_parts else 0
        if used + sep_cost + len(compressed) > budget:
            break
        preview_parts.append(compressed)
        used += sep_cost + len(compressed)

    preview = " / ".join(preview_parts)
    # Escape to keep attribute XML-safe: & first, then < and >, then "
    preview = preview.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "'")

    return f'<forwarded_messages count="{count_val}" preview="{preview}"/>'


def _forward_full_for_store(forward_full: str | None) -> str | None:
    """真实展开块才值得持久化;fetch_failed 自闭合变体无内容,不落库。"""
    if forward_full is None:
        return None
    if _FORWARD_SELF_CLOSING_RE.match(forward_full.strip()):
        return None
    return forward_full


@dataclass
class _RepliedMedia:
    """被引消息的提取产物(perception 与触发 handler 共用同一条提取路径)。"""

    sender_label: str = ""
    text: str = ""
    image_urls: list[str] = field(default_factory=list)
    forward_full: str | None = None
    msg_id: object = None


async def _extract_replied_media(event: Event, bot: Bot, *, adapter_name: str) -> _RepliedMedia:
    """抽取被引消息的发送者归属 / 文本 / 图片 / @ 占位 / 折叠转发块。

    perception(priority=1)与触发 handler(priority=98)共用:一条消息的引用图必须
    同源提取——此前只有触发轮临时并入载荷、不入库,历史行/召回拿不到(事故根因之一)。
    异常一律吞没:提取失败只丢引用,不拖垮主流程。
    被引消息若本身是折叠转发,块内节点图不注册发号(不属本条消息 idx),保持 [图片]。
    """
    out = _RepliedMedia()
    if not (hasattr(event, "reply") and event.reply):
        return out
    # 发送者归属独立提取(见 helper docstring):归属提取失败只丢标签,不拖垮引用本体。
    out.sender_label = _extract_replied_sender_label(event)
    out.msg_id = getattr(event.reply, "message_id", None)
    try:
        replied_message = await alconna.UniMessage.generate(message=event.reply.message, bot=bot)
        out.text = replied_message.extract_plain_text().strip()
        out.image_urls = await _extract_image_urls(replied_message, bot, adapter_name)
        replied_at = _collect_at_placeholders(replied_message)
        if replied_at:
            # 引用内历史@改写成「提及」,见 _rewrite_quoted_ats docstring
            suffix = _rewrite_quoted_ats(replied_at)
            out.text = (out.text + " " + suffix).strip() if out.text else suffix
        replied_forward, _replied_fwd_imgs = await _extract_forward_full(
            replied_message, bot, adapter_name=adapter_name
        )
        out.forward_full = replied_forward
    except Exception as e:
        logger.warning(f"[HERMES] 提取引用消息失败: {e}")
    return out


def _compose_quote_prefix(replied: _RepliedMedia, quote_base: int) -> str:
    """(引用 …) 前缀的唯一拼装处:字面@改写 → 转发摘要 → 引用图标记 → 归属包装。

    审计修复批(2026-10-16,疑似 B 维护者拍板=修):主 handler(当轮 current_message)
    与 perception(入库 content)同源调用,保证「出入站同文本」——历史行里的引用
    前缀与当轮呈递形态一致,换窗口/压缩后「谁引用了谁、引文是什么」不再丢失。
    quote_base = 本条消息 own+sticker 图数;引用图号 (引用图#k) 接排其后
    (kind='quote',与 message_images 的全局 idx 一致,见规划书 §8)。

    纯图引用只给 (引用图#k) 标记,不再补裸 [图片]:标记自带可寻址号,已足够;
    再补裸占位只会与标记重复且丢号(旧 L1310 [图片] 兜底的语义由标记继承)。
    返回空串 = 无可呈递引用(无文本/转发/图),调用方不拼前缀。
    """
    replied_text = replied.text
    if replied_text:
        # 本部署补丁: 前缀带发送者归属,空标签回退旧格式;
        # 引用文本先过一道字面手打 @ 的改写(中危⑥,见 _rewrite_quoted_literal_ats)
        replied_text = _rewrite_quoted_literal_ats(replied_text)
    # 引用折叠消息(维护者 2026-10-15 按审计建议 a 拍板):引用块里补单行摘要让内容可见,
    # 不做全量内联(省 token)。fetch_failed 自闭合变体同样拼——「有个取不到的折叠」比空白强。
    if replied.forward_full is not None:
        fwd_note = replied.forward_full
        if not _FORWARD_SELF_CLOSING_RE.match(fwd_note.strip()):
            fwd_note = _summarize_forward(fwd_note)
        replied_text = f"{replied_text} {fwd_note}".strip() if replied_text else fwd_note
    # 引用块:引用带图即补图标记(修事故根因——旧条件 [图片] 只在引用纯图(无文字)时才补,
    # 图文混合引用在文本层丢图标记,模型只能按位置猜)。号 = 该引用图在本条
    # message_images 的全局 idx(kind='quote',续排在 own+sticker 之后)。见规划书 §8。
    if replied.image_urls:
        marks = " ".join(
            f"({media_kind_word('quote')}#{quote_base + j})" for j in range(len(replied.image_urls))
        )
        replied_text = f"{replied_text} {marks}".strip() if replied_text else marks
    if not replied_text:
        return ""
    return _format_quote_prefix(replied_text, replied.sender_label)


def _label_groups(
    label_index: dict[str, tuple[str, int, int]],
    pairs,
) -> list[tuple[tuple[str, int, int] | None, list[str]]]:
    """[(url, frames)] → [((kind, mid, idx)|None, frames)],按 url 反查寻址号。

    去重丢的是重复 url(在 inline_* 内部),号随 url 走,天然不会错位(规划书 §8)。
    """
    return [(label_index.get(url), frames) for url, frames in pairs]


def _absent_media_note(
    label_index: dict[str, tuple[str, int, int]],
    *,
    requested: list[str],
    succeeded: list[str],
) -> str | None:
    """请求内联但整组缺席的媒体条 → `(图#0 加载失败)` 标注串。

    只对反查到库行(有号)的条目标注;无号的缺席交给调用方粗占位兜底(§8)。
    """
    done = set(succeeded)
    notes: list[str] = []
    seen: set[tuple[int, int]] = set()
    for u in requested:
        if u in done:
            continue
        addr = label_index.get(u)
        if addr is None:
            continue
        kind, mid, idx = addr
        if (mid, idx) in seen:
            continue
        seen.add((mid, idx))
        notes.append(f"({media_kind_word(kind, compact=True)}#{idx} 加载失败)")
    return " ".join(notes) if notes else None


# perception → 主 handler 的 db id 传递桥(2026-10-16 定案,B 方案)。
# 两 matcher(priority=1 perception / priority=98 主 handler)state 隔离,
# perception 入库拿到的 lastrowid 经此短 TTL 缓存按 (adapter, event.message_id)
# 挂号;主 handler 构造触发条目时凭自己 event 领号,条目自此自带真实 db id,
# 合并/剥皮/接力全程 id 跟随条目,渲染贴号直接读条目,不再「偷看 buffer 尾部」
# (旧桥 _peek_current_message_row 已拆:接力场景 db 尾部≠当前消息,必错标)。
_EVENT_DB_ID_TTL_S = 60.0
_event_db_id_cache: dict[tuple[str, str], tuple[int, float]] = {}


def _register_event_db_id(adapter_name: str, event_msg_id, db_id: int | None) -> None:
    """perception 入库后挂号:(adapter, event.message_id) → db 行 id。"""
    if event_msg_id is None or db_id is None:
        return
    _event_db_id_cache[(adapter_name, str(event_msg_id))] = (
        db_id,
        time.monotonic() + _EVENT_DB_ID_TTL_S,
    )
    if len(_event_db_id_cache) > 256:
        now_mono = time.monotonic()
        for k, (_v, exp) in [item for item in _event_db_id_cache.items() if item[1][1] <= now_mono]:
            _event_db_id_cache.pop(k, None)


def _claim_event_db_id(adapter_name: str, event_msg_id) -> int | None:
    """主 handler 凭本事件的 message_id 领号;领不到(合成消息/缓存 miss)返 None。"""
    if event_msg_id is None:
        return None
    hit = _event_db_id_cache.pop((adapter_name, str(event_msg_id)), None)
    if hit is None:
        return None
    db_id, exp = hit
    return db_id if exp > time.monotonic() else None


# Telegram `bot.get_file(file_id)` → URL 短期缓存。
# 同一事件经过 priority=1 perception + priority=98 main handler 两个 matcher,
# 各自跑一次 _extract_image_urls,如果不缓存就要打两次 Telegram API
# (~300-500ms 网络往返/次)。
#
# TTL 设 60 秒:Telegram 自己返的 file_path 大约 1 小时有效,我们 60s 内
# 复用足够覆盖事件突发,且远小于真实失效窗口,不引入隐患。
_RESOLVED_URL_TTL_S = 60.0
_resolved_url_cache: dict[tuple[str, str], tuple[str, float]] = {}


async def _resolve_telegram_file_url(bot: Bot, file_id: str) -> str | None:
    """Telegram file_id → 可拉的 HTTPS URL。失败返 None,perception 不崩。

    URL 里含 token,只在 plugin 本地 DB / fetcher 流转(不会进 prompt / MCP 返回)。
    file_path 一般 ~1h 失效,fetcher 必须及时抓——本设计走 perception 异步触发,
    秒级到达 fetcher,不会拖到失效。

    短期缓存:同一 (bot_self_id, file_id) 60s 内复用上次 resolve 结果,避免
    perception + main handler 两层各调一次 Telegram getFile API。
    """
    cache_key = (str(getattr(bot, "self_id", "?")), file_id)
    now = time.monotonic()
    cached = _resolved_url_cache.get(cache_key)
    if cached and cached[1] > now:
        return cached[0]
    try:
        file = await bot.get_file(file_id=file_id)
    except Exception as exc:
        logger.warning(f"[image] telegram get_file failed for file_id={file_id[:24]}...: {exc}")
        return None
    file_path = getattr(file, "file_path", None)
    token = getattr(getattr(bot, "bot_config", None), "token", None)
    if not file_path or not token:
        logger.warning(f"[image] telegram get_file returned no file_path/token (file_id={file_id[:24]}...)")
        return None
    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    _resolved_url_cache[cache_key] = (url, now + _RESOLVED_URL_TTL_S)
    # 偶尔顺手清掉过期 entry,避免长期跑爆字典(O(N) 但 N 很小)
    if len(_resolved_url_cache) > 256:
        expired = [k for k, (_u, exp) in _resolved_url_cache.items() if exp <= now]
        for k in expired:
            _resolved_url_cache.pop(k, None)
    return url


@perception_message.handle()
async def handle_perception(bot: Bot, event: Event):
    """记录消息到 MessageBuffer + 维护 BotRegistry。"""
    if _mcp.message_buffer is None or _mcp.bot_registry is None:
        return

    try:
        target = alconna.get_target()
        adapter_name = get_adapter_name(target)
        user_id = event.get_user_id()
    except Exception:
        return

    if user_id == str(bot.self_id):
        return

    try:
        uni_msg = alconna.UniMessage.generate_without_reply(event=event, bot=bot)
    except Exception:
        return

    msg_text = uni_msg.extract_plain_text().strip()
    image_urls = await _extract_image_urls(uni_msg, bot, adapter_name)
    # 收藏表情也入库(kind='sticker'):看过即焚会让历史里的 [表情包] 占位
    # 永远无法召回字节。是否当轮喂模型仍由 hermes_sticker_vision_enabled 门控,
    # 入库与内联是两件独立的事。
    sticker_urls = await _extract_sticker_urls(uni_msg, bot, adapter_name)
    # 引用图也入库(kind='quote',规划书 §8):提取路径与触发 handler 同源。
    # 此前 perception 不取引用——引用图只进触发轮临时载荷、不落库,历史行/召回拿不到
    # (事故根因之一:图文混合的引用只呈递文字)。
    replied = await _extract_replied_media(event, bot, adapter_name=adapter_name)
    quote_image_urls = list(replied.image_urls)
    nickname = _extract_sender_nickname(event, adapter_name) or user_id

    # 文本太长截断
    max_len = plugin_config.hermes_perception_text_length
    if msg_text and len(msg_text) > max_len:
        msg_text = msg_text[:max_len] + "..."

    if image_urls and plugin_config.hermes_perception_image_mode != "none":
        placeholder = " [图片]"
        msg_text = (msg_text + placeholder) if msg_text else placeholder.strip()

    # B-0: 非文本段占位 (sticker/voice/video/emoji)
    nontext_placeholders = _collect_nontext_placeholders(uni_msg)
    if nontext_placeholders:
        suffix = " ".join(nontext_placeholders)
        msg_text = (msg_text + " " + suffix) if msg_text else suffix

    # At 段回填:plain_text 默认丢 At,补回让历史里 "@C 你看" 这类指向可读
    # 传 event + bot_self_id 让 helper 检测「OneBot v11 _check_at_me 剥走的 @bot」并补回
    at_placeholders = _collect_at_placeholders(uni_msg, event=event, bot_self_id=str(bot.self_id))
    if at_placeholders:
        suffix = " ".join(at_placeholders)
        msg_text = (msg_text + " " + suffix) if msg_text else suffix

    # B-1.1: 合并转发提取 (perception 入站存 summary 版,避免 buffer 膨胀)。
    # 同一次 get_forward_msg 响应内兼收 image 段 URL(kind='forward_image',只注册发号、
    # 不内联);base = own + sticker + quote,号即该图在本条 message_images 的 idx。
    forward_base = len(image_urls) + len(sticker_urls) + len(quote_image_urls)
    forward_full, forward_image_urls = await _extract_forward_full(
        uni_msg, bot, adapter_name=adapter_name, number_base=forward_base
    )
    if forward_full is not None:
        summary = _summarize_forward(forward_full)
        msg_text = f"{msg_text}\n{summary}" if msg_text else summary

    if not msg_text and not image_urls and not quote_image_urls and not forward_image_urls:
        return

    # 引用前缀(审计修复批,2026-10-16,疑似 B 维护者拍板=修):与主 handler 同源调
    # _compose_quote_prefix——历史行/换窗口后「谁引用了谁、引文是什么」不再只活在当轮。
    # 拼在文本截断与空内容闸门之后:前缀绝不被 hermes_perception_text_length 截掉,
    # 也不改变「本条有无自身内容」的入库判定(纯引用空体消息维持不落库)。
    quote_prefix = _compose_quote_prefix(replied, len(image_urls) + len(sticker_urls))
    if quote_prefix:
        msg_text = f"{quote_prefix} {msg_text}".strip()

    now = _now_ms()
    group_id = None if target.private else target.id

    # 写 MessageBuffer
    if plugin_config.hermes_perception_enabled or plugin_config.hermes_active_session_enabled:
        buffered = BufferedMessage(
            ts=now,
            adapter=adapter_name,
            group_id=group_id,
            user_id=user_id,
            nickname=nickname,
            content=msg_text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            quote_image_urls=quote_image_urls,
            forward_image_urls=forward_image_urls,
            is_bot=False,
            forward_content=_forward_full_for_store(forward_full),
        )
        _mcp.message_buffer.append(buffered)
        # B 方案:入库即挂号,主 handler 凭同一 event 的 message_id 领真实 db id
        _register_event_db_id(adapter_name, getattr(event, "message_id", None), buffered.id)

    # 2026-10-15 补丁：发言者已有画像但不在群友行时，自动补进群友行
    if group_id:
        sync_buddy_to_group(adapter_name, group_id, user_id)

    # 写 BotRegistry
    scope = "private" if target.private else "group"
    scope_id = user_id if target.private else (group_id or "")
    if scope_id:
        _mcp.bot_registry.upsert(
            adapter=adapter_name,
            scope=scope,
            scope_id=scope_id,
            bot_self_id=str(bot.self_id),
            target=target,
            ts=now,
        )

    logger.debug(
        f"[HERMES perception] {adapter_name}/{scope}/{scope_id} user={user_id} "
        f"text_len={len(msg_text)} imgs={len(image_urls)}"
    )


# 本部署补丁: 应答防抖(凌晨批项6,维护者拍板)。
# 设计见 项目文档库「方案:应答防抖」。
# 要点:perception 在 priority=1 无条件写 buffer,turn 上下文从 buffer 拉,
# 所以延迟起跑的 turn 自动看到防抖期内到达的所有碎片,触发载荷只需管归属。
class _DebounceState:
    """per-target 防抖状态。首条消息的 handler 常驻(睡满防抖窗口后 dispatch),
    期内后续消息只更新本状态并立即返回——合并语义:同一个人的碎片「拼接」
    (文本换行拼、媒体追加),跨人碎片才「替换」载荷;截止时间顺延。"""

    __slots__ = ("first_mono", "deadline_mono", "payload", "merged")

    def __init__(self, first_mono: float, deadline_mono: float, payload: dict):
        self.first_mono = first_mono          # 首条消息到达时刻(monotonic),硬上限基准
        self.deadline_mono = deadline_mono    # 当前截止时间,期内新消息可顺延
        self.payload = payload                # 最新触发的 dispatch 载荷(kwargs)
        self.merged = 0                       # 被合并掉的碎片条数(观测用)


_debounce_states: dict[tuple[str, str], _DebounceState] = {}


def _merge_fragment_payload(old: dict, new: dict) -> None:
    """同一个人的防抖碎片并入在窗载荷(2026-10-12 维护者拍板:替换改拼接——
    「如果是替换还要防抖干嘛」)。文本换行拼接;图片/表情包 URL 追加去重;
    explicit/addressed 取并集(任一碎片被点名,合并触发即被点名);
    ephemeral 两者皆真才真;now_ms 取最新(触发时序以最后一条为准);
    event 保留首条——ack 已用它贴过 emoji,dispatch 不再用。"""

    new_text = (new.get("msg_text") or "").strip()
    if new_text:
        old_text = (old.get("msg_text") or "").strip()
        old["msg_text"] = f"{old_text}\n{new_text}" if old_text else new_text
    for field in ("image_urls", "sticker_urls"):
        incoming = list(new.get(field) or [])
        if incoming:
            merged = list(old.get(field) or [])
            merged.extend(u for u in incoming if u not in merged)
            old[field] = merged
    old["is_explicit_trigger"] = old["is_explicit_trigger"] or new["is_explicit_trigger"]
    old["addressed_to_bot"] = old["addressed_to_bot"] or new["addressed_to_bot"]
    old["ephemeral"] = old["ephemeral"] and new["ephemeral"]
    if new.get("context_limit") is not None:
        old["context_limit"] = new["context_limit"]
    # 引用 id(任务锚归并用):任一碎片带引用即保留首个——碎片同源一条引用链,
    # 首个非空即可代表;跨人碎片不走本函数(整体替换,随载荷自带)。
    if old.get("replied_msg_id") is None and new.get("replied_msg_id") is not None:
        old["replied_msg_id"] = new["replied_msg_id"]
    old["now_ms"] = new["now_ms"]


def _keyword_soft_hit(keywords, text: str) -> bool:
    """keyword-soft 点火判定(低危⑬-②,2026-10-14 维护者定案:判定与呈递分离)。

    只在「用户当下手打的本体快照」上匹配关键词。调用方必须传拼接信封前的
    本体文本——引用块/非文本占位/@回填/合并转发展开都照常进 msg_text 给
    LLM 看,但不参与点火(旧文本/转发日志里的关键词不再误触发)。
    """
    return any(kw and kw in text for kw in keywords)


def _debounce_arrive(
    key: tuple[str, str],
    payload: dict,
    *,
    debounce_sec: float,
    max_wait_sec: float,
    now_mono: float,
    replaceable: bool = True,
) -> tuple[_DebounceState, bool]:
    """一条触发到达防抖闸。返回 (state, is_new)。

    is_new=True: 调用方是首条触发,负责睡满窗口后 dispatch(state.payload)。
    is_new=False: 本条被合并,调用方应立即返回。合并规则(2026-10-12 坑2修复):
    同一个发送者的碎片一律拼接进在窗载荷(_merge_fragment_payload);
    跨发送者碎片(群聊)按 replaceable 决定是否整体替换;截止时间顺延
    (受 first+max_wait 硬上限)。
    replaceable=False 用于「纯图/纯表情包非显式碎片」:顺延但不替换(该形态
    会在 reactive 图片门控被整 turn 吞掉,替换会弄丢先前的文本触发载荷)。
    get-or-create 与状态更新之间无 await,asyncio 单线程下原子。
    """
    st = _debounce_states.get(key)
    if st is None:
        deadline = now_mono + debounce_sec
        if max_wait_sec > 0:
            # 低危⑬(2026-10-14):首条也套 max_wait 硬上限——debounce_sec 配得比
            # max_wait 大时,旧代码首条 deadline 会越过 first+max_wait 才发车。
            deadline = min(deadline, now_mono + max_wait_sec)
        st = _DebounceState(
            first_mono=now_mono,
            deadline_mono=deadline,
            payload=payload,
        )
        _debounce_states[key] = st
        return st, True
    if st.payload.get("user_id") == payload.get("user_id"):
        _merge_fragment_payload(st.payload, payload)
    elif replaceable:
        st.payload = payload
    st.merged += 1
    new_deadline = now_mono + debounce_sec
    if max_wait_sec > 0:
        new_deadline = min(new_deadline, st.first_mono + max_wait_sec)
    st.deadline_mono = max(st.deadline_mono, new_deadline)
    return st, False


async def _dispatch_chat(
    *,
    bot: Bot,
    event: Event,
    target,
    adapter_name: str,
    user_id: str,
    nickname: str | None,
    group_id: str | None,
    msg_text: str,
    image_urls: list[str],
    sticker_urls: list[str],
    is_explicit_trigger: bool,
    addressed_to_bot: bool,
    now_ms: int,
    ephemeral: bool = False,
    context_limit: int | None = None,
    replied_msg_id=None,
):
    """ack scope + passive/reactive 分叉(原 handle_message 末段,防抖批抽出以便
    防抖到期后与防抖关闭路径共用同一 dispatch)。"""
    # 任务锚+steer(规划书§3,2026-10-14):资格内纯文本直注在跑 turn——私聊恒资格,
    # 群聊仅委托且发送者在资格集合(在跑 turn 触发者 ∪ 在钉锚全体参与者)。
    # 直注成功即 return,不贴 ack(维护者定案);注不进去落回正常 dispatch,零丢失。
    if (
        plugin_config.hermes_runs_channel_enabled
        and not ephemeral
        and not image_urls
        and not sticker_urls
        and await _maybe_steer_direct(
            target=target, adapter_name=adapter_name, user_id=user_id, nickname=nickname,
            group_id=group_id, msg_text=msg_text, is_explicit_trigger=is_explicit_trigger,
            now_ms=now_ms,
        )
    ):
        return
    async with _ack_scope(
        bot,
        event,
        adapter_name=adapter_name,
        is_explicit_trigger=is_explicit_trigger,
        is_private=target.private,
    ):
        if target.private or not plugin_config.hermes_active_session_enabled:
            # 原 v0.1.6 等价路径:passive 模式,raw_text 直接当回复
            await _handle_passive_path(
                bot=bot,
                target=target,
                adapter_name=adapter_name,
                user_id=user_id,
                nickname=nickname,
                group_id=group_id,
                text=msg_text,
                image_urls=image_urls,
                sticker_urls=sticker_urls,
                is_private=target.private,
                now_ms=now_ms,
                event_msg_id=getattr(event, "message_id", None),
            )
            return

        # 群聊 + 活跃态启用 → reactive 决策
        await _handle_reactive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            text=msg_text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_explicit_trigger=is_explicit_trigger,
            addressed_to_bot=addressed_to_bot,
            now_ms=now_ms,
            event_msg_id=getattr(event, "message_id", None),
            # keyword-soft 窗口外命中:跑无窗口临时 turn(不开窗);窗口内则正常跑,
            # 两种都推 N1 条上下文(不用续发轮裁剪)。
            ephemeral=ephemeral,
            context_limit=context_limit,
            replied_msg_id=replied_msg_id,
        )


@receive_message.handle()
async def handle_message(bot: Bot, event: Event, matcher: Matcher):
    # H7 修复(2026-10-17):单例未装配(启动窗口期)原静默吞消息无日志——
    # 排障时无法区分「没收到」与「装配未完成」。补 WARN,60s 节流防刷屏。
    if _mcp.message_buffer is None or _mcp.active_sessions is None:
        _warn_unassembled_throttled("message_buffer/active_sessions")
        return

    try:
        target = alconna.get_target()
    except Exception:
        matcher.skip()

    adapter_name = get_adapter_name(target)
    # H9 收紧(2026-10-17 维护者拍板):取不到 user_id 的匿名/异形事件(群匿名消息、
    # 无发送者平台事件)不再折叠进公共 "user" 桶——共享 session/inflight/防抖键
    # 有理论串话风险。QQ 正常消息恒有 id,取不到即不处理+WARN。
    user_id = event.get_user_id()
    if not user_id:
        logger.warning(f"[HERMES] 事件取不到 user_id,不处理 (adapter={adapter_name})")
        return
    if user_id == str(bot.self_id):
        matcher.skip()

    if not check_isolation(event, target):
        logger.debug(
            f"[HERMES skip] isolation_denied adapter={get_adapter_name(target)} "
            f"target={target.id} private={target.private} user={user_id}"
        )
        matcher.skip()

    # 引用消息提取(与 perception 共用同一 helper:引用图入库/呈递必须同源)
    replied = await _extract_replied_media(event, bot, adapter_name=adapter_name)
    replied_image_urls: list[str] = list(replied.image_urls)
    replied_msg_id = replied.msg_id  # 任务锚 quote 自动归并用(规划书§2.2)

    try:
        uni_msg = alconna.UniMessage.generate_without_reply(event=event, bot=bot)
    except Exception:
        matcher.skip()

    msg_text = uni_msg.extract_plain_text().strip()
    # 低危⑬-②(2026-10-14 维护者定案):keyword-soft 判定的快照要在任何拼接之前取——
    # 引用前缀/占位/@回填/转发展开随后都会拼进 msg_text,那些文本不参与点火。
    keyword_match_text = msg_text
    # 本条消息自身媒体先抽(own/sticker):引用图的号接在其后,故先算 base。
    own_image_urls = await _extract_image_urls(uni_msg, bot, adapter_name)
    # QQ 大表情包单独抽,不进普通 image_urls;是否喂模型由 hermes_sticker_vision_enabled 门控
    sticker_urls = await _extract_sticker_urls(uni_msg, bot, adapter_name)
    quote_base = len(own_image_urls) + len(sticker_urls)
    # 引用前缀拼装收口到 _compose_quote_prefix(审计修复批,2026-10-16,疑似 B):
    # 与 perception(入库 content)同源调用——出入站同文本,历史行的引用前缀与
    # 当轮 current_message 形态一致。
    quote_prefix = _compose_quote_prefix(replied, quote_base)
    if quote_prefix:
        msg_text = f"{quote_prefix} {msg_text}".strip()

    # 内联载荷:当前消息自己的图 + 引用图(转发图恒不内联,Q3 定案);URL 去重交给内联入口。
    image_urls = own_image_urls + replied_image_urls
    nickname = _extract_sender_nickname(event, adapter_name) or user_id

    # B-0: 非文本段占位拼到 msg_text 末尾。注意这里**不重复**对 replied_message 做
    # collect: 引用消息已通过引用前缀((引用:...) 形态)进来,
    # 引用里的 voice/video 用户感知较低,且会让占位重复堆叠。
    nontext_placeholders = _collect_nontext_placeholders(uni_msg)
    if nontext_placeholders:
        suffix = " ".join(nontext_placeholders)
        msg_text = (msg_text + " " + suffix).strip() if msg_text else suffix

    # At 段回填:让 Hermes 在 decision_protocol 里能看到当前消息 @ 了谁;keyword
    # 模式的 startswith 检测不受影响(at_placeholders 拼在末尾,不在前缀)
    # 传 event + bot_self_id 让 helper 检测「OneBot v11 _check_at_me 剥走的 @bot」并补回
    # 本部署补丁: at_placeholders 计算上移到 has_real_content
    # 快照之前,并把「含 At 段」计入 has_real_content——纯 @bot 消息(先发图再补 @
    # 的常见操作)不再被空消息闸门吞掉,按正常显式触发进 chat。完全无内容的空消息
    # (无文本/无图/无 At/无转发)仍走原 skip。仅 @ 他人的消息由下游闸门兜底:
    # 窗口外被 not_active_not_explicit 挡,窗口内被 @s-only-other-users 挡。
    at_placeholders = _collect_at_placeholders(uni_msg, event=event, bot_self_id=str(bot.self_id))
    has_real_content = bool(msg_text) or bool(image_urls) or bool(at_placeholders)
    if at_placeholders:
        suffix = " ".join(at_placeholders)
        msg_text = (msg_text + " " + suffix).strip() if msg_text else suffix

    # B-1.1: 合并转发提取 (main path 用 full 版,LLM 当前 turn 看到展开后的转发内容)。
    # 同一次 get_forward_msg 响应内兼收转发图 URL 发号(kind='forward_image',只注册
    # 不内联);base = own + sticker + quote,与 perception 侧同一编号。在 keyword-stripping
    # 之前追加:keyword 前缀作用于 msg_text 开头的手打文本,转发块拼在末尾。
    forward_base = quote_base + len(replied_image_urls)
    forward_full, _forward_image_urls = await _extract_forward_full(
        uni_msg, bot, adapter_name=adapter_name, number_base=forward_base
    )
    if forward_full is not None:
        msg_text = f"{msg_text}\n{forward_full}" if msg_text else forward_full
        has_real_content = True

    logger.debug(
        f"[HERMES recv] adapter={adapter_name} target={target.id} private={target.private} "
        f"user={user_id} nick={nickname!r} is_tome={event.is_tome()} self_id={bot.self_id!r} "
        f"at_targets={[str(s.target) for s in uni_msg[alconna.At]] if uni_msg.has(alconna.At) else []} "
        f"text_len={len(msg_text)} imgs={len(image_urls)}"
    )

    if not has_real_content:
        logger.debug(f"[HERMES skip] empty adapter={adapter_name} user={user_id}")
        matcher.skip()

    group_id = None if target.private else target.id
    now = _now_ms()

    # --- 触发判断 ---
    # addressed_to_bot 是与触发正交的寻址事实:平台确认「这条冲 bot 说的」
    # (真 @ / 引用 bot 的消息)。它随消息进 prompt 的 runtime_state,
    # 让归属判断不必从回填的裸 `@<id>` 去猜。
    #
    # 本部署补丁(凌晨批项2 全隐式化): hermes_group_trigger 的
    # at/all/keyword 三模式作废,收敛为 keyword-soft 单路径;群聊侧
    # is_explicit_trigger 重定义 = addressed_to_bot——下游所有 explicit 挂点
    # (开窗 / ack emoji / 解析兜底转发 / 图片门控旁路 / cooldown 旁路 / 裁剪豁免)
    # 随之整体 rekey 到 addressed,「显式激活」不再是独立概念,@ 只是寻址事实
    # + 隐式激活信号。私聊维持恒 explicit(私聊决策信封内自决,见 _run_passive_turn)。
    is_explicit_trigger = False
    addressed_to_bot = False
    keyword_soft_hit = False  # keyword-soft 全文命中(隐式激活)
    # 凌晨批项3 黑名单:名单账号(按群配置)的消息不置 explicit/addressed——
    # 它们的 @ 降级为普通隐式信号,从入口消解 AI 互聊循环的燃料;消息照常入
    # buffer、窗口内跟帖照常可见(人类委托「去跟 X 说」的会话能继续)。
    sender_blacklisted = _is_blacklisted(group_id, user_id)
    if target.private:
        is_explicit_trigger = True
    else:
        # event.is_tome() 在用户引用 bot 旧消息时也会被置 True;若当前消息含 @ 段但
        # 全部指向他人,视作"用户在跟别人讲话,只是顺手引用了 bot 那条",不算显式 @bot。
        #
        # has_bot_at 要兜底翻 event.original_message:OneBot v11 adapter 在 _check_at_me
        # 里会把消息开头/结尾的 @bot 段从 event.message 中剥掉(并置 to_me=True),下游
        # uni_msg 里看不到 @bot,但用户其实 @ 了。否则「@bot @other 怎么看」会因
        # _msg_at_only_other_users=True 触发 quoted-only 抑制,完全不响应(连 emoji 都没)。
        has_bot_at = _is_bot_at(uni_msg, str(bot.self_id)) or _has_at_bot_in_original(event, str(bot.self_id))
        is_mentioned = has_bot_at or (event.is_tome() and not _msg_at_only_other_users(uni_msg, str(bot.self_id)))
        trigger_mode = plugin_config.hermes_group_trigger
        if trigger_mode != "keyword-soft" and trigger_mode not in _TRIGGER_MODE_DEPRECATED_WARNED:
            _TRIGGER_MODE_DEPRECATED_WARNED.add(trigger_mode)
            logger.warning(
                f"[HERMES] hermes_group_trigger={trigger_mode!r} 已作废(凌晨批项2 全隐式化,"
                "收敛 keyword-soft 单路径):@/引用 只作寻址与开窗信号,不再强制回复"
            )
        addressed_to_bot = is_mentioned and not sender_blacklisted
        is_explicit_trigger = addressed_to_bot
        # keyword-soft 单路径:关键词命中 = 隐式激活,不剥前缀、不升级
        # addressed_to_bot,由 LLM 拿 N1 条上下文自判是否点名。
        # 低危⑬-②(2026-10-14):判定只跑本体快照(keyword_match_text),引用块/
        # 转发等拼接文本不参与点火;内容本身仍在 msg_text 里完整呈递给 LLM。
        keyword_soft_hit = _keyword_soft_hit(plugin_config.hermes_keywords, keyword_match_text)

    # --- M1 核心:活跃态分支 ---
    in_active_window = (
        not target.private
        and plugin_config.hermes_active_session_enabled
        and group_id is not None
        and _mcp.active_sessions.is_active(adapter_name, group_id, now)
    )

    if not is_explicit_trigger and not in_active_window and not keyword_soft_hit:
        logger.debug(f"[HERMES skip] not_active_not_explicit adapter={adapter_name} group={group_id} user={user_id}")
        matcher.skip()

    # 隐式激活只延时不开窗(窗口生命周期定案):窗口内的非显式消息滑动续期,
    # 受 T5 封顶(touch 内部判)。窗口外的隐式激活(keyword-soft 命中 / 定时巡查)
    # 走 ephemeral 临时 turn,不调 trigger()。
    if in_active_window and not is_explicit_trigger:
        _mcp.active_sessions.touch(adapter_name, group_id, now)

    # C: 活跃窗口内,若消息只 @ 他人未点名 bot,视作非本路径触发,只让 perception
    # 写 buffer,不进 chat() 决策。修「跨目标 @ 误抢」模式 1。
    # 显式触发(at-bot)早已在上面计入 is_explicit_trigger,不会到这里被过滤。
    if in_active_window and not is_explicit_trigger and _msg_at_only_other_users(uni_msg, str(bot.self_id)):
        logger.debug(
            f"[HERMES reactive] skip: msg @s only other users (adapter={adapter_name} group={group_id} user={user_id})"
        )
        matcher.skip()

    # 显式触发(全隐式化后 = addressed):进入 / 续期活跃态(群聊场景)
    if is_explicit_trigger and not target.private and group_id and plugin_config.hermes_active_session_enabled:
        _mcp.active_sessions.trigger(adapter_name, group_id, user_id, now_ms=now)
        logger.info(f"[HERMES] active_session triggered/renewed: {adapter_name}/{group_id} by {user_id}")
        # 2026-10-13 队列化定案:入口不再自动钉锚。@ 只保证进委托队列并拿到一次
        # 主动判断轮;是否钉锚由该轮 submit_decision 显式给 pin_anchor(见 _run_reactive_turn)。
        # 这样玩笑/围观 @ 不会污染双槽,正常任务也不因锚满被挤掉。

    if not target.private:
        logger.info(
            f"[HERMES] dispatch: group={group_id} explicit={is_explicit_trigger} "
            f"in_active={in_active_window} mode="
            f"{'reactive' if plugin_config.hermes_active_session_enabled else 'passive'}"
        )

    # --- 应答防抖(本部署补丁:凌晨批项6) ---
    # 首条触发:登记状态 → 进 ack scope 秒贴 emoji → 睡满防抖窗 → 拿合并后载荷 dispatch。
    # 期内碎片:同人拼接/跨人替换 + 顺延截止(受 max_wait 硬上限)后立即返回,不另起 turn。
    # 防抖期间不占 inflight slot;T2 定时/投递轮询不经过本 handler,天然不防抖。
    # 显式/隐式闲聊一视同仁;但群里 addressed_to_bot 的委托 @ 不走群防抖——
    # 多人同时 @ 时,群级防抖的跨人替换会把先到的委托触发载荷顶掉(2026-10-13
    # 队列化配套)。委托直连 _dispatch_chat,排队交给 inflight 的委托队列。
    skip_debounce_for_commission = (
        (not target.private)
        and addressed_to_bot
        and user_id not in set(plugin_config.hermes_known_bots)
    )
    debounce_sec = (
        plugin_config.hermes_reply_debounce_private_sec
        if target.private
        else plugin_config.hermes_reply_debounce_group_sec
    )
    if debounce_sec > 0 and not skip_debounce_for_commission:
        debounce_key = make_inflight_key(adapter_name, target.private, user_id, group_id)
        payload = dict(
            bot=bot,
            event=event,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            msg_text=msg_text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_explicit_trigger=is_explicit_trigger,
            addressed_to_bot=addressed_to_bot,
            now_ms=now,
            replied_msg_id=replied_msg_id,
            ephemeral=keyword_soft_hit and not in_active_window,
            context_limit=plugin_config.hermes_keyword_soft_context if keyword_soft_hit else None,
        )
        # get-or-create 与状态更新之间无 await,asyncio 单线程下原子。
        loop = asyncio.get_running_loop()
        # 纯图/纯表情包非显式碎片不替换跨人载荷:它是 reactive 图片门控会整 turn
        # 吞掉的形态,替换会把先前的文本触发载荷弄丢;但仍顺延截止(图已在 buffer
        # 里,最终 turn 看得到)。同人碎片不走替换——_merge_fragment_payload 拼接,
        # 本保护只对群聊跨人碎片有意义。
        media_only = (bool(image_urls) or bool(sticker_urls)) and not msg_text.strip()
        st, is_new = _debounce_arrive(
            debounce_key,
            payload,
            debounce_sec=debounce_sec,
            max_wait_sec=plugin_config.hermes_reply_debounce_max_wait_sec,
            now_mono=loop.time(),
            replaceable=not (media_only and not is_explicit_trigger),
        )
        if not is_new:
            logger.debug(
                f"[HERMES debounce] merged fragment (key={debounce_key} user={user_id} "
                f"merged={st.merged} fire_in={st.deadline_mono - loop.time():.1f}s)"
            )
            return
        try:
            # ack scope 在睡前进入:首条消息秒贴 emoji,emoji 覆盖防抖+turn 全程。
            # 注意:ack 用首条消息的 event/explicit 判定;期内碎片(含 explicit)
            # 不另贴 emoji——它们的 handler 更新状态后即返回。
            async with _ack_scope(
                bot,
                event,
                adapter_name=adapter_name,
                is_explicit_trigger=is_explicit_trigger,
                is_private=target.private,
            ):
                while True:
                    remaining = st.deadline_mono - loop.time()
                    if remaining <= 0:
                        break
                    await asyncio.sleep(min(remaining, 0.5))
                logger.debug(
                    f"[HERMES debounce] fire (key={debounce_key} merged={st.merged} "
                    f"waited={loop.time() - st.first_mono:.1f}s)"
                )
                # 坑1修复(2026-10-12,维护者拍板):发车前先释放状态——此前状态活到
                # 整个 turn 结束才 pop,turn 期间到达的消息被当作「期内碎片」合并进
                # 已过发车点的死状态,随 pop 一并丢弃(私聊文本/@ 委托被无声吞掉,
                # 生产实证三连丢)。提前 pop 后,turn 期间的新消息开新防抖窗正常发车,
                # inflight 占线则进 pending 走现成 refire 接力。
                _debounce_states.pop(debounce_key, None)
                await _dispatch_chat(**st.payload)
        finally:
            _debounce_states.pop(debounce_key, None)  # 睡眠期异常兜底;正常路径已 pop,no-op
        return

    # --- 调用 Hermes (防抖关闭时走原路径直发) ---
    await _dispatch_chat(
        bot=bot,
        event=event,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
        msg_text=msg_text,
        image_urls=image_urls,
        sticker_urls=sticker_urls,
        is_explicit_trigger=is_explicit_trigger,
        addressed_to_bot=addressed_to_bot,
        now_ms=now,
        replied_msg_id=replied_msg_id,
        ephemeral=keyword_soft_hit and not in_active_window,
        context_limit=plugin_config.hermes_keyword_soft_context if keyword_soft_hit else None,
    )


# 上游 locked 类持久化失败 = 别的 Hermes 进程正在写 state.db,上游要求「过一会再发一次」。
# 等一拍再重试,让持有写锁的一方先落库;不做指数退避,单次补偿够覆盖锁窗口,
# 拖长只会把群里的回复延迟摊得更明显。
_PERSISTENCE_RETRY_DELAY_S = 1.0


# 同 turn 去重的判据。刻意很窄:只认「近乎逐字复述」。
#
# 早期规则是"本 turn 内 push 过就把 submit_decision 整条抑制",它把最自然的用法
# ——push 一句「在查」、decision 给真答案——也一起打死了:答案被静默吞掉,而 agent
# 以为自己发出去了,还会向用户汇报已发送。包含关系不能当判据,「引用刚说的话 + 补上
# 新内容」同样是包含。所以失败方向明确选:宁可群里多一条啰嗦的,不可吞掉答案。
_RESTATEMENT_NOISE = str.maketrans(
    "",
    "",
    " \t\r\n，。！？、；：~～…!?,.;:「」『』\"'“”‘’()()【】[]",
)


def _is_restatement(pushed: str, reply: str) -> bool:
    """reply 是否只是把刚发出去的 pushed 又说了一遍。

    归一化(去空白/标点/大小写)后完全相同,或一方包含另一方且多出来的部分几乎为零,
    才算复述。pushed 为空(不知道原文)一律返回 False。
    """
    a = pushed.translate(_RESTATEMENT_NOISE).casefold()
    b = reply.translate(_RESTATEMENT_NOISE).casefold()
    if not a or not b:
        return False
    if a == b:
        return True
    longer, shorter = (b, a) if len(b) >= len(a) else (a, b)
    if shorter not in longer:
        return False
    return len(longer) - len(shorter) <= max(4, len(longer) // 10)


async def _chat_and_adopt_rotation(**chat_kwargs) -> ChatResult:
    """调 chat();上游本轮换过 session(自动压缩触发)就立刻采纳新 id。

    采纳必须发生在每一次 chat 之后而不只是最后一次:重试那轮同样可能碰上轮换,
    漏掉就又把会话钉回已关闭的父会话。
    """
    result = await hermes_client.chat(**chat_kwargs)
    if result.effective_session_key:
        session_manager.adopt_session_key(chat_kwargs["session_key"], result.effective_session_key)
    # T1 活动基准:无论成败,chat() 走完就算本会话有 LLM 活动。
    # chat_kwargs 四元组在 passive / reactive 两条路径上都齐全。
    session_manager.mark_activity(
        chat_kwargs["adapter_name"],
        chat_kwargs["is_private"],
        chat_kwargs["user_id"],
        chat_kwargs.get("group_id"),
    )
    return result


async def _chat_with_persistence_retry(label: str, /, **chat_kwargs) -> ChatResult:
    """调 chat();命中瞬时(locked)持久化失败时用**同一个** session_key 重试一次。

    label 走 positional-only,余下 kwargs 原样转给 hermes_client.chat,
    调用点因此和直接调 chat() 读起来一样。

    不主动换 session_key:上游三类 cause(locked / disk / unknown)都指向同一个 state.db,
    换 key 只是新开一个会话写同一个库,治不了锁也治不了满盘,代价是整个群的
    Hermes 侧上下文当场清零。disk / unknown 直接不重试 —— 重发也写不进去,而持久化
    失败是在 turn 收尾阶段判定的(工具此时已经跑过),白重试一次等于让副作用再来一遍。

    **例外**:上游自己轮换了会话(同一轮里既压缩又撞锁)时,重试要跟上采纳后的新 key。
    那不是"我们换 key",而是上游告知"活的会话已经是这个了";继续用旧 key 是往一个
    已关闭的父会话里写,必然失败。
    """
    result = await _chat_and_adopt_rotation(**chat_kwargs)
    if not result.is_persistence_error:
        return result

    if result.persistence_cause != "locked":
        # disk / unknown 要人介入(清盘、修 state.db 权限、hermes doctor),
        # 拉到 error 级别让运维扫日志能看见;回复本身走 transport_error 兜底屏蔽。
        logger.error(
            f"[HERMES {label}] 上游 Session 持久化失败且不可自动恢复 "
            f"(cause={result.persistence_cause}),不重试;请检查 Hermes 侧磁盘 / state.db"
        )
        return result

    if result.effective_session_key:
        # 采纳只更新了 SessionManager 的映射,本轮的 kwargs 还攥着旧 key;
        # 不同步过来,重试就把已关闭的父会话又钉回去了。
        chat_kwargs["session_key"] = result.effective_session_key

    logger.warning(f"[HERMES {label}] 上游 state.db 写锁冲突,{_PERSISTENCE_RETRY_DELAY_S}s 后重试一次")
    await asyncio.sleep(_PERSISTENCE_RETRY_DELAY_S)
    retried = await _chat_and_adopt_rotation(**chat_kwargs)
    if retried.is_persistence_error:
        logger.error(f"[HERMES {label}] 重试后仍持久化失败 (cause={retried.persistence_cause}),放弃本轮")
    return retried


# 重置丢掉的是 Hermes 侧的 transcript,不是全部上下文:群聊每轮仍会注入插件自己的
# `<recent_messages>` 窗口(私聊不注入),近处的对话照样看得见,接不上的是窗口之外、
# 更早的那部分。措辞因此只说"再往前的",写成"什么都不记得了"在群聊里就是假的。
_SESSION_RESET_NOTICE = "(上下文满了,我重开了一段对话 —— 再往前的就接不上了)"


async def _chat_with_overflow_recovery(label: str, /, *, reply_bot: Bot, reply_target, **chat_kwargs) -> ChatResult:
    """调 chat();撞上「上下文溢出 + 上游自动压缩已关」时重置会话并重发一次。

    上游在 compression.enabled: false 下禁掉了包括会话轮换在内的全部自动恢复,只回一段
    提示文本。而窗口只涨不缩:不管它的话下一轮更大,这条会话从此每轮都失败。

    换 session key 是对症的唯一手段 —— 那段历史窗口本身就是病因。这与持久化失败**正好
    相反**(见 _chat_with_persistence_retry:那一类刻意不换 key,因为换了既治不了写锁也
    治不了满盘,却白白清空整群上下文)。两处的判断不要互相抄。

    只重发一次:新会话仍然溢出说明病因不在历史,而在单轮本身(system prompt + 历史窗口
    就已经超了),再转一圈只会连着烧 generation 且每次都失败。

    reply_bot / reply_target 刻意不叫 bot / target:chat_kwargs 里的 `target` 是出向接入点
    (HermesTarget),与发消息用的会话目标同名不同物。
    """
    # 阶段2a:bot_self_id 顺 kwargs 注入 chat()(runs 路 run_tracker 登记用),
    # 单点注入覆盖全部调用点;completions 路忽略此参数。
    result = await _chat_with_persistence_retry(
        label, bot_self_id=str(reply_bot.self_id), **chat_kwargs
    )
    if not result.is_context_overflow:
        return result

    adapter_name = chat_kwargs["adapter_name"]
    is_private = chat_kwargs["is_private"]
    user_id = chat_kwargs["user_id"]
    group_id = chat_kwargs["group_id"]
    logger.error(
        f"[HERMES {label}] 上游上下文溢出且自动压缩已关,重置会话后重发一次 "
        f"(group={group_id} stale_key={chat_kwargs.get('session_key')})"
    )

    # 先说再答:重发可能很慢,而且这条提示即使重发失败也依然属实(会话确实已经重置)。
    await send_text_with_media(
        bot=reply_bot,
        target=reply_target,
        text=_SESSION_RESET_NOTICE,
        media_urls=[],
        at_user_id=None,
        adapter_name=adapter_name,
    )

    session_manager.clear_session(adapter_name, is_private, user_id, group_id)
    chat_kwargs["session_key"] = session_manager.get_session_key(adapter_name, is_private, user_id, group_id)

    retried = await _chat_with_persistence_retry(
        label, bot_self_id=str(reply_bot.self_id), **chat_kwargs
    )
    if retried.is_context_overflow:
        logger.error(
            f"[HERMES {label}] 重置后仍然溢出,放弃本轮(group={group_id})—— 病因不在历史而在单轮本身:"
            "system prompt 加历史窗口就已经超了,请缩小窗口或换更大上下文的模型"
        )
    return retried


async def _run_passive_turn(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    text: str,
    image_urls: list[str],
    sticker_urls: list[str] | None = None,
    is_private: bool,
    now_ms: int,
    current_buffered: BufferedMessage | None = None,
):
    """跑一发 passive turn,返回 ChatResult 或 None(被 submit_decision 静默兜底等情况)。"""
    session_key = session_manager.get_session_key(
        adapter_name=adapter_name,
        is_private=is_private,
        user_id=user_id,
        group_id=group_id,
    )
    memory_key = session_manager.get_memory_key(
        adapter_name=adapter_name,
        is_private=is_private,
        user_id=user_id,
        group_id=group_id,
    )
    hermes_target = resolve_target(adapter_name, is_private, group_id)

    # 群聊 + 默认配置(active_session=false)+ perception_enabled:补回 0.1.6
    # 「@bot 时让 LLM 看到群里旁观历史」。before_ts=now_ms 排除 perception 在
    # 同一事件 priority=1 时刚写入的当前消息,避免历史里出现重复。
    # 私聊不注入(0.1.6 起 perception 在私聊就是 no-op,Hermes session 已覆盖)。
    # 历史从 0.2.x 起放进 user content 而非 system,以维持 system 字节稳定。
    recent: list[BufferedMessage] = []
    if not is_private and group_id and plugin_config.hermes_perception_enabled and _mcp.message_buffer is not None:
        recent = list(
            _mcp.message_buffer.get_recent(
                adapter=adapter_name,
                group_id=group_id,
                limit=plugin_config.hermes_perception_buffer,
                before_ts=now_ms,
            )
        )

    # 本部署补丁(凌晨批项2): 私聊挂 submit_decision 决策信封
    # (全隐式化:私聊不再「硬必回」,模型按私聊版协议自决,无实质内容可静默)。
    # 群聊 passive(active_session=off 的旧部署形态)维持 raw_text 直发不动。
    use_private_decision = is_private
    # 2026-10-15 补丁:计算记忆根路径,用于路由提示动态跟随配置
    _custom_dir = resolve_custom_dir()
    _memory_root = str(_custom_dir / "memory" / adapter_name) if _custom_dir else None
    # 身份表（2026-10-15）：发言者标签正主名注释，custom_dir 不可用/查不到都静默 None
    _name_resolver = (
        (lambda uid, _cd=_custom_dir, _ad=adapter_name: identity.resolve_display_name(_cd, _ad, uid))
        if _custom_dir else None
    )
    system_prompt = build_passive_system_prompt(
        adapter=adapter_name,
        is_private=is_private,
        user_id=user_id,
        group_id=group_id,
        private_decision=use_private_decision,
        # 群记忆外挂:群档案注入块(私聊 group_id=None 内部静默跳过)
        profile_block=build_group_profile_block(adapter_name, group_id),
        # 2026-10-15 补丁:传入功能开关,私聊时启用则注入简化版路由提示
        group_profile_enabled=plugin_config.hermes_group_profile_enabled,
        memory_root=_memory_root,
    )
    # 图片内联成 data: URL 再出站:上游对 http(s) URL 是纯透传,交出去就把可达性
    # 赌在 provider 出网与 URL 时效上,且部分 provider 形态会静默丢图。
    # 2026-10-15 三模式改造:私聊维持无条件内联;群聊 passive 与 reactive 同走
    # media_config 三模式(窗口=recent;passive 无 addressed 概念,按 True 传——
    # 群聊 passive 只在显式触发时被调用,等价于已被 @)。
    media_cfg = load_media_config()
    # 媒体寻址:窗口行 + 当前行建 url→(kind,m:id,idx) 反向映射,内联收集照旧走 url,
    # 收集完按 url 贴号(规划书 §8)。passive 的 recent 排除了当前消息,故单独拼当前行;
    # B 方案:当前行直接吃触发条目(构造时已凭 event 领得 db 真 id),不再偷看 buffer
    # 尾部(接力场景尾部≠当前消息必错标)。条目无 id(合成消息)则 current_row=None,
    # 贴号/寻址走现成无号降级。
    current_row = (
        current_buffered if (current_buffered is not None and current_buffered.id is not None) else None
    )
    label_index = build_media_address_index(list(recent) + ([current_row] if current_row else []))
    if is_private:
        inline_urls = list(image_urls)
    else:
        inline_urls = _collect_inline_image_urls(
            media_cfg,
            window_msgs=recent,
            current_image_urls=image_urls,
            current_user_id=user_id,
            addressed_to_bot=True,
            self_id=str(bot.self_id),
        )
    inlined = await inline_image_frames(inline_urls, cfg=media_cfg) if inline_urls else []
    # 2026-10-13 动图抽帧:表情走专用通道,返回 (url, 帧组)(动图多帧/静态单帧),
    # 帧组结构自配对防张冠李戴(见 image_inline.inline_gif_frames docstring)。
    sticker_inlined = (
        await inline_gif_frames(sticker_urls or [], cfg=media_cfg)
        if plugin_config.hermes_sticker_vision_enabled
        else []
    )
    # 缺席补占位:请求内联但整组缺席的当前消息媒体,在其号上补「(图#k 加载失败)」(§8)。
    absent_note = _absent_media_note(
        label_index,
        requested=list(image_urls) + list(sticker_urls or []),
        succeeded=[u for u, _ in inlined] + [u for u, _ in sticker_inlined],
    )
    passive_text = text or " "
    if absent_note:
        passive_text = f"{passive_text} {absent_note}".strip()
    user_content = build_passive_user_content(
        recent_messages=recent,
        current_text=passive_text,
        current_image_urls=_label_groups(label_index, inlined),
        current_sticker_urls=_label_groups(label_index, sticker_inlined),
        # 审计修复批 2026-10-16(疑似 C 拍板=修):当前消息行补 [m:ID] 前缀 + 媒体清单,
        # 与 reactive 的 current_message 同形同源;B 方案(同日晚):行来自触发条目
        # (构造时已凭 event 领得 db 真 id),不再偷看 buffer 尾部。
        current_message=current_row,
        # 2026-10-13 分级反馈:弹出本 scope 的插件提示便签贴进本轮(见即焚)
        plugin_note="\n".join(
            task_notes.pop_plugin_notes(_feedback_scope_key(adapter_name, is_private, user_id, group_id))
        ) or None,
        name_resolver=_name_resolver,
    )

    # 本部署补丁: 长任务旁白实时转发。旁白是本轮 turn 的附属
    # 输出,用 handler 现成的 bot/target 直发,不走 push_message(那是带窗口校验的
    # 独立发话通道);不 @ 人(进度广播不针对特定个体)。extract_response_media 剥掉
    # MEDIA:/md 图片标签——旁白里的本地路径原样发出会泄主机路径;媒体最终回复还会带。
    async def _interim_echo(segment: str) -> None:
        # 2026-10-13 分级反馈:旁白段过 where=interim/all 的 note 规则(确认轮不跟旁白)
        _check_interim_feedback(
            _feedback_scope_key(adapter_name, is_private, user_id, group_id), segment
        )
        cleaned, _, _ = extract_response_media(segment)
        if cleaned:
            await send_text_with_media(
                bot=bot,
                target=target,
                text=cleaned,
                media_urls=[],
                at_user_id=None,
                adapter_name=adapter_name,
            )

    result = await _chat_with_overflow_recovery(
        "passive",
        reply_bot=bot,
        reply_target=target,
        text="",
        image_urls=[],
        session_key=session_key,
        memory_key=memory_key,
        user_id=user_id,
        group_id=group_id,
        adapter_name=adapter_name,
        is_private=is_private,
        mode="passive",
        expect_structured=use_private_decision,
        structured_tool_name="submit_decision" if use_private_decision else None,
        system_prompt=system_prompt,
        user_content_override=user_content,
        target=hermes_target,
        interim_callback=_interim_echo if plugin_config.hermes_interim_echo_enabled else None,
        approval_callback=(
            _make_approval_notify(
                bot, adapter_name,
                f"私聊 {user_id}" if is_private else f"群 {group_id}",
                is_private=is_private, group_id=group_id,
            )
            if plugin_config.hermes_runs_channel_enabled else None
        ),
    )

    # 阶段2b:runs 路迟到 steer 回收(被动路径)——私聊/passive 直说按委托排队,
    # 与既有 passive pending 语义一致;turn 结束后的 refire 自然接力。
    if result is not None and result.pending_steer:
        _scope_key = make_inflight_key(adapter_name, is_private, user_id, group_id)
        _mcp.inflight.try_enter(
            _scope_key,
            BufferedMessage(
                ts=_now_ms(), adapter=adapter_name, group_id=group_id,
                user_id=user_id, nickname="(迟到 steer)",
                content=result.pending_steer, image_urls=[], sticker_urls=[],
                reply_to_ts=None, is_bot=False,
            ),
            is_explicit_trigger=True,
            original_msg_id=None,
            now_ms=_now_ms(),
            commission=True,
        )
        logger.info(
            f"[HERMES passive] 回收 pending_steer 进委托队列 "
            f"(key={_scope_key} len={len(result.pending_steer)})"
        )

    # 本部署补丁: 403 鉴权类错误硬静默——不发 fallback、
    # 不发 raw_text,不依赖 fallback 配置。403 是运维事件(key/额度),群里通知无意义,
    # 上游持续故障时每条触发一条通知就是刷屏。记 error 级日志让运维能看见。
    # 2026-10-11 扩展: provider 暂不可用/重试耗尽引导文案同口径静默。
    if result.is_forbidden_error or result.is_outage_error:
        _silence_kind = (
            "403 鉴权类错误" if result.is_forbidden_error
            else "provider 暂不可用(重试耗尽)引导文案"
        )
        logger.error(
            f"[HERMES passive] {_silence_kind},已静默丢弃 (group={group_id} is_private={is_private})"
            "——检查接入点 key / provider 额度与可用性"
        )
        return result

    # 阶段3b:turn 被主动终止,硬静默(同 reactive 分支注释)。
    if result.is_interrupted:
        logger.info(f"[HERMES passive] turn 被终止,硬静默 (group={group_id} is_private={is_private})")
        return result

    # 上游 transport_error 同款保护(见 _run_reactive_turn 同名分支注释)。
    # passive 路径下私聊总是显式对话,群聊已通过触发判断进得来,两边都该有可见反馈;
    # 配空 fallback_text 时静默,保留逃生口。
    if result.is_transport_error:
        fallback_text = plugin_config.hermes_transport_error_fallback_text
        logger.warning(
            f"[HERMES passive] transport error fallback "
            f"(group={group_id}, is_private={is_private}, "
            f"fallback={'silent' if not fallback_text else 'friendly_text'}); "
            f"upstream raw_text suppressed (len={len(result.raw_text or '')})"
        )
        if fallback_text:
            await send_text_with_media(
                bot=bot,
                target=target,
                text=fallback_text,
                media_urls=[],
                at_user_id=None if is_private else user_id,
                adapter_name=adapter_name,
            )
        return result

    # 本部署补丁(凌晨批项2): 私聊决策信封解析成功时按信封走——
    # should_reply=false 即静默(私聊不再硬必回)。解析失败回 raw_text:私聊是显式
    # 对话,静默更像故障;并保留既有防御性抠取(模型可能吐了破信封)。
    # 2026-10-13 分级反馈:background_task 便签登记(静默轮也登记)。
    _fb_key = _feedback_scope_key(adapter_name, is_private, user_id, group_id)
    if use_private_decision and result.structured is not None and not result.parse_failed:
        _record_background_note(_fb_key, result.structured, log_tag="passive")
        if not result.structured.get("should_reply"):
            logger.info(f"[HERMES passive] 私聊自决静默 (user={user_id})")
            return result
        reply_text = str(result.structured.get("reply_text") or "").strip()
    else:
        # 防御:同一 Hermes session 之前跑过 reactive 时学到 submit_decision 契约,
        # 切回 passive 后仍可能吐 JSON。检测并抠 reply_text;不命中则用原 raw_text。
        # 2026-10-17 闭环提醒(维护者定案):决策轮信封缺失(模型写了散文忘包 JSON,
        # 区别于 transport 错误)→ 留便签下轮提醒,不靠人介入纠偏。
        if use_private_decision and result.parse_failed and not result.is_transport_error:
            task_notes.push_plugin_note(
                _fb_key, "envelope_missing",
                "系统提示:你上一轮的最终回复没有包 submit_decision JSON 信封(走了散文兜底)。"
                "最终答复必须输出单个 JSON 对象,正文放进 reply_text 字段;过程想法走旁白即可。",
                cooldown_sec=3600.0,
            )
        reply_text = result.raw_text
        extracted = maybe_extract_decision_reply_text(reply_text)
        if extracted is not None:
            if extracted == "":
                logger.info(f"[HERMES passive] LLM 返回 should_reply=false 结构,静默(group={group_id})")
                return result
            logger.warning(f"[HERMES passive] 检测到 submit_decision 形 JSON 残留,抠 reply_text 后发送(group={group_id})")
            reply_text = extracted

    # 类②拦截点(passive,规划书 §3.3):reply_text 定稿后、extract_response_media 前。
    # 命中 action=bounce 规则 → 不投递,立刻追加重交轮;缺省(无 bounce 规则)恒放行。
    # 这里不套合成轮身份豁免:重交轮自己再吐残片正是「第二次检出放行」的那一发。
    if await _maybe_intercept_bounce(
        key=_fb_key, reply_text=reply_text, bot=bot, target=target,
        adapter_name=adapter_name, user_id=user_id, group_id=group_id,
        is_private=is_private,
    ):
        return result

    cleaned_text, extracted_media_urls, extracted_files = extract_response_media(reply_text)
    media_urls = list(result.media_urls) + [u for u in extracted_media_urls if u not in result.media_urls]
    # 本部署补丁(2026-10-08): 本地文件上传。client 层(普通文本路径)已收过一轮
    # file_paths; handler 层对 reply_text 的二次 extract 覆盖 JSON 残留场景。
    file_paths = list(result.file_paths) + [p for p in extracted_files if p not in result.file_paths]

    if not cleaned_text and not media_urls and not file_paths:
        return result
    await send_text_with_media(
        bot=bot,
        target=target,
        text=cleaned_text,
        media_urls=media_urls,
        at_user_id=None if is_private else user_id,
        adapter_name=adapter_name,
    )
    # 2026-10-13 分级反馈:发送成功后的文书暗示/离场确认检查(合成轮豁免)
    await _post_send_feedback(
        key=_fb_key, reply_text=cleaned_text, bot=bot, target=target,
        adapter_name=adapter_name, user_id=user_id, group_id=group_id,
        is_private=is_private, synthetic=user_id in _SYNTHETIC_USER_IDS,
        not_final=bool((result.structured or {}).get("not_final")),
    )
    # 文件跟在文本消息之后上传(占位符在前作说明, 文件随后到达)。
    await send_local_files(bot=bot, target=target, file_paths=file_paths, adapter_name=adapter_name)
    return result


async def _run_reactive_turn(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str,
    text: str,
    image_urls: list[str],
    sticker_urls: list[str] | None = None,
    is_explicit_trigger: bool,
    addressed_to_bot: bool = False,
    now_ms: int,
    nickname: str | None = None,
    trigger_msg_id: str | int | None = None,
    ephemeral: bool = False,
    recent_override: list[BufferedMessage] | None = None,
    context_limit: int | None = None,
    replied_msg_id=None,
    current_buffered: BufferedMessage | None = None,
):
    """跑一发 reactive turn,返回 hermes_client.chat() 的 ChatResult,或 None 表示提前 return。

    外壳 _handle_reactive_path 负责 inflight + 图片门控,这里只管:
    拉 recent → 组 prompt → 调 chat → 解析 decision → 发出向 → 回写 buffer。

    ephemeral=True(本部署新增,隐式激活用):无活跃窗口也跑,现场造一个不注册进
    管理器的临时 session;发言即开窗(见发送成功段),沉默不开窗。
    recent_override: 定时巡查(T2)传入的 T4 窗口消息(新→旧),替代默认拉取。
    context_limit: 覆盖 hermes_perception_buffer 的拉取条数(keyword-soft 的 N1),
    设置时不再做续发轮裁剪。
    """
    assert _mcp.message_buffer is not None and _mcp.active_sessions is not None

    # 用 get_if_active 而非 get():get() 是 debug-only 裸访问,可能返回已过期 session;
    # get_if_active 与 is_active(handle_message 入口处用过)同口径。
    session = _mcp.active_sessions.get_if_active(adapter_name, group_id, now_ms)
    if session is None:
        if not ephemeral:
            # turn 生命期内的 TTL 过期已由 begin_turn/end_turn 租约挡住。剩下两条来路:
            # 上一发 turn 返回了 should_exit_active(唯一会 end() 窗口的地方),或者窗口
            # 恰好在「turn 结束 → refire 接力」的交接瞬间过期且被 cron sweep 扫掉。
            # 两种都意味着有人的触发被吞了 —— 必须留痕,不能静默 return。
            logger.warning(
                f"[HERMES reactive] drop: no active window at turn start "
                f"(group={group_id} user={user_id} explicit={is_explicit_trigger} now_ms={now_ms})"
            )
            return None
        # 隐式激活临时 turn:造一个不注册进管理器的 session 实例。下游读操作
        # (triggered_by / topic_hint / last_bot_reply_at)天然兼容;写操作
        # (touch / mark_bot_replied / end_if_current)按 key 查管理器,全是安全 no-op。
        session = ActiveSession(
            adapter=adapter_name,
            group_id=group_id,
            triggered_by=user_id,
            started_at=now_ms,
            last_active_at=now_ms,
            expires_at=now_ms,  # 已过期:仅作数据载体,不代表有窗口
        )
        logger.debug(f"[HERMES reactive] ephemeral turn start (group={group_id} user={user_id})")

    # B.3: 快照本 turn 入口时的 last_bot_reply_at, 供 chat() 返回后判定 agent loop
    # 期间是否有外部(MCP push_message)推过 bot 自己的回复。 若发生, 即使 LLM 返
    # should_reply=True 也必须抑制本路 send, 否则同 turn 内双答。
    last_bot_reply_at_at_entry = session.last_bot_reply_at

    if recent_override is not None:
        # 定时巡查(T2):T4 窗口消息由调用方过滤好,新→旧顺序与 get_recent 一致。
        recent_full = recent_override
    else:
        recent_full = _mcp.message_buffer.get_recent(
            adapter=adapter_name,
            group_id=group_id,
            limit=context_limit or plugin_config.hermes_perception_buffer,
        )
    # 本部署补丁(增量推送,维护者立项): 只推「没推过」的新消息,
    # 旧历史由 Hermes 服务端转录承担——此前每轮全量重推历史块,块随 user 消息落进
    # 转录、后续每轮重发,复利占输入账单 ~10%(生产实证)。拉取上限即推送上限:
    # 水位过老导致新消息超上限时,更老的永久跳过(与旧窗口外不可见语义一致)。
    # 旧续发轮 OR 双闸/select_followup_window 随之废弃(维护者 2026-10-12 定:配置削减)。
    watermark = session_manager.get_push_watermark(adapter_name, user_id, group_id)
    recent, new_watermark = split_unpushed_messages(recent_full, watermark)

    # 平台账号名 + self_id 一起进 prompt:模型判归属只能靠这两样把消息里回填的
    # 裸 `@<id>` 和 [bot] 历史行绑回自己。取值走进程内缓存,首轮一次 RPC。
    self_nickname = await get_bot_nickname(bot)

    # 2026-10-15 补丁:计算记忆根路径,用于路由提示动态跟随配置
    _custom_dir = resolve_custom_dir()
    _memory_root = str(_custom_dir / "memory" / adapter_name) if _custom_dir else None
    # 身份表（2026-10-15）：发言者标签正主名注释，custom_dir 不可用/查不到都静默 None
    _name_resolver = (
        (lambda uid, _cd=_custom_dir, _ad=adapter_name: identity.resolve_display_name(_cd, _ad, uid))
        if _custom_dir else None
    )
    system_prompt = build_reactive_system_prompt(
        # 群记忆外挂:群档案注入块(私聊/无档案返回 None,函数内静默跳过)
        profile_block=build_group_profile_block(adapter_name, group_id),
        # 2026-10-15 补丁:传入功能开关,私聊时启用则注入简化版路由提示
        group_profile_enabled=plugin_config.hermes_group_profile_enabled,
        memory_root=_memory_root,
        # 2026-10-15 补丁:会话上下文挪入系统提示,吃缓存
        adapter=adapter_name,
        group_id=group_id,
        self_id=str(bot.self_id),
        self_nickname=self_nickname,
    )
    # 本部署补丁(凌晨批项3): 黑名单标记进 runtime_state。
    # 在 turn 内按 (group_id, user_id) 重算而非从 handler 透传——refire/pending
    # 接力链不用为此加字段。
    sender_blacklisted = _is_blacklisted(group_id, user_id)
    # 本部署补丁(凌晨批项4): 取在钉委托锚(懒式超龄剔除)。
    # 2026-10-13 改手动单轨后,入口不再自动钉锚;这里只会读到「之前判断轮已确认
    # 并 pin 成功」的锚。ephemeral 临时 session 不注册进管理器,天然无锚。
    pinned_anchors: list[AnchorRecord] = []
    if not ephemeral:
        pinned_anchors = [
            a
            for a in _mcp.active_sessions.get_anchors(
                adapter_name,
                group_id,
                now_ms,
                max_age_ms=plugin_config.hermes_anchor_max_age_sec * 1000,
            )
            if not (a.ts == now_ms and a.commissioner_id == user_id)
        ]
    # 见 _run_passive_turn 同名调用注释:出站一律内联,不发平台 URL。
    # 本部署补丁(图片投送管线三模式): 取代 9-29 addressed
    # 单点门控,三模式(full/semi/off)+例外名单由 media_config.yaml 现读现用。
    # 判定逻辑收口在 _collect_inline_image_urls;内联走帧组(静图单帧,动图抽帧)。
    media_cfg = load_media_config()
    # 媒体寻址:窗口行 + 当前行建 url→(kind,m:id,idx) 反向映射(规划书 §8 渲染时反查)。
    # B 方案:当前行直接吃触发条目(构造时已凭 event 领得 db 真 id),不再取
    # recent_full[0]——接力场景尾部≠当前消息;旧 user_id 同源护栏只挡跨用户,
    # 同用户连发接力照样错标,随桥一起拆。条目拼在尾部:与 recent_full 内同一
    # db 行撞 url 时后者覆盖前者(id 相同,无害)。条目无 id(合成消息)走现成无号降级。
    current_row = (
        current_buffered if (current_buffered is not None and current_buffered.id is not None) else None
    )
    label_index = build_media_address_index(
        list(recent_full) + ([current_row] if current_row else [])
    )
    sticker_inlined = (
        await inline_gif_frames(sticker_urls or [], cfg=media_cfg)
        if plugin_config.hermes_sticker_vision_enabled
        else []
    )
    inline_urls = _collect_inline_image_urls(
        media_cfg,
        # full 用增量窗口(recent=未推送过的新消息,其图从未进过视野);
        # semi 用全量窗口(recent_full,判定前后文需要看到已推送的紧邻消息)
        window_msgs=recent if media_cfg.mode == "full" else recent_full,
        current_image_urls=image_urls,
        current_user_id=user_id,
        addressed_to_bot=addressed_to_bot,
        self_id=str(bot.self_id),
    )
    inlined_frames = await inline_image_frames(inline_urls, cfg=media_cfg) if inline_urls else []
    # 缺席补占位:请求内联但整组缺席的当前消息媒体,在其号上补「(图#k 加载失败)」(§8);
    # 反查不到号(未入库/例外)的缺席仍走下面的粗占位兜底。
    absent_note = _absent_media_note(
        label_index,
        requested=list(image_urls) + list(sticker_urls or []),
        succeeded=[u for u, _ in inlined_frames] + [u for u, _ in sticker_inlined],
    )
    if absent_note:
        text = (text + " " + absent_note).strip() if text else absent_note
    elif image_urls and not inlined_frames and "[图片]" not in text:
        # 当前消息带图但未内联且无号可标(模式 off/semi 窗口无 @/例外名单):补粗占位,
        # 否则模型不知道图存在。纯图消息 text 为空,下游 current_text 兜底,这里幂等。
        text = (text + " [图片]").strip() if text else "[图片]"
    user_content = build_reactive_user_content(
        addressed_to_bot=addressed_to_bot,
        triggered_by=session.triggered_by,
        triggered_by_nickname=None,
        topic_hint=session.topic_hint,
        recent_messages=recent,
        current_user_id=user_id,
        current_nickname=nickname or user_id,
        current_text=text or "[图片]",
        current_image_urls=_label_groups(label_index, inlined_frames),
        current_sticker_urls=_label_groups(label_index, sticker_inlined),
        current_message=current_row,
        sender_blacklisted=sender_blacklisted,
        pinned_anchors=pinned_anchors,
        # 2026-10-13 分级反馈:弹出本 scope 的插件提示便签贴进本轮(见即焚)
        plugin_note="\n".join(
            task_notes.pop_plugin_notes(_feedback_scope_key(adapter_name, False, user_id, group_id))
        ) or None,
        now_ms=now_ms,
        name_resolver=_name_resolver,
    )

    # 出站 prompt 体量:模型在 explicit @ 下静默时,第一件要排除的就是「上下文被
    # 某条历史撑爆」。只记尺寸不记内容(历史可能几十万字符,也含用户隐私)。
    # oversized_history 记的是 sanitize **之前** 的原始长度 —— 渲染端已经会截断,
    # 但一条超长旧行说明 DB 里躺着脏数据,值得单独告警。
    prompt_chars = len(user_content) if isinstance(user_content, str) else sum(len(str(p)) for p in user_content)
    oversized = [(m.id, len(m.content)) for m in recent_full if len(m.content) > 2000]
    logger.debug(
        f"[HERMES reactive] prompt built group={group_id} user_content_chars={prompt_chars} "
        f"window={len(recent)}/{len(recent_full)} explicit={is_explicit_trigger} "
        f"mode={media_cfg.mode} imgs={len(inlined_frames)}/{len(image_urls)}"
    )
    if oversized:
        logger.warning(
            f"[HERMES reactive] <recent_messages> 含超长历史行(已按渲染上限截断,但 DB 里是脏数据): "
            f"group={group_id} rows={oversized}"
        )

    session_key = session_manager.get_session_key(
        adapter_name=adapter_name,
        is_private=False,
        user_id=user_id,
        group_id=group_id,
    )
    memory_key = session_manager.get_memory_key(
        adapter_name=adapter_name,
        is_private=False,
        user_id=user_id,
        group_id=group_id,
    )
    hermes_target = resolve_target(adapter_name, False, group_id)
    # 注:user_content_override 已携带 user message 的全部内容(text + 多模态);
    # text/image_urls 在 chat() 中会被忽略(见 hermes_client.chat 文档),此处显式传 ""
    # /[] 让契约清晰,避免被读者误以为 image_urls 也参与了构造。
    # 本部署补丁: 旁白实时转发,同 passive 路径注释。
    async def _interim_echo(segment: str) -> None:
        # 2026-10-13 分级反馈:旁白段过 where=interim/all 的 note 规则(确认轮不跟旁白)
        _check_interim_feedback(
            _feedback_scope_key(adapter_name, False, user_id, group_id), segment
        )
        cleaned, _, _ = extract_response_media(segment)
        if cleaned:
            await send_text_with_media(
                bot=bot,
                target=target,
                text=cleaned,
                media_urls=[],
                at_user_id=None,
                adapter_name=adapter_name,
            )

    result = await _chat_with_overflow_recovery(
        "reactive",
        reply_bot=bot,
        reply_target=target,
        text="",
        image_urls=[],
        session_key=session_key,
        memory_key=memory_key,
        user_id=user_id,
        group_id=group_id,
        adapter_name=adapter_name,
        is_private=False,
        mode="reactive",
        expect_structured=True,
        structured_tool_name="submit_decision",
        system_prompt=system_prompt,
        user_content_override=user_content,
        target=hermes_target,
        interim_callback=_interim_echo if plugin_config.hermes_interim_echo_enabled else None,
        approval_callback=(
            _make_approval_notify(bot, adapter_name, f"群 {group_id}", is_private=False, group_id=group_id)
            if plugin_config.hermes_runs_channel_enabled else None
        ),
    )

    # 阶段2b:runs 路 turn 收尾时,上游漏接的迟到 steer 随 completed 上浮——排回
    # pending refire,不能丢(那是用户在 turn 边界上说的话)。add_followup 合并
    # 语义,不顶掉 turn 期间新来的真实跟帖。
    if result is not None and result.pending_steer:
        _mcp.inflight.add_followup(
            make_inflight_key(adapter_name, False, user_id, group_id),
            PendingEntry(
                msg=BufferedMessage(
                    ts=_now_ms(), adapter=adapter_name, group_id=group_id,
                    user_id=user_id, nickname="(迟到 steer)",
                    content=result.pending_steer, image_urls=[], sticker_urls=[],
                    reply_to_ts=None, is_bot=False,
                ),
                is_explicit_trigger=False,
                # 迟到 steer 的出生轮语义:随条目走(2026-10-16 审计修复批:接力改读
                # 条目自己的值后,这里同步盖出生轮的 ephemeral——冷窗关键词轮里上浮的
                # steer 仍按无窗跑,不因接力方有窗而切换跑法,避免交接到无窗轮被吞)。
                ephemeral=ephemeral,
                # 2026-10-17 alive/56:打回收标记,refire 的 post-reply cooldown 闸门
                # 放行——此文本已被 steer_run 收过一次,不是「刚说完接话」类过触发。
                steer_recovered=True,
            ),
        )
        logger.info(
            f"[HERMES reactive] 回收 pending_steer 进 pending "
            f"(group={group_id} len={len(result.pending_steer)})"
        )

    # 增量推送水位推进:仅在内容确定落进上游转录时推进(非 transport 错误、
    # 非 403/暂不可用等 provider 报错家族);失败/本 turn 内发生会话轮换则不推进,
    # 下轮从旧水位(或全量)重推——宁重复,不丢失。
    if (
        new_watermark is not None
        and not result.is_transport_error
        and not result.is_forbidden_error
        and not result.is_outage_error
    ):
        session_manager.advance_push_watermark(
            adapter_name, user_id, group_id, new_watermark, expected_key=session_key
        )

    if result.parse_failed or result.structured is None:
        # 本部署补丁: 403 鉴权类错误硬静默(同 passive 路径注释)。
        # 2026-10-11 扩展: provider 暂不可用/重试耗尽引导文案同口径静默。
        if result.is_forbidden_error or result.is_outage_error:
            _silence_kind = (
                "403 鉴权类错误" if result.is_forbidden_error
                else "provider 暂不可用(重试耗尽)引导文案"
            )
            logger.error(
                f"[HERMES reactive] {_silence_kind},已静默丢弃 (group={group_id})"
                "——检查接入点 key / provider 额度与可用性"
            )
            return result
        # 阶段3b:turn 被主动终止(/run-stop 或 gateway 关停)——硬静默。
        # 终止是管理员的主动动作,命令回执已经说明,不再发「被中断」兜底文本。
        if result.is_interrupted:
            logger.info(f"[HERMES reactive] turn 被终止,硬静默 (group={group_id})")
            return result
        # 2026-10-17 闭环提醒(维护者定案):决策轮信封缺失(模型写散文忘包 JSON;
        # 403/outage/interrupted/transport 均已提前分流,到这里是模型自己的形状问题)
        # → 留便签下轮提醒,不靠人介入纠偏。
        if result.parse_failed and not result.is_transport_error:
            task_notes.push_plugin_note(
                _feedback_scope_key(adapter_name, False, user_id, group_id),
                "envelope_missing",
                "系统提示:你上一轮的最终回复没有包 submit_decision JSON 信封(走了散文兜底)。"
                "最终答复必须输出单个 JSON 对象,正文放进 reply_text 字段;过程想法走旁白即可。",
                cooldown_sec=3600.0,
            )
        # 上游 transport_error(5xx / 网络断 / 流被掐):raw_text 是服务端错误信息原文
        # (如 "Model generated invalid tool call: ..."),原文转发会把内部错误丢到群里
        # 既泄密又难看。换成 config 里的友好兜底文本;空串则静默。
        # parse_failed 但非 transport(LLM 真的回了点啥但结构错):原样转发 raw_text,
        # 仍可能是用户想要的回答。
        if result.is_transport_error and is_explicit_trigger:
            # 阶段2d(维护者定案「丢了就贴」):429 满额丢单(自动重试一次仍 429)不发
            # 文本,在触发消息上贴 emoji 表「忙,稍等」;无 msg id 时退回文本兜底。
            if result.is_rate_limited and trigger_msg_id is not None:
                logger.info(
                    f"[HERMES reactive] 429 满额丢单,贴表情 (group={group_id} msg={trigger_msg_id})"
                )
                await _emoji_like_quiet(bot, trigger_msg_id, plugin_config.hermes_rate_limit_emoji_id)
                return result
            fallback_text = plugin_config.hermes_transport_error_fallback_text
            logger.warning(
                f"[HERMES reactive] transport error fallback "
                f"(group={group_id}, fallback={'silent' if not fallback_text else 'friendly_text'}); "
                f"upstream raw_text suppressed (len={len(result.raw_text or '')})"
            )
            if fallback_text:
                await send_text_with_media(
                    bot=bot,
                    target=target,
                    text=fallback_text,
                    media_urls=[],
                    # 问题5(2026-10-12): transport 错误连模型输出都没有,不 @。
                    at_user_id=None,
                    adapter_name=adapter_name,
                )
            return result

        # raw_text 预览:用来分类失败模式(裸文本 / fenced ```json / 多 JSON 块 /
        # nested object 等)。换行折叠成 \n 字面量,300 字够覆盖单 turn submit_decision。
        raw = result.raw_text or ""
        # 纯散文兜底:模型完全没进 submit_decision 信封(连 "should_reply" 键都没出现),
        # 说明它抛开协议直接自然作答 —— 这段散文本身就是它想说的话,非显式 turn 也照发。
        # 关键区分:信封在场但破了(截断 / 语法脏)可能正带着「这条不归我」的
        # should_reply=false 决定,而那信号只以 JSON 形式存在,拿一段可能不该说的话去撞群
        # 比丢一条更糟,所以那种破法非显式 turn 仍静默。transport / persistence 错误的 raw
        # 是服务端报文,绝不当散文转发,故排除。
        # M9 修复(2026-10-17):信封在场的嗅探从「含 "should_reply" 字面值」收紧为
        # 「以 { 开头或存在 "should_reply": 键值形态」——旧口径下模型正文讨论协议
        # (提及字段名)会被误判破信封,非显式 turn 静默吞话。
        _looks_envelope = raw.strip().startswith("{") or bool(re.search(r'"should_reply"\s*:', raw))
        is_pure_prose = bool(raw) and not result.is_transport_error and not _looks_envelope
        forward_raw = bool(raw) and (is_explicit_trigger or is_pure_prose)
        raw_preview = preview_raw(raw)
        logger.warning(
            f"[HERMES reactive] structured parse failed (group={group_id}, "
            f"transport_error={result.is_transport_error}); fallback="
            f"{('raw_text' if is_explicit_trigger else 'prose') if forward_raw else 'silent'}; "
            f"raw_len={len(raw)} raw_preview={raw_preview!r}"
        )
        # 显式触发降级发 raw_text;非显式触发仅在纯散文时发,其余静默
        if forward_raw:
            fallback_text = raw
            extracted = maybe_extract_decision_reply_text(fallback_text)
            if extracted is not None:
                fallback_text = extracted
            cleaned_text, extracted_media_urls, extracted_files = extract_response_media(fallback_text)
            media_urls = list(result.media_urls) + [u for u in extracted_media_urls if u not in result.media_urls]
            # 本部署补丁(问题5): 散文兜底恒不 @——
            # 信封破了拿不到决策,没有任何 @ 依据,顺带消掉「兜底答错」。
            await send_text_with_media(
                bot=bot,
                target=target,
                text=cleaned_text,
                media_urls=media_urls,
                at_user_id=None,
                adapter_name=adapter_name,
            )
            # 本部署补丁(2026-10-08): 散文兜底同样可能带本地文件引用。
            await send_local_files(
                bot=bot, target=target,
                file_paths=list(result.file_paths) + [p for p in extracted_files if p not in result.file_paths],
                adapter_name=adapter_name,
            )
        return result

    logger.info(
        f"[HERMES reactive] decision adapter={adapter_name} group={group_id} user={user_id} "
        f"explicit={is_explicit_trigger} should_reply={result.structured.get('should_reply')} "
        f"should_exit_active={result.structured.get('should_exit_active')} "
        f"reply_text_len={len(str(result.structured.get('reply_text') or ''))} "
        f"topic_hint={result.structured.get('topic_hint')!r} "
        f"salvaged={result.structured.get('_salvaged', False)}"
    )
    # 解析成功时也留一份 raw 预览:decision 摘要看不出模型到底吐了什么形状
    # (是完整 submit_decision,还是只有 {"should_reply": false} 这种退化输出),
    # 而「该回却静默」的排查恰恰全靠这个区分。DEBUG 级,正常运行不刷日志。
    raw_preview = preview_raw(result.raw_text or "")
    logger.debug(f"[HERMES reactive] decision raw (len={len(result.raw_text or '')}): {raw_preview!r}")

    decision = result.structured
    # 2026-10-13 分级反馈:离场任务便签登记(静默轮也登记)
    _fb_key = _feedback_scope_key(adapter_name, False, user_id, group_id)
    _record_background_note(_fb_key, decision, log_tag="reactive")
    if decision.get("topic_hint"):
        _mcp.active_sessions.update_topic(adapter_name, group_id, str(decision["topic_hint"]))
    # 本部署补丁(凌晨批项4): 模型可控松锚。值=委托人
    # user_id 或 "all"(bool true 宽容视同 all)。ephemeral 无注册窗口,
    # release 按 key 查管理器,自然 no-op。
    release_anchor = decision.get("release_anchor")
    if release_anchor:
        who = "all" if release_anchor is True else str(release_anchor)
        released = _mcp.active_sessions.release_anchor(adapter_name, group_id, who)
        if released:
            logger.info(
                f"[HERMES reactive] anchor released: who={who} count={released} (group={group_id})"
            )
    if decision.get("should_exit_active") and not _mcp.active_sessions.end_if_current(session):
        # 收窗只对本 turn 谈的那个窗口生效:期间若有别人 @bot 建了新窗口,那条 @ 正排在
        # pending 里等 refire,把它的窗口一起弹掉就等于把它静默吞掉。
        logger.info(
            f"[HERMES reactive] should_exit_active not applied: window was re-triggered "
            f"during this turn (group={group_id})"
        )

    # 同一事实两处要用:静默分支判「是不是 push 已答」、send 分支做同 turn 去重。
    # chat() 返回后到这里没有 await,快照一次即可。
    mid_turn_push = session.last_bot_reply_at > last_bot_reply_at_at_entry

    if not decision.get("should_reply"):
        # 显式触发 + LLM 选择沉默是「看起来该回但没回」最常见的来源,
        # 提到 info 让群主能扫日志直接看见「不是插件 bug,是 LLM 自己判定的」。
        # 被点名(addressed_to_you: true 已进 prompt)却静默要更响一档:那是模型否掉了
        # 插件给的既定事实,属于要排查的形状。例外是本 turn 已经由 MCP push 答过 ——
        # 那种静默正是防重复该有的样子。
        emit = logger.warning if addressed_to_bot and not mid_turn_push else logger.info
        emit(
            f"[HERMES reactive] silent: LLM decided should_reply=false "
            f"(group={group_id} user={user_id} explicit={is_explicit_trigger} "
            f"addressed={addressed_to_bot} answered_mid_turn={mid_turn_push} "
            f"topic_hint={result.structured.get('topic_hint')!r})"
        )
        return result

    reply_text = str(decision.get("reply_text") or "").strip()
    cleaned_reply_text, media_urls, file_paths = extract_response_media(reply_text)
    if not cleaned_reply_text and not media_urls and not file_paths:
        return result

    # B.3: 同 turn 内防重复闸门 — 若 chat() agent loop 期间 last_bot_reply_at
    # 已被推进(即 push_message 在中途答过一次), 抑制本路 submit_decision 的 send。
    # 与入口 cooldown 不同, 这里对显式触发也生效:同 turn 双答属纯重复,与触发性质无关。
    # 注: mid_turn_push 直接读 `session.last_bot_reply_at` 而非重新查 active_sessions ──
    # mark_bot_replied 是对同一 dataclass 实例原地写, session 变量持有的就是那个实例。
    # 不查 active_sessions 也回避了 TTL 边界判定与 ended-and-retriggered 罕见竞态。
    if mid_turn_push and not _is_restatement(session.last_bot_reply_text, cleaned_reply_text):
        # 中途 push 过,但这条带的是新内容(典型:push 一句「在查」、decision 给真答案)。
        # 两条都发 —— 抑制它等于把答案吞掉,而 agent 会以为已经发出去了。
        logger.info(
            f"[HERMES reactive] mid-turn push detected but submit_decision adds new content; "
            f"sending both (group={group_id} push_len={len(session.last_bot_reply_text)} "
            f"reply_text_len={len(cleaned_reply_text)})"
        )
    elif mid_turn_push:
        # 复述。中途那发**没投出任何媒体**、而本发带着能投的图 → 不是纯重复,是
        # 「文本已答、图还没出去」。典型来路:agent 用 push_message 发文本,图给的是
        # Hermes 主机本地路径(投不出去),随后 submit_decision 里带着网关内联好的
        # data URL。整条抑制会把那张图彻底丢掉,所以只补媒体、不重复文本。
        if media_urls and session.last_bot_reply_media == 0:
            media_sent = await send_text_with_media(
                bot=bot,
                target=target,
                text="",
                media_urls=media_urls,
                at_user_id=None,
                adapter_name=adapter_name,
            )
            logger.info(
                f"[HERMES reactive] mid-turn push had no media; delivered submit_decision media only "
                f"(group={group_id} media={len(media_urls)} ok={media_sent})"
            )
            if media_sent:
                _mcp.active_sessions.mark_bot_replied(
                    adapter_name, group_id, now_ms=_now_ms(), media_count=len(media_urls), text=""
                )
            return result
        logger.info(
            f"[HERMES reactive] suppress submit_decision reply: it restates the mid-turn push "
            f"(group={group_id} user={user_id} explicit={is_explicit_trigger} "
            f"reply_text_len={len(reply_text)} mid_turn_media={session.last_bot_reply_media})"
        )
        return result

    # 本部署补丁(问题5,维护者拍板): 彻底废弃插件层 @ 兜底,
    # @ 完全由 LLM 决策——只认信封里的 at_user_id,且值必须落在本 turn recent
    # 窗口发言者集合内(防模型 @ 不在场的人);没给/非法就不 @。
    # 理由(维护者原话):@ 兜底本质是废弃机制,LLM 决策完全足够,兜底反而可能答错;
    # @ 是强提醒,漏 @ 无害、错 @ 才烦人。at_user 布尔字段随之废弃(协议契约已同步删除)。
    # M8 修复(2026-10-17):校验集合从 recent(增量水位后新消息)改为 recent_full
    # (缓冲窗全量)——模型上下文按全量渲染,旧口径下「上一轮刚发言、本轮没说话」
    # 的在场者会被当不在场,@ 被静默丢弃(「在场」定义与模型视野错位)。
    _decision_at_id = decision.get("at_user_id")
    at_user: str | None = None
    if _decision_at_id is not None and str(_decision_at_id) in {
        str(m.user_id) for m in recent_full
    }:
        at_user = str(_decision_at_id)
    # 类②拦截点(reactive,规划书 §3.3):cleaned_reply_text 定稿后、send_text_with_media 前。
    # 命中 action=bounce 规则 → 不投递该回复,立刻追加重交轮(不 mark_bot_replied、不入库)。
    if await _maybe_intercept_bounce(
        key=_fb_key, reply_text=cleaned_reply_text, bot=bot, target=target,
        adapter_name=adapter_name, user_id=user_id, group_id=group_id,
        is_private=False,
    ):
        return result

    sent = await send_text_with_media(
        bot=bot,
        target=target,
        text=cleaned_reply_text,
        media_urls=media_urls,
        at_user_id=at_user,
        adapter_name=adapter_name,
    )
    # 本部署补丁(2026-10-08): 本地文件跟在文本消息之后上传。
    await send_local_files(bot=bot, target=target, file_paths=file_paths, adapter_name=adapter_name)
    logger.debug(
        f"[HERMES reactive] sent adapter={adapter_name} group={group_id} "
        f"ok={sent} text_len={len(cleaned_reply_text)} media={len(media_urls)} at_user={at_user}"
    )
    # 2026-10-13 分级反馈:发送成功后的文书暗示/离场确认检查(合成轮豁免)
    if sent:
        await _post_send_feedback(
            key=_fb_key, reply_text=cleaned_reply_text, bot=bot, target=target,
            adapter_name=adapter_name, user_id=user_id, group_id=group_id,
            is_private=False, synthetic=user_id in _SYNTHETIC_USER_IDS,
            not_final=bool(decision.get("not_final")),
        )

    # 用 send 完成时的 wall clock,不复用入参 now_ms。
    # 入参 now_ms 是 _handle_reactive_path 进函数那一刻抓的, chat() + send 可能耗时
    # 任意长(上游重试 / 上下文压缩 / 工具调用累加),复用入参会让 last_bot_reply_at
    # 远早于真实 send 时间, 让下游 cooldown 闸门(_in_post_reply_cooldown)算出来的
    # elapsed 失真,把窗内的 refire 误判成窗外放过去。
    # 调一次 _now_ms() 拿到 reply_now_ms,三个写操作复用这一个快照,既校正 stale
    # 入参,又避免多次读时钟在三个字段间引入毫秒级偏差。
    if sent and _mcp.message_buffer is not None:
        reply_now_ms = _now_ms()
        # 发言即开窗(窗口生命周期定案):隐式激活的临时 turn 真说了话 → 开启活跃
        # 窗口,群内跟帖可被听见。should_exit_active 优先——LLM 明说了「这轮谈完了」,
        # 不得借发言把窗又顶开。开窗后 touch / mark_bot_replied 都有了落点。
        if ephemeral and not decision.get("should_exit_active"):
            _mcp.active_sessions.trigger(adapter_name, group_id, user_id, now_ms=reply_now_ms)
            logger.info(f"[HERMES reactive] ephemeral turn spoke; active window opened (group={group_id})")
        # 回写清洗后的文本 + [图片] 占位,绝不写 reply_text 原文:媒体在回复里是内联
        # data:image;base64 形态,单张图就是几十万字符,原文入库后会被逐 turn 渲染进
        # <recent_messages>,几轮把上下文顶满,模型开始引用错历史。入站侧本来就只存
        # [图片] 占位(见 handle_perception),出站对齐同一约定。
        buffered_content = cleaned_reply_text
        if media_urls:
            buffered_content = f"{buffered_content} [图片]".strip()
        _mcp.message_buffer.append(
            BufferedMessage(
                ts=reply_now_ms,
                adapter=adapter_name,
                group_id=group_id,
                user_id=str(bot.self_id),
                nickname=self_nickname,
                content=buffered_content,
                image_urls=[],
                is_bot=True,
            )
        )
        # 注:若 should_exit_active=True,session 已在上方 end(),touch / mark_bot_replied
        # 都是安全 no-op(两者文档统一:session 缺失则 no-op)。
        _mcp.active_sessions.touch(adapter_name, group_id, now_ms=reply_now_ms)
        # B.2: 记下「bot 刚回过」时间戳,供 _handle_reactive_path 入口 + _refire 入口的
        # cooldown 闸门判定。
        marked = _mcp.active_sessions.mark_bot_replied(
            adapter_name,
            group_id,
            now_ms=reply_now_ms,
            media_count=len(media_urls),
            text=cleaned_reply_text,
        )
        if not marked and not decision.get("should_exit_active"):
            # 排除 should_exit_active:那是本 turn 自己刚 end() 掉窗口,mark 落空是预期的。
            # 剩下的只能是别人的 turn 在本 turn 期间 end() 掉了这个 key(跨用户场景),
            # 本群 cooldown 闸门这一轮失去依据 —— 日志里 last_bot_reply_at 会一直是 0,
            # 没有这条就查不出为什么。
            logger.warning(
                f"[HERMES reactive] mark_bot_replied landed nowhere: active window gone "
                f"(group={group_id}); post-reply cooldown will not engage for follow-ups"
            )
        # 2026-10-13 队列化定案:委托锚改手动单轨——入口不再自动钉,只有本轮
        # submit_decision 显式 pin_anchor=true 且回复真发出去了才钉。放在 mark_bot_replied
        # 之后,避免新锚立刻被自己的发言标成 answered;should_exit_active 不钉。
        # 2026-10-13 审核修复(中危④): 去掉 ephemeral 排除——ephemeral 在 handler 入口
        # 按「开窗前」的 in_active_window 算,冷窗下「@bot+关键词」的委托轮恒 ephemeral=True,
        # 但入口 trigger() 已开真窗口,旧门把这支永远挡在钉锚外且无日志。真正的闸门是
        # 下面的 get_if_active:没窗(ephemeral 沉默/窗已死)自然钉不上,有窗就该能钉。
        # 任务锚重构(2026-10-14,规划书§2.2):merge 双通道优先于新钉——
        # ①信封 anchor_merge=发起者 id(模型判同任务;校验在钉发起者集合,非法走
        #   便签反馈通道,下轮贴回模型自修正);
        # ②quote 自动归并:pin_anchor=true 且当前消息引用的正是某锚的首条委托消息。
        # 闲聊引用不归并(没报 pin/merge 就不评估)。refire 接力出的委托没有
        # replied_msg_id(队列不存引用 id),只能靠信封通道——已知边界(§5.1)。
        merge_starter: str | None = None
        anchor_merge_raw = decision.get("anchor_merge")
        if anchor_merge_raw and not decision.get("should_exit_active"):
            candidate = str(anchor_merge_raw)
            if candidate in _mcp.active_sessions.pinned_commissioners(adapter_name, group_id):
                merge_starter = candidate
            else:
                task_notes.push_plugin_note(
                    _feedback_scope_key(adapter_name, False, user_id, group_id),
                    "anchor-merge-invalid",
                    f"你上一轮填的 anchor_merge={candidate} 不在在钉锚发起者集合,已忽略;"
                    "要归并请填在钉任务的发起者 id",
                    cooldown_sec=0,
                )
                logger.info(
                    f"[HERMES reactive] anchor_merge invalid: {candidate!r} (group={group_id})"
                )
        if (
            merge_starter is None
            and decision.get("pin_anchor") is True
            and not decision.get("should_exit_active")
            and replied_msg_id is not None
        ):
            merge_starter = _mcp.active_sessions.find_anchor_starter_by_msg_id(
                adapter_name, group_id, replied_msg_id
            )
        if merge_starter is not None:
            merge_session = _mcp.active_sessions.get_if_active(adapter_name, group_id, reply_now_ms)
            merged = merge_session is not None and _mcp.active_sessions.merge_into_anchor(
                merge_session, starter_id=merge_starter,
                user_id=user_id, nickname=nickname, now_ms=now_ms,
            )
            logger.info(
                f"[HERMES reactive] anchor merge {'merged' if merged else 'failed'} "
                f"(group={group_id} starter={merge_starter} user={user_id})"
            )
        elif decision.get("pin_anchor") is True and not decision.get("should_exit_active"):
            current_session = _mcp.active_sessions.get_if_active(adapter_name, group_id, reply_now_ms)
            if current_session is None:
                logger.info(f"[HERMES reactive] pin_anchor skipped: no active window (group={group_id} user={user_id})")
            else:
                pinned = _mcp.active_sessions.pin_anchor(
                    current_session,
                    AnchorRecord(
                        participants=[
                            Participant(user_id=user_id, nickname=nickname or user_id, joined_ts=now_ms)
                        ],
                        msg_id=trigger_msg_id,
                        text=(text or "")[:200],
                        ts=now_ms,
                    ),
                    # 2026-10-13 审核修复(BUG2): master_ids 必须拆前缀——
                    # hermes_admin_users 是「adapter:id」复合键(修改19 统一),
                    # 而 commissioner_id 是裸 user_id,直接比恒不命中,维护者槽形同虚设。
                    # 复用 approvals 同款拆法:只取当前适配器名下的裸 id,跨平台不串。
                    master_ids=set(admin_user_ids(set(plugin_config.hermes_admin_users), adapter_name)),
                    max_age_ms=plugin_config.hermes_anchor_max_age_sec * 1000,
                    now_ms=now_ms,
                )
                logger.info(
                    f"[HERMES reactive] pin_anchor {'pinned' if pinned else 'declined'} "
                    f"(group={group_id} user={user_id} msg_id={trigger_msg_id})"
                )

        # 低危⑬-① 方案D(2026-10-14 维护者定案+修正): answered 改模型自答自标——
        # 信封 answer_anchor=委托人 id|"all",回复真发出才标记(与 pin 同门槛,
        # 收窗轮不标记)。错填不静默丢:值校验在钉锚委托人集合,非法值走插件
        # 便签反馈通道,下一轮贴回模型自修正(维护者:「解挂不差一轮聊天」)。
        answer_anchor = decision.get("answer_anchor")
        if answer_anchor and not decision.get("should_exit_active"):
            who = "all" if answer_anchor is True else str(answer_anchor)
            valid_ids = _mcp.active_sessions.pinned_commissioners(adapter_name, group_id)
            if who != "all" and who not in valid_ids:
                task_notes.push_plugin_note(
                    _feedback_scope_key(adapter_name, False, user_id, group_id),
                    "answer-anchor-invalid",
                    f"你上一轮填的 answer_anchor={who} 不在当前在钉锚委托人集合"
                    f"({sorted(valid_ids) or '空'}),已忽略;要标应答请填在钉委托人的 id 或 all",
                    cooldown_sec=0,
                )
                logger.info(
                    f"[HERMES reactive] answer_anchor invalid: who={who!r} "
                    f"pinned={sorted(valid_ids)} (group={group_id})"
                )
            else:
                marked_anchors = _mcp.active_sessions.mark_anchor_answered(adapter_name, group_id, who)
                if marked_anchors:
                    logger.info(
                        f"[HERMES reactive] anchor answered: who={who} count={marked_anchors} (group={group_id})"
                    )

    return result


async def _pop_pending_after_aggregate_window(
    key: tuple[str, str], *, after_ts: int, aggregate: bool
) -> PendingEntry | None:
    """turn 收尾取下一发:私聊被动路径先等一个聚合窗,把尾随消息并进同一条目。

    条目 23 的实现点(规划书 §4.2)。设计要点:
      - 还有 pending 时**不释放 slot**,先 await 聚合窗——窗内新到消息经 try_enter
        自然落队、并入同委托人条目(_merge_pending_message);窗满再一次性 pop 开轮。
        slot 一旦释放,尾随消息就会各自 try_enter 成功、各自开一轮,这正是要治的
        「跨 turn 边界连发」——所以合并只能靠「不释放 slot + 窗内等待」达成。
      - pending 为空(含只有过期条目)时立即返回,秒回场景零额外延迟。
      - aggregate 只在私聊被动路径为 True:reactive 路径与群聊被动路径(active
        session 关闭时的兜底)行为完全不变。
      - pending_steer 回收条目在本 helper 之前已入队,天然并入,无需特判。

    取队判据走 peek_next_pending(非破坏读,与 pop 同 stale 规则),确认有货才等窗。
    """
    assert _mcp.inflight is not None
    if aggregate and _mcp.inflight.peek_next_pending(key, after_ts=after_ts) is not None:
        logger.debug(
            f"[HERMES queue-aggregate] 等聚合窗 {_QUEUE_AGGREGATE_WINDOW_SEC}s key={key}"
        )
        await asyncio.sleep(_QUEUE_AGGREGATE_WINDOW_SEC)
    return _mcp.inflight.pop_next_pending(key, after_ts=after_ts)


def _release_slot_and_retry(key, adapter_name: str, is_private: bool, user_id: str, group_id) -> None:
    """slot 释放 + 同 scope 保留行送达重试(run-resume A 方案,2026-10-16 维护者拍板)。

    旧路:送达窗口超时的保留行只等下次 nb 启动,一等几小时(当日实证 2h 古董消息)。
    新径:任何 turn 收尾(slot 释放)即触发本 scope 的重试;fire-and-forget 不阻塞
    收尾主路,重试内部的 15 分钟时效闸(B)让过期行到此自然丢弃。"""
    _mcp.inflight.exit(key)
    if _mcp.run_tracker is not None:
        from ..tasks.run_resume import retry_scope_deliveries

        spawn_bg_task(retry_scope_deliveries(adapter_name, is_private, user_id, group_id))


async def _handle_passive_path(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    text: str,
    image_urls: list[str],
    sticker_urls: list[str] | None = None,
    is_private: bool,
    now_ms: int,
    nickname: str | None = None,
    event_msg_id: str | int | None = None,
):
    """Passive 外壳:inflight 占位 → _run_passive_turn → 合并重燃。

    与 reactive 同形,key 含 private/group 前缀区分。
    """
    assert _mcp.inflight is not None

    key = make_inflight_key(adapter_name, is_private, user_id, group_id)

    current_buffered = BufferedMessage(
        ts=now_ms,
        adapter=adapter_name,
        group_id=group_id,
        user_id=user_id,
        nickname=nickname or user_id,
        content=text,
        image_urls=list(image_urls),
        sticker_urls=list(sticker_urls or []),
        reply_to_ts=None,
        is_bot=False,
    )
    # B 方案:凭本事件 message_id 领 perception 挂号的 db 真 id;合成消息领不到=None
    current_buffered.id = _claim_event_db_id(adapter_name, event_msg_id)

    if (
        _mcp.inflight.try_enter(
            key,
            current_buffered,
            is_explicit_trigger=True,  # passive 路径只在 (private OR active_session=off) 触发, 二者都需要 user 显式说话
            original_msg_id=event_msg_id,
            now_ms=now_ms,
            commission=True,  # passive/私聊直说都按委托排队,不做闲聊 followup 合并
        )
        != "entered"
    ):
        return

    should_refire = False
    try:
        result = await _run_passive_turn(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            group_id=group_id,
            text=text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_private=is_private,
            now_ms=now_ms,
            current_buffered=current_buffered,
        )
        should_refire = not (result is not None and result.is_transport_error)
    except Exception:
        logger.exception(f"[HERMES] passive turn raised; dropping pending for {key}")
        should_refire = False
        raise
    finally:
        if not should_refire:
            _release_slot_and_retry(key, adapter_name, is_private, user_id, group_id)
        else:
            # 条目 23(规划书章二):私聊被动路径的收尾边界先过聚合窗——窗内尾随消息
            # 并入同一条目后一次性开轮;窗内 slot 不释放(见 helper docstring)。
            pending_entry = await _pop_pending_after_aggregate_window(
                # 2026-10-16 维护者拍板:私聊聚合窗回退止血——当日三连 bug(m:id 错标/
                # 剥皮丢图/延迟送达)均与「合并批无单一 m:id」相关,深层拆批渲染单独立项
                # (见 planned-mods 私聊 m:id 专项);立项落地前私聊不等聚合窗,批规模压低。
                key, after_ts=current_buffered.ts, aggregate=False
            )
            if pending_entry is None:
                _release_slot_and_retry(key, adapter_name, is_private, user_id, group_id)
            else:
                spawn_bg_task(
                    _refire(
                        key=key,
                        trigger_msg=pending_entry.msg,
                        is_explicit_trigger=pending_entry.is_explicit_trigger,
                        addressed_to_bot=pending_entry.addressed_to_bot,
                        original_msg_id=pending_entry.original_msg_id,
                        depth=1,
                        mode="passive",
                        steer_recovered=pending_entry.steer_recovered,
                        bot=bot,
                        target=target,
                        adapter_name=adapter_name,
                        group_id=group_id,
                    )
                )


def _in_post_reply_cooldown(adapter_name: str, group_id: str, now_ms: int) -> bool:
    """B: 判断 (adapter, group_id) 是否处于 bot 上次回复后的冷却窗内。

    输入路径 (`_handle_reactive_path`) 和重燃路径 (`_refire`) 共用,确保:
      - 通过 reactive submit_decision 发出的回复 (_run_reactive_turn 末段写 mark_bot_replied)
      - 通过 MCP push_message 发出的回复 (push_message_impl 写 mark_bot_replied)
    两条路径都把后续非显式触发的旁观消息压在窗内,避免「同主题二次答复」。

    冷却仅对**非显式触发**生效——显式 @bot 必须立刻进 chat;调用方自行判断
    `is_explicit_trigger` 后再询问本 helper。

    冷却窗禁用 (`hermes_reactive_post_reply_cooldown_sec == 0`) 或 session 不存在
    /已过期、未记录过 last_bot_reply_at → 返回 False。
    """
    assert _mcp.active_sessions is not None

    cooldown_sec = plugin_config.hermes_reactive_post_reply_cooldown_sec
    if cooldown_sec <= 0:
        return False
    sess = _mcp.active_sessions.get_if_active(adapter_name, group_id, now_ms)
    if sess is None or not sess.last_bot_reply_at:
        return False
    elapsed_ms = now_ms - sess.last_bot_reply_at
    return 0 <= elapsed_ms < cooldown_sec * 1000


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


def _steer_eligible_ids(
    adapter_name: str, is_private: bool, user_id: str, group_id: str | None, now_ms: int
) -> set[str]:
    """steer 资格集合(任务锚+steer 规划书§3.1,2026-10-14 维护者定案):
    在跑 turn 触发者 ∪ 在钉锚全体参与者。私聊锚不存在,集合={live 触发者}。"""
    ids: set[str] = set()
    tracker = _mcp.run_tracker
    if tracker is not None:
        live = tracker.get_by_scope(make_scope_key(adapter_name, is_private, user_id, group_id))
        if live is not None:
            ids.add(live.user_id)
    if not is_private and group_id is not None and _mcp.active_sessions is not None:
        ids.update(
            _mcp.active_sessions.pinned_participant_ids(
                adapter_name, group_id, now_ms,
                max_age_ms=plugin_config.hermes_anchor_max_age_sec * 1000,
            )
        )
    return ids


async def _steer_text_into_live_run(
    adapter_name: str, is_private: bool, user_id: str, group_id: str | None, text: str
) -> bool:
    """把纯文本 steer 注入本 scope 在跑的 run。无 live run / 已收尾(409) /
    网络异常 → False(调用方落回正常排队,零丢失)。scope 感知版发送原语
    (规划书§3.2/3.3);群聊 followup 槽的 _try_steer_followup 也走它。"""
    tracker = _mcp.run_tracker
    if tracker is None:
        return False
    live = tracker.get_by_scope(make_scope_key(adapter_name, is_private, user_id, group_id))
    if live is None:
        return False
    ok = await hermes_client.steer_run(
        tgt=resolve_target(adapter_name, is_private, group_id), run_id=live.run_id, text=text
    )
    if ok:
        logger.info(
            f"[HERMES steer] 直注在跑 turn (scope={'private' if is_private else 'group'}:"
            f"{user_id if is_private else group_id} run_id={live.run_id} len={len(text)})"
        )
    return ok


async def _maybe_steer_direct(
    *,
    target,
    adapter_name: str,
    user_id: str,
    nickname: str | None,
    group_id: str | None,
    msg_text: str,
    is_explicit_trigger: bool,
    now_ms: int,
) -> bool:
    """资格内纯文本 steer 直注(规划书§3.2/3.3)。True=已注入,调用方不再 dispatch。

    私聊:本 scope 有 live run 即资格成立(在跑 run 必属本人)。
    群聊:仅委托(explicit=addressed)且发送者 ∈ 资格集合才直试;闲聊不直注,
    维持走 reactive 队尾的 followup 槽 steer(_try_steer_followup,现状)。
    """
    if not msg_text.strip():
        return False
    if not target.private:
        if not is_explicit_trigger:
            return False
        if user_id not in _steer_eligible_ids(adapter_name, False, user_id, group_id, now_ms):
            return False
    text = f"{_format_speaker_tag(nickname, user_id)}: {msg_text.strip()}"
    return await _steer_text_into_live_run(adapter_name, target.private, user_id, group_id, text)


async def _try_steer_followup(
    key: tuple[str, str],
    adapter_name: str,
    user_id: str,
    group_id: str,
) -> bool:
    """阶段2b:把 followup 槽里的跟帖 steer 注入在跑的 run,成功=True(不再排队)。

    只接纯文本跟帖:带图的走不了 steer(文本通道),留在槽里走 pending。
    表情包自 2026-10-14 起入队即剥(队列不承载),下方 sticker_urls 检查是
    防御性保留,正常路径不会再命中。注入文本前还会再剥一次 [表情包] 占位——
    占位对在跑 turn 是纯干扰(维护者定案),剥完无有效文本的条目不注入,
    留槽走接力。
    run 已收尾(409)/无 live run/网络异常 → 留在槽里走 pending,语义与旧路完全一致。

    2026-10-13 审核修复(中危②): 改 peek/settle——条目在 steer 全程不离槽,
    期间 turn 收尾由 pop_next_pending 捡走接力,不再存在「离槽-恢复」的静默吞消息路径。
    注入文本是 await 前的快照:期间若有新跟帖并入(合并发生在同一个 entry 上),
    结算时内容对不上就不摘——并入内容留在槽里走 pending 接力(注入过的前缀会重复
    一次,但零丢失;丢失与重复的代价不对称,选重复)。
    """
    # 常备探针(2026-10-14 维护者定案保留,详见 local-mods 修改 25):入口与各早退口
    # 全部 INFO 级留痕——现网 INFO 级别下原 DEBUG 分支不可见,无法区分
    # 「未被调用」与「调用后静默早退」。日志标记 [HERMES steer探针]。
    logger.info(
        f"[HERMES steer探针] 进入 followup steer 尝试 (group={group_id} user={user_id})"
    )
    tracker = _mcp.run_tracker
    if tracker is None:
        logger.info(f"[HERMES steer探针] 早退: run_tracker 为 None (group={group_id})")
        return False
    entry = _mcp.inflight.peek_followup(key)
    if entry is None:
        logger.info(f"[HERMES steer探针] 早退: followup 槽为空 (group={group_id} key={key})")
        return False
    if entry.msg.image_urls or entry.msg.sticker_urls:
        logger.info(
            f"[HERMES steer探针] 早退: 条目带媒体 (group={group_id} "
            f"imgs={len(entry.msg.image_urls)} stickers={len(entry.msg.sticker_urls)})"
        )
        return False
    live = tracker.get_by_scope(make_scope_key(adapter_name, False, user_id, group_id))
    if live is None:
        logger.info(
            f"[HERMES steer探针] 早退: 查无 live run (group={group_id} "
            f"scope={make_scope_key(adapter_name, False, user_id, group_id)})"
        )
        return False
    content_snapshot = entry.msg.content
    # 注入前剥掉 [表情包] 占位(2026-10-14 维护者定案):占位对在跑 turn 是纯干扰,
    # 轮内不需要知道「有人发了表情包」。剥完只剩空壳(纯表情包条目,或跨人并入
    # 留下的「昵称: 」空行)→ 不注入,条目留槽走 pending 接力,语义不变。
    steer_lines = []
    for ln in content_snapshot.split("\n"):
        ln = ln.replace("[表情包]", "").strip()
        if ln and not ln.endswith(":"):
            steer_lines.append(ln)
    steer_text = "\n".join(steer_lines).strip()
    if not steer_text:
        logger.info(
            f"[HERMES steer探针] 早退: 剥占位后无有效文本(纯表情包条目) "
            f"(group={group_id})"
        )
        return False
    text = f"{_format_speaker_tag(entry.msg.nickname, entry.msg.user_id)}: {steer_text}"
    ok = await hermes_client.steer_run(
        tgt=resolve_target(adapter_name, False, group_id), run_id=live.run_id, text=text
    )
    if not ok:
        logger.info(
            f"[HERMES steer探针] 早退: steer_run 返回 False (group={group_id} run_id={live.run_id})"
        )
        return False
    if _mcp.inflight.peek_followup(key) is entry and entry.msg.content == content_snapshot:
        _mcp.inflight.settle_followup(key, entry)
    else:
        # await 期间并入了新内容(或被接力取走):留槽走 pending,不摘。
        logger.debug(f"[HERMES reactive] steer 期间 followup 有并入,留槽接力 (group={group_id})")
    logger.info(
        f"[HERMES reactive] 跟帖已 steer 注入在跑 turn (group={group_id} "
        f"user={entry.msg.user_id} run_id={live.run_id} len={len(text)})"
    )
    return True


async def _handle_reactive_path(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str,
    text: str,
    image_urls: list[str],
    sticker_urls: list[str] | None = None,
    is_explicit_trigger: bool,
    addressed_to_bot: bool = False,
    now_ms: int,
    nickname: str | None = None,
    event_msg_id: str | int | None = None,
    ephemeral: bool = False,
    droppable: bool = False,
    recent_override: list[BufferedMessage] | None = None,
    replied_msg_id=None,
    context_limit: int | None = None,
):
    """Reactive 外壳:inflight 占位 → 调 _run_reactive_turn → finally 合并重燃。

    coalesce 语义:in-flight 期间到来的新触发不并发跑;委托 @ 进按委托人分键的
    FIFO 队列(总 2/每人 1,2026-10-13 由 3 调 2),旁观闲聊只留一个最新 followup。本发完成后
    pop_next_pending 取下一发,用 create_task 起 _refire 接力,本 task 立即 return,
    不阻塞 NoneBot 事件循环。

    ephemeral / recent_override / context_limit(本部署新增)原样透传给
    _run_reactive_turn,语义见该函数 docstring。ephemeral turn 没有窗口,
    cooldown 与租约本来就是 None 安全路径,照常走。
    """
    assert _mcp.inflight is not None and _mcp.active_sessions is not None

    # 图片门控:active window + 非显式触发 + 纯图/纯表情包无文本 → 跳过 chat()
    # 理由:LLM 自己的 should_reply 决策对图片要先看完才能定,而看图本身慢。
    # 这种「旁观纯图」最大概率是 should_reply=false,跳过它就是省一次多模态调用。
    # 消息已被 priority=1 perception 写入 MessageBuffer,等下次文本触发能看到。
    # 2026-10-14 补漏(维护者定案):sticker_urls 纳入门控——此前只查 image_urls,
    # 纯表情包旁观消息穿门控入队,把 followup 槽毒化成「带媒体」连坐 steer。
    in_active = _mcp.active_sessions.is_active(adapter_name, group_id, now_ms)
    if in_active and not is_explicit_trigger and (image_urls or sticker_urls) and not text.strip():
        logger.debug(
            f"[HERMES reactive] skip media-only passive in-window msg "
            f"(group={group_id} user={user_id}); buffered for next text trigger"
        )
        return

    # B: post-reply cooldown — bot 刚在本群发出 reactive 回复 N 秒内,非显式触发的
    # 新消息直接静默。压「我刚说完别人接话→我又凑一句」模式 2。
    # 显式 @bot 触发不受影响(is_explicit_trigger=True 直接旁路)。
    # 写在 inflight try_enter 之前,避免占用 slot 又立刻退出造成 pending 抖动。
    if in_active and not is_explicit_trigger:
        # debug 日志保留入口处,便于运维排查;helper 自己不打日志(refire 路径也会用)
        sess = _mcp.active_sessions.get_if_active(adapter_name, group_id, now_ms)
        cooldown_sec = plugin_config.hermes_reactive_post_reply_cooldown_sec
        logger.debug(
            f"[HERMES reactive] cooldown_check group={group_id} user={user_id} "
            f"sess_exists={sess is not None} "
            f"last_bot_reply_at={sess.last_bot_reply_at if sess else 'n/a'} "
            f"now_ms={now_ms} window_ms={cooldown_sec * 1000}"
        )
        if _in_post_reply_cooldown(adapter_name, group_id, now_ms):
            elapsed_ms = now_ms - (sess.last_bot_reply_at if sess else 0)
            logger.debug(
                f"[HERMES reactive] skip: post-reply cooldown "
                f"(group={group_id} elapsed_ms={elapsed_ms} window_ms={cooldown_sec * 1000})"
            )
            return

    key = make_inflight_key(adapter_name, False, user_id, group_id)
    current_buffered = BufferedMessage(
        ts=now_ms,
        adapter=adapter_name,
        group_id=group_id,
        user_id=user_id,
        nickname=nickname or user_id,
        content=text,
        image_urls=list(image_urls),
        sticker_urls=list(sticker_urls or []),
        reply_to_ts=None,
        is_bot=False,
    )
    # B 方案:凭本事件 message_id 领 perception 挂号的 db 真 id;合成消息领不到=None
    current_buffered.id = _claim_event_db_id(adapter_name, event_msg_id)

    is_commission = addressed_to_bot and user_id not in set(plugin_config.hermes_known_bots)
    # 2026-10-13 审核修复(巡查迟到闸门,维护者定案): droppable=True 的合成轮(巡查)
    # 撞在跑 turn 直接丢弃——「巡查天生可弃」,不占 followup 槽不接力,否则活跃期
    # 巡查会在用户 turn 结束后不请自来。投递轮询等不可弃合成轮不传此标记。
    if droppable and _mcp.inflight.is_inflight(key):
        logger.debug(f"[HERMES reactive] droppable turn discarded (inflight busy): group={group_id}")
        return
    inflight_result = _mcp.inflight.try_enter(
        key,
        current_buffered,
        is_explicit_trigger=is_explicit_trigger,
        original_msg_id=event_msg_id,
        now_ms=now_ms,
        addressed_to_bot=addressed_to_bot,
        # 疑似 A 修复(2026-10-16 审计修复批):条目自带 ephemeral——接力时按
        # 「这条排队消息自己的无窗判定」跑,不再被发包 turn 的值污染。
        ephemeral=ephemeral,
        commission=is_commission,  # 群聊只有平台确认冲 bot 的人类 @ 才进委托队列
    )
    if inflight_result != "entered":
        # 阶段2b:非委托跟帖在 runs 通道下先尝 steer 注入在跑 turn(成功即不排队);
        # 委托/带媒体/旧路/run 已收尾被拒 → 一律维持原 pending 语义。
        # 2026-10-13 审核修复(中危①): 排除 ephemeral——定时巡查(user=timer)、
        # 任务投递(user=delivery)这类合成输入也是非委托纯文本,此前会被原样注入
        # 用户进行中的 turn(合成提示词污染对话),且被消费后不再走 pending。
        if (
            inflight_result == "queued"
            and not is_commission
            and not ephemeral
            and plugin_config.hermes_runs_channel_enabled
            and await _try_steer_followup(key, adapter_name, user_id, group_id)
        ):
            return
        # 入队是 coalesce 的正常语义,但必须可观测:这条分支早先对 bystander 完全静默,
        # 一串消息集体没反应时日志里只剩 cooldown_check,查不出去向。
        # 常备探针(2026-10-14 维护者定案保留,详见 local-mods 修改 25):DEBUG→INFO 并
        # 补 steer 门各条件取值,定位「queued 后未走 steer」时卡在哪一项。
        logger.info(
            f"[HERMES steer探针] queued behind in-flight turn "
            f"(group={group_id} user={user_id} explicit={is_explicit_trigger} "
            f"commission={is_commission} ephemeral={ephemeral} "
            f"runs_channel={plugin_config.hermes_runs_channel_enabled} result={inflight_result})"
        )
        return

    # should_refire_pending=False 仅在 _run_reactive_turn 抛异常时:那是 plugin
    # 自己的 bug,不该把 pending 回放(可能立刻再炸)。transport_error 是上游
    # 故障(Hermes 超时 / 5xx),pending 里的下一发(特别是 explicit @)仍要
    # refire,不能被静默吞掉。
    should_refire_pending = False
    # 租约:本 turn 跑多久,活跃窗就至少活多久。慢 turn(生图 / 长工具链)跨过 TTL 后,
    # 收尾的 touch / mark_bot_replied 和 pending 的 refire 全都依赖窗口还在。
    lease = _mcp.active_sessions.begin_turn(adapter_name, group_id)
    try:
        await _run_reactive_turn(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            text=text,
            image_urls=image_urls,
            sticker_urls=sticker_urls,
            is_explicit_trigger=is_explicit_trigger,
            addressed_to_bot=addressed_to_bot,
            now_ms=now_ms,
            trigger_msg_id=event_msg_id,
            ephemeral=ephemeral,
            recent_override=recent_override,
            context_limit=context_limit,
            replied_msg_id=replied_msg_id,
            current_buffered=current_buffered,
        )
        should_refire_pending = True
    except Exception:
        logger.exception(f"[HERMES] reactive turn raised; dropping pending for {key}")
        raise
    finally:
        # 先还租约(剩余窗口不足时在这里垫到下限),再决定要不要接力 —— 顺序反了
        # 就等于让 refire 去撞一个刚死的窗口。
        _mcp.active_sessions.end_turn(lease, now_ms=_now_ms())
        if not should_refire_pending:
            _release_slot_and_retry(key, adapter_name, False, user_id, group_id)
        else:
            # 条目 23:群聊 reactive 路径**不启用**聚合窗(有群级防抖合并先例,
            # 另行核实再说)——aggregate=False 走同一 helper 保持出队语义单点。
            pending_entry = await _pop_pending_after_aggregate_window(
                key, after_ts=current_buffered.ts, aggregate=False
            )
            if pending_entry is None:
                _release_slot_and_retry(key, adapter_name, False, user_id, group_id)
            else:
                spawn_bg_task(
                    _refire(
                        key=key,
                        trigger_msg=pending_entry.msg,
                        is_explicit_trigger=pending_entry.is_explicit_trigger,
                        original_msg_id=pending_entry.original_msg_id,
                        # 2026-10-13 审核修复(中危⑤): 补上接力透传——此前这条 finally 链路
                        # 丢 addressed_to_bot,排队委托 refire 起跑后 addressed 门控全失效
                        # (图片不内联/归属标记缺行);passive 侧与递归链本来就传。
                        addressed_to_bot=pending_entry.addressed_to_bot,
                        depth=1,
                        mode="reactive",
                        # 疑似 A 修复(2026-10-16 审计修复批):读条目自己的 ephemeral——
                        # 此前透传发包 turn 的值,冷窗关键词 turn 里排队的 addressed
                        # 委托会被以 ephemeral=True 跑掉(不读钉锚 + 二次 trigger 窗口)。
                        ephemeral=pending_entry.ephemeral,
                        # 2026-10-17 alive/56:迟到 steer 回收标记透传(cooldown 豁免)。
                        steer_recovered=pending_entry.steer_recovered,
                        bot=bot,
                        target=target,
                        adapter_name=adapter_name,
                        group_id=group_id,
                    )
                )


async def _refire(
    *,
    key,
    trigger_msg: BufferedMessage,
    is_explicit_trigger: bool,
    original_msg_id: str | int | None,
    depth: int,
    mode: str,
    addressed_to_bot: bool = False,
    ephemeral: bool = False,
    steer_recovered: bool = False,
    bot: Bot,
    target,
    adapter_name: str,
    group_id,
):
    """链式重燃。fire-and-forget,深度上限 MAX_REFIRE_DEPTH。

    ephemeral(本部署新增)沿重燃链透传:临时 turn 期间排进 pending 的
    keyword-soft 命中需要以同样的无窗口方式续跑,否则会在 _run_reactive_turn
    入口的 get_if_active 处被当「窗口没了」误吞。显式 pending 不受影响——
    显式触发在 dispatch 时已 trigger() 开窗,get_if_active 必然命中。"""
    from ..core.inflight import MAX_REFIRE_DEPTH

    assert _mcp.inflight is not None

    # 表情包无需在此降级(2026-10-14 维护者定案):队列不承载表情包——入队时点
    # 已在 inflight 层剥离成 [表情包] 文本占位(见 core/inflight.py
    # _strip_stickers_for_queue),接力出来的条目 sticker_urls 恒为空。
    # 字节在到达时已入库,召回路径不受影响。

    if depth > MAX_REFIRE_DEPTH:
        _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)  # release slot first to avoid race with concurrent arrivals
        logger.warning(f"[HERMES] refire depth exceeded ({depth}); dropping pending {key}")
        return

    # now_ms 用 wall-clock 而不是 trigger_msg.ts:_run_*_turn 内部用它做
    # get_if_active 的 TTL 校验、active_sessions.touch 的滑动续期、以及 bot
    # 自己回复的 BufferedMessage.ts。如果用 trigger 时间会导致 touch 后窗口
    # 比预期早 N 秒过期、bot 回复时间戳倒退。trigger_msg.ts 只在 finally 的
    # pending.ts 比对里用,那是消息到达时序而非「当前是几点」。
    refire_now_ms = _now_ms()

    # B: refire 路径同款 post-reply cooldown 闸门。
    # 仅对非显式触发生效——explicit pending(如 @bot)须穿透 cooldown 直达 chat()。
    # 2026-10-17 alive/56 续:判定时刻从「接力起跑」改为「跟帖到达」——旧口径下
    # 本 turn 一发回复 last_bot_reply_at 即刷新,refire 必撞 8s 窗,等于把全部
    # 「turn 在跑期间到达的合法跟帖」接力一律静默吞掉(当日受控群两轮实测实锤;
    # 跟帖到达时已过入口闸,是合法排队,该不该再答应交接力轮的 submit_decision
    # 自决,而不是被闸门越权替模型决定)。steer_recovered 仍单列豁免:回收条目
    # 的 ts 是回收时刻(网关只还回文本,拿不到原到达时刻),按到达时刻判仍会误拦。
    if (
        mode == "reactive"
        and group_id is not None
        and not is_explicit_trigger
        and not steer_recovered
        and _in_post_reply_cooldown(adapter_name, str(group_id), trigger_msg.ts)
    ):
        logger.info(f"[HERMES reactive] refire skipped by post-reply cooldown (key={key} depth={depth})")
        _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)
        return

    lease = None
    if mode == "reactive" and group_id is not None and _mcp.active_sessions is not None:
        lease = _mcp.active_sessions.begin_turn(adapter_name, str(group_id))

    should_refire = False
    try:
        if mode == "reactive":
            assert group_id is not None
            result = await _run_reactive_turn(
                bot=bot,
                target=target,
                adapter_name=adapter_name,
                user_id=trigger_msg.user_id,
                nickname=trigger_msg.nickname,
                group_id=group_id,
                text=trigger_msg.content,
                image_urls=list(trigger_msg.image_urls),
                sticker_urls=list(trigger_msg.sticker_urls),
                is_explicit_trigger=is_explicit_trigger,
                addressed_to_bot=addressed_to_bot,
                now_ms=refire_now_ms,
                trigger_msg_id=original_msg_id,
                ephemeral=ephemeral,
                current_buffered=trigger_msg,
            )
        else:
            result = await _run_passive_turn(
                bot=bot,
                target=target,
                adapter_name=adapter_name,
                user_id=trigger_msg.user_id,
                group_id=trigger_msg.group_id,
                text=trigger_msg.content,
                image_urls=list(trigger_msg.image_urls),
                sticker_urls=list(trigger_msg.sticker_urls),
                is_private=trigger_msg.group_id is None,
                now_ms=refire_now_ms,
                current_buffered=trigger_msg,
            )
        should_refire = not (result is not None and result.is_transport_error)
        if result is None and is_explicit_trigger:
            # 排队的显式触发一路走到这里却没跑成 turn = 用户的 @ 被吞了。
            # 具体原因由 _run_*_turn 内部那条 drop 日志给出,这条负责点出「被吞的是
            # 一个排过队的 explicit @」,两条合起来才够定位。
            logger.warning(
                f"[HERMES] queued explicit trigger produced no turn (key={key} depth={depth} msg_id={original_msg_id})"
            )
    except Exception:
        logger.exception(f"[HERMES] refire raised at depth {depth}; dropping pending for {key}")
        should_refire = False
    finally:
        if lease is not None:
            _mcp.active_sessions.end_turn(lease, now_ms=_now_ms())
        if not should_refire:
            _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)
            # B012 豁免: try 侧异常已被上方 except 捕获记录,不存在被吞的活异常;
            # refire 是独立 create_task 的旁路接力,CancelledError 窗口仅停机时。
            return  # noqa: B012
        pending_entry = await _pop_pending_after_aggregate_window(
            key,
            after_ts=trigger_msg.ts,
            # 条目 23:接力轮的收尾边界是链式连发的另一半——私聊被动接力同样过聚合窗
            # (mode="passive" 且 group_id is None = 私聊);群聊接力/群聊被动保持不变。
            # 2026-10-16 维护者拍板:私聊聚合窗回退止血(同 _handle_passive_path 处注释),
            # 深层拆批渲染立项落地前统一不等窗。
            aggregate=False,
        )
        if pending_entry:
            spawn_bg_task(
                _refire(
                    key=key,
                    trigger_msg=pending_entry.msg,
                    is_explicit_trigger=pending_entry.is_explicit_trigger,
                    addressed_to_bot=pending_entry.addressed_to_bot,
                    original_msg_id=pending_entry.original_msg_id,
                    depth=depth + 1,
                    mode=mode,
                    # 疑似 A 修复(2026-10-16 审计修复批):链上每一跳都按「当前条目
                    # 自己的 ephemeral」接力,不再把链首外层值一路透传下去。
                    ephemeral=pending_entry.ephemeral,
                    # 2026-10-17 alive/56:迟到 steer 回收标记沿链透传。
                    steer_recovered=pending_entry.steer_recovered,
                    bot=bot,
                    target=target,
                    adapter_name=adapter_name,
                    group_id=group_id,
                )
            )
        else:
            _release_slot_and_retry(key, adapter_name, trigger_msg.group_id is None, trigger_msg.user_id, trigger_msg.group_id)


async def route_synthesized_input(
    *,
    bot: Bot,
    target,
    adapter_name: str,
    user_id: str,
    group_id: str | None,
    nickname: str | None,
    text: str,
    allow_passive: bool,
    addressed_to_bot: bool,
    now_ms: int,
):
    """合成消息的统一入口,供 notice handler 复用既有 message routing。

    派发规则:
      - private (target.private=True) → 仅 allow_passive=True 才走 passive,否则跳过
      - group + active_session 开 → 触发 active session 并走 reactive
        (synth 始终算 is_explicit_trigger=True)

    `addressed_to_bot` 由合成方按事件语义给:平台确认事件指向 bot 本体(如戳 bot)
    才是 True;群级事件(如入群)虽然 explicit 触发,但不是「冲 bot 说的」,
    断言了就是对模型撒谎,还会让「被点名却静默」的 WARNING 变成噪音。
      - group + active_session 关 → 仅 allow_passive=True 才走 passive

    `allow_passive` 控制无 active session 时的兜底:
      - 戳一戳: True (任何 mode 都开口)
      - 入群: False (仅 active 开时通过 reactive 让 Hermes 自决,否则不打扰)
    """
    if target.private:
        if not allow_passive:
            return
        await _handle_passive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=None,
            text=text,
            image_urls=[],
            is_private=True,
            now_ms=now_ms,
        )
        return

    # 群聊
    if not plugin_config.hermes_active_session_enabled:
        if not allow_passive:
            return
        await _handle_passive_path(
            bot=bot,
            target=target,
            adapter_name=adapter_name,
            user_id=user_id,
            nickname=nickname,
            group_id=group_id,
            text=text,
            image_urls=[],
            is_private=False,
            now_ms=now_ms,
        )
        return

    # 群 + active_session 开 → 显式触发 + reactive
    # (与 handle_message 显式触发同语义: 先 trigger,再 _handle_reactive_path)
    assert _mcp.active_sessions is not None
    _mcp.active_sessions.trigger(adapter_name, group_id or "", user_id, now_ms=now_ms)
    logger.info(f"[HERMES notice] synthesized reactive trigger: {adapter_name}/{group_id} by {user_id}")
    await _handle_reactive_path(
        bot=bot,
        target=target,
        adapter_name=adapter_name,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
        text=text,
        image_urls=[],
        is_explicit_trigger=True,
        addressed_to_bot=addressed_to_bot,
        now_ms=now_ms,
    )
