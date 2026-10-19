"""
入站解析域:入口 ignore 规则、@bot 检测(含 original_message 兜底)、
黑名单判定、单例未装配节流告警、昵称清洗与发送者/被引者归属标签、
引用前缀文本改写(At 段派生与字面手打 @ 的有界兜底)。
"""
from __future__ import annotations

import re
import time

import nonebot_plugin_alconna as alconna
from nonebot import logger
from nonebot.adapters import Event

from ..config import plugin_config


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
      - chat 不触发

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


def _is_blacklisted(group_id: str | None, user_id: str) -> bool:
    """黑名单判定(本部署补丁)。

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
# 正常人不会起这种名片,见即没收。
_FORGED_ID_RE = re.compile(r"[iI]\s*[dD]\s*＝\s*\S+")

# 单例未装配 WARN 的节流状态(模块级,60s 一次)。
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
    # 先归一化再校验——等号全角化后,
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
    """引用归属标签的定界符加固。

    `(引用 昵称(id): …)` 形态里半角 ( ) : 是结构字符——群名片或异平台 id 里
    混入这些字符能伪造归属边界、往引用块注入假内容。全角化后语义不变、结构不破。
    只用于归属标签拼装;昵称的通用清洗仍是 _sanitize_nickname。
    注意:映射表必须用 \\u 转义写,直接写全角字面量会被上游管道规范化回半角
    (踩过:replace("(", "(") 两侧同字节,等于没替换)。
    """
    _FW = {"(": "\uff08", ")": "\uff09", ":": "\uff1a", "[": "\uff3b", "]": "\uff3d"}
    return "".join(_FW.get(ch, ch) for ch in value)


def _extract_replied_sender_label(event: Event) -> str:
    """取被引消息的发送者标签 `昵称(user_id)`,取不到返回 ''。

    本部署补丁:信封的 `(引用: ...)` 原本不带被引消息归属,
    LLM 分不清引用的是 bot 自己的话、当前发言者的话还是第三方的话——「引用 A 的
    旧消息对 B 说『说的就是你』」会因归属缺失被误判成冲 bot 说。OneBot v11 的
    Reply 对象自带 sender(user_id/card/
    nickname),无需额外 API;昵称沿用 _extract_sender_nickname 的群名片优先约定
    并过 _sanitize_nickname 防 [user=…] 标签伪装。非 OneBot adapter 或无 sender
    字段时经 getattr 链安全落 '',下游回退旧的无归属格式,行为不退化。
    """
    try:
        reply = getattr(event, "reply", None)
        sender = getattr(reply, "sender", None)
        if sender is None:
            # v12 等 adapter 的 reply 不带 sender——记一笔
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

    本部署补丁(用「提及」):半角「@<bot_id>」
    随引用文本进 current_message 会字面命中 decision 协议「`@<你的 id>` 就是在 @ 你」
    的规则(群 200000003 误接话事故:引用含 @bot 的旧消息 + 当前在对
    别人说话,被判成冲 bot 说)。改写后 id 信息保留、字面 token 断开,「提及」与@
    语义等价。只作用于引用块内——当前消息自身的 @ 占位(at_placeholders)维持
    「@<id>」原格式,协议块靠「块外才是当前@」区分两者。
    """
    return " ".join("提及" + p.removeprefix("@") for p in placeholders)


# 字面手打 @ 的有界匹配:仅 QQ 号段(5-12 位数字,后不接数字),别碰邮箱/@英文名。
_QUOTED_LITERAL_AT_RE = re.compile(r"@(\d{5,12})(?!\d)")
_QUOTED_LITERAL_ATALL_RE = re.compile(r"@全体(?:成员)?")


def _rewrite_quoted_literal_ats(text: str) -> str:
    """引用文本里字面手打的「@QQ号」/「@全体」改写「提及」。

    _rewrite_quoted_ats 只覆盖 At 段派生的占位;手打进文本的 @数字 不经过它,
    会字面命中决策协议「@<id>=@你」规则——群 200000003 事故的同形态残余面。
    这里补一道有界兜底(只认 QQ 号段,不误伤邮箱/英文@)。只作用引用块内,
    当前消息正文不经过本函数。
    """
    text = _QUOTED_LITERAL_ATALL_RE.sub("提及全体", text)
    return _QUOTED_LITERAL_AT_RE.sub(r"提及\1", text)
