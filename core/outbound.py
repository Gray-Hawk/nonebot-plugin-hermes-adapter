"""统一出向发送器。

封装 alconna UniMessage 拼装 + send。
被两条路径共享:
  1. handlers/message.py 中收到结构化 reply_text 后发送
  2. mcp/tools/push_message.py 反向通道收到 push_message 后发送
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import nonebot_plugin_alconna as alconna
from nonebot import logger
from nonebot.adapters import Bot

from .chat_config import load_chat_config
from .log_throttle import throttled_info
from .media_config import load_media_config
from .misc_config import load_misc_config
from .sticker_library import (
    StickerLibrary,
    StickerLibraryError,
    default_sticker_library,
    sticker_feature_enabled,
)


# --- runs 半接管交付上下文(本部署补丁, alive/108 提交 B)。 ---
# delivery 为 None 时所有行为与旧版逐字一致;携带时发送前先原子认领组件,
# 平台成功后立即写回执。已 sent 的组件绝不再次调平台,unknown(平台结果
# 不明)一律失败关闭,不盲补。


@dataclass(frozen=True)
class DeliveryContext:
    """一次出站发送的交付台账身份;tracker 只依赖 run_tracker 的公开方法。"""

    tracker: Any
    run_id: str
    component_id: str = "primary"
    owner: str = field(default_factory=lambda: f"nb-{os.getpid()}")
    lease_sec: float = 30.0
    # alive/108:每次真正认领出站前再核 clear 水位;None 保持旧调用零变化。
    generation_guard: Any = None

    def for_component(self, component_id: str) -> "DeliveryContext":
        return replace(self, component_id=component_id)


class DeliveryClaimBusy(RuntimeError):
    """组件被他人认领或处于 unknown;调用方不得继续调平台。"""


def _delivery_begin(delivery: DeliveryContext | None) -> tuple[str, list]:
    """认领组件;sent 时同时返回既有 msg_ids(可能为空列表)。"""
    if delivery is None:
        return "send", []
    if delivery.generation_guard is not None:
        try:
            current = bool(delivery.generation_guard())
        except Exception as exc:
            logger.warning(f"[OUTBOUND] 代际栅栏读取失败,失败关闭 run={delivery.run_id}: {exc}")
            current = False
        if not current:
            # clear 与旧 run 晚完成/状态回写并发时,仍不得让旧答复穿透。
            try:
                delivery.tracker.set_expected_state(
                    delivery.run_id, "cancelled", cancel_reason="generation_cleared"
                )
            except Exception:
                logger.warning(f"[OUTBOUND] 旧代销账失败 run={delivery.run_id}", exc_info=True)
            raise DeliveryClaimBusy(f"stale generation: run={delivery.run_id}")
    outcome = delivery.tracker.claim_component(
        run_id=delivery.run_id, component_id=delivery.component_id,
        owner=delivery.owner, lease_sec=delivery.lease_sec,
    )
    existing = []
    if outcome == "sent":
        comp = delivery.tracker.get_component(
            run_id=delivery.run_id, component_id=delivery.component_id
        )
        existing = list((comp or {}).get("msg_ids") or [])
    return outcome, existing


def _delivery_raise_if_busy(outcome: str, delivery: DeliveryContext) -> None:
    if outcome == "busy":
        raise DeliveryClaimBusy(
            f"delivery component busy/unknown: run={delivery.run_id} "
            f"component={delivery.component_id}"
        )


def _delivery_mark_unknown(delivery: DeliveryContext | None, error) -> None:
    if delivery is None:
        return
    try:
        delivery.tracker.mark_component_unknown(
            run_id=delivery.run_id, component_id=delivery.component_id, error=error
        )
    except Exception as exc:
        # 回执写入失败不改变原异常面;组件保持 claimed,租约过期后由
        # reconcile 按 unknown 事实处理。
        logger.warning(f"run={delivery.run_id} 组件异常回执写入失败: {exc}")

# api_server 把本地生成的图片内联成 data:image/…;base64 URL——没有可抓取的
# http 地址,必须解码成字节直接附加。仅支持 base64 形态;RFC 2397 的
# percent-encoded 形态不支持(上游不产生)。
_IMAGE_DATA_URL_RE = re.compile(r"^data:(image/[\w.+-]+);base64,(.+)$", re.DOTALL)


def _parse_image_data_url(url: str) -> tuple[bytes, str, str] | None:
    """解析 base64 图片 data: URL → (raw_bytes, mimetype, b64_payload);不合法返回 None。

    宽容只做无损的两件事:去掉空白(payload 内联进 JSON 后可能被折行)、补齐缺失的
    `=` padding(补 padding 还原出的字节与原图一致)。**不做**有损修补:丢字符或
    放宽字符集校验能让半截 base64 也"解出来",但那是坏图——平台要么拒收要么显示
    破图,还把「上游把回复截断了」伪装成「发图成功」,连日志都查不到。宁可跳过。
    """
    m = _IMAGE_DATA_URL_RE.match(url)
    if m is None:
        return None
    mimetype = m.group(1)
    payload = re.sub(r"\s+", "", m.group(2))
    if not payload:
        return None

    rem = len(payload) % 4
    if rem in (2, 3):
        payload += "=" * (4 - rem)

    try:
        raw = base64.b64decode(payload, validate=True)
    except (binascii.Error, ValueError):
        return None

    if not raw:
        return None

    return raw, mimetype, payload


# 截断 suffix 长度 = 13 字符;最终消息 = max_len + 13 字符,故意溢出 max_len。
# 所有已知 adapter 上限 ≥ 4500,默认 max_len=4000,溢出安全。
_TRUNCATION_SUFFIX = "\n\n…（消息过长,已截断）"

# ---------------------------------------------------------------- 受控内联 @ 原语
#
# reply_text 里允许出现**唯一一种**控制标记:`[CQ:at,qq=<ASCII 数字>]`,可多次、
# 可出现在任意位置,与前后文字保持原序。出站前逐个转成 alconna At 段,绝不让标记
# 字面进 QQ。这里刻意不是「CQ 码解析器」:除 at 之外的任何 CQ 段(image/face/
# record/music/share…)、以及 at 的非数字形态都不转换,原样留在正文里当普通文本。
# 理由:模型能写的标记面越小,越不会有「用户收到不可见段」这类无从解释的故障;
# 需要扩类型时按需显式加白,不做形态猜测。
_INLINE_AT_MARKER_RE = re.compile(r"\[CQ:at,qq=([0-9]{1,20})\]")

# 观测用:正文里残留的 CQ 段形态。只统计与记日志,一个字节都不改。
_CQ_LIKE_RE = re.compile(r"\[CQ:[A-Za-z_]+[,\]]")

_REPLY_ID_RE = re.compile(r"-?[0-9]+")
_STICKER_MARKER_RE = re.compile(r"表情=([A-Za-z0-9_-]{1,64})")


@dataclass(frozen=True)
class SendResult:
    """一次发送的成功状态与平台回执 ID。

    “发送成功但没有回执 ID”与“发送失败”必须分开：前者仍要写入本地 bot 消息、
    结束 run-resume 等待并向调用方报告成功，只是 qq_message_id 留 NULL。
    """

    ok: bool
    msg_ids: list[str]
    sticker_id: str | None = None

    def __bool__(self) -> bool:
        return self.ok

    @property
    def primary_qq_message_id(self) -> str | None:
        return self.msg_ids[0] if self.msg_ids else None

    @property
    def sticker_media_count(self) -> int:
        return 1 if self.ok and self.sticker_id else 0

    def history_content(self, original_text: str) -> str:
        """bot 历史不暴露内部选图标记，沿用入站表情占位约定。"""
        return "[表情包]" if self.sticker_media_count else original_text


def send_result_history(result: object, original_text: str) -> str:
    """读取新结果的历史文案；旧结果对象与布尔返回值保持原行为。"""
    method = getattr(result, "history_content", None)
    if not callable(method):
        return original_text
    value = method(original_text)
    return value if isinstance(value, str) else original_text


def send_result_sticker_count(result: object) -> int:
    """混合基线下，旧发送结果没有表情字段时按零计。"""
    return int(getattr(result, "sticker_media_count", 0) or 0)


@dataclass(frozen=True)
class _InlineAt:
    """解析结果里的一个受控内联 @ 目标。"""

    qq: str


def _parse_inline_at_markers(text: str) -> list[str | _InlineAt]:
    """把 reply_text 切成 文本 / 内联 @ 的有序片段。

    只有精确形态 `[CQ:at,qq=<ASCII 数字>]` 转段;其余一律留在文本片段里:
      - `[CQ:at,qq=all]`、`[CQ:at,qq=0]`:OneBot 侧等同 @全体成员,不开放;
      - `[CQ:at,qq=]`、`[CQ:at,qq=abc]`、全角逗号等形态不符的:不认;
      - 任何非 at 类型(`[CQ:image,…]` 等):不认。
    相邻标记不产生空文本片段;空串返回空列表。
    """

    tokens: list[str | _InlineAt] = []
    pos = 0
    for match in _INLINE_AT_MARKER_RE.finditer(text):
        qq = match.group(1)
        if int(qq) == 0:
            # OneBot v11 把 qq=0 当 @全体成员,不在受控面内:不切断文本,原样留下。
            continue
        if match.start() > pos:
            tokens.append(text[pos : match.start()])
        tokens.append(_InlineAt(qq))
        pos = match.end()
    if pos < len(text):
        tokens.append(text[pos:])
    if not tokens and text:
        tokens.append(text)
    return tokens


def _tokens_to_segments(tokens: Sequence[str | _InlineAt]) -> list:
    """片段 → alconna 段列表(文本用裸 str,由 UniMessage 构造期收成 Text)。"""

    return [alconna.At("user", tok.qq) if isinstance(tok, _InlineAt) else tok for tok in tokens]


def _truncate_tokens(
    tokens: Sequence[str | _InlineAt], max_len: int
) -> tuple[list[str | _InlineAt], bool]:
    """按文本字符数截断片段序列,返回 (截断后片段, 是否发生截断)。

    @ 段不占文本预算(它不是文本);截断点之后的片段(含 @)整体丢弃——与「截断就是
    不要后半段」的既有语义一致,也保证不会把半个标记发出去。
    """

    kept: list[str | _InlineAt] = []
    used = 0
    for tok in tokens:
        if isinstance(tok, _InlineAt):
            kept.append(tok)
            continue
        remain = max_len - used
        if remain <= 0:
            return kept, True
        if len(tok) > remain:
            kept.append(tok[:remain])
            return kept, True
        kept.append(tok)
        used += len(tok)
    return kept, False


def _warn_residual_cq(tokens: Sequence[str | _InlineAt]) -> None:
    """正文里残留 CQ 段形态(被拒的 at 或其它类型)时留一条节流观测。"""

    samples: list[str] = []
    for tok in tokens:
        if isinstance(tok, str):
            samples.extend(m.group(0) for m in _CQ_LIKE_RE.finditer(tok))
    if not samples:
        return
    throttled_info(
        "outbound.residual_cq",
        f"[OUTBOUND] reply_text 有 {len(samples)} 处 CQ 段形态未转换,按普通文本发送: "
        f"{samples[:3]}",
    )


def _normalize_reply_id(raw: str | int | None, *, target: object = None) -> str | None:
    """引用消息 id 归一化:OneBot int32 十进制形态才用,其余值返回 None。

    非数字值送进 Reply 段会在导出期 int() 抛错,把整条回复带崩;这里先挡并留 WARN,
    让“引用没生效”在日志里有据可查,而不是静默不引用。负数是合法 int32,不能误杀。
    """

    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if _REPLY_ID_RE.fullmatch(text) is None:
        logger.warning(f"[OUTBOUND] 引用消息 id 非法(需 OneBot int32 十进制),已忽略: {text[:32]!r} target={target}")
        return None
    return text


# Bot self-nickname per process (per bot.self_id). Used for forward-node "name" field.
# get_login_info is an extra OneBot RPC; cache result for the lifetime of the process.
# 内存兜底(本部署补丁):插序容量上限,超了把最旧的一批削掉——self_id 集合实际
# 很小,这只是防长跑/异常重连下缓慢膨胀的边界。
_NICKNAME_CACHE_MAX = 256
_bot_nickname_cache: dict[str, str] = {}


async def get_bot_nickname(bot: Bot) -> str:
    """Return the bot's display nickname, cached per self_id. Fallback 'Bot' on failure.

    返回值一定是非空 str:除 forward-node 的 "name" 外,它还会进 prompt 的
    runtime_state.you 和 <recent_messages> 的 bot 行 —— 实现端返回 null / 非字符串
    时不能把那个对象原样渲染进 prompt。
    """
    self_id = bot.self_id
    if self_id in _bot_nickname_cache:
        return _bot_nickname_cache[self_id]
    nickname = "Bot"
    try:
        info = await bot.call_api("get_login_info")
        raw = info.get("nickname") if isinstance(info, dict) else None
        if isinstance(raw, str) and raw.strip():
            nickname = raw.strip()
    except Exception as exc:
        logger.debug(f"[OUTBOUND] get_login_info failed (self_id={self_id}): {exc}")
    if len(_bot_nickname_cache) >= _NICKNAME_CACHE_MAX:
        for k in list(_bot_nickname_cache)[: _NICKNAME_CACHE_MAX // 4]:
            _bot_nickname_cache.pop(k, None)
    _bot_nickname_cache[self_id] = nickname
    return nickname


def _split_text_chunks(text: str, max_chars: int) -> list[str]:
    """把长文本切成若干 ≤ max_chars 的块。

    策略(与旧转发分包一致,未改语义):
      1. 先按 '\\n\\n' 分段;单段超限就按 '\\n' 递归切,再不行硬切。
      2. 段落/碎片再累积成不超过 max_chars 的块。
    空文本返回 [''](保留「至少一块」语义)。
    """

    if len(text) <= max_chars:
        return [text]

    def _split_chunk(chunk: str) -> list[str]:
        if len(chunk) <= max_chars:
            return [chunk]
        parts = chunk.split("\n")
        if len(parts) > 1:
            return _accumulate(parts)
        result = []
        while chunk:
            result.append(chunk[:max_chars])
            chunk = chunk[max_chars:]
        return result

    def _accumulate(pieces: list[str]) -> list[str]:
        buckets: list[str] = []
        current = ""
        for piece in pieces:
            if len(piece) > max_chars:
                if current:
                    buckets.append(current)
                    current = ""
                buckets.extend(_split_chunk(piece))
            elif current and len(current) + 1 + len(piece) > max_chars:
                buckets.append(current)
                current = piece
            else:
                current = (current + "\n" + piece) if current else piece
        if current:
            buckets.append(current)
        return buckets

    paragraphs = text.split("\n\n")
    segments: list[str] = []
    for para in paragraphs:
        if len(para) > max_chars:
            segments.extend(_split_chunk(para))
        else:
            segments.append(para)

    buckets: list[str] = []
    current = ""
    for seg in segments:
        separator = "\n\n" if current else ""
        if current and len(current) + len(separator) + len(seg) > max_chars:
            buckets.append(current)
            current = seg
        else:
            current = current + separator + seg
    if current:
        buckets.append(current)
    return buckets or [""]


def _pack_forward_nodes(
    tokens: Sequence[str | _InlineAt],
    *,
    bot_self_id: str,
    bot_nickname: str,
    max_chars_per_node: int = 3500,
) -> list[alconna.CustomNode]:
    """把 文本/@ 片段装进合并转发的自定义节点。

    与纯文本分包的区别:**@ 段是独立段,不参与字符预算、也不会被切断**——先按片段
    切分文本,再按预算开新节点,@ 落在它当时所在的节点里,跨节点时各节点自带自己的
    @,整包不丢人也不重排。返回至少一个节点(空文本返回一个空内容节点)。
    """

    contents: list[list] = [[]]
    lengths: list[int] = [0]
    for tok in tokens:
        if isinstance(tok, _InlineAt):
            contents[-1].append(alconna.At("user", tok.qq))
            continue
        for chunk in _split_text_chunks(tok, max_chars_per_node):
            if not chunk:
                continue
            if contents[-1] and lengths[-1] + len(chunk) > max_chars_per_node:
                contents.append([])
                lengths.append(0)
            contents[-1].append(alconna.Text(chunk))
            lengths[-1] += len(chunk)
    if len(contents) > 1 and not contents[-1]:
        # 末节点是空壳(切分刚好落在边界)时不留空节点。
        contents.pop()
    return [
        alconna.CustomNode(str(bot_self_id), bot_nickname, content) for content in contents
    ]


# 本部署补丁: 本地文件上传(群文件/私聊文件)。
async def send_local_files(
    *,
    bot: Bot,
    target: alconna.Target,
    file_paths: Sequence[str],
    adapter_name: str | None = None,
    delivery: DeliveryContext | None = None,
) -> int:
    """把白名单内的本地文件经 upload_group_file / upload_private_file 上传,返回成功数。

    只在 onebotv11 启用(其余 adapter 没有对应 API);白名单判定逐文件进行,
    被拒/上传失败的单个文件不影响其余, 日志各记一条。文件与文本消息分开发:
    OneBot 的文件上传是独立 API, 不混在消息段里。

    携带 delivery(alive/108)时按原列表位置派生 file:<index> 组件:白名单拒绝
    是平台调用前的本地失败记 failed;已 sent 的文件跳过上传但计入成功数;
    上传 API 异常记 unknown(不自动重传)。
    """
    if not file_paths:
        return 0
    if adapter_name != "onebotv11":
        logger.warning(f"[OUTBOUND] 本地文件上传仅支持 onebotv11, 跳过 {len(file_paths)} 个文件 (adapter={adapter_name})")
        return 0
    from .media_whitelist import validate_local_file

    max_bytes = load_media_config().local_file_max_mb * 1024 * 1024
    sent = 0
    for idx, raw in enumerate(file_paths):
        ctx = delivery.for_component(f"file:{idx}") if delivery is not None else None
        path = validate_local_file(raw, max_bytes=max_bytes)
        if path is None:
            if ctx is not None:
                ctx.tracker.mark_component_failed(
                    run_id=ctx.run_id, component_id=ctx.component_id,
                    error="whitelist rejected or missing",
                )
            logger.warning(f"[OUTBOUND] 本地文件被白名单拒绝或不存在, 跳过: {raw[:120]}")
            continue
        try:
            outcome, _existing = _delivery_begin(ctx)
            if outcome == "sent":
                sent += 1
                continue
            if ctx is not None:
                _delivery_raise_if_busy(outcome, ctx)
        except DeliveryClaimBusy as exc:
            assert ctx is not None  # 无交付上下文时不会产生认领拒绝。
            logger.info(
                f"[OUTBOUND] 文件组件按台账取消上传 run={ctx.run_id} "
                f"component={ctx.component_id}: {exc}"
            )
            break
        try:
            if target.private:
                await bot.call_api("upload_private_file", user_id=int(target.id), file=str(path), name=path.name)
            else:
                await bot.call_api("upload_group_file", group_id=int(target.id), file=str(path), name=path.name)
            if ctx is not None:
                # 文件上传没有 QQ 消息 id;以组件事实作为回执。
                ctx.tracker.mark_component_sent(
                    run_id=ctx.run_id, component_id=ctx.component_id, msg_ids=[]
                )
            sent += 1
            logger.debug(f"[OUTBOUND] 文件已上传 target={target} name={path.name} size={path.stat().st_size}")
        except Exception as exc:
            _delivery_mark_unknown(ctx, exc)
            logger.warning(f"[OUTBOUND] 文件上传失败 {path.name}: {exc}")
    return sent


def _extract_msg_id(item: object) -> str | None:
    """从回执条目里抠出真正的消息 id。

    OneBot v11 的 `Receipt.msg_ids` 装的是 call_api 的**原始返回 dict**
    (`{"message_id": 12345, "message_id_v2": ...}`),不是裸 id——直接 str() 会得到
    一串字典字面量,拿去做消息留痕/引用全是废号。这里按平台常见键取 id,裸标量
    (其它 adapter 的形态)原样透传。
    """

    if item is None:
        return None
    if isinstance(item, dict):
        for key in ("message_id", "msg_id", "messageId", "id"):
            value = item.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None
    value = getattr(item, "message_id", None)
    if value is not None:
        text = str(value).strip()
        if text:
            return text
    text = str(item).strip()
    return text or None


def _receipt_msg_ids(receipt: object) -> list[str]:
    """把 alconna Receipt.msg_ids 归一化成 list[str](真 id、剔除空值)。"""

    raw = getattr(receipt, "msg_ids", None)
    if not isinstance(raw, (list, tuple)):
        return []
    ids: list[str] = []
    for item in raw:
        msg_id = _extract_msg_id(item)
        if msg_id:
            ids.append(msg_id)
    return ids


async def _send_unimsg(
    msg: alconna.UniMessage, *, bot: Bot, target: alconna.Target,
    delivery: DeliveryContext | None = None,
) -> list[str]:
    """唯一出站出口:发送 UniMessage,返回平台回执里的消息 id 列表。

    普通文本、媒体、长文截断、合并转发、转发后的 @ 提醒全部从这里出去——调用方拿到
    统一形态的 id(供消息留痕/撤回/后续引用使用),不必各自摸 receipt。平台没回
    msg_ids 时留 WARN 再返回空列表:「发出去了但拿不到 id」必须可见,不靠猜。

    携带 delivery(alive/108)时:发送前先原子认领组件;已 sent 直接返回既有
    回执,绝不再调平台;平台异常记 unknown 后沿原异常面上抛;回执写库失败
    向外传播,调用方不得把本轮当成已完成(DeliveryUnknown 语义)。
    """
    outcome, existing = _delivery_begin(delivery)
    if outcome == "sent":
        return existing
    if delivery is not None:
        _delivery_raise_if_busy(outcome, delivery)
    try:
        receipt = await msg.send(target=target, bot=bot)
    except Exception as exc:
        _delivery_mark_unknown(delivery, exc)
        raise
    msg_ids = _receipt_msg_ids(receipt)
    if delivery is not None:
        delivery.tracker.mark_component_sent(
            run_id=delivery.run_id, component_id=delivery.component_id, msg_ids=msg_ids
        )
    if not msg_ids:
        logger.warning(f"[OUTBOUND] 发送成功但回执无 msg_ids(消息 id 无法留痕) target={target}")
    return msg_ids


async def send_text_with_media(
    *,
    bot: Bot,
    target: alconna.Target,
    text: str,
    media_urls: Sequence[str] = (),
    reply_qq_message_id: str | int | None = None,
    reply_at_user_id: str | int | None = None,
    adapter_name: str | None = None,
    sticker_library: StickerLibrary | None = None,
    delivery: DeliveryContext | None = None,
) -> SendResult:
    """组装并发送一条消息,分别返回发送状态与平台消息 ID。

    - target 是 alconna Target(只在内存可用)
    - text 里允许受控内联 @ 标记 `[CQ:at,qq=<数字>]`(可多次、任意位置),逐个转成
      alconna At 段并与前后文字保持原序;非 at/非数字形态原样当普通文本。合法标记
      绝不会字面发出去。
    - reply_qq_message_id: 合法 OneBot int32 时在消息最前面构造 Reply(id=...) 引用;
      None/空串/空白/非数字不引用(非数字留 WARN 后忽略,避免导出期崩掉整条回复)
    - SendResult.ok 表示是否发出;msg_ids 是真实回执 ID,可能为空。空消息与发送异常
      都返回 ok=False;已发出但无回执 ID 返回 ok=True,msg_ids=[]
    - 空消息(text='' 且无 @ 段、无有效媒体——http/https URL 或可解码的 data:image)
      直接返回失败,防 push 路径在 Hermes 回空时构造空 UniMessage 发送出去
    """
    sticker_match = _STICKER_MARKER_RE.fullmatch(text.strip())
    if sticker_match is not None and sticker_feature_enabled():
        sticker_id = sticker_match.group(1)
        if adapter_name != "onebotv11":
            logger.warning(f"[OUTBOUND] 本地表情仅支持 onebotv11 id={sticker_id} adapter={adapter_name}")
            return SendResult(ok=False, msg_ids=[])
        library = sticker_library or default_sticker_library()
        try:
            raw, mimetype = library.read(sticker_id)
        except (StickerLibraryError, OSError) as exc:
            logger.warning(f"[OUTBOUND] 本地表情发送失败 id={sticker_id}: {exc}")
            return SendResult(ok=False, msg_ids=[])
        # Alconna Image.sticker 在 OneBot v11 导出的是 subType=1；NapCat
        # 实测契约是 sub_type=1。这里在统一发送器内走一小段显式 OneBot 载荷，
        # 避免客户端把它当普通图片。其余文本／媒体仍共用 UniMessage 出口。
        image = {
            "type": "image",
            "data": {
                "file": "base64://" + base64.b64encode(raw).decode("ascii"),
                "sub_type": 1,
            },
        }
        try:
            outcome, existing = _delivery_begin(delivery)
            if outcome == "sent":
                return SendResult(ok=True, msg_ids=existing, sticker_id=sticker_id)
            if delivery is not None:
                _delivery_raise_if_busy(outcome, delivery)
        except DeliveryClaimBusy as exc:
            assert delivery is not None  # 无交付上下文时不会产生认领拒绝。
            logger.info(
                f"[OUTBOUND] 表情组件按台账取消出站 run={delivery.run_id} "
                f"component={delivery.component_id}: {exc}"
            )
            return SendResult(ok=False, msg_ids=[])
        try:
            if target.private:
                response = await bot.call_api(
                    "send_private_msg", user_id=int(target.id), message=[image]
                )
            else:
                response = await bot.call_api(
                    "send_group_msg", group_id=int(target.id), message=[image]
                )
        except Exception as exc:
            _delivery_mark_unknown(delivery, exc)
            logger.warning(f"[OUTBOUND] 本地表情平台发送失败 id={sticker_id}: {exc}")
            return SendResult(ok=False, msg_ids=[])
        msg_id = _extract_msg_id(response)
        msg_ids = [msg_id] if msg_id else []
        if delivery is not None:
            delivery.tracker.mark_component_sent(
                run_id=delivery.run_id, component_id=delivery.component_id, msg_ids=msg_ids
            )
        if not msg_ids:
            logger.warning(f"[OUTBOUND] 本地表情发送成功但无回执 id={sticker_id}")
        logger.info(f"[OUTBOUND] 本地表情已发送 id={sticker_id} msg_ids={len(msg_ids)}")
        return SendResult(ok=True, msg_ids=msg_ids, sticker_id=sticker_id)

    original_len = len(text)
    max_len = load_misc_config().max_length

    tokens = _parse_inline_at_markers(text)
    _warn_residual_cq(tokens)
    reply_id = _normalize_reply_id(reply_qq_message_id, target=target)
    reply_at = ""
    if reply_id and not target.private and reply_at_user_id is not None:
        candidate = str(reply_at_user_id).strip()
        if re.fullmatch(r"[1-9][0-9]*", candidate):
            reply_at = candidate
        else:
            logger.warning(
                f"[OUTBOUND] 引用目标发送者 ID 非法，仅保留 Reply target={target} "
                f"value={str(reply_at_user_id)[:80]!r}"
            )

    # B-1.2: 群聊 + OneBot v11 + 超长 → 合并转发 (而非截断)
    # 私聊永远走截断 (send_private_forward_msg 实现端覆盖差);
    # 非 onebotv11 同样保持截断。
    if (
        text
        and not target.private
        and adapter_name == "onebotv11"
        and load_chat_config().long_reply_forward
        and original_len > max_len
    ):
        try:
            nickname = await get_bot_nickname(bot)
            nodes = _pack_forward_nodes(
                tokens,
                bot_self_id=bot.self_id,
                bot_nickname=nickname,
            )
            # 媒体挂到最后一个节点,跟着同一批转发出去。
            forward_media_count = 0
            if media_urls:
                last_node_content = nodes[-1].content
                for u in media_urls:
                    if u.startswith(("http://", "https://")):
                        last_node_content.append(alconna.Image(url=u))
                        forward_media_count += 1
                    elif u.startswith("data:"):
                        parsed = _parse_image_data_url(u)
                        if parsed is None:
                            logger.warning(f"[OUTBOUND] unsupported data: URL skipped (len={len(u)})")
                            continue
                        last_node_content.append(
                            alconna.Image(raw=parsed[0], mimetype=parsed[1])
                        )
                        forward_media_count += 1
                    else:
                        logger.warning(
                            f"[OUTBOUND] 无法投递的媒体引用,已跳过(需要 http(s) 或 data:): {u[:120]}"
                        )
            if reply_id:
                # 合并转发 API 没有包外 Reply 槽位。alconna 会把 Reply 导出进首节点
                # content;客户端是否展示节点内引用仍需 QQ 实测,日志不宣称必然生效。
                nodes[0].content.insert(0, alconna.Reply(reply_id))
                if reply_at:
                    nodes[0].content[1:1] = [alconna.At("user", reply_at), alconna.Text(" ")]
                logger.warning(
                    f"[OUTBOUND] 合并转发无外层引用槽位,Reply 已放入首节点首段;"
                    f"客户端展示待实测 target={target} reply_id={reply_id}"
                )
            msg_ids = await _send_unimsg(
                alconna.UniMessage([alconna.Reference()(*nodes)]), bot=bot, target=target,
                # 无交付上下文时保持旧调用形状(兼容既有 monkeypatch/包装)。
                **({"delivery": delivery} if delivery is not None else {}),
            )
            logger.debug(
                f"[OUTBOUND] sent-via-forward target={target} text_len={original_len} "
                f"nodes={len(nodes)} media={forward_media_count} ats="
                f"{sum(1 for t in tokens if isinstance(t, _InlineAt))} msg_ids={len(msg_ids)}"
            )
            return SendResult(ok=True, msg_ids=msg_ids)
        except DeliveryClaimBusy as exc:
            assert delivery is not None  # 长文分支只在携带交付上下文时产生此异常。
            logger.info(
                f"[OUTBOUND] 长文组件按台账取消出站 run={delivery.run_id} "
                f"component={delivery.component_id}: {exc}"
            )
            return SendResult(ok=False, msg_ids=[])
        except Exception as exc:
            logger.warning(f"[OUTBOUND] long-reply forward failed, falling back to truncation: {exc}")
            # Fall through to existing truncation path

    msg = alconna.UniMessage()

    if reply_id:
        msg += alconna.UniMessage([alconna.Reply(reply_id)])
        if reply_at:
            msg += alconna.UniMessage([alconna.At("user", reply_at), alconna.Text(" ")])

    truncated = False
    if text and original_len > max_len:
        tokens, _ = _truncate_tokens(tokens, max_len)
        tokens.append(_TRUNCATION_SUFFIX)
        truncated = True
    if tokens:
        msg += alconna.UniMessage(_tokens_to_segments(tokens))

    sent_media_count = 0
    for u in media_urls:
        if u.startswith(("http://", "https://")):
            msg += alconna.UniMessage(alconna.Image(url=u))
            sent_media_count += 1
        elif u.startswith("data:"):
            parsed = _parse_image_data_url(u)
            if parsed is None:
                # 不打 URL 本体——base64 可能几 MB,日志只记长度。
                logger.warning(f"[OUTBOUND] unsupported data: URL skipped (len={len(u)})")
                continue
            raw, mimetype, _ = parsed
            msg += alconna.UniMessage(alconna.Image(raw=raw, mimetype=mimetype))
            sent_media_count += 1
        else:
            # 其余 scheme(主机本地路径 / file:// / 相对路径)bot 侧取不到字节。
            # 以前这里没有 else,直接静默丢:调用方(尤其 MCP push_message)拿到
            # "发送成功",用户却没看到图,连日志都查不到。
            logger.warning(f"[OUTBOUND] 无法投递的媒体引用,已跳过(需要 http(s) 或 data:): {u[:120]}")

    # 空消息守卫:正文(含内联 @)与媒体都为空才跳过;Reply 本身不算内容。
    if not tokens and sent_media_count == 0:
        logger.warning(
            f"[OUTBOUND] empty message skipped target={target} "
            f"(text_len={original_len} input_media={len(media_urls)} "
            f"valid_media={sent_media_count})"
        )
        return SendResult(ok=False, msg_ids=[])

    try:
        msg_ids = await _send_unimsg(
            msg, bot=bot, target=target,
            **({"delivery": delivery} if delivery is not None else {}),
        )
        logger.debug(
            f"[OUTBOUND] sent target={target} text_len={original_len} "
            f"truncated={truncated} media_sent={sent_media_count}/{len(media_urls)} "
            f"ats={sum(1 for t in tokens if isinstance(t, _InlineAt))} "
            f"reply={reply_id or '-'} msg_ids={len(msg_ids)}"
        )
        return SendResult(ok=True, msg_ids=msg_ids)
    except DeliveryClaimBusy as exc:
        # 旧代、他人持有租约或 unknown 都是预期的“不得出站”，不是平台故障。
        run_id = delivery.run_id if delivery is not None else "-"
        component_id = delivery.component_id if delivery is not None else "-"
        logger.info(
            f"[OUTBOUND] 交付组件按台账取消出站 run={run_id} "
            f"component={component_id}: {exc}"
        )
        return SendResult(ok=False, msg_ids=[])
    except Exception as exc:
        # Target 的字符串化自身可能依赖平台字段；错误路径不能再被二次异常吞掉。
        logger.error(
            f"[OUTBOUND] 发送失败 target_type={type(target).__name__}: {exc}",
            exc_info=True,
        )
        return SendResult(ok=False, msg_ids=[])
