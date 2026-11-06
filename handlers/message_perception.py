"""
感知入库域:priority=1 perception matcher、消息入库 MessageBuffer + BotRegistry、
event→db id 挂号/领号传递桥、防抖碎片载荷合并。
"""
from __future__ import annotations

import time

import nonebot_plugin_alconna as alconna
from nonebot import logger, on_message
from nonebot.adapters import Bot, Event

from .. import mcp as _mcp
from ..core.group_profile import sync_buddy_to_group
from ..core.message_buffer import BufferedMessage
from ..core.message_origin import MessageOrigin
from ..core.misc_config import load_misc_config
from ..utils import get_adapter_name

from .message_ingress import _extract_sender_nickname, _now_ms
from .message_media import (
    _collect_at_placeholders,
    _collect_file_placeholders,
    _collect_nontext_placeholders,
    _compose_quote_prefix,
    _extract_file_entries,
    _extract_forward_full,
    _extract_image_urls,
    _extract_replied_media,
    _extract_sticker_urls,
    _forward_full_for_store,
    _summarize_forward,
)


perception_message = on_message(priority=1, block=False)


# perception → 主 handler 的 db id 传递桥(定案,B 方案)。
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


def _peek_event_db_id(adapter_name: str, event_msg_id) -> int | None:
    """读取本事件的真实 db id 但不领走；占位渲染与后续条目构造可共用。"""
    if event_msg_id is None:
        return None
    hit = _event_db_id_cache.get((adapter_name, str(event_msg_id)))
    if hit is None:
        return None
    db_id, exp = hit
    return db_id if exp > time.monotonic() else None


def _claim_event_db_id(adapter_name: str, event_msg_id) -> int | None:
    """主 handler 凭本事件的 message_id 领号;领不到(合成消息/缓存 miss)返 None。"""
    if event_msg_id is None:
        return None
    hit = _event_db_id_cache.pop((adapter_name, str(event_msg_id)), None)
    if hit is None:
        return None
    db_id, exp = hit
    return db_id if exp > time.monotonic() else None


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
    # 永远无法召回字节。是否当轮喂模型仍由 media_config.sticker_vision_enabled 门控,
    # 入库与内联是两件独立的事。
    sticker_urls = await _extract_sticker_urls(uni_msg, bot, adapter_name)
    # 引用图也入库(kind='quote',规划书 §8):提取路径与触发 handler 同源。
    # 此前 perception 不取引用——引用图只进触发轮临时载荷、不落库,历史行/召回拿不到
    # (事故根因之一:图文混合的引用只呈递文字)。
    replied = await _extract_replied_media(event, bot, adapter_name=adapter_name)
    quote_image_urls = list(replied.image_urls)
    nickname = _extract_sender_nickname(event, adapter_name) or user_id

    # 文本太长截断(入库与渲染共用 history_text_max_chars,取消「入库 200、渲染 800」双层口径)
    max_len = load_misc_config().history_text_max_chars
    if msg_text and len(msg_text) > max_len:
        msg_text = msg_text[:max_len] + "..."

    if image_urls:
        placeholder = " [图片]"
        msg_text = (msg_text + placeholder) if msg_text else placeholder.strip()

    file_entries = _extract_file_entries(uni_msg)
    # 平台真实 message_id:与 m:id 同一行落库(身份映射),供主线回执/取文件回平台侧。
    # 无 ID 的 event(合成/本地构造)保持 None——NULL 是明确事实,不猜。
    event_msg_id = getattr(event, "message_id", None)
    qq_message_id = str(event_msg_id).strip() if event_msg_id not in (None, "") else None
    for entry in file_entries:
        entry["qq_message_id"] = qq_message_id

    # B-0: 非文本段占位；文件标签等入库拿到真实 db id 后单独补写。
    nontext_placeholders = _collect_nontext_placeholders(uni_msg, include_files=False)
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

    if not msg_text and not image_urls and not quote_image_urls and not forward_image_urls and not file_entries:
        return

    # 引用前缀与主 handler 同源调
    # _compose_quote_prefix——历史行/换窗口后「谁引用了谁、引文是什么」不再只活在当轮。
    # 拼在文本截断与空内容闸门之后:前缀绝不被 history_text_max_chars 截掉,
    # 也不改变「本条有无自身内容」的入库判定(纯引用空体消息维持不落库)。
    quote_prefix = _compose_quote_prefix(replied, len(image_urls) + len(sticker_urls))
    if quote_prefix:
        msg_text = f"{quote_prefix} {msg_text}".strip()

    now = _now_ms()
    group_id = None if target.private else target.id

    # 写 MessageBuffer(perception 固定开启,恒入库)
    buffered = BufferedMessage(
        ts=now,
        adapter=adapter_name,
        group_id=group_id,
        user_id=user_id,
        nickname=nickname,
        content=msg_text,
        origin=MessageOrigin.human(),
        image_urls=image_urls,
        sticker_urls=sticker_urls,
        file_entries=file_entries,
        quote_image_urls=quote_image_urls,
        forward_image_urls=forward_image_urls,
        is_bot=False,
        forward_content=_forward_full_for_store(forward_full),
        qq_message_id=qq_message_id,
    )
    _mcp.message_buffer.append(buffered)
    if file_entries and buffered.id is not None:
        labels = _collect_file_placeholders(file_entries, buffered.id)
        suffix = " ".join(labels)
        buffered.content = f"{buffered.content} {suffix}".strip() if buffered.content else suffix
        _mcp.message_buffer.update_content(buffered.id, buffered.content)
    # B 方案:入库即挂号,主 handler 凭同一 event 的 message_id 领真实 db id
    _register_event_db_id(adapter_name, getattr(event, "message_id", None), buffered.id)

    # 本部署补丁:发言者已有画像但不在群友行时，自动补进群友行
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


def _merge_fragment_payload(old: dict, new: dict) -> None:
    """同一个人的防抖碎片并入在窗载荷(替换改拼接——「如果是替换还要防抖干嘛」)。
    文本换行拼接;图片/表情包 URL 追加去重;
    explicit/addressed 取并集(任一碎片被点名,合并触发即被点名);
    ephemeral 两者皆真才真;now_ms 取最新(触发时序以最后一条为准);
    event 保留首条——dispatch 不再用。
    有序 fragments 序列是唯一身份轨:每条碎片的完整 BufferedMessage 按序追加、
    不替换不取末条——steer/current 逐行呈递、媒体寻址与钉锚来源都从这份序列
    派生;旧的平行碎片 ID 轨已删(P-08),不再维护第二套碎片身份。
    """

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
    # 拆批 fragments 序列:每条碎片的完整 BufferedMessage 按序暂存,current/steer
    # 渲染拆回真实消息序列逐行贴真号,不查库。
    incoming_frags = list(new.get("fragments") or [])
    if incoming_frags:
        old_frags = list(old.get("fragments") or [])
        old_frags.extend(incoming_frags)
        old["fragments"] = old_frags
    old["is_direct"] = old["is_direct"] or new["is_direct"]
    old["addressed_to_bot"] = old["addressed_to_bot"] or new["addressed_to_bot"]
    old["ephemeral"] = old["ephemeral"] and new["ephemeral"]
    if new.get("context_limit") is not None:
        old["context_limit"] = new["context_limit"]
    # 引用 id(任务锚归并用):任一碎片带引用即保留首个——碎片同源一条引用链,
    # 首个非空即可代表;跨人碎片不走本函数(整体替换,随载荷自带)。
    if old.get("replied_msg_id") is None and new.get("replied_msg_id") is not None:
        old["replied_msg_id"] = new["replied_msg_id"]
    old["now_ms"] = new["now_ms"]
