"""
媒体/转发域:图片/表情包 URL 解析、非文本段与 @ 占位收集、
合并转发块提取(短 TTL 缓存)、被引消息媒体提取与引用前缀拼装、
内联图寻址贴号、缺席媒体标注、Telegram file_id 解析。
"""
from __future__ import annotations

import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field

import nonebot_plugin_alconna as alconna
from nonebot import logger
from nonebot.adapters import Bot, Event

from ..config import plugin_config
from ..core.media_config import MediaConfig, load_media_config
from ..core.message_buffer import BufferedMessage, media_kind_word

from .message_ingress import (
    _extract_replied_sender_label,
    _format_quote_prefix,
    _has_at_bot_in_original,
    _rewrite_quoted_ats,
    _rewrite_quoted_literal_ats,
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


# 本部署补丁(图片投送管线三模式): 取代早期
# addressed 单点门控。
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
