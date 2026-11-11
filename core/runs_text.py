"""分节符正文信封：独立整行 `§` 是唯一标记，只有完整闭合的段才可发送。

协议（主人 2026-11-11 定案）：

- 每份回复以独立一行 `§` 开头，以独立一行 `§` 结尾；多条正文之间也用独立一行
  `§` 分隔。标记是**分隔符**：相邻两个标记之间的内容就是一份正文。
- 单条 ``§\\n正文\\n§``；两条 ``§\\nA\\n§\\nB\\n§``。
- 静默：整段只含 `§` 与空白字符（``§``／``§§``／``§\\n§``／``§ §``／多行）——
  去掉 `§` 与空白后没有任何内容就是静默，不发送、也不报错。
- 只有完整闭合的段可发送：第一个标记之前的正文（缺开头）与最后一个标记之后的
  正文（缺末尾）都不是正文，不产生单位，只留一条可见 WARNING。
- 行内 `§`（``第 3 节 §3``）与带空格的 `§`（`` §``／``§ ``／`` § ``）都是普通
  正文，不是标记；代码围栏没有豁免——围栏内独立整行 `§` 同样分隔。
- 旧裸正文不保留兼容层：整段没有任何独立整行 `§` 时不产生单位（``no_marker``）。
- 单个单位超 ``MAX_BODY_CHARS`` 一律 fail-closed（``TextUnitsError``），绝不发半截。

本模块是正常链／流式链／恢复链／对账链共用的唯一解析器：纯函数（除不可发送段的
WARNING），不含流式状态机。段内去重由 ``RunsEventParser`` 按「完整单位全文匹配」
维护，不在这里加第二套协议。
"""

from __future__ import annotations

SEPARATOR = "§"
MAX_BODY_CHARS = 4000

# 不可发送段的原因：只用于日志，永远不参与发送决策。
_NO_MARKER = "no_marker"          # 整段没有任何独立整行 §（旧裸正文／缺开头也缺末尾）
_MISSING_OPEN = "missing_open"    # 首个 § 之前还有正文
_MISSING_CLOSE = "missing_close"  # 末个 § 之后还有正文（没等到闭合标记）


class TextUnitsError(ValueError):
    """正文单位不合法：调用方必须 fail-closed，发送前整体拒绝。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _unit_length(text: str) -> int:
    """计量长度：内联 data URL 先摘掉，避免把整段 base64 当正文长度误判超限。"""
    from .media_payload import _detach_data_urls

    shrunk, _detached = _detach_data_urls(text)
    return len(shrunk)


def _warn_dropped(reason: str, text: str) -> None:
    """不可发送段留一条可见日志（截断预览，绝不把整段正文写进日志）。"""
    from nonebot import logger

    flat = " ".join(text.split())
    preview = flat if len(flat) <= 80 else flat[:80] + "…"
    logger.warning(
        f"[HERMES] 正文信封无效 reason={reason} chars={len(text)} preview={preview!r}"
    )


def split_text_units(text: str, *, max_body_chars: int = MAX_BODY_CHARS) -> list[str]:
    """整段正文 → 有序发送单位；静默／全不可发送返回 ``[]``，超长抛 ``TextUnitsError``。"""
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return []
    # 静默优先：只含 § 与空白就是「什么都不说」，不做任何分段／报错分支。
    if not raw.replace(SEPARATOR, "").strip():
        return []
    lines = raw.split("\n")
    # 严格整行匹配：前后带空格（` §`、`§ `、` § `）都是普通正文，不切分。
    # 不设第二套规则：代码围栏内的整行 § 同样分隔，没有转义/豁免。
    positions = [index for index, line in enumerate(lines) if line == SEPARATOR]
    if not positions:
        _warn_dropped(_NO_MARKER, raw)
        return []

    head = "\n".join(lines[:positions[0]]).strip()
    if head:
        _warn_dropped(_MISSING_OPEN, head)
    units: list[str] = []
    for start, end in zip(positions, positions[1:]):
        body = "\n".join(lines[start + 1:end]).strip()
        if not body:
            continue  # 空段（相邻标记或只有空白）不是正文，也不算违规。
        if _unit_length(body) > max_body_chars:
            raise TextUnitsError("body_overflow")
        units.append(body)
    tail = "\n".join(lines[positions[-1] + 1:]).strip()
    if tail:
        _warn_dropped(_MISSING_CLOSE, tail)
    return units


def units_from_text(text: str, *, max_body_chars: int = MAX_BODY_CHARS) -> list[dict]:
    """整段正文 → 有序 unit（body／媒体／文件／空 reply_to），静默返回 ``[]``。"""
    from .runs_terminal import _text_unit

    units: list[dict] = []
    for unit_text in split_text_units(text, max_body_chars=max_body_chars):
        unit = _text_unit(unit_text)
        if unit is not None:
            units.append(unit)
    return units


def decisions_from_text(text: str, *, max_body_chars: int = MAX_BODY_CHARS) -> list[dict]:
    """整段正文 → 有序决策（正文保持原文，媒体清洗留给共享计划器）。"""
    return [
        {"body": unit, "should_reply": True}
        for unit in split_text_units(text, max_body_chars=max_body_chars)
    ]
