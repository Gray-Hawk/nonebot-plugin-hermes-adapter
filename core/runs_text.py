"""纯文本正文协议：正文就是用户看到的消息，独立整行 `§` 只作连发分隔。

规则（对应设计 t_306c4938 v5）：

- 只有「整行内容严格等于 `§`、且不在代码围栏内」才是单位边界；` §`、`§ `、` § `
  都只是普通正文（不做 strip 后的宽松识别）；不按行、不按空行切；
- 首／尾／连续 `§` 不产生空消息，空单位直接丢弃；
- 单个单位超 `MAX_BODY_CHARS` 一律 fail-closed（``TextUnitsError``），绝不发半截；
- 解析是纯函数：同一段文本永远得到同一串单位，正常链／恢复链／对账链共用这一份。

本模块不含流式状态机、旧格式识别或兼容分支——段内去重由 ``RunsEventParser``
按「完整发送单元全文匹配」维护，不在这里加第二套协议。
"""

from __future__ import annotations

import re

SEPARATOR = "§"
MAX_BODY_CHARS = 4000

# 代码围栏：行首（允许缩进）连续三个以上反引号或波浪号。
_FENCE = re.compile(r"^\s*(`{3,}|~{3,})")


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


def split_text_units(text: str, *, max_body_chars: int = MAX_BODY_CHARS) -> list[str]:
    """整段正文 → 有序发送单位；空文本返回 ``[]``（静默），超长抛 ``TextUnitsError``。"""
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return []
    units: list[str] = []
    current: list[str] = []
    fence: str | None = None
    for line in raw.split("\n"):
        fence_match = _FENCE.match(line)
        if fence_match:
            marker = fence_match.group(1)[0]
            fence = None if fence == marker else (fence or marker)
            current.append(line)
            continue
        # 严格整行匹配：前后带空格（` §`、`§ `、` § `）都是普通正文，不切分。
        if fence is None and line == SEPARATOR:
            units.append("\n".join(current))
            current = []
            continue
        current.append(line)
    units.append("\n".join(current))
    cleaned = [unit.strip() for unit in units]
    kept = [unit for unit in cleaned if unit]
    for unit in kept:
        if _unit_length(unit) > max_body_chars:
            raise TextUnitsError("body_overflow")
    return kept


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
