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
WARNING），不含流式状态机。:func:`take_closed_units` 是同一协议的增量形式，供
``RunsEventParser`` 在 delta 到达时按闭合拆单位；:func:`rebase_after_interim` 只在段
边界决定未闭合碎片的去留——两者都不是第二套协议。段内去重由 ``RunsEventParser``
按「完整单位全文匹配」维护，不在这里加第二套协议。
"""

from __future__ import annotations

SEPARATOR = "§"
MAX_BODY_CHARS = 4000
# 发送单元的轻量分类（主人 2026-11-11 定案）：当前只有正文；未来工具显示可以加
# ``kind="tool"`` 复用同一条 ``_stream_pending``／发送队列，不另起通道。
TEXT_UNIT_KIND = "text"

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


def take_closed_units(
    text: str, *, max_body_chars: int = MAX_BODY_CHARS
) -> tuple[list[str], str]:
    """流式增量：取出已经闭合的正文单位，返回 ``(units, remainder)``。

    与 :func:`split_text_units` 同一条协议，只做增量：``units`` 是这次新闭合的正文，
    ``remainder`` 从最后一个**已经被换行终结**的整行 ``§`` 开始 —— 那个标记既是上一份
    正文的结尾，也是下一份正文的开头。相邻正文共享的 ``§`` 于是只在拆分这一步处理完，
    调用方既不需要对原串做前缀裁剪，也不需要给它补换行。

    帧尾那个还没有换行终结的 ``§`` 不算标记：它可能是下一帧 ``§B`` 的行首字符。
    这类尾巴原样留在 ``remainder`` 里，等下一帧、interim 或封段时再看。
    """
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw:
        return [], ""
    lines = raw.split("\n")
    # 最后一行没有换行终结时不能当整行标记（可能是 §B 的前半截）。
    ended = len(lines) if raw.endswith("\n") else len(lines) - 1
    positions = [index for index in range(ended) if lines[index] == SEPARATOR]
    if not positions:
        return [], raw
    head = "\n".join(lines[: positions[0]]).strip()
    if head:
        _warn_dropped(_MISSING_OPEN, head)
    units: list[str] = []
    for start, end in zip(positions, positions[1:]):
        body = "\n".join(lines[start + 1 : end]).strip()
        if not body:
            continue  # 空段（相邻标记或只有空白）不是正文，也不算违规。
        if _unit_length(body) > max_body_chars:
            raise TextUnitsError("body_overflow")
        units.append(body)
    return units, "\n".join(lines[positions[-1] :])


def _marker_tail(text: str) -> str:
    """从最后一个独立整行 ``§`` 起（含该标记）的原文；没有整行标记时原样返回。"""
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = raw.split("\n")
    positions = [index for index, line in enumerate(lines) if line == SEPARATOR]
    return "\n".join(lines[positions[-1]:]) if positions else raw


def _terminate_marker_line(text: str) -> str:
    """保留的共享标记补齐行尾换行，免得下一帧和它拼成同一行的 ``§§``。

    只在最后一行已经是**独立整行 ``§``** 时补；``§B``／``§ `` 这类行内／带空格的
    行原样返回——绝不把普通正文修成标记（reviewer t_049df793 第二轮的边界）。
    """
    if not text or text.endswith(("\n", "\r")):
        return text
    return text + "\n" if text.split("\n")[-1] == SEPARATOR else text


def rebase_after_interim(interim: str, fragment: str) -> str:
    """interim（本段到这次 tool 调用为止的全文）之后，delta 碎片还剩什么。

    完整正文单位在 delta 到达时就已经按闭合拆出去入队了，这里只决定**未闭合碎片**
    的去留，判据全部来自原输入的行结构：

    - interim 文本不是碎片的开头（分叉）时，碎片原样留到封段，缺开头照旧被丢弃；
    - interim 覆盖到碎片里时，把覆盖过的那截去掉。它末尾那个 ``§`` 只有在原输入里
      右邻就是行尾才算两份正文共享的标记，才留给尾巴当开头；右邻是正文（``§B``／
      ``§ ``）时原输入里就没有这个标记，绝不靠新补的换行把它修成合法开头。
    - 反过来，碎片本身是 interim 的前缀时（delta 截在半份正文里），碎片内容已经被
      interim 的正文单位完全覆盖：换成 interim 自己的结构剩余（最后一个整行 ``§``
      起），免得半份碎片和下一帧拼成一份原文里并不存在的完整正文。
    """
    if not fragment or not interim:
        return fragment
    raw = interim.replace("\r\n", "\n").replace("\r", "\n")
    text = raw.rstrip("\n")
    if not text:
        return fragment
    if not fragment.startswith(text):
        if text.startswith(fragment.rstrip("\n")):
            return _terminate_marker_line(_marker_tail(raw))
        return fragment
    end = len(text)
    if text[-1] == SEPARATOR and (end == len(fragment) or fragment[end] in ("\n", "\r")):
        remainder = fragment[end - 1 :]
        # interim 覆盖了整个碎片时，这个共享标记是 interim 自己的完整行：补上它缺的
        # 行尾换行，下一帧不管是新信封还是同段续写都不会拼成同一行的「§§」。碎片比
        # interim 长时仍按原输入逐字切片，一个字符都不补。
        return _terminate_marker_line(remainder) if end == len(fragment) else remainder
    return fragment[end:]


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
