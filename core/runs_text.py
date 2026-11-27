"""分节符正文信封：独立整行 `§` 是唯一标记，只有完整闭合的信封才可发送。

协议（取消共享分界）：

- 每份正文自己打开、自己闭合：独立一行 `§` 开头，独立一行 `§` 结尾。
- 单条 ``§\\n正文\\n§``；两条 ``§\\nA\\n§\\n§\\nB\\n§``——每份都有独立开头/闭合，
  闭合标记随它自己那份正文消费，绝不兼任下一份的开头。
- 旧共享分界 ``§\\nA\\n§\\nB\\n§`` 不再解析成两份：A 的闭合被 A 消费掉，B 缺自己的
  开头，按缺开头丢弃并留 WARNING。不留兼容分支。
- 静默：整段只含 `§` 与空白字符（``§``／``§§``／``§\\n§``／``§ §``／多行）——
  去掉 `§` 与空白后没有任何内容就是静默，不发送、也不报错。
- 只有完整闭合的信封可发送：第一个标记之前的正文（缺开头）、信封闭合之后又出现的
  游离正文、以及最后一个未配对标记之后的正文（缺末尾）都不是正文，不产生单位，
  只留一条可见 WARNING。
- 行内 `§`（``第 3 节 §3``）与带空格的 `§`（`` §``／``§ ``／`` § ``）都是普通
  正文，不是标记；代码围栏没有豁免——围栏内独立整行 `§` 同样闭合当前正文。
- 旧裸正文不保留兼容层：整段没有任何独立整行 `§` 时不产生单位（``no_marker``）。
- 单个单位超 ``MAX_BODY_CHARS`` 一律 fail-closed（``TextUnitsError``），绝不发半截。

本模块是正常链／流式链／恢复链／对账链共用的唯一解析器：纯函数（除不可发送段的
WARNING），不含流式状态机。:func:`take_closed_units` 是同一协议的增量形式，供
``RunsEventParser`` 在 delta 到达时按闭合拆单位；:func:`unsealed_tail` 与
:func:`pending_after_interim` 只回答「interim 之后还剩什么未消费的字节」——同一套
行结构判据，不是第二套协议。段内去重由 ``RunsEventParser`` 按「完整单位全文匹配」
维护，不在这里加第二套协议。
"""

from __future__ import annotations

SEPARATOR = "§"
MAX_BODY_CHARS = 4000
# 发送单元的轻量分类（主人 2026-11-11 定案）：当前只有正文；未来工具显示可以加
# ``kind="tool"`` 复用同一条 ``_stream_pending``／发送队列，不另起通道。
TEXT_UNIT_KIND = "text"

# 不可发送段的原因：只用于日志，永远不参与发送决策。
_NO_MARKER = "no_marker"          # 整段没有任何独立整行 §（旧裸正文／缺开头也缺末尾）
_MISSING_OPEN = "missing_open"    # 出现在信封之外的正文（首个标记之前／两份之间）
_MISSING_CLOSE = "missing_close"  # 未配对的最后一个标记之后还有正文（没等到闭合）


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


def _normalize(text: str) -> str:
    """与 :func:`split_text_units` 同口径：CRLF／CR 都当 ``\\n``。"""
    return (text or "").replace("\r\n", "\n").replace("\r", "\n")


def _normalize_with_origin(text: str) -> tuple[str, list[int]]:
    """归一化并记录每个归一字符在原串中的下标（末尾附 ``len(text)``）。

    流式碎片必须保留**原输入**的行尾信息：帧尾一个孤立的 ``\\r`` 如果立刻被改写成
    ``\\n``，下一帧开头那个 ``\\n`` 就会拼出第二个换行（同一份 CRLF 正文跨帧变成
    ``A\\n\\nB``）。所以分析在归一文本上做，返回给调用方的切片仍取原串。
    """
    chars: list[str] = []
    origin: list[int] = []
    index = 0
    size = len(text)
    while index < size:
        char = text[index]
        if char == "\r":
            chars.append("\n")
            origin.append(index)
            index += 2 if index + 1 < size and text[index + 1] == "\n" else 1
        else:
            chars.append(char)
            origin.append(index)
            index += 1
    origin.append(size)
    return "".join(chars), origin


def _line_offsets(lines: list[str]) -> list[int]:
    """每行行首在归一文本中的字符下标。"""
    offsets = [0]
    for line in lines[:-1]:
        offsets.append(offsets[-1] + len(line) + 1)
    return offsets


def _envelope_pairs(positions: list[int]) -> list[tuple[int, int]]:
    """整行标记下标 → 独立信封对 (开, 闭)：偶数位打开、奇数位闭合。

    位置多于配对所需的那个尾巴是「还没等到闭合的开口」——它之后的正文一律算缺末尾。
    """
    return list(zip(positions[0::2], positions[1::2]))


def _marker_offsets(lines: list[str], positions: list[int]) -> list[int]:
    offsets = _line_offsets(lines)
    return [offsets[index] for index in positions]


def take_closed_units(
    text: str, *, max_body_chars: int = MAX_BODY_CHARS
) -> tuple[list[str], str]:
    """流式增量：取出已经闭合的正文单位，返回 ``(units, remainder)``。

    与 :func:`split_text_units` 同一条协议，只做增量：``units`` 是这次新闭合的正文，
    ``remainder`` 是**最后一个已消费闭合标记之后**的原样字节——闭合标记随它自己那份
    正文一起消费，下一份正文必须自带独立开头，所以这里不再留下任何共享标记。

    帧尾那个还没有换行终结的 ``§`` 不算标记：它可能是下一帧 ``§B`` 的行首字符。
    这类尾巴原样留在 ``remainder`` 里，等下一帧、interim 或封段时再看。

    ``remainder`` 是**原输入的原样切片**（不归一化）：帧尾的 ``\\r`` 原样留着，下一帧
    开头的 ``\\n`` 才能和它拼回同一个 CRLF，跨帧与整帧得到同一份正文。
    """
    source = text or ""
    raw, origin = _normalize_with_origin(source)
    if not raw:
        return [], ""
    lines = raw.split("\n")
    offsets = _line_offsets(lines)
    # 最后一行没有换行终结时不能当整行标记（可能是 §B 的前半截）。
    ended = len(lines) if raw.endswith("\n") else len(lines) - 1
    positions = [index for index in range(ended) if lines[index] == SEPARATOR]
    if not positions:
        return [], source
    head = "\n".join(lines[: positions[0]]).strip()
    if head:
        _warn_dropped(_MISSING_OPEN, head)
    units: list[str] = []
    consumed_close: int | None = None
    previous_close: int | None = None
    for start, end in _envelope_pairs(positions):
        if previous_close is not None:
            # 上一份闭合之后、这一份开头之前还夹着正文：它没有自己的开头，不发。
            stray = "\n".join(lines[previous_close + 1 : start]).strip()
            if stray:
                _warn_dropped(_MISSING_OPEN, stray)
        body = "\n".join(lines[start + 1 : end]).strip()
        if body:
            if _unit_length(body) > max_body_chars:
                raise TextUnitsError("body_overflow")
            units.append(body)
        consumed_close = previous_close = end
    if consumed_close is None:
        # 连一个闭合都没有：整段从第一个开口标记起原样留着，等下一帧。
        return units, source[origin[offsets[positions[0]]] :]
    start_line = consumed_close + 1
    if start_line >= len(lines):
        return units, ""
    return units, source[origin[offsets[start_line]] :]


def unsealed_tail(text: str) -> str:
    """``text`` 里最后一个已闭合信封之后仍未消费的原样字节（没有则空串）。

    与 :func:`take_closed_units` 同一套行结构判据：整行 ``§`` 两两配对（开、闭），
    闭合标记与它的正文一起消费掉。没有配对（含整段无标记）时，尾巴从第一个标记起算；
    整段没有标记时整段就是尾巴——正文还没开始，原样留着不会被当成正文。
    """
    source = text or ""
    if not source:
        return ""
    raw, origin = _normalize_with_origin(source)
    lines = raw.split("\n")
    offsets = _line_offsets(lines)
    positions = [index for index, line in enumerate(lines) if line == SEPARATOR]
    if not positions:
        return source
    consumed_close: int | None = None
    for _start, end in _envelope_pairs(positions):
        consumed_close = end
    if consumed_close is None:
        return source[origin[offsets[positions[0]]] :]
    start_line = consumed_close + 1
    if start_line >= len(lines):
        return ""
    return source[origin[offsets[start_line]] :]


def _last_closed_body(text_norm: str) -> str:
    """归一文本里最后一对信封之间的正文（没有闭合信封时为空串）。"""
    lines = text_norm.split("\n")
    positions = [index for index, line in enumerate(lines) if line == SEPARATOR]
    pairs = _envelope_pairs(positions)
    if not pairs:
        return ""
    start, end = pairs[-1]
    return "\n".join(lines[start + 1 : end]).strip()


def _recuts_a_closed_unit(fragment_norm: str, interim_norm: str) -> bool:
    """碎片是不是 interim **最后一份已闭合正文**的旧切帧（``§\\n<body>`` 开头且被覆盖）。

    闭合标记随正文消费以后，碎片若以独立开头起、正文又是 interim 已经交付过的那份
    正文的前缀，它只能是同一份正文更早的切帧——留着会和下一帧拼成原文里没有的新正文。
    """
    lines = fragment_norm.split("\n")
    if len(lines) < 2 or lines[0] != SEPARATOR:
        return False
    body = "\n".join(lines[1:]).strip()
    if not body:
        return False
    last = _last_closed_body(interim_norm)
    return bool(last) and last.startswith(body)


def _consume_confirmed_envelopes(interim_norm: str, frag: str) -> int:
    """碎片头部由 interim 末尾确认过的完整信封 → 已消费的字符数（归一坐标）。

    只在碎片是**完整信封**（独立整行 ``§`` 开、独立整行 ``§`` 闭）且它逐字等于
    interim 的结尾时才消费：interim 是这一段到这次 tool 调用为止的权威全文，它的
    末份信封（含闭合行）已经在 ``_emit_text`` 里交付过，这些字节再回到碎片区就会
    和下一帧粘成原文里没有的新正文（``§\\n乙\\n§`` + ``§\\n丙\\n§`` → ``乙\\n§§\\n丙``）。

    半个标记、半个正文（delta 更早的切帧）一律不动：下一帧还可能补全它。整条判据
    只看原输入的行结构，不补换行、不造标记。
    """
    pos = 0
    consumed = 0
    rest_interim = interim_norm
    while True:
        rest = frag[pos:]
        lead = len(rest) - len(rest.lstrip("\n"))
        rest = rest[lead:]
        pos += lead
        lines = rest.split("\n")
        if len(lines) < 3 or lines[0] != SEPARATOR:
            break
        close = next(
            (index for index in range(1, len(lines)) if lines[index] == SEPARATOR),
            None,
        )
        if close is None:
            break
        envelope = "\n".join(lines[: close + 1])
        if not rest_interim.endswith(envelope):
            break
        rest_interim = rest_interim[: len(rest_interim) - len(envelope)]
        pos += len(envelope)
        consumed += 1
    return pos if consumed else 0


def pending_after_interim(interim: str, fragment: str) -> str:
    """interim（本段到这次 tool 调用为止的全文快照）之后，delta 碎片还剩什么。

    完整信封在 delta 到达时就已经按闭合拆出去入队了，这里只决定**未消费碎片**的去留，
    判据全部来自原输入的行结构：

    - 碎片为空：什么都不留。delta 已经消费到哪儿就是哪儿，绝不从 interim 的未闭合
      尾巴里复活一份已经发过的正文。
    - 碎片比 interim 长（delta 跑在前面）：保留「interim 的未消费尾巴 + 多出来的字节」，
      原样切片，一个字符都不补。
    - 碎片被 interim 覆盖（截断视图／更早的切帧）：换成 interim 自己的未消费尾巴。
    - interim 已经闭合完、碎片又是最后一份已交付正文的旧切帧：丢弃（不重播也不粘连）。
    - 其余分叉的碎片：原样留着，封段时照旧按缺开头／缺末尾拒绝。

    旧共享分界那条「把 interim 末尾的 § 还给尾巴当开头」的路线已经删除：闭合标记
    属于它自己那份正文，下一份正文必须自带开头。
    """
    if not fragment:
        return ""
    raw, origin = _normalize_with_origin(fragment)
    frag = raw.rstrip("\n")
    if not frag.strip():
        return fragment
    interim_norm = _normalize(interim).rstrip("\n")
    tail = unsealed_tail(interim)
    tail_norm = _normalize(tail).rstrip("\n")
    if interim_norm and frag.startswith(interim_norm):
        # 碎片带着 interim 的尾巴，还多出后续字节：从尾巴开头原样切出来。
        keep_from = len(interim_norm) - len(tail_norm)
        return fragment[origin[keep_from] :]
    if interim_norm and interim_norm.startswith(frag):
        # 包括单个开口标记：interim 已覆盖它，不能留下来和下一份正文粘在一起。
        return tail
    if not tail_norm and not frag.replace(SEPARATOR, "").strip():
        # interim 已确认这段没有未闭合正文：纯标记碎片里的空信封（含 EOF 闭合）随它
        # 自己消费；奇数标记留下的独立开头仍由 unsealed_tail 原样保留。
        return unsealed_tail(fragment)
    if not interim_norm:
        return fragment
    if not tail_norm and _recuts_a_closed_unit(frag, interim_norm):
        return ""
    consumed = _consume_confirmed_envelopes(interim_norm, frag)
    if consumed:
        # interim 已确认的完整末份信封（含闭合行）随它自己那份正文消费掉；后面的
        # 碎片（真正的下一份信封）原样保留，一个字符都不补。
        rest = frag[consumed:]
        if rest.strip():
            return fragment[origin[consumed] :]
        return tail
    return fragment


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
    # 不设第二套规则：代码围栏内的整行 § 同样闭合，没有转义/豁免。
    positions = [index for index, line in enumerate(lines) if line == SEPARATOR]
    if not positions:
        _warn_dropped(_NO_MARKER, raw)
        return []

    head = "\n".join(lines[: positions[0]]).strip()
    if head:
        _warn_dropped(_MISSING_OPEN, head)
    units: list[str] = []
    previous_close: int | None = None
    for start, end in _envelope_pairs(positions):
        if previous_close is not None:
            stray = "\n".join(lines[previous_close + 1 : start]).strip()
            if stray:
                _warn_dropped(_MISSING_OPEN, stray)
        body = "\n".join(lines[start + 1 : end]).strip()
        previous_close = end
        if not body:
            continue  # 空信封（相邻标记或只有空白）不是正文，也不算违规。
        if _unit_length(body) > max_body_chars:
            raise TextUnitsError("body_overflow")
        units.append(body)
    if len(positions) % 2 == 1:
        # 最后一个标记没等到自己的闭合：它前面的游离正文缺开头，后面的正文缺末尾。
        if previous_close is not None:
            stray = "\n".join(lines[previous_close + 1 : positions[-1]]).strip()
            if stray:
                _warn_dropped(_MISSING_OPEN, stray)
        tail = "\n".join(lines[positions[-1] + 1 :]).strip()
        if tail:
            _warn_dropped(_MISSING_CLOSE, tail)
    elif previous_close is not None:
        # 所有信封都闭合了：闭合行之后如果还有游离正文，它没有自己的开头（下一份
        # 正文必须自带独立开头），同样拒发，并留一条准确的 missing_open。
        stray = "\n".join(lines[previous_close + 1 :]).strip()
        if stray:
            _warn_dropped(_MISSING_OPEN, stray)
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
