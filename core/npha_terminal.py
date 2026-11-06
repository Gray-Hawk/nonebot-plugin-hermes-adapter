"""completions／Runs 终态与 Runs delta 的单行 JSON 解码。

协议（completions 只接受一份终态决策，Runs 终态允许连续多份）：

    {"reply_to":"m:123","not_final":false}
    {"body":"正文\\n第二行"}

`decode_terminal` 的真实口径（与下面的实现逐条对应）：

- 首行不以 `{`／`｛` 开头 → 整段按唯一正文直发（A 类无信封），不做“自然文本还是
  JSON 残骸”的分类；只按 ``max_body_chars`` 计长，超限报 ``naked_overflow``。
- 首行是对象行 → 先试严格 JSON（首行控制对象 + 其余行正文对象，且 ``body`` 是字符串）：
  成功即采用。未知键忽略；重复键按 ``json`` 语义取最后一个，不另做重复键检测。
- 严格 JSON 不成立 → 宽松路径：按字段名在首行做硬编码搜索取控制值（容忍全角
  ``｛ ： ， ｝`` 与字段周围的杂字符，不做结构归一，也没有反转义），第二行起当正文
  文本，用 ``parse_body`` 取 ``body`` 值，取不到就把整行当正文；首行认不出任何决策
  字段且第二行也没有 body → 整段按 A 类正文直发。
- 宽松路径仍以“恰好一份”为界：以 `{`／`｛` 开头的行超过两行报 ``multiple``；正文为
  空 → 空 body 加自愈提示；正文超过 ``max_body_chars`` 报 ``body_overflow``。
- 媒体：解码前摘取 data URL、解码后回填，长度按摘取后的文本计量（内联图片不误判超长）。

Runs 终态多份（E2）与 Runs delta 候选检测（E3）都复用同一个 ``decode_terminal``：
前者只做切分与完整消费校验，后者只挑出已完整到达的两行决策，都不是第二套解析器。

本模块只做解码与候选检测，不含流式状态机、恢复窗口或旧协议兼容。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

MAX_BODY_CHARS = 4000

# 新协议控制字段：should_reply / reply_text / reply_context / reply_seq 全部退出。
CONTROL_STR_FIELDS = (
    "reply_to",
    "topic_hint",
    "release_anchor",
    "answer_anchor",
    "anchor_merge",
    "background_task",
)
CONTROL_BOOL_FIELDS = ("not_final", "should_exit_active", "pin_anchor")


class TerminalFormatError(ValueError):
    """终态信封不合法：调用方必须整体失败，不得回退原始信封发送。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class TerminalDecision:
    """control 与 body 分离；发送只消费 body。"""

    body: str
    control: dict[str, Any] = field(default_factory=dict)
    raw: str = ""
    reminder: str = ""   # 需要在下轮提示里带给模型的自愈提示

    @property
    def silent(self) -> bool:
        return not self.body.strip()


class _DuplicatedKey(Exception):
    pass


def parse_control(line: str) -> tuple[dict[str, Any], bool]:
    """硬编码搜索：在首行按字段名找值；找不到就用缺省，bad=True 表示一个字段都没认出来。"""
    control: dict[str, Any] = {name: "" for name in CONTROL_STR_FIELDS}
    control |= {name: False for name in CONTROL_BOOL_FIELDS}
    found = False
    for name in CONTROL_BOOL_FIELDS:
        raw = _find_value(line, name)
        if raw is None:
            continue
        if raw.startswith("true"):
            control[name] = True
            found = True
        elif raw.startswith("false"):
            control[name] = False
            found = True
    for name in CONTROL_STR_FIELDS:
        raw = _find_value(line, name)
        if raw is None:
            continue
        control[name] = re.split(r'["\,，}｝]', raw, 1)[0]
        found = True
    return control, not found


def _find_value(line: str, name: str) -> str | None:
    """在行里找 name 后面的值；只认成员位置（前面是 { , : 或行首/引号）。"""
    for match in re.finditer(re.escape(name), line):
        before = line[:match.start()].rstrip().rstrip('"').rstrip()
        if before and before[-1] not in '{｛,，:："':
            continue
        rest = line[match.end():].lstrip().lstrip('"').lstrip()
        if not rest.startswith((":", "：")):
            continue
        return rest[1:].lstrip().lstrip('"')
    return None


def parse_body(line: str) -> str:
    """正文：取 body 字段的值；取不到就把整行当正文（模型手误兜底）。"""
    raw = _find_value(line, "body")
    if raw is None:
        return line.strip().strip('{}"').strip()
    return raw.strip().rstrip("｝}").rstrip().rstrip('"').rstrip()


def structured_from_terminal(decision: TerminalDecision) -> dict[str, Any]:
    """终态决策 → handler 内部结构；自愈提示单独放在 _self_heal_hint。"""
    payload = {"body": decision.body, "should_reply": not decision.silent, **decision.control}
    if decision.reminder:
        payload["_self_heal_hint"] = decision.reminder
    return payload


def decode_terminal(text: str, *, max_body_chars: int = MAX_BODY_CHARS) -> TerminalDecision:
    """第一行搜决策字段，第二行起是正文；只做硬编码搜索，不解析、不修复、不猜结构。"""
    if not isinstance(text, str) or not text.strip():
        raise TerminalFormatError("empty")
    from .media_payload import _detach_data_urls, _reattach_data_urls

    shrunk, detached = _detach_data_urls(text)

    def _done(body_text: str, control: dict[str, Any], reminder: str = "") -> TerminalDecision:
        body = _reattach_data_urls(body_text, detached) if detached else body_text
        return TerminalDecision(body=body, control=control, raw=shrunk, reminder=reminder)

    lines = shrunk.split("\n")
    if not lines[0].lstrip().startswith(("{", "｛")):
        # 首行不是对象行：整段普通正文
        if len(shrunk) > max_body_chars:
            raise TerminalFormatError("naked_overflow")
        return _done(shrunk, parse_control("")[0], ENVELOPE_MISSING_HINT)
    # 合法输入先走标准 JSON：成功就正常解码，不再报坏信封
    try:
        control_obj = json.loads(lines[0])
        body_obj = json.loads("\n".join(lines[1:])) if len(lines) > 1 else None
    except json.JSONDecodeError:
        control_obj = None
        body_obj = None
    if isinstance(control_obj, dict) and isinstance(body_obj, dict) and isinstance(body_obj.get("body"), str):
        control = {name: "" for name in CONTROL_STR_FIELDS}
        control |= {name: False for name in CONTROL_BOOL_FIELDS}
        for name in CONTROL_STR_FIELDS:
            value = control_obj.get(name)
            if isinstance(value, str):
                control[name] = value
        for name in CONTROL_BOOL_FIELDS:
            value = control_obj.get(name)
            if isinstance(value, bool):
                control[name] = value
        return _done(body_obj["body"], control, "")

    object_lines = [ln for ln in lines if ln.lstrip().startswith(("{", "｛"))]
    if len(object_lines) > 2:
        raise TerminalFormatError("multiple")
    control, bad = parse_control(lines[0])
    if bad and not (len(lines) > 1 and _find_value(lines[1], "body") is not None):
        # 首行没有任何决策字段、第二行也没有 body：整段就是普通正文
        if len(shrunk) > max_body_chars:
            raise TerminalFormatError("naked_overflow")
        return _done(shrunk, parse_control("")[0], ENVELOPE_MISSING_HINT)

    body_text = "\n".join(lines[1:]).strip()
    if not body_text:
        return _done("", control, ENVELOPE_BROKEN_HINT)
    body = parse_body(body_text)
    if len(body) > max_body_chars:
        raise TerminalFormatError("body_overflow")
    return _done(body, control, ENVELOPE_BROKEN_HINT)


ENVELOPE_BROKEN_HINT = "上一轮信封格式炸了：只输出一行决策字段 JSON，再一行 body，不要全角括号、不要额外转义。"
ENVELOPE_MISSING_HINT = "上一轮没有写信封：本轮请先输出一行决策字段 JSON，再一行 body；只回普通正文会走直发、拿不到 reply_to 等决策字段。"


# ---------------------------------------------------------------------------
# E2：Runs 终态多份（本部署补丁）
#
# Runs 的 run.completed.output 允许「连续多份两行决策」：每份仍是
# 控制对象行 + 只含 body 的正文对象行，相邻两份之间允许空行。与 completions
# 的 decode_terminal（恰好一份）共用同一套单份解码（本文件上半部分），这里只做
# 切分与完整消费校验，不另起第二套解析器。
#
# 失败一律在任何发送／副作用之前整体拒绝：残段、非法尾随、只给控制不给正文、
# 连续两份控制、正文超限都返回 TerminalFormatError，调用方不得取首份或末份。
# ---------------------------------------------------------------------------


def _control_defaults() -> dict[str, Any]:
    control: dict[str, Any] = {name: "" for name in CONTROL_STR_FIELDS}
    control |= {name: False for name in CONTROL_BOOL_FIELDS}
    return control


def _is_object_line(line: str) -> bool:
    return line.lstrip().startswith(("{", "｛"))


def _body_only_value(line: str) -> str | None:
    """整行是只含 `body` 字符串的对象时返回 body；否则 None（缺控制单元判定）。"""
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    if isinstance(obj, dict) and set(obj) == {"body"} and isinstance(obj["body"], str):
        return obj["body"]
    return None


def _control_shaped(line: str) -> bool:
    """行里出现任一控制字段名即按控制行看待（识别「连续两份控制、缺正文」）。"""
    return any(
        _find_value(line, name) is not None
        for name in (*CONTROL_BOOL_FIELDS, *CONTROL_STR_FIELDS)
    )


def _body_only_decision(line: str, body: str, max_body_chars: int) -> TerminalDecision:
    """缺失控制行、只有正文对象的单元：控制全缺省，正文交付。"""
    from .media_payload import _detach_data_urls, _reattach_data_urls

    shrunk, detached = _detach_data_urls(body)
    if len(shrunk) > max_body_chars:
        raise TerminalFormatError("body_overflow")
    real = _reattach_data_urls(shrunk, detached) if detached else body
    return TerminalDecision(
        body=real, control=_control_defaults(), raw=line, reminder=ENVELOPE_MISSING_HINT,
    )


def decode_terminal_batch(
    text: str, *, max_body_chars: int = MAX_BODY_CHARS
) -> list[TerminalDecision]:
    """Runs 终态：连续两行一份决策，允许一份或多份；必须恰好完整消费。

    - 每份：控制对象行 + 正文行（正文对象，或控制行后紧跟的一行纯文本正文）。
    - 缺控制（只有 `{"body":"..."}` 行）→ 该份控制缺省、正文交付。
    - 空行只作份间分隔，不参与内容。
    - 首行不是对象 → A 类无信封，整段按唯一一份正文处理（与 completions 同口径）。
    - 其余任何消费不完整的形态（残段／非法尾随／有控制无正文／连续两份控制）整体失败。
    """
    if not isinstance(text, str) or not text.strip():
        raise TerminalFormatError("empty")
    lines = [line for line in text.split("\n") if line.strip()]
    if not _is_object_line(lines[0]):
        # A 类无信封：与 completions 完全同一份解码（含媒体摘取/回填计量），
        # 不能直接拿原始长度计量——真实 inliner 产出的内联图片 base64 会误判超长。
        return [decode_terminal(text, max_body_chars=max_body_chars)]
    units: list[TerminalDecision] = []
    index = 0
    while index < len(lines):
        control_line = lines[index]
        if not _is_object_line(control_line):
            raise TerminalFormatError("residue")
        body_inline = _body_only_value(control_line)
        if body_inline is not None:
            units.append(_body_only_decision(control_line, body_inline, max_body_chars))
            index += 1
            continue
        if index + 1 >= len(lines):
            raise TerminalFormatError("missing_body")
        body_line = lines[index + 1]
        if _is_object_line(body_line) and _body_only_value(body_line) is None \
                and _control_shaped(body_line):
            # 第二行仍是控制对象：这一份只有控制没有正文，不得把下一份的控制当正文。
            raise TerminalFormatError("control_without_body")
        units.append(
            decode_terminal(control_line + "\n" + body_line, max_body_chars=max_body_chars)
        )
        index += 2
    return units


def structured_batch_from_terminal(
    decisions: list[TerminalDecision],
) -> list[dict[str, Any]]:
    """多份终态 → handler 内部结构列表（保持顺序，逐份独立 control）。"""
    return [structured_from_terminal(decision) for decision in decisions]


# ---------------------------------------------------------------------------
# E3：Runs delta 流里的「完整决策」候选检测（本部署补丁）
#
# 只做一件事：在持续追加的 delta 文本里找出**已经完整到达**的两行决策，交给
# 上面同一份 decode_terminal 解码。不是第二套正文解析器：
#   - 只认「完整对象行」（首字符 {／｛、末字符 }／｝）；
#   - 控制行必须能被 decode_terminal 认成决策形状（控制字段／{}／缺控制正文对象）；
#   - 未闭合的候选原样留在缓冲区，等下一帧继续；
#   - 中间的过程旁白不消费、不发送（旁白走既有的 interim 通道）。
# 候选判定失败就跳过该行（终态 decode_terminal_batch 仍按 E2 口径整体校验）。
# ---------------------------------------------------------------------------

def _complete_object_line(line: str) -> bool:
    """整行是否形如一个闭合对象：首 {／｛、末 }／｝（协议要求对象单行）。"""
    stripped = line.strip()
    return len(stripped) >= 2 and stripped[0] in "{｛" and stripped[-1] in "}｝"


def _line_is_body_object(line: str) -> bool:
    """整行是只含 body 的正文对象（严格 JSON；分片未闭合时绝不提前发送）。

    正文对象不走全角/坏转义修复：协议里 body 行恒为合法 JSON，若这里放宽，
    正文自身含 ``}`` 且恰好卡在分片边界时会被误当成完整对象提前发送。
    """
    stripped = line.strip()
    if not _complete_object_line(stripped):
        return False
    return _body_only_value(stripped) is not None


def _balanced_quotes(line: str) -> bool:
    """未转义双引号是否成对（分片把字符串截断时不把控制行当完整）。"""
    count = 0
    escaped = False
    for ch in line:
        if escaped:
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == '"':
            count += 1
    return count % 2 == 0


def _line_is_control_object(line: str) -> bool:
    """整行像控制对象：空对象 {} 或带任一控制字段；纯正文对象不算。"""
    stripped = line.strip()
    if not _complete_object_line(stripped):
        return False
    if _body_only_value(stripped) is not None:
        return False
    try:
        obj = json.loads(stripped)
    except ValueError:
        # E1 已定案的全角结构修复：只认控制字段明确、引号闭合的行。
        return _balanced_quotes(stripped) and (
            _control_shaped(stripped)
            or bool(re.fullmatch(r"[{｛]\s*[}｝]", stripped))
        )
    return isinstance(obj, dict) and (not obj or _control_shaped(stripped))


def scan_stream_decisions(
    text: str, start: int = 0, *, max_body_chars: int = MAX_BODY_CHARS
) -> tuple[list[TerminalDecision], int]:
    """从 ``text[start:]`` 里取出所有已完整到达的决策；返回 (决策, 下次起点)。

    - ``start`` 之前的内容视为已消费；返回值 >= start，未闭合候选的起点会被保留。
    - 空行、过程旁白、非法对象行只推进游标，不产出决策。
    - 控制行后缺正文对象（下一行仍是控制/未闭合）时只跳过该控制行，不死等。
    - 缓冲区末尾没有换行的行，除非本身是完整决策，否则一律等下一帧——分片可能
      把正文/控制截断在任意字符，提前消费会永久丢掉这一段。
    """
    units, keep = _scan_stream_units(text, start, max_body_chars=max_body_chars)
    return [decision for decision, _start, _end in units], keep


def _scan_stream_units(
    text: str, start: int = 0, *, max_body_chars: int = MAX_BODY_CHARS
) -> tuple[list[tuple[TerminalDecision, int, int]], int]:
    """``scan_stream_decisions`` 的带区间版本：每个决策附带它在 ``text`` 里的
    ``[start, end)``。旁白出口据此把已即时消费的决策从间隙缓冲里摘掉，
    不再另写一套识别逻辑。"""
    decisions: list[tuple[TerminalDecision, int, int]] = []
    pos = start
    keep = start
    length = len(text)
    while pos < length:
        newline = text.find("\n", pos)
        terminated = newline >= 0
        line = text[pos:] if newline < 0 else text[pos:newline]
        next_pos = length if newline < 0 else newline + 1
        stripped = line.strip()
        if not stripped:
            if not terminated:
                break
            pos = next_pos
            keep = pos
            continue
        body_inline = _body_only_value(stripped)
        if body_inline is not None:
            decisions.append(
                (_body_only_decision(stripped, body_inline, max_body_chars), pos, next_pos)
            )
            pos = next_pos
            keep = pos
            continue
        if _line_is_control_object(stripped):
            # 控制行已完整：找下一条非空行作为正文候选。
            scan = next_pos
            body_line = ""
            body_next = next_pos
            body_terminated = False
            while scan < length:
                nl2 = text.find("\n", scan)
                candidate = text[scan:] if nl2 < 0 else text[scan:nl2]
                body_next = length if nl2 < 0 else nl2 + 1
                if candidate.strip():
                    body_line = candidate.strip()
                    body_terminated = nl2 >= 0
                    break
                if nl2 < 0:
                    break
                scan = body_next
            if body_line and _line_is_body_object(body_line):
                try:
                    decision = decode_terminal(
                        stripped + "\n" + body_line, max_body_chars=max_body_chars
                    )
                except TerminalFormatError:
                    pos = next_pos
                    keep = pos
                    continue
                decisions.append((decision, pos, body_next))
                pos = body_next
                keep = pos
                continue
            # 正文不是合法正文对象：只有确认**下一条非空行已换行终结**（不可能再被
            # 后续分片补全）才跳过控制行；缓冲区到控制行结尾时一律等下一帧。
            if body_line and body_terminated and terminated:
                pos = next_pos
                keep = pos
                continue
            break
        if terminated:
            pos = next_pos
            keep = pos
            continue
        break
    return decisions, keep


def strip_complete_decisions(
    text: str, *, max_body_chars: int = MAX_BODY_CHARS
) -> str:
    """去掉已经**完整到达**的决策信封，只留旁白文本。

    R1：delta 全文同时喂给即时决策扫描与工具间隙旁白缓冲，决策已由
    ``drain_stream_decisions`` 即时发送后，不能再把同一段原始 JSON 当旁白发一遍。
    这里复用同一份 ``_scan_stream_units`` 的区间结果做摘除，不另写识别逻辑；
    普通旁白（不含完整信封的行）原样保留。未闭合的候选不摘——它还没成为决策。
    """
    units, _keep = _scan_stream_units(text, 0, max_body_chars=max_body_chars)
    if not units:
        return text
    parts: list[str] = []
    cursor = 0
    for _decision, start, end in units:
        parts.append(text[cursor:start])
        cursor = end
    parts.append(text[cursor:])
    return "".join(parts)
