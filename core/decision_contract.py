"""决策信封(submit_decision)字段契约的单一来源。

信封字段此前在四处各自手写:hermes_client 的 _DECISION_HINT(英文)、
prompt_builder 的 _DECISION_PROTOCOL_BLOCK(中文主协议块)、同文件近因 reminder
字段单、同文件私聊版协议块。加/改/删一个字段要人肉同步多处,漏一处模型侧与插件
侧就行为分裂(技能台账已把「信封字段加/改/删必须同步」列为硬性规则,本模块把
「字段清单」收口成一处)。

只收口字段清单:协议块里的其余叙述(@ 规则、锚规则、流程说明、行动诚实/回复纪律
等)仍原位保留在 prompt_builder;_DECISION_HINT 与协议块「两份都在场」的现状不变
(行为零变化,只把内容来源收口)。

各消费点渲染形态与收口前逐字节一致(收口批 diff 核对):
  - render_en_field_lines(): _DECISION_HINT 的字段行(英文,desc 在括号内);
  - render_cn_field_lines(): 主协议块字段行(全集)/ 私聊版字段行(names 子集 + short);
  - render_field_name_list(): 近因 reminder 的 slash 字段单。

另收口信封里「消息地址」的解析:parse_reply_to() 只接受 `m:<正整数>`(本会话
消息库自增号),裸数字 / QQ message_id / 跨格式一律按没给处理。reply_text 正文
内联 @ 的标记语法(`[CQ:at,qq=<数字>]`,仅此一种 CQ 码)由出站解析方
`core/outbound.py` 的 `_INLINE_AT_MARKER_RE` 承担,契约模块不重复定义正则。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DecisionFieldSpec:
    """一个字段的展示形态(中英双语)。

    en_note / cn_note = 括号内的类型与必填标注(逐消费点手写形态原样搬入);
    en_desc = 英文行括号内 ` — ` 之后的说明("");
    cn_desc = 中文行括号外 ` — ` 之后的说明("" = 无,可含换行);
    cn_desc_short = 私聊版简写说明("" = 回落 cn_desc)。
    required 仅为字段元数据,渲染看各 note 原文。
    """

    name: str
    required: bool
    en_note: str
    en_desc: str
    cn_note: str
    cn_desc: str
    cn_desc_short: str = ""


# 权威字段表:顺序 = 渲染顺序(reminder 字段单 / 协议块字段行都按此序)。
DECISION_FIELDS: tuple[DecisionFieldSpec, ...] = (
    DecisionFieldSpec(
        name='should_reply',
        required=True,
        en_note='boolean, required',
        en_desc='',
        cn_note='boolean, required',
        cn_desc='是否要回复',
    ),
    DecisionFieldSpec(
        name='reply_text',
        required=False,
        en_note='string, optional, required when should_reply=true',
        en_desc='',
        cn_note='string',
        cn_desc='should_reply=true 时必填,留空表示静默',
        cn_desc_short='should_reply=true 时必填',
    ),
    DecisionFieldSpec(
        name='reply_to',
        required=False,
        en_note='string, optional',
        en_desc='quote one message of this conversation by its ref `m:<id>` — the number in front of a recent_messages / current_message line, e.g. "m:123". Only that exact form is accepted: bare numbers, QQ message ids and any other shape are ignored, refs outside this conversation are rejected. Omit when not quoting. In groups, a resolved Reply automatically mentions the quoted sender immediately after it; do not manually repeat that person at the start. Private replies never auto-mention. Additional people or other positions use [CQ:at,qq=<qq id>] inside reply_text',
        cn_note='string',
        cn_desc='引用本会话里的一条消息:值写成 `m:<id>`(如 "m:123"),即 recent_messages\n    / current_message 行首的 m: 号。只认这一种写法:裸数字、QQ 消息 id、别的形态\n    一律当没给,不属于本会话的号会被拒;不确定号就不要引用。\n    群聊成功解析 Reply 会自动 @ 被引用者,私聊不自动 @;\n    额外的人或其它位置用 reply_text 正文内联 @',
        cn_desc_short='引用某条消息时填它行首的 m 号,如 "m:123"',
    ),
    DecisionFieldSpec(
        name='topic_hint',
        required=False,
        en_note='string, optional',
        en_desc='',
        cn_note='string',
        cn_desc='简短话题标记(中文 OK),将带入下一轮 runtime_state',
    ),
    DecisionFieldSpec(
        name='should_exit_active',
        required=False,
        en_note='boolean, optional',
        en_desc='',
        cn_note='boolean',
        cn_desc='谨慎使用,见下方退出门槛',
    ),
    DecisionFieldSpec(
        name='pin_anchor',
        required=False,
        en_note='boolean, optional',
        en_desc='commission judgement turns only: true pins the current request into <pinned_tasks>; omit/false for jokes or one-off chatter',
        cn_note='boolean',
        cn_desc='委托判断轮专用:确认当前 @/私聊请求是需要后续跟踪的\n    任务时给 true,把它钉进 <pinned_tasks>;玩笑、围观、一次性闲聊不要钉。',
    ),
    DecisionFieldSpec(
        name='release_anchor',
        required=False,
        en_note='string, optional',
        en_desc='commissioner user_id or "all" to unpin finished task anchors from <pinned_tasks>',
        cn_note='string',
        cn_desc='松锚:值为委托人的 user_id 或 "all"。\n    看到 <pinned_tasks> 里的锚已完结(任务办完/话题已漂)时设置,见下方任务锚节',
    ),
    DecisionFieldSpec(
        name='answer_anchor',
        required=False,
        en_note='string, optional',
        en_desc='commissioner user_id or "all": declare your reply answered that pinned task; unknown ids are rejected and echoed back next turn',
        cn_note='string',
        cn_desc='标应答:本轮回复解答了某条在钉委托时,填委托人的\n    user_id 或 "all";只填在钉锚里的 id,填错会被忽略并在下一轮提醒你',
    ),
    DecisionFieldSpec(
        name='anchor_merge',
        required=False,
        en_note='string, optional',
        en_desc='starter user_id of a pinned task this message belongs to: merges the sender into its participants instead of pinning new',
        cn_note='string',
        cn_desc='归并:当前消息属于某条在钉任务(补充/验收)时,填该\n    任务发起者的 user_id;只填在钉锚的发起者 id,填错会被忽略并下轮提醒',
    ),
    DecisionFieldSpec(
        name='background_task',
        required=False,
        en_note='string, optional',
        en_desc='a one-line note when you just dispatched background work (subagent/background process); on completion the system wakes you and echoes the note. Omit when nothing is running',
        cn_note='string',
        cn_desc='离场任务便签:刚派出后台任务(子代理/后台进程)时\n    填一句任务摘要;完成时系统会叫醒你并把便签回投。便签让唤醒轮知道「在等谁」,\n    不派活不要填。填了便签的回合可以直接静默收尾,不必承诺「稍后汇报」',
    ),
    DecisionFieldSpec(
        name='not_final',
        required=False,
        en_note='boolean, optional',
        en_desc='true when this reply is NOT your final result but a task acknowledgement: the plugin will resend your reply as-is and then immediately fire a continue turn so you can actually do the work and deliver the final answer in the normal flow. Set it only when a concrete task is still pending; omit it for chit-chat and wrapped-up turns',
        cn_note='boolean',
        cn_desc='这不是最终回复(仅在有明确未完成任务时填 true):插件会把你这条\n    原样发出(当作任务回执),随即立刻追加一发「继续执行轮」,让你真去干活、\n    完成后按正常流程输出最终回复。闲聊/正常收尾不要填;它与 background_task 语义\n    互斥(那个是后台已在跑、等系统唤醒),同轮同填时以 not_final 为准(先续跑,便签照登记)',
        cn_desc_short='这不是最终回复,有未完成任务才填',
    ),
)


RUNS_DECISION_FIELDS: tuple[DecisionFieldSpec, ...] = (
    DecisionFieldSpec(
        name="reply_context",
        required=True,
        en_note="string, runs required",
        en_desc="copy the current runs_reply_context value verbatim; never reuse an older value",
        cn_note="string, runs required",
        cn_desc="逐字回带当前 runs_reply_context 的值,禁止沿用历史值",
        cn_desc_short="逐字回带当前 runs_reply_context",
    ),
    DecisionFieldSpec(
        name="reply_seq",
        required=True,
        en_note="integer, runs required",
        en_desc="start at 1 and increment for every final decision in this run, including silent decisions",
        cn_note="integer, runs required",
        cn_desc="本 run 每份最终决策从 1 起递增,静默决策也占号",
        cn_desc_short="本 run 决策序号,从 1 递增,静默也占号",
    ),
)


def append_reply_context(content: Any, reply_context: str) -> Any:
    """把本轮权威身份附到 user 内容；多模态列表复制后追加，不改调用方对象。"""
    marker = (
        "\n\n[runs_reply_context]\n"
        f"reply_context={reply_context}；本 run 各份最终决策逐字回带此值，"
        "reply_seq 从 1 递增，静默决策也占号"
    )
    if isinstance(content, str):
        return content + marker
    if isinstance(content, list):
        return [*content, {"type": "text", "text": marker.lstrip()}]
    raise TypeError("unsupported user content type")


# 引用槽(reply_to)唯一合法形态:`m:<正整数>`,号 = 消息库自增主键,与
# recent_messages / current_message 行首的 m 号同源。QQ 的 message_id 与 Hermes
# 转录行 id 只作内部映射,不接受。
REPLY_TO_PATTERN = re.compile(r"m:([1-9][0-9]*)")


def parse_reply_to(value: Any) -> int | None:
    """信封 reply_to → 本会话消息库 id;不合法返回 None(按没给处理)。

    严格全串匹配,只认 `m:<正整数>`:
      - 裸数字 / int / bool:一律拒(分不清是 QQ message_id 还是 m 号,不给猜);
      - QQ message_id 形态(负数、超长串、带别的前缀):格式不符即拒;
      - 跨格式(`M:1`、`#1`、`reply:1`、`[m:1]`、`m:1:2`、`m: 1`、尾随空白、
        前导零):拒绝,不做 trim/补全。
    归属校验(号是否属于当前 adapter / 会话)由调用方查库,本函数只做形态。
    """
    if not isinstance(value, str):
        return None
    m = REPLY_TO_PATTERN.fullmatch(value)
    if m is None:
        return None
    return int(m.group(1))


def _render_line(
    spec: DecisionFieldSpec, *, lang: str, short: bool = False
) -> str:
    """单字段行。

    EN: `  name (note)` / `  name (note — desc)`;
    CN: `  name (note)` / `  name (note) — desc`(desc 可含换行)。
    """
    if lang == "en":
        if spec.en_desc:
            return f"  {spec.name} ({spec.en_note} — {spec.en_desc})"
        return f"  {spec.name} ({spec.en_note})"
    note = spec.cn_note
    desc = spec.cn_desc_short if short and spec.cn_desc_short else spec.cn_desc
    line = f"  {spec.name} ({note})"
    if desc:
        line += f" — {desc}"
    return line


def render_en_field_lines(*, runs_identity: bool = False) -> str:
    """英文字段行(_DECISION_HINT 用),每行 `\n` 收尾。"""
    fields = DECISION_FIELDS + (RUNS_DECISION_FIELDS if runs_identity else ())
    return "".join(_render_line(f, lang="en") + "\n" for f in fields)


def _select(
    names: tuple[str, ...] | None,
    fields: tuple[DecisionFieldSpec, ...],
) -> tuple[DecisionFieldSpec, ...]:
    if names is None:
        return fields
    wanted = set(names)
    selected = tuple(
        f for f in fields
        if f.name in wanted and f not in RUNS_DECISION_FIELDS
    )
    return selected + tuple(f for f in fields if f in RUNS_DECISION_FIELDS)


def render_cn_field_lines(
    names: tuple[str, ...] | None = None,
    *,
    short: bool = False,
    runs_identity: bool = False,
) -> str:
    """中文字段行(主协议块 names=None;私聊版 names 子集 + short=True),每行 `\n` 收尾。"""
    fields = DECISION_FIELDS + (RUNS_DECISION_FIELDS if runs_identity else ())
    return "".join(
        _render_line(f, lang="cn", short=short) + "\n"
        for f in _select(names, fields)
    )


def render_field_name_list(*, runs_identity: bool = False) -> str:
    """`a / b / c` 形态的字段名清单(近因 reminder 用)。"""
    fields = DECISION_FIELDS + (RUNS_DECISION_FIELDS if runs_identity else ())
    return " / ".join(f.name for f in fields)
