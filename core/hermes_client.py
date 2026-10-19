"""
Hermes API Server HTTP 客户端

通过 /v1/chat/completions 与 Hermes Agent 通信。
M1-mem 路径 B(P0-spike 决策):tools/tool_choice 被 Hermes 吞掉,改用 system prompt 强约束 + JSON5 容错解析。
"""

from __future__ import annotations

import json
import re
import uuid
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx
import json5  # type: ignore[import-untyped]
from nonebot import logger

from ..config import plugin_config
from .decision_contract import render_en_field_lines
from .media_patterns import _MD_IMAGE_PATTERN, _MEDIA_TAG_PATTERN
from .media_whitelist import inline_local_media_images
from .message_context import render_message_context_head
from .routing import HermesTarget, default_target
from .run_tracker import make_scope_key

# user_content_override 期望形态:纯文本 或 OpenAI 多模态 parts 列表
UserContent = str | list[dict[str, Any]]

# 出站媒体引用形态正则(_MD_IMAGE_PATTERN / _MEDIA_TAG_PATTERN)的单一来源:
# core/media_patterns.py(2026-10-16 同源化收口批,审计确认8)——与
# media_whitelist 的内联收口共用同一组,任一侧形态改动即两侧同步。import 见文首。

# Hermes apiserver 把 provider 端错误包成 502, body 形如:
#   {"error": {"message": "Error code: 400 - {'error': {'message': '...'}}"}}
# 把外层 502 当真因显示会误导, 因此抠出内层 status 与 reason。
_INNER_STATUS_RE = re.compile(r"Error code:\s*(\d+)")
_VISION_UNSUPPORTED_RE = re.compile(
    r"unknown variant\s+[`'\"]image_url|image_url.*not.*support|does not support.*image|"
    r"input_image.*not.*support|multimodal.*not.*support",
    re.IGNORECASE,
)

# 提取首个 balanced `{...}` 子串:栈式扫描,支持任意嵌套深度,
# 正确穿过字符串字面量内的花括号(`{"x": "}"}` 不会误判提前闭合)。
# 这取代了之前的单层正则——小模型(Flash 系)有时会输出 nested object
# 或在 reply_text 里嵌带 `{}` 的代码段,正则会抠错或抠不全。


def _find_first_balanced_json_object(text: str) -> str | None:
    """扫描 text,返回从首个 `{` 到与之配平的 `}` 的子串。找不到完整平衡块返回 None。

    规则(对齐 JSON5):
      - 双引号 / 单引号字符串都识别
      - 字符串内的 `\\X` 整对透传(`\\"` / `\\\\` / `\\n` 等),不参与 quote/brace 计数
      - 字符串内出现的 `{` / `}` 不计入深度
      - 字符串内允许出现真换行(JSON5 不允,但调用方有 _escape_raw_newlines 二次回退)
    """
    if not text:
        return None
    n = len(text)
    i = 0
    while i < n and text[i] != "{":
        i += 1
    if i >= n:
        return None
    start = i
    depth = 0
    in_string = False
    quote = ""
    while i < n:
        c = text[i]
        if in_string:
            if c == "\\" and i + 1 < n:
                i += 2
                continue
            if c == quote:
                in_string = False
                quote = ""
            i += 1
            continue
        if c == '"' or c == "'":
            in_string = True
            quote = c
            i += 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
        i += 1
    return None


# 字段行单一来源:core/decision_contract.py(2026-10-16 同源化收口批,审计确认5);
# 渲染形态与收口前逐字节一致。与中文协议块「两份都在场」的现状不变。
_DECISION_HINT = (
    "\n\n=== STRUCTURED OUTPUT ===\n"
    "Your reply MUST be a single JSON object with these keys:\n"
    + render_en_field_lines()
    + "Output ONLY the JSON object, no preamble, no postscript, no markdown fences.\n"
    "All string values MUST be single-line; escape line breaks inside strings as \\n (no raw newlines)."
)


def _escape_raw_newlines_in_strings(s: str) -> str:
    """把 JSON 字符串字面量内部的裸 \\n/\\r/\\t 转义掉,让 json5 能解析。

    LLM 经常在 reply_text 里嵌真换行(段落分隔),JSON5 字符串不允许;首发 json5
    抛 `Unexpected "\\n"` 后我们走这一遍状态机重试一次。状态:跟踪 " / ' 进出
    string、`\\X` 整对透传(不参与 quote 计数),quoted 区里把裸控制字符替换。
    """
    out: list[str] = []
    in_string = False
    quote = ""
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if not in_string:
            if c == '"' or c == "'":
                in_string = True
                quote = c
            out.append(c)
            i += 1
            continue
        # 在 string 内
        if c == "\\" and i + 1 < n:
            # 转义序列整对透传(含 \" / \' / \\ / \n 等),不参与 quote 计数
            out.append(s[i : i + 2])
            i += 2
            continue
        if c == quote:
            in_string = False
            quote = ""
            out.append(c)
            i += 1
            continue
        if c == "\n":
            out.append("\\n")
        elif c == "\r":
            out.append("\\r")
        elif c == "\t":
            out.append("\\t")
        else:
            out.append(c)
        i += 1
    return "".join(out)


# 中文模型常把 JSON 结构位(字段分隔、键值冒号)吐成全角标点,让 json / json5 双双拒解析。
# 只归一化**字符串外**的这些 —— reply_text 正文里的全角逗号/冒号是合法中文,动一个字节
# 都会污染发给用户的文本。用与 _escape_raw_newlines_in_strings 同款状态机圈定 string 边界。
_FULLWIDTH_JSON_PUNCT = {
    "，": ",",  # U+FF0C 全角逗号
    "：": ":",  # U+FF1A 全角冒号
}


def _normalize_fullwidth_json_punct(s: str) -> str:
    """把字符串**外**的全角结构标点(，/：)换成 ASCII;字符串内部原样保留。

    合法 JSON 里字符串外只可能出现结构 token,所以字符串外的全角 ，/： 必是错吐的
    分隔符/冒号,归一是安全的;合规输入下本函数是 no-op。
    """
    out: list[str] = []
    in_string = False
    quote = ""
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if not in_string:
            if c == '"' or c == "'":
                in_string = True
                quote = c
                out.append(c)
            else:
                out.append(_FULLWIDTH_JSON_PUNCT.get(c, c))
            i += 1
            continue
        # 在 string 内:转义对整体透传(不参与 quote 计数),内容一律不动
        if c == "\\" and i + 1 < n:
            out.append(s[i : i + 2])
            i += 2
            continue
        if c == quote:
            in_string = False
            quote = ""
        out.append(c)
        i += 1
    return "".join(out)


def _summarize_error_body(body: str) -> tuple[str, int | None]:
    """从 Hermes 错误响应体里抠出可读 reason 与内层 status。

    Hermes apiserver 习惯把 provider 端错误外包成 502, body 是 JSON, `error.message`
    形如 `"Error code: 400 - {...}"`。直接把 200 字符 body 倒进日志噪音大、
    用户看到只剩外层 502 完全不知道发生了什么, 所以这里先剥一层。

    返回 (reason_snippet, inner_status_or_None);body 不可解析时退化到 body 截断。
    """
    if not body:
        return "(empty body)", None
    raw = body.strip()
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return raw[:200], None
    err = parsed.get("error") if isinstance(parsed, dict) else None
    if isinstance(err, dict):
        msg = str(err.get("message") or err).strip()
    elif isinstance(err, str):
        msg = err.strip()
    else:
        msg = raw[:200]
    inner_status: int | None = None
    m = _INNER_STATUS_RE.search(msg)
    if m:
        try:
            inner_status = int(m.group(1))
        except ValueError:
            inner_status = None
    return msg[:300], inner_status


def _transport_failure(status: int, body: str) -> "ChatResult":
    """非 200 响应的统一归纳(本部署补丁 2026-10-08 抽出, 非流式/流式分支共用)。

    403 识别:外层 HTTP 状态或 502 包裹的内层 status 任一为 403 即打
    is_forbidden_error 硬标记,handler 层据此硬静默(防 key 失效/额度耗尽时
    每条触发刷一条「AI 服务异常」)。
    """
    reason, inner_status = _summarize_error_body(body)
    inner_tag = f" inner={inner_status}" if inner_status else ""
    logger.error(f"[HERMES] upstream HTTP {status}{inner_tag}: {reason}")
    return ChatResult(
        raw_text=_user_facing_error(reason),
        parse_failed=True,
        is_transport_error=True,
        is_forbidden_error=(status == 403 or inner_status == 403),
        is_rate_limited=(status == 429),
    )


def preview_raw(text: str, *, head: int = 220, tail: int = 220) -> str:
    """给日志用的回复预览:**头尾都取**,中间省略。

    只取头部对诊断结构化解析失败没用 —— 破在末尾的情况(缺闭合括号、多余字段、
    尾随逗号)恰好都在被截掉的那一半里。先切片再转义:内联大图时 text 是 MB 级,
    反过来做会白拷一遍整串。
    """
    if not text:
        return ""

    def _esc(s: str) -> str:
        return s.replace("\n", "\\n").replace("\r", "\\r")

    if len(text) <= head + tail:
        return _esc(text)
    return f"{_esc(text[:head])}…[省略 {len(text) - head - tail} 字符]…{_esc(text[-tail:])}"


def _user_facing_error(reason: str) -> str:
    """把 _summarize_error_body 的 reason 翻译成给群里发的简短提示。

    命中已知模式(目前: 图片输入被非 vision 模型拒)就给精准提示;否则带 reason 片段
    让用户能直接看到真因, 不再只露一个误导性的 502。
    """
    if _VISION_UNSUPPORTED_RE.search(reason):
        return "⚠️ 当前主模型不支持图片识别,请改用文字提问或换用 vision 模型"
    snippet = reason.strip().splitlines()[0][:140] if reason.strip() else "未知错误"
    return f"⚠️ AI 服务异常: {snippet}"


# 输出被截断时 markdown 图片可能停在 base64 中途,没有闭合 `)`。半截 base64
# 解出来是坏图,平台要么拒收要么显示破图,还会把「上游截断了回复」伪装成「发图成功」,
# 所以只留占位、不投递(投递前的合法性最终判定在 outbound._parse_image_data_url)。
_TRUNCATED_MD_IMAGE_PATTERN = re.compile(r"!\[[^\]]*\]\(data:image/[\w.+-]+;base64,[A-Za-z0-9+/=\s]*$")
_TRUNCATED_IMAGE_PLACEHOLDER = "[图片传输不完整]"


def _media_basename(ref: str) -> str:
    """只留 basename,不泄完整主机路径。"""
    return ref.rsplit("/", 1)[-1] or ref


def extract_response_media(text: str) -> tuple[str, list[str], list[str]]:
    """从 Hermes 回复中提取 markdown 图片 / MEDIA: 标签 URL,返回 (清洗后文本, URL 列表, 本地文件路径列表)。

    两种标签同一套分流规则:http(s) 与 data:image/…;base64 进媒体列表;
    主机本地路径(本部署补丁 2026-10-08 起)进 file_paths 供 send_local_files
    上传文件本体(白名单判定在发送侧), 文本原位仍留 [生成了文件: 文件名]
    占位——上传被白名单拒绝时用户仍能看到提示, 上传成功时占位符即说明文字;
    其余 data: 非图片 payload 直接去掉。
    """
    media_urls: list[str] = []
    file_paths: list[str] = []

    def _collect(ref: str) -> str | None:
        """进媒体列表返回 None;否则返回该原位替换成的占位文本。"""
        # data:image/…;base64 是 api_server 对本地生成图片(≤5MB)的内联形态,
        # 与 http(s) 同样要进 media 列表;解码与合法性校验在 outbound 侧做。
        if ref.startswith(("http://", "https://", "data:image/")):
            media_urls.append(ref)
            return None
        if ref.startswith("data:"):
            # 非图片 data URL(text/plain 等):既不是可投递媒体也不是文件,直接去掉,
            # 不能按路径切 basename——那会把 payload 本身当文件名贴到群里。
            return ""
        file_paths.append(ref)
        return f"[生成了文件: {_media_basename(ref)}]"

    def _md_image_repl(m: re.Match) -> str:
        return _collect(m.group(2)) or ""

    def _media_tag_repl(m: re.Match) -> str:
        return _collect(m.group(1)) or ""

    cleaned = _MD_IMAGE_PATTERN.sub(_md_image_repl, text)
    cleaned = _MEDIA_TAG_PATTERN.sub(_media_tag_repl, cleaned)
    cleaned = _TRUNCATED_MD_IMAGE_PATTERN.sub(_TRUNCATED_IMAGE_PLACEHOLDER, cleaned)

    return cleaned.strip(), media_urls, file_paths


# 救补判据原只认双引号键——单引号键(json5 合法)叠加截断时整条回复
# 丢失。键名引号形态双收;字符串值按开引号配对回捕。
def _salvage_bool_re(key: str) -> re.Pattern:
    return re.compile(rf"[\"']{key}[\"']\s*:\s*(true|false)", re.IGNORECASE)


_SALVAGE_SHOULD_REPLY_RE = _salvage_bool_re("should_reply")
_SALVAGE_REPLY_TEXT_RE = re.compile(
    r"[\"']reply_text[\"']\s*:\s*(?P<q>[\"'])(?P<v>(?:\\.|(?!(?P=q)).)*)(?P=q)?", re.DOTALL
)
_SALVAGE_TOPIC_HINT_RE = re.compile(
    r"[\"']topic_hint[\"']\s*:\s*(?P<q>[\"'])(?P<v>(?:\\.|(?!(?P=q)).)*)(?P=q)?", re.DOTALL
)
_SALVAGE_EXIT_ACTIVE_RE = _salvage_bool_re("should_exit_active")
# M1 收尾(2026-10-17 维护者拍板):not_final 入救补——布尔抠错概率低,丢失则
# 回执发出但续跑轮没来=任务烂尾;其余字段(锚家族/at_user_id)抠错代价大于丢失,
# 维持不认。
_SALVAGE_NOT_FINAL_RE = _salvage_bool_re("not_final")


def _salvage_truncated_reply_text(text: str) -> dict[str, Any] | None:
    """当 JSON 块语法破坏 (如输出被截断、未闭合双引号或花括号) 时,
    尝试通过正则从 raw_text 抠出 should_reply / reply_text 两个字段。

    救补的判据只有「信封在场」这一条 —— should_reply 与 reply_text 两个键都能捞到。
    不按响应长度或媒体标记设闸门:信封破损与回复大小无关,一条纯文本短回复里多一个
    引号同样能让平衡块扫不出来,而那时落空的代价是整条回复丢失(窗内跟随)或整段
    JSON 被当散文发进群(显式触发),都比一段可能不完整的正文更糟。

    should_reply 必须照抄原文里的布尔值:硬编码 True 会把模型「这条不归我」的
    决定翻成插话,抢答别人的对话比丢一条回复更糟。读不出布尔值时才默认 True
    (reply_text 存在说明模型本来打算说话)。
    """
    if not text or ('"should_reply"' not in text and "'should_reply'" not in text):
        return None
    m = _SALVAGE_REPLY_TEXT_RE.search(text)
    if m is None:
        return None
    raw_val = m.group("v")
    escaped_val = raw_val.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    try:
        val = json.loads(f'"{escaped_val}"')
    except Exception:
        val = raw_val
    # 典型破法:reply_text 以 `![image](…)` 结尾时模型漏掉闭合 `"`,于是 reply_text
    # 正则一路吃到 `,"topic_hint"` 前的那个引号,尾部就多带一个逗号。那是信封的残渣,
    # 不是内容,发到群里只是一个突兀的逗号。
    val = val.rstrip().removesuffix(",").rstrip()

    m_flag = _SALVAGE_SHOULD_REPLY_RE.search(text)
    should_reply = True if m_flag is None else m_flag.group(1).lower() == "true"
    out: dict[str, Any] = {"should_reply": should_reply, "reply_text": val, "_salvaged": True}

    # 另外两个字段也按正则捞回来:它们在原文里通常是完好的,丢掉的话每个带图 turn
    # 都会连带丢掉话题跟踪与退场判断 —— 而带图 turn 恰恰是最常触发救补的那些。
    m_topic = _SALVAGE_TOPIC_HINT_RE.search(text)
    if m_topic:
        out["topic_hint"] = m_topic.group("v")
    m_exit = _SALVAGE_EXIT_ACTIVE_RE.search(text)
    if m_exit:
        out["should_exit_active"] = m_exit.group(1).lower() == "true"
    # M1 收尾(2026-10-17):not_final 也捞——它丢了=回执发出但续跑轮没来=烂尾。
    m_nf = _SALVAGE_NOT_FINAL_RE.search(text)
    if m_nf:
        out["not_final"] = m_nf.group(1).lower() == "true"
    return out


# json5 是纯 Python 解析器,MB 级 payload 要几十秒 CPU,而解析跑在 event loop 里 ——
# 一次内联大图就能把整个 bot 冻住。超过这个尺寸只用 stdlib json(C 实现,同 payload
# 毫秒级)与正则救补,不再交给 json5 兜。
_JSON5_MAX_CHARS = 262_144


def _load_json_candidate(candidate: str) -> Any:
    """按 stdlib json → 转义裸控制字符 → json5(仅小 payload)顺序解析,全失败返回 None。

    stdlib json 优先不只是快:结构化输出在正常路径下就是合法 JSON,json5 只用来
    兜模型的语法脏(单引号 / 尾随逗号 / 裸 key)。
    """
    try:
        return json.loads(candidate)
    except Exception:
        pass
    # 中文模型常把字段分隔/键值冒号吐成全角 ，/:,让 json / json5 双双拒解析。先把
    # **字符串外**的全角结构标点归一成 ASCII(正文里的全角标点原样保留),后续阶梯都用它。
    # 合规输入下归一是 no-op,与旧行为等价。
    normalized = _normalize_fullwidth_json_punct(candidate)
    try:
        return json.loads(normalized)
    except Exception:
        pass
    # LLM 常在 reply_text 里嵌真换行(JSON 不允),转义后 stdlib json 仍能吃下
    escaped = _escape_raw_newlines_in_strings(normalized)
    try:
        return json.loads(escaped)
    except Exception:
        pass
    if len(candidate) > _JSON5_MAX_CHARS:
        logger.warning(
            f"[HERMES] 结构化回复过大({len(candidate)} 字符)且非合法 JSON,跳过 json5 回退以免阻塞 event loop,转正则救补"
        )
        return None
    try:
        return json5.loads(normalized)
    except Exception:
        pass
    try:
        return json5.loads(escaped)
    except Exception:
        return None


# 内联 data URL 摘出/回填。api_server 把 MEDIA: 图片换成 data URL 是在**生成之后**
# 做的,所以一条带图回复的 JSON 是 MB 级,而里面真正的 JSON 结构只有几百字节。
# 先把 payload 换成短 token 再解析:候选串回到 KB 级,解析器阶梯(含 json5)全程可用,
# 不必再靠尺寸阈值放弃 —— 之前跳过 json5 会让带图回复退到正则救补,连带丢掉
# topic_hint / should_exit_active(救补只认 should_reply + reply_text)。
# 阈值 256:短 data URL 本来就不影响解析,只处理真正撑爆候选串的那些。
_DATA_URL_DETACH_RE = re.compile(r"data:[\w.+-]+/[\w.+-]+;base64,[A-Za-z0-9+/=]{256,}")
_DETACH_TOKEN_RE = re.compile(r"⟦hermes-media-(\d+)⟧")


def _detach_data_urls(text: str) -> tuple[str, list[str]]:
    """把长 data URL 替换成 ⟦hermes-media-N⟧,返回 (缩短后的文本, 原始 URL 列表)。"""
    urls: list[str] = []

    def _repl(m: re.Match) -> str:
        urls.append(m.group(0))
        return f"⟦hermes-media-{len(urls) - 1}⟧"

    return _DATA_URL_DETACH_RE.sub(_repl, text), urls


def _reattach_data_urls(value: str, urls: list[str]) -> str:
    """把 ⟦hermes-media-N⟧ 换回原始 data URL。索引越界时原样留着 token。"""

    def _repl(m: re.Match) -> str:
        idx = int(m.group(1))
        return urls[idx] if 0 <= idx < len(urls) else m.group(0)

    return _DETACH_TOKEN_RE.sub(_repl, value)


def _try_parse_first_json_block(text: str) -> dict[str, Any] | None:
    """从模型回复中提取首个 {...} 块并解析。失败返回 None,调用方记 parse_failed。

    流程:摘掉内联 data URL → 提平衡块 → _load_json_candidate(json → json5)
    → 失败则 _salvage_truncated_reply_text 正则抠取 → 把 data URL 回填进字符串字段。
    """
    if not text:
        return None
    shrunk, detached = _detach_data_urls(text)
    parsed: Any = None
    candidate = _find_first_balanced_json_object(shrunk)
    if candidate is not None:
        parsed = _load_json_candidate(candidate)
    if not isinstance(parsed, dict):
        parsed = _salvage_truncated_reply_text(shrunk)
    if parsed is None:
        return None
    if detached:
        parsed = {k: (_reattach_data_urls(v, detached) if isinstance(v, str) else v) for k, v in parsed.items()}
    return parsed


def maybe_extract_decision_reply_text(text: str) -> str | None:
    """passive 路径的防御:如果 raw_text 是 submit_decision 形 JSON,抠出 reply_text。

    场景:同一 Hermes session 之前跑过 reactive 模式,LLM 上下文学到了
    submit_decision 契约;之后切回 passive(active_session 关掉)时,session
    依旧吐 JSON,导致整段 JSON 被当作回复发给用户。

    返回值:
      - 字符串:JSON 是 submit_decision 形且 should_reply=true,返回 reply_text
      - 空串 "":JSON 是 submit_decision 形但 should_reply=false(显式静默)
      - None:不是 submit_decision 形,调用方应继续用原始 raw_text
    """
    parsed = _try_parse_first_json_block(text)
    if parsed is None or "should_reply" not in parsed:
        return None
    if not parsed.get("should_reply"):
        return ""
    rt = parsed.get("reply_text")
    if isinstance(rt, str):
        return rt
    return None


_PERSISTENCE_ERROR_PATTERNS = (
    "session storage could not be written",
    "the turn was stopped because session storage",
    "session_persistence_failed",
)

# 上游把 cause 只写在散文里(结构化的 failure_reason 没有透到 /v1/chat/completions
# 的响应体),所以按各 cause 的特征词分类。三类的可恢复性完全不同:
#   locked  — 别的 Hermes 进程正在写 state.db,上游明说「消息已存下,过一会再发一次」
#   disk    — 磁盘满 / state.db 权限,重发也写不进去
#   unknown — 需要人去跑 hermes doctor
PersistenceCause = Literal["locked", "disk", "unknown"]

_PERSISTENCE_LOCKED_PATTERNS = ("session storage was busy", "another hermes process was writing")
_PERSISTENCE_DISK_PATTERNS = ("free some space", "full disk", "disk full")


# 上游会把 provider 的失败包成 200 + 报错正文当助手回复返回,而不是一个 HTTP 错误码。
# 这段报文于是走到「模型确实说了点什么」的分支,被当成散文发进群 —— 既把内部报错摊给
# 用户,也把报文里那条带时效凭据的媒体 URL 一起泄出去。与持久化中断同属一类:200 的
# 外壳,错误的内容。
_PROVIDER_ERROR_PREFIX_RE = re.compile(r"^\s*HTTP\s+\d{3}\s*:", re.IGNORECASE)
_PROVIDER_ERROR_MARKERS = (
    "error from provider",
    "upstream request failed",
    "invalid_request_error",
)

# 上游 turn_failure_copy 的鉴权失败引导文案 (2026-10-09 实测线上模型 5h 额度 403):
#   "{label} rejected your API key, so the model can't be reached. ...\n\nProvider said: HTTP 403: ..."
# "HTTP 403" 被挪到第二段,老前缀正则失配,报错被当正常回复发进群。
# 认完整固定句式而非锚定开头:该句式只由上游失败路径生成,模型正常回复不会逐字产出。
_PROVIDER_AUTH_COPY_RE = re.compile(
    r"rejected your (?:API key|sign-in), so the model can't be reached", re.IGNORECASE
)

# 本部署补丁: 上游 turn_failure_copy 的「重试耗尽/暂不可用」
# 引导文案家族 (exhausted_copy 全部 lead + reset 变体 + nous_rate_limit):
#   "{label} didn't respond in time on any of {n} attempts — it looks temporarily
#    unavailable. Wait a minute and send /retry, or switch models with /model.
#    To avoid this in future, add a backup provider with `hermes fallback add`.
#    \n\nProvider said: Connection error."
# 认 CLI 指令残片而非 lead 句(lead 带 {label}/{attempts} 模板参数且上游会演进):
# 面向 QQ 的正常回复绝不会教用户敲 hermes CLI。非重试家族(model_not_found /
# format_error / ssl / policy_blocked 等)刻意不覆盖——它们语义不同,维护者未拍板。
_PROVIDER_OUTAGE_COPY_RE = re.compile(
    r"hermes\s+fallback\s+add"
    r"|send\s+/retry[^\n]{0,80}?switch\s+models\s+with\s+/model",
    re.IGNORECASE,
)


def is_provider_outage_text(text: str) -> bool:
    """检测回复是否为上游「provider 暂不可用/重试耗尽」引导文案(200 外壳)。"""
    return bool(text) and bool(_PROVIDER_OUTAGE_COPY_RE.search(text))


def is_provider_error_text(text: str) -> bool:
    """检测回复内容是否为上游透传的 provider 报错(200 外壳 + 错误正文)。

    老形态:前缀与特征词必须同时命中:只认前缀会把正经聊到状态码的回复误伤,只认特征词会把
    讨论报错本身的对话误伤 —— 而误伤的代价是把用户想要的回答换成一句兜底。
    新形态(鉴权引导文案):固定句式直接命中,不再要求特征词。
    本部署补丁(2026-10-11): 暂不可用引导文案家族同理会骗过前两道,直接认 CLI 残片。
    """
    if not text:
        return False
    if _PROVIDER_AUTH_COPY_RE.search(text):
        return True
    if is_provider_outage_text(text):
        return True
    if not _PROVIDER_ERROR_PREFIX_RE.match(text):
        return False
    lower = text.lower()
    return any(marker in lower for marker in _PROVIDER_ERROR_MARKERS)


# 上游在 `compression.enabled: false` 下撞到上下文溢出时会**放弃**:agent/turn_recovery.py
# 禁掉包括会话轮换在内的全部自动恢复路径,只把一句英文提示当作 final_response 返回,而
# api_server 又把它原样塞进 assistant content 以 200 发回 —— 形态与 provider 报错同类,
# 但可恢复性完全不同:那一段历史窗口本身就是病因,换个 session key 就能继续。
#
# 锚点必须落在**开头**:上游那段是整条回复的全文,而解释性的正经回答不会以它开头。
# 只认特征词会把「群友正在聊这个报错」的回答一起误伤,而误伤的代价是清空整条会话。
_CONTEXT_OVERFLOW_ANCHOR_RE = re.compile(r"^\s*Context overflow\b", re.IGNORECASE)
_CONTEXT_OVERFLOW_MARKER = "auto-compaction is disabled"


def is_context_overflow_text(text: str) -> bool:
    """检测回复是否为上游「上下文溢出且自动压缩已关」的放弃提示。"""
    if not text:
        return False
    if not _CONTEXT_OVERFLOW_ANCHOR_RE.match(text):
        return False
    return _CONTEXT_OVERFLOW_MARKER in text.lower()


def rotated_session_key(header_value: str | None, sent_key: str) -> str | None:
    """响应头里的有效 session id;缺失或与请求发出的 key 相同时返回 None(未轮换)。"""
    value = (header_value or "").strip()
    if not value or value == sent_key:
        return None
    return value


# --- 本部署补丁: SSE 流式 + 中间旁白切分 ---

# 旁白段最小长度:低于这个长度不值得发一条群消息(空白/单标点段)。
_INTERIM_MIN_CHARS = 2


class InterimSegmenter:
    """SSE 行流 → 旁白段切分 + 最终回复累积。纯逻辑无网络依赖,可单测。

    实测时序(2026-10-08 对生产 api_server 探测): agent 每轮工具调用前的旁白
    content delta 完整流出之后, 该轮的 hermes.tool.progress(status=running) 帧
    才到达。因此以 running 帧为段界: 帧到时把累积缓冲作为一段旁白吐出; EOS 时
    剩余缓冲即最终回复。无工具的 turn 没有 running 帧, 整段=最终回复,
    与非流式语义一致。

    reasoning_content / hermes.status / approval.request 帧一律不消费;
    tool.progress 的 completed 帧不切分(段在 running 时已吐, completed 时缓冲为空)。
    """

    def __init__(self) -> None:
        self._seg: list[str] = []
        self._event = ""

    def feed(self, line: str) -> str | None:
        """喂一行 SSE 原始行。返回切出的旁白段(strip 后, 达最小长度),否则 None。"""
        if not line or line.startswith(":"):  # 空行(帧界)与 keepalive
            return None
        if line.startswith("event:"):
            self._event = line[6:].strip()
            return None
        if not line.startswith("data:"):
            return None
        data_str = line[5:].strip()
        if data_str == "[DONE]":
            return None
        event, self._event = self._event, ""
        try:
            payload = json.loads(data_str)
        except ValueError:
            return None
        if event == "hermes.tool.progress":
            if isinstance(payload, dict) and payload.get("status") == "running":
                return self._pop_segment()
            return None
        if event:
            # 其余带名事件帧(approval.request / hermes.status 等)一律不消费。
            # approval.request 的 payload 带 choices=["once","session",...](list[str]),
            # 漏到下方会被当 chat chunk 解析, choices[0].get 抛 AttributeError
            # ('str' object has no attribute 'get'), 2026-10-12 实证炸断 SSE。
            return None
        # 普通 chat.completion.chunk(无 event 名): 只取 content delta, reasoning 等丢弃
        choices = payload.get("choices") if isinstance(payload, dict) else None
        if not choices or not isinstance(choices[0], dict):
            return None
        delta = choices[0].get("delta")
        piece = delta.get("content") if isinstance(delta, dict) else None
        if piece:
            self._seg.append(piece)
        return None

    def _pop_segment(self) -> str | None:
        text = "".join(self._seg).strip()
        self._seg.clear()
        return text if len(text) >= _INTERIM_MIN_CHARS else None

    def final_text(self) -> str:
        """EOS 时调用: 剩余缓冲 = 最终回复段(送原解析管线的 raw_text)。"""
        return "".join(self._seg)


# 进程级共享 AsyncClient 连接池复用——原实现每轮/每次探测新建 client,
# 高频群聊下每 turn 一次 TCP+TLS 握手。插件单 event loop,httpx.AsyncClient 并发
# 安全;超时一律请求级传参(与旧 client 级超时同语义)。shutdown 经
# close_shared_http_client()(插件 __init__ 注册)释放。
_shared_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _shared_http_client
    if _shared_http_client is None or _shared_http_client.is_closed:
        _shared_http_client = httpx.AsyncClient()
    return _shared_http_client


async def close_shared_http_client() -> None:
    global _shared_http_client
    if _shared_http_client is not None and not _shared_http_client.is_closed:
        try:
            await _shared_http_client.aclose()
        except Exception:
            pass
    _shared_http_client = None


# 429 重试读 Retry-After——原实现固定 sleep(1),上游改节奏不跟随。
def _parse_retry_after(raw: str | None) -> float | None:
    """Retry-After 头解析:数字秒(上游实测形态),非法/缺失返回 None。钳制 0.5~30s。"""
    if not raw:
        return None
    try:
        val = float(raw.strip())
    except (TypeError, ValueError):
        return None
    return min(30.0, max(0.5, val))


class StreamHttpError(Exception):
    """流式分支的非 200 响应;携带 status 与 body 让主流程走统一错误归纳。

    retry_after 携带响应的 Retry-After 秒数(仅 429 等限流响应有)。
    """

    def __init__(self, status: int, body: str, retry_after: float | None = None) -> None:
        super().__init__(f"stream HTTP {status}")
        self.status = status
        self.body = body
        self.retry_after = retry_after


class RunsEventParser:
    """runs events SSE 行流 → 旁白段 + 终态。纯逻辑无网络依赖,可单测。

    本部署补丁(runs 迁移阶段1)。消费规则按阶段0实测定案:
    - message.interim = 上游切好的旁白段 → 逐段交给调用方(达最小长度,strip);
    - message.delta  = 混有旁白的预览流 → 只计数,**绝不拼正文**(output 才是正文);
    - run.completed → 正文取 output;run.failed → 取 error(已脱敏);
      run.cancelled → interrupted 标记;
    - 其余帧(tool.*/reasoning.*/approval.*/run.steered)与注释帧(: keepalive 等)
      一律不消费。runs 帧是 data-only(事件名在 payload["event"]),event: 行形态
      作兜底兼容。
    """

    def __init__(self) -> None:
        self.output: str | None = None
        self.error: str | None = None
        self.interrupted = False
        self.terminal = False
        self.stream_closed = False
        self.usage: dict[str, Any] = {}
        self.delta_count = 0
        self.last_seq = -1
        self.pending_steer = ""
        self.approval: dict | None = None
        self._event = ""

    def feed(self, line: str) -> str | None:
        """喂一行 SSE 原始行。切出旁白段则返回(strip 后,达最小长度),否则 None。"""
        if not line:
            return None
        if line.startswith(":"):
            # 注释帧:keepalive/open 忽略;stream closed 记录(供调用方判提早断流)
            if "stream closed" in line:
                self.stream_closed = True
            return None
        if line.startswith("event:"):
            self._event = line[6:].strip()
            return None
        if line.startswith("id:"):
            return None
        if not line.startswith("data:"):
            return None
        data_str = line[5:].strip()
        if data_str == "[DONE]":
            return None
        event, self._event = self._event, ""
        try:
            payload = json.loads(data_str)
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        seq = payload.get("seq")
        if isinstance(seq, int):
            self.last_seq = seq
        if not event:
            event = str(payload.get("event") or "")
        if event == "message.interim":
            text = str(payload.get("text") or "").strip()
            return text if len(text) >= _INTERIM_MIN_CHARS else None
        if event == "message.delta":
            self.delta_count += 1
            return None
        if event == "approval.request":
            # 阶段3c:整帧原文暂存(command 上游已脱敏),由消费循环交 approval_callback
            self.approval = payload
            return None
        if event == "run.completed":
            self.output = str(payload.get("output") or "")
            usage = payload.get("usage")
            self.usage = usage if isinstance(usage, dict) else {}
            # 阶段2b:turn 收尾后才到的 steer 上浮在这里(源码:turn_finalizer 漏接的
            # steer 进 result.pending_steer,api_server_runs 透到 completed 帧)
            pending = payload.get("pending_steer")
            self.pending_steer = str(pending) if pending else ""
            self.terminal = True
            return None
        if event == "run.failed":
            self.error = str(payload.get("error") or "unknown")
            self.terminal = True
            return None
        if event == "run.cancelled":
            self.interrupted = True
            self.terminal = True
            return None
        if event == "run.interrupted":
            # 网关关停/owner 失联会把 run 打成
            # interrupted,终态集与网关 TERMINAL_STATUSES 对齐,别落成「事件流提前
            # 关闭」的传输错误文案——按取消同语义硬静默处理。
            self.interrupted = True
            self.terminal = True
            return None
        return None


InterimCallback = Callable[[str], Awaitable[None]]
ApprovalCallback = Callable[[dict], Awaitable[None]]  # 阶段3c:approval.request 帧原文(已脱敏)


def is_persistence_error_text(text: str) -> bool:
    """检测回复内容是否为 Hermes 上游 SQLite session 持久化失败引发的中断报错文本。"""
    if not text:
        return False
    lower = text.lower()
    return any(pattern in lower for pattern in _PERSISTENCE_ERROR_PATTERNS)


def classify_persistence_error(text: str) -> PersistenceCause:
    """把持久化中断文本分到 locked / disk / unknown。只在 is_persistence_error_text 为真时有意义。"""
    lower = (text or "").lower()
    if any(pattern in lower for pattern in _PERSISTENCE_LOCKED_PATTERNS):
        return "locked"
    if any(pattern in lower for pattern in _PERSISTENCE_DISK_PATTERNS):
        return "disk"
    return "unknown"


@dataclass
class ChatResult:
    raw_text: str
    structured: dict[str, Any] | None = None
    media_urls: list[str] = field(default_factory=list)
    file_paths: list[str] = field(default_factory=list)
    """本部署补丁(2026-10-08): 回复里引用到的本地文件路径(MEDIA:/path 等),
    供 handler 层 send_local_files 上传文件本体。白名单判定在发送侧。"""
    parse_failed: bool = False
    """期望结构化输出但解析失败(JSON 提取不到 / json5 解析报错 / 非 dict 类型)。"""

    is_transport_error: bool = False
    """HTTP 失败(非 200 / timeout / connect error / 其他异常)。

    handler 据此决定:transport_error → 可重试或对用户报错;parse_failed →
    通常静默降级(模型回了不可解析的内容);两者**不互斥**(transport_error
    场景下 parse_failed 也设 True 以阻止 caller 误把 raw_text 当模型有效输出)。
    """

    is_forbidden_error: bool = False
    """本部署补丁: 403 鉴权类错误硬标记。

    覆盖三种来源:api_server HTTP 403(key 无效/缺失)、502 包裹的内层 403
    (provider 侧 403 经上游包装)、200 包裹的 provider 报错文本含 HTTP 403。
    handler 层对此标记一律静默(不发 fallback、不发 raw_text),不依赖
    hermes_transport_error_fallback_text 配置——403 是运维事件(key/额度),
    群里通知无意义,上游持续故障时每条触发一条通知就是刷屏。
    """

    is_outage_error: bool = False
    """本部署补丁: provider 暂不可用/重试耗尽硬标记。

    来源:200 包裹的上游 exhausted_copy 引导文案("didn't respond in time ...
    Provider said: Connection error." 等家族)。与 is_forbidden_error 同系统:
    handler 层一律静默——provider 暂时挂了是运维事件,群里发英文 CLI 引导文案
    无意义,故障期每条触发刷一条就是刷屏。
    """

    is_persistence_error: bool = False
    """上游 Hermes session DB 持久化失败(session_persistence_failed)。

    Hermes Gateway 在持久化失败时仍返回 HTTP 200,但 content 是中断解释文本。
    置 True 让 handler 能识别并按 persistence_cause 决定重试还是静默屏蔽,
    而不是把上游报错原文丢进群里。
    """

    pending_steer: str = ""
    """runs 路限定(阶段2b):turn 收尾后才到达的 steer 文本(上游 turn_finalizer
    漏接后上浮)。非空时 handler 应把它作为跟进消息排回 pending refire,
    不能丢——那是用户在 turn 边界上说的话。completions 路恒空。"""

    is_rate_limited: bool = False
    """HTTP 429 满额丢单(阶段2d)。自动重试一次仍 429 才打此标记;
    handler 据此「丢了就贴」:群聊在触发消息上贴 emoji 不发文本,私聊文本兜底。"""

    is_interrupted: bool = False
    """runs turn 被取消/中断(阶段3b, /run-stop 或 gateway 关停)。
    handler 硬静默:终止是管理员的主动动作,命令回执已说明,不再发错误兜底文本。"""

    is_context_overflow: bool = False
    """上游上下文溢出,且它那边关掉了自动压缩(compression.enabled: false)。

    上游此时禁掉包括会话轮换在内的全部自动恢复,同样以 HTTP 200 回一段英文提示。
    窗口只涨不缩,不换 session key 的话这条会话从此每轮都失败 —— handler 据此
    重置会话再重发一次,见 _chat_with_overflow_recovery。
    """

    effective_session_key: str | None = None
    """上游本轮实际使用的 session id,**仅在与请求发出的 key 不同时**有值。

    Hermes 自动压缩上下文时会轮换会话:旧 id 被 end_reason='compression' 关闭,
    新建 continuation 子会话,新 id 走响应头 X-Hermes-Session-Id 回传。不采纳的话
    下一轮又钉回已关闭的父会话——读还能跟随 tip,写全部失败,且每次压缩再分叉一个
    兄弟会话,直到 live 子会话不止一个,上游血缘恢复判定歧义后永久写不进去。
    """

    persistence_cause: PersistenceCause | None = None
    """持久化失败的类别,仅 is_persistence_error=True 时有值。

    只有 locked(别的进程正在写库)是瞬时可恢复的 —— 上游对这一类明确要求
    「过一会再发一次」;disk / unknown 重发也写不进去,重试只是浪费一次 agent turn。
    """


# 上游 /v1/capabilities 里与长期记忆作用域相关的能力键(见 api_server 的 features 块)。
_MEMORY_CAPABILITY_KEYS = ("session_key_header",)


def missing_memory_capabilities(caps: dict[str, Any]) -> list[str]:
    """返回 caps 中缺失的长期记忆能力键。

    上游把能力放在 {"features": {...}} 下;宽容处理平铺形状,避免因为
    响应外壳变化就误报"上游不支持"。
    """
    features = caps.get("features")
    if not isinstance(features, dict):
        features = caps
    return [key for key in _MEMORY_CAPABILITY_KEYS if not features.get(key)]


class HermesClient:
    def __init__(self) -> None:
        # 无实例状态:接入点按群解析,缓存会让路由与配置漂移。
        pass

    @property
    def api_url(self) -> str:
        """默认接入点的 base URL(日志、启动期能力探测用)。"""
        return default_target().base_url

    @property
    def api_key(self) -> str:
        return default_target().api_key

    @property
    def timeout(self) -> int:
        return default_target().timeout

    def get_headers(
        self,
        session_key: str = "",
        memory_key: str | None = None,
        *,
        api_key: str | None = None,
    ) -> dict[str, str]:
        """拼装出向 header。

        X-Hermes-Session-Id  = 短期 transcript 作用域(每轮必发)
        X-Hermes-Session-Key = 长期记忆作用域(仅在调用方给了值且本轮有 api_key 时发)

        没有 api_key 却发记忆头会被上游 403 整轮打回,所以这里与 Authorization
        绑在同一个条件上。

        api_key=None 表示用默认接入点的 key;按群路由时由调用方传入该群接入点的 key。
        """
        key = self.api_key if api_key is None else api_key
        h = {"Content-Type": "application/json", "X-Hermes-Session-Id": session_key}
        if key:
            h["Authorization"] = f"Bearer {key}"
            if memory_key:
                h["X-Hermes-Session-Key"] = memory_key
        return h

    async def chat(
        self,
        *,
        text: str,
        image_urls: list[str] | None = None,
        session_key: str,
        memory_key: str | None = None,
        user_id: str,
        group_id: str | None,
        adapter_name: str,
        is_private: bool,
        mode: Literal["reactive", "passive"] = "passive",
        expect_structured: bool = False,
        structured_tool_name: str | None = None,
        system_prompt: str | None = None,
        user_content_override: UserContent | None = None,
        target: HermesTarget | None = None,
        interim_callback: InterimCallback | None = None,
        bot_self_id: str | None = None,
        approval_callback: "ApprovalCallback | None" = None,
    ) -> ChatResult:
        """调用 Hermes,返回 ChatResult。

        - mode='passive': 普通文本回复(不强制结构化)
        - mode='reactive' + expect_structured=True + structured_tool_name='submit_decision':
          system prompt 追加 STRUCTURED OUTPUT 段,期望模型回复纯 JSON;解析失败 parse_failed=True
        - system_prompt: 由 prompt_builder 注入;None 走默认 Message Context 拼装。
          **注意**:外部传入 system_prompt 时,Platform/User/Group 上下文须由调用方
          自行包含,本方法不会再额外补。
        - memory_key: 长期记忆作用域(X-Hermes-Session-Key)。None = 不发该头。
          由 SessionManager.get_memory_key() 产出,与 session_key 相互独立:后者会随
          /clear 与上游压缩轮换,前者不会。
        - user_content_override: 由 prompt_builder 直接给出 user message 的 content
          (str 或 OpenAI 多模态 parts 列表;text + image_urls 参数将被忽略)
        - target: 本轮发往的接入点(url + key + timeout)。None = 默认接入点。
          按群路由时由 handler 经 routing.resolve_target() 解析后传入。
        - mode 字段当前为路由元数据,chat() 内部不分支判断;Task 15 handler 据此决定
          如何呈现结果(reactive 走 structured 流,passive 走 raw_text)。
        - interim_callback(本部署补丁): 非 None 时改走 SSE 流式,
          agent 每轮工具调用前的旁白段实时回调给它(发送失败仅记日志,不影响主 turn);
          raw_text 只含最终回复段。None = 旧行为(一次性非流式)。
        """
        tgt = target or default_target()
        url = f"{tgt.base_url}/v1/chat/completions"

        if user_content_override is not None:
            content: Any = user_content_override
        else:
            cur_imgs = image_urls or []
            if not cur_imgs:
                content = text
            else:
                parts: list[dict[str, Any]] = [{"type": "text", "text": text}]
                for u in cur_imgs:
                    parts.append({"type": "image_url", "image_url": {"url": u}})
                content = parts

        if system_prompt is None:
            # 头段单一拼装处:core/message_context.py(2026-10-16 同源化收口批,
            # 审计确认7);与 prompt_builder.build_passive_system_prompt 同源,
            # 字节一致由代码同源保证(原先靠测试互锁)。
            system_prompt = render_message_context_head(
                adapter=adapter_name,
                is_private=is_private,
                user_id=user_id,
                group_id=group_id,
            )

        if expect_structured and structured_tool_name == "submit_decision":
            system_prompt = system_prompt + _DECISION_HINT

        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": content},
        ]

        payload: dict[str, Any] = {
            "model": "hermes-agent",
            "messages": messages,
            "stream": interim_callback is not None,
        }
        # 路径 B:不发 tools / tool_choice(Hermes 透传不可靠,P0-spike 已验)

        headers = self.get_headers(session_key, memory_key, api_key=tgt.api_key)
        runs_pending_steer = ""

        # 低危⑧(2026-10-14):幂等键每轮只造一次,429 自动重试与提交段网络重试都
        # 复用同一键——网关 lost-acceptance 重放只认同键同指纹,每试一次换键等于
        # 名不副实,还会在提交应答丢失时放大出孤儿 run。
        turn_idem_key = f"turn-{uuid.uuid4().hex}"

        async def _do_transport() -> tuple[str, str | None, "ChatResult | None", str]:
            """三分支统一出口 (raw_text, rotated, failure, pending_steer)。

            非 2xx 一律抛 StreamHttpError(含非流式分支——原本直接 return
            _transport_failure),让 429 自动重试只写一个点(阶段2d)。
            """
            if plugin_config.hermes_runs_channel_enabled:
                # 本部署补丁(runs 迁移阶段1): 开关开启时整路
                # 改走 /v1/runs;下游持久化/溢出/provider 文案检测与 JSON 解析完全复用。
                return await self._chat_via_runs(
                    tgt=tgt, headers=headers, content=content,
                    system_prompt=system_prompt, session_key=session_key,
                    timeout=tgt.timeout, interim_callback=interim_callback,
                    approval_callback=approval_callback,
                    idempotency_key=turn_idem_key,
                    scope=dict(
                        adapter=adapter_name, is_private=is_private, user_id=user_id,
                        group_id=group_id, bot_self_id=bot_self_id or "",
                        mode=mode, expect_structured=expect_structured,
                    ),
                )
            if interim_callback is not None:
                # 本部署补丁: SSE 流式读取,旁白段实时回调,
                # raw_text 只取最终回复段(旁白不进原解析管线)。
                raw_text, rotated = await self._stream_chat(
                    url, payload, headers, tgt.timeout, session_key, interim_callback
                )
                return raw_text, rotated, None, ""
            client = _get_http_client()
            resp = await client.post(url, json=payload, headers=headers, timeout=tgt.timeout)
            if resp.status_code != 200:
                raise StreamHttpError(resp.status_code, resp.text, retry_after=_parse_retry_after(resp.headers.get("Retry-After")))
            data = resp.json()
            rotated = rotated_session_key(resp.headers.get("X-Hermes-Session-Id"), session_key)
            if not (data.get("choices") or []):
                # choices 为空是结构性失败,与 expect_structured 无关——原实现
                # 只在 expect_structured=True 时打标,False 时空串被当正常回复吞掉。
                logger.warning(
                    f"[HERMES] upstream returned empty choices (expect_structured={expect_structured})"
                )
                return "", rotated, ChatResult(
                    raw_text="", parse_failed=True,
                ), ""
            msg = (data.get("choices") or [{}])[0].get("message") or {}
            # 注意:本变量绝不可命名为 content——content 是外层 chat() 的局部,
            # 闭包内任何对该名的赋值都会让全函数域的 content 读点(runs/流式分支)
            # 变成 UnboundLocalError(有生产事故先例)。
            final_content = msg.get("content") or ""
            if not final_content:
                # content 空同样留痕(行为不变,仅观测)——上游异常与模型静默
                # 此前不可区分。
                logger.warning(
                    f"[HERMES] upstream returned empty content (expect_structured={expect_structured})"
                )
            return final_content, rotated, None, ""

        # 阶段2d(维护者定案「丢了就贴」前置):429 满额先自动重试一次(Retry-After: 1s),
        # 再 429 才归纳上抛——贴表情是「重试也进不去」的兜底,不是一见 429 就躺。
        _rl_retried = False
        while True:
            try:
                raw_text, rotated, runs_failure, runs_pending_steer = await _do_transport()
                break
            except StreamHttpError as exc:
                if exc.status == 429 and not _rl_retried:
                    _rl_retried = True
                    # 优先跟随上游 Retry-After,缺失回落 1s 旧值。
                    delay = exc.retry_after if exc.retry_after is not None else 1.0
                    logger.warning(f"[HERMES] 429 满额,{delay:g}s 后自动重试一次")
                    await asyncio.sleep(delay)
                    continue
                # 流式分支的非 200:与非流式同一套归纳(含 403 硬标记),保证错误面一致。
                return _transport_failure(exc.status, exc.body)
            except httpx.TimeoutException:
                logger.error(f"[HERMES] API 请求超时 ({tgt.timeout}s, target={tgt.label})")
                return ChatResult(
                    raw_text="⚠️ AI 服务响应超时,请稍后重试",
                    parse_failed=True,
                    is_transport_error=True,
                )
            except httpx.ConnectError:
                logger.error(f"[HERMES] 无法连接到 {tgt.base_url} (target={tgt.label})")
                return ChatResult(
                    raw_text="⚠️ 无法连接到 AI 服务",
                    parse_failed=True,
                    is_transport_error=True,
                )
            except Exception as exc:
                logger.error(f"[HERMES] API 请求异常: {exc}", exc_info=True)
                return ChatResult(
                    raw_text=f"⚠️ AI 服务异常: {exc}",
                    parse_failed=True,
                    is_transport_error=True,
                )
        if runs_failure is not None:
            # 空 choices(非流式)等 completions 侧失败也要带回轮换 id;runs 侧 rotated
            # 恒 None,挂上无害。
            runs_failure.effective_session_key = rotated
            return runs_failure

        def _out(result: ChatResult) -> ChatResult:
            """给本轮所有出口统一挂上有效 session id(轮换才有值)。"""
            result.effective_session_key = rotated
            if runs_pending_steer:
                result.pending_steer = runs_pending_steer
            return result

        # 检查是否为上游持久化失败错误(Hermes 返回 200 但 content 为 Session DB 错误提示)
        if is_persistence_error_text(raw_text):
            cause = classify_persistence_error(raw_text)
            preview = preview_raw(raw_text, head=200, tail=0)
            logger.warning(
                f"[HERMES] 捕获到上游 Session 持久化失败中断提示 "
                f"(cause={cause} raw_len={len(raw_text)} preview={preview!r})"
            )
            return _out(
                ChatResult(
                    raw_text=raw_text,
                    parse_failed=True,
                    is_transport_error=True,
                    is_persistence_error=True,
                    persistence_cause=cause,
                )
            )

        if is_context_overflow_text(raw_text):
            preview = preview_raw(raw_text, head=200, tail=0)
            logger.warning(
                f"[HERMES] 上游上下文溢出且自动压缩已关,本轮被放弃 (raw_len={len(raw_text)} preview={preview!r})"
            )
            return _out(
                ChatResult(
                    raw_text=raw_text,
                    parse_failed=True,
                    is_transport_error=True,
                    is_context_overflow=True,
                )
            )

        if is_provider_error_text(raw_text):
            preview = preview_raw(raw_text, head=200, tail=0)
            logger.warning(
                f"[HERMES] 上游以 200 返回了 provider 报错,按传输失败处理 (raw_len={len(raw_text)} preview={preview!r})"
            )
            # 本部署补丁(2026-10-08): 文本形态 "HTTP 403: ..." 的 provider 鉴权错误
            # 同样打硬标记,handler 硬静默。
            # 2026-10-09 扩展:引导文案形态 ("...rejected your API key... Provider said: HTTP 403: ...")
            # 的 403 不在开头,此处已认定是 provider 报错,全文搜 "HTTP 403" 即可,无误伤面。
            forbidden = bool(
                re.match(r"^\s*HTTP\s+403\b", raw_text, re.IGNORECASE)
                or re.search(r"\bHTTP\s+403\b", raw_text, re.IGNORECASE)
            )
            # 本部署补丁(2026-10-11): 暂不可用引导文案家族打独立硬标记,
            # handler 与 403 同口径硬静默。
            outage = is_provider_outage_text(raw_text)
            return _out(ChatResult(
                raw_text=raw_text, parse_failed=True, is_transport_error=True,
                is_forbidden_error=forbidden,
                is_outage_error=outage,
            ))

        # 路径 B:从 raw_text 提取首个 {...} JSON5 块
        if expect_structured and structured_tool_name == "submit_decision":
            structured = _try_parse_first_json_block(raw_text)
            if structured is None:
                preview = preview_raw(raw_text)
                logger.warning(
                    f"[HERMES] 路径 B 未能从回复中解析出 JSON 块 (raw_len={len(raw_text)} preview={preview!r})"
                )
                return _out(ChatResult(raw_text=raw_text, parse_failed=True))
            return _out(ChatResult(raw_text=raw_text, structured=structured))

        # 普通文本路径(passive 或未要求结构化)
        cleaned, media_urls, file_paths = extract_response_media(raw_text)
        return _out(ChatResult(raw_text=cleaned, media_urls=media_urls, file_paths=file_paths))

    async def _stream_chat(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str],
        timeout: int,
        session_key: str,
        interim_callback: InterimCallback,
    ) -> tuple[str, str | None]:
        """SSE 流式读取(本部署补丁)。返回 (最终回复段, rotated_session_key)。

        旁白段经 InterimSegmenter 切出后逐段 await interim_callback;回调失败仅记
        WARNING(旁白是附属输出,发不出去不能搞砸主 turn)。

        错误面与 chat() 非流式分支对齐:非 200 抛 StreamHttpError(由调用方归纳),
        超时/连接/其他异常原样向上抛(chat() 的 except 链统一接管)。
        SSE keepalive 帧保证长 turn 中 read timeout 不会误杀(上游按
        CHAT_COMPLETIONS_SSE_KEEPALIVE_SECONDS 周期发 `: keepalive`)。
        """
        seg = InterimSegmenter()
        rotated: str | None = None
        client = _get_http_client()
        async with client.stream("POST", url, json=payload, headers=headers, timeout=timeout) as resp:
            if resp.status_code != 200:
                body = (await resp.aread()).decode("utf-8", "replace")
                raise StreamHttpError(resp.status_code, body, retry_after=_parse_retry_after(resp.headers.get("Retry-After")))
            rotated = rotated_session_key(resp.headers.get("X-Hermes-Session-Id"), session_key)
            async for line in resp.aiter_lines():
                interim = seg.feed(line)
                if interim is None:
                    continue
                try:
                    await interim_callback(interim)
                except Exception:
                    logger.warning("[HERMES] 旁白转发失败(不影响主 turn)", exc_info=True)
        # 流式下上游不做 _resolve_media_to_data_urls(只在非流式响应路径),
        # 本地图片在这里按插件白名单补内联, 让 raw_text 形态与非流式一致(2026-10-08 补丁)。
        return inline_local_media_images(seg.final_text()), rotated

    async def respond_approval(self, *, tgt: HermesTarget, run_id: str, choice: str) -> tuple[bool, str]:
        """POST /v1/runs/{id}/approval 应答挂起审批(阶段3c)。返回 (成功, 说明)。

        choice 合法集(API key 路径):once / session / always / deny;
        approve/approved/allow 上游别名映射到 once。409 = 无挂中审批(已答/已超时)。
        """
        try:
            client = _get_http_client()
            resp = await client.post(
                f"{tgt.base_url}/v1/runs/{run_id}/approval",
                json={"choice": choice},
                headers=self.get_headers(api_key=tgt.api_key),
                timeout=15,
            )
            if resp.status_code == 200:
                return True, str(resp.json().get("choice") or choice)
            try:
                detail = str(resp.json().get("error", {}).get("message") or resp.text)[:200]
            except ValueError:
                detail = resp.text[:200]
            return False, f"HTTP {resp.status_code}: {detail}"
        except Exception as exc:
            return False, f"请求异常: {exc}"

    async def get_run_status(self, *, tgt: HermesTarget, run_id: str) -> dict | None:
        """GET /v1/runs/{id} 取 run 状态(阶段3a),失败返回 None。"""
        try:
            client = _get_http_client()
            resp = await client.get(
                f"{tgt.base_url}/v1/runs/{run_id}",
                headers=self.get_headers(api_key=tgt.api_key),
                timeout=15,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    async def stop_run(self, *, tgt: HermesTarget, run_id: str) -> bool:
        """POST /v1/runs/{id}/stop 协作式终止(阶段3b),返回是否被受理(2xx)。"""
        try:
            client = _get_http_client()
            resp = await client.post(
                f"{tgt.base_url}/v1/runs/{run_id}/stop",
                json={},
                headers=self.get_headers(api_key=tgt.api_key),
                timeout=15,
            )
            return resp.status_code in (200, 202)
        except Exception:
            return False

    async def steer_run(self, *, tgt: HermesTarget, run_id: str, text: str) -> bool:
        """POST /v1/runs/{id}/steer 注入文本(阶段2b)。返回是否被接受。

        接受语义(上游 api_server_runs._handle_steer_run):run 在 running 且 agent
        支持 steer → 排队到 _pending_steer,当前工具批次后作为独立 user 行投递;
        run 不在 running(收尾/终态)→ 409 不接受,调用方回退 pending 排队语义。
        任何网络/协议异常都归 False,调用方走同一回退,不区分原因。
        """
        headers = self.get_headers(api_key=tgt.api_key)
        try:
            client = _get_http_client()
            resp = await client.post(
                f"{tgt.base_url}/v1/runs/{run_id}/steer",
                json={"input": text},
                headers=headers,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.debug(f"[HERMES] steer 被拒 run_id={run_id} http={resp.status_code}")
                return False
            return bool(resp.json().get("accepted"))
        except Exception as exc:
            logger.debug(f"[HERMES] steer 请求失败 run_id={run_id}: {exc}")
            return False

    async def _submit_run_with_replay(
        self, client: httpx.AsyncClient, tgt: HermesTarget, body: dict, submit_headers: dict, timeout: int
    ):
        """POST /v1/runs 带一次网络类重试(低危⑧, 2026-10-14)。

        同一 Idempotency-Key 下重试是安全的:首次若已被网关接纳但应答丢失
        (lost-acceptance),重试命中网关幂等库,原样 202 归还同一 run_id
        (replayed=true),不会重复开 run;首次若根本没到达,重试即正常新建。
        非 2xx(含 429 满额)不在此重试——429 的统一重试在 chat() 层。
        """
        for attempt in (1, 2):
            try:
                return await client.post(f"{tgt.base_url}/v1/runs", json=body, headers=submit_headers, timeout=timeout)
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                if attempt == 2:
                    raise
                logger.warning(
                    f"[HERMES] runs 提交网络异常({type(exc).__name__}),同幂等键重试一次"
                )

    async def _stop_run_quiet(self, base_url: str, headers: dict[str, str], run_id: str) -> None:
        """尽力 stop 一个 run(超时/异常止损用),任何失败只记日志。

        本部署补丁(runs 迁移阶段1):旧路中断=断连接,runs 路
        中断必须显式 stop,否则孤儿 run 在 gateway 继续占槽跑完。"""
        try:
            client = _get_http_client()
            await client.post(f"{base_url}/v1/runs/{run_id}/stop", json={}, headers=headers, timeout=10)
        except Exception:
            logger.warning(f"[HERMES] runs stop 止损失败 run_id={run_id}(孤儿 run 将自行跑完)", exc_info=True)

    async def _chat_via_runs(
        self,
        *,
        tgt: HermesTarget,
        headers: dict[str, str],
        content: Any,
        system_prompt: str,
        session_key: str,
        timeout: int,
        interim_callback: InterimCallback | None,
        approval_callback: "ApprovalCallback | None",
        idempotency_key: str,
        scope: dict[str, Any],
    ) -> tuple[str, str | None, "ChatResult | None", str]:
        """runs 通道(本部署补丁:阶段1)。返回 (raw_text, rotated, failure)。

        - rotated 恒 None:压缩轮换由上游 _resolve_live_session_id 服务端透明处理,
          插件不再手动 adopt;
        - failure 非 None 时 chat() 直接返回该 ChatResult(run.failed / run.cancelled /
          事件流提早关闭),completed 时 failure=None,raw_text 走下游统一解析;
        - 提交带 Idempotency-Key(每轮 chat 一个 key,由调用方造好传入;429 重试与
          提交段网络重试全程复用):网络抖动重交=网关重放同一 run,不重复开;
        - 错误面与流式分支对齐:提交/订阅非 2xx 抛 StreamHttpError,超时/连接/其他
          异常向上抛(chat() except 链统一接管),抛出前对 run 发 stop 止损;
        - 图片等 content parts 原样透传(2026-10-13 实测 runs 转发给 provider)。
        """
        body = {
            "input": [{"role": "user", "content": content}],
            "instructions": system_prompt,
            "session_id": session_key,
            "model": "hermes-agent",
        }
        submit_headers = dict(headers)
        submit_headers["Idempotency-Key"] = idempotency_key
        parser = RunsEventParser()
        run_id = ""
        tracker = None
        try:
            client = _get_http_client()
            resp = await self._submit_run_with_replay(client, tgt, body, submit_headers, timeout)
            if resp.status_code not in (200, 202):
                raise StreamHttpError(resp.status_code, resp.text, retry_after=_parse_retry_after(resp.headers.get("Retry-After")))
            run_id = str(resp.json().get("run_id") or "")
            if resp.json().get("replayed"):
                # 同键重放认领:首次提交的应答丢过,网关把原 run 还回来了
                logger.warning(f"[HERMES] runs 提交重放认领 (run_id={run_id})")
            if not run_id:
                raise StreamHttpError(202, f"missing run_id: {resp.text[:200]}")
            # 阶段2a:登记在跑 run(nb 重启后 run_resume 按此行 reattach 续跑)。
            # bot_self_id 缺失(旧调用点未传)时不登记——resume 找不到 bot 投不出去,
            # 登记了也是死行;缺 id 只发生在非 handler 直调的探测场景。
            from .. import mcp as _mcp  # 延迟导入防环(mcp 装配在 handlers 之后)

            tracker = _mcp.run_tracker
            if tracker is not None and scope.get("bot_self_id"):
                tracker.upsert(
                    run_id=run_id,
                    scope_key=make_scope_key(
                        scope["adapter"], scope["is_private"], scope["user_id"], scope["group_id"]
                    ),
                    adapter=scope["adapter"],
                    is_private=scope["is_private"],
                    user_id=scope["user_id"],
                    group_id=scope["group_id"],
                    bot_self_id=scope["bot_self_id"],
                    session_key=session_key,
                    mode=scope["mode"],
                    expect_structured=bool(scope["expect_structured"]),
                )
            # 订阅略早于执行器注册是上游支持的 race 窗口(events 处理器内置等待)
            async with client.stream(
                "GET", f"{tgt.base_url}/v1/runs/{run_id}/events", headers=headers, timeout=timeout
            ) as ev:
                if ev.status_code != 200:
                    raise StreamHttpError(ev.status_code, (await ev.aread()).decode("utf-8", "replace"), retry_after=_parse_retry_after(ev.headers.get("Retry-After")))
                async for line in ev.aiter_lines():
                    interim = parser.feed(line)
                    if interim and interim_callback is not None:
                        try:
                            await interim_callback(interim)
                        except Exception:
                            logger.warning("[HERMES] 旁白转发失败(不影响主 turn)", exc_info=True)
                    # 阶段3c:审批挂起帧即时上交(command 已脱敏;回调失败不拖垮主 turn)
                    if parser.approval is not None and approval_callback is not None:
                        approval, parser.approval = parser.approval, None
                        try:
                            await approval_callback(approval)
                        except Exception:
                            logger.warning("[HERMES] 审批通知回调失败(不影响主 turn)", exc_info=True)
                    # 粗粒度写库:旁白帧与终态帧才更新 last_seq(delta 高频不写)
                    if tracker is not None and parser.last_seq >= 0 and (interim or parser.terminal):
                        tracker.update_seq(run_id, parser.last_seq)
                    if parser.terminal:
                        break
        except Exception:
            # 超时/连接/解析任何异常:孤儿 run 止损后原样上抛,chat() except 链归纳
            if run_id:
                await self._stop_run_quiet(tgt.base_url, headers, run_id)
            if tracker is not None:
                tracker.finish(run_id)
            raise

        def _finish() -> None:
            if tracker is not None:
                tracker.finish(run_id)

        if parser.interrupted:
            logger.warning(f"[HERMES] runs turn 被中断 (run_id={run_id})")
            _finish()
            return "", None, ChatResult(
                raw_text="⚠️ AI 服务响应被中断,请稍后重试",
                parse_failed=True,
                is_transport_error=True,
                is_interrupted=True,
            ), ""
        if parser.error is not None:
            # run.failed:error 已脱敏。403 硬标记与 provider 报错同口径(handler 硬静默)
            logger.warning(f"[HERMES] runs turn 失败 (run_id={run_id} error={parser.error[:200]!r})")
            forbidden = bool(re.search(r"\bHTTP\s+403\b", parser.error, re.IGNORECASE))
            _finish()
            return "", None, ChatResult(
                raw_text=parser.error,
                parse_failed=True,
                is_transport_error=True,
                is_forbidden_error=forbidden,
                is_outage_error=is_provider_outage_text(parser.error),
            ), ""
        if not parser.terminal:
            logger.warning(f"[HERMES] runs 事件流未见终态帧即关闭 (run_id={run_id})")
            _finish()
            return "", None, ChatResult(
                raw_text="⚠️ AI 服务响应中断(事件流提前关闭),请稍后重试",
                parse_failed=True,
                is_transport_error=True,
            ), ""
        # completed:与流式分支同形补内联,rotated=None(轮换上游服务端处理)
        _finish()
        return inline_local_media_images(parser.output or ""), None, None, parser.pending_steer

    async def fetch_capabilities(self) -> dict[str, Any]:
        """取 /v1/capabilities;任何失败都返回空 dict —— 探测不能阻塞启动。"""
        try:
            client = _get_http_client()
            resp = await client.get(f"{self.api_url}/v1/capabilities", headers=self.get_headers(), timeout=5)
            if resp.status_code != 200:
                return {}
            data = resp.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    async def health_check(self, target: HermesTarget | None = None) -> bool:
        """探活一个接入点。target=None 探默认接入点。

        探 /v1/models 而不是 /health:前者要鉴权,所以 key 配错会如实报 401 ——
        这正是"命名 profile 的 key 填错 / 忘开 multiplex"能被看见的原因。
        """
        tgt = target or default_target()
        try:
            headers = self.get_headers(api_key=tgt.api_key)
            client = _get_http_client()
            resp = await client.get(f"{tgt.base_url}/v1/models", headers=headers, timeout=5)
            return resp.status_code == 200
        except Exception:
            return False


hermes_client = HermesClient()
