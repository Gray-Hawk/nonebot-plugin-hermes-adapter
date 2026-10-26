"""AstrBot 原生 chat API 的轻量客户端。"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass

import httpx

_NO_LABEL = "无独立标签"
_ORDINAL_HEADER = "[图片序数对照 按上传顺序]"
_ORDINAL_SEP = "\uff1d"
# prompt_builder 产出的独立媒体标签形态（parts 里紧贴 image_url 的那一行）：
# 单帧带号 `[图 m:ID#IDX]` / `[表情包 m:ID#IDX]`、无号表情包 `[表情包]`，
# 多帧 `[动图|表情包 (m:ID#IDX )第i/N帧,同一动图按时间顺序]`。
# 只认整段 fullmatch：正文尾行、形似但不合法的方括号串一律算「无独立标签」。
_MEDIA_LABEL_RE = re.compile(
    r"\[(?:"
    r"(?:图|表情包) m:\d+#\d+"
    r"|表情包"
    r"|(?:动图|表情包)(?: m:\d+#\d+)? 第\d+/\d+帧[,，]同一动图按时间顺序"
    r")\]"
)
_TOOL_CHAIN_TYPES = frozenset({"tool_call", "tool_call_result"})
_DATA_URL_RE = re.compile(
    r"\Adata:(image/[A-Za-z0-9][A-Za-z0-9.+-]*);base64,([A-Za-z0-9+/]+={0,2})\Z"
)


def build_prompt_envelope(system_prompt: str, user_text: str) -> str:
    """把提示词与用户正文封装为 Star 桥可识别的唯一 JSON 消息。

    信封只承载文本；媒体一律走 chat 消息段里的 attachment_id。
    """
    if not isinstance(system_prompt, str) or not isinstance(user_text, str):
        raise AstrBotProtocolError("AstrBot 信封只接受文本，媒体请走 attachment 消息段")
    return json.dumps(
        {
            "normi_prompt_bridge": {
                "version": 1,
                "system": system_prompt,
                "user": user_text,
            }
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def build_data_url(data: bytes, mime: str) -> str:
    """把下载到的图片字节重新编码成下游约定的 data URL 形态。"""
    return f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}"


def encode_session_id(session_key: str) -> str:
    """把内部会话键映射为只含 URL 安全字符的确定性会话 id。"""
    encoded = base64.urlsafe_b64encode(session_key.encode("utf-8")).decode("ascii")
    return "nph-" + encoded.rstrip("=")


class AstrBotProtocolError(Exception):
    """上游返回了无法安全解释为完整对话结果的协议数据。"""


class AstrBotLocalContentError(AstrBotProtocolError):
    """本轮内容在发出任何 HTTP 请求前就被本地契约拒绝。

    与传输故障严格区分：上游从未收到本轮内容，调用方不得把它当网络错误重试，
    也不得推进增量水位或写回复冷却。
    """


@dataclass(frozen=True)
class ContentImage:
    """从 OpenAI content 里抽出的单张图片（字节与 MIME 分离，正文不含 base64）。"""

    mime: str
    data: bytes


@dataclass(frozen=True)
class NormalizedContent:
    """归一化后的一轮用户内容：纯文本正文 + 按出现顺序排列的图片。"""

    text: str
    images: tuple[ContentImage, ...] = ()


def normalize_user_content(content: object) -> NormalizedContent:
    """把 OpenAI 形态 user content 归一成 (正文, 图片列表)。

    ``str`` 原样返回。列表只接受 text 与 image_url 两种 part，图片只接受
    合法的 ``data:image/...;base64,...``；不合契约的内容在发 HTTP 前以
    ``AstrBotLocalContentError`` 结束。

    AstrBot 侧把一轮输入摊平成「一整段 prompt + 一个扁平图片列表」，
    原图标签与图片的相邻关系会丢；因此有图时在正文末尾追加一段按上传顺序
    的序数对照（第 N 张＝紧邻的原图标签），原文本一字不动，base64 不进正文。
    """
    if isinstance(content, str):
        return NormalizedContent(text=content)
    if not isinstance(content, (list, tuple)):
        raise AstrBotLocalContentError("AstrBot 后端不接受非文本 user content")
    texts: list[str] = []
    images: list[ContentImage] = []
    labels: list[str] = []
    previous_text: str | None = None
    for part in content:
        if not isinstance(part, dict):
            raise AstrBotLocalContentError("user content part 不是对象")
        part_type = part.get("type")
        if part_type == "text":
            value = part.get("text")
            if not isinstance(value, str):
                raise AstrBotLocalContentError("text part 的 text 不是字符串")
            texts.append(value)
            previous_text = value
        elif part_type == "image_url":
            images.append(_image_from_part(part))
            labels.append(_label_candidate(previous_text))
            previous_text = None
        else:
            raise AstrBotLocalContentError(f"user content part 类型不受支持: {part_type!r}")
    text = "\n".join(texts)
    if images:
        mapping = "\n".join(
            f"第 {index} 张{_ORDINAL_SEP}{label}" for index, label in enumerate(labels, 1)
        )
        block = f"{_ORDINAL_HEADER}\n{mapping}"
        text = f"{text}\n\n{block}" if text else block
    return NormalizedContent(text=text, images=tuple(images))


def _image_from_part(part: dict) -> ContentImage:
    """从 image_url part 里解出 (MIME, 字节)；非 data:image 形态一律本地拒绝。"""
    image = part.get("image_url")
    if not isinstance(image, dict):
        raise AstrBotLocalContentError("image_url part 缺少 image_url 对象")
    url = image.get("url")
    if not isinstance(url, str):
        raise AstrBotLocalContentError("image_url part 的 url 不是字符串")
    match = _DATA_URL_RE.match(url)
    if match is None:
        raise AstrBotLocalContentError(
            "AstrBot 后端只接受 data:image/*;base64 图片，拒绝其它 URL 形态"
        )
    try:
        data = base64.b64decode(match.group(2), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise AstrBotLocalContentError("图片 data URL 的 base64 无法解码") from exc
    if not data:
        raise AstrBotLocalContentError("图片 data URL 没有内容")
    return ContentImage(mime=match.group(1).lower(), data=data)


def _label_candidate(previous_text: str | None) -> str:
    """紧邻上一段文本的最后一行若是独立媒体标签则用它，否则记「无独立标签」。

    只接受 prompt_builder 实际会产出的标签整段形态（fullmatch）。旧口径无条件取
    最后一行，正文尾行（「这是我拍的夜景」）会被当标签写进序数对照，模型据此对位
    必错；形似但不合法的方括号串同样不算标签。
    """
    if previous_text is None:
        return _NO_LABEL
    lines = [line.strip() for line in previous_text.splitlines() if line.strip()]
    if not lines:
        return _NO_LABEL
    candidate = lines[-1]
    return candidate if _MEDIA_LABEL_RE.fullmatch(candidate) else _NO_LABEL


@dataclass(frozen=True)
class SavedAttachment:
    """模型本轮产出、已由上游落库的媒体附件（帧里只有 id 与类型）。"""

    attachment_id: str
    kind: str


class AstrBotSseParser:
    """逐行消费 AstrBot SSE，只在完整收尾后交付最终文本。"""

    def __init__(self, expected_session_id: str, *, allow_plain_fallback: bool = False):
        self.expected_session_id = expected_session_id
        self.allow_plain_fallback = allow_plain_fallback
        self._data_lines: list[str] = []
        self._plain_parts: list[str] = []
        self._last_complete: str | None = None
        self._session_seen = False
        self._ended = False
        self._attachments: list[SavedAttachment] = []

    @property
    def attachments(self) -> tuple[SavedAttachment, ...]:
        """本轮落库的媒体附件（按 attachment_saved 帧出现顺序）。"""
        return tuple(self._attachments)

    def feed_line(self, line: str) -> None:
        if not line:
            self._consume_event()
            return
        if line.startswith(":"):
            return
        if line.startswith("data:"):
            value = line[5:]
            self._data_lines.append(value[1:] if value.startswith(" ") else value)

    def _consume_event(self) -> None:
        if not self._data_lines:
            return
        raw = "\n".join(self._data_lines)
        self._data_lines.clear()
        try:
            frame = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise AstrBotProtocolError("SSE data 不是合法 JSON") from exc
        if not isinstance(frame, dict):
            raise AstrBotProtocolError("SSE data 不是对象")
        frame_type = frame.get("type")
        data = frame.get("data")
        if frame_type == "session_id":
            session_id = frame.get("session_id")
            if session_id != self.expected_session_id:
                raise AstrBotProtocolError("SSE 返回了不同的 session id")
            self._session_seen = True
        elif frame_type == "complete":
            if not isinstance(data, str):
                raise AstrBotProtocolError("complete 帧正文不是字符串")
            self._last_complete = data
        elif frame_type == "plain":
            if not isinstance(data, str):
                raise AstrBotProtocolError("plain 帧正文不是字符串")
            if frame.get("chain_type") in _TOOL_CHAIN_TYPES:
                # 工具链的 plain 帧装的是工具参数/结果 JSON，不是给用户的正文。
                return
            self._plain_parts.append(data)
        elif frame_type == "error":
            if isinstance(data, dict):
                message = data.get("message") or data.get("error") or data
            else:
                message = data or "未知错误"
            raise AstrBotProtocolError(f"AstrBot error 帧: {message}")
        elif frame_type == "attachment_saved":
            payload = data
            if not isinstance(payload, dict):
                raise AstrBotProtocolError("attachment_saved 帧缺少 data 对象")
            attachment_id = payload.get("id")
            kind = payload.get("type")
            if not isinstance(attachment_id, str) or not attachment_id:
                raise AstrBotProtocolError("attachment_saved 帧缺少 id")
            if not isinstance(kind, str) or not kind:
                raise AstrBotProtocolError("attachment_saved 帧缺少 type")
            self._attachments.append(SavedAttachment(attachment_id=attachment_id, kind=kind))
        elif frame_type == "end":
            self._ended = True

    def finish(self) -> str:
        self._consume_event()
        if not self._session_seen:
            raise AstrBotProtocolError("SSE 缺少 session_id 帧")
        if not self._ended:
            raise AstrBotProtocolError("SSE 在 end 帧前中断")
        if self._last_complete is not None:
            return self._last_complete
        if not self.allow_plain_fallback:
            raise AstrBotProtocolError("流式 SSE 缺少 complete 最终正文帧")
        return "".join(self._plain_parts)


class AstrBotHttpError(Exception):
    """保留 HTTP 错误面，供公共传输层执行统一重试与归纳。"""

    def __init__(self, status: int, body: str, retry_after: float | None = None):
        super().__init__(f"AstrBot HTTP {status}: {body[:200]}")
        self.status = status
        self.body = body
        self.retry_after = retry_after


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return min(30.0, max(0.5, float(value.strip())))
    except (TypeError, ValueError):
        return None


def _auth_headers(api_key: str) -> dict[str, str]:
    """AstrBot 三种等价鉴权写法里最简单的一种；空 key 表示未配置。"""
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


def build_message_parts(
    system_prompt: str,
    user_text: str,
    attachment_ids: Sequence[str] = (),
) -> str | list[dict]:
    """组本轮 ``message``：无附件时仍是纯信封字符串，有附件时是消息段列表。

    图片段只引用 attachment_id，字节走上传通道，绝不把 base64 塞进消息段。
    """
    envelope = build_prompt_envelope(system_prompt, user_text)
    if not attachment_ids:
        return envelope
    parts: list[dict] = [{"type": "plain", "text": envelope}]
    for attachment_id in attachment_ids:
        parts.append({"type": "image", "attachment_id": attachment_id})
    return parts


@dataclass(frozen=True)
class ChatCompletionResult:
    """一次完整对话的结果：最终正文 + 本轮落库的媒体附件。"""

    text: str
    attachments: tuple[SavedAttachment, ...] = ()


async def complete_chat_result(
    http: httpx.AsyncClient,
    *,
    base_url: str,
    api_key: str,
    session_key: str,
    system_prompt: str,
    user_text: str,
    timeout: float,
    attachment_ids: Sequence[str] = (),
) -> ChatCompletionResult:
    """执行一次对话，返回正文与附件清单（complete_chat 的唯一实现）。"""
    if not isinstance(user_text, str):
        raise AstrBotLocalContentError("AstrBot 后端不接受未经归一化的多模态 content")
    session_id = encode_session_id(session_key)
    payload = {
        "username": "nonebot-hermes-adapter",
        "session_id": session_id,
        "message": build_message_parts(system_prompt, user_text, attachment_ids),
        "_skip_user_history": True,
        "flags": {
            "enable_streaming": True,
            "enable_reasoning": False,
            "enable_default_system_prompt": False,
            "enable_inline_genui": False,
        },
    }
    headers = _auth_headers(api_key)
    url = base_url.rstrip("/") + "/api/v1/chat"
    async with http.stream("POST", url, json=payload, headers=headers, timeout=timeout) as resp:
        if resp.status_code != 200:
            body = (await resp.aread()).decode("utf-8", "replace")
            raise AstrBotHttpError(
                resp.status_code,
                body,
                retry_after=_retry_after(resp.headers.get("Retry-After")),
            )
        if "application/json" in resp.headers.get("content-type", "").lower():
            raw = (await resp.aread()).decode("utf-8", "replace")
            try:
                data = json.loads(raw)
            except ValueError as exc:
                raise AstrBotProtocolError("HTTP 200 返回了无法解析的 JSON") from exc
            if isinstance(data, dict):
                message = data.get("message") or data.get("error") or raw
            else:
                message = raw
            raise AstrBotProtocolError(f"AstrBot HTTP 200 错误响应: {message}")
        parser = AstrBotSseParser(session_id)
        async for line in resp.aiter_lines():
            parser.feed_line(line)
        return ChatCompletionResult(text=parser.finish(), attachments=parser.attachments)


async def complete_chat(
    http: httpx.AsyncClient,
    *,
    base_url: str,
    api_key: str,
    session_key: str,
    system_prompt: str,
    user_text: str,
    timeout: float,
    attachment_ids: Sequence[str] = (),
) -> str:
    """执行一次对话并只返回最终正文（调用方约定的兼容外观）。"""
    result = await complete_chat_result(
        http,
        base_url=base_url,
        api_key=api_key,
        session_key=session_key,
        system_prompt=system_prompt,
        user_text=user_text,
        timeout=timeout,
        attachment_ids=attachment_ids,
    )
    return result.text


async def upload_attachment(
    http: httpx.AsyncClient,
    *,
    base_url: str,
    api_key: str,
    data: bytes,
    filename: str,
    content_type: str,
    timeout: float,
) -> str:
    """上传一份附件，返回可放进 chat 消息段的 attachment_id。

    上游用上传时的 Content-Type 推导附件类型（image/* → image），所以 MIME
    必须显式传真实值，否则图会落成普通文件。失败仍是 HTTP 200 + 错误信封，
    只看状态码会误判成功，因此这里两种失败面分开报。
    """
    headers = _auth_headers(api_key)
    resp = await http.post(
        base_url.rstrip("/") + "/api/v1/file",
        headers=headers,
        files={"file": (filename, data, content_type)},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise AstrBotHttpError(
            resp.status_code,
            resp.text,
            retry_after=_retry_after(resp.headers.get("Retry-After")),
        )
    try:
        payload = resp.json()
    except ValueError as exc:
        raise AstrBotProtocolError("上传响应不是合法 JSON") from exc
    if not isinstance(payload, dict):
        raise AstrBotProtocolError("上传响应不是对象")
    if payload.get("status") == "error":
        raise AstrBotProtocolError(f"上传失败: {payload.get('message') or payload}")
    envelope = payload.get("data")
    if not isinstance(envelope, dict):
        raise AstrBotProtocolError("上传响应缺少 data 对象")
    attachment_id = envelope.get("attachment_id")
    if not isinstance(attachment_id, str) or not attachment_id:
        raise AstrBotProtocolError("上传响应缺少 attachment_id")
    if envelope.get("type") != "image":
        raise AstrBotProtocolError(f"上传后的附件类型不是图片: {envelope.get('type')!r}")
    return attachment_id


async def download_attachment(
    http: httpx.AsyncClient,
    *,
    base_url: str,
    api_key: str,
    attachment_id: str,
    timeout: float,
) -> tuple[bytes, str]:
    """下载一份图片附件，返回 (字节, MIME)。

    失败面分三类：非 200 走 HTTP 错误面；200 + 错误信封与非法 MIME 走协议错误。
    非图片附件不静默丢弃——本后端只消费图片，其它类型按协议失败上报。
    """
    resp = await http.get(
        base_url.rstrip("/") + "/api/v1/file",
        params={"attachment_id": attachment_id},
        headers=_auth_headers(api_key),
        timeout=timeout,
    )
    raw_content_type = resp.headers.get("content-type") or ""
    media_type = raw_content_type.split(";")[0].strip().lower()
    if resp.status_code != 200:
        raise AstrBotHttpError(
            resp.status_code,
            resp.content.decode("utf-8", "replace"),
            retry_after=_retry_after(resp.headers.get("Retry-After")),
        )
    if "json" in media_type:
        try:
            payload = resp.json()
        except ValueError as exc:
            raise AstrBotProtocolError("下载响应不是合法 JSON") from exc
        message = payload.get("message") if isinstance(payload, dict) else payload
        raise AstrBotProtocolError(f"下载失败: {message or payload}")
    if not media_type.startswith("image/"):
        raise AstrBotProtocolError(
            f"下载到的附件不是图片: {media_type or '缺少 content-type'!r}"
        )
    return resp.content, media_type


async def check_health(
    http: httpx.AsyncClient,
    *,
    base_url: str,
    api_key: str,
    timeout: float = 5,
) -> bool:
    """用需要对话权限的轻量端点同时检查服务与凭据。"""
    resp = await http.get(
        base_url.rstrip("/") + "/api/v1/chat/configs",
        headers=_auth_headers(api_key),
        timeout=timeout,
    )
    return resp.status_code == 200
