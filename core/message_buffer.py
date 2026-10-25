"""消息缓冲外观层。

外部调用方 (handlers / prompt_builder / mcp tools) 仍然只看到 `MessageBuffer`
这一类型,语义跟之前一致 (append / get_recent / counts_by_scope + 私聊桶隔离)。
内部转调:
- `MessageStore` (持久化 + autoincrement msg_id)
- `ImageFetcher` (异步把消息里的 image_urls 抓回 ImageCache)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .storage.image_fetcher import ImageFetcher
    from .storage.message_store import MessageStore


@dataclass
class BufferedMessage:
    ts: int
    adapter: str
    group_id: str | None  # None = 私聊
    user_id: str
    nickname: str
    content: str
    image_urls: list[str] = field(default_factory=list)
    sticker_urls: list[str] = field(default_factory=list)
    """QQ 收藏/自定义表情 (Image.sticker=True) 的 URL。与普通图片分开存,
    message_images 表里以 kind='sticker' 区分,idx 接在 image_urls 之后连续编号。"""
    quote_image_urls: list[str] = field(default_factory=list)
    """被本条消息引用的消息里的图片 URL(kind='quote')。idx 续排在 sticker 之后。
    入库即走 ImageFetcher 落字节,与普通图同语义(可召回、可内联)。"""
    forward_image_urls: list[str] = field(default_factory=list)
    """本条合并转发展开(第一层)内节点的图片 URL(kind='forward_image')。idx 接在
    引用图之后连续编号。只注册发号、按需拉图,恒不内联。"""
    reply_to_ts: int | None = None
    is_bot: bool = False
    is_synthetic: bool = False
    """合成轮身份事实：本条是系统合成（离场确认/接力重交/对账补投/任务投递/主动唤起等），
    而非真人键入。默认 False 是安全方向（真人消息永不误判为合成）。合并函数凭它做
    合成 fragment 的身份分界，昵称常量（系统核对）只承担显示、不再参与身份识别。"""
    id: int | None = None
    """DB 主键。perception 构造时为 None,MessageStore.append 写入后回填。
    handlers 不应直接读写;由 MessageStore.append 管控。"""
    forward_content: str | None = None
    """合并转发的完整展开块(<forwarded_messages>…</forwarded_messages>)。
    content 里只存单行 preview;全文走 message_forwards 附表,由
    MessageStore.append 写入、get_recent 水合。无折叠消息时恒为 None。"""
    file_entries: list[dict] = field(default_factory=list)
    """本消息附带的文件元数据(QQ 群文件/私聊文件)。每条 dict 键:
    file_id:str、name:str、size:int、url:str|None、qq_message_id:str|None。
    idx 由列表顺序隐式决定,独立文件编号,不与图片共享。文件不自动下载,
    文件正文不自动进 prompt——perception 拿到真实 db id 后按需回填带
    m:id#fN 的 content(见 MessageStore.update_content)。"""


# --- 媒体枚举序不变量(焊死) ---
# 一条消息的 message_images idx 连续编号 = 普通图 → 表情包 → 引用图 → 转发图。
# 写入侧(MessageStore.append 的 idx、MessageBuffer.append 给 fetcher 的 start_index)
# 与渲染侧(历史行清单、parts 标签、转发块计数)共用同一序列——即下方
# iter_message_media 的迭代顺序(原 MEDIA_KINDS 常量零引用已删);
# 禁止各处各自拼序。

# kind → 中文种类词。compact 是历史行清单里的简称("图"/"表情"/"引用"/"转发"),
# full 是 parts 全称标签里的写法("图"/"表情包"/"引用图"/"转发图")。
_MEDIA_KIND_COMPACT = {"image": "图", "sticker": "表情", "quote": "引用", "forward_image": "转发"}
_MEDIA_KIND_FULL = {"image": "图", "sticker": "表情包", "quote": "引用图", "forward_image": "转发图"}


def iter_message_media(msg: "BufferedMessage") -> list[tuple[str, str]]:
    """按枚举序产出 (kind, url);下标即该媒体在 message_images 表的 idx。"""
    items: list[tuple[str, str]] = []
    for kind, urls in (
        ("image", msg.image_urls),
        ("sticker", msg.sticker_urls),
        ("quote", msg.quote_image_urls),
        ("forward_image", msg.forward_image_urls),
    ):
        for u in urls:
            items.append((kind, u))
    return items


def media_kind_word(kind: str, *, compact: bool = False) -> str:
    """kind → 中文种类词。compact=清单简称;否则全称标签写法。未知 kind 原样返回。"""
    table = _MEDIA_KIND_COMPACT if compact else _MEDIA_KIND_FULL
    return table.get(kind, kind)


def media_label(kind: str, message_id: int, idx: int, *, compact: bool = False) -> str:
    """媒体寻址标签。compact=历史行清单形态(`图#0`);否则 parts 全称形态(`[图 m:22371#0]`)。"""
    if compact:
        return f"{media_kind_word(kind, compact=True)}#{idx}"
    return f"[{media_kind_word(kind)} m:{message_id}#{idx}]"


def build_media_address_index(rows) -> dict[str, tuple[str, int, int]]:
    """行序列 → {url: (kind, message_id, idx)}。

    同一 url 重复出现(同图发两次)取最近一行——后者覆盖前者,标签指向更近的库行,
    这是本方案的取舍:清单按行渲染,同号指认以最近一次为准。id 缺失(未入库)的行跳过。
    """
    idx_map: dict[str, tuple[str, int, int]] = {}
    for m in rows:
        if m.id is None:
            continue
        for i, (kind, url) in enumerate(iter_message_media(m)):
            idx_map[url] = (kind, m.id, i)
    return idx_map


def render_media_summary(msg: "BufferedMessage") -> str | None:
    """历史行尾紧凑媒体清单。idx 为全局连续编号,种类词只在同种媒体首次出现时给。

    例: `(图#0)` / `(图#0 #1 #2 表情#3)` / `(图#0 引用#2 转发#3)`。无媒体时返回 None。
    """
    items = iter_message_media(msg)
    if not items:
        return None
    parts: list[str] = []
    seen: set[str] = set()
    for i, (kind, _u) in enumerate(items):
        if kind in seen:
            parts.append(f"#{i}")
        else:
            parts.append(f"{media_kind_word(kind, compact=True)}#{i}")
            seen.add(kind)
    return "(" + " ".join(parts) + ")"


class MessageBuffer:
    """对外 API 不变;实现转调 MessageStore + ImageFetcher。"""

    def __init__(self, *, store: MessageStore, fetcher: ImageFetcher) -> None:
        self._store = store
        self._fetcher = fetcher

    def append(self, msg: BufferedMessage) -> None:
        msg_id = self._store.append(msg)
        if msg_id is None:
            return
        # 枚举序不变量:own → sticker → quote → forward,连续 idx(与
        # MessageStore.append 的编号同一份定义)。用 start_index 把每段的
        # offset 告诉 fetcher,回填 sha 时 (msg_id, idx) 才能对上行。
        base = 0
        for urls in (msg.image_urls, msg.sticker_urls, msg.quote_image_urls, msg.forward_image_urls):
            if urls:
                self._fetcher.submit(msg_id, list(urls), start_index=base)
            base += len(urls)

    def get_recent(
        self,
        adapter: str,
        group_id: str | None,
        limit: int,
        before_ts: int | None = None,
        owner_user_id: str | None = None,
    ) -> list[BufferedMessage]:
        return self._store.get_recent(
            adapter=adapter,
            group_id=group_id,
            limit=limit,
            before_ts=before_ts,
            owner_user_id=owner_user_id,
        )

    def update_content(self, message_id: int, content: str) -> None:
        """回填指定消息的 content(perception 拿真实 db id 后对含文件消息补
        m:id#fN 标签)。只改 content 列,不动 ts/媒体行。"""
        self._store.update_content(message_id, content)

    def counts_by_scope(self) -> dict[tuple[str, str], int]:
        return self._store.counts_by_scope()
