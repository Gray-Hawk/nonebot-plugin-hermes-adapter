"""内联媒体 data URL 的摘取／回填（hermes_client 与终态解码共用）。

长 data URL 会撑爆 JSON 候选与行长上限，所以在解析／计量前先换成
⟦hermes-media-N⟧ 占位，处理完再原样接回。这里只有这两个纯函数，
不引入新的媒体系统。
"""

from __future__ import annotations

import re

_DATA_URL_DETACH_RE = re.compile(r"data:[\w.+-]+/[\w.+-]+;base64,[A-Za-z0-9+/=]{256,}")
_DETACH_TOKEN_RE = re.compile(r"⟦hermes-media-(\d+)⟧")


def _detach_data_urls(text: str) -> tuple[str, list[str]]:
    """把长 data URL 替换成 ⟦hermes-media-N⟧,返回 (缩短后的文本, 原始 URL 列表)。"""
    urls: list[str] = []

    def _repl(match: re.Match) -> str:
        urls.append(match.group(0))
        return f"⟦hermes-media-{len(urls) - 1}⟧"

    return _DATA_URL_DETACH_RE.sub(_repl, text), urls


def _reattach_data_urls(value: str, urls: list[str]) -> str:
    """把 ⟦hermes-media-N⟧ 换回原始 data URL。索引越界时原样留着 token。"""

    def _repl(match: re.Match) -> str:
        idx = int(match.group(1))
        return urls[idx] if 0 <= idx < len(urls) else match.group(0)

    return _DETACH_TOKEN_RE.sub(_repl, value)
