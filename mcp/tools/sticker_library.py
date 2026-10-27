"""本地表情库的检索与预览工具；预览复用消息看图的编码管线。"""

from __future__ import annotations

import base64
import csv
import io
import json

from mcp.types import ImageContent, TextContent
from nonebot import logger
from pydantic import BaseModel

from ...core.media_config import MediaConfig
from ...core.sticker_library import StickerLibrary, StickerLibraryError
from .get_message_images import _encode_outbound


class StickerSearchResult(BaseModel):
    ok: bool
    csv: str
    total: int
    count: int
    error: str | None = None


def search_sticker_library_impl(
    query: str,
    limit: int,
    *,
    library: StickerLibrary,
) -> StickerSearchResult:
    """只返回 CSV 数据字段，绝不把库在主机上的绝对路径交给模型。"""
    try:
        rows, total = library.search(query, limit=limit)
    except Exception as exc:
        logger.warning(f"[STICKER] 检索失败: {type(exc).__name__}: {exc}")
        return StickerSearchResult(ok=False, csv="", total=0, count=0, error="library_unavailable")
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", "kind", "file", "note"])
    for row in rows:
        writer.writerow([row.id, row.kind, row.file, row.note])
    return StickerSearchResult(
        ok=True,
        csv=output.getvalue(),
        total=total,
        count=len(rows),
    )


async def preview_sticker_impl(
    sticker_id: str,
    *,
    library: StickerLibrary,
    cfg: MediaConfig,
) -> list:
    """按稳定 ID 预览；静图与动图均沿用 get_message_images 的出站编码。"""
    try:
        raw, mime = library.read(sticker_id)
    except StickerLibraryError as exc:
        reason = "not_found" if library.get(sticker_id) is None else "unavailable"
        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {"id": sticker_id, "available": False, "reason": reason},
                    ensure_ascii=False,
                ),
            )
        ]
    except OSError as exc:
        logger.warning(f"[STICKER] 预览读取失败 id={sticker_id}: {exc}")
        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {"id": sticker_id, "available": False, "reason": "unavailable"},
                    ensure_ascii=False,
                ),
            )
        ]

    frames = await _encode_outbound(raw, mime, "sticker", cfg)
    if frames is None:
        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {"id": sticker_id, "available": False, "reason": "compress_failed"},
                    ensure_ascii=False,
                ),
            )
        ]
    header = TextContent(
        type="text",
        text=json.dumps(
            {
                "id": sticker_id,
                "available": True,
                "frames": len(frames),
                "preview_only": True,
            },
            ensure_ascii=False,
        ),
    )
    blocks: list = [header]
    for index, (data, frame_mime) in enumerate(frames):
        suffix = "" if len(frames) == 1 else f" 第{index + 1}/{len(frames)}帧"
        blocks.append(TextContent(type="text", text=f"[sticker library {sticker_id}{suffix}]"))
        blocks.append(
            ImageContent(
                type="image",
                data=base64.b64encode(data).decode("ascii"),
                mimeType=frame_mime,
            )
        )
    return blocks
