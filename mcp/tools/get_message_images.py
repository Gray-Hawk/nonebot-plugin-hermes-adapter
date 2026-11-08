"""get_message_images: 按消息 id 列表精确取回图字节。

返回 MCP `content[]` 数组,混合 TextContent (元信息 JSON + 每图前的 marker)
与 ImageContent (实际 base64 图字节)。Hermes Agent 端 `tools/mcp_tool.py:2231`
会把 ImageContent 块缓存为 MEDIA:<path> 并注入下一轮 LLM 调用。

返回结构示例:
  [0] TextContent({"results": [...每张图的元数据列表(含 kind)...]})
  [1] TextContent("[image for m:1234 idx=0]")
  [2] ImageContent(data="<base64>", mimeType="image/jpeg")
  [3] TextContent("[sticker for m:1234 idx=1]")
  [4] ImageContent(...)

kind 取值:image = 普通图片;sticker = QQ 收藏/自定义表情。marker 文本
按 kind 前缀,模型据以区分「画面内容」与「情绪信号」两种解读口径。
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any

from mcp.types import ImageContent, TextContent
from nonebot import logger
from pydantic import BaseModel, Field

from ...core.image_inline import encode_image_frames_bytes, sniff_mime
from ...core.media_config import MediaConfig, load_media_config
from ...core.routing import CallerScope


class GetMessageImagesInput(BaseModel):
    message_ids: list[int] = Field(
        ...,
        min_length=1,
        max_length=4,
        description="要取图字节的消息 id 列表;最多 4 个",
    )
    idx: list[int] | None = Field(
        default=None,
        description=(
            "可选:只取这些 idx 的图(对全部 message_ids 生效);不传=现状整条消息全拉。"
            "idx 取自历史行/当前消息的媒体清单(如 `(图#0 #1 表情#2)` 里的号)。"
        ),
    )
    adapter: str | None = Field(default=None, description="可选防御性过滤")
    group_id: str | None = Field(default=None, description="可选防御性过滤")


_TOTAL_RESULT_MAX_BYTES = 25 * 1024 * 1024  # 25MB


async def _encode_outbound(
    bytes_: bytes, real_mime: str | None, kind: str, cfg: MediaConfig
) -> list[tuple[bytes, str]] | None:
    """拉图出站编码:返回 (bytes, mime) 帧列表;失败 None。

    - 动图:一律抽帧(fetch 档 gif_max_frames),帧按 fetch 梯度逐帧编码
    - 表情:≤ sticker_gate_bytes 原样直出(小图无害);超过强制 fetch 梯度压缩
    - 普通静图:≤ fetch_gate_bytes 直出;撞门走 fetch 梯度,两级压不进返回 None
      (调用方回退占位符并提示用户自行压缩——这种图通常不是正常图片)
    """
    from ...core.image_inline import is_animated_bytes

    if is_animated_bytes(bytes_):
        return await asyncio.to_thread(
            encode_image_frames_bytes,
            bytes_,
            cfg.fetch_ladder,
            cfg.fetch_gate_bytes,
            cfg.fetch_gif_max_frames,
        )
    if kind == "sticker" and len(bytes_) <= cfg.sticker_gate_bytes:
        return [(bytes_, real_mime or sniff_mime(bytes_))]
    if kind != "sticker" and len(bytes_) <= cfg.fetch_gate_bytes:
        return [(bytes_, real_mime or sniff_mime(bytes_))]
    return await asyncio.to_thread(
        encode_image_frames_bytes,
        bytes_,
        cfg.fetch_ladder,
        cfg.fetch_gate_bytes,
        cfg.fetch_gif_max_frames,
    )


async def get_message_images_impl(
    inp: GetMessageImagesInput,
    *,
    store,
    cache,
    scope: CallerScope | None,
) -> list:
    """按 input message_ids 顺序取图,返回 MCP content[] 数组。

    每张图的可用性记在 header 的 results 数组里;不可用时 reason 取值:
      cache_miss      — sha256 NULL 或文件被淘汰
      compress_failed — 撞大小门后两级压缩仍超限(提示用户自行压缩重发)
      cap_exceeded    — 累计字节超 25MB
      not_found       — message_id 在 DB 不存在,**或不在调用方范围内**
    拉图出站压缩:静图撞 fetch 大小门走梯度压缩,动图一律抽帧,
    表情超 sticker 门控强制压缩(见 _encode_outbound)。
    """
    # 范围判定必须按行做:入参的 adapter/group_id 是调用方自报的(而且是可选的),
    # message_id 是自增小整数、可枚举,所以真正的归属只能从 DB 反查。
    # 越权 id 一律按 not_found 处理 —— 回一个专门的 reason 等于给越权方一个
    # "这个 id 存在"的探测口。
    owners = store.get_message_owners(inp.message_ids)
    denied = {
        mid for mid, (adapter, group_id) in owners.items() if scope is None or not scope.allows(adapter, group_id or "")
    }
    if denied:
        logger.warning(
            f"[HERMES MCP] get_message_images 越权 message_id 已按 not_found 处理: {sorted(denied)} "
            f"(caller: {'unresolved' if scope is None else scope.describe()})"
        )

    metas = store.get_message_images_meta(inp.message_ids)
    # 可选 idx 过滤——只拉指定号的图,省字节 + vision token,
    # 且按号指认精确。不传=现状全拉(零回归)。scope 校验/大小门/抽帧逻辑不变。
    if inp.idx is not None:
        wanted = set(inp.idx)
        metas = {mid: [m for m in lst if m["idx"] in wanted] for mid, lst in metas.items()}
    results: list[dict[str, Any]] = []
    content_blocks: list[Any] = []
    total_bytes = 0
    cap_exceeded_remaining = False
    # ⑫-1:配置挪出逐图循环——原先每张图现读一次 YAML 磁盘文件,
    # 现单次调用读一次、循环内复用同一份快照(单次调用内配置一致性反而更好)。
    media_cfg = load_media_config()

    # 严格按入参顺序处理;同条消息内按 idx 升序(get_message_images_meta 已按此排)
    for message_id in inp.message_ids:
        if message_id in denied or message_id not in metas or not metas[message_id]:
            results.append(
                {
                    "message_id": message_id,
                    "image_idx": 0,
                    "available": False,
                    "reason": "not_found",
                }
            )
            continue
        for meta in metas[message_id]:
            idx = meta["idx"]
            kind = meta.get("kind") or "image"
            if cap_exceeded_remaining:
                results.append(
                    {
                        "message_id": message_id,
                        "image_idx": idx,
                        "kind": kind,
                        "available": False,
                        "reason": "cap_exceeded",
                    }
                )
                continue
            sha = meta["sha256"]
            mime = meta["mime_type"]
            if not sha:
                results.append(
                    {
                        "message_id": message_id,
                        "image_idx": idx,
                        "kind": kind,
                        "available": False,
                        "reason": "cache_miss",
                    }
                )
                continue
            payload = cache.get_bytes(sha)
            if payload is None:
                results.append(
                    {
                        "message_id": message_id,
                        "image_idx": idx,
                        "kind": kind,
                        "available": False,
                        "reason": "cache_miss",
                    }
                )
                continue
            bytes_, real_mime = payload
            # 本部署补丁(拉图出站压缩): 取代原样直出。
            # 静图 ≤ gate_bytes 直出;撞门走 fetch 梯度(两级都压不进→compress_failed,
            # 提示用户自行压缩,不扔进 LLM——这种情况的图通常不是正常图片)。
            # 动图不分大小一律抽帧(fetch 档帧数);表情 ≤sticker_gate 直出,
            # 超过强制压缩(防巨型表情连发压预算)。
            blocks = await _encode_outbound(bytes_, real_mime, kind, media_cfg)
            if blocks is None:
                results.append(
                    {
                        "message_id": message_id,
                        "image_idx": idx,
                        "kind": kind,
                        "available": False,
                        "reason": "compress_failed",
                        "byte_size": len(bytes_),
                        "hint": "图超限且两级压缩均失败,请让发送者自行压缩后重发",
                    }
                )
                continue
            blocks_size = sum(len(b) for b, _ in blocks)
            if total_bytes + blocks_size > _TOTAL_RESULT_MAX_BYTES:
                cap_exceeded_remaining = True
                results.append(
                    {
                        "message_id": message_id,
                        "image_idx": idx,
                        "kind": kind,
                        "available": False,
                        "reason": "cap_exceeded",
                    }
                )
                continue
            # marker 按 kind 区分:表情包和普通照片/截图语义不同,模型看到
            # "[sticker for ...]" 就知道该按情绪信号解读而非画面内容。
            n_blocks = len(blocks)
            for bi, (data, mime) in enumerate(blocks):
                if n_blocks == 1:
                    marker = f"[{kind} for m:{message_id} idx={idx}]"
                else:
                    marker = f"[{kind} for m:{message_id} idx={idx} 第{bi + 1}/{n_blocks}帧，同一动图按时间顺序]"
                content_blocks.append(TextContent(type="text", text=marker))
                content_blocks.append(
                    ImageContent(
                        type="image",
                        data=base64.b64encode(data).decode("ascii"),
                        mimeType=mime,
                    )
                )
            total_bytes += blocks_size
            results.append(
                {
                    "message_id": message_id,
                    "image_idx": idx,
                    "kind": kind,
                    "available": True,
                    "mime": blocks[0][1],
                    "byte_size": blocks_size,
                    "frames": n_blocks,
                    "compressed": blocks_size < len(bytes_),
                }
            )

    header = TextContent(
        type="text",
        text=json.dumps({"results": results}, ensure_ascii=False),
    )
    return [header, *content_blocks]
