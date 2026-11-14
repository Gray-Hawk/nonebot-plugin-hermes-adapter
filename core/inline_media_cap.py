"""Runs 出站前的内联图片数量闸：单轮最多 10 张 ``data:image`` part。

为什么在插件侧截：上游 ``gateway/session_api_media.commit_api_images`` 给 API
轮次的 data 图片设了 ``_IMAGE_LIMIT=10``（与 ``prompt.submit`` 附件同一个按次上限），
超一张即 ``RuntimeStoreError('invalid_params')``，整轮 POST /v1/runs 400；插件侧
之前没有兜底，群里表现为静默。多张图的语义不必为此拆轮：窗口里的旧图仍可按
``[图 m:id#idx]`` / ``[表情包 m:id#idx]`` 文本标记召回，当前/最新媒体优先留在
payload 里。

约束（主人定案）：
- parts 顺序是旧→新，超限时保留**最后** ``limit`` 个 data 图片 part；
- 只移除超限的图片 part；文本 part、寻址标签、远程 http(s) URL part 与其他
  part 一律原样保留（远程 URL 不受此上限约束，也不占名额）；
- 未超限（含恰好 ``limit`` 张、纯文本）原对象原样返回，不记日志；
- WARNING 只报数量：不打印 data URL、图片内容或完整提示词。

这是数量闸，不是字节预算：单图已由 ``media_config.inline.max_bytes`` 压到
50 万字节级，10 张远低于上游默认总字节限额（128MiB），故不新建总字节配置或状态。
"""

from __future__ import annotations

from typing import Any

from nonebot import logger

# 与上游 ``gateway/session_api_media._IMAGE_LIMIT`` 对齐（同一个按次上限口径）。
INLINE_IMAGE_LIMIT = 10


def _inline_image_url(part: Any) -> str | None:
    """``data:image/...`` 内联图片 part 的 url；其它 part 返回 None。"""
    if not isinstance(part, dict) or part.get("type") != "image_url":
        return None
    image_url = part.get("image_url")
    if not isinstance(image_url, dict):
        return None
    url = image_url.get("url")
    if isinstance(url, str) and url.lower().startswith("data:image/"):
        return url
    return None


def cap_inline_image_parts(content: Any, limit: int = INLINE_IMAGE_LIMIT) -> Any:
    """``content`` 的 data 图片 part 截到最多 ``limit`` 个（保留最新）。

    非 parts 形态（纯文本/空）与未超限内容原样返回；超限时返回去掉最早若干
    图片 part 的新列表，其余 part（含被删图片的寻址标签文本）保持原顺序。
    """
    if not isinstance(content, list):
        return content
    inline = [index for index, part in enumerate(content) if _inline_image_url(part) is not None]
    if len(inline) <= limit:
        return content
    keep = set(inline[-limit:]) if limit > 0 else set()
    dropped = len(inline) - len(keep)
    kept = len(keep)
    logger.warning(
        f"[media] 单轮内联图片超限：丢弃最早 {dropped} 张，保留最新 {kept} 张"
        f"（上限 {limit}，文本与寻址标记保留）"
    )
    return [part for index, part in enumerate(content) if index in keep or _inline_image_url(part) is None]
