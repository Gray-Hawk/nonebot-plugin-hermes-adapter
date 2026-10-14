"""把当前轮的图片转成自包含的 `data:` URL,供多模态 parts 直接携带。

为什么不能直接把平台图片 URL 交出去:Hermes 对 http(s) 图片 URL 是**纯透传**,
自己不下载,原样转给 provider。于是链路上每一环都可能失败,且失败形态不统一 ——
provider 拉不到会把整轮打成 400;Anthropic 形态转成 `{"type":"url"}` 仍由对端拉取;
而 Gemini native 会**静默丢弃**非 `data:` 的 part,Bedrock 把它降级成一行文本。
平台侧的 URL 还普遍带时效凭据或鉴权 token,交出去既会过期也会外泄。

内联不会增加上游持久化体积:Hermes 落库前把 image part 投影成文本占位,
图片字节不进它的会话库。代价只在单次请求体上,而请求体有硬上限,所以
归一化是必需项而非优化项 —— 一张手机原图 base64 之后就能吃掉大半配额。

下采样到最长边 `max_edge` 后重编码,同时也丢掉 EXIF(含地理位置)。视觉模型
内部本就会降采样,原图分辨率换不来识别质量,只换更高的 token 成本。
"""

from __future__ import annotations

import asyncio
import base64
import io
from collections.abc import Sequence
from typing import TYPE_CHECKING

import httpx
from nonebot import logger

from ..config import plugin_config

if TYPE_CHECKING:
    from PIL import Image

# 逐级降质阶梯的收紧目标。实际阶梯由 _build_ladder 对用户配置取下界生成,
# 保证每一级都不高于上一级 —— 直接用固定值会在 max_edge 被配得更小时反向放大。
# 走完仍超限就放弃这张图:继续压下去得到的已经不是「这张图」了,不如明确缺席。
_TIGHTEN_STEPS: tuple[tuple[int, int], ...] = ((1024, 75), (768, 68))


def _build_ladder(max_edge: int, quality: int) -> tuple[tuple[int, int], ...]:
    """(edge, quality) 单调不增的尝试序列,去掉重复级。"""
    ladder: list[tuple[int, int]] = [(max_edge, quality)]
    for edge, q in _TIGHTEN_STEPS:
        step = (min(edge, max_edge), min(q, quality))
        if step != ladder[-1]:
            ladder.append(step)
    return tuple(ladder)


def _composite_rgb(im: "Image.Image") -> "Image.Image":
    """RGBA/LA/P 合成到纯白底 RGB(JPEG 无 alpha,直接 convert 会把透明区压黑)。"""
    from PIL import Image

    if im.mode in ("RGBA", "LA", "P"):
        im = im.convert("RGBA")
        bg = Image.new("RGB", im.size, (255, 255, 255))
        bg.paste(im, mask=im.split()[-1])
        return bg
    elif im.mode != "RGB":
        return im.convert("RGB")
    else:
        return im.copy()


def _normalize_image(raw: bytes) -> Image.Image | None:
    """解码原始字节为摆正方向、消除透明通道的 RGB 图像对象。

    同步 CPU 密集操作,必须在工作线程中调用。
    """
    from PIL import Image, ImageOps

    try:
        with Image.open(io.BytesIO(raw)) as im:
            # EXIF 方向必须在缩放前应用,否则竖拍照片会以横向送进模型。
            im = ImageOps.exif_transpose(im)

            # 动图只取首帧:后续帧对静态视觉理解无信息增量。
            # (表情动图的多帧感知走 inline_sticker_frames,不经本函数。)
            if getattr(im, "is_animated", False):
                im.seek(0)

            return _composite_rgb(im)
    except Exception as exc:
        logger.warning(f"[image_inline] decode failed ({type(exc).__name__}: {exc})")
        return None


def _encode_image(base_im: "Image.Image", max_edge: int, quality: int, per_image_cap: int) -> tuple[bytes, str] | None:
    """已归一化的 RGB 图像 → 下采样 → JPEG 重编码。超限返回 None。"""
    from PIL import Image

    ladder = _build_ladder(max_edge, quality)
    for edge, q in ladder:
        try:
            target_im = base_im
            if max(base_im.size) > edge:
                target_im = base_im.copy()
                target_im.thumbnail((edge, edge), Image.LANCZOS)

            buf = io.BytesIO()
            # optimize=True 生成最优霍夫曼编码表;不写入 exif 参数即可自动剔除所有地理位置等隐私元数据。
            target_im.save(buf, format="JPEG", quality=q, optimize=True)
            out = buf.getvalue()
            if len(out) <= per_image_cap:
                return out, "image/jpeg"
        except Exception as exc:
            logger.warning(f"[image_inline] encode failed ({type(exc).__name__}: {exc})")
            return None

    return None


def _encode_one(raw: bytes, max_edge: int, quality: int, per_image_cap: int) -> tuple[bytes, str] | None:
    """解码 → 摆正 → 下采样 → JPEG 重编码。返回 (bytes, mime);无法处理或超限返回 None。

    同步 CPU 密集,调用方须放进线程。
    """
    base_im = _normalize_image(raw)
    if base_im is None:
        return None
    return _encode_image(base_im, max_edge, quality, per_image_cap)


# --- 表情动图抽帧(2026-10-13,维护者批准直接落地) ---
# 背景:K3 实测 GIF 直输被静态化且失真(帧叠化/取尾帧),动图感知只有抽帧喂多张
# 静态图一条路;多帧理解已实测通过(5 帧按序描述+动作弧正确),成本 ~440 token/帧。

# 采样形状(hard-plans 2026-10-13 待定案形状落地):短动图全抽,长动图等距 6~8 帧
# (含首末帧)。视觉 token 按帧数倍乘,帧数别再往上抬——动图表情在热群高频出现。
_GIF_FULL_SAMPLE_MAX = 5   # n<=5 全抽
_GIF_SAMPLE_MIN = 6        # 长动图至少采 6 帧
_GIF_SAMPLE_MAX = 8        # 至多采 8 帧


def sample_frame_indices(n_frames: int) -> list[int]:
    """动图采样帧序号:n<=5 全抽;否则等距 6~8 帧(含首末帧,升序)。纯函数。"""
    if n_frames <= 0:
        return []
    if n_frames <= _GIF_FULL_SAMPLE_MAX:
        return list(range(n_frames))
    k = min(_GIF_SAMPLE_MAX, max(_GIF_SAMPLE_MIN, n_frames // 4))
    if n_frames <= k:
        return list(range(n_frames))
    # round 可能撞重(如 n=9,k=8),set 去重后升序
    return sorted({round(i * (n_frames - 1) / (k - 1)) for i in range(k)})


def _extract_frames_rgb(raw: bytes, indices: list[int]) -> list["Image.Image"]:
    """从动图字节抽出指定帧,逐帧白底合成 RGB。同步 CPU 密集,须在线程中调用。

    不做 EXIF 摆正:ImageOps.exif_transpose 会把对象换成已加载的静态副本,
    再 seek 直接 EOFError;GIF/APNG 本来也不带 EXIF 方向,跳过零损失。
    """
    from PIL import Image

    frames: list[Image.Image] = []
    with Image.open(io.BytesIO(raw)) as im:
        for i in indices:
            im.seek(i)
            frames.append(_composite_rgb(im))
    return frames


def _encode_sticker_one(raw: bytes, max_edge: int, quality: int, per_image_cap: int) -> list[str] | None:
    """一张表情 → 帧组 data URL 列表(静态=单元素;动图=采样帧)。失败返回 None。

    返回帧组(而不是扁平图列)是防张冠李戴的结构保证:一张表情的所有帧
    始终装在同一组里,标签由 prompt_builder 按组内位置生成,跨表情不可能错位。
    """
    from PIL import Image

    try:
        with Image.open(io.BytesIO(raw)) as im:
            n_frames = getattr(im, "n_frames", 1)
    except Exception as exc:
        logger.warning(f"[image_inline] sticker decode failed ({type(exc).__name__}: {exc})")
        return None

    if n_frames <= 1:
        res = _encode_one(raw, max_edge, quality, per_image_cap)
        return [f"data:{res[1]};base64,{base64.b64encode(res[0]).decode('ascii')}"] if res else None

    indices = sample_frame_indices(n_frames)
    try:
        frames = _extract_frames_rgb(raw, indices)
    except Exception as exc:
        logger.warning(f"[image_inline] sticker frame extract failed ({type(exc).__name__}: {exc})")
        return None
    out: list[str] = []
    for frame in frames:
        res = _encode_image(frame, max_edge, quality, per_image_cap)
        if res is None:
            continue  # 单帧编码失败只丢该帧,保其余
        data, mime = res
        out.append(f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}")
    return out or None


async def inline_sticker_frames(urls: Sequence[str]) -> list[list[str]]:
    """收藏表情专用内联:每张表情 → 帧组(list[str]),动图抽多帧,静态单帧。

    - 非 http(s) / 抓取失败 / 解码失败的表情整组缺席(组内无元素不产生空组)
    - 帧计入本轮总预算:按表情顺序记账,超预算先丢当前表情的后续帧(保首帧),
      再超则后续表情整组缺席——与 inline_image_urls 的降级哲学一致(少图能答话)
    - `hermes_image_inline_enabled` 关 → 原样透传(每 url 单帧组)
    """
    if not urls:
        return []
    if not plugin_config.hermes_image_inline_enabled:
        return [[u] for u in urls]

    ordered = list(dict.fromkeys(urls))
    max_edge = plugin_config.hermes_image_inline_max_edge
    quality = plugin_config.hermes_image_inline_quality
    per_image_cap = plugin_config.hermes_image_inline_max_bytes
    total_cap = plugin_config.hermes_image_inline_total_max_bytes

    to_fetch = [u for u in ordered if u.startswith(("http://", "https://"))]
    fetched: dict[str, bytes] = {}
    # data: URL 自包含,直接解码入抓取表(与 inline_image_urls 的 passthrough 同位,
    # 但动图仍要过抽帧,不能跳过编码管线)
    for u in ordered:
        if u.startswith("data:"):
            try:
                b64 = u.split(",", 1)[1]
                fetched[u] = base64.b64decode(b64)
            except Exception:
                logger.warning(f"[image_inline] sticker data URL 解码失败,跳过(前 40 字符): {u[:40]}")
    if to_fetch:
        timeout = plugin_config.hermes_image_fetch_timeout_s
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            raws = await asyncio.gather(*(_fetch(client, u) for u in to_fetch))
        fetched = {u: raw for u, raw in zip(to_fetch, raws) if raw is not None}

    # 每张表情的解码/抽帧/编码作为一个线程任务,帧组内顺序由采样序保证。
    encoded_map: dict[str, list[str]] = {}
    targets = {u: fetched[u] for u in ordered if u in fetched}
    if targets:
        results = await asyncio.gather(
            *(asyncio.to_thread(_encode_sticker_one, raw, max_edge, quality, per_image_cap) for raw in targets.values())
        )
        for u, res in zip(targets.keys(), results):
            if res:
                encoded_map[u] = res

    groups: list[list[str]] = []
    total_bytes = 0
    for u in ordered:
        frames = encoded_map.get(u)
        if not frames:
            continue
        kept: list[str] = []
        for f in frames:
            if total_bytes + len(f) > total_cap:
                # 预算见底:丢当前表情的后续帧(保已收帧),并停收后续表情
                logger.warning(f"[image_inline] sticker total budget {total_cap}B exhausted, tail frames dropped")
                break
            kept.append(f)
            total_bytes += len(f)
        if kept:
            groups.append(kept)
        if total_bytes >= total_cap:
            break
    return groups


async def _fetch(client: httpx.AsyncClient, url: str) -> bytes | None:
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content
    except Exception as exc:
        # 截断 URL:平台图片地址常把鉴权 token 带在 query 里,整条进日志会导致凭据落盘泄露。
        logger.warning(f"[image_inline] fetch failed url={url[:80]} ({type(exc).__name__}: {exc})")
        return None


async def inline_image_urls(urls: Sequence[str]) -> list[str]:
    """把 http(s) 图片 URL 抓取并转成 `data:image/jpeg;base64,...`。

    - 已经是 `data:` 的原样保留(自包含,无需重复抓取与编码)
    - 抓取失败 / 解码失败 / 超出单图上限的那张被跳过,其余照常返回
    - 累计超出本轮总预算后不再处理剩余图片

    跳过是刻意的降级:少一张图仍能答话,整轮 400 则什么都没有。
    """
    if not urls or not plugin_config.hermes_image_inline_enabled:
        return list(urls)

    # 保留顺序且高效去重(转发/引用叠加消息中可能出现重复图片)。
    ordered = list(dict.fromkeys(urls))

    max_edge = plugin_config.hermes_image_inline_max_edge
    quality = plugin_config.hermes_image_inline_quality
    per_image_cap = plugin_config.hermes_image_inline_max_bytes
    total_cap = plugin_config.hermes_image_inline_total_max_bytes

    passthrough: dict[str, str] = {}
    to_fetch: list[str] = []
    for u in ordered:
        if u.startswith("data:"):
            passthrough[u] = u
        elif u.startswith(("http://", "https://")):
            to_fetch.append(u)
        else:
            logger.warning(f"[image_inline] unsupported scheme, skipped: {u[:40]}")

    # 并发拉取所有网络图片。
    fetched: dict[str, bytes] = {}
    if to_fetch:
        timeout = plugin_config.hermes_image_fetch_timeout_s
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            raws = await asyncio.gather(*(_fetch(client, u) for u in to_fetch))
        fetched = {u: raw for u, raw in zip(to_fetch, raws) if raw is not None}

    # 在工作线程池中并发执行 CPU 密集的图片归一化与阶梯重编码。
    encoded_map: dict[str, str] = {}
    if fetched:
        encoded_results = await asyncio.gather(
            *(asyncio.to_thread(_encode_one, raw, max_edge, quality, per_image_cap) for raw in fetched.values())
        )
        for u, raw, res in zip(fetched.keys(), fetched.values(), encoded_results):
            if res is None:
                logger.warning(f"[image_inline] dropped oversized/undecodable image url={u[:80]} raw={len(raw)}B")
                continue
            data, mime = res
            encoded = f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}"
            logger.debug(f"[image_inline] inlined url={u[:80]} raw={len(raw)}B -> {len(data)}B")
            encoded_map[u] = encoded

    # 按原始顺序组装输出,并进行本轮总字节预算检查。
    out: list[str] = []
    total_bytes = 0
    for pos, u in enumerate(ordered):
        encoded = passthrough.get(u) or encoded_map.get(u)
        if encoded is None:
            continue
        if total_bytes + len(encoded) > total_cap:
            logger.warning(
                f"[image_inline] total budget {total_cap}B exhausted; "
                f"{len(ordered) - pos} image(s) dropped from this turn"
            )
            break
        out.append(encoded)
        total_bytes += len(encoded)

    return out
