"""把图片转成自包含的 `data:` URL,供多模态 parts 直接携带。

为什么不能直接把平台图片 URL 交出去:Hermes 对 http(s) 图片 URL 是**纯透传**,
自己不下载,原样转给 provider。于是链路上每一环都可能失败,且失败形态不统一 ——
provider 拉不到会把整轮打成 400;Anthropic 形态转成 `{"type":"url"}` 仍由对端拉取;
而 Gemini native 会**静默丢弃**非 `data:` 的 part,Bedrock 把它降级成一行文本。
平台侧的 URL 还普遍带时效凭据或鉴权 token,交出去既会过期也会外泄。

内联不会增加上游持久化体积:Hermes 落库前把 image part 投影成文本占位,
图片字节不进它的会话库。代价只在单次请求体上。

压缩参数全部来自 media_config.yaml(2026-10-15 维护者定稿,见
项目文档库):
- 梯度 (max_edge, quality) 逐级从**原图**重压(不叠压),压不进 cap 明确缺席
  (回退占位符,少一张图仍能答话,压糊的图不是「这张图」)
- 动图一律走抽帧通道(帧组),不再取首帧——首帧丢失后续帧信息
- 单图字节 cap 是唯一预算闸(无总量上限)
"""

from __future__ import annotations

import asyncio
import base64
import io
from collections.abc import Sequence

import httpx
from nonebot import logger

from .media_config import MediaConfig, load_media_config

# --- 抽帧采样(通用动图通道,原表情专用,2026-10-15 通用化) ---
# 背景:GIF 直输会被静态化且失真(帧叠化/取尾帧),动图感知只有抽帧喂多张
# 静态图一条路;多帧理解已实测通过(按序描述+动作弧正确)。


def sample_frame_indices(n_frames: int, max_frames: int) -> list[int]:
    """动图采样帧序号:n<=max_frames 全抽;否则等距 max_frames 帧(含首末帧,升序)。纯函数。"""
    if n_frames <= 0 or max_frames <= 0:
        return []
    if n_frames <= max_frames:
        return list(range(n_frames))
    return sorted({round(i * (n_frames - 1) / (max_frames - 1)) for i in range(max_frames)})


def is_animated_bytes(raw: bytes) -> bool:
    """快速判断字节是否为动图(GIF/APNG/多帧)。解码失败按 False(静图路径会再报一次)。"""
    from PIL import Image

    try:
        with Image.open(io.BytesIO(raw)) as im:
            return bool(getattr(im, "is_animated", False)) and getattr(im, "n_frames", 1) > 1
    except Exception:
        return False


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


def _normalize_image(raw: bytes) -> "Image.Image | None":
    """解码原始字节为摆正方向、消除透明通道的 RGB 图像对象(静图)。

    动图不在此处理——调用方先用 is_animated_bytes 判动图并分流到抽帧通道。
    同步 CPU 密集操作,必须在工作线程中调用。
    """
    from PIL import Image, ImageOps

    try:
        with Image.open(io.BytesIO(raw)) as im:
            # EXIF 方向必须在缩放前应用,否则竖拍照片会以横向送进模型。
            im = ImageOps.exif_transpose(im)
            return _composite_rgb(im)
    except Exception as exc:
        logger.warning(f"[image_inline] decode failed ({type(exc).__name__}: {exc})")
        return None


def _encode_image_ladder(
    base_im: "Image.Image", ladder: Sequence[tuple[int, int]], per_image_cap: int
) -> tuple[bytes, str] | None:
    """已归一化的 RGB 图像 → 按梯度逐级(从原图)下采样+JPEG 重编码。全级超限返回 None。

    梯度每级 (max_edge, quality):小图不放大;每级都从 base_im 重压而非在上一级
    产物上叠压(维护者 2026-10-15 大小门设计:二次降档基于原图)。
    optimize=True 生成最优霍夫曼编码表;不写 exif 参数即剔除全部地理位置等隐私元数据。
    """
    from PIL import Image

    for edge, q in ladder:
        try:
            target_im = base_im
            if max(base_im.size) > edge:
                target_im = base_im.copy()
                target_im.thumbnail((edge, edge), Image.LANCZOS)

            buf = io.BytesIO()
            target_im.save(buf, format="JPEG", quality=q, optimize=True)
            out = buf.getvalue()
            if len(out) <= per_image_cap:
                return out, "image/jpeg"
        except Exception as exc:
            logger.warning(f"[image_inline] encode failed ({type(exc).__name__}: {exc})")
            return None
    return None


def encode_still_with_ladder(
    raw: bytes, ladder: Sequence[tuple[int, int]], per_image_cap: int
) -> tuple[bytes, str] | None:
    """静图:解码 → 摆正 → 梯度重压。返回 (bytes, mime);无法处理或全级超限返回 None。

    同步 CPU 密集,调用方须放进线程。动图请先判 is_animated_bytes 走抽帧。
    """
    base_im = _normalize_image(raw)
    if base_im is None:
        return None
    return _encode_image_ladder(base_im, ladder, per_image_cap)


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


def encode_gif_frames_bytes(
    raw: bytes,
    ladder: Sequence[tuple[int, int]],
    per_image_cap: int,
    max_frames: int,
) -> list[tuple[bytes, str]] | None:
    """一张动图 → 采样帧 (bytes, mime) 列表。失败返回 None。

    bytes 级接口供 MCP 出站等自行包装的场景;内联路径用 encode_gif_frames
    (data URL 包装)。单帧编码失败只丢该帧,保其余。
    """
    from PIL import Image

    try:
        with Image.open(io.BytesIO(raw)) as im:
            n_frames = getattr(im, "n_frames", 1)
    except Exception as exc:
        logger.warning(f"[image_inline] gif decode failed ({type(exc).__name__}: {exc})")
        return None

    indices = sample_frame_indices(n_frames, max_frames)
    if not indices:
        return None
    try:
        frames = _extract_frames_rgb(raw, indices)
    except Exception as exc:
        logger.warning(f"[image_inline] frame extract failed ({type(exc).__name__}: {exc})")
        return None
    out: list[tuple[bytes, str]] = []
    for frame in frames:
        res = _encode_image_ladder(frame, ladder, per_image_cap)
        if res is None:
            continue
        out.append(res)
    return out or None


def encode_gif_frames(
    raw: bytes,
    ladder: Sequence[tuple[int, int]],
    per_image_cap: int,
    max_frames: int,
) -> list[str] | None:
    """一张动图 → 帧组 data URL 列表(采样帧)。失败返回 None。

    返回帧组(而不是扁平图列)是防张冠李戴的结构保证:一张动图的所有帧
    始终装在同一组里,标签由 prompt_builder 按组内位置生成,跨图不可能错位。
    """
    frames = encode_gif_frames_bytes(raw, ladder, per_image_cap, max_frames)
    if not frames:
        return None
    return [f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}" for data, mime in frames]


def encode_image_frames_bytes(
    raw: bytes,
    ladder: Sequence[tuple[int, int]],
    per_image_cap: int,
    gif_max_frames: int,
) -> list[tuple[bytes, str]] | None:
    """统一编码入口(bytes 级):静图 → 单帧 (bytes,mime);动图 → 抽帧组。失败 None。"""
    if is_animated_bytes(raw):
        return encode_gif_frames_bytes(raw, ladder, per_image_cap, gif_max_frames)
    res = encode_still_with_ladder(raw, ladder, per_image_cap)
    return [res] if res else None


def encode_image_frames(
    raw: bytes,
    ladder: Sequence[tuple[int, int]],
    per_image_cap: int,
    gif_max_frames: int,
) -> list[str] | None:
    """统一编码入口:静图 → 单帧组;动图 → 抽帧组。失败返回 None(调用方回退占位符)。

    同步 CPU 密集,调用方须放进线程。
    """
    frames = encode_image_frames_bytes(raw, ladder, per_image_cap, gif_max_frames)
    if not frames:
        return None
    return [f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}" for data, mime in frames]


def sniff_mime(raw: bytes) -> str:
    """魔数嗅探图片格式(直出路径用,不经过重编码时需要真实 mime)。"""
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if raw[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if raw[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image/webp"
    return "application/octet-stream"


async def _fetch(client: httpx.AsyncClient, url: str) -> bytes | None:
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content
    except Exception as exc:
        # 截断 URL:平台图片地址常把鉴权 token 带在 query 里,整条进日志会导致凭据落盘泄露。
        logger.warning(f"[image_inline] fetch failed url={url[:80]} ({type(exc).__name__}: {exc})")
        return None


async def _fetch_all(urls: Sequence[str]) -> dict[str, bytes]:
    """并发抓取 http(s) 图片;data: URL 直接解码入表。失败的缺席。"""
    ordered = list(dict.fromkeys(urls))
    fetched: dict[str, bytes] = {}
    for u in ordered:
        if u.startswith("data:"):
            try:
                fetched[u] = base64.b64decode(u.split(",", 1)[1])
            except Exception:
                logger.warning(f"[image_inline] data URL 解码失败,跳过(前 40 字符): {u[:40]}")
    to_fetch = [u for u in ordered if u.startswith(("http://", "https://"))]
    if to_fetch:
        from ..config import plugin_config

        timeout = plugin_config.hermes_image_fetch_timeout_s
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            raws = await asyncio.gather(*(_fetch(client, u) for u in to_fetch))
        for u, raw in zip(to_fetch, raws):
            if raw is not None:
                fetched[u] = raw
    for u in ordered:
        if not u.startswith(("http://", "https://", "data:")):
            logger.warning(f"[image_inline] unsupported scheme, skipped: {u[:40]}")
    return fetched


async def inline_image_frames(
    urls: Sequence[str], *, cfg: MediaConfig | None = None
) -> list[list[str]]:
    """图片内联统一入口:每张图 → 帧组(静图单帧,动图抽帧组),按输入顺序。

    - 压缩参数:cfg.inline_ladder / inline_max_bytes / inline_gif_max_frames
    - 抓取失败/解码失败/梯度全级超限的图整组缺席(调用方据缺席数补占位符)
    - 无总量预算(维护者 2026-10-15 定:只有单图上限)
    """
    if not urls:
        return []
    cfg = cfg or load_media_config()
    ordered = list(dict.fromkeys(urls))
    fetched = await _fetch_all(ordered)

    targets = {u: fetched[u] for u in ordered if u in fetched}
    encoded_map: dict[str, list[str]] = {}
    if targets:
        results = await asyncio.gather(
            *(
                asyncio.to_thread(
                    encode_image_frames,
                    raw,
                    cfg.inline_ladder,
                    cfg.inline_max_bytes,
                    cfg.inline_gif_max_frames,
                )
                for raw in targets.values()
            )
        )
        for u, raw, res in zip(targets.keys(), targets.values(), results):
            if res is None:
                logger.warning(f"[image_inline] dropped oversized/undecodable image url={u[:80]} raw={len(raw)}B")
                continue
            encoded_map[u] = res

    return [encoded_map[u] for u in ordered if u in encoded_map]


async def inline_gif_frames(
    urls: Sequence[str], *, cfg: MediaConfig | None = None, max_frames: int | None = None
) -> list[list[str]]:
    """通用抽帧内联通道(原表情专用 inline_sticker_frames,2026-10-15 通用化改名)。

    每张图 → 帧组(动图采样帧,静态单帧)。帧数上限:max_frames 入参优先,
    否则 cfg.inline_gif_max_frames(内联档缺省 5;拉图档 10 由调用方传入)。
    固定走内联档压缩参数;表情大图的压缩档由调用方按门控选档后借道
    encode_image_frames,不经本函数。
    """
    if not urls:
        return []
    cfg = cfg or load_media_config()
    frames_cap = max_frames or cfg.inline_gif_max_frames
    ordered = list(dict.fromkeys(urls))
    fetched = await _fetch_all(ordered)

    targets = {u: fetched[u] for u in ordered if u in fetched}
    encoded_map: dict[str, list[str]] = {}
    if targets:
        results = await asyncio.gather(
            *(
                asyncio.to_thread(
                    encode_image_frames,
                    raw,
                    cfg.inline_ladder,
                    cfg.inline_max_bytes,
                    frames_cap,
                )
                for raw in targets.values()
            )
        )
        for u, res in zip(targets.keys(), results):
            if res:
                encoded_map[u] = res

    return [encoded_map[u] for u in ordered if u in encoded_map]
