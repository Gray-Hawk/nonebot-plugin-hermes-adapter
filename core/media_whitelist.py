"""本部署补丁: 本地文件投递白名单校验 + 本地图片内联。

两个用途:
  1. inline_local_media_images —— 流式收口的 MEDIA:/本地图片路径 → data URL
     (补上游 _resolve_media_to_data_urls 只在非流式路径执行的缺口)。
  2. validate_local_file —— send_local_files(outbound) 上传群/私聊文件前的判定。

安全思路与上游 validate_media_delivery_path 相反: 上游是 denylist(凭据清单随版本
演进, 复刻必漂移), 插件侧是**白名单**——只有落在白名单根内的已存在普通文件才放行。
宁严勿宽: 判不了就退回 [生成了文件] 占位符, 永远不可能把不该发的文件发出去。
默认白名单 = ~/.hermes/cache/scratch + hermes 内建媒体缓存目录; 工作目录刻意不进
默认(2026-10-08 维护者拍板), 由 hermes_media_allow_dirs 配置项追加。
"""

from __future__ import annotations

import base64
import os
import re
from pathlib import Path

from nonebot import logger

from ..config import plugin_config
from .media_patterns import _MD_IMAGE_PATTERN, _MEDIA_TAG_PATTERN

# 图片内联的尺寸上限, 与上游 _MEDIA_DATA_URL_MAX_BYTES 对齐(5MB)。
_INLINE_IMAGE_MAX_BYTES = 5 * 1024 * 1024

_IMAGE_MIME = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".bmp": "image/bmp",
}

# 出站媒体引用形态正则已收口至 core/media_patterns.py 单点定义(2026-10-16 同源化
# 收口批,审计确认8):hermes_client 的提取与本模块的内联共用同一组,原先两文件各
# 一份「为回避循环 import 独立一份」的形态消除。import 见文件头。


def _default_roots() -> list[Path]:
    home = Path.home()
    hermes = home / ".hermes"
    roots = [hermes / "cache" / "scratch"]
    # 与上游 MEDIA_DELIVERY_SAFE_ROOTS 对齐的两代缓存布局。
    for d in ("images", "audio", "videos", "documents", "screenshots"):
        roots.append(hermes / d)
    for d in ("image_cache", "audio_cache", "video_cache", "document_cache", "browser_screenshots"):
        roots.append(hermes / d)
    for d in ("images", "audio", "videos", "documents", "screenshots"):
        roots.append(hermes / "cache" / d)
    return roots


def allowed_roots() -> list[Path]:
    """默认白名单根 + hermes_media_allow_dirs 配置的追加项(逐次 resolve)。"""
    roots = list(_default_roots())
    extra = plugin_config.hermes_media_allow_dirs or ""
    for chunk in extra.split(os.pathsep):
        chunk = chunk.strip()
        if not chunk:
            continue
        roots.append(Path(os.path.expanduser(chunk)))
    resolved: list[Path] = []
    for r in roots:
        try:
            resolved.append(r.resolve(strict=False))
        except (OSError, RuntimeError, ValueError):
            continue
    return resolved


def validate_local_file(raw_path: str, *, max_bytes: int) -> Path | None:
    """白名单判定: resolve 后落在白名单根内 + 已存在的普通文件 + 不超尺寸。

    任何一步不过都返回 None(调用方退回占位符/跳过), 不区分原因——
    区分原因只值一条 debug 日志, 不值得让调用方分支。
    """
    try:
        p = Path(os.path.expanduser(raw_path.strip().strip('"').strip("'")))
    except (OSError, RuntimeError, ValueError):
        return None
    if not p.is_absolute():
        return None
    try:
        resolved = p.resolve(strict=True)  # 解 symlink, 不存在即 None
    except (OSError, RuntimeError, ValueError):
        return None
    if not resolved.is_file():
        return None
    try:
        if resolved.stat().st_size > max_bytes:
            return None
    except OSError:
        return None
    for root in allowed_roots():
        if resolved == root or root in resolved.parents:
            return resolved
    return None


def _image_to_data_url(path: Path) -> str | None:
    mime = _IMAGE_MIME.get(path.suffix.lower())
    if mime is None:
        return None
    try:
        b64 = base64.b64encode(path.read_bytes()).decode()
    except OSError:
        return None
    return f"data:{mime};base64,{b64}"


def inline_local_media_images(text: str) -> str:
    """把文本里的本地图片引用(MEDIA:/path 与 ![..](/path))换成 data:image URL。

    只处理白名单内、≤5MB、图片扩展名的引用; 其余(白名单外/非图片/不存在/超限)
    原样保留——下游 extract_response_media 会把它们收成 file_paths 或换占位符,
    行为与现状一致。已是 http(s)/data: 的引用不动。
    """
    if not text or ("MEDIA:" not in text and "](" not in text):
        return text

    def _convert(ref: str) -> str | None:
        if ref.startswith(("http://", "https://", "data:")):
            return None
        p = validate_local_file(ref, max_bytes=_INLINE_IMAGE_MAX_BYTES)
        if p is None:
            return None
        return _image_to_data_url(p)

    def _md_repl(m: re.Match) -> str:
        url = _convert(m.group(2))
        return f"![{m.group(1)}]({url})" if url else m.group(0)

    def _tag_repl(m: re.Match) -> str:
        # 包成 markdown 图片形态,与非流式上游 _resolve_media_to_data_urls 的
        # ![image](data:...) 输出对齐:裸 data: URL 不匹配 extract_response_media
        # 的 _MD_IMAGE_PATTERN/_MEDIA_TAG_PATTERN,会原样留在文本里发进群。
        url = _convert(m.group(1))
        return f"![image]({url})" if url else m.group(0)

    try:
        out = _MD_IMAGE_PATTERN.sub(_md_repl, text)
        out = _MEDIA_TAG_PATTERN.sub(_tag_repl, out)
        return out
    except Exception:
        logger.warning("[HERMES] 本地图片内联异常, 返回原文", exc_info=True)
        return text
