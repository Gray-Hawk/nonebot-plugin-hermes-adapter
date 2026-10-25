"""媒体投送配置：`<hermes_custom_dir>/media_config.yaml`。

「图片投送管线三模式改造」的配置载体（设计定案与施工规划见
项目文档库）：

- 三模式：full 全内联 / semi 半内联（前后窗口内有 @bot 才内联）/ off 全占位符
- 例外名单：按用户 id 覆盖内联行为——off=永不内联（可拉图）/ semi=按半内联规则收窄；
  优先级低于总 mode（总 off 全灭）。full 值不实装，
  解析层遇 full 按未支持值 WARN 跳过
- 压缩档、拉图大小门、抽帧帧数、表情门控全部在此，不进 .env

S2 起并入 v6 七键（media_allow_dirs / local_file_max_mb / sticker_vision_enabled /
image_cache_dir / image_cache_quota_mb / image_fetch_timeout_s / image_fetch_max_attempts），
与既有 mode、inline 等共同校验；七键以本文件为唯一配置源，缺省使用内置默认值。

格式约定：人改的配置一律 YAML（可注释），纯机器读的才用 JSON。

加载纪律（S2 起改为启动缓存 + 显式重载，不再每次读盘）：缺档自动建档，
坏档完整校验失败时保留旧缓存（重载路径）或回退默认（启动路径）。字段类型错
按字段回退默认。此文件缺档时只写一次模板，手动重载不重排、不覆盖用户已有文件。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nonebot import logger

from ..config import resolve_custom_dir
from .config_fields import bool_field, int_field, str_field
from .config_store import ConfigSnapshotStore
from .config_templates import MEDIA_CONFIG_TEMPLATE

_CONFIG_FILENAME = "media_config.yaml"

# 缺省值
_DEFAULT_MODE = "semi"
_DEFAULT_SEMI_WINDOW = 3
_DEFAULT_INLINE_LADDER: tuple[tuple[int, int], ...] = ((2048, 80), (1024, 70))
_DEFAULT_INLINE_MAX_BYTES = 500_000
_DEFAULT_INLINE_GIF_MAX_FRAMES = 5
_DEFAULT_FETCH_GATE_BYTES = 2_000_000
_DEFAULT_FETCH_LADDER: tuple[tuple[int, int], ...] = ((4096, 95), (4096, 70))
_DEFAULT_FETCH_GIF_MAX_FRAMES = 10
_DEFAULT_STICKER_GATE_BYTES = 200_000
_DEFAULT_FORWARD_IMAGE_CAP = 30
"""单条消息合并转发图的注册上限:超出截断 + WARN,按内容去重。
转发图只注册发号、按需拉图,恒不内联(见规划书 §5 Q3 / §8)。"""

_VALID_MODES = ("full", "semi", "off")

# 例外名单实装两档。
# off=该用户图永不内联(可拉图);semi=该用户图按半内联规则收窄
# (总 full 下不再恒收,只在 semi 窗口命中时收;总 semi 下与默认一致)。
_EXEMPTION_MODES = ("off", "semi")


@dataclass(frozen=True)
class MediaConfig:
    """媒体投递配置快照。frozen：单轮内共享一份，显式重载后才更换。"""

    mode: str = _DEFAULT_MODE
    """投送总模式：full=全内联直通；semi=窗口内有 @bot 才内联；off=全部占位符。"""
    semi_window: int = _DEFAULT_SEMI_WINDOW
    """semi 模式的窗口半径：当前消息前后各 N 条内存在 @bot 才内联。"""
    inline_ladder: tuple[tuple[int, int], ...] = _DEFAULT_INLINE_LADDER
    """内联压缩梯度 (max_edge, quality)，逐级从原图重压。"""
    inline_max_bytes: int = _DEFAULT_INLINE_MAX_BYTES
    """内联单图压缩后字节上限（大小门），梯度走完仍超则回退占位符。"""
    inline_gif_max_frames: int = _DEFAULT_INLINE_GIF_MAX_FRAMES
    """内联动图抽帧上限。"""
    fetch_gate_bytes: int = _DEFAULT_FETCH_GATE_BYTES
    """主动拉图大小门：原图不超过则直出，超过走 fetch 梯度压缩。"""
    fetch_ladder: tuple[tuple[int, int], ...] = _DEFAULT_FETCH_LADDER
    """拉图撞门后的压缩梯度（与内联独立，可给更高质量档）。"""
    fetch_gif_max_frames: int = _DEFAULT_FETCH_GIF_MAX_FRAMES
    """拉图动图抽帧上限。"""
    sticker_gate_bytes: int = _DEFAULT_STICKER_GATE_BYTES
    """表情门控：不超过直出，超过强制走 fetch 梯度压缩（防巨型表情连发压预算）。"""
    forward_image_cap: int = _DEFAULT_FORWARD_IMAGE_CAP
    """单条消息合并转发图的注册上限。超出截断（按内容去重）+ WARN。转发图恒不内联。"""
    exemptions: dict[str, str] = field(default_factory=dict)
    """例外名单：用户 id → "off"(永不内联,可拉图) / "semi"(按半内联规则收窄——
    总 full 下不再恒收,只在 semi 窗口命中时收;总 semi 下与默认一致)。全局生效。
    full 值不实装,解析层按未支持值跳过。"""

    # --- v6 迁入的 7 键（旧 .env MEDIA/IMAGE 系列） ---
    media_allow_dirs: str = ""
    """额外允许发送的本地文件与图片根目录白名单（os.pathsep 分隔，支持 ~）。"""
    local_file_max_mb: int = 50
    """本地文件通过群文件或私聊文件上传时的尺寸上限（MB）。"""
    sticker_vision_enabled: bool = False
    """是否抓取收藏表情并内联给模型，默认关闭。"""
    image_cache_dir: str = ""
    """图片字节缓存根目录，缺省跟随 AGENT_PLUGIN_CUSTOM_DIR，也可独立覆盖。重启生效。"""
    image_cache_quota_mb: int = 200
    """图片缓存总容量上限（MB），超出后按访问时间从旧到新淘汰。重启生效。"""
    image_fetch_timeout_s: int = 10
    """单张图片 HTTP 抓取超时秒数。重启生效。"""
    image_fetch_max_attempts: int = 2
    """单张图片总抓取尝试次数，包含首次尝试。重启生效。"""

    def exemption_for(self, user_id: str) -> str | None:
        """查例外名单；不在名单返回 None。总 mode=off 时调用方应先短路，不查名单。"""
        return self.exemptions.get(user_id)


def _parse_ladder(raw: Any, default: tuple[tuple[int, int], ...], name: str) -> tuple[tuple[int, int], ...]:
    """解析压缩梯度 [[edge, quality], ...]；坏值整体回退默认。"""
    if not isinstance(raw, list) or not raw:
        return default
    steps: list[tuple[int, int]] = []
    for item in raw:
        if (
            isinstance(item, list)
            and len(item) == 2
            and isinstance(item[0], int)
            and isinstance(item[1], int)
            and item[0] > 0
            and 1 <= item[1] <= 100
        ):
            steps.append((item[0], item[1]))
    if not steps:
        logger.warning(f"[HERMES media-config] {name} 无有效梯度级,回退默认")
        return default
    return tuple(steps)


_last_exempt_warn: str = ""


def _parse_exemptions(raw: Any) -> dict[str, str]:
    """例外名单：{用户id: 模式}；id 只允许数字/字母/下划线/横线。

    实装两档:off / semi。其余值(含 full)按未支持值
    跳过 + WARN;WARN 按内容去重,避免一个坏条目在反复重载时刷屏。
    """
    global _last_exempt_warn
    out: dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    skipped: list[str] = []
    for k, v in raw.items():
        key = str(k).strip()
        if not key or not all(c.isalnum() or c in "_-" for c in key):
            continue
        if isinstance(v, str) and v in _EXEMPTION_MODES:
            out[key] = v
        else:
            skipped.append(f"{key!r}: {v!r}")
    if skipped:
        warn = f"[HERMES media-config] 例外条目模式不支持,已跳过: {', '.join(skipped)}"
        if warn != _last_exempt_warn:
            logger.warning(warn)
            _last_exempt_warn = warn
    return out


def _int_field(doc: dict, *path: str, default: int, min_v: int = 1) -> int:
    """按路径取 int 字段，缺失/类型错/越界回退默认。"""
    node: Any = doc
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return default
        node = node[key]
    if isinstance(node, bool) or not isinstance(node, int) or node < min_v:
        return default
    return node


def _builtin_defaults() -> dict[str, Any]:
    """七个迁入键的运行时缺省；YAML 是唯一配置源，不再回落旧 ENV。"""
    return {
        "media_allow_dirs": "",
        "local_file_max_mb": 50,
        "sticker_vision_enabled": False,
        "image_cache_dir": "",
        "image_cache_quota_mb": 200,
        "image_fetch_timeout_s": 10,
        "image_fetch_max_attempts": 2,
    }


def _default_media_config() -> MediaConfig:
    d = _builtin_defaults()
    return MediaConfig(
        media_allow_dirs=d["media_allow_dirs"],
        local_file_max_mb=d["local_file_max_mb"],
        sticker_vision_enabled=d["sticker_vision_enabled"],
        image_cache_dir=d["image_cache_dir"],
        image_cache_quota_mb=d["image_cache_quota_mb"],
        image_fetch_timeout_s=d["image_fetch_timeout_s"],
        image_fetch_max_attempts=d["image_fetch_max_attempts"],
    )


def parse_media_config(doc: dict) -> MediaConfig:
    """解析 media_config.yaml 顶层映射为 MediaConfig 快照。

    既有嵌套结构（inline/fetch/sticker/exemptions）与 v6 七键同验；
    七键使用内置缺省值，字段级坏值回退默认。
    """
    d = _builtin_defaults()
    mode = doc.get("mode")
    if mode not in _VALID_MODES:
        if mode is not None:
            logger.warning(f"[HERMES media-config] mode 非法回退默认: {mode!r}")
        mode = _DEFAULT_MODE

    return MediaConfig(
        mode=mode,
        semi_window=_int_field(doc, "semi_window", default=_DEFAULT_SEMI_WINDOW),
        inline_ladder=_parse_ladder(doc.get("inline", {}).get("ladder") if isinstance(doc.get("inline"), dict) else None, _DEFAULT_INLINE_LADDER, "inline.ladder"),
        inline_max_bytes=_int_field(doc, "inline", "max_bytes", default=_DEFAULT_INLINE_MAX_BYTES),
        inline_gif_max_frames=_int_field(doc, "inline", "gif_max_frames", default=_DEFAULT_INLINE_GIF_MAX_FRAMES),
        fetch_gate_bytes=_int_field(doc, "fetch", "gate_bytes", default=_DEFAULT_FETCH_GATE_BYTES),
        fetch_ladder=_parse_ladder(doc.get("fetch", {}).get("ladder") if isinstance(doc.get("fetch"), dict) else None, _DEFAULT_FETCH_LADDER, "fetch.ladder"),
        fetch_gif_max_frames=_int_field(doc, "fetch", "gif_max_frames", default=_DEFAULT_FETCH_GIF_MAX_FRAMES),
        sticker_gate_bytes=_int_field(doc, "sticker", "gate_bytes", default=_DEFAULT_STICKER_GATE_BYTES),
        forward_image_cap=_int_field(doc, "forward_image_cap", default=_DEFAULT_FORWARD_IMAGE_CAP),
        exemptions=_parse_exemptions(doc.get("exemptions")),
        media_allow_dirs=str_field(doc, "media_allow_dirs", d["media_allow_dirs"]),
        local_file_max_mb=int_field(doc, "local_file_max_mb", d["local_file_max_mb"], min_v=1),
        sticker_vision_enabled=bool_field(doc, "sticker_vision_enabled", d["sticker_vision_enabled"]),
        image_cache_dir=str_field(doc, "image_cache_dir", d["image_cache_dir"]),
        image_cache_quota_mb=int_field(doc, "image_cache_quota_mb", d["image_cache_quota_mb"], min_v=1),
        image_fetch_timeout_s=int_field(doc, "image_fetch_timeout_s", d["image_fetch_timeout_s"], min_v=1),
        image_fetch_max_attempts=int_field(doc, "image_fetch_max_attempts", d["image_fetch_max_attempts"], min_v=1),
    )


def _path():
    base = resolve_custom_dir()
    return base / _CONFIG_FILENAME if base else None


_store = ConfigSnapshotStore(
    label="media_config",
    path_provider=_path,
    parser=parse_media_config,
    default_factory=_default_media_config,
    default_content=MEDIA_CONFIG_TEMPLATE,
)


def load_media_config() -> MediaConfig:
    """返回启动加载或最近一次显式重载后的缓存快照；不再每次读盘。"""
    return _store.get()
