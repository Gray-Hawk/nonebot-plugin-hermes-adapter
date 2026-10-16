"""媒体投送配置：`<hermes_custom_dir>/media_config.yaml`，现读现用（保存即生效）。

2026-10-15 维护者定稿「图片投送管线三模式改造」的配置载体（设计定案与施工规划见
项目文档库）：

- 三模式：full 全内联 / semi 半内联（前后窗口内有 @bot 才内联）/ off 全占位符
- 例外名单：按用户 id 覆盖内联行为（如 bot 账号永不内联），优先级低于总 mode
- 压缩档、拉图大小门、抽帧帧数、表情门控全部在此，不进 .env

格式约定（维护者 2026-10-15 定）：人改的配置一律 YAML（可注释），纯机器读的才用 JSON。

纪律：文件缺失/坏档 → 全默认值（WARN 一次，内容变化才再警告）；字段类型错
按字段回退默认。此文件只读不写，任何字段缺失都有缺省。
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from typing import Any

from nonebot import logger

from ..config import resolve_custom_dir

_CONFIG_FILENAME = "media_config.yaml"

# 缺省值（2026-10-15 维护者拍板）
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
"""单条消息合并转发图的注册上限(2026-10-15 定案):超出截断 + WARN,按内容去重。
转发图只注册发号、按需拉图,恒不内联(见规划书 §5 Q3 / §8)。"""

_VALID_MODES = ("full", "semi", "off")


@dataclass(frozen=True)
class MediaConfig:
    """媒体投送配置快照。frozen：单轮内共享一份，改文件下轮生效。"""

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
    """例外名单：用户 id → "off"(永不内联,可拉图) / "semi" / "full"。全局生效。"""

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


def _parse_exemptions(raw: Any) -> dict[str, str]:
    """例外名单：{用户id: 模式}；id 只允许数字/字母/下划线/横线，模式非法跳过。"""
    out: dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        key = str(k).strip()
        if not key or not all(c.isalnum() or c in "_-" for c in key):
            continue
        if isinstance(v, str) and v in _VALID_MODES:
            out[key] = v
        else:
            logger.warning(f"[HERMES media-config] 例外条目模式非法跳过 ({key!r}): {v!r}")
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


_last_warn: str = ""


def load_media_config() -> MediaConfig:
    """加载 media_config.yaml。文件缺失/坏档 → 全默认（WARN 按内容去重）。

    现读现用：每次调用现读文件，保存即生效，无需重启 nb。
    """
    global _last_warn
    custom_dir = resolve_custom_dir()
    path = custom_dir / _CONFIG_FILENAME if custom_dir else None
    if path is None or not path.is_file():
        return MediaConfig()
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            raise ValueError("顶层必须是映射(YAML mapping)")
    except Exception as exc:
        warn = f"[HERMES media-config] 配置文件不可用,回退全默认: {exc}"
        if warn != _last_warn:
            logger.warning(warn)
            _last_warn = warn
        return MediaConfig()

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
    )
