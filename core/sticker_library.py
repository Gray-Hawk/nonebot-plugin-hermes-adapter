"""本地 CSV 表情库：稳定 ID、轻量检索与受限文件读取。"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

from nonebot import logger


_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
_MIME_BY_SUFFIX = {
    ".gif": "image/gif",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}
_MAX_STICKER_BYTES = 25 * 1024 * 1024


class StickerLibraryError(ValueError):
    """表情库条目存在，但文件不能安全读取。"""


@dataclass(frozen=True)
class StickerEntry:
    id: str
    kind: str
    file: str
    note: str
    path: Path
    mime: str


class StickerLibrary:
    """按 index.csv 的 mtime_ns＋size 签名缓存索引，手改后无需重启。"""

    def __init__(self, root: Path | None) -> None:
        self.root = root
        self._signature: tuple[int, int] | None = None
        self._entries: dict[str, StickerEntry] = {}

    @property
    def index_path(self) -> Path | None:
        return self.root / "index.csv" if self.root is not None else None

    def _load(self) -> dict[str, StickerEntry]:
        index = self.index_path
        if index is None or not index.is_file():
            self._signature = None
            self._entries = {}
            return self._entries
        stat = index.stat()
        signature = (stat.st_mtime_ns, stat.st_size)
        if signature == self._signature:
            return self._entries

        entries: dict[str, StickerEntry] = {}
        try:
            with index.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None or not {"id", "kind", "file"}.issubset(reader.fieldnames):
                    raise ValueError("index.csv 缺少 id,kind,file 表头")
                for line_no, row in enumerate(reader, start=2):
                    raw_id = row.get("id")
                    # 本部署补丁：首列 lstrip 后以 # 开头即注释行，整行跳过（在 ID 校验前）；
                    # # 本就不是合法 ID 字符，不会放过原本会被拒的条目。
                    if raw_id is not None and raw_id.lstrip().startswith("#"):
                        continue
                    sticker_id = (raw_id or "").strip()
                    relative = (row.get("file") or "").strip()
                    if _ID_RE.fullmatch(sticker_id) is None:
                        logger.warning(f"[STICKER] index.csv:{line_no} ID 非法，已跳过: {sticker_id!r}")
                        continue
                    if sticker_id in entries:
                        logger.warning(f"[STICKER] index.csv:{line_no} ID 重复，已跳过: {sticker_id}")
                        continue
                    try:
                        path, mime = self._resolve_file(relative)
                    except (StickerLibraryError, OSError) as exc:
                        logger.warning(f"[STICKER] index.csv:{line_no} 文件不可用，已跳过 {sticker_id}: {exc}")
                        continue
                    entries[sticker_id] = StickerEntry(
                        id=sticker_id,
                        kind=(row.get("kind") or "").strip(),
                        file=relative,
                        note=(row.get("note") or "").strip(),
                        path=path,
                        mime=mime,
                    )
        except (OSError, ValueError, csv.Error) as exc:
            logger.warning(f"[STICKER] 表情索引读取失败，按空库处理: {exc}")
            entries = {}

        self._entries = entries
        self._signature = signature
        return self._entries

    def reload(self) -> int:
        """强制重读索引并返回有效条数，供管理员指令显式刷新。"""
        self._signature = None
        return len(self._load())

    def _resolve_file(self, relative: str) -> tuple[Path, str]:
        if self.root is None or not relative or Path(relative).is_absolute():
            raise StickerLibraryError("只接受库目录内的相对路径")
        suffix = Path(relative).suffix.lower()
        mime = _MIME_BY_SUFFIX.get(suffix)
        if mime is None:
            raise StickerLibraryError("不支持的图片扩展名")
        root = self.root.resolve(strict=True)
        path = (self.root / relative).resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file():
            raise StickerLibraryError("文件路径越出库目录或不是普通文件")
        return path, mime

    def search(self, query: str = "", *, limit: int = 20) -> tuple[list[StickerEntry], int]:
        """返回当前页与过滤后的总数；limit 固定在 1～50。"""
        needle = (query or "").strip().casefold()
        rows = [
            row
            for row in self._load().values()
            if not needle or needle in f"{row.id}\n{row.kind}\n{row.note}".casefold()
        ]
        bounded = max(1, min(50, int(limit)))
        return rows[:bounded], len(rows)

    def kinds(self) -> list[str]:
        """返回去重、稳定排序的非空类型，供提示词按索引签名热更新。"""
        return sorted({row.kind for row in self._load().values() if row.kind})

    def get(self, sticker_id: str) -> StickerEntry | None:
        return self._load().get(sticker_id)

    def read(self, sticker_id: str) -> tuple[bytes, str]:
        entry = self.get(sticker_id)
        if entry is None:
            raise StickerLibraryError(f"未知表情 ID: {sticker_id}")
        # 索引建立后文件仍可能被替换或改成越库符号链接，读取前重新过同一道闸。
        path, mime = self._resolve_file(entry.file)
        size = path.stat().st_size
        if size <= 0 or size > _MAX_STICKER_BYTES:
            raise StickerLibraryError(f"图片大小无效或超过 25MB: {size}")
        return path.read_bytes(), mime


_DEFAULT_LIBRARY: StickerLibrary | None = None
_STICKER_FEATURE_ENABLED = False


def sticker_feature_enabled() -> bool:
    """返回启动时定型的表情总开关。

    设计如此：开关只在启动初始化时读取一次并固化；``/reload-config`` 只替换配置
    快照、不会调用 :func:`init_sticker_feature`，故改 ``sticker_enabled`` 需重启
    进程生效，避免工具注册、提示词与出站半套热切换。
    """
    return _STICKER_FEATURE_ENABLED


def init_sticker_feature() -> None:
    """在配置快照初始化后固定本进程的工具、提示词与出站开关（仅启动时调用）。"""
    global _STICKER_FEATURE_ENABLED
    from .misc_config import load_misc_config

    _STICKER_FEATURE_ENABLED = load_misc_config().sticker_enabled


def default_sticker_index_path() -> Path | None:
    from ..config import resolve_custom_dir

    custom_dir = resolve_custom_dir()
    return custom_dir / "sticker_library" / "index.csv" if custom_dir else None


def ensure_sticker_library_template() -> bool:
    """启动时缺档创建目录和 CSV 范本；已有索引绝不覆盖。"""
    path = default_sticker_index_path()
    if path is None:
        return False
    from .config_bootstrap import ensure_file

    return ensure_file(
        path,
        "id,kind,file,note\n"
        "# 示例行为注释（整行跳过）：表头须保持第一行，图片放本目录，按四列填写真实条目。\n"
        "# speechless,无语|无奈,speechless.gif,被离谱消息整无语时使用\n",
        log_tag="HERMES sticker",
        label="表情索引范本",
    )


def default_sticker_library() -> StickerLibrary:
    """缺省使用 <custom_dir>/sticker_library；可用性由 sticker_enabled 启动闸决定。"""
    global _DEFAULT_LIBRARY
    if _DEFAULT_LIBRARY is None:
        from ..config import resolve_custom_dir

        custom_dir = resolve_custom_dir()
        _DEFAULT_LIBRARY = StickerLibrary(custom_dir / "sticker_library" if custom_dir else None)
    return _DEFAULT_LIBRARY
