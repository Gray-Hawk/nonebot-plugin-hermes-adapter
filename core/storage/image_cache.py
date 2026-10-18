"""文件系统图字节缓存。

按 SHA256 内容寻址命名,LRU 按 atime 淘汰直至总大小符合配额。

并发安全:put 用 tmpfile + 原子 rename,并发写同 sha 不损坏。
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from nonebot import logger

_MIME_TO_EXT = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
}
# 已知扩展 → 标准 MIME 的反向表。未知扩展回退到 application/octet-stream。
_EXT_TO_MIME = {
    "jpg": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp",
    "gif": "image/gif",
    "bin": "application/octet-stream",
}
# get_bytes 扫文件时按这个顺序试扩展名,bin 放最后是因为正常图都用具体 ext。
_KNOWN_EXTS = ("jpg", "png", "webp", "gif", "bin")


def _mime_to_ext(mime: str) -> str:
    normalized = (mime or "").split(";", 1)[0].strip().lower()
    return _MIME_TO_EXT.get(normalized, "bin")


def _ext_to_mime(ext: str) -> str:
    return _EXT_TO_MIME.get(ext.lower(), "application/octet-stream")


def _mount_opts_for(path: Path) -> str | None:
    """M31:找 path 所属挂载点的选项串(取最长前缀匹配)。

    非 Linux(/proc/self/mounts 不存在)或读取失败返回 None——检测不到就闭嘴,
    不影响主流程。
    """
    try:
        target = str(path.resolve())
        best_mount = ""
        best_opts: str | None = None
        with open("/proc/self/mounts", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                fields = line.split()
                if len(fields) < 4:
                    continue
                # mounts 里空格转义为 \040 形态
                mount_point = fields[1].replace("\\040", " ")
                if (
                    mount_point != "/"
                    and target != mount_point
                    and not target.startswith(mount_point + "/")
                ):
                    continue
                if len(mount_point) >= len(best_mount):
                    best_mount = mount_point
                    best_opts = fields[3]
        return best_opts
    except OSError:
        return None


class ImageCache:
    """SHA256 命名的扁平目录,LRU 按 atime。"""

    def __init__(self, cache_dir: Path, quota_bytes: int) -> None:
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._quota = quota_bytes
        # M31:挂载选项自检。noatime 下读文件不刷 atime,LRU 名义上退化——但本插件
        # 全部读路径都经 get_bytes 的显式 os.utime 触探(显式 utime 不受 noatime
        # 抑制),atime 仍由应用层维护;淘汰排序另按 max(atime, mtime) 加固兜底。
        opts = _mount_opts_for(self._dir)
        if opts and "noatime" in opts.split(","):
            logger.info(
                "[image_cache] 挂载选项含 noatime:LRU 依赖 get_bytes 的显式 utime 触探"
                "(本插件读路径全经它),淘汰排序已按 max(atime, mtime) 加固"
            )

    def put(self, raw_bytes: bytes, mime_type: str) -> str:
        """存字节,返回 sha256。已存在的 sha 跳过写。"""
        sha = hashlib.sha256(raw_bytes).hexdigest()
        ext = _mime_to_ext(mime_type)
        path = self._dir / f"{sha}.{ext}"
        if path.exists():
            return sha
        tmp = self._dir / f"{sha}.{ext}.tmp.{os.getpid()}"
        try:
            tmp.write_bytes(raw_bytes)
            os.replace(tmp, path)
        except OSError as exc:
            logger.warning(f"[image_cache] write {sha} failed: {exc}")
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return sha

    def get_bytes(self, sha256: str) -> tuple[bytes, str] | None:
        """读字节;sha 未知 / 文件已被外部删 → None。读到后 touch atime。"""
        for ext in _KNOWN_EXTS:
            path = self._dir / f"{sha256}.{ext}"
            if not path.exists():
                continue
            try:
                bytes_ = path.read_bytes()
            except OSError:
                return None
            # 把 atime 推到当下,用于 LRU 排序
            try:
                os.utime(path, None)
            except OSError:
                pass
            return bytes_, _ext_to_mime(ext)
        return None

    def evict_if_over_quota(self) -> int:
        """目录总大小超 quota 时按 atime 老到新删,返回删除字节数。"""
        entries: list[tuple[Path, int, float]] = []
        total = 0
        for p in self._dir.iterdir():
            if not p.is_file():
                continue
            name = p.name
            # 跳过 tmp 写中间态(.tmp.<pid>)
            if ".tmp." in name:
                continue
            try:
                st = p.stat()
            except OSError:
                continue
            entries.append((p, st.st_size, st.st_atime, st.st_mtime))
            total += st.st_size

        if total <= self._quota:
            return 0

        # M31:新近度取 max(atime, mtime)——atime 失灵(noatime/触探失败)时退化为
        # 写入序,至少是有界确定行为,不至于近似任意删。
        entries.sort(key=lambda e: max(e[2], e[3]))  # 最老在前
        deleted = 0
        for p, size, _atime, _mtime in entries:
            try:
                p.unlink()
                deleted += size
                total -= size
            except OSError as exc:
                logger.warning(f"[image_cache] unlink {p.name} failed: {exc}")
                continue
            if total <= self._quota:
                break
        return deleted

    def total_size_bytes(self) -> int:
        """当前缓存总大小(诊断用)。"""
        total = 0
        for p in self._dir.iterdir():
            if p.is_file() and ".tmp." not in p.name:
                try:
                    total += p.stat().st_size
                except OSError:
                    continue
        return total
