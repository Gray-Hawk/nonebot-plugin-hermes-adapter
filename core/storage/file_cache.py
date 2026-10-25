"""文件系统文件字节缓存(QQ 消息附带文件的本地落地)。

按 SHA256 内容寻址命名,同名文件只存一份;LRU 按 max(atime, mtime) 淘汰直至
总大小符合配额。与 image_cache 同族,但面向任意类型文件(不限图片),扩展名只
从文件名安全推导(小写字母数字、长度受限,否则 bin)。

并发安全:写入走同目录临时文件 + os.replace 原子提交(FileDownloader 流式写
tmp 后调 commit 落定),并发写同 sha 不损坏;读路径显式 touch atime 供 LRU 排序
(显式 utime 不受挂载 noatime 抑制)。
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

from nonebot import logger

# 扩展名长度上限:超长/畸形扩展名一律回退 bin,防止把不可信文件名写进路径。
_MAX_EXT_LEN = 8

# 临时文件标记:下载中的半截文件用此前缀命名,淘汰与按 sha 查找时跳过。
_TMP_PREFIX = ".part-"


def safe_ext(file_name: str) -> str:
    """从文件名推导安全扩展名(不带点)。只保留小写字母数字,长度受限;
    无扩展/超长/全非字母数字时返回 'bin'。"""
    name = Path(file_name or "").name
    suffix = Path(name).suffix
    ext = suffix.lstrip(".").lower()
    ext = "".join(c for c in ext if c.isalnum())
    if not ext or len(ext) > _MAX_EXT_LEN:
        return "bin"
    return ext


def default_file_cache_dir() -> Path:
    """文件缓存缺省目录:localstore 插件 cache 目录下的 files/(与 images 同层)。

    文件字节是可再下载资源,走 cache_dir 而非 data_dir——cache_dir 默认在
    XDG_CACHE_HOME 下,系统/用户可放心整目录清理。正路走 get_plugin_cache_dir
    (尊重 localstore_plugin_cache_dir 重映射);插件未经插件管理器加载(离线冒烟
    直 import)时调用栈识别会抛 RuntimeError,退化为按 PLUGIN_MODULE_NAME
    调用 get_cache_dir 兜底——与正路默认解析结果一致,只是不吃重映射。
    """
    try:
        import nonebot_plugin_localstore as store

        try:
            return store.get_plugin_cache_dir() / "files"
        except Exception:
            from ...config import PLUGIN_MODULE_NAME

            return store.get_cache_dir(PLUGIN_MODULE_NAME) / "files"
    except Exception:
        # localstore 不可用:回退到插件包所在位置的兜底缓存目录(尽力而为)。
        return Path(__file__).resolve().parent.parent / "cache" / "files"


class FileCache:
    """SHA256 命名的扁平目录,文件 0600、目录 0700,LRU 按 max(atime, mtime)。"""

    def __init__(self, cache_dir: Path, quota_bytes: int) -> None:
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self._dir, 0o700)
        except OSError:
            pass
        self._quota = quota_bytes

    @property
    def quota_bytes(self) -> int:
        return self._quota

    def set_quota(self, quota_bytes: int) -> None:
        """更新缓存配额；下一次淘汰按新值执行。"""
        self._quota = max(1, int(quota_bytes))

    def tmp_path(self) -> Path:
        """同目录临时文件路径(下载中写入用)。与最终文件同文件系统,os.replace 原子。"""
        return self._dir / f"{_TMP_PREFIX}{uuid.uuid4().hex}"

    def path_for(self, sha256: str, file_name: str) -> Path:
        """按 sha + 文件名推导的规范路径 `<sha>.<ext>`(不保证存在)。"""
        return self._dir / f"{sha256}.{safe_ext(file_name)}"

    def commit(self, tmp_path: Path, sha256: str, file_name: str) -> Path:
        """把已写满的临时文件原子落定为 `<sha>.<ext>`,置 0600,返回最终路径。"""
        final = self.path_for(sha256, file_name)
        os.replace(tmp_path, final)
        try:
            os.chmod(final, 0o600)
        except OSError:
            pass
        return final

    def get_path(self, sha256: str) -> Path | None:
        """按 sha 找已有文件(任意扩展名),读到后 touch atime。找不到/被外部删返 None。"""
        prefix = sha256 + "."
        try:
            entries = list(self._dir.iterdir())
        except OSError:
            return None
        for p in entries:
            if p.name.startswith(prefix) and p.is_file():
                try:
                    os.utime(p, None)
                except OSError:
                    pass
                return p
        return None

    def evict_if_over_quota(self) -> int:
        """目录总大小超 quota 时按 max(atime, mtime) 老到新删,返回删除字节数。

        跳过临时文件(.part- 前缀)——它们是下载中的半截文件,不该被淘汰。
        """
        entries: list[tuple[Path, int, float]] = []
        total = 0
        try:
            for p in self._dir.iterdir():
                if not p.is_file():
                    continue
                if p.name.startswith(_TMP_PREFIX):
                    continue
                try:
                    st = p.stat()
                except OSError:
                    continue
                entries.append((p, st.st_size, max(st.st_atime, st.st_mtime)))
                total += st.st_size
        except OSError:
            return 0

        if total <= self._quota:
            return 0

        entries.sort(key=lambda e: e[2])  # 最老在前
        deleted = 0
        for p, size, _recency in entries:
            try:
                p.unlink()
                deleted += size
                total -= size
            except OSError as exc:
                logger.warning(f"[file_cache] unlink {p.name} failed: {exc}")
                continue
            if total <= self._quota:
                break
        return deleted

    def total_size_bytes(self) -> int:
        """当前缓存总大小(诊断用),跳过临时文件。"""
        total = 0
        try:
            for p in self._dir.iterdir():
                if p.is_file() and not p.name.startswith(_TMP_PREFIX):
                    try:
                        total += p.stat().st_size
                    except OSError:
                        continue
        except OSError:
            return 0
        return total
