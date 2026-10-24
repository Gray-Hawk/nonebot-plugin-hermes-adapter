"""文件下载/缓存配置:`<hermes_custom_dir>/file_config.yaml`,现读现用(保存即生效)。

QQ 消息附带文件(群文件/私聊文件)的元数据入库、按需下载与本地缓存的配置载体:

- max_file_size_mb:单文件下载大小上限(预期大小/Content-Length/流式累计三重门)
- max_files_per_message:单条消息可注册的文件数上限
- max_concurrent_downloads:文件下载并发上限(实例信号量)
- cache_quota_mb:本地文件缓存总体积上限,超出按 max(atime, mtime) LRU 淘汰
- timeout_s:单文件 HTTP 超时秒数
- max_attempts:单文件总尝试次数(1=不重试)

格式约定:人改的配置一律 YAML(可注释)。纪律:文件缺失/坏档 → 全默认值
(WARN 按内容去重);字段类型错按字段回退默认。此文件只读不写,任何字段缺失都有
缺省。不进 .env——与 media_config.yaml / feedback_rules.yaml 同族,现读现用免重启。
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass

from nonebot import logger

from ..config import resolve_custom_dir

_CONFIG_FILENAME = "file_config.yaml"

_DEFAULT_MAX_FILE_SIZE_MB = 100
_DEFAULT_MAX_FILES_PER_MESSAGE = 4
_DEFAULT_MAX_CONCURRENT_DOWNLOADS = 2
_DEFAULT_CACHE_QUOTA_MB = 1024
_DEFAULT_TIMEOUT_S = 60
_DEFAULT_MAX_ATTEMPTS = 2


@dataclass(frozen=True)
class FileConfig:
    """文件下载/缓存配置快照。frozen:单轮内共享一份,改文件下轮生效。"""

    max_file_size_mb: int = _DEFAULT_MAX_FILE_SIZE_MB
    max_files_per_message: int = _DEFAULT_MAX_FILES_PER_MESSAGE
    max_concurrent_downloads: int = _DEFAULT_MAX_CONCURRENT_DOWNLOADS
    cache_quota_mb: int = _DEFAULT_CACHE_QUOTA_MB
    timeout_s: int = _DEFAULT_TIMEOUT_S
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS

    @property
    def max_file_size_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

    @property
    def cache_quota_bytes(self) -> int:
        return self.cache_quota_mb * 1024 * 1024


def _int_field(doc: dict, key: str, default: int, min_v: int = 1) -> int:
    """按 key 取 int 字段,缺失/类型错/越界回退默认。bool 是 int 子类,显式排除。"""
    node = doc.get(key) if isinstance(doc, dict) else None
    if isinstance(node, bool) or not isinstance(node, int) or node < min_v:
        return default
    return node


_last_warn: str = ""


def load_file_config() -> FileConfig:
    """加载 file_config.yaml。文件缺失/坏档 → 全默认(WARN 按内容去重)。

    现读现用:每次调用现读文件,保存即生效,无需重启 nb。
    """
    global _last_warn
    custom_dir = resolve_custom_dir()
    path = custom_dir / _CONFIG_FILENAME if custom_dir else None
    if path is None or not path.is_file():
        return FileConfig()
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            raise ValueError("顶层必须是映射(YAML mapping)")
    except Exception as exc:
        warn = f"[HERMES file-config] 配置文件不可用,回退全默认: {exc}"
        if warn != _last_warn:
            logger.warning(warn)
            _last_warn = warn
        return FileConfig()

    return FileConfig(
        max_file_size_mb=_int_field(doc, "max_file_size_mb", _DEFAULT_MAX_FILE_SIZE_MB),
        max_files_per_message=_int_field(doc, "max_files_per_message", _DEFAULT_MAX_FILES_PER_MESSAGE),
        max_concurrent_downloads=_int_field(doc, "max_concurrent_downloads", _DEFAULT_MAX_CONCURRENT_DOWNLOADS),
        cache_quota_mb=_int_field(doc, "cache_quota_mb", _DEFAULT_CACHE_QUOTA_MB),
        timeout_s=_int_field(doc, "timeout_s", _DEFAULT_TIMEOUT_S),
        max_attempts=_int_field(doc, "max_attempts", _DEFAULT_MAX_ATTEMPTS),
    )
