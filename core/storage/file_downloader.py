"""异步 HTTP 文件下载器(QQ 消息附件的按需下载)。

get_message_files 工具在需要时调用 download(),把附件字节流式落进 FileCache:
- 只收 http/https;非 http(s) 直接 FileDownloadError
- 三重 100MB 限额:声明大小(expected_size)、Content-Length、流式累计,任一超限即止
- 成功校验实际大小:声明大小 > 0 且与实收不一致时判失败
- 同目录临时文件 + 边下边 SHA-256,成功后 os.replace 原子落定(FileCache.commit)
  并 chmod 0600;任何失败清理半截文件,不返回可读路径
- 提交后触发缓存淘汰(evict_if_over_quota)
- 全程流式,不把整文件读进内存
- timeout/retry(实例信号量限并发)
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import httpx
from nonebot import logger

from .file_cache import FileCache

_CHUNK_SIZE = 64 * 1024


class FileDownloadError(Exception):
    """下载失败(非 http(s)/超限/校验失败/重试耗尽)。调用方据此更新 status=failed。"""


@dataclass(frozen=True)
class DownloadedFile:
    path: Path
    sha256: str
    size: int


class FileDownloader:
    def __init__(
        self,
        *,
        cache: FileCache,
        timeout_s: int = 60,
        max_attempts: int = 2,
        max_size_bytes: int,
        concurrency: int = 2,
        config_loader=None,
    ) -> None:
        self._cache = cache
        self._timeout = timeout_s
        self._max_attempts = max(1, max_attempts)
        self._max_size = max_size_bytes
        self._semaphore = asyncio.Semaphore(max(1, concurrency))
        self._config_loader = config_loader

    def _current_config(self):
        """每次下载现读限制；未注入加载器时使用构造参数，便于离线测试。"""
        if self._config_loader is not None:
            return self._config_loader()
        return SimpleNamespace(
            timeout_s=self._timeout,
            max_attempts=self._max_attempts,
            max_file_size_bytes=self._max_size,
            cache_quota_bytes=self._cache.quota_bytes,
        )

    async def download(
        self, url: str, file_name: str, expected_size: int = 0
    ) -> DownloadedFile:
        """下载一个文件到缓存,返回 DownloadedFile(path, sha256, size)。

        失败抛 FileDownloadError;绝不返回半截文件的路径。
        """
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            raise FileDownloadError("only http(s) urls are supported")
        cfg = self._current_config()
        self._cache.set_quota(cfg.cache_quota_bytes)
        max_size = min(cfg.max_file_size_bytes, cfg.cache_quota_bytes)
        if expected_size and expected_size > max_size:
            raise FileDownloadError(
                f"declared size {expected_size} exceeds limit {max_size}"
            )

        async with self._semaphore:
            tmp = self._cache.tmp_path()
            try:
                async with httpx.AsyncClient(
                    timeout=cfg.timeout_s, follow_redirects=True
                ) as client:
                    last_exc: BaseException | None = None
                    for attempt in range(1, cfg.max_attempts + 1):
                        try:
                            return await self._download_once(
                                client, url, tmp, file_name, expected_size, max_size
                            )
                        except FileDownloadError:
                            raise
                        except Exception as exc:
                            last_exc = exc
                            if attempt < cfg.max_attempts:
                                await asyncio.sleep(0.2 * attempt)
                    raise FileDownloadError(
                        f"download failed after {cfg.max_attempts} attempts "
                        f"({type(last_exc).__name__})"
                    )
            finally:
                # 成功时 tmp 已被 os.replace 移走;失败时清掉半截文件。
                try:
                    tmp.unlink(missing_ok=True)
                except OSError:
                    pass

    async def _download_once(
        self,
        client: httpx.AsyncClient,
        url: str,
        tmp: Path,
        file_name: str,
        expected_size: int,
        max_size: int,
    ) -> DownloadedFile:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            content_length = resp.headers.get("content-length")
            if content_length:
                try:
                    cl = int(content_length)
                except (TypeError, ValueError):
                    cl = 0
                if cl > max_size:
                    raise FileDownloadError(
                        f"content-length {cl} exceeds limit {max_size}"
                    )
            hasher = hashlib.sha256()
            size = 0
            with open(tmp, "wb") as fh:
                async for chunk in resp.aiter_bytes(_CHUNK_SIZE):
                    size += len(chunk)
                    if size > max_size:
                        raise FileDownloadError(
                            f"streamed size exceeds limit {max_size}"
                        )
                    hasher.update(chunk)
                    fh.write(chunk)
            if expected_size and size != expected_size:
                raise FileDownloadError(
                    f"size mismatch: expected {expected_size}, got {size}"
                )
            sha = hasher.hexdigest()
            path = self._cache.commit(tmp, sha, file_name)
            self._cache.evict_if_over_quota()
            logger.info(
                f"[file_downloader] cached {sha[:12]}... ({size} bytes)"
            )
            return DownloadedFile(path=path, sha256=sha, size=size)
