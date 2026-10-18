"""异步 HTTP 抓取 worker。

perception 把 (msg_id, urls) submit 进来,内部 single-worker 协程消费;
失败重试至 max_attempts,最终失败时只让 message_images.sha256 留 NULL,
log warning,不抛 —— perception 路径不能因为图抓不到而崩。

URL-level dedupe:同一 URL 在 in-flight 期间被二次 submit(常见于用户连发
两次同一张图,或反向 push_message 把同 URL 入第二条消息)时,**不重复
HTTP fetch**;两条消息的 (msg_id, idx) 都挂在同一 URL 的 pending 列表上,
fetch 一次结果 fan-out 到所有等待行。
"""

from __future__ import annotations

import asyncio
from collections import deque

import httpx
from nonebot import logger

from .image_cache import ImageCache
from .message_store import MessageStore

# 图片字节魔数 → 标准 MIME。用于 Telegram CDN 这类返回
# `application/octet-stream` 的源 —— Content-Type 不可信,从字节头识别。
_MAGIC_TO_MIME: list[tuple[bytes, str]] = [
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
]


# M29 修复:临时性失败(网络层/超时/5xx/429)的延迟重排队档位——
# 原实现 max_attempts 烧完即弃,一次网络抖动就让 sha256 永久 NULL(QQ URL 几天
# 后过期,届时再抓不回来)。重排队期间 pending 行原位保留,新提交同 URL 照常挂靠。
_REQUEUE_DELAYS_S = (60.0, 300.0)


def _is_transient_error(exc: BaseException) -> bool:
    """M29:临时性失败判定。HTTP 4xx(429 除外)是永久结论(链坏/越权/过期),
    重排队无意义;网络层/超时/5xx/429 及其余未分类异常按临时处理——延迟重抓的
    代价只是几次空跑,丢图的代价是 sha 永久 NULL,两害相权选重抓。"""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return True


def _sniff_image_mime(data: bytes) -> str | None:
    """Return a normalized image MIME if the byte head matches a known magic,
    else None. WebP needs both RIFF header + WEBP at offset 8.
    """
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    for magic, mime in _MAGIC_TO_MIME:
        if data.startswith(magic):
            return mime
    return None


class ImageFetcher:
    """单 worker 协程消费 queue;失败重试 max_attempts 次。

    数据结构:
    - `_queue: deque[str]` —— 待抓 URL 的 FIFO。每个 URL 在 queue 里最多一份。
    - `_pending: dict[url, list[(msg_id, idx)]]` —— 每个 in-flight / queued URL
      对应一组等待结果回填的 (msg_id, idx)。fetch 完成时 pop 整个列表批量 update。
    - 单 `httpx.AsyncClient` 实例横跨整个 worker 生命周期(start ↔ stop),
      复用连接池,省去每次 fetch 建/拆连接的开销。
    """

    def __init__(
        self,
        *,
        store: MessageStore,
        cache: ImageCache,
        timeout_s: int = 10,
        max_attempts: int = 2,
        queue_max: int = 1000,
    ) -> None:
        self._store = store
        self._cache = cache
        self._timeout = timeout_s
        self._max_attempts = max(1, max_attempts)
        self._queue_max = queue_max
        self._queue: deque[str] = deque()
        self._pending: dict[str, list[tuple[int, int]]] = {}
        # M29:重排队轮次记账(url → 已排档数)与延迟任务句柄(stop 时取消)。
        self._rounds: dict[str, int] = {}
        self._delayed: set[asyncio.Task] = set()
        self._wake: asyncio.Event = asyncio.Event()
        self._stop: asyncio.Event = asyncio.Event()
        self._worker: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

    def submit(self, message_id: int, urls: list[str], *, start_index: int = 0) -> None:
        """把每张图挂进 pending 表。同 URL 已 in-flight 时只加 (msg_id, idx),
        不重复入队。overflow 时按 URL 维度丢最老。

        start_index:本批 URL 在 message_images 表里的起始 idx。普通图从 0 起,
        sticker 接在普通图之后(见 MessageBuffer.append),回填 sha 按 (msg_id, idx)
        定位行,idx 编号必须与 MessageStore.append 写入时一致。

        只收 http(s):worker 是 httpx,喂它主机本地路径或 data: URL 只会抛
        UnsupportedProtocol 并重试到上限,白占队列还刷 warning。
        """
        new_urls: list[str] = []
        for idx, url in enumerate(urls, start=start_index):
            if not url:
                continue
            if not url.startswith(("http://", "https://")):
                logger.debug(f"[image_fetcher] skip non-http url (无法抓取): {url[:80]}")
                continue
            if url in self._pending:
                # 已 queued / in-flight,挂上这条 (msg_id, idx) 等同一份结果
                self._pending[url].append((message_id, idx))
                continue
            self._pending[url] = [(message_id, idx)]
            new_urls.append(url)

        for url in new_urls:
            while len(self._queue) >= self._queue_max:
                dropped = self._queue.popleft()
                pending = self._pending.pop(dropped, [])
                self._rounds.pop(dropped, None)
                logger.warning(
                    f"[image_fetcher] queue full, dropped url={dropped[:80]} with {len(pending)} pending writes"
                )
            self._queue.append(url)
        if new_urls:
            self._wake.set()

    async def start(self) -> None:
        if self._worker is not None:
            return
        self._stop.clear()
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._worker = asyncio.create_task(self._run(), name="hermes-image-fetcher")

    async def stop(self) -> None:
        if self._worker is None:
            return
        self._stop.set()
        self._wake.set()
        try:
            await asyncio.wait_for(self._worker, timeout=5)
        except asyncio.TimeoutError:
            self._worker.cancel()
            logger.warning("[image_fetcher] worker did not stop within 5s; cancelled")
        self._worker = None
        # M29:取消在途的延迟重排队任务,防停机后睡醒了往已停的队列里塞。
        for task in self._delayed:
            task.cancel()
        self._delayed.clear()
        self._rounds.clear()
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception as exc:
                logger.warning(f"[image_fetcher] httpx client aclose failed: {exc}")
            self._client = None
        if self._queue or self._pending:
            logger.info(
                f"[image_fetcher] shutdown: queued_urls={len(self._queue)} "
                f"pending_writes={sum(len(v) for v in self._pending.values())} dropped"
            )
        self._queue.clear()
        self._pending.clear()

    async def _run(self) -> None:
        while not self._stop.is_set():
            if not self._queue:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1)
                except asyncio.TimeoutError:
                    continue
            if self._stop.is_set():
                break
            if not self._queue:
                continue
            url = self._queue.popleft()
            await self._fetch_one(url)

    async def _fetch_one(self, url: str) -> None:
        last_err: str | None = None
        last_exc: BaseException | None = None
        transient = True
        for attempt in range(1, self._max_attempts + 1):
            try:
                assert self._client is not None, "fetcher not started"
                resp = await self._client.get(url)
                resp.raise_for_status()
                bytes_ = resp.content
                mime = self._resolve_mime(resp.headers.get("content-type"), bytes_)
                if mime is None:
                    last_err = (
                        f"non-image: ct={resp.headers.get('content-type')!r} "
                        f"head={bytes_[:16].hex() if bytes_ else 'empty'}"
                    )
                    transient = False  # 非图判定是永久结论,重排队无意义
                    break
                sha = self._cache.put(bytes_, mime)
                pending = self._pending.pop(url, [])
                self._rounds.pop(url, None)
                for message_id, idx in pending:
                    self._store.update_image_sha(message_id, idx, sha, mime)
                return
            except Exception as exc:
                last_exc = exc
                last_err = repr(exc)
                if attempt < self._max_attempts:
                    await asyncio.sleep(0.2 * attempt)  # 简单线性退避
        if last_exc is not None:
            transient = _is_transient_error(last_exc)
        # M29:临时性失败不立刻放弃,按 _REQUEUE_DELAYS_S 排档延迟重排队;
        # pending 行原位保留(重抓成功时仍按原 (msg_id, idx) 回填)。
        round_no = self._rounds.get(url, 0)
        if transient and round_no < len(_REQUEUE_DELAYS_S) and not self._stop.is_set():
            delay = _REQUEUE_DELAYS_S[round_no]
            self._rounds[url] = round_no + 1
            self._schedule_requeue(url, delay)
            logger.info(
                f"[image_fetcher] fetch failed (transient), requeue in {delay:.0f}s "
                f"(round {round_no + 1}/{len(_REQUEUE_DELAYS_S)}) url={url[:80]}: {last_err}"
            )
            return
        self._rounds.pop(url, None)
        pending = self._pending.pop(url, [])
        logger.warning(
            f"[image_fetcher] fetch failed url={url[:80]} "
            f"pending={len(pending)} after {self._max_attempts} attempts: {last_err}"
        )

    def _schedule_requeue(self, url: str, delay: float) -> None:
        """M29 补偿通道:delay 秒后把 url 塞回队列尾部。句柄入 _delayed,stop 时取消。"""

        async def _later() -> None:
            try:
                await asyncio.sleep(delay)
                if self._stop.is_set() or url not in self._pending:
                    return
                self._queue.append(url)
                self._wake.set()
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(_later(), name="hermes-image-fetcher-requeue")
        self._delayed.add(task)
        task.add_done_callback(self._delayed.discard)

    @staticmethod
    def _resolve_mime(content_type: str | None, bytes_: bytes) -> str | None:
        """信任顺序:
        1. Content-Type 是 image/* → 直接用
        2. Content-Type 是 application/octet-stream(Telegram CDN 典型行为)
           或缺失 → 嗅探字节魔数
        3. 字节嗅探命中已知图片格式 → 用嗅探结果
        4. 都不是 → 返回 None,表示真不是图
        """
        ct = (content_type or "").split(";", 1)[0].strip().lower()
        if ct.startswith("image/"):
            return ct
        sniffed = _sniff_image_mime(bytes_)
        return sniffed
