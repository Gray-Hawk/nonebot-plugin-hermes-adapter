"""get_message_files: 按 (message_id, idx) 精确取回 QQ 消息附件的本地文件路径。

与 get_message_images 同族的反向通道工具,但取回的是**文件**(群文件/私聊文件)
而非图片字节:工具按需把附件下载进 FileCache,返回 file_name / local_path /
sha256 / actual_size 元数据,让 Hermes 端拿本地路径做后续处理。

安全边界:
- 范围校验按行做:message_id 是自增小整数、可枚举,归属只能从 DB 反查
  (store.get_message_owners);越权 id 一律按 not_found 处理,不给探测口。
- 不返回完整下载 URL(短效、含令牌风险),只返回本地缓存路径。

下载链路(未缓存时):
1. 优先用消息段的 url(row.url)直接下;
2. 失败/缺失 → 按群 get_group_file_url(group_id, file_id)、私聊
   get_private_file_url(file_id) 刷新 url;
3. file_id 可能 24h 失效 → 再 get_msg(message_id=qq_message_id) 从返回的 file
   段按「原 file_id / 文件名 / 文件段序」匹配刷新 file_id+url,并重试。
成功更新 message_files 状态并返回元数据;失败更新 status=failed/error,返回可见
错误、绝不返回半截路径。

注:bot 经 bot_resolver(同步 callable)由 server 注入,不在此固定持有;本 impl 只
定义签名与逻辑,不在此注册。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from nonebot import logger
from pydantic import BaseModel, Field

from ...core.routing import CallerScope

_MAX_REFS = 4


class FileRef(BaseModel):
    message_id: int = Field(..., description="DB 主键(get_recent_messages 返回的 id)")
    idx: int = Field(..., ge=0, description="该消息内的文件编号(0 起,独立编号不与图片共享)")


class GetMessageFilesInput(BaseModel):
    refs: list[FileRef] = Field(
        ..., min_length=1, max_length=_MAX_REFS, description="要取文件的 (message_id, idx) 引用,最多 4 个"
    )


class FileResult(BaseModel):
    message_id: int
    idx: int
    available: bool
    file_name: str | None = None
    local_path: str | None = None
    sha256: str | None = None
    actual_size: int | None = None
    reason: str | None = None
    """不可用原因:not_found(message_id 不存在或越权) / download_failed(下载失败)。"""
    error: str | None = None
    """可见错误说明(仅失败时);绝不携带半截路径。"""


class GetMessageFilesResult(BaseModel):
    results: list[FileResult]


def _find_row(rows: list[dict[str, Any]], idx: int) -> dict[str, Any] | None:
    for r in rows:
        if r["idx"] == idx:
            return r
    return None


def _resolve_cached(row: dict[str, Any], cache) -> Path | None:
    """已缓存判读:local_path 存在直接用;否则按 sha 在缓存里找(文件被外部删/挪)。"""
    if not row["sha256"]:
        return None
    lp = row["local_path"]
    if lp and Path(lp).is_file():
        return Path(lp)
    return cache.get_path(row["sha256"])


async def _try_download(downloader, url: str, file_name: str, expected_size: int):
    """下载并折返回 (DownloadedFile | None, error | None)。"""
    try:
        return await downloader.download(url, file_name, expected_size=expected_size), None
    except Exception as exc:
        if type(exc).__name__ == "FileDownloadError":
            return None, str(exc)
        return None, f"download error ({type(exc).__name__})"


async def _bot_file_url(bot, group_id: str | None, file_id: str) -> str | None:
    """群 get_group_file_url / 私聊 get_private_file_url 刷新下载 url。失败返 None。"""
    try:
        if group_id:
            resp = await bot.get_group_file_url(group_id=group_id, file_id=file_id)
        else:
            resp = await bot.get_private_file_url(file_id=file_id)
    except Exception as exc:
        logger.warning(
            f"[HERMES MCP] get_message_files 刷新文件 url 失败 "
            f"group={group_id!r} file_id={file_id[:24] if file_id else ''}: {exc}"
        )
        return None
    return (resp or {}).get("url") if isinstance(resp, dict) else None


async def _refresh_via_get_msg(
    bot, qq_message_id: str, idx: int, target_file_id: str, file_name: str
) -> dict[str, Any] | None:
    """file_id 过期后经 get_msg 重拿 file 段,刷新 file_id/url。

    匹配策略(依序):文件段序(idx) → 原 file_id 精确 → 文件名精确。
    全部不中返 None。
    """
    try:
        resp = await bot.get_msg(message_id=qq_message_id)
    except Exception as exc:
        logger.warning(f"[HERMES MCP] get_message_files get_msg 失败 qq_message_id={qq_message_id!r}: {exc}")
        return None
    message = resp.get("message") if isinstance(resp, dict) else None
    if not isinstance(message, list):
        return None
    segments: list[dict[str, Any]] = []
    for seg in message:
        if isinstance(seg, dict) and seg.get("type") == "file":
            data = seg.get("data")
            if isinstance(data, dict):
                segments.append(data)
    # 优先按 idx 定位:FileRef.idx 即原消息 File 段序号,同名文件也能正确区分。
    if 0 <= idx < len(segments):
        return segments[idx]
    for seg in segments:
        fid = seg.get("file_id") or seg.get("file") or seg.get("id")
        if fid and target_file_id and fid == target_file_id:
            return seg
    for seg in segments:
        name = seg.get("name") or seg.get("file_name") or seg.get("file")
        if name and file_name and name == file_name:
            return seg
    return None


def _resolve_bot(bot_resolver, adapter: str, group_id: str | None, user_id: str):
    """解析消息对应的 bot 连接。resolver 返回 None/抛异常时降级为 None(刷新环节
    静默跳过,消息 url 直下不受影响)。"""
    try:
        return bot_resolver(adapter, group_id, user_id)
    except Exception as exc:
        logger.warning(
            f"[HERMES MCP] get_message_files bot_resolver 失败 "
            f"adapter={adapter} group={group_id!r} user={user_id!r}: {exc}"
        )
        return None


async def get_message_files_impl(
    inp: GetMessageFilesInput,
    *,
    store,
    cache,
    downloader,
    bot_resolver,
    scope: CallerScope | None,
) -> GetMessageFilesResult:
    """按入参顺序取回文件;越权/不存在 → not_found,下载失败 → download_failed。

    越权校验先于 bot 解析:scope 不过的 id 不调 resolver,不泄露别群归属,也不为
    被拒请求做路由工作。resolver 是同步 callable(adapter, group_id, user_id) ->
    Bot,group_id 空时按私聊 user_id 选连接。
    """
    mids = [r.message_id for r in inp.refs]
    targets = store.get_message_targets(mids)
    files_by_mid = store.get_message_files(mids)

    results: list[FileResult] = []
    for ref in inp.refs:
        mid, idx = ref.message_id, ref.idx
        target = targets.get(mid)
        if target is None or scope is None or not scope.allows(target[0], target[1] or ""):
            if target is not None:
                logger.warning(
                    f"[HERMES MCP] get_message_files 越权 message_id 已按 not_found 处理: "
                    f"{mid} (caller: {'unresolved' if scope is None else scope.describe()})"
                )
            results.append(FileResult(message_id=mid, idx=idx, available=False, reason="not_found"))
            continue
        row = _find_row(files_by_mid.get(mid, []), idx)
        if row is None:
            results.append(FileResult(message_id=mid, idx=idx, available=False, reason="not_found"))
            continue
        adapter, group_id, user_id = target
        bot = _resolve_bot(bot_resolver, adapter, group_id, user_id)
        results.append(await _fetch_one(mid, idx, row, group_id, store, cache, downloader, bot))
    return GetMessageFilesResult(results=results)


async def _fetch_one(
    mid: int,
    idx: int,
    row: dict[str, Any],
    group_id: str | None,
    store,
    cache,
    downloader,
    bot,
) -> FileResult:
    file_name = row["name"] or f"file_{mid}_{idx}"
    expected_size = row["size"] or 0

    # 1. 缓存命中:local_path 存在 / 按 sha 找回
    cached = _resolve_cached(row, cache)
    if cached is not None:
        actual = row["actual_size"] or cached.stat().st_size
        if str(cached) != (row["local_path"] or ""):
            store.update_file_download(
                mid, idx, sha256=row["sha256"], local_path=str(cached), actual_size=actual
            )
        return FileResult(
            message_id=mid, idx=idx, available=True, file_name=file_name,
            local_path=str(cached), sha256=row["sha256"], actual_size=actual,
        )

    # 2. 优先消息段 url
    url = row["url"]
    dl = None
    last_err: str | None = None
    if url:
        dl, last_err = await _try_download(downloader, url, file_name, expected_size)

    # 3. 群/私聊 file url 刷新
    if dl is None and row["file_id"]:
        new_url = await _bot_file_url(bot, group_id, row["file_id"])
        if new_url and new_url != url:
            store.update_file_ref(mid, idx, url=new_url)
            dl, last_err = await _try_download(downloader, new_url, file_name, expected_size)

    # 4. get_msg 过期恢复(file_id 24h 失效)
    if dl is None and row["qq_message_id"]:
        refreshed = await _refresh_via_get_msg(
            bot, row["qq_message_id"], idx, row["file_id"], file_name
        )
        if refreshed:
            new_fid = refreshed.get("file_id") or refreshed.get("file") or row["file_id"]
            new_url = refreshed.get("url")
            store.update_file_ref(
                mid, idx, file_id=new_fid, url=new_url, qq_message_id=row["qq_message_id"]
            )
            if new_url:
                dl, last_err = await _try_download(downloader, new_url, file_name, expected_size)
            if dl is None and new_fid:
                refreshed_url = await _bot_file_url(bot, group_id, new_fid)
                if refreshed_url and refreshed_url != new_url:
                    store.update_file_ref(
                        mid, idx, file_id=new_fid, url=refreshed_url,
                        qq_message_id=row["qq_message_id"]
                    )
                    dl, last_err = await _try_download(
                        downloader, refreshed_url, file_name, expected_size
                    )

    if dl is None:
        err = last_err or "no downloadable url"
        store.update_file_failed(mid, idx, error=err)
        return FileResult(
            message_id=mid, idx=idx, available=False, reason="download_failed",
            file_name=file_name, error=err,
        )

    store.update_file_download(mid, idx, sha256=dl.sha256, local_path=str(dl.path), actual_size=dl.size)
    return FileResult(
        message_id=mid, idx=idx, available=True, file_name=file_name,
        local_path=str(dl.path), sha256=dl.sha256, actual_size=dl.size,
    )
