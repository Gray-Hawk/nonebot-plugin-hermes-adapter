"""group_files: 群文件库浏览(只读)与按 file_id 选择下载。

浏览与下载是两个独立工具,职责正交:
  - browse_group_files:只调 get_group_file_system_info + 根/子目录列表 API,
    把目录与文件元数据原样透出,绝不下载、绝不触下载器;
  - download_group_file:按 file_id 取下载链接(get_group_file_url),把链接交给
    注入的 downloader 落地到本地,回本地路径、真实大小、sha256、file_name。

依赖注入:
  - bot:由 server 注入(OneBot 适配器),本模块不 get_bot;
  - downloader:由 server 注入的鸭子类型,async download(url, file_name=..., expected_size=...)
    返回带 path(Path)/sha256(str)/size(int) 的对象。本模块不实现下载逻辑。

范围校验:mcp.auth.scope_refusal 先判,越权返回 error(不抛异常、不打 traceback)。
失败一律折进 result.error,不向调用方暴露下载链接或 token。
"""

from __future__ import annotations

from nonebot import logger
from pydantic import BaseModel, Field

from ...core.routing import CallerScope
from ...core.storage.file_downloader import FileDownloadError
from ..auth import scope_refusal


class BrowseGroupFilesInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    group_id: str = Field(..., description="Group ID")
    folder_id: str | None = Field(
        default=None, description="指定子目录;None = 浏览根目录"
    )
    offset: int = Field(default=0, ge=0, description="分页起始偏移(0 起)")
    limit: int = Field(default=100, ge=1, le=200, description="每页条数上限 200")


class GroupFolderView(BaseModel):
    folder_id: str
    folder_name: str
    total_file_count: int | None = None
    create_time: int | None = None
    creator: int | None = None
    creator_name: str | None = None


class GroupFileView(BaseModel):
    file_id: str
    file_name: str
    file_size: int | None = None
    busid: int | None = None
    upload_time: int | None = None
    dead_time: int | None = None
    modify_time: int | None = None
    download_times: int | None = None
    uploader: int | None = None
    uploader_name: str | None = None


class BrowseGroupFilesResult(BaseModel):
    ok: bool
    error: str | None = None
    file_count: int | None = None
    """get_group_file_system_info 透出的文件总数。"""
    limit_count: int | None = None
    used_space: int | None = None
    total_space: int | None = None
    total: int = 0
    """本次列表(目录 + 文件)分页前的总条数。"""
    folders: list[GroupFolderView] = Field(default_factory=list)
    files: list[GroupFileView] = Field(default_factory=list)


class DownloadGroupFileInput(BaseModel):
    adapter: str = Field(..., description="Adapter name (lowercased), e.g. 'ob11'")
    group_id: str = Field(..., description="Group ID")
    file_id: str = Field(..., description="浏览结果里的 file_id,用于 get_group_file_url")
    file_name: str = Field(default="group_file.bin", description="落地文件名")
    expected_size: int = Field(default=0, ge=0, description="预期大小(字节);0 = 不预检")


class DownloadGroupFileResult(BaseModel):
    ok: bool
    error: str | None = None
    file_name: str | None = None
    local_path: str | None = None
    size: int | None = None
    sha256: str | None = None


def _get(raw, key):
    """从 dict 或对象上取字段;缺失返回 None。"""
    if isinstance(raw, dict):
        return raw.get(key)
    return getattr(raw, key, None)


def _build_file_view(raw) -> GroupFileView | None:
    """透传文件元数据;缺 file_id 的条目视为不可寻址,跳过。"""
    file_id = _get(raw, "file_id")
    if file_id is None:
        return None
    file_name = _get(raw, "file_name")
    file_size = _get(raw, "file_size")
    if file_size is None:
        # 个别实现用 size 而非 file_size,做一层别名兜底。
        file_size = _get(raw, "size")
    return GroupFileView(
        file_id=str(file_id),
        file_name=str(file_name) if file_name is not None else "",
        file_size=file_size,
        busid=_get(raw, "busid"),
        upload_time=_get(raw, "upload_time"),
        dead_time=_get(raw, "dead_time"),
        modify_time=_get(raw, "modify_time"),
        download_times=_get(raw, "download_times"),
        uploader=_get(raw, "uploader"),
        uploader_name=_get(raw, "uploader_name"),
    )


def _build_folder_view(raw) -> GroupFolderView | None:
    """透传目录元数据;缺 folder_id 的条目无法导航,跳过。"""
    folder_id = _get(raw, "folder_id")
    if folder_id is None:
        return None
    folder_name = _get(raw, "folder_name")
    return GroupFolderView(
        folder_id=str(folder_id),
        folder_name=str(folder_name) if folder_name is not None else "",
        total_file_count=_get(raw, "total_file_count"),
        create_time=_get(raw, "create_time"),
        creator=_get(raw, "creator"),
        creator_name=_get(raw, "creator_name"),
    )


def _paginate(
    folders: list[GroupFolderView],
    files: list[GroupFileView],
    offset: int,
    limit: int,
) -> tuple[int, list[GroupFolderView], list[GroupFileView]]:
    """目录在前、文件在后合成一条序列,按 offset/limit 切片。"""
    combined: list[tuple[str, object]] = [("f", f) for f in folders] + [
        ("i", f) for f in files
    ]
    total = len(combined)
    page = combined[offset : offset + limit]
    out_folders = [item for kind, item in page if kind == "f"]
    out_files = [item for kind, item in page if kind == "i"]
    return total, out_folders, out_files


async def browse_group_files_impl(
    inp: BrowseGroupFilesInput,
    *,
    bot,
    scope: CallerScope | None,
) -> BrowseGroupFilesResult:
    """浏览群文件(只读):系统信息 + 根/子目录列表,透传元数据并分页。"""
    refusal = scope_refusal(inp.adapter, inp.group_id, scope)
    if refusal is not None:
        return BrowseGroupFilesResult(ok=False, error=refusal)

    try:
        sysinfo = await bot.call_api(
            "get_group_file_system_info", group_id=int(inp.group_id)
        )
    except Exception as exc:
        logger.warning(
            f"[MCP group_files] get_group_file_system_info 失败 group={inp.group_id}: {exc}"
        )
        return BrowseGroupFilesResult(ok=False, error="读取群文件系统信息失败(见 bot 日志)")

    if inp.folder_id is None:
        api_name = "get_group_root_files"
        api_kwargs = {"group_id": int(inp.group_id)}
    else:
        api_name = "get_group_files_by_folder"
        api_kwargs = {"group_id": int(inp.group_id), "folder_id": inp.folder_id}
    try:
        listing = await bot.call_api(api_name, **api_kwargs)
    except Exception as exc:
        logger.warning(
            f"[MCP group_files] {api_name} 失败 group={inp.group_id} folder={inp.folder_id}: {exc}"
        )
        return BrowseGroupFilesResult(ok=False, error="读取群文件列表失败(见 bot 日志)")

    raw_folders = listing.get("folders", []) if isinstance(listing, dict) else []
    raw_files = listing.get("files", []) if isinstance(listing, dict) else []
    folders = [v for v in (_build_folder_view(raw) for raw in raw_folders) if v is not None]
    files = [v for v in (_build_file_view(raw) for raw in raw_files) if v is not None]

    total, page_folders, page_files = _paginate(folders, files, inp.offset, inp.limit)
    return BrowseGroupFilesResult(
        ok=True,
        file_count=sysinfo.get("file_count") if isinstance(sysinfo, dict) else getattr(sysinfo, "file_count", None),
        limit_count=sysinfo.get("limit_count") if isinstance(sysinfo, dict) else getattr(sysinfo, "limit_count", None),
        used_space=sysinfo.get("used_space") if isinstance(sysinfo, dict) else getattr(sysinfo, "used_space", None),
        total_space=sysinfo.get("total_space") if isinstance(sysinfo, dict) else getattr(sysinfo, "total_space", None),
        total=total,
        folders=page_folders,
        files=page_files,
    )


async def download_group_file_impl(
    inp: DownloadGroupFileInput,
    *,
    bot,
    scope: CallerScope | None,
    downloader,
) -> DownloadGroupFileResult:
    """按 file_id 取下载链接,交 downloader 落地,回本地路径/真实大小/sha256/file_name。"""
    refusal = scope_refusal(inp.adapter, inp.group_id, scope)
    if refusal is not None:
        return DownloadGroupFileResult(ok=False, error=refusal)

    try:
        resp = await bot.call_api(
            "get_group_file_url", group_id=int(inp.group_id), file_id=inp.file_id
        )
    except Exception as exc:
        logger.warning(
            f"[MCP group_files] get_group_file_url 失败 group={inp.group_id} file={inp.file_id}: {exc}"
        )
        return DownloadGroupFileResult(ok=False, error="获取下载链接失败(见 bot 日志)")

    url = resp.get("url") if isinstance(resp, dict) else getattr(resp, "url", None)
    if not url:
        logger.warning(
            f"[MCP group_files] 未取得下载链接 group={inp.group_id} file={inp.file_id}"
        )
        return DownloadGroupFileResult(ok=False, error="未取得下载链接")

    try:
        result = await downloader.download(
            url, file_name=inp.file_name, expected_size=inp.expected_size
        )
    except FileDownloadError as exc:
        logger.warning(f"[MCP group_files] download 校验失败: {exc}")
        return DownloadGroupFileResult(ok=False, error=str(exc))
    except Exception as exc:
        logger.warning(f"[MCP group_files] download 失败 ({type(exc).__name__})")
        return DownloadGroupFileResult(ok=False, error="下载失败(见 bot 日志)")
    if result is None:
        return DownloadGroupFileResult(ok=False, error="下载失败(无结果)")

    path = getattr(result, "path", None)
    if path is None:
        return DownloadGroupFileResult(ok=False, error="下载失败(无本地路径)")
    return DownloadGroupFileResult(
        ok=True,
        file_name=inp.file_name,
        local_path=str(path),
        size=getattr(result, "size", None),
        sha256=getattr(result, "sha256", None),
    )
