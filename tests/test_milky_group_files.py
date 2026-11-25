"""QQ-06：群文件浏览/下载在 Milky 上走原生 API，不假装 OneBot 方法存在。

真实 Milky ``Bot``（只替换线形 I/O）+ 真实 ``FilesInfo`` 模型：浏览用
``get_group_files``，下载用 ``get_group_file_download_url``；Milky 没有
「群文件系统信息」API，容量字段必须诚实留空，不编造数字。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests._qqtpc_fixtures import MilkyHarness  # noqa: E402

from nonebot_plugin_hermes_adapter.core.routing import CallerScope  # noqa: E402
from nonebot_plugin_hermes_adapter.mcp.tools import group_files as gf  # noqa: E402

GROUP = "462540549"


class _RecorderBot:
    self_id = "999"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def call_api(self, action, **kwargs):
        self.calls.append((action, kwargs))
        if action == "get_group_file_system_info":
            return {"file_count": 3, "limit_count": 100, "used_space": 1, "total_space": 2}
        if action == "get_group_root_files":
            return {"folders": [], "files": []}
        return {"url": "https://example.invalid/ob"}


class _Downloader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def download(self, url, *, file_name, expected_size=0):
        self.calls.append((url, file_name, expected_size))
        return SimpleNamespace(path=Path("/tmp") / file_name, size=5, sha256="a" * 64)


class MilkyGroupFilesTests(unittest.IsolatedAsyncioTestCase):
    def _harness(self) -> MilkyHarness:
        harness = MilkyHarness()
        harness.replies["get_group_files"] = {
            "folders": [
                {
                    "group_id": int(GROUP), "folder_id": "fold-1", "folder_name": "资料",
                    "parent_folder_id": "/", "created_time": 100,
                    "last_modified_time": 200, "creator_id": 10001, "file_count": 2,
                }
            ],
            "files": [
                {
                    "group_id": int(GROUP), "file_id": "file-1", "file_name": "报告.pdf",
                    "parent_folder_id": "/", "file_size": 1024, "uploaded_time": 300,
                    "expire_time": None, "uploader_id": 10001, "downloaded_times": 4,
                }
            ],
        }
        harness.replies["get_group_file_download_url"] = {
            "download_url": "https://example.invalid/milky"
        }
        return harness

    async def test_browse_uses_native_listing_and_maps_metadata(self) -> None:
        harness = self._harness()

        result = await gf.browse_group_files_impl(
            gf.BrowseGroupFilesInput(adapter="milky", group_id=GROUP),
            bot=harness.bot, scope=CallerScope.dev(),
        )

        self.assertTrue(result.ok, result.error)
        self.assertEqual(harness.actions(), ["get_group_files"])
        params = harness.params_for("get_group_files")[0]
        self.assertEqual(params["group_id"], int(GROUP))
        self.assertIsNone(params.get("parent_folder_id"))
        self.assertEqual(result.total, 2)
        self.assertEqual(result.folders[0].folder_id, "fold-1")
        self.assertEqual(result.folders[0].total_file_count, 2)
        self.assertEqual(result.files[0].file_id, "file-1")
        self.assertEqual(result.files[0].upload_time, 300)
        self.assertEqual(result.files[0].download_times, 4)
        self.assertEqual(result.files[0].uploader, 10001)
        # Milky 没有群文件系统信息 API：容量字段诚实留空，不编造。
        self.assertIsNone(result.file_count)
        self.assertIsNone(result.used_space)

    async def test_browse_nested_folder_passes_parent_id(self) -> None:
        harness = self._harness()

        result = await gf.browse_group_files_impl(
            gf.BrowseGroupFilesInput(adapter="milky", group_id=GROUP, folder_id="fold-1"),
            bot=harness.bot, scope=CallerScope.dev(),
        )

        self.assertTrue(result.ok, result.error)
        self.assertEqual(harness.params_for("get_group_files")[0]["parent_folder_id"], "fold-1")

    async def test_download_uses_native_url_api(self) -> None:
        harness = self._harness()
        downloader = _Downloader()

        result = await gf.download_group_file_impl(
            gf.DownloadGroupFileInput(
                adapter="milky", group_id=GROUP, file_id="file-1",
                file_name="报告.pdf", expected_size=1024,
            ),
            bot=harness.bot, scope=CallerScope.dev(), downloader=downloader,
        )

        self.assertTrue(result.ok, result.error)
        self.assertEqual(harness.actions(), ["get_group_file_download_url"])
        params = harness.params_for("get_group_file_download_url")[0]
        self.assertEqual(params, {"group_id": int(GROUP), "file_id": "file-1"})
        self.assertEqual(downloader.calls, [("https://example.invalid/milky", "报告.pdf", 1024)])
        self.assertEqual(result.local_path, "/tmp/报告.pdf")

    async def test_milky_rejection_becomes_error_result(self) -> None:
        harness = self._harness()
        harness.failures["get_group_files"] = {
            "status": "failed", "retcode": 100, "message": "no permission",
        }

        result = await gf.browse_group_files_impl(
            gf.BrowseGroupFilesInput(adapter="milky", group_id=GROUP),
            bot=harness.bot, scope=CallerScope.dev(),
        )

        self.assertFalse(result.ok)
        self.assertIn("群文件列表", result.error)

    async def test_onebot_path_is_unchanged(self) -> None:
        bot = _RecorderBot()

        result = await gf.browse_group_files_impl(
            gf.BrowseGroupFilesInput(adapter="onebotv11", group_id=GROUP),
            bot=bot, scope=CallerScope.dev(),
        )

        self.assertTrue(result.ok, result.error)
        self.assertEqual(
            [name for name, _ in bot.calls],
            ["get_group_file_system_info", "get_group_root_files"],
        )
        self.assertEqual(result.file_count, 3)


if __name__ == "__main__":
    unittest.main()
