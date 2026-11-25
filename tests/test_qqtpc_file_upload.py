"""QQ-02：本地群/私文件上传支持 Milky 原生 API；不支持的能力必须明确结案。

真实 Milky ``Bot``（只替换线形 I/O）与真实交付台账；OneBot 侧只记录帧。
断言三件事：
  - Milky 走 ``upload_group_file`` / ``upload_private_file``（``path`` + ``file_name``）；
  - OneBot 帧形态不变（``file`` + ``name``）；
  - 平台没有上传能力（官方 QQ 等）时 file 组件必须离开 ``pending``（``not_required``），
    绝不允许永久 pending 被对账器无限重试。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from tests._qqtpc_fixtures import MilkyHarness  # noqa: E402

import nonebot_plugin_alconna as alconna  # noqa: E402
from nonebot_plugin_hermes_adapter.core import media_whitelist as mw  # noqa: E402
from nonebot_plugin_hermes_adapter.core import outbound as ob  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    prepare_terminal_batch,
)

GROUP = "462540549"
USER = "10001"


class _RecorderBot:
    """OneBot 形状的最小 bot：只记录 call_api。"""

    self_id = "999"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def call_api(self, action, **kwargs):
        self.calls.append((action, kwargs))
        return {"message_id": 1}


class FileUploadTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.file = self.tmp / "报表 2026.txt"
        self.file.write_text("hello", encoding="utf-8")

        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)

        patches = [
            patch.object(
                ob, "load_media_config",
                lambda: SimpleNamespace(local_file_max_mb=10, media_allow_dirs=str(self.tmp)),
            ),
            patch.object(mw, "allowed_roots", lambda: [self.tmp.resolve()]),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)

    def _land_delivery(self, run_id: str) -> None:
        """真实台账：绑定 submission → 终态计划（一份正文 + 一个文件组件）。"""
        self.tracker.upsert(
            run_id=run_id, scope_key="qqtpc:group:" + GROUP, adapter="milky",
            is_private=False, user_id="u1", group_id=GROUP, bot_self_id="3889000001",
            session_key="s", mode="reactive", expect_structured=True,
        )
        self.tracker.record_submission(
            idempotency_key=f"idem-{run_id}", request_body={}, target_fingerprint="x|y",
            scope_key="qqtpc:group:" + GROUP, session_key="s", mode="reactive",
            expect_structured=True, bot_self_id="3889000001",
        )
        self.tracker.bind_submission_and_register(
            idempotency_key=f"idem-{run_id}", run_id=run_id,
            scope_key="qqtpc:group:" + GROUP, adapter="milky", is_private=False,
            user_id="u1", group_id=GROUP, bot_self_id="3889000001",
            session_key="s", mode="reactive", expect_structured=True,
        )
        self.tracker.record_expected_delivery(
            run_id=run_id, output="§\n正文\n§", expect_structured=True,
        )
        prepare_terminal_batch(
            tracker=self.tracker, run_id=run_id,
            units=[{
                "body": "正文", "media_urls": [], "file_paths": [str(self.file)],
                "reply_to": "",
            }],
        )

    def _state(self, run_id: str) -> str:
        return self.tracker.get_component(run_id=run_id, component_id="file:0:0")["state"]

    async def test_milky_group_upload_uses_native_api(self) -> None:
        self.tracker.upsert(
            run_id="run-milky", scope_key="qqtpc:group:" + GROUP, adapter="milky",
            is_private=False, user_id="u1", group_id=GROUP, bot_self_id="3889000001",
            session_key="s", mode="reactive", expect_structured=True,
        )
        self.tracker.record_submission(
            idempotency_key="idem-run-milky", request_body={},
            target_fingerprint="x|y", scope_key="qqtpc:group:" + GROUP,
            session_key="s", mode="reactive", expect_structured=True,
            bot_self_id="3889000001",
        )
        self.tracker.bind_submission_and_register(
            idempotency_key="idem-run-milky", run_id="run-milky",
            scope_key="qqtpc:group:" + GROUP, adapter="milky", is_private=False,
            user_id="u1", group_id=GROUP, bot_self_id="3889000001",
            session_key="s", mode="reactive", expect_structured=True,
        )
        self.tracker.record_expected_delivery(
            run_id="run-milky", output="§\n正文\n§", expect_structured=True,
        )
        prepare_terminal_batch(
            tracker=self.tracker, run_id="run-milky",
            units=[{
                "body": "正文", "media_urls": [], "file_paths": [str(self.file)],
                "reply_to": "",
            }],
        )
        harness = MilkyHarness()
        harness.replies["upload_group_file"] = {"file_id": "f-1"}
        ctx = ob.DeliveryContext(
            tracker=self.tracker, run_id="run-milky", component_id="file:0:0",
            generation_guard=lambda: True,
        )
        target = alconna.Target(GROUP, adapter="milky")

        sent = await ob.send_local_files(
            bot=harness.bot, target=target, file_paths=[str(self.file)],
            adapter_name="milky", delivery=ctx, component_prefix="0:",
        )

        self.assertEqual(sent, 1)
        self.assertEqual(harness.actions(), ["upload_group_file"])
        params = harness.params_for("upload_group_file")[0]
        self.assertEqual(params["group_id"], int(GROUP))
        self.assertEqual(params["file_name"], self.file.name)
        self.assertTrue(str(params["file_uri"]).startswith("file://"))
        self.assertEqual(self._state("run-milky"), "sent")

    async def test_milky_private_upload_uses_native_api(self) -> None:
        self._land_delivery("run-private")
        harness = MilkyHarness()
        harness.replies["upload_private_file"] = {"file_id": "f-2"}
        ctx = ob.DeliveryContext(
            tracker=self.tracker, run_id="run-private", component_id="file:0:0",
            generation_guard=lambda: True,
        )

        sent = await ob.send_local_files(
            bot=harness.bot, target=alconna.Target(USER, private=True, adapter="milky"),
            file_paths=[str(self.file)], adapter_name="milky", delivery=ctx,
            component_prefix="0:",
        )

        self.assertEqual(sent, 1)
        self.assertEqual(harness.actions(), ["upload_private_file"])
        params = harness.params_for("upload_private_file")[0]
        self.assertEqual(params["user_id"], int(USER))
        self.assertEqual(params["file_name"], self.file.name)

    async def test_onebot_upload_payload_is_unchanged(self) -> None:
        self._land_delivery("run-onebot")
        bot = _RecorderBot()
        ctx = ob.DeliveryContext(
            tracker=self.tracker, run_id="run-onebot", component_id="file:0:0",
            generation_guard=lambda: True,
        )

        sent = await ob.send_local_files(
            bot=bot, target=alconna.Target(GROUP, adapter="onebotv11"),
            file_paths=[str(self.file)], adapter_name="onebotv11", delivery=ctx,
            component_prefix="0:",
        )

        self.assertEqual(sent, 1)
        self.assertEqual(bot.calls[0][0], "upload_group_file")
        self.assertEqual(bot.calls[0][1]["file"], str(self.file.resolve()))
        self.assertEqual(bot.calls[0][1]["name"], self.file.name)

    async def test_unsupported_platform_settles_file_component_not_required(self) -> None:
        self._land_delivery("run-qq")
        bot = _RecorderBot()
        ctx = ob.DeliveryContext(
            tracker=self.tracker, run_id="run-qq", component_id="file:0:0",
            generation_guard=lambda: True,
        )
        self.assertEqual(self._state("run-qq"), "pending")

        sent = await ob.send_local_files(
            bot=bot, target=alconna.Target(GROUP, adapter="qq"),
            file_paths=[str(self.file)], adapter_name="qq", delivery=ctx,
            component_prefix="0:",
        )

        self.assertEqual(sent, 0)
        self.assertEqual(bot.calls, [])
        self.assertEqual(
            self._state("run-qq"), "not_required",
            "不支持上传的平台必须让 file 组件离开 pending,不得永久滞留",
        )

    async def test_sent_file_is_not_downgraded_by_capability_settle(self) -> None:
        self._land_delivery("run-settled")
        self.tracker.mark_component_sent(
            run_id="run-settled", component_id="file:0:0", msg_ids=["ack-1"],
        )
        bot = _RecorderBot()
        ctx = ob.DeliveryContext(
            tracker=self.tracker, run_id="run-settled", component_id="file:0:0",
            generation_guard=lambda: True,
        )

        await ob.send_local_files(
            bot=bot, target=alconna.Target(GROUP, adapter="qq"),
            file_paths=[str(self.file)], adapter_name="qq", delivery=ctx,
            component_prefix="0:",
        )

        self.assertEqual(
            self._state("run-settled"), "sent",
            "已发送的文件组件不得被「平台不支持」改写成 not_required",
        )

    async def test_whitelist_rejection_still_marks_failed(self) -> None:
        self._land_delivery("run-reject")
        harness = MilkyHarness()
        ctx = ob.DeliveryContext(
            tracker=self.tracker, run_id="run-reject", component_id="file:0:0",
            generation_guard=lambda: True,
        )

        sent = await ob.send_local_files(
            bot=harness.bot, target=alconna.Target(GROUP, adapter="milky"),
            file_paths=["/etc/passwd"], adapter_name="milky", delivery=ctx,
            component_prefix="0:",
        )

        self.assertEqual(sent, 0)
        self.assertEqual(harness.actions(), [])
        self.assertEqual(self._state("run-reject"), "failed")


if __name__ == "__main__":
    unittest.main()
