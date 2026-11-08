"""Runs 手动压缩控制路径:客户端、回执与命令接线。

网关侧压缩器本身不在这里测(上游 `compress_now`),这里锁的是本部署补丁的三件事:
1. `hermes_client.compress_session` 的请求形态与 200/404/409 归类;
2. 轮换后命令必须采纳新 session id(不能写回已关闭旧会话);
3. 忙碌是明确冲突,只回执不重试。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402
from nonebot_plugin_hermes_adapter.core.session import SessionManager  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import commands  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers.commands import (  # noqa: E402
    compress_receipt_lines,
    handle_compress,
)

TGT = SimpleNamespace(base_url="http://gw.test", api_key="sk-test", timeout=5, label="test")


class _Resp:
    def __init__(self, status_code: int, payload=None, text: str = "", headers=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}
        self.content = b"x" if payload is not None or text else b""

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _Client:
    def __init__(self, resp):
        self.resp = resp
        self.calls: list[dict] = []

    async def post(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.resp


class PreviewArgTests(unittest.TestCase):
    """``/compress`` 余文 → (--preview 开关, focus)。"""

    @staticmethod
    def _msg(tail: str) -> str:
        head = f"/{commands.CMD_PREFIX} compress" if commands.CMD_PREFIX else "/compress"
        return f"{head} {tail}".strip()

    def test_bare_flag(self):
        self.assertEqual(commands.split_compress_args(self._msg("--preview")), (True, ""))

    def test_flag_before_focus(self):
        self.assertEqual(
            commands.split_compress_args(self._msg("--preview 保留配方")), (True, "保留配方"))

    def test_flag_after_focus(self):
        self.assertEqual(
            commands.split_compress_args(self._msg("保留配方 --preview")), (True, "保留配方"))

    def test_flag_keeps_boundary_args_for_the_gateway_parser(self):
        self.assertEqual(commands.split_compress_args(self._msg("here 3 --preview")), (True, "here 3"))

    def test_no_flag_keeps_focus_verbatim(self):
        self.assertEqual(commands.split_compress_args(self._msg("保留配方")), (False, "保留配方"))
        self.assertEqual(commands.split_compress_args(self._msg("")), (False, ""))

    def test_flag_must_be_a_whole_token(self):
        self.assertEqual(
            commands.split_compress_args(self._msg("--previewx 配方")), (False, "--previewx 配方"))


class CompressClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_in_place_success_reports_same_session(self):
        client = _Client(_Resp(200, {
            "status": "compressed", "session_id_after": "hermes-g1",
            "rotated": False, "in_place": True,
            "before_messages": 14, "after_messages": 9,
            "message": "🗜️ Compressed: 14 → 9 messages",
        }, headers={"X-Hermes-Session-Id": "hermes-g1"}))
        with patch.object(hc, "_get_http_client", return_value=client):
            result = await hc.hermes_client.compress_session(
                tgt=TGT, session_key="hermes-g1", focus_topic="保留配方")
        self.assertTrue(result["ok"])
        self.assertEqual(result["session_id"], "hermes-g1")
        self.assertFalse(result["rotated"])
        self.assertTrue(result["in_place"])
        self.assertEqual((result["before_messages"], result["after_messages"]), (14, 9))
        call = client.calls[0]
        # '+' 必须编码进路径,否则某些上游会把它当空格
        self.assertEqual(call["url"], "http://gw.test/api/plugins/session-compress/sessions/hermes-g1/compress")
        self.assertEqual(call["json"], {"focus": "保留配方"})
        self.assertEqual(call["headers"]["Authorization"], "Bearer sk-test")

    async def test_session_key_is_url_quoted(self):
        client = _Client(_Resp(200, {"status": "compressed", "session_id_after": "k", "rotated": False}))
        with patch.object(hc, "_get_http_client", return_value=client):
            await hc.hermes_client.compress_session(
                tgt=TGT, session_key="hermes-onebotv11+group+462540549")
        self.assertEqual(
            client.calls[0]["url"],
            "http://gw.test/api/plugins/session-compress/sessions/hermes-onebotv11%2Bgroup%2B462540549/compress",
        )

    async def test_rotation_comes_from_header_when_body_omits_it(self):
        client = _Client(_Resp(200, {"status": "compressed", "rotated": True, "in_place": False},
                               headers={"X-Hermes-Session-Id": "child-1"}))
        with patch.object(hc, "_get_http_client", return_value=client):
            result = await hc.hermes_client.compress_session(tgt=TGT, session_key="parent-1")
        self.assertTrue(result["ok"])
        self.assertTrue(result["rotated"])
        self.assertEqual(result["session_id"], "child-1")

    async def test_preview_sends_flag_and_parses_usage(self):
        client = _Client(_Resp(200, {
            "status": "preview", "session_id_after": "hermes-g1", "rotated": False,
            "in_place": False, "before_messages": 6, "after_messages": 6,
            "focus_topic": "keep codes",
            "message": "🗜️ Preview — no changes made.",
            "context_tokens": 61362, "context_messages": 311,
            "context_limit": 1048576, "context_model": "k3.1-pro",
            "context_source": "config", "history_tokens": 78904, "history_messages": 1290,
        }))
        with patch.object(hc, "_get_http_client", return_value=client):
            result = await hc.hermes_client.compress_session(
                tgt=TGT, session_key="hermes-g1", focus_topic="keep codes", preview=True)
        self.assertEqual(client.calls[0]["json"], {"focus": "keep codes", "preview": True})
        self.assertEqual(result["status"], "preview")
        self.assertEqual(result["preview"], {
            "context_tokens": 61362, "context_messages": 311, "context_limit": 1048576,
            "context_model": "k3.1-pro", "context_source": "config",
            "history_tokens": 78904, "history_messages": 1290,
        })

    async def test_preview_without_structured_fields_leaves_them_empty(self):
        client = _Client(_Resp(200, {"status": "preview", "session_id_after": "s"}))
        with patch.object(hc, "_get_http_client", return_value=client):
            result = await hc.hermes_client.compress_session(tgt=TGT, session_key="s", preview=True)
        self.assertEqual(result["preview"], {})

    async def test_busy_is_classified_not_retried(self):
        resp = _Resp(409, {"error": {"message": "Session is running a turn; retry when it settles",
                                     "code": "session_busy"}})
        client = _Client(resp)
        with patch.object(hc, "_get_http_client", return_value=client):
            result = await hc.hermes_client.compress_session(tgt=TGT, session_key="s")
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], 409)
        self.assertEqual(result["status"], "session_busy")
        self.assertEqual(len(client.calls), 1)  # 只发一次,不重试

    async def test_unknown_session_is_404(self):
        client = _Client(_Resp(404, {"error": {"message": "Session not found: s", "code": "session_not_found"}}))
        with patch.object(hc, "_get_http_client", return_value=client):
            result = await hc.hermes_client.compress_session(tgt=TGT, session_key="s")
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], 404)
        self.assertEqual(result["status"], "session_not_found")

    async def test_transport_error_is_reported_not_raised(self):
        class _Boom:
            async def post(self, *a, **k):
                raise RuntimeError("connect refused")
        with patch.object(hc, "_get_http_client", return_value=_Boom()):
            result = await hc.hermes_client.compress_session(tgt=TGT, session_key="s")
        self.assertFalse(result["ok"])
        self.assertIn("connect refused", result["error"])


class ReceiptTests(unittest.TestCase):
    def test_in_place_receipt(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "compressed", "before_messages": 14, "after_messages": 9,
             "in_place": True, "rotated": False, "message": "headline"},
            previous_key="k1")
        self.assertIn("14 → 9", lines[0])
        self.assertIn("in-place", "\n".join(lines))

    def test_rotation_receipt_reports_adoption(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "compressed", "rotated": True, "in_place": False,
             "session_id": "k2", "message": ""},
            previous_key="k1", adopted=True)
        text = "\n".join(lines)
        self.assertIn("k1 → k2", text)
        self.assertIn("已采纳", text)

    def test_rotation_receipt_warns_when_not_adopted(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "compressed", "rotated": True, "in_place": False,
             "session_id": "k2", "message": ""},
            previous_key="k1", adopted=False)
        self.assertIn("未能写入插件映射", "\n".join(lines))

    def test_aborted_receipt_says_nothing_changed(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "aborted", "before_messages": 20, "after_messages": 20,
             "in_place": False, "rotated": False, "message": "refused"},
            previous_key="k1")
        self.assertIn("未压缩", lines[0])
        self.assertIn("20", lines[0])

    def test_busy_receipt_is_explicit(self):
        lines = compress_receipt_lines(
            {"ok": False, "code": 409, "status": "session_busy", "error": "busy"},
            previous_key="k1")
        self.assertIn("正忙", lines[0])
        self.assertIn("不会并发压缩", lines[1])

    def test_missing_session_receipt(self):
        lines = compress_receipt_lines(
            {"ok": False, "code": 404, "status": "session_not_found", "error": "nope"},
            previous_key="k1")
        self.assertIn("没有这个会话", lines[0])

    def test_preview_receipt_shows_usage_over_limit(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "preview", "rotated": False, "in_place": False,
             "focus_topic": "保留配方", "message": "🗜️ Preview — no changes made.",
             "preview": {"context_tokens": 61362, "context_messages": 311,
                         "context_limit": 1048576, "context_model": "k3.1-pro",
                         "context_source": "config", "history_tokens": 78904,
                         "history_messages": 1290}},
            previous_key="k1")
        self.assertEqual(lines[0], "🗜️ 上下文预览（只读，未压缩）")
        # 卡面示例形态:当前占用 / 上限(百分比),消息数;数字半角、中文标点全角
        self.assertEqual(
            lines[1], "当前上下文约 61,362 / 1,048,576 tokens（约6％），311 条消息")
        self.assertIn("全量会话历史约 78,904 tokens", lines[2])
        self.assertIn("1290 条", lines[2])
        self.assertEqual(lines[3], "🎯 聚焦：保留配方")
        text = "\n".join(lines)
        for banned in ("已压缩", "压缩完成", "Compressed", "→"):
            self.assertNotIn(banned, text)

    def test_preview_receipt_without_known_limit(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "preview", "focus_topic": "",
             "preview": {"context_tokens": 1863, "context_messages": 12, "context_limit": 0,
                         "context_model": "mystery", "context_source": "default",
                         "history_tokens": 1863, "history_messages": 12}},
            previous_key="k1")
        self.assertEqual(lines[1], "当前上下文约 1,863 tokens（上限未知），12 条消息")
        self.assertNotIn("/", lines[1])          # 不编造上限
        self.assertEqual(len(lines), 2)          # 无全量历史差异、无 focus 行

    def test_preview_receipt_under_one_percent_is_not_zero(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "preview",
             "preview": {"context_tokens": 1000, "context_messages": 5, "context_limit": 1048576,
                         "history_tokens": 1000, "history_messages": 5}},
            previous_key="k1")
        self.assertIn("（不足1％）", lines[1])

    def test_preview_receipt_falls_back_to_gateway_message(self):
        lines = compress_receipt_lines(
            {"ok": True, "status": "preview", "message": "🗜️ Preview — no changes made.",
             "preview": {}},
            previous_key="k1")
        self.assertEqual(lines[0], "🗜️ 压缩预览（只读，未写入）")
        self.assertIn("no changes made", lines[1])


class SessionAdoptionTests(unittest.TestCase):
    def test_adopt_rotation_rewrites_mapping(self):
        sm = SessionManager()
        key = sm.get_session_key("onebotv11", False, "u1", "g1")
        self.assertTrue(sm.adopt_session_key(key, "child-1"))
        self.assertEqual(sm.get_session_key("onebotv11", False, "u1", "g1"), "child-1")


class _RecorderMessage:
    sent: list[str] = []

    def __init__(self, text: str):
        self.text = text

    async def send(self, **kwargs):
        _RecorderMessage.sent.append(self.text)


class HandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _RecorderMessage.sent = []
        self.target = SimpleNamespace(private=False, id="462540549")
        self.session_manager = MagicMock()
        self.session_manager.get_session_key.return_value = "hermes-g1"
        self.session_manager.adopt_session_key.return_value = True

    async def _call(self, result, plaintext: str | None = None):
        patches = [
            patch.object(commands.alconna, "get_target", return_value=self.target),
            patch.object(commands, "check_isolation", return_value=True),
            patch.object(commands, "get_adapter_name", return_value="onebotv11"),
            patch.object(commands, "_is_admin", return_value=True),
            patch.object(commands, "hermes_control_plane_enabled", return_value=True),
            patch.object(commands, "resolve_target", return_value=TGT),
            patch.object(commands, "session_manager", self.session_manager),
            patch.object(commands.alconna, "UniMessage", _RecorderMessage),
            patch.object(commands.hermes_client, "compress_session",
                         AsyncMock(return_value=result)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        prefix = f"/{commands.CMD_PREFIX} compress" if commands.CMD_PREFIX else "/compress"
        event = SimpleNamespace(get_user_id=lambda: "u1",
                                get_plaintext=lambda: plaintext or f"{prefix} 保留配方")
        await handle_compress(bot=MagicMock(), event=event, matcher=MagicMock())
        return commands.hermes_client.compress_session

    async def test_rotation_is_adopted_by_command(self):
        call = await self._call({
            "ok": True, "status": "compressed", "rotated": True, "in_place": False,
            "session_id": "child-1", "before_messages": 14, "after_messages": 9, "message": "",
        })
        self.assertEqual(call.await_args.kwargs["focus_topic"], "保留配方")
        self.session_manager.adopt_session_key.assert_called_once_with("hermes-g1", "child-1")
        self.assertTrue(any("已采纳" in text for text in _RecorderMessage.sent))

    async def test_in_place_does_not_adopt(self):
        await self._call({
            "ok": True, "status": "compressed", "rotated": False, "in_place": True,
            "session_id": "hermes-g1", "before_messages": 14, "after_messages": 9, "message": "",
        })
        self.session_manager.adopt_session_key.assert_not_called()

    async def test_busy_reports_conflict(self):
        await self._call({"ok": False, "code": 409, "status": "session_busy", "error": "busy"})
        self.assertTrue(any("正忙" in text for text in _RecorderMessage.sent))

    async def test_preview_command_passes_flag_and_never_adopts(self):
        prefix = f"/{commands.CMD_PREFIX} compress" if commands.CMD_PREFIX else "/compress"
        call = await self._call(
            {"ok": True, "status": "preview", "rotated": False, "in_place": False,
             "session_id": "hermes-g1", "focus_topic": "",
             "preview": {"context_tokens": 61362, "context_messages": 311,
                         "context_limit": 1048576, "context_model": "k3.1-pro",
                         "context_source": "config", "history_tokens": 61362,
                         "history_messages": 311}},
            plaintext=f"{prefix} --preview")
        self.assertTrue(call.await_args.kwargs["preview"])
        self.assertEqual(call.await_args.kwargs["focus_topic"], "")
        self.session_manager.adopt_session_key.assert_not_called()
        text = _RecorderMessage.sent[-1]
        self.assertIn("当前上下文约 61,362 / 1,048,576 tokens（约6％），311 条消息", text)
        self.assertNotIn("已压缩", text)

    async def test_preview_command_keeps_focus(self):
        prefix = f"/{commands.CMD_PREFIX} compress" if commands.CMD_PREFIX else "/compress"
        call = await self._call(
            {"ok": True, "status": "preview", "rotated": False, "in_place": False,
             "focus_topic": "保留配方",
             "preview": {"context_tokens": 10, "context_messages": 1, "context_limit": 0}},
            plaintext=f"{prefix} 保留配方 --preview")
        self.assertEqual(call.await_args.kwargs["focus_topic"], "保留配方")
        self.assertTrue(call.await_args.kwargs["preview"])
        self.assertIn("🎯 聚焦：保留配方", _RecorderMessage.sent[-1])

    async def test_normal_compress_is_not_marked_preview(self):
        call = await self._call({
            "ok": True, "status": "compressed", "rotated": False, "in_place": True,
            "session_id": "hermes-g1", "before_messages": 14, "after_messages": 9, "message": "",
        })
        self.assertFalse(call.await_args.kwargs["preview"])


if __name__ == "__main__":
    unittest.main()
