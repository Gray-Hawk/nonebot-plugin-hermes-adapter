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

    async def _call(self, result):
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
        event = SimpleNamespace(get_user_id=lambda: "u1",
                                get_plaintext=lambda: "/feiyu compress 保留配方")
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


if __name__ == "__main__":
    unittest.main()
