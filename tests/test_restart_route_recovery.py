"""P-10 回归：重启后 Bot 连接即恢复送达路由,唤醒 reattach 与 ready 对账。

真实 SQLite 台账 + 真实 BotRegistry + 真实恢复/对账/出站链;只把平台边界
(``alconna.UniMessage.send``)换成进程内替身,gateway 用一个真实 loopback HTTP
服务(走真实 httpx,不做 AsyncClient 假类)。

覆盖:
  - 负对照:空 registry 时对账只 deferred=bot_not_ready、零发送、组件保持 pending;
  - 空 registry + ready delivery:bot 连接后补 synthetic 路由并补投,且只发一次
    (重复连接不再发);
  - 空 registry + running active run:bot 连接后唤醒 ``_resume_row``,在没有任何
    新入站文本的情况下经真实 status→plan→send 链补发一次;
  - 真实感知 entry 优先:synthetic 不覆盖既有的真实 entry,也不碰消息缓冲;
  - 私聊 scope 按 user_id 恢复 private Target。
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import tempfile
import threading
import time
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import outbound as ob  # noqa: E402
from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor  # noqa: E402
from nonebot_plugin_hermes_adapter.core.bot_registry import BotRegistry  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_delivery_config import (  # noqa: E402
    RunsDeliveryConfig,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    prepare_terminal_batch,
)

BOT_ID = "999"


def _load_run_resume():
    """按真实包路径加载 tasks.run_resume，绕开 tasks/__init__ 的 apscheduler 依赖。"""
    pkg_name = "nonebot_plugin_hermes_adapter.tasks"
    if pkg_name not in sys.modules:
        package = types.ModuleType(pkg_name)
        package.__path__ = [str(ROOT / "tasks")]
        package.__package__ = pkg_name
        sys.modules[pkg_name] = package
    return importlib.import_module(f"{pkg_name}.run_resume")


run_resume = _load_run_resume()


class _GatewayHandler(BaseHTTPRequestHandler):
    """真实 loopback gateway:任何 GET 都回当前 status 载荷(测试逐例设置)。"""

    payload: dict = {}

    def do_GET(self):  # noqa: N802 - http.server 约定的方法名
        body = json.dumps(self.payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A002 - 静默测试服务日志
        return


class RestartRouteRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.registry = BotRegistry()  # 重启即清:真实实现就是空表
        self.message_buffer = MagicMock()
        self.runtime = SimpleNamespace(
            run_tracker=self.tracker,
            bot_registry=self.registry,
            message_buffer=self.message_buffer,
        )
        self.cfg = RunsDeliveryConfig(mode="repair", grace_sec=0.0)
        self.platform_calls: list[tuple[str, bool]] = []
        self.sent_texts: list[str] = []
        # 每个用例显式覆盖自己那条 run 的 gateway 终态,避免用例间载荷串味。
        _GatewayHandler.payload = {
            "status": "completed",
            "output": '{}\n{"body":"重启恢复正文"}',
            "updated_at": time.time(),
        }

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _GatewayHandler)
        self.addCleanup(self.server.server_close)
        self.server_thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.server_thread.start()
        self.addCleanup(self.server.shutdown)
        port = self.server.server_address[1]
        self.base_url = f"http://127.0.0.1:{port}"

        self.platform_patches = [
            patch.object(monitor, "_runtime", lambda: self.runtime),
            patch.object(monitor, "load_runs_delivery_config", lambda: self.cfg),
            patch.object(monitor, "get_bot", lambda self_id: MagicMock(self_id=self_id)),
            patch.object(monitor, "_maybe_prune_delivery_ledger", lambda t: None),
            patch.object(ob, "load_misc_config", lambda: SimpleNamespace(max_length=4000)),
            patch.object(ob, "load_chat_config", lambda: SimpleNamespace(long_reply_forward=False)),
            patch.object(ob, "get_bot_nickname", AsyncMock(return_value="ledger-bot")),
            patch.object(ob.alconna.UniMessage, "send", self._platform_boundary()),
            patch.object(run_resume._mcp, "run_tracker", self.tracker),
            patch.object(run_resume._mcp, "bot_registry", self.registry),
            patch.object(
                run_resume._mcp, "inflight",
                MagicMock(is_inflight=MagicMock(return_value=False)),
            ),
            patch.object(run_resume._mcp, "message_buffer", self.message_buffer),
            patch.object(
                run_resume._mcp, "active_sessions",
                MagicMock(get_if_trigger_alive=MagicMock(return_value=None)),
            ),
            patch.object(run_resume, "get_bot", MagicMock(return_value=MagicMock(self_id=BOT_ID))),
            patch.object(run_resume, "get_bot_nickname", AsyncMock(return_value="nano")),
            patch.object(
                run_resume, "deliver_companion_run_output", AsyncMock(return_value=False)
            ),
            patch.object(
                run_resume, "resolve_target",
                MagicMock(return_value=SimpleNamespace(
                    base_url=self.base_url, api_key="", timeout=5.0,
                )),
            ),
        ]
        for item in self.platform_patches:
            item.start()
            self.addCleanup(item.stop)

    def _platform_boundary(self):
        """平台边界替身:按 UniMessage 实例方法绑定语义接收 self。"""

        async def platform_send(uni_self, *, target=None, bot=None, **kwargs):
            self.platform_calls.append(
                (getattr(target, "id", "?"), bool(getattr(target, "private", False)))
            )
            self.sent_texts.append(str(uni_self))
            return SimpleNamespace(msg_ids=[f"ack-{len(self.platform_calls)}"])

        return platform_send

    def _bot(self):
        return SimpleNamespace(
            self_id=BOT_ID,
            adapter=SimpleNamespace(get_name=lambda: "OneBot V11"),
        )

    def _bind(self, *, run_id: str, scope: str, is_private: bool, user_id: str, group_id):
        self.tracker.upsert(
            run_id=run_id, scope_key=scope, adapter="onebotv11", is_private=is_private,
            user_id=user_id, group_id=group_id, bot_self_id=BOT_ID, session_key="s",
            mode="reactive", expect_structured=True,
        )
        self.tracker.record_submission(
            idempotency_key=f"idem-{run_id}", request_body={}, target_fingerprint="x|y",
            scope_key=scope, session_key="s", mode="reactive",
            expect_structured=True, bot_self_id=BOT_ID,
        )
        self.tracker.bind_submission_and_register(
            idempotency_key=f"idem-{run_id}", run_id=run_id, scope_key=scope,
            adapter="onebotv11", is_private=is_private, user_id=user_id, group_id=group_id,
            bot_self_id=BOT_ID, session_key="s", mode="reactive", expect_structured=True,
        )

    def _land_ready_delivery(
        self, *, run_id: str = "run-ready", group_id: str = "g1", body: str = "重启前应发出的正文"
    ) -> None:
        """落一条「run 已终态、交付待对账、进程刚重启」的真实台账现场。"""
        self._bind(
            run_id=run_id, scope=f"onebotv11:group:{group_id}", is_private=False,
            user_id="u1", group_id=group_id,
        )
        self.tracker.record_expected_delivery(
            run_id=run_id, output=f'{{}}\n{{"body":"{body}"}}', expect_structured=True,
        )
        prepare_terminal_batch(
            tracker=self.tracker, run_id=run_id,
            units=[{"body": body, "media_urls": [], "file_paths": [], "reply_to": ""}],
        )
        # 真实链:expected 先落账,再删 active 行(与 turns 收尾同序);重启后只剩 ready 交付。
        self.tracker.finish(run_id)

    async def _wait_until(self, predicate, timeout: float = 3.0) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.02)
        return bool(predicate())

    def _component(self, run_id: str, component_id: str = "primary:0") -> dict:
        return self.tracker.get_component(run_id=run_id, component_id=component_id)

    async def test_without_bot_connect_reconcile_defers_and_sends_nothing(self) -> None:
        """负对照（修复前现场）：空 registry 时启动对账 deferred，回复一直发不出去。"""
        self._land_ready_delivery()
        self.assertEqual(self.registry.known(), [])

        await monitor.reconcile_run("run-ready", reason="startup")

        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-ready")["state"], "pending")
        self.assertEqual(self.tracker.get_delivery("run-ready")["expected_state"], "ready")

    async def test_bot_connect_restores_route_and_delivers_once(self) -> None:
        self._land_ready_delivery()
        self.assertEqual(self.registry.known(), [])

        run_resume.recover_bot_deliveries(self._bot())

        self.assertTrue(
            await self._wait_until(lambda: self._component("run-ready")["state"] == "sent")
        )
        entry = self.registry.get("onebotv11", "group", "g1")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.bot_self_id, BOT_ID)
        self.assertTrue(entry.synthetic)
        self.assertEqual(entry.target.id, "g1")
        self.assertFalse(entry.target.private)
        self.assertEqual(self.platform_calls, [("g1", False)])
        self.assertEqual(self.sent_texts, ["重启前应发出的正文"])
        self.assertEqual(self._component("run-ready", "history:0")["state"], "sent")
        self.assertEqual(
            self.tracker.get_delivery("run-ready")["expected_state"], "terminal"
        )

        # 断线重连再次触发同一钩子:不补第二条路由、不重发
        run_resume.recover_bot_deliveries(self._bot())
        await self._wait_until(lambda: False, timeout=0.2)
        self.assertEqual(self.platform_calls, [("g1", False)])
        self.assertEqual(self._component("run-ready")["attempt_count"], 1)

    async def test_bot_connect_resumes_running_run_without_new_inbound_text(self) -> None:
        """重启时仍在跑的 run:bot 连上即 reattach,真实 status 链补发一次。"""
        self._bind(run_id="run-active", scope="onebotv11:group:g1", is_private=False,
                   user_id="u1", group_id="g1")
        _GatewayHandler.payload = {
            "status": "completed",
            "output": '{}\n{"body":"停机期间完成的回复"}',
            "updated_at": time.time(),
        }
        self.assertEqual(self.registry.known(), [])

        run_resume.recover_bot_deliveries(self._bot())

        self.assertTrue(
            await self._wait_until(
                lambda: self.tracker.get_by_run_id("run-active") is None
            )
        )
        self.assertEqual(self.platform_calls, [("g1", False)])
        self.assertEqual(self.sent_texts, ["停机期间完成的回复"])
        self.assertEqual(self._component("run-active")["state"], "sent")
        # 全程没有任何入站感知写入:落进消息缓冲的只有 bot 自己的出站历史行
        appended = [call.args[0] for call in self.message_buffer.append.call_args_list]
        self.assertTrue(appended)
        self.assertTrue(all(item.is_bot for item in appended))

    async def test_real_perception_entry_is_not_overwritten(self) -> None:
        """真实感知 entry 优先:恢复只补缺,不覆盖既有真实 target。"""
        real_target = SimpleNamespace(id="g1", private=False, source="perception")
        self.registry.upsert(
            adapter="onebotv11", scope="group", scope_id="g1",
            bot_self_id=BOT_ID, target=real_target, ts=0,
        )
        self._land_ready_delivery()

        restored = run_resume.restore_bot_routes(self._bot())

        self.assertEqual(restored, 0)
        entry = self.registry.get("onebotv11", "group", "g1")
        self.assertFalse(entry.synthetic)
        self.assertIs(entry.target, real_target)
        self.message_buffer.append.assert_not_called()

        # 真实感知 entry 本身也就是补投所需的 target:对账照常送达
        await monitor.reconcile_run("run-ready", reason="startup")
        self.assertEqual(self.platform_calls, [("g1", False)])

    async def test_startup_resume_then_bot_connect_delivers_both(self) -> None:
        """生产顺序:空 registry 启动 → 首轮恢复/对账 deferred → bot 连接后全部补上。"""
        self._land_ready_delivery(run_id="run-ready2", body="重启前应发出的正文")
        self._bind(run_id="run-live2", scope="onebotv11:group:g2", is_private=False,
                   user_id="u1", group_id="g2")
        _GatewayHandler.payload = {
            "status": "completed",
            "output": '{}\n{"body":"停机期间完成的回复"}',
            "updated_at": time.time(),
        }
        with patch.object(run_resume, "_RESUME_RETRY_SEC", 0.01), \
                patch.object(run_resume, "_RESUME_RETRY_MAX", 3):
            await run_resume.resume_active_runs()
            # 等首轮恢复任务全部退出(它们拿不到路由,只会保留行)
            self.assertTrue(
                await self._wait_until(lambda: not run_resume._resume_inflight)
            )
        self.assertEqual(self.platform_calls, [])
        self.assertEqual(self._component("run-ready2")["state"], "pending")
        self.assertIsNotNone(self.tracker.get_by_run_id("run-live2"))

        run_resume.recover_bot_deliveries(self._bot())

        self.assertTrue(
            await self._wait_until(
                lambda: self._component("run-ready2")["state"] == "sent"
                and self.tracker.get_by_run_id("run-live2") is None
            )
        )
        self.assertEqual(
            sorted(self.sent_texts), sorted(["重启前应发出的正文", "停机期间完成的回复"])
        )
        self.assertEqual(len(self.platform_calls), 2)  # 每条 run 恰好一次
        self.assertEqual(self._component("run-ready2")["attempt_count"], 1)

    async def test_overlapping_active_and_ready_send_once(self) -> None:
        """崩溃窗口现场:active 行与 ready 交付同时存在,恢复与对账重叠也只发一次。"""
        self._bind(run_id="run-overlap", scope="onebotv11:group:g1", is_private=False,
                   user_id="u1", group_id="g1")
        self.tracker.record_expected_delivery(
            run_id="run-overlap", output='{}\n{"body":"崩溃窗口补投"}', expect_structured=True,
        )
        prepare_terminal_batch(
            tracker=self.tracker, run_id="run-overlap",
            units=[{"body": "崩溃窗口补投", "media_urls": [], "file_paths": [], "reply_to": ""}],
        )
        # 注意:这里刻意不 finish —— active_runs 与 ready 交付并存,恢复与对账会同时被唤醒。
        _GatewayHandler.payload = {
            "status": "completed",
            "output": '{}\n{"body":"崩溃窗口补投"}',
            "updated_at": time.time(),
        }

        run_resume.recover_bot_deliveries(self._bot())

        self.assertTrue(
            await self._wait_until(
                lambda: self._component("run-overlap")["state"] == "sent"
            )
        )
        await self._wait_until(lambda: False, timeout=0.3)  # 让两路重叠任务跑完
        self.assertEqual(self.platform_calls, [("g1", False)])
        self.assertEqual(self._component("run-overlap")["attempt_count"], 1)
        self.assertEqual(self._component("run-overlap")["msg_ids"], ["ack-1"])

    async def test_private_scope_route_is_restored(self) -> None:
        """私聊 scope 同样按 user_id 恢复(private=True 的 Target)。"""
        self._bind(run_id="run-private", scope="onebotv11:private:u1",
                   is_private=True, user_id="u1", group_id=None)
        self.tracker.record_expected_delivery(
            run_id="run-private", output='{}\n{"body":"私聊补投"}', expect_structured=True,
        )
        prepare_terminal_batch(
            tracker=self.tracker, run_id="run-private",
            units=[{"body": "私聊补投", "media_urls": [], "file_paths": [], "reply_to": ""}],
        )
        self.tracker.finish("run-private")
        _GatewayHandler.payload = {
            "status": "completed",
            "output": '{}\n{"body":"私聊补投"}',
            "updated_at": time.time(),
        }

        run_resume.recover_bot_deliveries(self._bot())

        self.assertTrue(
            await self._wait_until(
                lambda: self._component("run-private")["state"] == "sent"
            )
        )
        entry = self.registry.get("onebotv11", "private", "u1")
        self.assertIsNotNone(entry)
        self.assertTrue(entry.target.private)
        self.assertEqual(self.platform_calls, [("u1", True)])


if __name__ == "__main__":
    unittest.main()
