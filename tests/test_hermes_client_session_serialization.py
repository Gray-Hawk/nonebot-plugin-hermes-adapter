"""HermesClient 会话级串行:同一 session key 的 chat 与 compress 一次只跑一个。

保活、自动压缩与正常 chat 共用同一个客户端入口。上一轮只在扫描前看 inflight,
保活开始后新来的正常 chat 仍可能与它并发写同一会话;这里改为在客户端入口按
session key 串行,不新建消息队列、不碰 handler 的 Bot/_refire。

覆盖:
1. 同 key:保活占住时正常 chat 在入口等待,释放后才进入;
2. 不同 key:互不阻塞,可并行;
3. 调用抛异常后锁必须释放;
4. chat 与 compress_session 同 key 也串行(两个公共方法共享一把锁)。
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import hermes_client as hc  # noqa: E402

TGT = SimpleNamespace(base_url="http://gw.test", api_key="", timeout=5, label="test")


class _Resp:
    def __init__(self, status_code: int = 200, payload: dict | None = None) -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self.content = b"x"
        self._payload = payload if payload is not None else {}

    def json(self) -> dict:
        return self._payload


class _GatedHTTP:
    """记录进入顺序/并发度,并把每个请求挂在同一个 event 上。"""

    def __init__(self, gate: asyncio.Event) -> None:
        self.gate = gate
        self.started: list[str] = []
        self.active = 0
        self.max_active = 0

    async def post(self, url: str, **_kwargs):
        kind = "compress" if url.split("?")[0].endswith("/compress") else "chat"
        self.started.append(kind)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await self.gate.wait()
        finally:
            self.active -= 1
        if kind == "compress":
            return _Resp(payload={"status": "compressed", "rotated": False, "in_place": True})
        return _Resp(payload={"choices": [{"message": {"content": "§\n§"}}]})


class SessionSerializationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        from nonebot_plugin_hermes_adapter.config import plugin_config

        self._iface = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "completions")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", self._iface)

    def _patch_client(self, fake) -> None:
        p = patch.object(hc, "_get_http_client", lambda: fake)
        p.start()
        self.addCleanup(p.stop)

    async def _chat(self, key: str, *, target=TGT):
        return await hc.hermes_client.chat(
            text="hi",
            session_key=key,
            user_id="u1",
            group_id="g1",
            adapter_name="qq",
            is_private=False,
            mode="passive",
            system_prompt="SYS",
            target=target,
        )

    async def _compress(self, key: str, *, target=TGT):
        return await hc.hermes_client.compress_session(tgt=target, session_key=key)

    async def test_same_key_chat_waits_for_running_chat(self):
        gate = asyncio.Event()
        fake = _GatedHTTP(gate)
        self._patch_client(fake)

        first = asyncio.create_task(self._chat("hermes-same"))
        await asyncio.sleep(0.05)
        self.assertEqual(fake.started, ["chat"], "第一轮必须已经进入传输")

        second = asyncio.create_task(self._chat("hermes-same"))
        await asyncio.sleep(0.05)
        self.assertEqual(fake.started, ["chat"], "同 key 的第二个调用在客户端入口等待")
        self.assertFalse(second.done())

        gate.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=2.0)
        self.assertEqual(fake.started, ["chat", "chat"], "释放后正常 chat 才进入")
        self.assertEqual(fake.max_active, 1, "同 key 不得并发进入传输")

    async def test_different_keys_run_concurrently(self):
        gate = asyncio.Event()
        fake = _GatedHTTP(gate)
        self._patch_client(fake)

        a = asyncio.create_task(self._chat("hermes-a"))
        b = asyncio.create_task(self._chat("hermes-b"))
        await asyncio.sleep(0.05)

        self.assertEqual(len(fake.started), 2, "不同 key 都在等传输,互不阻塞")
        self.assertEqual(fake.max_active, 2)

        gate.set()
        await asyncio.wait_for(asyncio.gather(a, b), timeout=2.0)

    async def test_lock_released_after_exception(self):
        gate = asyncio.Event()
        gate.set()
        fake = _GatedHTTP(gate)
        self._patch_client(fake)

        # 缺 base_url 的目标会让 chat 在入口抛出真实异常。
        broken = SimpleNamespace(api_key="", timeout=5, label="broken")
        with self.assertRaises(AttributeError):
            await self._chat("hermes-boom", target=broken)

        result = await asyncio.wait_for(self._chat("hermes-boom", target=TGT), timeout=2.0)
        self.assertFalse(result.is_transport_error)
        self.assertEqual(fake.started, ["chat"], "异常后同 key 仍可继续使用")

    async def test_compress_waits_for_chat_on_same_key(self):
        gate = asyncio.Event()
        fake = _GatedHTTP(gate)
        self._patch_client(fake)

        chat_task = asyncio.create_task(self._chat("hermes-mixed"))
        await asyncio.sleep(0.05)
        compress_task = asyncio.create_task(self._compress("hermes-mixed"))
        await asyncio.sleep(0.05)

        self.assertEqual(fake.started, ["chat"], "同 key 的压缩必须等 chat 结束")
        self.assertFalse(compress_task.done())

        gate.set()
        await asyncio.wait_for(asyncio.gather(chat_task, compress_task), timeout=2.0)
        self.assertEqual(fake.started, ["chat", "compress"])
        self.assertEqual(fake.max_active, 1)

    async def test_lock_table_does_not_grow_with_rotations(self):
        """反复换代/串行调用后锁表必须回到空,不随会话数无界增长。"""
        gate = asyncio.Event()
        gate.set()
        fake = _GatedHTTP(gate)
        self._patch_client(fake)

        for i in range(50):
            await self._chat(f"hermes-rot-{i}")

        self.assertEqual(hc._session_mutex.active_keys(), 0)
        self.assertEqual(len(fake.started), 50)


if __name__ == "__main__":
    unittest.main()
