"""阶段4最小收口：ACP 重启回归（离线）。

只把预研已实测的语义固化成回归，不改产品行为：

- 进程死亡中途：已发单位不重发、未闭合 pending 不外发、`acp_sessions` 行保留、
  没有伪造的成功结果／stopReason；
- 新客户端（新进程）`session/resume` 复用同一 sessionId，每轮重带 `mcpServers`；
  `system_hash` 不变时 resume 不带 `_meta`，变化时才 append 一次；
- 不调用 `session/load` 回灌历史、不做跨轮同文去重、不伪造 run_id。

夹具复用 `tests/fixtures/fake_agy_acp.py`（只新增 `--persist-sessions`：把会话 ID
落到文件，等价于真实 agy 的跨进程会话库）与 `tests/test_antigravity_acp.py` 的
`FakeACPBase`／真实 `AntigravityACPClient`，不另造夹具体系。
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from tests.test_antigravity_acp import FakeACPBase, sha  # noqa: E402
from tests.test_antigravity_mcp_tools import (  # noqa: E402
    GROUP,
    expected_mcp_servers,
    mcp_config,
)

from nonebot_plugin_agent_adapter.backends.antigravity.client import (  # noqa: E402
    ACPProcessError,
)
from nonebot_plugin_agent_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)

SESSION_KEY = "hermes-onebotv11+group+462540549"

# 夹具默认把 reply 对半切成两帧：第一帧 `§\n第一段已发\n§\n第` 正好闭合一段并留下
# 未闭合的 `§\n第`；`--exit-after-chunk` 在发完第一帧后直接退出（等价 front 死）。
DEATH_REPLY = "§\n第一段已发\n§\n第二段还没写完的尾巴内容"
DEATH_CLOSED_UNITS = ["第一段已发"]

GROUP_META = {
    "adapter": "onebotv11",
    "is_private": False,
    "user_id": "123",
    "group_id": GROUP,
}


class RestartFixture(FakeACPBase):
    """复用既有 FakeACPBase，只补重启所需的两个动作：重开 DB、读 outbound 帧。"""

    def reopen_store(self) -> SessionKeyStore:
        """模拟 NB 新进程：重新打开同一个 session_keys.db。"""
        store = SessionKeyStore(self.tmp / "session_keys.db")
        self.addCleanup(store.close)
        return store

    def outbound(self) -> list[dict]:
        """夹具真实发出的帧（`send()` 落盘），用来证明没有 result／stopReason。"""
        path = Path(str(self.log_path) + ".sent")
        if not path.exists():
            return []
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def results_for(self, request_id: object) -> list[dict]:
        return [
            frame
            for frame in self.outbound()
            if frame.get("id") == request_id and "result" in frame
        ]

    async def turn(self, client, *, system_prompt="SYS", user_text="hi", store=None,
                   turn_meta=None) -> tuple[str, list[str]]:
        units: list[str] = []
        text = await client.prompt(
            session_key=SESSION_KEY,
            system_prompt=system_prompt,
            user_text=user_text,
            store=self.store if store is None else store,
            turn_meta=turn_meta,
            on_unit=lambda decision: units.append(decision["body"]),
        )
        return text, units


class ProcessDeathMidTurnTests(RestartFixture):
    """进程中途死亡：只保证已发不重复、未闭合不补发、不装成功。"""

    async def _die_mid_turn(self) -> list[str]:
        self.reply = DEATH_REPLY
        self.flags = ("--exit-after-chunk", "--persist-sessions")
        client = self.make_client()
        units: list[str] = []
        with self.assertRaises(ACPProcessError):
            await client.prompt(
                session_key=SESSION_KEY,
                system_prompt="SYS",
                user_text="开始",
                store=self.store,
                on_unit=lambda decision: units.append(decision["body"]),
            )
        return units

    async def test_dead_turn_emits_only_closed_units_and_keeps_the_session_row(self) -> None:
        units = await self._die_mid_turn()

        # 前提核验：死亡确实发生在**半截正文之后**——客户端已实收一帧，
        # 该帧闭合了一段并留下未闭合的 `§\n第`。否则下面的断言是空的。
        streamed = [
            ((frame.get("params") or {}).get("update") or {}).get("content", {}).get("text", "")
            for frame in self.outbound()
            if frame.get("method") == "session/update"
            and ((frame.get("params") or {}).get("update") or {}).get("sessionUpdate")
            == "agent_message_chunk"
        ]
        self.assertEqual(streamed, ["§\n第一段已发\n§\n第"])

        self.assertEqual(units, DEATH_CLOSED_UNITS, "已闭合单位照发，且只发一次")
        self.assertFalse(
            any("第二段" in unit for unit in units),
            "未闭合的尾巴（此刻只有 `§\\n第`）不得外发",
        )
        # 会话映射行是本轮唯一的恢复台账：进程死不得清掉它。
        record = self.store.load_acp_session(SESSION_KEY)
        self.assertIsNotNone(record, "acp_sessions 行必须保留")
        session_id, system_hash, model = record
        self.assertTrue(session_id)
        self.assertEqual(system_hash, sha("SYS"))
        self.assertEqual(model, "")

    async def test_dead_turn_has_no_result_frame_or_stop_reason(self) -> None:
        await self._die_mid_turn()

        prompts = self.entries("session/prompt")
        self.assertEqual(len(prompts), 1)
        self.assertEqual(
            self.results_for(prompts[0]["id"]),
            [],
            "死亡轮不得补一条 result／stopReason（不能伪造成 end_turn）",
        )
        self.assertFalse(
            any("stopReason" in json.dumps(frame) for frame in self.outbound())
        )

    async def test_next_client_resumes_same_session_without_replaying_sent_units(self) -> None:
        units = await self._die_mid_turn()
        self.assertEqual(units, DEATH_CLOSED_UNITS)
        session_id = self.store.load_acp_session(SESSION_KEY)[0]

        # 新进程 + 重开的 DB：按 acp_sessions 续聊，不新建会话、不重放旧单位。
        store = self.reopen_store()
        self.reply = "§\n第三段\n§"
        self.flags = ("--persist-sessions",)
        client = self.make_client()
        text, fresh_units = await self.turn(client, user_text="继续", store=store)

        self.assertEqual(text, self.reply)
        self.assertEqual(fresh_units, ["第三段"], "上一轮已发单位不得重发")
        self.assertEqual(len(self.entries("session/new")), 1, "重启后不得新建会话")
        resume = self.entries("session/resume")
        self.assertEqual(len(resume), 1)
        self.assertEqual(resume[0]["params"]["sessionId"], session_id)
        self.assertEqual(self.entries("session/load"), [], "不用 session/load 回灌")


class DeathChatResultTests(RestartFixture):
    """客户端之上：半截正文 + 进程死亡必须落成传输失败，不装成功。"""

    async def test_partial_body_then_death_is_a_transport_error(self) -> None:
        from nonebot_plugin_agent_adapter.backends.antigravity import client as ag
        from nonebot_plugin_agent_adapter.config import plugin_config
        from nonebot_plugin_agent_adapter.core import hermes_client as hc

        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "antigravity")
        self.addCleanup(object.__setattr__, plugin_config, "hermes_api_interface", previous)

        self.reply = DEATH_REPLY
        self.flags = ("--exit-after-chunk", "--persist-sessions")
        client = self.make_client()
        target = SimpleNamespace(
            base_url="http://127.0.0.1:1", api_key="", timeout=5, label="fake"
        )
        with patch.object(ag, "get_client", return_value=client), patch.object(
            hc, "_get_http_client", side_effect=AssertionError("Hermes HTTP 不得被调用")
        ):
            result = await hc.hermes_client.chat(
                text="",
                session_key=SESSION_KEY,
                user_id="123",
                group_id=GROUP,
                adapter_name="onebotv11",
                is_private=False,
                mode="passive",
                expect_structured=True,
                structured_tool_name="submit_decision",
                system_prompt="SYS",
                user_content_override="开始",
                target=target,
            )

        self.assertTrue(result.is_transport_error, "进程死亡必须走传输失败面")
        self.assertTrue(result.raw_text.startswith("⚠️ Antigravity 调用失败"))
        self.assertIsNone(result.run_id, "没有 Runs 台账时不得伪造 run_id")
        self.assertFalse(
            result.terminal_decisions, "半截正文不得变成终态正文"
        )


class RestartResumeContractTests(RestartFixture):
    """普通重启后的恢复契约：同一 sessionId + 每轮 mcpServers + 只按哈希补 _meta。"""

    def setUp(self) -> None:
        super().setUp()
        mcp_config(self)

    async def _first_turn(self, *, system_prompt: str = "SYS") -> str:
        self.reply = "§\n第一段\n§"
        self.flags = ("--persist-sessions",)
        client = self.make_client()
        await self.turn(
            client,
            system_prompt=system_prompt,
            user_text="一",
            turn_meta=GROUP_META,
        )
        await client.close()
        return self.store.load_acp_session(SESSION_KEY)[0]

    async def test_restart_resumes_the_same_session_and_repeats_mcp_servers(self) -> None:
        session_id = await self._first_turn()

        store = self.reopen_store()
        self.reply = "§\n第二段\n§"
        client = self.make_client()
        await self.turn(client, user_text="二", store=store, turn_meta=GROUP_META)

        new = self.entries("session/new")
        resume = self.entries("session/resume")
        self.assertEqual(len(new), 1, "重启后必须 resume，不得新建会话")
        self.assertEqual(len(resume), 1)
        self.assertEqual(resume[0]["params"]["sessionId"], session_id)
        self.assertEqual(resume[0]["params"]["cwd"], str(self.tmp))
        self.assertEqual(
            resume[0]["params"]["mcpServers"],
            expected_mcp_servers(),
            "每次 resume 都要重带 mcpServers（跨进程不粘）",
        )
        self.assertNotIn(
            "_meta", resume[0]["params"], "system_hash 不变时不得重复 append 系统提示"
        )
        self.assertEqual(self.entries("session/load"), [], "不得用 session/load 回灌")
        self.assertEqual(
            store.load_acp_session(SESSION_KEY), (session_id, sha("SYS"), "")
        )

    async def test_restart_with_changed_system_prompt_appends_meta_only_that_turn(self) -> None:
        await self._first_turn(system_prompt="SYS-A")

        store = self.reopen_store()
        self.reply = "§\nA2\n§"
        client = self.make_client()
        await self.turn(
            client, system_prompt="SYS-B", user_text="二", store=store,
            turn_meta=GROUP_META,
        )

        resume = self.entries("session/resume")
        self.assertEqual(len(resume), 1)
        self.assertEqual(
            resume[0]["params"]["_meta"]["antigravity"]["systemPrompt"]["append"], "SYS-B"
        )
        self.assertEqual(store.load_acp_session(SESSION_KEY)[1], sha("SYS-B"))

        # 哈希已更新：下一轮同上下文 resume 不再带 _meta。
        await self.turn(
            client, system_prompt="SYS-B", user_text="三", store=store,
            turn_meta=GROUP_META,
        )
        resumes = self.entries("session/resume")
        self.assertEqual(len(resumes), 2)
        self.assertNotIn("_meta", resumes[1]["params"])

    async def test_restart_sends_the_same_wording_again_in_a_new_turn(self) -> None:
        self.reply = "§\n同一段\n§"
        self.flags = ("--persist-sessions",)
        client = self.make_client()
        _text, units_first = await self.turn(client, user_text="一", turn_meta=GROUP_META)
        await client.close()

        store = self.reopen_store()
        client = self.make_client()
        _text, units_second = await self.turn(client, user_text="二", store=store,
                                              turn_meta=GROUP_META)

        self.assertEqual(units_first, ["同一段"])
        self.assertEqual(
            units_second,
            ["同一段"],
            "跨轮同文是不同正文：不得有跨轮去重或发送台账",
        )
        self.assertEqual(self.entries("session/load"), [])


if __name__ == "__main__":
    unittest.main()
