"""P-04 尝试令牌围栏回归：迟到尝试不得覆盖接管尝试的终态事实。

真实 SQLite、真实 `core.outbound` 发送链与 `claim_component_attempt` 认领语义；
只把平台边界（`alconna.UniMessage.send`）换成进程内替身，不发真实 QQ。

时序：A 认领但租约立即过期（等价于「A 挂起已超租约」）→ A 卡在平台调用 →
B 以新 owner 接管并发成功 → A 迟到返回，按自己的令牌回写 sent／unknown／failed。
四条事实必须全部保持 B 的：state、msg_ids、attempt_count、无额外平台调用。
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core import outbound as ob  # noqa: E402
from nonebot_plugin_hermes_adapter.core import run_delivery_monitor as monitor  # noqa: E402
from nonebot_plugin_hermes_adapter.core.bot_registry import BotRegistry  # noqa: E402
from nonebot_plugin_hermes_adapter.core.outbound import DeliveryContext  # noqa: E402
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker  # noqa: E402
from nonebot_plugin_hermes_adapter.core.runs_delivery_config import (  # noqa: E402
    RunsDeliveryConfig,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    prepare_terminal_batch,
)

BOT = "999"
GROUP = "g1"
SCOPE = f"onebotv11:group:{GROUP}"
TEXT = "补发的正文"
OUTPUT = '{}\n{"body":"补发的正文"}'


def land_run(tracker: RunTracker, *, run_id: str) -> None:
    """落一条「run 已终态、交付待对账」的真实台账现场（primary:0／history:0）。"""
    tracker.upsert(
        run_id=run_id, scope_key=SCOPE, adapter="onebotv11", is_private=False,
        user_id="u1", group_id=GROUP, bot_self_id=BOT, session_key="s",
        mode="reactive", expect_structured=True,
    )
    tracker.record_submission(
        idempotency_key=f"idem-{run_id}", request_body={}, target_fingerprint="x|y",
        scope_key=SCOPE, session_key="s", mode="reactive",
        expect_structured=True, bot_self_id=BOT,
    )
    tracker.bind_submission_and_register(
        idempotency_key=f"idem-{run_id}", run_id=run_id, scope_key=SCOPE,
        adapter="onebotv11", is_private=False, user_id="u1", group_id=GROUP,
        bot_self_id=BOT, session_key="s", mode="reactive", expect_structured=True,
    )
    tracker.record_expected_delivery(run_id=run_id, output=OUTPUT, expect_structured=True)
    prepare_terminal_batch(
        tracker=tracker, run_id=run_id,
        units=[{"body": TEXT, "media_urls": [], "file_paths": [], "reply_to": ""}],
    )


class PlatformStandIn:
    """平台边界替身：首个调用挂起等释放，后续调用立即成功。"""

    def __init__(self, *, first_call: str) -> None:
        self.first_call = first_call          # "ok" | "error"
        self.calls: list[str] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def send(self):
        async def fake_send(_msg_self, *, target=None, bot=None, **kwargs):
            self.calls.append(str(getattr(target, "id", "?")))
            if len(self.calls) == 1:
                self.entered.set()
                await self.release.wait()
                if self.first_call == "error":
                    raise RuntimeError("platform timeout after lease expiry")
                return SimpleNamespace(msg_ids=["ack-A"])
            return SimpleNamespace(msg_ids=["ack-B"])

        return fake_send


class AttemptTokenFenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracker = RunTracker(Path(tmp.name) / "tracker.db")
        self.addCleanup(self.tracker.close)
        self.registry = BotRegistry()
        self.registry.upsert(
            adapter="onebotv11", scope="group", scope_id=GROUP,
            bot_self_id=BOT, target=SimpleNamespace(private=False, id=GROUP), ts=0,
        )
        self.runtime = SimpleNamespace(
            run_tracker=self.tracker, bot_registry=self.registry, message_buffer=MagicMock(),
        )

    def _patches(self, send, *, cfg: RunsDeliveryConfig | None = None):
        cfg = cfg or RunsDeliveryConfig(mode="repair", grace_sec=0)
        ob_mod = monitor._ob
        stack = [
            patch.object(monitor, "_runtime", lambda: self.runtime),
            patch.object(monitor, "load_runs_delivery_config", lambda: cfg),
            patch.object(monitor, "get_bot", lambda self_id: MagicMock(self_id=self_id)),
            patch.object(monitor, "_maybe_prune_delivery_ledger", lambda t: None),
            patch.object(ob_mod, "load_misc_config", lambda: SimpleNamespace(max_length=4000)),
            patch.object(ob_mod, "load_chat_config",
                         lambda: SimpleNamespace(long_reply_forward=False)),
            patch.object(ob_mod, "get_bot_nickname", AsyncMock(return_value="fence-bot")),
            patch.object(ob_mod.alconna.UniMessage, "send", send),
        ]
        return stack

    async def _stale_attempt_taken_over(self, *, first_call: str) -> dict:
        """A 挂起超租约 → B 接管发送成功 → A 迟到返回。"""
        platform = PlatformStandIn(first_call=first_call)
        stack = self._patches(platform.send())
        for item in stack:
            item.start()
        try:
            target = SimpleNamespace(private=False, id=GROUP)
            bot = MagicMock()
            ctx_a = DeliveryContext(
                tracker=self.tracker, run_id="run-p04", component_id="primary:0",
                owner="attempt-A", lease_sec=-1,
            )
            task_a = asyncio.create_task(
                ob.send_text_with_media(
                    bot=bot, target=target, text=TEXT,
                    adapter_name="onebotv11", delivery=ctx_a,
                )
            )
            await asyncio.wait_for(platform.entered.wait(), 2)
            claim_b = self.tracker.claim_component_attempt(
                run_id="run-p04", component_id="primary:0", owner="attempt-B", lease_sec=30,
            )
            sent_b = await ob.send_text_with_media(
                bot=bot, target=target, text=TEXT, adapter_name="onebotv11",
                delivery=DeliveryContext(
                    tracker=self.tracker, run_id="run-p04", component_id="primary:0",
                    owner="attempt-B",
                ),
            )
            platform.release.set()
            try:
                await asyncio.wait_for(task_a, 2)
            except Exception:
                pass
            return {
                "claim_b": claim_b, "sent_b": sent_b, "calls": list(platform.calls),
                "component": self.tracker.get_component(
                    run_id="run-p04", component_id="primary:0"),
            }
        finally:
            for item in reversed(stack):
                item.stop()

    async def test_stale_sent_write_does_not_override_taken_over_component(self) -> None:
        """旧尝试迟到的 sent 回写不得改写接管尝试的 msg_ids。"""
        land_run(self.tracker, run_id="run-p04")
        result = await self._stale_attempt_taken_over(first_call="ok")
        component = result["component"]
        self.assertEqual(result["claim_b"].outcome, "send")
        self.assertTrue(result["sent_b"].ok)
        self.assertNotEqual(result["claim_b"].token, None)
        self.assertEqual(component["state"], "sent")
        self.assertEqual(component["msg_ids"], ["ack-B"])
        self.assertEqual(component["attempt_count"], 2)
        self.assertEqual(result["calls"], [GROUP, GROUP])

    async def test_stale_unknown_write_keeps_sent_and_causes_no_resend(self) -> None:
        """旧尝试迟到的 unknown 不得把 sent 降级，也不得诱发第三次补发。"""
        land_run(self.tracker, run_id="run-p04")
        result = await self._stale_attempt_taken_over(first_call="error")
        self.assertEqual(result["component"]["state"], "sent")
        self.assertEqual(result["component"]["msg_ids"], ["ack-B"])
        cfg = RunsDeliveryConfig(
            mode="repair", grace_sec=0, unknown_backoff_sec=0, unknown_max_attempts=3,
        )
        calls: list[str] = []

        async def counting_send(_msg_self, *, target=None, bot=None, **kwargs):
            calls.append(str(getattr(target, "id", "?")))
            return SimpleNamespace(msg_ids=["ack-C"])

        stack = self._patches(counting_send, cfg=cfg)
        for item in stack:
            item.start()
        try:
            await monitor.reconcile_run("run-p04", reason="startup")
        finally:
            for item in reversed(stack):
                item.stop()
        self.assertEqual(calls, [])
        component = self.tracker.get_component(run_id="run-p04", component_id="primary:0")
        self.assertEqual(component["state"], "sent")
        self.assertEqual(component["msg_ids"], ["ack-B"])

    async def test_stale_failed_write_is_rejected_by_token(self) -> None:
        """本地失败回写同样带令牌：旧尝试不能把已 sent 的组件改回 failed。"""
        land_run(self.tracker, run_id="run-p04")
        result = await self._stale_attempt_taken_over(first_call="ok")
        applied = self.tracker.mark_component_failed(
            run_id="run-p04", component_id="primary:0", error="whitelist rejected",
            token=result["claim_b"].token + "-stale",
        )
        self.assertFalse(applied)
        component = self.tracker.get_component(run_id="run-p04", component_id="primary:0")
        self.assertEqual(component["state"], "sent")
        self.assertEqual(component["msg_ids"], ["ack-B"])

    def test_reentrant_claim_reuses_token_and_takeover_rotates_it(self) -> None:
        """同 owner 有效租约重入拿同一令牌；租约过期后换 owner 必然换令牌。"""
        land_run(self.tracker, run_id="run-claim")
        first = self.tracker.claim_component_attempt(
            run_id="run-claim", component_id="primary:0", owner="owner-1", lease_sec=30,
        )
        reentrant = self.tracker.claim_component_attempt(
            run_id="run-claim", component_id="primary:0", owner="owner-1", lease_sec=30,
        )
        self.assertEqual(first.outcome, "send")
        self.assertTrue(first.token)
        self.assertEqual(reentrant.token, first.token)
        component = self.tracker.get_component(run_id="run-claim", component_id="primary:0")
        self.assertEqual(component["attempt_count"], 1)  # 重入不重复计一次尝试
        self.assertEqual(component["claim_token"], first.token)

        # 另一个 run：lease_sec=-1 等价于「旧尝试的租约已过期」，新 owner 接管换令牌。
        land_run(self.tracker, run_id="run-takeover")
        expired = self.tracker.claim_component_attempt(
            run_id="run-takeover", component_id="primary:0", owner="owner-1", lease_sec=-1,
        )
        taken_over = self.tracker.claim_component_attempt(
            run_id="run-takeover", component_id="primary:0", owner="owner-2", lease_sec=30,
        )
        self.assertEqual(expired.outcome, "send")
        self.assertEqual(taken_over.outcome, "send")
        self.assertNotEqual(taken_over.token, expired.token)
        component = self.tracker.get_component(run_id="run-takeover", component_id="primary:0")
        self.assertEqual(component["attempt_count"], 2)
        self.assertEqual(component["claim_token"], taken_over.token)

    def test_settled_component_claim_returns_no_token(self) -> None:
        """已 sent／他人有效租约都不产生新令牌（没有新尝试）。"""
        land_run(self.tracker, run_id="run-settled")
        self.tracker.mark_component_sent(
            run_id="run-settled", component_id="primary:0", msg_ids=["m1"],
        )
        settled = self.tracker.claim_component_attempt(
            run_id="run-settled", component_id="primary:0", owner="owner-x", lease_sec=30,
        )
        self.assertEqual((settled.outcome, settled.token), ("sent", None))

        land_run(self.tracker, run_id="run-busy")
        self.tracker.claim_component_attempt(
            run_id="run-busy", component_id="primary:0", owner="owner-a", lease_sec=30,
        )
        busy = self.tracker.claim_component_attempt(
            run_id="run-busy", component_id="primary:0", owner="owner-b", lease_sec=30,
        )
        self.assertEqual((busy.outcome, busy.token), ("busy", None))

    def test_local_settlement_without_token_keeps_legacy_semantics(self) -> None:
        """history 缓冲等本地结算没有平台尝试，令牌为空时按原语义写入。"""
        land_run(self.tracker, run_id="run-local")
        self.assertTrue(self.tracker.mark_component_sent(
            run_id="run-local", component_id="history:0", msg_ids=["h1"],
        ))
        component = self.tracker.get_component(run_id="run-local", component_id="history:0")
        self.assertEqual(component["state"], "sent")
        self.assertEqual(component["msg_ids"], ["h1"])
        self.assertIsNone(component["claim_token"])

    def test_tokenless_local_failure_cannot_overwrite_sent(self) -> None:
        """平台调用前的本地失败（白名单拒绝等）不得把已 sent 的事实改回 failed。"""
        land_run(self.tracker, run_id="run-local-fail")
        self.assertTrue(self.tracker.mark_component_sent(
            run_id="run-local-fail", component_id="primary:0", msg_ids=[],
        ))
        land_run(self.tracker, run_id="run-local-retry")
        self.assertFalse(self.tracker.mark_component_failed(
            run_id="run-local-fail", component_id="primary:0", error="whitelist rejected",
        ))
        component = self.tracker.get_component(run_id="run-local-fail", component_id="primary:0")
        self.assertEqual(component["state"], "sent")
        # 未结算的组件仍然记 failed（可修复后重试的既有语义）。
        self.assertTrue(self.tracker.mark_component_failed(
            run_id="run-local-retry", component_id="primary:0", error="whitelist rejected",
        ))
        component = self.tracker.get_component(run_id="run-local-retry", component_id="primary:0")
        self.assertEqual(component["state"], "failed")

    def test_claim_component_wrapper_still_returns_outcome_string(self) -> None:
        """旧入口（探针／测试）继续拿到 send／sent／busy 字符串。"""
        land_run(self.tracker, run_id="run-wrapper")
        self.assertEqual(
            self.tracker.claim_component(
                run_id="run-wrapper", component_id="primary:0", owner="owner-w", lease_sec=30,
            ),
            "send",
        )

    def test_old_database_gets_claim_token_column(self) -> None:
        """升级前的库自动补列：历史行 claim_token 为 NULL，认领后才有令牌。"""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = Path(tmp.name) / "old.db"
        conn = sqlite3.connect(str(db))
        conn.execute(
            "CREATE TABLE delivery_components ("
            " run_id TEXT NOT NULL, component_id TEXT NOT NULL, kind TEXT NOT NULL,"
            " payload_sha256 TEXT, state TEXT NOT NULL DEFAULT 'pending',"
            " claim_owner TEXT, claim_until REAL, attempt_count INTEGER NOT NULL DEFAULT 0,"
            " msg_ids_json TEXT, last_error TEXT, created_at REAL NOT NULL,"
            " updated_at REAL NOT NULL, PRIMARY KEY (run_id, component_id))"
        )
        conn.execute(
            "INSERT INTO delivery_components VALUES"
            " ('legacy-run','primary:0','primary',NULL,'pending',NULL,NULL,0,NULL,NULL,1.0,1.0)"
        )
        conn.commit()
        conn.close()

        tracker = RunTracker(db)
        self.addCleanup(tracker.close)
        columns = {
            row[1] for row in tracker._conn.execute("PRAGMA table_info(delivery_components)")
        }
        self.assertIn("claim_token", columns)
        legacy = tracker.get_component(run_id="legacy-run", component_id="primary:0")
        self.assertEqual(legacy["state"], "pending")
        self.assertIsNone(legacy["claim_token"])
        claim = tracker.claim_component_attempt(
            run_id="legacy-run", component_id="primary:0", owner="owner-l", lease_sec=30,
        )
        self.assertEqual(claim.outcome, "send")
        self.assertTrue(claim.token)
        component = tracker.get_component(run_id="legacy-run", component_id="primary:0")
        self.assertEqual(component["claim_token"], claim.token)


if __name__ == "__main__":
    unittest.main()
