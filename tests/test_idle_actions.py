"""统一闲置动作扫描器:clear / compress / keepalive 的离线行为测试。

扫描器本身不碰 QQ、不建 agent:外部边界只有两处,测试里全部注入假实现——
``chat``(HermesClient.chat)与 ``compress``(HermesClient.compress_session)。
其余全是真实代码:``SessionManager`` + 临时 ``SessionKeyStore`` + 可拨时钟。

覆盖闲置调度验收矩阵:
1. 优先级与每拍一个动作;
2. 每会话独立计时;
3. keepalive 循环、clear/compress 后旧 key 失效;
4. 保活不刷新用户活动、compress 后不误 clear;
5. inflight / lease 保护；``/delay`` 只暂停 clear；
6. 0 / 1 秒 / 超大秒数与非法的配置边界;
7. 保活请求形态(单空格、时间通知、无 QQ 出站);
8. 压缩轮换采纳;
9. 故障隔离;
10. 会话定时清理(clear)回归;
11. 定时清理豁免名单(配置、扫描器与 /delay、/resume)。
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401

import nonebot  # noqa: E402

# tasks/ 包内的定时任务模块用 require 声明 apscheduler;仓库该包同时有直接 import
# 的模块(background_report),必须先把它作为插件装载,否则 require 会报
# 「imported before loading」。生产由 NoneBot 启动顺序保证,离线测试在这里补上。
nonebot.load_plugin("nonebot_plugin_apscheduler")

from nonebot_plugin_hermes_adapter.core.chat_config import ChatConfig  # noqa: E402
from nonebot_plugin_hermes_adapter.core.hermes_client import ChatResult  # noqa: E402
from nonebot_plugin_hermes_adapter.core.session import SessionManager  # noqa: E402
from nonebot_plugin_hermes_adapter.core.storage.session_key_store import (  # noqa: E402
    SessionKeyStore,
)
from nonebot_plugin_hermes_adapter.tasks import idle_actions as idle  # noqa: E402

# 未归一平台:internal_id 直接带真实 adapter,测试不依赖在线 bot 解析。
ADAPTER = "qq"
GROUP_A = "900100"
GROUP_B = "900200"
GROUP_C = "900300"
GROUP_D = "900400"


class _Chat:
    """假 HermesClient.chat:记录请求,按预置返回/抛错。"""

    def __init__(self, result: ChatResult | None = None, error: Exception | None = None):
        self.calls: list[dict] = []
        self.result = result if result is not None else ChatResult(raw_text="§\n§")
        self.error = error

    async def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result


class _Compress:
    """假 HermesClient.compress_session:记录请求,按预置返回/抛错。"""

    def __init__(self, result: dict | None = None, error: Exception | None = None):
        self.calls: list[dict] = []
        self.result = result
        self.error = error

    async def __call__(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        if self.result is not None:
            return dict(self.result)
        key = kwargs["session_key"]
        return {
            "ok": True,
            "status": "compressed",
            "code": 200,
            "session_id": key,
            "rotated": False,
            "in_place": True,
            "before_messages": 20,
            "after_messages": 8,
            "message": "",
        }


class _Inflight:
    def __init__(self) -> None:
        self.keys: set[tuple[str, str]] = set()

    def is_inflight(self, key) -> bool:
        return key in self.keys


class _Leases:
    def __init__(self) -> None:
        self.counts: dict[tuple[str, str], int] = {}

    def lease_count(self, adapter: str, group_id: str) -> int:
        return self.counts.get((adapter, group_id), 0)


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = SessionKeyStore(Path(self._tmp.name) / "session_keys.db")
        self.addCleanup(self.store.close)
        self.manager = SessionManager()
        self.manager.bind_store(self.store)
        self.clock = [1_000_000.0]
        self.chat = _Chat()
        self.compress = _Compress()
        self.inflight = _Inflight()
        self.leases = _Leases()
        self.config = ChatConfig()
        self._keepalive_supported = patch.object(
            idle, "keepalive_supported", lambda: True, create=True
        )
        self._keepalive_supported.start()
        self.addCleanup(self._keepalive_supported.stop)

    # --- 会话夹具 ---------------------------------------------------------
    def open_group(self, group_id: str, at: float) -> str:
        """建会话并把用户活动时间钉在 ``at``;返回 session key。"""
        key = self.manager.get_session_key(ADAPTER, False, "", group_id)
        self.manager.mark_activity(ADAPTER, False, "", group_id, now=at)
        return key

    # --- 扫描器夹具 -------------------------------------------------------
    def scanner(self, **overrides) -> "idle.IdleActionScanner":
        kwargs = dict(
            manager=self.manager,
            config_loader=lambda: self.config,
            now=lambda: self.clock[0],
            chat=self.chat,
            compress=self.compress,
            inflight=self.inflight,
            active_sessions=self.leases,
            target_resolver=lambda adapter, is_private, group_id: SimpleNamespace(
                base_url="http://gw.test", api_key="sk-test", timeout=5
            ),
        )
        kwargs.update(overrides)
        return idle.IdleActionScanner(**kwargs)

    def sweep(self, now: float, scanner=None) -> list:
        self.clock[0] = now
        return asyncio.run((scanner or self.scanner()).sweep())

    def restart(self) -> SessionManager:
        """模拟插件重启:新 manager 绑同一个库,只靠持久化状态恢复。"""
        manager = SessionManager()
        manager.bind_store(self.store)
        return manager


class TestClearAction(_Base):
    """旧 T1 迁移:clear 仍是静默一次性重置。"""

    def test_clear_fires_after_idle_and_resets_activity(self):
        self.config = ChatConfig(session_idle_reset_sec=100)
        key = self.open_group(GROUP_A, at=0.0)

        done = self.sweep(now=200.0)

        self.assertEqual([(r.internal_id, r.action) for r in done], [(f"qq+group+{GROUP_A}", "clear")])
        new_key = self.manager.get_session_key(ADAPTER, False, "", GROUP_A)
        self.assertNotEqual(new_key, key, "clear 必须换代出新 session key")
        self.assertEqual(self.manager._generation[f"qq+group+{GROUP_A}"], 1)
        # 一次性:资格关闭后,后续 T1 周期不再重复清空同一个会话。
        self.assertEqual(self.sweep(now=200.0), [])
        self.assertEqual(self.sweep(now=50_000.0), [], "clear 成功后不再每拍重复清")
        self.assertEqual(self.manager._generation[f"qq+group+{GROUP_A}"], 1)

    def test_clear_skips_before_threshold_and_when_disabled(self):
        self.config = ChatConfig(session_idle_reset_sec=100)
        self.open_group(GROUP_A, at=0.0)
        self.assertEqual(self.sweep(now=100.0), [], "未超过阈值不清")
        self.config = ChatConfig(session_idle_reset_sec=0)
        self.assertEqual(self.sweep(now=10_000.0), [], "0 = 关闭")

    def test_delay_pauses_and_resume_rearms_timer(self):
        self.config = ChatConfig(session_idle_reset_sec=100)
        self.open_group(GROUP_A, at=0.0)
        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, True, now=0.0)
        self.assertEqual(self.sweep(now=500.0), [], "/delay 暂停中不清")
        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, False, now=500.0)
        self.assertEqual(self.sweep(now=550.0), [], "resume 后重新计时")
        self.assertEqual(
            [r.action for r in self.sweep(now=700.0)],
            ["clear"],
            "resume 满阈值后恢复清理",
        )


class TestCompressAction(_Base):
    """自动压缩:复用现有客户端,一次性,轮换必须采纳。"""

    def test_compress_fires_at_own_interval_and_adopts_rotation(self):
        self.config = ChatConfig(session_idle_reset_sec=1000, session_auto_compress_sec=500)
        key = self.open_group(GROUP_A, at=0.0)
        self.compress.result = {
            "ok": True,
            "status": "compressed",
            "code": 200,
            "session_id": "hermes-rotated-1",
            "rotated": True,
            "in_place": False,
            "before_messages": 40,
            "after_messages": 6,
            "message": "",
        }

        done = self.sweep(now=600.0)

        self.assertEqual(
            [(r.internal_id, r.action) for r in done], [(f"qq+group+{GROUP_A}", "compress")]
        )
        self.assertEqual(self.compress.calls[0]["session_key"], key, "必须针对当前会话 key")
        self.assertEqual(
            set(self.compress.calls[0]), {"tgt", "session_key"},
            "自动压缩走现有压缩入口:不是只读预览、不带 focus 参数",
        )
        self.assertEqual(
            self.manager.get_session_key(ADAPTER, False, "", GROUP_A),
            "hermes-rotated-1",
            "轮换后必须采纳新 session id",
        )
        # 一次性:同一空闲期不再压缩,新活动基准也不让旧时间立刻触发 clear。
        self.sweep(now=700.0)
        self.assertEqual(len(self.compress.calls), 1)
        self.assertEqual(
            self.manager.get_session_key(ADAPTER, False, "", GROUP_A),
            "hermes-rotated-1",
            "失败/重复扫描不得改写已采纳的会话",
        )

    def test_compress_skips_before_threshold_and_when_disabled(self):
        self.config = ChatConfig(session_idle_reset_sec=1000, session_auto_compress_sec=500)
        self.open_group(GROUP_A, at=0.0)
        self.assertEqual([r.action for r in self.sweep(now=499.0)], [], "未到阈值不压缩")
        self.config = ChatConfig(session_idle_reset_sec=0, session_auto_compress_sec=0)
        self.assertEqual([r.action for r in self.sweep(now=10_000.0)], [], "0 = 关闭")

    def test_delay_does_not_pause_compress(self):
        self.config = ChatConfig(session_idle_reset_sec=1000, session_auto_compress_sec=500)
        self.open_group(GROUP_A, at=0.0)
        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, True, now=0.0)
        self.assertEqual([r.action for r in self.sweep(now=600.0)], ["compress"], "/delay 只暂停 clear")


class TestKeepaliveAction(_Base):
    """缓存保活:runs 空格轮,循环执行,结果丢弃。"""

    def test_keepalive_loops_and_never_refreshes_user_activity(self):
        self.config = ChatConfig(session_cache_keepalive_sec=30)
        key = self.open_group(GROUP_A, at=0.0)
        scanner = self.scanner()

        self.assertEqual([r.action for r in self.sweep(now=30.0, scanner=scanner)], ["keepalive"])
        call = self.chat.calls[0]
        self.assertEqual(call["text"], " ", "正文必须严格一个普通空格")
        self.assertEqual(call["session_key"], key)
        self.assertEqual(call["mode"], "passive")
        for absent in ("decision_callback", "overwritten_reply_callback", "on_suppressed", "activity_callback"):
            self.assertNotIn(absent, call, "保活不得挂任何投递回调")
        notice = call["context_notices"]
        self.assertIn("<系统通知区域起始>", notice)
        self.assertIn(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(30.0)),
            notice,
            "通知只带当前时间",
        )
        self.assertTrue(notice.strip().endswith("<系统通知区域结束>"))
        prompt = call["system_prompt"]
        self.assertIn("工具", prompt, "必须要求不调用工具")
        self.assertIn("§", prompt)
        self.assertNotIn("bot_self_id", {k for k, v in call.items() if v}, "不登记交付台账")
        # 保活不是用户活动:活动基准完全不动。
        self.assertEqual(self.manager._last_activity["qq+group+900100"], 0.0)

        # 循环:未到下一周期不动,到点再保活;活动基准仍不动。
        self.assertEqual(self.sweep(now=59.0, scanner=scanner), [])
        self.assertEqual([r.action for r in self.sweep(now=60.0, scanner=scanner)], ["keepalive"])
        self.assertEqual(len(self.chat.calls), 2)
        self.assertEqual(self.manager._last_activity["qq+group+900100"], 0.0)

    def test_keepalive_only_on_runs_interface(self):
        self.config = ChatConfig(session_cache_keepalive_sec=30)
        self.open_group(GROUP_A, at=0.0)
        with patch.object(idle, "keepalive_supported", lambda: False):
            self.assertEqual(self.sweep(now=60.0), [])
        self.assertEqual(self.chat.calls, [], "非 runs 通道不发起保活、也不伪造成功")

    def test_keepalive_uses_real_activity_to_restart_cadence(self):
        self.config = ChatConfig(session_cache_keepalive_sec=30)
        self.open_group(GROUP_A, at=0.0)
        scanner = self.scanner()
        self.assertEqual([r.action for r in self.sweep(now=30.0, scanner=scanner)], ["keepalive"])
        # 真实用户 turn 更新活动基准后,保活周期从用户活动重新起算。
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=40.0)
        self.assertEqual(self.sweep(now=65.0, scanner=scanner), [], "用户活动后 30s 内不保活")
        self.assertEqual([r.action for r in self.sweep(now=70.0, scanner=scanner)], ["keepalive"])

    def test_module_has_no_qq_outbound_path(self):
        src = (ROOT / "tasks" / "idle_actions.py").read_text(encoding="utf-8")
        for token in ("push_message", "alconna", ".send(", "send_group", "call_api"):
            self.assertNotIn(token, src, f"闲置动作不得有 QQ 出站路径: {token}")


class TestPriorityAndPerSessionTiming(_Base):
    """固定优先级 clear > compress > keepalive;每会话独立计时、每拍一个动作。"""

    def _three_sessions(self):
        cfg = ChatConfig(
            session_idle_reset_sec=100,
            session_auto_compress_sec=50,
            session_cache_keepalive_sec=10,
        )
        self.config = cfg
        self.open_group(GROUP_A, at=0.0)  # 200s → clear(三动作同时到点)
        self.open_group(GROUP_B, at=140.0)  # 60s → compress(压缩与保活同时到点)
        self.open_group(GROUP_C, at=180.0)  # 20s → keepalive
        self.open_group(GROUP_D, at=195.0)  # 5s  → 不动

    def test_one_action_per_session_by_priority(self):
        self._three_sessions()
        done = self.sweep(now=200.0)

        actions = {r.internal_id: r.action for r in done}
        self.assertEqual(
            actions,
            {
                f"qq+group+{GROUP_A}": "clear",
                f"qq+group+{GROUP_B}": "compress",
                f"qq+group+{GROUP_C}": "keepalive",
            },
            "同一拍:clear 压过 compress/keepalive,compress 压过 keepalive,未到点不动",
        )
        self.assertEqual(len(done), 3, "每会话最多一个动作")
        self.assertEqual(len(self.compress.calls), 1)
        self.assertEqual(len(self.chat.calls), 1)

    def test_compress_wins_over_keepalive_when_clear_disabled(self):
        self.config = ChatConfig(
            session_idle_reset_sec=0,
            session_auto_compress_sec=50,
            session_cache_keepalive_sec=10,
        )
        self.open_group(GROUP_A, at=0.0)
        self.assertEqual([r.action for r in self.sweep(now=60.0)], ["compress"])


class TestLoopInvalidation(_Base):
    """clear / compress 之后旧保活循环作废,新会话等真实活动重新武装。"""

    def test_clear_invalidates_keepalive_until_real_activity(self):
        self.config = ChatConfig(session_idle_reset_sec=100, session_cache_keepalive_sec=10)
        key = self.open_group(GROUP_A, at=0.0)
        scanner = self.scanner()
        self.assertEqual([r.action for r in self.sweep(now=10.0, scanner=scanner)], ["keepalive"])
        self.assertEqual([r.action for r in self.sweep(now=20.0, scanner=scanner)], ["keepalive"])

        self.assertEqual([r.action for r in self.sweep(now=200.0, scanner=scanner)], ["clear"])
        new_key = self.manager.get_session_key(ADAPTER, False, "", GROUP_A)
        self.assertNotEqual(new_key, key)
        # 清场后没有任何真实活动:旧循环不得在新 key 上继续跑。
        self.assertEqual(self.sweep(now=300.0, scanner=scanner), [])
        self.assertEqual(len(self.chat.calls), 2, "clear 后不得再保活新会话")
        # 一次真实活动重新武装后才恢复循环。
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=310.0)
        self.assertEqual([r.action for r in self.sweep(now=320.0, scanner=scanner)], ["keepalive"])

    def test_compress_invalidates_keepalive_and_does_not_leak_to_clear(self):
        self.config = ChatConfig(
            session_idle_reset_sec=1000,
            session_auto_compress_sec=100,
            session_cache_keepalive_sec=10,
        )
        self.open_group(GROUP_A, at=0.0)
        self.compress.result = {
            "ok": True,
            "status": "compressed",
            "code": 200,
            "session_id": "hermes-rotated-2",
            "rotated": True,
            "in_place": False,
            "before_messages": 40,
            "after_messages": 6,
            "message": "",
        }
        scanner = self.scanner()
        self.assertEqual([r.action for r in self.sweep(now=10.0, scanner=scanner)], ["keepalive"])
        self.assertEqual([r.action for r in self.sweep(now=100.0, scanner=scanner)], ["compress"])

        # 压缩后:旧空闲时间不得在下一拍误判 clear,也不得复活 keepalive。
        self.assertEqual(self.sweep(now=150.0, scanner=scanner), [])
        self.assertEqual(self.sweep(now=1050.0, scanner=scanner), [], "旧计时不得触发 clear")
        self.assertEqual(len(self.chat.calls), 1, "压缩后不得自动继承/复活保活")
        # 真实活动后:保活在新的周期里恢复。
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=2000.0)
        self.assertEqual([r.action for r in self.sweep(now=2010.0, scanner=scanner)], ["keepalive"])

    def test_manual_clear_does_not_rearm_keepalive_without_activity(self):
        self.config = ChatConfig(session_cache_keepalive_sec=30)
        self.open_group(GROUP_A, at=0.0)
        scanner = self.scanner()
        self.assertEqual([r.action for r in self.sweep(now=30.0, scanner=scanner)], ["keepalive"])
        # 手动 /clear 走 clear_session(不经过扫描器),同样不得让新 key 继承旧循环。
        self.manager.clear_session(ADAPTER, False, "", GROUP_A)
        self.assertEqual(self.sweep(now=60.0, scanner=scanner), [])
        self.assertEqual(self.sweep(now=90.0, scanner=scanner), [])
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=100.0)
        self.assertEqual([r.action for r in self.sweep(now=130.0, scanner=scanner)], ["keepalive"])


class _PerKeyChat(_Chat):
    """按 session key 决定失败/成功,用于故障隔离用例。"""

    def __init__(self, fail_keys: set[str] | None = None):
        super().__init__()
        self.fail_keys = fail_keys or set()
        self.keys: list[str] = []

    async def __call__(self, **kwargs):
        self.keys.append(kwargs["session_key"])
        if kwargs["session_key"] in self.fail_keys:
            self.calls.append(kwargs)
            raise RuntimeError("boom")
        return await super().__call__(**kwargs)


class TestGuards(_Base):
    """inflight / active lease / /delay 保护。"""

    def test_inflight_blocks_actions_until_turn_finishes(self):
        self.config = ChatConfig(session_idle_reset_sec=100)
        self.open_group(GROUP_A, at=0.0)
        self.inflight.keys.add((ADAPTER, f"group:{GROUP_A}"))
        self.assertEqual(self.sweep(now=200.0), [], "turn 在跑时绝不动会话")
        self.inflight.keys.clear()
        self.assertEqual([r.action for r in self.sweep(now=200.0)], ["clear"])

    def test_inflight_blocks_keepalive(self):
        self.config = ChatConfig(session_cache_keepalive_sec=30)
        self.open_group(GROUP_A, at=0.0)
        self.inflight.keys.add((ADAPTER, f"group:{GROUP_A}"))
        self.assertEqual(self.sweep(now=60.0), [], "turn 在跑时不做保活")
        self.assertEqual(self.chat.calls, [])
        self.inflight.keys.clear()
        self.assertEqual([r.action for r in self.sweep(now=60.0)], ["keepalive"])

    def test_group_lease_blocks_actions(self):
        self.config = ChatConfig(session_idle_reset_sec=100)
        self.open_group(GROUP_A, at=0.0)
        self.leases.counts[(ADAPTER, GROUP_A)] = 1
        self.assertEqual(self.sweep(now=200.0), [], "群租约在时不动会话")
        self.leases.counts.clear()
        self.assertEqual([r.action for r in self.sweep(now=200.0)], ["clear"])

    def test_delay_does_not_pause_keepalive_and_resume_restarts_timer(self):
        self.config = ChatConfig(session_cache_keepalive_sec=30)
        self.open_group(GROUP_A, at=0.0)
        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, True, now=0.0)
        self.assertEqual([r.action for r in self.sweep(now=120.0)], ["keepalive"], "/delay 不暂停保活")
        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, False, now=120.0)
        self.assertEqual(self.sweep(now=149.0), [], "resume 后重新计时")
        self.assertEqual([r.action for r in self.sweep(now=150.0)], ["keepalive"])


class TestFailureIsolation(_Base):
    """单个会话/动作失败只影响它自己这一拍,并按固定冷却退避。"""

    def test_keepalive_exception_is_isolated_per_session(self):
        self.config = ChatConfig(session_cache_keepalive_sec=10)
        key_a = self.open_group(GROUP_A, at=0.0)
        self.open_group(GROUP_B, at=0.0)
        self.open_group(GROUP_C, at=0.0)
        chat = _PerKeyChat(fail_keys={key_a})
        scanner = self.scanner(chat=chat)

        done = self.sweep(now=20.0, scanner=scanner)

        self.assertEqual(len(done), 3, "三个会话各尝试一次")
        self.assertEqual(
            set(chat.keys),
            {key_a, f"hermes-qq+group+{GROUP_B}", f"hermes-qq+group+{GROUP_C}"},
        )
        # 成功会话的循环已推进;失败会话进入冷却,这一拍以后不再立刻重试。
        chat.keys.clear()
        self.sweep(now=30.0, scanner=scanner)
        self.assertNotIn(key_a, chat.keys, "失败会话冷却期内不重试")
        self.assertIn(f"hermes-qq+group+{GROUP_B}", chat.keys)
        # 冷却结束后可以再试。
        chat.keys.clear()
        self.sweep(now=20.0 + idle.FAIL_COOLDOWN_SEC + 1, scanner=scanner)
        self.assertIn(key_a, chat.keys)

    def test_compress_busy_is_logged_and_never_rewrites_session(self):
        self.config = ChatConfig(session_auto_compress_sec=50)
        key = self.open_group(GROUP_A, at=0.0)
        self.compress.result = {
            "ok": False,
            "code": 409,
            "status": "session_busy",
            "session_id": key,
            "rotated": False,
            "message": "",
        }
        scanner = self.scanner()

        self.assertEqual([r.action for r in self.sweep(now=60.0, scanner=scanner)], ["compress"])
        self.assertEqual(self.manager.get_session_key(ADAPTER, False, "", GROUP_A), key)
        self.assertEqual(len(self.compress.calls), 1)
        # 409 之后冷却,不每拍重撞;活动基准也不被改写。
        self.sweep(now=61.0, scanner=scanner)
        self.assertEqual(len(self.compress.calls), 1)
        self.assertEqual(self.manager._last_activity["qq+group+900100"], 0.0)

    def test_compress_exception_does_not_stop_other_sessions(self):
        self.config = ChatConfig(session_auto_compress_sec=50)
        self.open_group(GROUP_A, at=0.0)
        self.open_group(GROUP_B, at=0.0)
        calls: list[str] = []

        async def flaky(**kwargs):
            calls.append(kwargs["session_key"])
            if len(calls) == 1:
                raise RuntimeError("network down")
            return {
                "ok": True,
                "status": "compressed",
                "code": 200,
                "session_id": kwargs["session_key"],
                "rotated": False,
                "in_place": True,
                "before_messages": 3,
                "after_messages": 2,
                "message": "",
            }

        done = self.sweep(now=60.0, scanner=self.scanner(compress=flaky))

        self.assertEqual(len(done), 2, "一个会话压缩抛错不影响另一个")
        self.assertEqual(len(calls), 2)


class TestIdleStatePersistence(_Base):
    """最后活动时间与动作资格进 session_keys.db:重启原样读回,不再全量播种。"""

    def test_armed_session_restores_original_activity_time(self):
        """重启不重填开机时间:按真实闲置时长判定,不是从开机重新等。"""
        self.config = ChatConfig(session_idle_reset_sec=100)
        base = self.clock[0]
        self.open_group(GROUP_A, at=base)

        manager = self.restart()

        self.assertEqual(manager._last_activity[f"qq+group+{GROUP_A}"], base)
        self.assertTrue(manager.is_idle_armed(f"qq+group+{GROUP_A}"))
        scanner = self.scanner(manager=manager)
        self.assertEqual([r.action for r in self.sweep(now=base + 99.0, scanner=scanner)], [])
        self.assertEqual(
            [r.action for r in self.sweep(now=base + 101.0, scanner=scanner)],
            ["clear"],
            "重启后按真实闲置时长(不是开机时刻)清理",
        )

    def test_keepalive_timestamp_survives_restart(self):
        """上次保活也随最后状态恢复，重启不能提前多发一轮。"""
        self.config = ChatConfig(session_cache_keepalive_sec=30)
        base = self.clock[0]
        self.open_group(GROUP_A, at=base)
        scanner = self.scanner()
        self.assertEqual(
            [r.action for r in self.sweep(now=base + 30.0, scanner=scanner)],
            ["keepalive"],
        )
        self.assertEqual(len(self.chat.calls), 1)

        manager = self.restart()
        restarted_scanner = self.scanner(manager=manager)
        self.assertEqual(
            self.sweep(now=base + 59.0, scanner=restarted_scanner),
            [],
            "距上次保活仅 29 秒，重启后不得按用户活动时间提前再跑",
        )
        self.assertEqual(len(self.chat.calls), 1)
        self.assertEqual(
            [r.action for r in self.sweep(now=base + 60.0, scanner=restarted_scanner)],
            ["keepalive"],
        )
        self.assertEqual(len(self.chat.calls), 2)

    def test_disarmed_session_restores_and_stays_quiet(self):
        """已关闭资格的会话重启后仍不动,直到下一次真实活动。"""
        self.config = ChatConfig(
            session_idle_reset_sec=100,
            session_auto_compress_sec=5,
            session_cache_keepalive_sec=10,
        )
        base = self.clock[0]
        self.open_group(GROUP_A, at=base)
        self.assertEqual([r.action for r in self.sweep(now=base + 200.0)], ["clear"])

        manager = self.restart()

        self.assertFalse(manager.is_idle_armed(f"qq+group+{GROUP_A}"))
        self.assertEqual(
            self.sweep(now=base + 5000.0, scanner=self.scanner(manager=manager)),
            [],
            "disarmed 会话恢复后不得自动 clear / compress / keepalive",
        )
        self.assertEqual(self.chat.calls, [], "三个动作(clear/compress/keepalive)全部跳过")
        self.assertEqual(self.compress.calls, [])
        # 真实活动重新武装:资格对三个动作一起恢复(优先级仍是 compress > keepalive)。
        manager.mark_activity(ADAPTER, False, "", GROUP_A, now=base + 5001.0)
        self.assertEqual(
            [r.action for r in self.sweep(now=base + 5006.0, scanner=self.scanner(manager=manager))],
            ["compress"],
        )

    def test_bind_store_does_not_seed_known_sessions(self):
        """只有 key 映射、没有活动状态的会话不被播种(旧全量播种已废除)。"""
        base = self.clock[0]
        self.manager.get_session_key(ADAPTER, False, "", GROUP_A)

        manager = self.restart()

        self.assertEqual(manager.idle_snapshot(), [], "重启后没有可参与闲置动作的会话")
        self.assertNotIn(f"qq+group+{GROUP_A}", manager._last_activity)
        self.config = ChatConfig(
            session_idle_reset_sec=1, session_auto_compress_sec=1, session_cache_keepalive_sec=1
        )
        self.assertEqual(
            self.sweep(now=base + 1_000_000.0, scanner=self.scanner(manager=manager)), []
        )

    def test_legacy_database_migrates_to_disarmed_and_is_replayable(self):
        """存量库补三列,旧行默认无活动、资格关闭、无保活记录。"""
        import sqlite3

        path = Path(self._tmp.name) / "legacy.db"
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                "CREATE TABLE session_keys (internal_id TEXT PRIMARY KEY, session_key TEXT NOT NULL,"
                " generation INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL)"
            )
            conn.execute("CREATE TABLE idle_paused (internal_id TEXT PRIMARY KEY)")
            conn.execute(
                "INSERT INTO session_keys VALUES (?, ?, ?, ?)",
                (f"qq+group+{GROUP_A}", f"hermes-qq+group+{GROUP_A}", 0, 1.0),
            )
            conn.commit()
        finally:
            conn.close()

        store = SessionKeyStore(path)
        self.addCleanup(store.close)
        self.assertEqual(store.load_idle_states()[f"qq+group+{GROUP_A}"], (0.0, False, 0.0))
        columns = [
            row[1]
            for row in store._conn.execute("PRAGMA table_info(session_keys)").fetchall()
        ]
        self.assertEqual(
            columns,
            ["internal_id", "session_key", "generation", "updated_at", "last_activity", "idle_armed", "keepalive_last"],
        )
        # 可重放:再开一次库不报错、不清数据、值不变。
        again = SessionKeyStore(path)
        self.addCleanup(again.close)
        self.assertEqual(again.load_idle_states()[f"qq+group+{GROUP_A}"], (0.0, False, 0.0))

        manager = SessionManager()
        manager.bind_store(again)
        self.assertEqual(manager.idle_snapshot(), [], "迁移后的旧行默认不参与闲置动作")
        self.assertFalse(manager.is_idle_armed(f"qq+group+{GROUP_A}"))

    def test_domain_migration_carries_idle_state(self):
        """会话域归一必须原样携带最后活动时刻与资格,不得丢失。"""
        old_id = "onebotv11+group+900100"
        new_id = "qqtpc+group+900100"
        self.store.put(old_id, "hermes-mig", 3, now=1.0)
        self.store.set_idle_state(old_id, 123.0, True, keepalive_last=77.0)
        self.store.set_idle_paused(old_id, True)

        report = self.store.migrate_session_domain(
            {"onebotv11": "qqtpc", "milky": "qqtpc", "icqq": "qqtpc"}
        )

        self.assertEqual(report.session_keys_migrated, 1)
        self.assertTrue(report.integrity_ok)
        self.assertEqual(self.store.load_all()[new_id], ("hermes-mig", 3))
        self.assertEqual(self.store.load_idle_states()[new_id], (123.0, True, 77.0))
        self.assertNotIn(old_id, self.store.load_idle_states())
        # /delay 暂停表本来就跟着归一迁(既有语义),旧行不残留。
        self.assertEqual(self.store.load_idle_paused(), {new_id})

        # 归一后的状态能被新进程原样恢复,继续按原活动时刻计时。
        manager = SessionManager()
        manager.bind_store(self.store)
        self.assertEqual(manager._last_activity[new_id], 123.0)
        self.assertTrue(manager.is_idle_armed(new_id))
        self.assertEqual(manager.keepalive_last(new_id), 77.0)


class TestEligibilityLifecycle(_Base):
    """clear／compress／轮换成功关闭资格,失败与预览不关闭,只有真实活动重开。"""

    def _rotated(self, new_key: str) -> dict:
        return {
            "ok": True,
            "status": "compressed",
            "code": 200,
            "session_id": new_key,
            "rotated": True,
            "in_place": False,
            "before_messages": 40,
            "after_messages": 6,
            "message": "",
        }

    def test_auto_compress_success_disarms_and_survives_restart(self):
        self.config = ChatConfig(
            session_idle_reset_sec=100, session_auto_compress_sec=50, session_cache_keepalive_sec=10
        )
        base = self.clock[0]
        self.open_group(GROUP_A, at=base)
        self.compress.result = self._rotated("hermes-rotated-3")

        self.assertEqual([r.action for r in self.sweep(now=base + 60.0)], ["compress"])

        internal_id = f"qq+group+{GROUP_A}"
        self.assertFalse(self.manager.is_idle_armed(internal_id), "压缩成功必须关闭资格")
        manager = self.restart()
        self.assertFalse(manager.is_idle_armed(internal_id), "关闭状态跨重启保持")
        self.assertEqual(manager.current_session_key(internal_id), "hermes-rotated-3")
        self.assertEqual(
            self.sweep(now=base + 10_000.0, scanner=self.scanner(manager=manager)), []
        )
        self.assertEqual(len(self.chat.calls), 0, "压缩后不得保活/再压缩/误清")

    def test_auto_compress_failure_keeps_eligibility(self):
        self.config = ChatConfig(session_auto_compress_sec=50)
        base = self.clock[0]
        key = self.open_group(GROUP_A, at=base)
        self.compress.result = {
            "ok": False,
            "code": 404,
            "status": "session_not_found",
            "session_id": key,
            "rotated": False,
            "message": "",
        }

        self.assertEqual([r.action for r in self.sweep(now=base + 60.0)], ["compress"])

        internal_id = f"qq+group+{GROUP_A}"
        self.assertTrue(self.manager.is_idle_armed(internal_id), "未成功压缩不关闭资格")
        self.assertEqual(self.manager._last_activity[internal_id], base, "也不改写活动时刻")
        # 冷却结束后同一空闲期仍可再试。
        self.assertEqual(
            [r.action for r in self.sweep(now=base + 60.0 + idle.FAIL_COOLDOWN_SEC + 1)],
            ["compress"],
        )

    def test_adopt_rotation_closes_eligibility_until_real_activity(self):
        self.config = ChatConfig(session_cache_keepalive_sec=10)
        base = self.clock[0]
        key = self.open_group(GROUP_A, at=base)
        internal_id = f"qq+group+{GROUP_A}"

        self.assertTrue(self.manager.adopt_session_key(key, "hermes-upstream-1"))

        self.assertFalse(self.manager.is_idle_armed(internal_id), "上游轮换先关闭资格")
        self.assertEqual(
            self.sweep(now=base + 10_000.0, scanner=self.scanner()), [], "旧循环不得挂到新 key"
        )
        # 真实 chat 的收口顺序 adopt → mark_activity:活动把它重新打开。
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=base + 10_001.0)
        self.assertTrue(self.manager.is_idle_armed(internal_id))

    def test_keepalive_rotation_adopts_and_stops_old_loop(self):
        self.config = ChatConfig(session_cache_keepalive_sec=10)
        base = self.clock[0]
        key = self.open_group(GROUP_A, at=base)
        self.chat.result = ChatResult(raw_text="§\n§", effective_session_key="hermes-rotated-k")
        scanner = self.scanner()

        self.assertEqual([r.action for r in self.sweep(now=base + 10.0, scanner=scanner)], ["keepalive"])

        internal_id = f"qq+group+{GROUP_A}"
        self.assertEqual(self.manager.current_session_key(internal_id), "hermes-rotated-k")
        self.assertFalse(self.manager.is_idle_armed(internal_id), "保活轮换后关闭资格")
        self.assertEqual(self.sweep(now=base + 100.0, scanner=scanner), [])
        self.assertEqual(len(self.chat.calls), 1, "不得继续旧循环")
        # 下一次真实活动才重新开放。
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=base + 101.0)
        self.assertEqual([r.action for r in self.sweep(now=base + 111.0, scanner=scanner)], ["keepalive"])
        self.assertEqual(self.chat.calls[0]["session_key"], key)

    def test_context_overflow_is_not_counted_as_keepalive_success(self):
        self.config = ChatConfig(session_cache_keepalive_sec=10)
        base = self.clock[0]
        self.open_group(GROUP_A, at=base)
        self.chat.result = ChatResult(raw_text="上下文已达上限", is_context_overflow=True)
        scanner = self.scanner()

        self.assertEqual([r.action for r in self.sweep(now=base + 10.0, scanner=scanner)], ["keepalive"])

        internal_id = f"qq+group+{GROUP_A}"
        self.assertIn((internal_id, "keepalive"), scanner._fail_until, "溢出按失败进冷却")
        self.assertEqual(self.manager.keepalive_last(internal_id), 0.0, "不得当成成功推进节奏")
        self.assertTrue(self.manager.is_idle_armed(internal_id), "失败不关闭资格")
        self.assertEqual(self.sweep(now=base + 20.0, scanner=scanner), [], "冷却期内不重撞")

    def test_resume_never_arms_a_disarmed_session(self):
        self.config = ChatConfig(session_cache_keepalive_sec=10)
        base = self.clock[0]
        self.open_group(GROUP_A, at=base)
        self.manager.clear_session(ADAPTER, False, "", GROUP_A)
        internal_id = f"qq+group+{GROUP_A}"

        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, False, now=base + 50.0)

        self.assertFalse(self.manager.is_idle_armed(internal_id), "/resume 不偷偷打开资格")
        self.assertEqual(self.sweep(now=base + 500.0), [])
        self.assertEqual(self.chat.calls, [])
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=base + 501.0)
        self.assertEqual([r.action for r in self.sweep(now=base + 511.0)], ["keepalive"])

    def test_manual_clear_before_first_sweep_never_keepalives(self):
        """重启前最后一次手动 /clear:恢复后不得因旧活动时刻开始保活。"""
        self.config = ChatConfig(session_cache_keepalive_sec=10)
        base = self.clock[0]
        self.open_group(GROUP_A, at=base)
        self.manager.clear_session(ADAPTER, False, "", GROUP_A)

        manager = self.restart()

        self.assertEqual(self.sweep(now=base + 1000.0, scanner=self.scanner(manager=manager)), [])
        self.assertEqual(self.chat.calls, [], "手动 clear 后无真实活动,不得保活")

    def test_stale_keepalive_completion_cannot_write_new_generation(self):
        self.config = ChatConfig(session_cache_keepalive_sec=10)
        base = self.clock[0]
        old_key = self.open_group(GROUP_A, at=base)
        internal_id = f"qq+group+{GROUP_A}"
        self.manager.clear_session(ADAPTER, False, "", GROUP_A)

        self.assertFalse(
            self.manager.mark_keepalive(internal_id, old_key, now=base + 10.0),
            "旧 key 的迟到保活结果不得落进新代",
        )
        manager = self.restart()
        self.assertEqual(manager.keepalive_last(internal_id), 0.0)
        self.assertFalse(manager.is_idle_armed(internal_id))


class TestConfigBoundaries(unittest.TestCase):
    """0 / 1 秒 / 超大秒数都按定案处理;非法值按现有配置规范回默认。"""

    def test_parse_accepts_one_second_and_huge_values(self):
        from nonebot_plugin_hermes_adapter.core.chat_config import parse_chat_config

        for key in ("session_auto_compress_sec", "session_cache_keepalive_sec"):
            for value in (1, 2, 10**9):
                cfg = parse_chat_config({key: value})
                self.assertEqual(getattr(cfg, key), value, f"{key} 必须原样接受 {value}")
            self.assertEqual(getattr(parse_chat_config({key: 0}), key), 0, "0 = 关闭")
            self.assertEqual(
                getattr(parse_chat_config({key: -5}), key), 0, "负值回默认(0),不静默夹紧"
            )
            self.assertEqual(getattr(parse_chat_config({key: True}), key), 0, "布尔回默认")
            self.assertEqual(getattr(parse_chat_config({key: "30"}), key), 0, "字符串回默认")

    def test_idle_actions_enabled_any_action(self):
        from nonebot_plugin_hermes_adapter.core.chat_config import idle_actions_enabled

        self.assertFalse(idle_actions_enabled(ChatConfig()))
        self.assertTrue(idle_actions_enabled(ChatConfig(session_idle_reset_sec=1)))
        self.assertTrue(idle_actions_enabled(ChatConfig(session_auto_compress_sec=1)))
        self.assertTrue(idle_actions_enabled(ChatConfig(session_cache_keepalive_sec=1)))


class TestOneSecondAndHugeBehaviour(_Base):
    def test_one_second_keepalive_fires_after_one_second(self):
        self.config = ChatConfig(session_cache_keepalive_sec=1)
        self.open_group(GROUP_A, at=0.0)
        self.assertEqual([r.action for r in self.sweep(now=1.0)], ["keepalive"])

    def test_one_second_compress_fires_after_one_second(self):
        self.config = ChatConfig(session_auto_compress_sec=1)
        self.open_group(GROUP_A, at=0.0)
        self.assertEqual([r.action for r in self.sweep(now=1.0)], ["compress"])

    def test_huge_interval_never_fires_in_practice(self):
        self.config = ChatConfig(session_cache_keepalive_sec=10**9)
        self.open_group(GROUP_A, at=0.0)
        self.assertEqual(self.sweep(now=10**6), [])


class TestCommandGating(_Base):
    """任一闲置动作启用就显示/接受 /delay、/resume。"""

    def test_help_lists_delay_when_only_keepalive_enabled(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        with patch.object(
            commands, "idle_actions_enabled", lambda: True
        ):
            text = commands.build_help_text(True, False)
        self.assertIn("/delay", text)
        self.assertIn("/resume", text)
        with patch.object(commands, "idle_actions_enabled", lambda: False):
            text = commands.build_help_text(True, False)
        self.assertNotIn("/delay", text)

    def test_delay_command_pauses_when_only_keepalive_enabled(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        replies: list[str] = []

        class _Message:
            def __init__(self, text):
                self.text = text

            async def send(self, **_kwargs):
                replies.append(str(self.text))

        target = SimpleNamespace(private=True, id="10001")
        event = SimpleNamespace(get_user_id=lambda: "10001")
        matcher = SimpleNamespace(skip=lambda: None)
        with patch.object(commands.alconna, "get_target", lambda: target), patch.object(
            commands, "check_isolation", lambda _e, _t: True
        ), patch.object(commands, "get_adapter_name", lambda _t: ADAPTER), patch.object(
            commands, "idle_actions_enabled", lambda: True
        ), patch.object(
            commands, "session_manager", self.manager
        ), patch.object(
            commands.alconna, "UniMessage", _Message
        ):
            asyncio.run(commands.handle_delay(bot=None, event=event, matcher=matcher))

        self.assertTrue(replies, "启用任一动作为 /delay 必须给出回执")
        self.assertIn("会话定时清理", replies[0])
        self.assertNotIn("空闲自动动作", replies[0])
        self.assertIn(f"qq+private+10001", self.manager._idle_paused)

    def test_delay_command_refuses_when_nothing_enabled(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        replies: list[str] = []

        class _Message:
            def __init__(self, text):
                self.text = text

            async def send(self, **_kwargs):
                replies.append(str(self.text))

        target = SimpleNamespace(private=True, id="10001")
        event = SimpleNamespace(get_user_id=lambda: "10001")
        matcher = SimpleNamespace(skip=lambda: None)
        with patch.object(commands.alconna, "get_target", lambda: target), patch.object(
            commands, "check_isolation", lambda _e, _t: True
        ), patch.object(commands, "get_adapter_name", lambda _t: ADAPTER), patch.object(
            commands, "idle_actions_enabled", lambda: False
        ), patch.object(
            commands, "session_manager", self.manager
        ), patch.object(
            commands.alconna, "UniMessage", _Message
        ):
            asyncio.run(commands.handle_delay(bot=None, event=event, matcher=matcher))

        self.assertIn("未启用", replies[0])
        self.assertNotIn("空闲自动动作", replies[0])
        self.assertEqual(self.manager._idle_paused, set())


class TestIdleResetExemptConfig(unittest.TestCase):
    """session_idle_reset_exempt:字符串集合解析,私聊/群聊精确匹配。"""

    def test_parse_defaults_empty_and_reads_both_scopes(self):
        from nonebot_plugin_hermes_adapter.core.chat_config import parse_chat_config

        self.assertEqual(ChatConfig().session_idle_reset_exempt, set())
        self.assertEqual(parse_chat_config({}).session_idle_reset_exempt, set())
        cfg = parse_chat_config(
            {"session_idle_reset_exempt": ["private:10001", "group:900100"]}
        )
        self.assertEqual(
            cfg.session_idle_reset_exempt, {"private:10001", "group:900100"}
        )

    def test_private_and_group_are_matched_exactly(self):
        cfg = ChatConfig(session_idle_reset_exempt={"private:10001", "group:900100"})

        self.assertTrue(
            cfg.idle_reset_exempt(is_private=True, user_id="10001", group_id=None)
        )
        self.assertTrue(
            cfg.idle_reset_exempt(is_private=False, user_id="", group_id="900100")
        )
        # 同号不同域不得互相豁免。
        self.assertFalse(
            cfg.idle_reset_exempt(is_private=False, user_id="", group_id="10001")
        )
        self.assertFalse(
            cfg.idle_reset_exempt(is_private=True, user_id="900100", group_id=None)
        )
        self.assertFalse(
            cfg.idle_reset_exempt(is_private=False, user_id="", group_id=None),
            "没有群号时不得命中 group:None",
        )

    def test_bad_value_falls_back_to_empty_set(self):
        from nonebot_plugin_hermes_adapter.core.chat_config import parse_chat_config

        self.assertEqual(
            parse_chat_config(
                {"session_idle_reset_exempt": "private:10001"}
            ).session_idle_reset_exempt,
            set(),
            "标量不是字符串集合,回默认空名单",
        )


class TestIdleResetExemptionScan(_Base):
    """豁免只作用于 clear;compress 与 keepalive 完全不看名单。"""

    def test_exempt_group_skips_clear_but_compress_still_fires(self):
        self.config = ChatConfig(
            session_idle_reset_sec=100,
            session_auto_compress_sec=500,
            session_idle_reset_exempt={"group:900100"},
        )
        key = self.open_group(GROUP_A, at=0.0)

        self.assertEqual(self.sweep(now=200.0), [], "豁免会话跳过定时清理")
        self.assertEqual(
            self.manager.get_session_key(ADAPTER, False, "", GROUP_A),
            key,
            "豁免会话会话代际不得变化",
        )
        self.assertEqual(
            [r.action for r in self.sweep(now=600.0)],
            ["compress"],
            "压缩到点仍执行,不受豁免影响",
        )

    def test_exempt_group_keepalive_still_fires(self):
        self.config = ChatConfig(
            session_idle_reset_sec=100,
            session_cache_keepalive_sec=30,
            session_idle_reset_exempt={"group:900100"},
        )
        self.open_group(GROUP_A, at=0.0)

        self.assertEqual(
            [r.action for r in self.sweep(now=200.0)],
            ["keepalive"],
            "保活到点仍执行,豁免只挡 clear",
        )

    def test_private_exemption_does_not_leak_to_same_numbered_group(self):
        self.config = ChatConfig(
            session_idle_reset_sec=100, session_idle_reset_exempt={"private:900100"}
        )
        self.manager.get_session_key(ADAPTER, True, "900100", None)
        self.manager.mark_activity(ADAPTER, True, "900100", None, now=0.0)
        self.open_group(GROUP_A, at=0.0)  # 同一个号码的群聊

        done = self.sweep(now=200.0)

        self.assertEqual(
            [(r.internal_id, r.action) for r in done],
            [(f"qq+group+{GROUP_A}", "clear")],
            "私聊豁免不得落到同号群聊上",
        )


class TestExemptCommands(_Base):
    """豁免期间 /delay、/resume 只提示,不改暂停状态与活动计时。"""

    SHIELD = "🛡️ 本会话已豁免定时清理，原状态保持不变"

    def _run(self, handler, *, private: bool, target_id: str, user_id: str) -> list[str]:
        from nonebot_plugin_hermes_adapter.handlers import commands

        replies: list[str] = []

        class _Message:
            def __init__(self, text):
                self.text = text

            async def send(self, **_kwargs):
                replies.append(str(self.text))

        target = SimpleNamespace(private=private, id=target_id)
        event = SimpleNamespace(get_user_id=lambda: user_id)
        matcher = SimpleNamespace(skip=lambda: None)
        with patch.object(commands.alconna, "get_target", lambda: target), patch.object(
            commands, "check_isolation", lambda _e, _t: True
        ), patch.object(commands, "get_adapter_name", lambda _t: ADAPTER), patch.object(
            commands, "idle_actions_enabled", lambda: True
        ), patch.object(
            commands, "load_chat_config", lambda: self.config
        ), patch.object(
            commands, "session_manager", self.manager
        ), patch.object(
            commands.alconna, "UniMessage", _Message
        ):
            asyncio.run(handler(bot=None, event=event, matcher=matcher))
        return replies

    def test_delay_on_exempt_session_keeps_unpaused_and_activity(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        self.config = ChatConfig(
            session_idle_reset_sec=100, session_idle_reset_exempt={"private:10001"}
        )
        self.manager.mark_activity(ADAPTER, True, "10001", None, now=1000.0)
        internal_id = "qq+private+10001"

        replies = self._run(
            commands.handle_delay, private=True, target_id="10001", user_id="10001"
        )

        self.assertEqual(replies, [self.SHIELD], "豁免期间 /delay 只提示")
        self.assertFalse(self.manager.is_idle_paused(internal_id), "不得写入暂停")
        self.assertEqual(
            self.manager._last_activity[internal_id], 1000.0, "不得重置活动计时"
        )

    def test_resume_on_exempt_session_keeps_paused_and_activity(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        self.config = ChatConfig(
            session_idle_reset_sec=100, session_idle_reset_exempt={"group:900100"}
        )
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=1000.0)
        internal_id = f"qq+group+{GROUP_A}"
        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, True, now=1000.0)

        replies = self._run(
            commands.handle_resume, private=False, target_id=GROUP_A, user_id="10002"
        )

        self.assertEqual(replies, [self.SHIELD], "豁免期间 /resume 只提示")
        self.assertTrue(self.manager.is_idle_paused(internal_id), "不得清除暂停")
        self.assertEqual(
            self.manager._last_activity[internal_id], 1000.0, "不得重置活动计时"
        )

    def test_unpaused_state_survives_after_removal(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        self.config = ChatConfig(
            session_idle_reset_sec=100, session_idle_reset_exempt={"private:10001"}
        )
        self.manager.get_session_key(ADAPTER, True, "10001", None)
        self.manager.mark_activity(ADAPTER, True, "10001", None, now=1000.0)
        self._run(commands.handle_delay, private=True, target_id="10001", user_id="10001")
        self.assertFalse(self.manager.is_idle_paused("qq+private+10001"))

        # 通过 reload-config 换快照移出名单,不触碰 idle_paused。
        self.config = ChatConfig(session_idle_reset_sec=100)

        self.assertEqual(
            [r.action for r in self.sweep(now=1200.0)],
            ["clear"],
            "移出名单后,豁免期间未暂停的会话恢复定时清理",
        )

    def test_paused_state_survives_after_removal(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        self.config = ChatConfig(
            session_idle_reset_sec=100, session_idle_reset_exempt={"group:900100"}
        )
        self.manager.mark_activity(ADAPTER, False, "", GROUP_A, now=1000.0)
        self.manager.set_idle_paused(ADAPTER, False, "", GROUP_A, True, now=1000.0)
        self._run(commands.handle_resume, private=False, target_id=GROUP_A, user_id="10002")
        self.assertTrue(self.manager.is_idle_paused(f"qq+group+{GROUP_A}"))

        self.config = ChatConfig(session_idle_reset_sec=100)

        self.assertEqual(
            self.sweep(now=1200.0),
            [],
            "移出名单后,豁免期间仍暂停的会话保持暂停",
        )


class TestHelpMenuWording(unittest.TestCase):
    """菜单全角化,且用户可见文字不再出现内部代号。"""

    def test_menu_fullwidth_and_session_cleanup_wording(self):
        from nonebot_plugin_hermes_adapter.handlers import commands

        with patch.object(commands, "idle_actions_enabled", lambda: True), patch.object(
            commands, "hermes_control_plane_enabled", lambda: True
        ):
            text = commands.build_help_text(True, True)

        self.assertNotIn(" - ", text, "命令与描述之间用全角冒号")
        self.assertNotIn("T1", text, "用户可见文字不得出现内部代号")
        self.assertNotIn("空闲自动动作", text)
        self.assertIn("暂停会话定时清理", text)
        self.assertIn("恢复会话定时清理", text)
        self.assertIn("：重置对话", text)
        self.assertIn("（管理员）", text)
        self.assertNotIn("(管理员)", text)
        self.assertIn("/compress：手动压缩当前会话上下文（管理员）", text)
        self.assertIn("/compress preview：只读查看会话上下文占用（管理员）", text)
        self.assertIn("查看／切换", text)
        # 旧半角标点与旧开关均不得出现在菜单里。
        self.assertNotIn("--preview", text)
        self.assertNotIn("上下文；--preview", text)
        # 命令名与斜杠保持半角原样。
        self.assertIn("/delay", text)
        self.assertIn("/resume", text)
        self.assertIn("/reasoning", text)
