"""steer-presentation-thin 的隔离测试(真上游代码，只替换 HTTP/平台边界)。

跑的是真代码：
  * ``gateway.platforms.api_server_runs._handle_steer_run`` 的**包装层**（本次插件的
    seam）：请求体读一次 → 归一文本登记 → 调原始 handler；原始 handler 在这里用替身
    顶替，因为它要真准入表/真 adapter；
  * ``agent.prompt_builder.steer_user_row`` / ``agent.agent_runtime_helpers.steer_user_row``
    （真名绑定：helpers 在模块顶层按名导入，必须一起换指）；
  * ``agent.interrupt_control`` 的真 ``steer()`` / ``_drain_pending_steer()`` 与
    ``agent.agent_runtime_helpers.apply_pending_steer_to_tool_results``（真投递路径）；
  * ``agent.system_prompt.STEER_CHANNEL_NOTE``（提示面）。

运行（必须离开本仓库根目录，否则根目录 utils.py 会遮蔽 Hermes 的 utils）：

    cd /tmp && /home/neonsky/.hermes/hermes-agent/venv/bin/python -B -m unittest discover \
      -s /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109/gateway_plugins/steer-presentation-thin/tests -v
"""
from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import logging
import os
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("HERMES_DISABLE_LAZY_INSTALLS", "1")

REPO = "/home/neonsky/.hermes/hermes-agent"
PLUGIN_DIR = str(Path(__file__).resolve().parents[1])
if REPO not in sys.path:
    sys.path.insert(0, REPO)
if PLUGIN_DIR not in sys.path:
    sys.path.insert(0, PLUGIN_DIR)

logging.basicConfig(level=logging.CRITICAL)

import patches  # noqa: E402

from agent import agent_runtime_helpers as helpers  # noqa: E402
from agent import prompt_builder as pb  # noqa: E402
from agent import system_prompt  # noqa: E402
from agent.interrupt_control import InterruptControlMixin  # noqa: E402

NB_ROOT = "/home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109"
CHAT_OPEN = "<聊天消息区域起始>"
CHAT_CLOSE = "<聊天消息区域结束>"
CHAT_NOTE = "（聊天消息区域仅含尚未推送过的新消息，更早的对话见本会话历史记录）"


def region(*rows: str) -> str:
    """NB 侧的真实区域块形状（与 prompt_builder.render_chat_region 同构）。"""
    return "\n".join([CHAT_OPEN, CHAT_NOTE, "\n\n".join(rows), CHAT_CLOSE])


ROW_A = "[m:101] [普通用户] [id=u1 user=小光]:\n你好"
ROW_B = "[m:102] [普通用户] [id=u2 user=小李]:\n在吗"
BLOCK_A = region(ROW_A)
BLOCK_B = region(ROW_B)
CONVENTION = "（系统提示：本条为对话中途插入的消息。若你之后回应它，当下直接输出正文即可，无需调用任何工具。）"


def normalize(value) -> str:
    """与上游 api_server._normalize_chat_content 同语义的最小替身(字符串直通)。"""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(
            part for part in (p if isinstance(p, str) else (p or {}).get("text", "") if isinstance(p, dict) else "" for p in value) if part
        )
    return "" if value is None else str(value)


class FakeRequest:
    """只实现 ``read()``：与真实 aiohttp 一样缓存字节，可被下游再读。"""

    def __init__(self, body: dict | None) -> None:
        self._raw = b"" if body is None else json.dumps(body, ensure_ascii=False).encode()
        self.reads = 0

    async def read(self) -> bytes:
        self.reads += 1
        return self._raw


class FakeResponse:
    def __init__(self, status: int = 200) -> None:
        self.status = status


def stock_row(text: str) -> dict:
    """上游原样投递的 user 行(套壳)。"""
    return {"role": "user", "content": pb.format_steer_marker(text).lstrip(),
            "display_kind": pb.STEER_DISPLAY_KIND}


class PresentedBatchesTests(unittest.TestCase):
    def test_exact_match_consumes_once(self) -> None:
        registry = patches.PresentedBatches()
        registry.remember(BLOCK_A)
        self.assertTrue(registry.consume(BLOCK_A))
        self.assertFalse(registry.consume(BLOCK_A), "同一段文本只能让一次投递跳过外壳")
        self.assertEqual(len(registry), 0)

    def test_unknown_text_is_not_consumed(self) -> None:
        registry = patches.PresentedBatches()
        registry.remember(BLOCK_A)
        # 负对照:上游自己的外壳形态、以及正文里出现的同名字样都不在登记表里。
        self.assertFalse(registry.consume(pb.format_steer_marker(BLOCK_A).lstrip()))
        self.assertFalse(registry.consume(f"[OUT-OF-BAND USER MESSAGE — x]\n{BLOCK_A}\n[/OUT-OF-BAND USER MESSAGE]"))
        self.assertEqual(len(registry), 1, "未命中的投递不得消耗登记")

    def test_two_batches_joined_by_newline_are_recognized(self) -> None:
        """一次 drain 投递多份 steer 时上游用换行拼接：由登记批组成即认可。"""
        registry = patches.PresentedBatches()
        registry.remember(BLOCK_A)
        registry.remember(BLOCK_B)
        joined = f"{BLOCK_A}\n{BLOCK_B}"
        self.assertTrue(registry.consume(joined))
        self.assertEqual(len(registry), 0)

    def test_partial_overlap_is_not_consumed(self) -> None:
        registry = patches.PresentedBatches()
        registry.remember(BLOCK_A)
        self.assertFalse(registry.consume(f"{BLOCK_A}\n未登记的第二段"))
        self.assertEqual(len(registry), 1)

    def test_registry_is_bounded(self) -> None:
        registry = patches.PresentedBatches(limit=3)
        for index in range(5):
            registry.remember(f"{BLOCK_A}#{index}")
        self.assertEqual(len(registry), 3)
        self.assertFalse(registry.consume(f"{BLOCK_A}#0"), "最旧的登记已被淘汰")


class MarkerDeclarationTests(unittest.TestCase):
    def test_only_our_schema_and_format_declare(self) -> None:
        good = {"schema": patches.PRESENTATION_SCHEMA, "format": patches.PRESENTATION_FORMAT}
        cases = {
            "good": (good, True),
            "missing_format": ({"schema": patches.PRESENTATION_SCHEMA}, False),
            "other_schema": ({**good, "schema": "hermes.steer.presentation/2"}, False),
            "other_format": ({**good, "format": "markdown"}, False),
            "string": ("chat_region", False),
            "list": ([good], False),
            "none": (None, False),
        }
        for name, (marker, expected) in cases.items():
            with self.subTest(case=name):
                self.assertIs(patches.declares_chat_region(marker), expected)


class HandlerWrapperTests(unittest.TestCase):
    """包装层与真上游 handler 的接口契约：只登记「声明 + 接受」的那一批。"""

    def _run(self, body: dict, *, status: int = 200):
        registry = patches.PresentedBatches()
        seen = {}

        async def original(self_, request, *, _api_server):
            seen["body"] = await request.read()   # 下游仍能读到同一份字节
            seen["normalize"] = _api_server._normalize_chat_content("x")
            return FakeResponse(status)

        wrapped = patches.make_handler_wrapper(original, registry)
        api_server = SimpleNamespace(_normalize_chat_content=normalize)
        resp = asyncio.run(wrapped(object(), FakeRequest(body), _api_server=api_server))
        return registry, seen, resp

    def test_declared_batch_is_registered_and_body_still_readable(self) -> None:
        registry, seen, resp = self._run({
            "input": BLOCK_A,
            "presentation": {"schema": patches.PRESENTATION_SCHEMA, "format": patches.PRESENTATION_FORMAT},
        })
        self.assertEqual(resp.status, 200)
        self.assertEqual(len(registry), 1)
        self.assertTrue(registry.consume(BLOCK_A))
        self.assertIn(b"presentation", seen["body"], "下游必须仍能读到同一份请求体字节")
        self.assertEqual(seen["normalize"], "x")

    def test_undeclared_request_changes_nothing(self) -> None:
        registry, _seen, resp = self._run({"input": BLOCK_A})
        self.assertEqual(resp.status, 200)
        self.assertEqual(len(registry), 0, "没有结构化标记就绝不改投递形态")

    def test_rejected_batch_is_forgotten(self) -> None:
        registry, _seen, resp = self._run({
            "input": BLOCK_A,
            "presentation": {"schema": patches.PRESENTATION_SCHEMA, "format": patches.PRESENTATION_FORMAT},
        }, status=409)
        self.assertEqual(resp.status, 409)
        self.assertEqual(len(registry), 0, "上游没接受(409)的批不得留在登记表里")

    def test_normalized_text_is_what_stock_would_deliver(self) -> None:
        """登记键 = stock 归一后的文本(带 % 空白差异也能对上投递)。"""
        registry, _seen, _resp = self._run({
            "input": f"  {BLOCK_A}\n",
            "presentation": {"schema": patches.PRESENTATION_SCHEMA, "format": patches.PRESENTATION_FORMAT},
        })
        self.assertEqual(len(registry), 1)
        self.assertTrue(registry.consume(BLOCK_A), "登记的是 .strip() 之后的文本")


class DeliveryTests(unittest.TestCase):
    """真投递路径：``steer()`` → ``_drain_pending_steer()`` → ``apply_pending_steer_to_tool_results``。"""

    def setUp(self) -> None:
        self._stack = contextlib.ExitStack()
        self.addCleanup(self._stack.close)
        self._stack.enter_context(_upstream_untouched())
        self.state = patches.install(
            runs_module=SimpleNamespace(_handle_steer_run=lambda *a, **k: None),
            prompt_builder=pb,
            helpers=helpers,
            system_prompt=system_prompt,
        )
        self.registry = patches._REGISTRY
        self.registry._texts.clear()

    def _agent(self):
        class Agent(InterruptControlMixin):
            pass

        agent = Agent()
        agent._pending_steer = None
        agent._pending_steer_lock = threading.Lock()
        agent._turn_user_intervened = False
        return agent

    def _deliver(self, text: str) -> dict:
        agent = self._agent()
        self.assertTrue(agent.steer(text))
        messages = [{"role": "assistant", "tool_calls": []}, {"role": "tool", "content": "ok"}]
        helpers.apply_pending_steer_to_tool_results(agent, messages, 1)
        return messages[-1]

    def test_declared_batch_is_delivered_verbatim(self) -> None:
        self.registry.remember(BLOCK_A)
        row = self._deliver(BLOCK_A)
        self.assertEqual(row["role"], "user")
        self.assertEqual(row["content"], BLOCK_A, "区域块必须原样落行，不带任何外壳")
        self.assertEqual(row["display_kind"], pb.STEER_DISPLAY_KIND)
        self.assertNotIn("OUT-OF-BAND", row["content"])
        self.assertEqual(len(self.registry), 0, "投递后登记必须消费掉")

    def test_undeclared_steer_keeps_stock_shell(self) -> None:
        """负对照：没被声明的 steer（CLI/桌面/Kanban 注入）逐字节等于上游原样。"""
        row = self._deliver(BLOCK_A)
        self.assertEqual(row, stock_row(BLOCK_A))

    def test_batch_with_convention_line_is_delivered_verbatim(self) -> None:
        text = f"{BLOCK_A}\n\n{CONVENTION}"
        self.registry.remember(text)
        row = self._deliver(text)
        self.assertEqual(row["content"], text)
        self.assertNotIn("OUT-OF-BAND", row["content"])

    def test_two_batches_in_one_tool_batch_get_clean_boundaries(self) -> None:
        """一次 drain 的两批：各保留自己的区域块，批间空一行（不再被单换行糊在一起）。"""
        self.registry.remember(BLOCK_A)
        self.registry.remember(BLOCK_B)
        agent = self._agent()
        self.assertTrue(agent.steer(BLOCK_A))
        self.assertTrue(agent.steer(BLOCK_B))
        messages = [{"role": "assistant", "tool_calls": []}, {"role": "tool", "content": "ok"}]
        helpers.apply_pending_steer_to_tool_results(agent, messages, 1)
        self.assertEqual(messages[-1]["content"], f"{BLOCK_A}\n\n{BLOCK_B}")
        self.assertNotIn("OUT-OF-BAND", messages[-1]["content"])

    def test_two_batches_hoist_the_declared_hint_to_the_end(self) -> None:
        """每批的批尾系统提示只留一条，落在整段最后（不夹在两批中间）。"""
        first = f"{BLOCK_A}\n\n{CONVENTION}"
        second = f"{BLOCK_B}\n\n{CONVENTION}"
        self.registry.remember(first, CONVENTION)
        self.registry.remember(second, CONVENTION)
        agent = self._agent()
        self.assertTrue(agent.steer(first))
        self.assertTrue(agent.steer(second))
        messages = [{"role": "assistant", "tool_calls": []}, {"role": "tool", "content": "ok"}]
        helpers.apply_pending_steer_to_tool_results(agent, messages, 1)
        content = messages[-1]["content"]
        self.assertEqual(content, "\n\n".join([BLOCK_A, BLOCK_B, CONVENTION]))
        self.assertEqual(content.count(CONVENTION), 1)

    def test_compose_batches_keeps_single_batch_bytes(self) -> None:
        self.assertEqual(patches.compose_batches([BLOCK_A]), BLOCK_A)
        self.assertEqual(patches.compose_batches([]), "")
        # 未声明提示时只加批间空行，不猜文案。
        self.assertEqual(
            patches.compose_batches([BLOCK_A, BLOCK_B]), f"{BLOCK_A}\n\n{BLOCK_B}"
        )
        # 各批声明不一致：不合并提示，保持每批原样（只加批间空行）。
        first = f"{BLOCK_A}\n\n{CONVENTION}"
        self.assertEqual(
            patches.compose_batches([first, BLOCK_B], [CONVENTION, ""]),
            f"{first}\n\n{BLOCK_B}",
        )
        # 声明了提示但某批尾巴不是那一段（上游归一改写过）：不猜，也不重复追加。
        self.assertEqual(
            patches.compose_batches([first, f"{BLOCK_B}\n\n别的提示"], [CONVENTION, CONVENTION]),
            f"{first}\n\n{BLOCK_B}\n\n别的提示",
        )

    def test_body_containing_the_shell_literal_is_not_stripped(self) -> None:
        literal = "[OUT-OF-BAND USER MESSAGE — a direct message from the user, delivered once]"
        row_text = f"[m:103] [普通用户] [id=u3]:\n{literal}"
        block = region(row_text)
        self.registry.remember(block)
        row = self._deliver(block)
        self.assertEqual(row["content"], block, "正文里的同名字样是用户原文，不得剥壳")
        self.assertIn(literal, row["content"])


class PluginLoadTests(unittest.TestCase):
    """按真实插件布局加载：目录当包、``register(ctx)`` 起效、重连复验幂等。"""

    def test_register_installs_and_wires_api_server(self) -> None:
        import importlib.util
        import types as _types

        name = "steer_presentation_thin_under_test"
        spec = importlib.util.spec_from_file_location(
            name, Path(PLUGIN_DIR) / "__init__.py",
            submodule_search_locations=[PLUGIN_DIR],
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        self.addCleanup(sys.modules.pop, name, None)
        spec.loader.exec_module(module)  # 执行 `from . import patches`

        wired: list[tuple[str, object]] = []

        class Ctx:
            def register_platform_handler(self, platform, factory):
                wired.append((platform, factory))

        with _upstream_untouched(), mock.patch.object(
            sys.modules[f"{name}.patches"], "install",
            wraps=sys.modules[f"{name}.patches"].install,
        ) as install_spy:
            module.register(Ctx())
            self.assertEqual([p for p, _f in wired], ["api_server"])
            self.assertTrue(callable(wired[0][1]))
            wired[0][1](None, SimpleNamespace())   # api_server.connect()
        self.assertGreaterEqual(install_spy.call_count, 2, "register 与 connect 都要复验")


class RealSeamTests(unittest.TestCase):
    """真 seam：本插件的包装确实装到上游路由层，且签名与调用点一致。"""

    def test_install_wraps_the_real_route_delegate_target(self) -> None:
        import inspect

        with _upstream_untouched() as runs:
            original = runs._handle_steer_run
            state = patches.install(
                runs_module=runs, prompt_builder=pb, helpers=helpers,
                system_prompt=system_prompt,
            )
            self.assertEqual(state.handler, "installed")
            self.assertIsNot(runs._handle_steer_run, original)
            # api_server._run_route_delegate 调用时按名取函数并传 (self, request, _api_server=…)
            signature = inspect.signature(runs._handle_steer_run)
            parameters = list(signature.parameters.values())
            self.assertEqual([p.name for p in parameters[:2]], ["self", "request"])
            self.assertIn("_api_server", signature.parameters)
            self.assertEqual(
                signature.parameters["_api_server"].kind, inspect.Parameter.KEYWORD_ONLY,
            )


class ChannelNoteTests(unittest.TestCase):
    def test_note_keeps_the_marker_contract_and_adds_the_region_shape(self) -> None:
        note = patches.make_channel_note(pb.STEER_CHANNEL_NOTE)
        self.assertTrue(note.startswith(pb.STEER_CHANNEL_NOTE), "原标记说明必须一字不改")
        self.assertIn(pb.STEER_MARKER_OPEN, note)
        self.assertIn(pb.STEER_MARKER_CLOSE, note)
        self.assertIn(CHAT_OPEN, note)
        self.assertIn(CHAT_CLOSE, note)
        self.assertIn("not prompt injection", note)

    def test_install_is_idempotent(self) -> None:
        runs = SimpleNamespace(_handle_steer_run=lambda *a, **k: None)
        with _upstream_untouched():
            first = patches.install(runs_module=runs, prompt_builder=pb,
                                    helpers=helpers, system_prompt=system_prompt)
            second = patches.install(runs_module=runs, prompt_builder=pb,
                                     helpers=helpers, system_prompt=system_prompt)
            self.assertEqual(first.describe(), "handler=installed row_builder=installed "
                                                "channel_note=installed")
            self.assertEqual(second.describe(), "handler=already row_builder=already "
                                                "channel_note=already")
            self.assertEqual(system_prompt.STEER_CHANNEL_NOTE.count(CHAT_OPEN), 1,
                             "重复安装不得把说明叠加两遍")

    def test_unrecognised_upstream_shape_raises(self) -> None:
        with self.assertRaises(RuntimeError):
            patches.install(
                runs_module=SimpleNamespace(), prompt_builder=pb,
                helpers=helpers, system_prompt=system_prompt,
            )


class SchemaParityTests(unittest.TestCase):
    """NB 与网关插件两侧的字面量必须逐字一致（跨仓契约，靠这里钉住）。"""

    def test_literals_match_nb_side(self) -> None:
        nb_path = Path(NB_ROOT) / "core" / "steer_presentation.py"
        if not nb_path.exists():
            self.skipTest(f"NB 源码树不在 {NB_ROOT}")
        source = nb_path.read_text(encoding="utf-8")
        self.assertIn(f'PRESENTATION_SCHEMA = "{patches.PRESENTATION_SCHEMA}"', source)
        self.assertIn(f'PRESENTATION_FORMAT = "{patches.PRESENTATION_FORMAT}"', source)


@contextlib.contextmanager
def _upstream_untouched():
    """快照/还原四个被包装的上游对象，避免用例之间互相污染。"""
    from gateway.platforms import api_server_runs as runs

    saved = [
        (runs, "_handle_steer_run", runs._handle_steer_run),
        (pb, "steer_user_row", pb.steer_user_row),
        (helpers, "steer_user_row", helpers.steer_user_row),
        (system_prompt, "STEER_CHANNEL_NOTE", system_prompt.STEER_CHANNEL_NOTE),
    ]
    try:
        yield runs
    finally:
        for module, name, value in saved:
            setattr(module, name, value)
        patches._REGISTRY._texts.clear()


if __name__ == "__main__":
    unittest.main(verbosity=2)
