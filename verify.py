#!/usr/bin/env python3
"""分节符正文信封的只读验收入口。

跑法：python3 verify.py    （需要 nonebot 环境，例如生产 venv 的 python）
逐项打印 PASS/FAIL，退出码 0 = 全过。

本脚本早期是 E3 信封时代的验收清单。`JSON 决策信封退役`（Runs 与 completions
统一纯文本）之后，旧的 `core/npha_terminal.py`、`decode_terminal_batch`、
`fold_stream_decisions`、`strip_complete_decisions`、`scan_stream_decisions`
与两个 replay 脚本都已删除，且它引用的基线提交不在本仓库——本版改为断言当前
事实：只有一份分节符信封解析器、协议块里没有信封字段、控制与续跑各只有一条链路。
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.dont_write_bytecode = True  # verify 自身 import 不得在交付树留下 __pycache__

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(ok), detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-c", f"safe.directory={ROOT}", *args],
        cwd=ROOT, capture_output=True, text=True,
    )


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_cache() -> None:
    offenders: list[str] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        if ".git" in Path(dirpath).parts:
            continue
        for name in dirnames:
            if name in ("__pycache__", ".pytest_cache"):
                offenders.append(str(Path(dirpath) / name))
        offenders.extend(
            str(Path(dirpath) / name) for name in filenames if name.endswith(".pyc")
        )
    check("交付树无 pyc／pytest 缓存", not offenders, "; ".join(offenders[:5]))


def check_tests() -> None:
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    proc = subprocess.run(
        [sys.executable, "-B", "-m", "unittest", "discover",
         "-s", "tests", "-t", ".", "-q"],
        cwd=ROOT, capture_output=True, text=True, env=env,
    )
    tail = (proc.stdout + proc.stderr).strip().splitlines()[-3:]
    match = re.search(r"Ran (\d+) tests", proc.stderr + proc.stdout)
    ran = int(match.group(1)) if match else 0
    ok = proc.returncode == 0 and ran >= 400
    check("全量既有＋新增测试通过", ok, f"exit={proc.returncode} ran={ran} {' / '.join(tail)}")


def check_no_deleted_modules() -> None:
    """信封时代的模块与符号必须真的消失：删了文件却留引用比留着更糟。"""
    gone_files = (
        "core/npha_terminal.py",
        "scripts/replay_runs_terminal.py",
        "scripts/replay_runs_stream.py",
    )
    still_there = [path for path in gone_files if (ROOT / path).exists()]
    tokens = (
        "npha_terminal",
        "decode_terminal_batch",
        "decode_terminal(",
        "fold_stream_decisions",
        "strip_complete_decisions",
        "scan_stream_decisions",
        "_pop_gap_segment",
    )
    sources = (
        "core/hermes_client.py",
        "core/runs_terminal.py",
        "core/run_delivery_monitor.py",
        "handlers/message_turns.py",
        "tasks/run_resume.py",
    )
    refs = [
        f"{path}:{token}"
        for path in sources
        for token in tokens
        if token in read(path)
    ]
    check(
        "已删除的信封模块／符号零引用",
        not still_there and not refs,
        f"文件仍在={still_there} 引用={refs[:5]}",
    )


def check_single_text_parser() -> None:
    """正常链／流式链／恢复链／对账链共用同一份纯文本切分。"""
    client = read("core/hermes_client.py")
    resume = read("tasks/run_resume.py")
    monitor = read("core/run_delivery_monitor.py")
    runs_terminal = read("core/runs_terminal.py")
    runs_text = read("core/runs_text.py")
    turns = read("handlers/message_turns.py")
    ok = (
        "def split_text_units" in runs_text
        and "def decisions_from_text" in runs_text
        and "_FENCE" not in runs_text  # 代码围栏不是分隔规则的例外
        and "decisions_from_text(raw_text)" in client
        and "def units_from_output" in runs_terminal
        and "units_from_output(output)" in resume
        and "_build_units" in monitor
        and "units_from_decisions" in turns
        and "def decode_" not in runs_terminal  # 交付计划模块不另起解码器
        and client.count("class RunsEventParser") == 1
    )
    check("Runs 终态／流式／恢复／对账共用同一纯文本解析器", ok, "")


def check_envelope_protocol() -> None:
    """分节符信封的真实语义（跑解析器与提示词，不是文本匹配）。"""
    sys.path.insert(0, str(ROOT))
    from tests import _bootstrap  # noqa: F401

    from nonebot_plugin_hermes_adapter.core import prompt_protocol as pp
    from nonebot_plugin_hermes_adapter.core import runs_text

    sep = runs_text.SEPARATOR

    def units(text: str) -> list[str]:
        return runs_text.split_text_units(text)

    def raises_overflow(text: str) -> bool:
        try:
            runs_text.split_text_units(text)
        except runs_text.TextUnitsError as exc:
            return exc.reason == "body_overflow"
        return False

    cases = {
        "单条": units(f"{sep}\n正文\n{sep}") == ["正文"],
        "两条独立信封": units(f"{sep}\nA\n{sep}\n{sep}\nB\n{sep}") == ["A", "B"],
        "旧共享分界不再拆两份": units(f"{sep}\nA\n{sep}\nB\n{sep}") == ["A"],
        "静默零单位": (
            units(f"{sep}\n{sep}") == []
            and units(f"{sep}{sep}") == []
            and units(f"{sep} {sep}") == []
        ),
        "空段丢弃": units(f"{sep}\nA\n{sep}\n{sep}\nB\n{sep}") == ["A", "B"],
        "缺开头只丢头部": units(f"裸\n{sep}\nB\n{sep}") == ["B"],
        "缺末尾只丢尾部": units(f"{sep}\nA\n{sep}\n裸") == ["A"],
        "旧裸正文不兼容": units("你好") == [] and units('{"body":"旧"}') == [],
        "行内与带空格 § 是正文": (
            units(f"{sep}\n第 3 节 §3\n{sep}") == ["第 3 节 §3"]
            and units(f"{sep}\n甲\n § \n乙\n{sep}") == ["甲\n § \n乙"]
        ),
        "围栏内 § 仍闭合信封": units(f"{sep}\n```\n§\n```\n{sep}") == ["```"],
        "超长 fail-closed": raises_overflow(f"{sep}\n" + "长" * 4001 + f"\n{sep}"),
    }
    failed = [name for name, ok in cases.items() if not ok]
    check("信封解析语义（单条/多条/静默/缺开缺闭/旧裸正文不兼容）", not failed, f"失败={failed}")

    prompt_ok = all(
        "以独立一行 § 开头，以独立一行 § 结尾" in block
        and "每份都要自己写开头和结尾" in block
        and "只有被两个 § 完整包住的段才会发出去" in block
        and "只输出两行 §" in block
        and "没有 § 就是一条消息" not in block
        and "两条之间也单独写一行 §" not in block
        for block in (
            pp.render_mode_text(pp._PROTOCOL, runs=True),
            pp.render_mode_text(pp._PROTOCOL, runs=False),
        )
    )
    check("Runs／completions 提示词同口径写死信封格式", prompt_ok, "")


def check_prompt_protocol() -> None:
    sys.path.insert(0, str(ROOT))
    from tests import _bootstrap  # noqa: F401

    from nonebot_plugin_hermes_adapter.core import prompt_builder as pb
    from nonebot_plugin_hermes_adapter.core import prompt_protocol as pp

    for attr in ("_DECISION_PROTOCOL_BLOCK_COMPLETIONS", "_DECISION_PROTOCOL_BLOCK_RUNS"):
        block = getattr(pb, attr)
        ok = (
            "submit_decision" not in block
            and "should_reply" not in block
            and "reply_text" not in block
            # 反例保留：教模型「这种 JSON 信封是错的」，而不是要求它写。
            and '错误：{"body"' in block
        )
        check(f"{attr} 纯文本（无信封字段）", ok, "")

    runs = pb._DECISION_PROTOCOL_BLOCK_RUNS
    private_runs = pb._JSON_LINES_PRIVATE_BLOCK_RUNS
    completions = pb._DECISION_PROTOCOL_BLOCK_COMPLETIONS
    check(
        "runs 协议块：§ 独立信封、无 push_message、无布尔箭头",
        "每份都要自己写开头和结尾" in runs
        and "push_message" not in runs
        and "→ true" not in runs
        and "→ false" not in runs
        and "should_exit_active=false" not in runs
        and "决定 正文 范围" not in runs,
        "",
    )
    check("runs 私聊协议块无 push_message", "push_message" not in private_runs, "")
    check(
        "completions 协议块保留 push_message 进度口径",
        "push_message" in completions and "request_continuation" in completions,
        "",
    )
    check(
        "completions 私聊块无信封",
        "submit_decision" not in pb._JSON_LINES_PRIVATE_BLOCK,
        "",
    )
    # 模式标记行只在对应模式出现：runs 渲染文本里不得残留 completions 专有指令。
    check(
        "协议渲染：runs/completions 互不串味",
        "push_message" not in pp.render_mode_text(pp._PROTOCOL, runs=True)
        and "push_message" in pp.render_mode_text(pp._PROTOCOL, runs=False),
        "",
    )


# 网关薄插件与 NB 侧不是同一套测试发现：`tests/` 的数字不含它们（reviewer t_049df793 R2）。
# 这些插件测试必须用 Hermes venv、离开本仓库根目录（根目录 utils.py 会遮蔽 Hermes 的
# utils），并且以各自声明的用例数下界做闸——少跑不等于通过。
HERMES_REPO = Path("/home/neonsky/.hermes/hermes-agent")
HERMES_VENV_PY = HERMES_REPO / "venv" / "bin" / "python"
PLUGIN_TEST_SUITES = {
    "queued-lane-interim-thin": 48,
    "steer-presentation-thin": 21,
}


def check_gateway_plugin_tests() -> None:
    """网关插件独立测试：不在 `tests/` 里，必须单独跑，不能拿全量数字冒充包含它们。"""
    if not HERMES_VENV_PY.is_file():
        check("网关插件独立测试", False, f"缺 Hermes 解释器 {HERMES_VENV_PY}")
        return
    import shutil
    import tempfile

    base = Path(tempfile.mkdtemp(prefix="verify-plugins-"))
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
               PYTHONPATH=str(HERMES_REPO), TMPDIR=str(base))
    env.pop("HERMES_KANBAN_TASK", None)
    details: list[str] = []
    ok = True
    for name, minimum in PLUGIN_TEST_SUITES.items():
        suite = ROOT / "gateway_plugins" / name / "tests"
        if not suite.is_dir():
            details.append(f"{name}: 缺 tests/")
            ok = False
            continue
        proc = subprocess.run(
            [str(HERMES_VENV_PY), "-B", "-m", "unittest", "discover",
             "-s", str(suite), "-q"],
            cwd=str(base), capture_output=True, text=True, env=env,
        )
        match = re.search(r"Ran (\d+) tests", proc.stderr + proc.stdout)
        ran = int(match.group(1)) if match else 0
        verdict = "OK" if proc.returncode == 0 else "FAILED"
        details.append(f"{name}: {verdict} ran={ran} (需 >= {minimum})")
        ok = ok and proc.returncode == 0 and ran >= minimum
    shutil.rmtree(base, ignore_errors=True)
    check("网关插件独立测试（不在 tests/ 全量里）", ok, "; ".join(details))


def check_plan_and_stream() -> None:
    runs_terminal = read("core/runs_terminal.py")
    tracker = read("core/run_tracker.py")
    turns = read("handlers/message_turns.py")
    ok = (
        "def fold_plan_units" in runs_terminal
        and "allow_prefix_growth=True" in runs_terminal
        and "def _is_plan_prefix_growth" in tracker
        and "def open_stream_delivery" in tracker
        and "append_stream_unit" in turns
        and "streamed_decisions" in turns
        and "def _text_unit" in runs_terminal
        and "def _decision_unit" in runs_terminal
    )
    check("流式只追加计划／终态与恢复共用计划适配器", ok, "")


def check_control_and_continuation() -> None:
    client = read("core/hermes_client.py")
    turns = read("handlers/message_turns.py")
    control = read("core/control_intent.py")
    continuation = read("core/continuation.py")
    server = read("mcp/server.py")
    protocol = read("core/prompt_protocol.py")
    ok = (
        "def bind_control" in control
        and "def resolve_control" in control
        and "def request_continuation" in continuation
        and "def take_continuation" in continuation
        and "def submit_control" in server
        and "def request_continuation" in server
        and "submit_control" in protocol
        and "request_continuation" in protocol
        # 控制封段固定在控制工具 tool.started：此时控制尚未登记。
        and "_is_control_tool_payload" in client
        and turns.count("take_continuation(") >= 3
    )
    check("控制 FIFO 与续跑登记各只有一条链路", ok, "")


def check_push_gate() -> None:
    push = read("mcp/tools/push_message.py")
    ok = (
        "has_active_run_for_target" in push
        and 'hermes_api_interface == "runs"' in push
        and "in-turn text must go" in push
    )
    check("Runs 轮内 push 硬拒（轮外/completions 不变）", ok, "")


def check_plan_before_send() -> None:
    source = read("handlers/message_turns.py")
    for func in ("async def _run_reactive_turn", "async def _run_passive_turn"):
        start = source.index(func)
        end = source.find("\nasync def ", start + 10)
        if end < 0:
            end = len(source)
        body = source[start:end]
        plan_at = body.find("_prepare_terminal_batch_contexts(")
        if func.endswith("_run_passive_turn"):
            # 私聊路径的发送体在嵌套函数里定义；计划必须先于它。
            send_at = body.find("async def _deliver_passive_once")
        else:
            send_at = body.find("_terminal_delivery(")
        ok = (
            plan_at >= 0 and send_at >= 0 and plan_at < send_at
            and "except TerminalPlanError" in body
        )
        if not ok:
            check(f"{func.rsplit(' ', 1)[-1]} 先落计划再发送", False, "")
            return
    check("终态链先落整批计划再发送", True, "")


ACCEPTANCE_CASES = {
    # 分节符信封（tests/test_runs_envelope.py）
    "envelope_single": "test_single_body",
    "envelope_multi": "test_multiple_bodies_share_one_marker_each_side",
    "envelope_silent": "test_silence_is_two_markers",
    "envelope_silent_rule": "test_silence_is_only_markers_and_whitespace",
    "envelope_empty_segment": "test_empty_segments_are_dropped_without_complaint",
    "envelope_missing_open": "test_missing_open_drops_only_the_head",
    "envelope_missing_close": "test_missing_close_drops_the_tail",
    "envelope_no_marker": "test_no_marker_is_not_a_body",
    "envelope_half_line": "test_half_line_markers_are_plain_text",
    "envelope_spaced": "test_spaced_markers_are_plain_text",
    "envelope_fenced": "test_fenced_marker_splits",
    "envelope_overflow": "test_over_limit_is_fail_closed",
    "envelope_stream_char_by_char": "test_char_by_char_delta",
    "envelope_stream_marker_split": "test_marker_split_across_frames",
    "envelope_stream_dangling": "test_dangling_marker_is_not_sent_until_closed",
    "envelope_interim_partial": "test_interim_only_sends_closed_segments",
    "envelope_terminal_tail": "test_run_completed_only_uses_the_final_output",
    "envelope_prompt_envelope": "test_all_four_prompts_state_the_envelope",
    "envelope_prompt_parser": "test_prompt_three_body_example_parses_to_three",
    "envelope_prompt_silence": "test_prompt_silence_example_is_silent",
    "envelope_prompt_literal": "test_prompt_literal_marker_example_is_plain_text",
    "envelope_prompt_no_escapes": "test_prompt_uses_real_multiline_examples_not_escapes",
    # 纯文本切分
    "single_sentence": "test_single_sentence_is_one_unit",
    "multiline_no_split": "test_multiline_without_separator_stays_one",
    "three_units": "test_three_units_in_order",
    "strict_separator": "test_separator_with_spaces_is_literal",
    "fenced_separator_splits": "test_fenced_separator_still_splits",
    "legacy_json_verbatim": "test_units_from_output_keeps_legacy_json_verbatim",
    "decisions_raw_body": "test_decisions_from_text_keeps_raw_body",
    "overlong_fail_closed": "test_over_limit_fails_closed",
    # 流式解析器
    "stream_single": "test_single_sentence_is_one_verbatim_unit",
    "stream_separator_cross_frame": "test_separator_split_across_delta_frames",
    "stream_spaced_separator": "test_separator_with_spaces_does_not_split",
    "stream_interim_one_char": "test_interim_one_char_progress_is_sent",
    "stream_interim_dedupe": "test_interim_and_delta_same_text_sent_once_per_segment",
    "stream_interim_prefix_shared_marker": "test_interim_prefix_keeps_the_shared_marker_of_the_tail",
    "stream_interim_prefix_tail_close": "test_interim_prefix_tail_still_needs_its_own_close",
    "stream_interim_prefix_no_invented_marker": "test_interim_prefix_does_not_invent_a_marker",
    "stream_interim_prefix_whole_line_only": "test_interim_prefix_keeps_marker_only_when_it_was_a_whole_line",
    "stream_cross_segment_twice": "test_same_text_across_segments_sends_twice",
    "stream_control_seals": "test_control_tool_frame_seals_segment_without_new_batch",
    "stream_unsealed_tail": "test_terminal_ignores_unsealed_delta_tail",
    "stream_suppression_observable": "test_segment_duplicate_suppression_is_observable",
    "stream_repeat_terminal": "test_terminal_repeating_last_segment_reports_no_suppression",
    "stream_invalid_log": "test_invalid_envelope_leaves_visible_log",
    "stream_incomplete": "test_incomplete_envelope_is_not_sent",
    "stream_real_samples": "test_real_retained_samples",
    "stream_regression_scenarios": "test_regression_scenarios",
    # 真实 client 链
    "client_single_zero_calls": "test_single_sentence_zero_extra_calls",
    "client_multi_terminal": "test_three_units_from_terminal_in_order",
    "client_legacy_rejected": "test_legacy_json_sends_nothing",
    "client_silent": "test_silent_output_sends_nothing",
    "client_progress_then_final": "test_interim_progress_then_final",
    "client_interim_partial": "test_incomplete_interim_progress_is_not_sent",
    "client_interim_shared_marker": "test_shared_marker_between_units_survives_interim_prefix",
    "client_interim_shared_marker_unclosed": "test_shared_marker_tail_without_close_is_still_refused",
    "client_interim_inline_marker_not_invented": "test_inline_marker_prefix_does_not_invent_a_unit",
    "client_control_seal": "test_control_frame_seals_segment_before_registration",
    "client_terminal_repeat_resent": "test_terminal_repeating_last_segment_is_resent",
    "client_overflow_fail_closed": "test_overflow_terminal_fails_closed",
    "client_stream_break": "test_stream_break_sends_no_half_content",
    # 计划／恢复／控制
    "plan_persists": "test_plan_persists_all_components",
    "plan_reject": "test_changed_plan_is_rejected",
    "plan_failure_zero_send": "test_plan_failure_means_zero_sends",
    "already_sent_not_resent": "test_already_sent_component_not_resent",
    "resume_same_plan": "test_normal_plan_matches_resume_adapter",
    "empty_reply_to_plan": "test_empty_reply_to_normalized_in_plan",
    "passive_second_control": "test_second_unit_control_kept",
    "unpaired_control_dropped": "test_unpaired_control_dropped_at_turn_end",
    "completions_control": "test_single_completions_text_unit_gets_control",
    "completions_silent": "test_completions_silent_leaves_controls_alone",
    "runs_silent": "test_runs_silent_unit_leaves_controls_alone",
    "exit_window_group": "test_batch_exit_control_closes_window_after_group",
    "bounce_no_repair": "test_reactive_bounced_unit_not_repaired",
    "bounce_all_suppressed": "test_reactive_all_bounced_settles_suppressed",
    "bounce_real_rule": "test_real_bounce_rule_marks_unit_not_required",
    "passive_bounce": "test_passive_bounced_unit_not_repaired",
    # 本轮审查修复面
    "astrbot_normalize": "test_reply_text_becomes_body_and_control_fields_private",
    "astrbot_private_text": "test_astrbot_private_reply_text_delivered",
    "astrbot_group_text": "test_astrbot_group_reply_text_delivered",
    "passive_continuation_zero_body": "test_passive_zero_body_still_fires_continuation",
    "passive_continuation_empty_content": "test_passive_empty_content_still_fires_continuation",
}


def check_acceptance_cases() -> None:
    haystack = "\n".join(
        read(path) for path in (
            "tests/test_runs_envelope.py",
            "tests/test_runs_terminal.py",
            "tests/test_runs_stream.py",
            "tests/test_queued_lane_interim_client.py",
            "tests/test_astrbot_text_compat.py",
            "tests/test_continuation.py",
        )
    )
    missing = [name for name in ACCEPTANCE_CASES.values() if f"def {name}" not in haystack]
    check("关键验收用例齐全", not missing, f"缺={missing}")


def check_worktree() -> None:
    staged = git("diff", "--cached", "--name-only").stdout.strip()
    check("验收不依赖暂存区", not staged, f"staged={staged.split() if staged else []}")
    worktree = git("status", "--porcelain").stdout.strip()
    check("交付树干净（无未提交改动）", not worktree, worktree.splitlines()[:5])


def main() -> int:
    print(f"interpreter={sys.executable}")
    print(f"root={ROOT}")
    hr = git("log", "--oneline", "-1").stdout.strip()
    print(f"HEAD={hr}")
    check_cache()
    check_tests()
    check_no_deleted_modules()
    check_single_text_parser()
    check_envelope_protocol()
    check_prompt_protocol()
    check_gateway_plugin_tests()
    check_plan_and_stream()
    check_control_and_continuation()
    check_push_gate()
    check_plan_before_send()
    check_acceptance_cases()
    check_worktree()
    check_cache()  # 复跑后仍无缓存
    failed = [name for name, ok, _ in RESULTS if not ok]
    print()
    if failed:
        print(f"FAILED: {len(failed)}/{len(RESULTS)} 项未通过")
        return 1
    print(f"ALL PASS: {len(RESULTS)}/{len(RESULTS)} 项通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
