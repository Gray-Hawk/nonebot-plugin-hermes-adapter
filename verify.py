#!/usr/bin/env python3
"""E3 统一验收入口（只读，不改交付树）。

跑法：python3 verify.py    （需要 nonebot 环境，例如生产 venv 的 python）
逐项打印 PASS/FAIL，退出码 0 = 全过。
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.dont_write_bytecode = True  # verify 自身 import 不得在交付树留下 __pycache__
BASELINE = "498e7a60ff45737a75814a668813d9d37473e9cd"
ALLOWED_CHANGED = {
    "core/hermes_client.py",
    "core/npha_terminal.py",
    "core/prompt_builder.py",
    "core/run_tracker.py",
    "core/runs_terminal.py",
    "handlers/message_turns.py",
    "mcp/tools/push_message.py",
    "tasks/run_resume.py",
    "scripts/replay_runs_stream.py",
    "tests/test_runs_terminal.py",
    "tests/test_runs_stream.py",
    # 卫生批（t_65479fd7）：误导性测试名修正（断言与输入不变）
    "tests/test_completions_regression.py",
    "SAMPLES/runs_terminal_single_b4.json",
    "SAMPLES/runs_terminal_multi_b5.json",
    # 交付文档与验收入口（不是代码改动，随交付入库）
    "REPORT.md",
    "DIFF_STAT.md",
    "TEST_LOG.txt",
    "SHA256SUMS.txt",
    "verify.py",
    "PATCHES/01-modified.patch",
    "PATCHES/02-new-files.patch",
    # E3 验收运行证据（真实输出）
    "EVIDENCE/e3_chain_tests.txt",
    "EVIDENCE/e3_replay.txt",
    "EVIDENCE/e3_verify.txt",
    "EVIDENCE/e3_baseline_tests.txt",
    "EVIDENCE/e3_round1_fixes.txt",
    "EVIDENCE/e3_round1_reproduce_pass.txt",
    "EVIDENCE/e3_round2_fixes.txt",
    "EVIDENCE/e3_round2_probes_pass.txt",
    "EVIDENCE/e3_round2_patch_rebuild.txt",
    # E3 评审证据原样归档（评审新增；脚本以 .py.txt 归档，内容未改）
    "REVIEW_EVIDENCE/E3_ROUND1_REVIEW.md",
    "REVIEW_EVIDENCE/e3_round1_output.txt",
    "REVIEW_EVIDENCE/e3_round1_reproduce.py.txt",
    "REVIEW_EVIDENCE/e3_round1_patch_check.py.txt",
    "REVIEW_EVIDENCE/e3_round1_patch_output.txt",
    "REVIEW_EVIDENCE/E3_ROUND2_REVIEW.md",
    "REVIEW_EVIDENCE/e3_round2_output.txt",
    "REVIEW_EVIDENCE/e3_round2_probes.py.txt",
    "REVIEW_EVIDENCE/e3_round2_prior_probes.txt",
    "REVIEW_EVIDENCE/e3_round2_patch_check.txt",
    # E3 评审证据原样归档（评审新增，未改内容）
    "REVIEW_EVIDENCE/ROUND1_REVIEW.md",
    "REVIEW_EVIDENCE/regression_output.txt",
    "REVIEW_EVIDENCE/replay_output.txt",
    "REVIEW_EVIDENCE/round1_output.txt",
    "REVIEW_EVIDENCE/round1_reproduce.py",
    "REVIEW_EVIDENCE/ROUND2_REVIEW.md",
    "REVIEW_EVIDENCE/round2_output.txt",
    "REVIEW_EVIDENCE/round2_regression_output.txt",
    "REVIEW_EVIDENCE/round2_replay_output.txt",
    "REVIEW_EVIDENCE/round2_reproduce.py",
    "REVIEW_EVIDENCE/ROUND3_REVIEW.md",
    "REVIEW_EVIDENCE/round3_output.txt",
    "REVIEW_EVIDENCE/round3_regression_output.txt",
    "REVIEW_EVIDENCE/round3_replay_output.txt",
    "REVIEW_EVIDENCE/round3_verify.py",
    "REVIEW_EVIDENCE/E3_ROUND1_OUTPUT.txt",
    # 卫生批（t_65479fd7）：归档探针改为自行收集 drain 结果（断言未变）
    "REVIEW_EVIDENCE/e3_live_offline_verify.py",
}
MUST_NOT_CHANGE = (
    "config.py",
    "core/runs_companion.py",
    "core/reply_recovery.py",
    "core/steer_reconcile.py",
    "core/companion_runtime.py",
    "tasks/proactive_activation.py",
    "mcp/server.py",
    "backends/astrbot/client.py",
)
# 基线 852de61 上算出的 E1 定版协议块哈希（completions 行为不得回退）
BASELINE_COMPLETIONS_BLOCK_SHA = "a39c76d954276e8530c8e19b62f8774f753e9f6f205c5c63e92f87a2b6243246"
BASELINE_COMPLETIONS_PRIVATE_SHA = "43a06ad2d6339514160a9824b8880b888ec4dc2acc28c6bd8dd90fffa793da94"
BASELINE_OLD_BLOCK_SHA = "61bc0269262556713340e890c2d620857d1d8f4d360c45d15b1486c32a54b8d5"

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
        [sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests", "-q"],
        cwd=ROOT, capture_output=True, text=True, env=env,
    )
    tail = (proc.stdout + proc.stderr).strip().splitlines()[-3:]
    match = re.search(r"Ran (\d+) tests", proc.stderr + proc.stdout)
    ran = int(match.group(1)) if match else 0
    ok = proc.returncode == 0 and ran >= 152
    check("全量既有＋新增测试通过", ok, f"exit={proc.returncode} ran={ran} {' / '.join(tail)}")


def check_replay() -> None:
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    for script, label in (
        ("scripts/replay_runs_terminal.py", "真实 Runs 终态样本回放一致"),
        ("scripts/replay_runs_stream.py", "真实 Runs SSE 流式多决策回放一致"),
    ):
        proc = subprocess.run(
            [sys.executable, "-B", script],
            cwd=ROOT, capture_output=True, text=True, env=env,
        )
        ok = proc.returncode == 0 and "ALL PASS" in proc.stdout
        detail = "exit=%d" % proc.returncode
        if not ok:
            detail += " " + (proc.stdout + proc.stderr).strip()[-300:]
        check(label, ok, detail)


def check_completions_prompt() -> None:
    sys.path.insert(0, str(ROOT))
    from tests import _bootstrap  # noqa: F401

    from nonebot_plugin_hermes_adapter.core import prompt_builder as pb

    def sha(text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()

    check(
        "completions 协议块字节未变（E1 定版）",
        sha(pb._DECISION_PROTOCOL_BLOCK_COMPLETIONS) == BASELINE_COMPLETIONS_BLOCK_SHA
        and sha(pb._JSON_LINES_PRIVATE_BLOCK) == BASELINE_COMPLETIONS_PRIVATE_SHA
        and sha(pb._DECISION_PROTOCOL_BLOCK) == BASELINE_OLD_BLOCK_SHA,
        "",
    )
    runs_prompt = pb._DECISION_PROTOCOL_BLOCK_RUNS
    check(
        "runs 协议块允许即时连续多份（工具前后均可）",
        "若干份两行 JSON" in runs_prompt and "工具间隙" in runs_prompt
        and "不要攒到整轮结束" in runs_prompt
        and "工具调用可以夹在任意两份之间" in runs_prompt
        and "submit_decision" not in runs_prompt and "should_reply" not in runs_prompt,
        "",
    )
    private_runs = pb._JSON_LINES_PRIVATE_BLOCK_RUNS
    check(
        "runs 私聊协议块同步即时多份",
        "不要攒到整轮结束" in private_runs and "工具调用可以夹在任意两份之间" in private_runs,
        "",
    )


def check_scope() -> None:
    proc = git("diff", "--name-only", BASELINE, "HEAD")
    changed = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    extra = sorted(changed - ALLOWED_CHANGED)
    check("改动范围只在允许清单内", not extra, f"越界={extra}")
    dirty = sorted(
        path for path in MUST_NOT_CHANGE
        if git("diff", "--quiet", BASELINE, "HEAD", "--", path).returncode != 0
    )
    check("禁止改动文件未变", not dirty, f"被改={dirty}")
    staged = git("diff", "--cached", "--name-only").stdout.strip()
    check("验收不依赖暂存区", not staged, f"staged={staged.split() if staged else []}")
    worktree = git("status", "--porcelain").stdout.strip()
    check("交付树干净（无未提交改动）", not worktree, worktree.splitlines()[:5])


def check_shared_decoder() -> None:
    client = read("core/hermes_client.py")
    resume = read("tasks/run_resume.py")
    monitor = read("core/run_delivery_monitor.py")
    runs_terminal = read("core/runs_terminal.py")
    terminal = read("core/npha_terminal.py")
    turns = read("handlers/message_turns.py")
    ok = (
        "decode_terminal_batch(raw_text)" in client
        and "def units_from_output" in runs_terminal
        and "units_from_output(output, expect_structured)" in resume
        and "_build_units" in monitor
        and "units_from_decisions" in turns
        and "def decode_" not in runs_terminal  # 交付计划模块不另起解码器
        # E3:clientside 决策候选检测复用同一 decode_terminal，且只有一份 scanner
        and "def scan_stream_decisions" in terminal
        and bool(re.search(r"decode_terminal\(\s*stripped", terminal))
        and bool(re.search(r"scan_stream_decisions\(\s*self\.stream_text", client))
        and terminal.count("def scan_stream_decisions") == 1
        and client.count("class RunsEventParser") == 1
    )
    check("Runs 终态／流式复用同一解码器（无第二套）", ok, "")


def check_stream_fold_and_growth() -> None:
    runs_terminal = read("core/runs_terminal.py")
    tracker = read("core/run_tracker.py")
    turns = read("handlers/message_turns.py")
    ok = (
        "def fold_stream_decisions" in runs_terminal
        and "def fold_plan_units" in runs_terminal
        and "allow_prefix_growth=True" in runs_terminal
        and "def _is_plan_prefix_growth" in tracker
        and "def open_stream_delivery" in tracker
        and "append_stream_unit" in turns
        and "streamed_decisions" in turns
    )
    check("E3 流式折叠／只追加计划／复用终态恢复", ok, "")


def check_round1_fixes() -> None:
    """首轮评审 R1/R2/R3 的修复面必须留在交付代码里，而不是只靠测试。"""
    client = read("core/hermes_client.py")
    terminal = read("core/npha_terminal.py")
    resume = read("tasks/run_resume.py")
    runs_terminal = read("core/runs_terminal.py")
    ok = (
        # R1: 两个旁白出口都用同一份扫描区间摘除已消费的决策信封
        "def strip_complete_decisions" in terminal
        and client.count("strip_complete_decisions(") >= 2
        and "_pop_gap_segment" in client
        # R2: 比较用规范表示复用计划器的清洗/媒体摘取
        and runs_terminal.count("def normalize_decision") == 1
        and "_media_identity(" in runs_terminal
        and "_decision_unit(decision)" in runs_terminal
        # R3: 恢复链先看既有计划里允许补发的 pending 份
        and "def _pending_plan_indexes" in resume
        and "pending_existing" in resume
    )
    check("R1/R2/R3 修复面在位（旁白隔离／媒体规范表示／恢复尊重既有计划）", ok, "")


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
    "single": "test_single_unit",
    "multi": "test_multiple_units_in_order",
    "empty_control": "test_empty_control_object",
    "missing_control": "test_missing_control_body_only",
    "bad_control": "test_bad_control_still_delivers_body",
    "empty_body": "test_empty_body_is_silent",
    "same_twice": "test_same_body_twice_is_two_decisions",
    "long_body": "test_long_body_delivers_intact",
    "quotes_emoji": "test_quotes_backslash_emoji_preserved",
    "naked": "test_naked_body_is_single_decision",
    "illegal_tail": "test_illegal_trailing_plain_line_rejected",
    "residue": "test_complete_unit_plus_residue_rejected",
    "plan_reject": "test_changed_plan_is_rejected",
    "sent_not_reset": "test_repeated_prepare_keeps_plan_and_sent_state",
    "registration_zero_send": "test_plan_failure_means_zero_sends",
    "resume_no_dup": "test_already_sent_component_not_resent",
    # 首轮评审 R1-R4 反例转成的正式行为断言
    "silent_control_completions": "test_completions_silent_keeps_controls",
    "silent_control_runs": "test_runs_silent_unit_keeps_controls",
    "mixed_batch_control": "test_mixed_batch_runs_controls_and_skips_only_send",
    "resume_plan_same": "test_normal_plan_matches_resume_adapter",
    "empty_reply_to_same_plan": "test_empty_reply_to_normalized_in_plan",
    "bounce_no_repair": "test_reactive_bounced_unit_not_repaired",
    "bounce_all_suppressed": "test_reactive_all_bounced_settles_suppressed",
    "bounce_real_rule": "test_real_bounce_rule_marks_unit_not_required",
    "passive_bounce_no_repair": "test_passive_bounced_unit_not_repaired",
    "legacy_component_names": "test_completions_file_and_history_use_legacy_names",
    "group_passive_legacy_file": "test_group_passive_file_claim_uses_legacy_component",
    # 第二轮评审 R5-R6 反例转成的正式行为断言
    "passive_second_control": "test_second_unit_background_task_reaches_control",
    "passive_silent_second_control": "test_silent_second_unit_keeps_background_task",
    "passive_all_silent_control": "test_all_silent_batch_keeps_background_task",
    "passive_self_heal_order": "test_every_unit_self_heal_hint_consumed_in_order",
    "passive_single_control": "test_single_completions_control_unchanged",
    "runs_naked_inline_image": "test_runs_naked_image_survives_chat_and_handler",
    "plain_naked_overflow": "test_plain_overlong_naked_text_still_rejected",
    # E3 验收：真实 delta 分段、即时多份、折叠、steer、重连恢复
    "stream_b4_tool_before_after": "test_b4_two_units_with_tool_between",
    "stream_b5_multi": "test_b5_two_units_no_tools",
    "stream_split_body": "test_body_with_brace_survives_split",
    "stream_no_premature": "test_no_decision_before_body_complete",
    "stream_fold_suffix": "test_terminal_is_suffix_of_stream",
    "stream_same_text_segments": "test_same_text_different_segment_not_collapsed",
    "stream_plan_growth": "test_append_grows_plan_by_suffix",
    "stream_plan_prefix_locked": "test_changed_prefix_rejected_even_with_growth",
    "stream_client_wiring": "test_chat_wires_immediate_callback_and_streamed_result",
    "stream_terminal_parse_fail_no_dup": "test_terminal_parse_failure_after_stream_does_not_duplicate",
    "stream_tool_before_after_send": "test_tool_before_and_after_both_sent_immediately",
    "stream_multi_send": "test_normal_multi_decision_immediate_and_folded",
    "stream_same_text_send_once": "test_same_text_in_two_segments_sends_both_once",
    "stream_silent_control": "test_silent_empty_body_control_kept_and_next_unit_sent",
    "stream_terminal_only_unit": "test_terminal_only_unit_appended_and_sent",
    "stream_steer_immediate": "test_steer_midway_gets_immediate_reply",
    "stream_terminal_replay": "test_terminal_replay_does_not_resend",
    "stream_push_gate": "test_in_turn_group_push_rejected",
    "stream_push_completions": "test_completions_push_unchanged",
    "stream_retry_guard": "test_persistence_retry_skipped_after_live_send",
    "stream_resume_fold": "test_resume_sends_only_pending_unit",
    # 第 1 轮评审 R1-R3 反例转成的正式行为断言
    "interim_no_decision_echo": "test_interim_enabled_does_not_resend_decision_envelope",
    "interim_keeps_narration": "test_interim_enabled_keeps_ordinary_narration",
    "interim_bounce_isolation": "test_interim_cannot_bypass_bounce",
    "gap_segment_no_envelope": "test_real_b4_gap_segment_excludes_sent_decision",
    "interim_frame_no_envelope": "test_interim_frame_decision_envelope_not_echoed",
    "media_fold_canonical": "test_local_image_delta_and_terminal_fold_to_one",
    "media_reactive_once": "test_local_image_sent_once_reactive",
    "media_passive_once": "test_private_passive_local_image_sent_once",
    "resume_silent_pending": "test_silent_terminal_keeps_pending_stream_unit",
    "resume_silent_partial": "test_silent_terminal_only_resends_pending_not_sent",
    "resume_reconcile_no_dup": "test_followup_reconcile_does_not_resend_or_strand",
}


def check_acceptance_cases() -> None:
    source = read("tests/test_runs_terminal.py")
    missing = [
        name for name in ACCEPTANCE_CASES.values()
        if f"def {name}" not in source and f"def {name}" not in read("tests/test_runs_stream.py")
    ]
    check("E2/E3 验收用例齐全", not missing, f"缺={missing}")


def check_no_new_switch() -> None:
    proc = git("diff", "--name-only", BASELINE, "HEAD", "--", "config.py", "core/config_fields.py")
    check("无新增配置开关", not proc.stdout.strip(), proc.stdout.strip())


def main() -> int:
    print(f"interpreter={sys.executable}")
    print(f"root={ROOT}")
    hr = git("log", "--oneline", "-1").stdout.strip()
    print(f"HEAD={hr}")
    check_cache()
    check_tests()
    check_replay()
    check_completions_prompt()
    check_scope()
    check_shared_decoder()
    check_stream_fold_and_growth()
    check_round1_fixes()
    check_push_gate()
    check_plan_before_send()
    check_acceptance_cases()
    check_no_new_switch()
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
