#!/usr/bin/env python3
"""E3：真实 Runs SSE 回放（delta 分段 → 即时决策 → 终态折叠）。

数据来源：
- SAMPLES/runs_terminal_single_b4.json：真实隔离实例 /v1/runs 采集（工具前 + 工具后
  各一份决策，终态 output 只有后一份）。
- SAMPLES/runs_terminal_multi_b5.json：真实采集（无工具，两份决策，终态 output 含两份）。
- steer 帧序列是按上游 gateway/platforms/api_server_runs.py 的帧形状（message.delta /
  run.steered / run.completed）合成的链式用例，**不是**真实 steer 捕获；真实捕获环境
  本轮不可用，脚本里显式标注。

本脚本不发 QQ、不连网关：只把事件按 SSE 行喂真实 RunsEventParser，逐帧记录
"决策在哪个 seq 完成（即可即时发送）"，再用真实折叠/计划器核对终态不会重发。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: E402,F401
from nonebot_plugin_hermes_adapter.core.hermes_client import RunsEventParser  # noqa: E402
from nonebot_plugin_hermes_adapter.core.npha_terminal import (  # noqa: E402
    decode_terminal_batch,
    structured_batch_from_terminal,
    structured_from_terminal,
)
from nonebot_plugin_hermes_adapter.core.runs_terminal import (  # noqa: E402
    build_batch_plan,
    fold_stream_decisions,
)


def sse_lines(events: list[dict]) -> list[str]:
    return [f"data: {json.dumps(event, ensure_ascii=False)}" for event in events]


def replay(label: str, events: list[dict], terminal_output: str) -> dict:
    parser = RunsEventParser()
    emitted: list[dict] = []
    drained: list = []
    tool_seen = 0
    for event in events:
        for line in sse_lines([event]):
            parser.feed(line)
        for decision in parser.drain_stream_decisions():
            drained.append(decision)
            emitted.append({
                "at_seq": event.get("seq"),
                "after_event": event.get("event"),
                "body": decision.body,
                "immediate": True,
            })
        if str(event.get("event", "")).startswith("tool."):
            tool_seen += 1
    streamed = [structured_from_terminal(d) for d in drained]
    terminal = structured_batch_from_terminal(decode_terminal_batch(terminal_output))
    merged, folded = fold_stream_decisions(streamed, terminal)
    plan, _ = build_batch_plan(
        [{"body": d["body"], "media_urls": [], "file_paths": [], "reply_to": d.get("reply_to", "")}
         for d in merged]
    )
    result = {
        "label": label,
        "terminal": parser.terminal,
        "usage": parser.usage,
        "immediate_units": emitted,
        "tool_frames": tool_seen,
        "terminal_units": [d["body"] for d in terminal],
        "folded_terminal_units": folded,
        "plan_contents": [u["cleaned_text"] for u in plan["units"]],
    }
    print(f"[{label}] terminal={parser.terminal} usage={parser.usage}")
    for item in emitted:
        print(
            f"  即时决策 after={item['after_event']} seq={item['at_seq']} "
            f"body={item['body']!r}"
        )
    print(f"  工具帧={tool_seen} 终态份={result['terminal_units']!r}")
    print(f"  折叠终态份={folded} 计划份={result['plan_contents']!r}")
    return result


def steer_events() -> list[dict]:
    """按上游帧形状合成 steer 中途插入（非真实捕获）。"""
    return [
        {"event": "message.delta", "delta": '{"body":"先答这句"}', "seq": 0},
        {"event": "run.steered", "accepted": True, "seq": 1},
        {"event": "message.delta", "delta": '\n\n{}\n{"body":"回应 steer"}', "seq": 2},
        {"event": "run.completed", "output": '{"body":"回应 steer"}', "seq": 3},
    ]


def main() -> int:
    failures: list[str] = []
    b4 = json.loads((ROOT / "SAMPLES" / "runs_terminal_single_b4.json").read_text(encoding="utf-8"))
    b4_result = replay("b4_tool_before_after", b4["events"], b4["status"]["output"])
    if len(b4_result["immediate_units"]) != 2:
        failures.append("b4 即时份数 != 2")
    if "先记录" not in b4_result["immediate_units"][0]["body"]:
        failures.append("b4 工具前正文未即时产出")
    # 工具后决策必须晚于工具帧、且两份之间确有工具边界
    first_seq = b4_result["immediate_units"][0]["at_seq"]
    last_seq = b4_result["immediate_units"][1]["at_seq"]
    if not any(
        str(e.get("event", "")).startswith("tool.")
        and first_seq < e.get("seq", -1) < last_seq
        for e in b4["events"]
    ):
        failures.append("b4 工具后正文与工具前正文之间没有工具边界")
    if b4_result["folded_terminal_units"] != 1:
        failures.append("b4 终态份未折叠")

    b5 = json.loads((ROOT / "SAMPLES" / "runs_terminal_multi_b5.json").read_text(encoding="utf-8"))
    b5_result = replay("b5_multi_no_tools", b5["events"], b5["status"]["output"])
    if len(b5_result["immediate_units"]) != 2:
        failures.append("b5 即时份数 != 2")
    if b5_result["folded_terminal_units"] != 2:
        failures.append("b5 终态两份未折叠")

    steer_result = replay(
        "steer_midway_synthetic", steer_events(), '{"body":"回应 steer"}'
    )
    if [u["body"] for u in steer_result["immediate_units"]] != ["先答这句", "回应 steer"]:
        failures.append("steer 中途插入未得到两份即时决策")
    if steer_result["folded_terminal_units"] != 1:
        failures.append("steer 终态未折叠")

    if failures:
        print("FAIL: " + "; ".join(failures))
        return 1
    print(
        "ALL PASS: 3 组回放（2 组真实 Runs 捕获 + 1 组合成 steer 帧形状），"
        "即时决策与终态折叠一致"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
