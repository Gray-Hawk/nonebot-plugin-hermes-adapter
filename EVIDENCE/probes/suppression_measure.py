"""观察口统计探针（仅供测试/复核，不接生产日志）。

重放真实留存 SSE 样例（SAMPLES/）与回归场景，统计：
- 分母：有正文的 assistant 段数、发送单元数（流式 + 终态）；
- 段内全文匹配去重命中（reason=segment_duplicate）；
- 终态丢弃未封口 delta（reason=unsealed_delta_tail）。
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests import _bootstrap  # noqa: F401,E402
from nonebot_plugin_hermes_adapter.core.hermes_client import RunsEventParser  # noqa: E402
from nonebot_plugin_hermes_adapter.core import runs_text  # noqa: E402


def measure(events):
    parser = RunsEventParser()
    suppressed = []
    parser.on_suppressed = lambda reason, text, source: suppressed.append((reason, text, source))
    stream_units = 0
    segments = 0
    seg_has_text = False
    for event in events:
        name = event.get("event")
        if name == "message.interim":
            seg_has_text = seg_has_text or bool(str(event.get("text") or "").strip())
        elif name == "message.delta":
            seg_has_text = seg_has_text or bool(str(event.get("delta") or "").strip())
        parser.feed(f"data: {json.dumps(event, ensure_ascii=False)}")
        stream_units += len(parser.drain_stream_decisions())
        if name in ("tool.started", "run.completed"):
            if seg_has_text:
                segments += 1
            seg_has_text = False
    terminal = runs_text.decisions_from_text(parser.output) if parser.output else []
    return {
        "segments": segments,
        "stream_units": stream_units,
        "terminal_units": len(terminal),
        "units": stream_units + len(terminal),
        "dedup": sum(1 for reason, _, _ in suppressed if reason == "segment_duplicate"),
        "tail": sum(1 for reason, _, _ in suppressed if reason == "unsealed_delta_tail"),
        "suppressed": suppressed,
        "error": parser.text_error,
    }


def run(label, events):
    got = measure(events)
    print(f"{label}: {json.dumps(got, ensure_ascii=False)}")


for sample in ("runs_terminal_single_b4", "runs_terminal_multi_b5"):
    data = json.loads((ROOT / "SAMPLES" / f"{sample}.json").read_text(encoding="utf-8"))
    run(f"real/{sample} ({data.get('label')})", data["events"])

same = "甲\n§\n乙"
run("reg/interim_delta_same_text", [
    {"event": "message.interim", "text": "好", "seq": 0},
    {"event": "message.delta", "delta": "好", "seq": 1},
    {"event": "tool.started", "tool": "todo_list", "seq": 2},
    {"event": "run.completed", "output": "好", "seq": 3},
])
run("reg/unsealed_delta_tail", [
    {"event": "message.delta", "delta": "未封口的尾巴", "seq": 0},
    {"event": "run.completed", "output": "真正的终态正文", "seq": 1},
])
run("reg/terminal_repeat_last_segment", [
    {"event": "message.delta", "delta": "正文", "seq": 0},
    {"event": "tool.started", "tool": "submit_control", "seq": 1},
    {"event": "run.completed", "output": "正文", "seq": 2},
])
run("reg/a_tool_a", [
    {"event": "message.delta", "delta": "甲", "seq": 0},
    {"event": "tool.started", "tool": "lookup", "seq": 1},
    {"event": "message.delta", "delta": "甲", "seq": 2},
    {"event": "tool.started", "tool": "lookup", "seq": 3},
    {"event": "run.completed", "output": "", "seq": 4},
])
run("reg/multi_unit_segment", [
    {"event": "message.interim", "text": same, "seq": 0},
    {"event": "message.delta", "delta": same, "seq": 1},
    {"event": "tool.started", "tool": "lookup", "seq": 2},
    {"event": "run.completed", "output": "丙\n§\n丁", "seq": 3},
])
