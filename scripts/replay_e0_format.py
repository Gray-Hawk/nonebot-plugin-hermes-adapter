#!/usr/bin/env python3
"""用 E0 两份真实乙模型结果文件回放 completions 终态解码器。

合规样本必须能解出一份决策；非合规样本（含 length 空输出）必须明确失败，
不得被当成格式成功，也不得回退原始信封。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.npha_terminal import TerminalFormatError, decode_terminal  # noqa: E402


def main() -> int:
    files = sorted((ROOT.parent / "REFERENCE").glob("FORMAT_RESULTS*.jsonl"))
    if not files:
        print("NO_SAMPLES")
        return 2
    total = ok = 0
    problems = []
    for path in files:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("format") != "B":
                continue
            total += 1
            reason = ""
            try:
                dec = decode_terminal(row.get("raw_output") or "")
                accepted = True
            except TerminalFormatError as exc:
                accepted = False
                reason = exc.reason
            # completions 只接受一份：E0 的多决策样本必须按“多份”失败
            expect_accept = bool(row.get("compliant")) and row.get("scenario") != "multi"
            if accepted == expect_accept:
                ok += 1
            else:
                problems.append({
                    "file": path.name, "scenario": row.get("scenario"), "rep": row.get("rep"),
                    "compliant": row.get("compliant"), "finish_reason": row.get("finish_reason"),
                    "accepted": accepted, "reason": None if accepted else reason,
                })
    print(f"E0 乙样本回放: {ok}/{total} 与既有判定一致")
    for item in problems:
        print("MISMATCH", json.dumps(item, ensure_ascii=False))
    return 0 if not problems else 1


if __name__ == "__main__":
    raise SystemExit(main())
