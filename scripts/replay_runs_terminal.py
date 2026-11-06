"""真实 Runs 终态样本回放（E2，只读）。

样本来自隔离实例实采的 `POST /v1/runs` + `GET /v1/runs/{id}/events`：

  SAMPLES/runs_terminal_single_b4.json  乙格式提示词＋工具，单份终态
  SAMPLES/runs_terminal_multi_b5.json   乙格式提示词，无工具，两份终态

回放做三件事，全部只读、不发网络请求：
1. 把 events 逐条还原成 SSE data 行喂 `RunsEventParser`，核对终态 output/usage
   与样本 status 完全一致；
2. 用 E2 解码器 `decode_terminal_batch` 解终态 output，打印每份的 control/body；
3. 用 `structured_batch_from_terminal` 确认发送侧只消费 body。

用法： python3 scripts/replay_runs_terminal.py [样本路径...]
退出码：0 = 全部一致；1 = 有样本不一致（打印差异）。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 工作树本身是插件包（含相对导入），必须按包名导入：与测试同一条 bootstrap
# 路径（初始化 nonebot、把工作树挂成 nonebot_plugin_hermes_adapter）。
from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.core.hermes_client import RunsEventParser  # noqa: E402
from nonebot_plugin_hermes_adapter.core.npha_terminal import (  # noqa: E402
    decode_terminal_batch,
    structured_batch_from_terminal,
)

DEFAULT_SAMPLES = (
    ROOT / "SAMPLES" / "runs_terminal_single_b4.json",
    ROOT / "SAMPLES" / "runs_terminal_multi_b5.json",
)


def replay(path: Path) -> list[str]:
    problems: list[str] = []
    payload = json.loads(path.read_text(encoding="utf-8"))
    status = payload.get("status") or {}
    events = payload.get("events") or []
    parser = RunsEventParser()
    for event in events:
        parser.feed("data: " + json.dumps(event, ensure_ascii=False))
    print(f"[{path.name}] run_id={payload.get('run_id')}")
    print(f"  status={status.get('status')} usage={status.get('usage')}")
    print(f"  events={len(events)} last_seq={parser.last_seq} terminal={parser.terminal}")
    if not parser.terminal:
        problems.append(f"{path.name}: 事件流未见终态帧")
    if parser.output != status.get("output"):
        problems.append(f"{path.name}: 事件终态 output 与 status.output 不一致")
    if not parser.usage:
        problems.append(f"{path.name}: 终态帧缺 usage")
    output = status.get("output") or ""
    try:
        decisions = decode_terminal_batch(output)
    except Exception as exc:  # noqa: BLE001 - 回放脚本显式打印失败原因
        problems.append(f"{path.name}: 终态解码失败 {exc}")
        return problems
    structured = structured_batch_from_terminal(decisions)
    for index, (decision, item) in enumerate(zip(decisions, structured)):
        print(
            f"  unit[{index}] control={ {k: v for k, v in decision.control.items() if v} } "
            f"body={decision.body!r} silent={decision.silent} "
            f"send_body={item['body']!r} should_reply={item['should_reply']}"
        )
        if "reply_text" in item:
            problems.append(f"{path.name}: structured 残留 reply_text")
    return problems


def main(argv: list[str]) -> int:
    samples = [Path(item) for item in argv[1:]] or list(DEFAULT_SAMPLES)
    problems: list[str] = []
    for sample in samples:
        if not sample.is_file():
            problems.append(f"样本不存在: {sample}")
            continue
        problems.extend(replay(sample))
    if problems:
        print("FAIL:")
        for item in problems:
            print(" -", item)
        return 1
    print(f"ALL PASS: {len(samples)} 个真实 Runs 终态样本回放一致")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
