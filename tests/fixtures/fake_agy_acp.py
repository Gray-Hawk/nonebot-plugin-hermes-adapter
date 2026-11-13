#!/usr/bin/env python3
"""Minimal fake `agy acp` stdio server for offline TDD.

Speaks NDJSON JSON-RPC on stdin/stdout. Every inbound frame is appended to the
log file given as argv[1] so tests can assert the exact wire shape without
touching real Antigravity.

Usage:
    fake_agy_acp.py <logfile> [reply-json] [flags...]

reply-json is a JSON string (e.g. '"§\\n你好\\n§"'); flags:
    --permission       send session/request_permission before the second chunk
    --exit-on-prompt   exit(7) as soon as session/prompt arrives
    --exit-after-chunk exit(8) after the first agent_message_chunk
    --hang-prompt      never answer session/prompt
    --error-prompt     answer session/prompt with JSON-RPC error -32603
    --error-model      answer session/set_model with JSON-RPC error -32602
    --gate-prompt=<p>  hold the prompt reply until file <p> exists (clear-fence
                       tests keep a normal prompt in flight while /clear runs)
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time

LOG_PATH = sys.argv[1]
REPLY = json.loads(sys.argv[2]) if len(sys.argv) > 2 else "§\n你好\n§"
FLAGS = set(sys.argv[3:])

_write_lock = threading.Lock()
_sessions: dict[str, dict] = {}
_count_lock = threading.Lock()
_perm_lock = threading.Lock()
_permissions: dict[int, dict] = {}


def send(payload: dict) -> None:
    with _write_lock:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def log(payload: dict) -> None:
    with open(LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        handle.flush()


def log_argv() -> None:
    """记下真实启动命令行，供测试断言 `--agent` 等 argv 项的落点。"""
    with open(LOG_PATH + ".argv", "w", encoding="utf-8") as handle:
        json.dump(sys.argv, handle, ensure_ascii=False)
        handle.flush()


def update(session_id: str, kind: str, text: str) -> None:
    send(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": kind,
                    "content": {"type": "text", "text": text},
                },
            },
        }
    )


def _gate_file() -> str:
    for flag in FLAGS:
        if flag.startswith("--gate-prompt="):
            return flag.partition("=")[2]
    return ""


def wait_gate() -> None:
    """Clear-fence 夹具:普通 prompt 先挂起,等测试跑完 /clear 再放行回复。"""
    gate = _gate_file()
    if not gate:
        return
    deadline = time.monotonic() + 10
    while not os.path.exists(gate):
        if time.monotonic() > deadline:
            raise RuntimeError("fake agy gate release timed out")
        time.sleep(0.01)


def run_prompt(request: dict) -> None:
    params = request.get("params") or {}
    session_id = params.get("sessionId") or ""
    request_id = request.get("id")
    if "--hang-prompt" in FLAGS:
        return
    wait_gate()
    if "--exit-on-prompt" in FLAGS:
        os._exit(7)
    if "--error-prompt" in FLAGS:
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "fake prompt failure"},
            }
        )
        return

    # A thought chunk and a plan update must never reach the QQ body.
    update(session_id, "agent_thought_chunk", "THOUGHT-MUST-NOT-SURFACE")
    send(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "plan",
                    "entries": [{"content": "PLAN-MUST-NOT-SURFACE", "priority": "low", "status": "pending"}],
                },
            },
        }
    )

    split_at = max(0, len(REPLY) // 2)
    update(session_id, "agent_message_chunk", REPLY[:split_at])

    if "--permission" in FLAGS:
        permission_id = 9000
        event = threading.Event()
        with _perm_lock:
            _permissions[permission_id] = {"event": event, "response": None}
        send(
            {
                "jsonrpc": "2.0",
                "id": permission_id,
                "method": "session/request_permission",
                "params": {
                    "sessionId": session_id,
                    "toolCall": {"toolCallId": "tc-1", "title": "write_file"},
                    "options": [
                        {"optionId": "allow-once", "name": "Allow", "kind": "allow_once"},
                        {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
                    ],
                },
            }
        )
        event.wait(5)

    if "--exit-after-chunk" in FLAGS:
        os._exit(8)

    update(session_id, "agent_message_chunk", REPLY[split_at:])
    send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "end_turn"}})


def handle_request(message: dict) -> None:
    method = message.get("method")
    params = message.get("params") or {}
    request_id = message.get("id")

    if method == "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": 1,
                    "agentCapabilities": {"loadSession": True},
                    "agentInfo": {"name": "fake-agy", "version": "0"},
                },
            }
        )
        return

    if method == "session/new":
        with _count_lock:
            session_id = f"sess-{len(_sessions) + 1}"
            _sessions[session_id] = {"cwd": params.get("cwd"), "model": "default-model"}
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "sessionId": session_id,
                    "models": {
                        "currentModelId": "default-model",
                        "availableModels": [
                            {"modelId": "default-model", "name": "Default"},
                            {"modelId": "gemini-3.8-flash-high", "name": "Gemini 3.8 Flash High"},
                        ],
                    },
                },
            }
        )
        return

    if method == "session/resume":
        session_id = params.get("sessionId") or ""
        if session_id not in _sessions:
            send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32602, "message": "unknown session"},
                }
            )
            return
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
        return

    if method == "session/set_model":
        if "--error-model" in FLAGS:
            send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32602, "message": "unknown model"},
                }
            )
            return
        session_id = params.get("sessionId") or ""
        if session_id in _sessions:
            _sessions[session_id]["model"] = params.get("modelId")
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
        return

    if method == "session/prompt":
        threading.Thread(target=run_prompt, args=(message,), daemon=True).start()
        return

    if method == "session/cancel":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
        return

    send(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"method not found: {method}"},
        }
    )


def main() -> None:
    log_argv()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            continue
        log(message)
        if not isinstance(message, dict):
            continue
        if "method" not in message:
            # Response from the client to an agent request (permission).
            permission = _permissions.get(message.get("id"))
            if permission is not None:
                permission["response"] = message
                permission["event"].set()
            continue
        handle_request(message)


if __name__ == "__main__":
    main()
