#!/usr/bin/env python3
"""Minimal fake `agy acp` stdio server for offline TDD.

Speaks NDJSON JSON-RPC on stdin/stdout. Every inbound frame is appended to the
log file given as argv[1] so tests can assert the exact wire shape without
touching real Antigravity.

Usage:
    fake_agy_acp.py <logfile> [reply-json] [flags...]

reply-json is a JSON string (e.g. '"§\\n你好\\n§"'); flags:
    --permission       send session/request_permission before the second chunk
    --permission-id=<v>        permission RPC id（可给字符串，验证原 id 类型保留）
    --permission-title=<t>     toolCall.title（缺省 write_file）
    --permission-name=<n>      toolCall.name（阶段2C1 用 mcp_ 前缀区分 MCP 工具；缺省不发该字段）
    --permission-raw=<json>    toolCall.rawInput（缺省 {"CommandLine": "pwd"}）
    --permission-options=<json> 权限 options 数组（缺省 allow_once/reject_once）
    --exit-on-prompt   exit(7) as soon as session/prompt arrives
    --exit-after-chunk exit(8) after the first agent_message_chunk
    --hang-prompt      never answer session/prompt
    --error-prompt     answer session/prompt with JSON-RPC error -32603
    --error-model      answer session/set_model with JSON-RPC error -32602
    --gate-prompt=<p>  hold the prompt reply until file <p> exists (clear-fence
                       tests keep a normal prompt in flight while /clear runs)

Streaming / steer scripts（阶段2A，离线夹具）:
    --chunks=<json>         首条 prompt 的 agent_message_chunk 序列
    --steer-chunks=<json>   第二条及以后 prompt 的分段序列（list of list）
    --chunk-delay=<sec>     相邻 chunk 间隔（真实流式顺序可被测试观察）
    --ack-wait=<sec>        最后一条 chunk 之后、回复 end_turn 之前的等待，
                            用于让测试在「正文流完但 RPC 未收尾」窗口插入 steer
    --late-chunk=<text>     整轮收尾后再发一条游离 agent_message_chunk
    --steer-ack-mode=early  收到后续 prompt 时先给上一条 RPC 回 end_turn，
                            再继续流出旧正文、随后 user_message_chunk 与新正文
                            （主人实测语义；默认即此形态）
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
_permissions: dict[object, dict] = {}
# 无 id 的 `session/cancel` 通知：把该 session 的驱动线程按 cancelled 收尾。
_cancel_lock = threading.Lock()
_cancelled_sessions: dict[str, bool] = {}


def _permission_id() -> object:
    """权限请求的 RPC id；字符串形态用于验证客户端保留原 id 类型。"""
    value = _flag_value("--permission-id=")
    return value if value is not None else 9000


def _permission_title() -> str:
    return _flag_value("--permission-title=") or "write_file"


def _permission_name() -> str | None:
    """toolCall.name；缺省不发该字段（2B 帧形态保持不变）。

    阶段2C1 靠真实 `mcp_` 前缀识别 MCP 工具，所以夹具必须能如实带出这个名字。
    """
    return _flag_value("--permission-name=")


def _permission_raw() -> dict:
    raw = _flag_value("--permission-raw=")
    if raw is None:
        return {"CommandLine": "pwd"}
    try:
        parsed = json.loads(raw)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _permission_options() -> list:
    raw = _flag_value("--permission-options=")
    if raw is None:
        return [
            {"optionId": "allow-once", "name": "Allow", "kind": "allow_once"},
            {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
        ]
    try:
        parsed = json.loads(raw)
    except ValueError:
        return []
    return parsed if isinstance(parsed, list) else []


def _cancel_requested(session_id: str) -> bool:
    with _cancel_lock:
        return bool(_cancelled_sessions.get(session_id))


def _mark_cancelled(session_id: str) -> None:
    with _cancel_lock:
        _cancelled_sessions[session_id] = True


def _clear_cancelled(session_id: str) -> None:
    with _cancel_lock:
        _cancelled_sessions.pop(session_id, None)


def send(payload: dict) -> None:
    with _write_lock:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        sys.stdout.flush()
        # 记录真实出站帧（测试断言 cancelled／权限应答时不依赖客户端内部状态）。
        try:
            with open(LOG_PATH + ".sent", "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
                handle.flush()
        except OSError:
            pass


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


def _flag_value(prefix: str) -> str | None:
    for flag in FLAGS:
        if flag.startswith(prefix):
            return flag[len(prefix) :]
    return None


def _prompt_text(request: dict) -> str:
    params = request.get("params") or {}
    blocks = params.get("prompt") or []
    parts = [
        str(block.get("text") or "")
        for block in blocks
        if isinstance(block, dict)
    ]
    return "".join(parts)


_script_lock = threading.Lock()
_script_states: dict[str, dict] = {}


def _recv_scripted_prompt(session_id: str, message: dict) -> None:
    """流式脚本：所有 prompt 进同一队列，单条驱动线程按到达顺序推进。"""
    state = _script_states.setdefault(
        session_id, {"queue": [], "driver": False, "steer_index": 0, "requests": []}
    )
    with _script_lock:
        state["queue"].append(message)
        state["requests"].append(message)
        start = not state["driver"]
        state["driver"] = True
    if start:
        threading.Thread(
            target=_drive_script, args=(session_id,), daemon=True
        ).start()


def _ack(request: dict, stop_reason: str = "end_turn") -> None:
    send(
        {
            "jsonrpc": "2.0",
            "id": request.get("id"),
            "result": {"stopReason": stop_reason},
        }
    )


def _ask_permission(session_id: str, permission_id: object | None = None) -> None:
    """流式脚本里的权限请求：原 RPC id 等待管理员应答。"""
    permission_id = _permission_id() if permission_id is None else permission_id
    event = threading.Event()
    with _perm_lock:
        _permissions[permission_id] = {"event": event, "response": None}
    tool_call: dict = {
        "toolCallId": "tc-1",
        "title": _permission_title(),
        "rawInput": _permission_raw(),
    }
    tool_name = _permission_name()
    if tool_name is not None:
        tool_call["name"] = tool_name
    send(
        {
            "jsonrpc": "2.0",
            "id": permission_id,
            "method": "session/request_permission",
            "params": {
                "sessionId": session_id,
                "toolCall": tool_call,
                "options": _permission_options(),
            },
        }
    )
    event.wait(5)


def _drive_script(session_id: str) -> None:
    """实测 steer 语义：新 prompt 到达 → 上一条 RPC 提前 end_turn，旧正文继续流出，
    随后 user_message_chunk 与新正文，终态归最新 prompt。"""
    state = _script_states[session_id]
    first_chunks = json.loads(_flag_value("--chunks=") or "[]")
    steer_chunks = json.loads(_flag_value("--steer-chunks=") or "[]")
    delay = float(_flag_value("--chunk-delay=") or 0)
    ack_wait = float(_flag_value("--ack-wait=") or 0)
    with _script_lock:
        first = state["queue"].pop(0) if state["queue"] else None
    if first is None:
        state["driver"] = False
        return
    _clear_cancelled(session_id)
    if "--permission" in FLAGS:
        _ask_permission(session_id)
    round_ = {"request": first, "chunks": first_chunks, "pos": 0, "next": None}

    def take_pending() -> dict | None:
        with _script_lock:
            return state["queue"].pop(0) if state["queue"] else None

    def abort_cancelled() -> bool:
        """无 id 的 session/cancel：本会话所有在等 prompt 以 cancelled 收尾并停流。

        真实 agy 只回原 prompt id；steer 让多条 prompt 同一时刻在等，因此按到达顺序
        逐条回 cancelled（已 end_turn 过的重复帧由客户端自行忽略）。
        """
        if not _cancel_requested(session_id):
            return False
        with _script_lock:
            waiting = list(state["requests"])
            state["requests"].clear()
            state["queue"].clear()
            state["driver"] = False
        for request in waiting:
            _ack(request, "cancelled")
        return True

    while True:
        if abort_cancelled():
            return
        while round_["pos"] < len(round_["chunks"]):
            if abort_cancelled():
                return
            text = round_["chunks"][round_["pos"]]
            round_["pos"] += 1
            update(session_id, "agent_message_chunk", text)
            if delay:
                time.sleep(delay)
            pending = take_pending()
            if pending is not None and round_["next"] is None:
                _ack(round_["request"])
                round_["next"] = pending
        if round_["next"] is None:
            pending = take_pending()
            if pending is not None:
                _ack(round_["request"])
                round_["next"] = pending
        if round_["next"] is None and ack_wait:
            time.sleep(ack_wait)
            pending = take_pending()
            if pending is not None:
                _ack(round_["request"])
                round_["next"] = pending
        if round_["next"] is None:
            _ack(round_["request"])
            break
        nxt = round_["next"]
        with _script_lock:
            index = state["steer_index"]
            state["steer_index"] += 1
        chunks = steer_chunks[index] if index < len(steer_chunks) else (
            steer_chunks[-1] if steer_chunks else []
        )
        update(session_id, "user_message_chunk", _prompt_text(nxt))
        round_ = {"request": nxt, "chunks": chunks, "pos": 0, "next": None}

    late = _flag_value("--late-chunk=")
    if late:
        time.sleep(delay or 0.05)
        update(session_id, "agent_message_chunk", late)
    with _script_lock:
        state["driver"] = False


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
        _ask_permission(session_id)

    if _cancel_requested(session_id):
        _ack(request, "cancelled")
        return

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
        if _flag_value("--chunks=") is not None:
            _recv_scripted_prompt(params.get("sessionId") or "", message)
            return
        threading.Thread(target=run_prompt, args=(message,), daemon=True).start()
        return

    if method == "session/cancel":
        # 无 id 通知：真实 agy 用它把在跑 prompt 以 stopReason=cancelled 收尾，
        # 自身不回帧。驱动线程在下一次检查时按 cancelled 结束当前 RPC。
        _mark_cancelled(params.get("sessionId") or "")
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
